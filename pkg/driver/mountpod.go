package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mount pods: in "pod" mount mode each (volume, node) pair gets its own pod running dfuse in the
// foreground on the staging path. The FUSE daemons then live independently of the node plugin,
// so restarting or upgrading the plugin does not break the volumes of running pods.

const (
	mountPodLabel = "daos-csi/mount"
	annVolume     = "daos-csi/volume"
	annStaging    = "daos-csi/staging"
	mountTimeout  = 2 * time.Minute
)

func mountPodName(node, volID string) string {
	h := sha256.Sum256([]byte(node + "\x00" + volID))
	return "daos-mount-" + hex.EncodeToString(h[:])[:20]
}

func (d *Driver) mountPodSpec(name, staging, volID string, args []string, res podResources) map[string]any {
	env := []map[string]any{}
	if d.cfg.DfuseLogDir != "" {
		logName := strings.NewReplacer("/", "_", " ", "_").Replace(volID) + ".log"
		env = append(env, map[string]any{"name": "D_LOG_FILE", "value": d.cfg.DfuseLogDir + "/" + logName})
	}
	// A dfuse that died leaves a stale mount behind; clear it before starting again.
	script := `mp="$1"; shift; fusermount3 -uz "$mp" 2>/dev/null; mkdir -p "$mp"; exec "$@"`
	cmd := append([]string{"sh", "-c", script, "mount", staging, d.cfg.DfuseBin, "--foreground"}, args...)

	hostDir := func(n, path, typ string) map[string]any {
		return map[string]any{"name": n, "hostPath": map[string]any{"path": path, "type": typ}}
	}
	mounts := []any{
		map[string]any{"name": "kubelet-dir", "mountPath": "/var/lib/kubelet",
			"mountPropagation": "Bidirectional"},
		map[string]any{"name": "agent", "mountPath": "/var/run/daos_agent"},
		map[string]any{"name": "dev-fuse", "mountPath": "/dev/fuse"},
	}
	vols := []any{
		hostDir("kubelet-dir", "/var/lib/kubelet", "Directory"),
		hostDir("agent", d.cfg.AgentDir, "Directory"),
		hostDir("dev-fuse", "/dev/fuse", "CharDevice"),
	}
	if d.cfg.DfuseLogDir != "" {
		mounts = append(mounts, map[string]any{"name": "logs", "mountPath": d.cfg.DfuseLogDir})
		vols = append(vols, hostDir("logs", d.cfg.DfuseLogDir, "DirectoryOrCreate"))
	}
	container := map[string]any{
		"name":            "dfuse",
		"image":           d.cfg.MountImage,
		"imagePullPolicy": "IfNotPresent",
		"command":         cmd,
		"env":             env,
		"securityContext": map[string]any{"privileged": true},
		"lifecycle": map[string]any{"preStop": map[string]any{"exec": map[string]any{
			"command": []string{"fusermount3", "-u", staging},
		}}},
		"volumeMounts": mounts,
	}
	if r := res.spec(); r != nil {
		container["resources"] = r
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":        name,
			"labels":      map[string]any{"app": "daos-csi-mount", mountPodLabel: "true"},
			"annotations": map[string]any{annVolume: volID, annStaging: staging},
		},
		"spec": map[string]any{
			"nodeName":    d.cfg.NodeID,
			"hostNetwork": true,
			// dfuse --allow-idmap only shares its pool and container handles with interception
			// libraries in its pid namespace or one nested in it, i.e. in any pod if it runs in
			// the host's.
			"hostPID":                       true,
			"dnsPolicy":                     "ClusterFirstWithHostNet",
			"priorityClassName":             "system-node-critical",
			"restartPolicy":                 "Always",
			"terminationGracePeriodSeconds": 30,
			"tolerations":                   []any{map[string]any{"operator": "Exists"}},
			"enableServiceLinks":            false,
			"automountServiceAccountToken":  false,
			"containers":                    []any{container},
			"volumes":                       vols,
		},
	}
}

// ensureMountPod makes sure the mount pod for the volume exists and its dfuse is mounted at
// staging.
func (d *Driver) ensureMountPod(staging, volID string, args []string, res podResources) error {
	st, fsType, err := d.mnt.state(staging)
	if err != nil {
		return status.Errorf(codes.Internal, "checking %s: %v", staging, err)
	}
	if st == mounted && fsType == fuseType {
		return nil
	}
	if st == staleMount {
		// the pod's dfuse died; its restart clears the mount and mounts again
		d.metrics.stale.inc("staging")
	}
	if st == mounted {
		return status.Errorf(codes.Internal, "%s is already mounted with %s", staging, fsType)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mountTimeout)
	defer cancel()
	name := mountPodName(d.cfg.NodeID, volID)
	pod, err := d.kube.getPod(ctx, name)
	switch {
	case errors.Is(err, errNotFound):
		if err := os.MkdirAll(staging, 0o750); err != nil {
			return status.Errorf(codes.Internal, "mkdir %s: %v", staging, err)
		}
		if err := d.kube.createPod(ctx, d.mountPodSpec(name, staging, volID, args, res)); err != nil {
			return status.Errorf(codes.Internal, "creating mount pod %s: %v", name, err)
		}
		slog.Info("created mount pod", "pod", name, "volume", volID, "path", staging)
	case err != nil:
		return status.Errorf(codes.Unavailable, "getting mount pod %s: %v", name, err)
	case pod.Metadata.DeletionTimestamp != nil:
		return status.Errorf(codes.Unavailable, "mount pod %s is terminating, retry", name)
	case pod.Status.Phase == "Failed":
		return d.dropFailedMountPod(ctx, name, pod)
	case pod.Metadata.Annotations[annStaging] != staging:
		return status.Errorf(codes.Internal, "mount pod %s serves %s, not %s", name,
			pod.Metadata.Annotations[annStaging], staging)
	}

	// Wait for dfuse to register the mount (visible here through Bidirectional propagation).
	for i := 1; ; i++ {
		if st, fsType, _ := d.mnt.state(staging); st == mounted && fsType == fuseType {
			return nil
		}
		// kubelet can reject the pod at once, e.g. when its requests do not fit the node
		if i%4 == 0 {
			if pod, err := d.kube.getPod(ctx, name); err == nil && pod.Status.Phase == "Failed" {
				return d.dropFailedMountPod(ctx, name, pod)
			}
		}
		select {
		case <-ctx.Done():
			return status.Errorf(codes.DeadlineExceeded, "dfuse in mount pod %s did not mount %s: %s",
				name, staging, d.mountPodProblem(name))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// dropFailedMountPod deletes a mount pod that failed for good, so that the next attempt creates it
// again, and reports why it failed. A restartPolicy: Always pod only fails when kubelet rejects
// it: its requests do not fit the node (OutOfcpu, OutOfmemory), or the node is shutting down.
func (d *Driver) dropFailedMountPod(ctx context.Context, name string, pod *podStatus) error {
	if err := d.kube.deletePod(ctx, name); err != nil {
		slog.Warn("deleting failed mount pod", "pod", name, "err", err)
	}
	slog.Warn("mount pod failed, deleted it", "pod", name, "reason", pod.Status.Reason,
		"message", pod.Status.Message)
	return status.Errorf(codes.ResourceExhausted, "mount pod %s failed: %s: %s", name,
		pod.Status.Reason, pod.Status.Message)
}

// mountPodProblem summarizes why a mount pod's dfuse is not running, for error messages.
func (d *Driver) mountPodProblem(name string) string {
	pod, err := d.kube.getPod(context.Background(), name)
	if err != nil {
		return err.Error()
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil {
			return fmt.Sprintf("%s: %s (restarts %d)", w.Reason, w.Message, cs.RestartCount)
		}
		if t := cs.State.Terminated; t != nil {
			return fmt.Sprintf("terminated %s exit %d: %s", t.Reason, t.ExitCode, t.Message)
		}
	}
	return "pod phase " + pod.Status.Phase
}

// deleteMountPod stops the volume's dfuse and waits for the staging mount to go away.
func (d *Driver) deleteMountPod(staging, volID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mountTimeout)
	defer cancel()
	name := mountPodName(d.cfg.NodeID, volID)
	if err := d.kube.deletePod(ctx, name); err != nil {
		return status.Errorf(codes.Internal, "deleting mount pod %s: %v", name, err)
	}
	for {
		if _, err := d.kube.getPod(ctx, name); errors.Is(err, errNotFound) {
			// dfuse unmounts on SIGTERM and in preStop; clean up if it could not
			return d.mnt.unmountAll(staging)
		}
		select {
		case <-ctx.Done():
			// Do not leave a mount behind even if the pod is slow to go
			return d.mnt.unmountAll(staging)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
