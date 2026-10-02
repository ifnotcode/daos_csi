package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fuseType = "fuse.daos"
	// Written next to the staging path so that a restarted node plugin can remount volumes
	// whose dfuse died with the previous plugin container.
	stageRecord = "daos-csi.json"
)

// What the node service reads and does on the node; unit tests point them elsewhere.
var (
	kubeletDir    = "/var/lib/kubelet" // as seen inside the node plugin container
	mountinfoFile = "/proc/self/mountinfo"
	procDir       = "/proc" // the host's: the node plugin runs in the host's pid namespace
	statfs        = syscall.Statfs
	kill          = syscall.Kill
)

// stagingDir is kubelet's staging area for this driver.
func stagingDir() string {
	return filepath.Join(kubeletDir, "plugins/kubernetes.io/csi", DriverName)
}

// stagingGlob matches the stage records, one next to each staging path.
func stagingGlob() string { return filepath.Join(stagingDir(), "*", stageRecord) }

type stageInfo struct {
	VolumeID    string   `json:"volumeID"`
	StagingPath string   `json:"stagingPath"`
	DfuseArgs   []string `json:"dfuseArgs"`
}

func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	types := []csi.NodeServiceCapability_RPC_Type{csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME}
	if d.cfg.Health.Interval > 0 {
		// answered from the health checks
		types = append(types, csi.NodeServiceCapability_RPC_GET_VOLUME_STATS)
	}
	caps := make([]*csi.NodeServiceCapability, len(types))
	for i, t := range types {
		caps[i] = &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{Type: t}}}
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	// The scheduler counts the distinct volumes of this driver used by pods on a node, which is
	// the number of dfuse processes there, and keeps it at or below this.
	return &csi.NodeGetInfoResponse{NodeId: d.cfg.NodeID, MaxVolumesPerNode: d.cfg.MaxVolumesPerNode}, nil
}

// NodeStageVolume mounts the container with dfuse at the staging path, once per node.
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volID, staging := req.GetVolumeId(), req.GetStagingTargetPath()
	if volID == "" || staging == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging path are required")
	}
	if err := checkCaps([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	pool, cont, err := parseVolumeID(volID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	args, res, err := d.mountSpec(pool, cont, staging, req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "volume %s: %v", volID, err)
	}
	unlock := d.locks.lock(volID)
	defer unlock()

	if err := d.ensureMounted(staging, volID, args, res); err != nil {
		return nil, err
	}
	if err := applyRootAttrs(staging, req.GetVolumeContext()); err != nil {
		return nil, status.Errorf(codes.Internal, "setting root owner/mode of %s: %v", volID, err)
	}
	rec, _ := json.Marshal(stageInfo{VolumeID: volID, StagingPath: staging, DfuseArgs: args})
	if err := os.WriteFile(filepath.Join(filepath.Dir(staging), stageRecord), rec, 0o600); err != nil {
		slog.Warn("cannot write stage record", "path", staging, "err", err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// mountSpec is what the dfuse of a volume runs with: its arguments and, in mount pods, its
// resources (the node's defaults with the volume's settings over them).
func (d *Driver) mountSpec(pool, cont, staging string, volCtx map[string]string) ([]string,
	podResources, error) {
	var res podResources
	if d.cfg.MountMode == "pod" {
		res = d.cfg.MountPodResources.withVolume(volCtx)
		if err := res.validate(); err != nil {
			return nil, res, err
		}
	}
	extra := strings.Fields(volCtx[ParamDfuseArgs])
	if n := res.threadCount(extra); n > 0 {
		extra = append(extra, "--thread-count="+strconv.Itoa(n))
	}
	return dfuseArgs(pool, cont, staging, extra), res, nil
}

func dfuseArgs(pool, cont, staging string, extra []string) []string {
	// --multi-user adds allow_other, so that pods (other uids, user namespaces) can use the mount
	args := []string{"--multi-user", "--pool", pool, "--container", cont}
	return append(append(args, extra...), staging)
}

// ensureMounted makes sure a live dfuse serves staging, in a mount pod or in this container. res
// only applies to mount pods.
func (d *Driver) ensureMounted(staging, volID string, args []string, res podResources) error {
	if st, fsType, err := d.mnt.state(staging); err == nil && st == mounted && fsType == fuseType {
		return nil
	}
	start := time.Now()
	var err error
	if d.cfg.MountMode == "pod" {
		err = d.ensureMountPod(staging, volID, args, res)
	} else {
		err = d.ensureDfuse(staging, volID, args)
	}
	if err != nil {
		d.metrics.dfuseMount.inc("error")
		return err
	}
	d.metrics.dfuseMount.inc("ok")
	d.metrics.mountTime.observe(time.Since(start))
	return nil
}

// ensureDfuse makes sure a live dfuse is mounted at staging, (re)starting it if needed.
func (d *Driver) ensureDfuse(staging, volID string, args []string) error {
	st, fsType, err := d.mnt.state(staging)
	if err != nil {
		return status.Errorf(codes.Internal, "checking %s: %v", staging, err)
	}
	switch {
	case st == mounted && fsType == fuseType:
		return nil
	case st == mounted:
		return status.Errorf(codes.Internal, "%s is already mounted with %s", staging, fsType)
	case st == staleMount:
		slog.Warn("dfuse mount is stale, remounting", "path", staging, "volume", volID)
		d.metrics.stale.inc("staging")
		if err := d.mnt.unmountAll(staging); err != nil {
			return status.Errorf(codes.Internal, "unmounting stale %s: %v", staging, err)
		}
	}
	if err := os.MkdirAll(staging, 0o750); err != nil {
		return status.Errorf(codes.Internal, "mkdir %s: %v", staging, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// dfuse forks and the parent exits once the mount is registered with the kernel. The daemon
	// keeps running in this container, detached from this request.
	cmd := exec.CommandContext(ctx, d.cfg.DfuseBin, args...)
	cmd.Env = os.Environ()
	if d.cfg.DfuseLogDir != "" {
		_ = os.MkdirAll(d.cfg.DfuseLogDir, 0o750)
		name := strings.NewReplacer("/", "_", " ", "_").Replace(volID) + ".log"
		cmd.Env = append(cmd.Env, "D_LOG_FILE="+filepath.Join(d.cfg.DfuseLogDir, name))
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return status.Errorf(codes.Internal, "dfuse %s: %v: %s", strings.Join(args, " "), err,
			strings.TrimSpace(out.String()))
	}
	if st, fsType, _ := d.mnt.state(staging); st != mounted || fsType != fuseType {
		return status.Errorf(codes.Internal, "dfuse exited but %s is not mounted: %s", staging,
			strings.TrimSpace(out.String()))
	}
	slog.Info("dfuse mounted", "path", staging, "volume", volID)
	return nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volID, staging := req.GetVolumeId(), req.GetStagingTargetPath()
	if volID == "" || staging == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging path are required")
	}
	unlock := d.locks.lock(volID)
	defer unlock()

	if d.cfg.MountMode == "pod" {
		if err := d.deleteMountPod(staging, volID); err != nil {
			return nil, err
		}
	} else if st, _, err := d.mnt.state(staging); err == nil && st != notMounted {
		// Let dfuse flush and exit cleanly; fall back to a (lazy) unmount.
		if out, err := exec.Command("fusermount3", "-u", staging).CombinedOutput(); err != nil {
			slog.Warn("fusermount3 failed, unmounting", "path", staging, "err", err,
				"out", strings.TrimSpace(string(out)))
		}
		if err := d.mnt.unmountAll(staging); err != nil {
			return nil, status.Errorf(codes.Internal, "unmounting %s: %v", staging, err)
		}
	}
	_ = os.Remove(filepath.Join(filepath.Dir(staging), stageRecord))
	if err := os.Remove(staging); err != nil && !os.IsNotExist(err) {
		slog.Warn("cannot remove staging dir", "path", staging, "err", err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staged dfuse mount into the pod's volume directory.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Inline volumes are not staged. kubelet only marks them with csi.storage.k8s.io/ephemeral
	// when podInfoOnMount is set, so recognize them by their type.
	if req.GetVolumeContext()[ParamType] != "" || req.GetStagingTargetPath() == "" {
		return d.publishEphemeral(req)
	}
	volID, staging, target := req.GetVolumeId(), req.GetStagingTargetPath(), req.GetTargetPath()
	if volID == "" || staging == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id, staging and target path are required")
	}
	if err := checkCaps([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	unlock := d.locks.lock(volID)
	defer unlock()

	if st, _, err := d.mnt.state(target); err == nil && st == mounted {
		return &csi.NodePublishVolumeResponse{}, nil
	} else if st == staleMount {
		slog.Warn("pod's mount is stale, mounting again", "path", target, "volume", volID)
		d.metrics.stale.inc("target")
		_ = d.mnt.unmountAll(target)
	}

	// The staging mount may be stale after a node plugin restart; bring it back first.
	if st, fsType, _ := d.mnt.state(staging); st != mounted || fsType != fuseType {
		pool, cont, err := parseVolumeID(volID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		args, res, err := d.mountSpec(pool, cont, staging, req.GetVolumeContext())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "volume %s: %v", volID, err)
		}
		if err := d.ensureMounted(staging, volID, args, res); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "volume is not staged: %v", err)
		}
	}

	if err := os.MkdirAll(target, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir %s: %v", target, err)
	}
	if err := d.mnt.bind(staging, target, req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount %s -> %s: %v", staging, target, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// publishEphemeral serves inline CSI volumes. The only type is "agent-socket": the directory with
// daos_agent.sock, which libdaos (libpil4dfs, the daos CLI) needs inside the pod. Unlike a hostPath
// volume it is allowed by the baseline and restricted Pod Security levels.
func (d *Driver) publishEphemeral(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if typ := req.GetVolumeContext()[ParamType]; typ != TypeAgentSocket {
		return nil, status.Errorf(codes.InvalidArgument,
			"inline volumes need volumeAttributes.type=%s, got %q", TypeAgentSocket, typ)
	}
	if st, _, err := d.mnt.state(target); err == nil && st == mounted {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := os.MkdirAll(target, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir %s: %v", target, err)
	}
	// Read-only is enough: connecting to a unix socket does not write to the filesystem.
	if err := d.mnt.bind(d.cfg.AgentDirInPlugin, target, true); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount agent dir -> %s: %v", target, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volID, target := req.GetVolumeId(), req.GetTargetPath()
	if volID == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	unlock := d.locks.lock(volID)
	defer unlock()

	if err := d.mnt.unmountAll(target); err != nil {
		return nil, status.Errorf(codes.Internal, "unmounting %s: %v", target, err)
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "removing %s: %v", target, err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// applyRootAttrs sets the owner and mode of the volume root from the volume context, if given
// and different. dfuse runs with --multi-user, so it stores the requested owner in DAOS.
func applyRootAttrs(root string, volCtx map[string]string) error {
	var st syscall.Stat_t
	if err := syscall.Stat(root, &st); err != nil {
		return err
	}
	uid, gid := -1, -1
	if v := volCtx[ParamRootUID]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", ParamRootUID, err)
		}
		if uint32(n) != st.Uid {
			uid = n
		}
	}
	if v := volCtx[ParamRootGID]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", ParamRootGID, err)
		}
		if uint32(n) != st.Gid {
			gid = n
		}
	}
	if uid != -1 || gid != -1 {
		if err := os.Lchown(root, uid, gid); err != nil {
			return err
		}
	}
	if v := volCtx[ParamRootMode]; v != "" {
		m, err := strconv.ParseUint(v, 8, 32)
		if err != nil {
			return fmt.Errorf("%s: %w", ParamRootMode, err)
		}
		if uint32(m)&0o7777 != st.Mode&0o7777 {
			if err := syscall.Chmod(root, uint32(m)); err != nil {
				return err
			}
		}
	}
	return nil
}

// recoverStaged remounts staged volumes whose dfuse died with a previous node plugin container.
// Pods that were already running keep their dead bind mounts and must be restarted; new pods and
// pods restarted afterwards get the new mount.
func (d *Driver) recoverStaged() {
	recs, _ := filepath.Glob(stagingGlob())
	for _, path := range recs {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var info stageInfo
		if err := json.Unmarshal(data, &info); err != nil || info.StagingPath == "" {
			continue
		}
		if st, _, _ := d.mnt.state(info.StagingPath); st == mounted {
			continue
		}
		if err := d.ensureMounted(info.StagingPath, info.VolumeID, info.DfuseArgs,
			podResources{}); err != nil {
			slog.Error("recovering staged volume", "volume", info.VolumeID, "err", err)
			continue
		}
		slog.Info(fmt.Sprintf("recovered staged volume %s; pods using it must be restarted",
			info.VolumeID))
	}
}
