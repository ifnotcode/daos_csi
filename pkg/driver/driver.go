// Package driver implements a CSI driver for DAOS POSIX containers.
//
// Volumes are DAOS POSIX containers identified as "<pool>/<container>". The controller creates
// and destroys containers with the daos CLI. The node service mounts a container once per node
// with dfuse at the staging path (NodeStageVolume) and bind-mounts it into each pod
// (NodePublishVolume). The bind mount is what the container runtime idmaps for pods running with
// hostUsers: false, which requires a dfuse built with FUSE_CAP_ALLOW_IDMAP and Linux >= 6.12.
package driver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	DriverName = "csi.daos.io"
	Version    = "0.1.0"

	// Volume context / StorageClass parameter keys
	ParamPool         = "pool"
	ParamContainer    = "container"
	ParamDfuseArgs    = "dfuseArgs"  // extra dfuse command line arguments, space separated
	ParamContProps    = "properties" // daos cont create --properties
	ParamContOclass   = "oclass"     // daos cont create --oclass
	ParamContDirClass = "dirOclass"  // daos cont create --dir-oclass
	ParamContFileCls  = "fileOclass" // daos cont create --file-oclass
	ParamChunkSize    = "chunkSize"  // daos cont create --chunk-size
	ParamContGroup    = "group"      // daos cont create --group, e.g. "admin@"
	// Extra container ACL entries, comma separated, e.g. "A::admin@:rwta". Pods reach DAOS as
	// the principal of their uid, or as daos_agent's client_user_map entry for uids unknown on
	// the node (user namespaces); that principal needs an entry here. The dfuse mount pods run
	// as root and use OWNER@ (the controller creates the container as root).
	ParamContACL = "acl"
	// Owner and mode applied to the container root directory by the node when it mounts the
	// volume, so that non-root pods can write to a fresh volume without fsGroup (which would
	// chown the whole tree).
	ParamRootUID  = "rootUid"
	ParamRootGID  = "rootGid"
	ParamRootMode = "rootMode" // octal, e.g. "0775"

	// Inline (ephemeral) volumes: volumeAttributes.type
	ParamType       = "type"
	TypeAgentSocket = "agent-socket"
)

type Config struct {
	Endpoint    string
	NodeID      string
	Controller  bool
	Node        bool
	DaosBin     string
	DfuseBin    string
	DfuseLogDir string
	// "pod": dfuse runs in a mount pod per volume and node (survives node plugin restarts);
	// "process": dfuse runs inside the node plugin container.
	MountMode  string
	MountImage string // image of the mount pods (normally the node plugin's own image)
	AgentDir   string // host directory with daos_agent.sock
	// The same directory as seen by the node plugin. The plugin mounts the parent (/var/run)
	// so that the directory stays reachable when it is deleted and recreated, e.g. by systemd.
	AgentDirInPlugin string
	// host:port for /metrics, /healthz and /readyz; empty to disable
	HTTPEndpoint string
	// default resources of the dfuse container in mount pods; volumes can override them
	MountPodResources podResources
	// how many DAOS volumes the scheduler may put on this node (each one is a dfuse); 0: no limit
	MaxVolumesPerNode int64
	// health checks of the dfuse mounts (node service)
	Health healthConfig
}

type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	cfg Config
	// per-volume serialization of node and controller operations
	locks   *keyLocks
	kube    *kubeClient
	mnt     mounter
	metrics *driverMetrics
	health  *healthTracker
}

func newDriver(cfg Config) *Driver {
	d := &Driver{cfg: cfg, locks: newKeyLocks(), mnt: sysMounter{}, health: newHealthTracker()}
	var staged func() float64
	var healthy func() map[string]float64
	if cfg.Node {
		staged = stagedVolumes
		if cfg.Health.Interval > 0 {
			healthy = d.healthGauges
		}
	}
	d.metrics = newDriverMetrics(staged, healthy)
	return d
}

func Run(cfg Config) error {
	d := newDriver(cfg)

	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme != "unix" {
		return fmt.Errorf("endpoint must be unix://path, got %q", cfg.Endpoint)
	}
	if err := cfg.MountPodResources.validate(); err != nil {
		return fmt.Errorf("mount pod resources: %w", err)
	}
	sock := u.Path
	if sock == "" {
		sock = u.Opaque
	}
	if err := os.Remove(sock); err != nil && !os.IsNotExist(err) {
		return err
	}
	lis, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}

	srv := grpc.NewServer(grpc.UnaryInterceptor(d.instrument))
	csi.RegisterIdentityServer(srv, d)
	if cfg.Controller {
		csi.RegisterControllerServer(srv, d)
	}
	if cfg.Node {
		switch cfg.MountMode {
		case "pod":
			if cfg.MountImage == "" {
				return fmt.Errorf("mount mode pod needs -mount-image (MOUNT_IMAGE)")
			}
			if d.kube, err = newKubeClient(); err != nil {
				return fmt.Errorf("mount mode pod: %w", err)
			}
		case "process":
			d.recoverStaged()
		default:
			return fmt.Errorf("unknown mount mode %q", cfg.MountMode)
		}
		csi.RegisterNodeServer(srv, d)
	}

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		// Leave dfuse mounts alone: they belong to running pods.
		srv.GracefulStop()
	}()

	if cfg.Node && cfg.Health.Interval > 0 {
		go d.healthLoop()
	}
	var ready atomic.Bool
	if cfg.HTTPEndpoint != "" {
		go d.serveHTTP(cfg.HTTPEndpoint, &ready)
	}
	ready.Store(true)
	slog.Info("serving", "driver", DriverName, "version", Version, "endpoint", cfg.Endpoint,
		"controller", cfg.Controller, "node", cfg.Node, "nodeID", cfg.NodeID)
	return srv.Serve(lis)
}

// instrument logs CSI calls and records their metrics.
func (d *Driver) instrument(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (any, error) {
	method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
	quiet := method == "Probe" || method == "NodeGetCapabilities" || method == "GetPluginInfo" ||
		method == "NodeGetInfo" || method == "GetPluginCapabilities"
	attrs := []any{"method", method}
	if v, ok := req.(interface{ GetVolumeId() string }); ok && v.GetVolumeId() != "" {
		attrs = append(attrs, "volume", v.GetVolumeId())
	}
	if v, ok := req.(interface{ GetTargetPath() string }); ok && v.GetTargetPath() != "" {
		attrs = append(attrs, "target", v.GetTargetPath())
	}
	if !quiet {
		slog.Info("call", attrs...)
		slog.Debug("call request", "method", method, "req", req)
	}
	start := time.Now()
	resp, err := handler(ctx, req)
	elapsed := time.Since(start)
	d.metrics.calls.inc(method, status.Code(err).String())
	d.metrics.callTime.observe(elapsed, method)
	attrs = append(attrs, "duration", elapsed.Round(time.Millisecond))
	if err != nil {
		slog.Error("call failed", append(attrs, "err", err)...)
	} else if !quiet {
		slog.Info("call done", attrs...)
	}
	return resp, err
}

// parseVolumeID splits "<pool>/<container>".
func parseVolumeID(id string) (pool, cont string, err error) {
	pool, cont, ok := strings.Cut(id, "/")
	if !ok || pool == "" || cont == "" || strings.Contains(cont, "/") {
		return "", "", fmt.Errorf("volume id %q is not <pool>/<container>", id)
	}
	return pool, cont, nil
}
