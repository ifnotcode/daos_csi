// daos-csi is a CSI driver for DAOS POSIX containers mounted with dfuse.
package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/daos-stack/daos-csi/pkg/driver"
)

func main() {
	var cfg driver.Config

	flag.StringVar(&cfg.Endpoint, "endpoint", "unix:///csi/csi.sock", "CSI endpoint")
	flag.StringVar(&cfg.NodeID, "node-id", os.Getenv("NODE_ID"), "node name (node mode)")
	flag.BoolVar(&cfg.Controller, "controller", false, "serve the controller service")
	flag.BoolVar(&cfg.Node, "node", false, "serve the node service")
	flag.StringVar(&cfg.DaosBin, "daos", "daos", "daos CLI used by the controller")
	flag.StringVar(&cfg.DfuseBin, "dfuse", "dfuse", "dfuse binary used by the node service")
	flag.StringVar(&cfg.DfuseLogDir, "dfuse-log-dir", "/var/log/daos-csi",
		"directory for per-volume dfuse logs (D_LOG_FILE), empty to disable")
	flag.StringVar(&cfg.MountMode, "mount-mode", "pod",
		"where dfuse runs: pod (a mount pod per volume and node) or process (in this container)")
	flag.StringVar(&cfg.MountImage, "mount-image", os.Getenv("MOUNT_IMAGE"),
		"image for mount pods (mount-mode=pod)")
	flag.StringVar(&cfg.AgentDir, "agent-dir", "/var/run/daos_agent",
		"host directory containing daos_agent.sock, mounted into mount pods")
	flag.StringVar(&cfg.AgentDirInPlugin, "agent-dir-in-plugin", "/host/var/run/daos_agent",
		"the host's daos_agent directory as mounted in this container (for agent-socket volumes)")
	flag.StringVar(&cfg.HTTPEndpoint, "http-endpoint", "",
		"host:port for /metrics, /healthz and /readyz, empty to disable")
	flag.StringVar(&cfg.MountPodResources.CPURequest, "mount-pod-cpu-request", "",
		"CPU request of dfuse in mount pods, e.g. 100m (volumes may override: dfuseCpuRequest)")
	flag.StringVar(&cfg.MountPodResources.CPULimit, "mount-pod-cpu-limit", "",
		"CPU limit of dfuse in mount pods, e.g. 2 (volumes may override: dfuseCpuLimit)")
	flag.StringVar(&cfg.MountPodResources.MemoryRequest, "mount-pod-memory-request", "",
		"memory request of dfuse in mount pods, e.g. 256Mi (volumes may override: dfuseMemoryRequest)")
	flag.StringVar(&cfg.MountPodResources.MemoryLimit, "mount-pod-memory-limit", "",
		"memory limit of dfuse in mount pods, e.g. 4Gi (volumes may override: dfuseMemoryLimit)")
	flag.DurationVar(&cfg.Health.Interval, "health-interval", time.Minute,
		"how often to probe each dfuse mount with statfs (a DAOS pool query), 0 to disable")
	flag.DurationVar(&cfg.Health.Timeout, "health-timeout", 30*time.Second,
		"a probe unanswered for longer makes the volume unhealthy")
	flag.DurationVar(&cfg.Health.KillAfter, "health-kill-after", 5*time.Minute,
		"kill a dfuse unhealthy for this long, so that it is restarted; 0 to never kill")
	flag.Int64Var(&cfg.MaxVolumesPerNode, "max-volumes-per-node", 0,
		"DAOS volumes (dfuse processes) the scheduler may place on a node, 0 for no limit")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn or error")
	logFormat := flag.String("log-format", "text", "log format: text or json")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		slog.Error("bad -log-level", "err", err)
		os.Exit(2)
	}
	opts := &slog.HandlerOptions{Level: level}
	switch *logFormat {
	case "text":
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, opts)))
	case "json":
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, opts)))
	default:
		slog.Error("-log-format must be text or json", "got", *logFormat)
		os.Exit(2)
	}
	logger := slog.Default()
	if cfg.NodeID != "" {
		logger = logger.With("node", cfg.NodeID)
	}
	slog.SetDefault(logger)

	if !cfg.Controller && !cfg.Node {
		slog.Error("at least one of -controller or -node is required")
		os.Exit(2)
	}
	if cfg.Node && cfg.NodeID == "" {
		slog.Error("-node-id (or NODE_ID) is required in node mode")
		os.Exit(2)
	}

	if err := driver.Run(cfg); err != nil {
		slog.Error("driver exited", "err", err)
		os.Exit(1)
	}
}
