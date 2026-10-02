package driver

import (
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// serveHTTP serves /metrics (Prometheus), /healthz (the process is alive) and /readyz (the CSI
// endpoint is being served). Kubernetes probes use the last two. It runs until the process
// exits.
func (d *Driver) serveHTTP(addr string, ready *atomic.Bool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := d.metrics.reg.write(w); err != nil {
			slog.Debug("writing metrics", "err", err)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not serving yet", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	slog.Info("serving metrics and health", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("metrics and health server stopped", "err", err)
	}
}

// stagedVolumes counts the dfuse mounts at this driver's staging paths, from mountinfo only.
func stagedVolumes() float64 {
	prefix := stagingDir() + "/"
	ents, err := allMounts()
	if err != nil {
		return math.NaN()
	}
	n := 0
	for _, e := range ents {
		if e.fsType == fuseType && strings.HasPrefix(e.mountPoint, prefix) {
			n++
		}
	}
	return float64(n)
}
