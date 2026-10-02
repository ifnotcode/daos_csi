package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Health checks of the dfuse mounts on this node (node service).
//
// Every HealthInterval each staged volume gets a statfs probe. dfuse answers it with a pool query,
// a round trip to the DAOS servers that the kernel never caches, so the probe fails when dfuse or
// its network path is stuck, e.g. on a dead network port. A probe runs in a goroutine of its own,
// at most one per volume, and never under the volume's lock: a hung probe only blocks itself.
//
// A volume is unhealthy while its last probe failed, its probe has gone unanswered for longer
// than HealthTimeout, or its staging path is not mounted. Then:
//   - after HealthKillAfter of it, the dfuse serving the volume is killed. A mount pod restarts
//     it (in mount mode process, the node service does). The kill is the only disruptive step,
//     hence the long default: a slow but working DAOS system must not lose its mounts.
//   - daos_csi_volume_healthy is 0 for it, and the node service logs the problem.
//
// Pods keep the mount of the dfuse they started with. Once a new dfuse serves the staging path,
// each pod's target that is still a mount of an earlier dfuse (another device number in
// mountinfo) is bind-mounted again from the staging path, on the node. A container that restarts
// then gets the working mount (runc binds the target anew), so an application whose liveness
// probe covers the volume recovers by itself. A running container keeps its dead mount.

type healthConfig struct {
	Interval  time.Duration // 0: no health checks
	Timeout   time.Duration // a probe unanswered for longer makes the volume unhealthy
	KillAfter time.Duration // kill dfuse after being unhealthy for this long; 0: never
}

type volHealth struct {
	volID, staging string
	args           []string

	// The dfuse serving the staging path now: the device of its mount, when it was first seen,
	// and a generation number. Results of probes of an earlier dfuse are ignored, and a new one
	// gets the whole KillAfter before it can be killed: right after an outage it may still be
	// connecting.
	dev        string
	devSince   time.Time
	gen        int
	probeGen   int
	probeStart time.Time // zero: no probe in flight
	timedOut   bool      // the probe in flight has been counted as timed out
	badSince   time.Time // zero: healthy
	problem    string
	probed     bool // a probe has succeeded at least once
	total      uint64
	avail      uint64
	lastKill   time.Time
}

type healthTracker struct {
	mu   sync.Mutex
	vols map[string]*volHealth // by volume id
}

func newHealthTracker() *healthTracker { return &healthTracker{vols: map[string]*volHealth{}} }

// setBad marks h unhealthy for the given reason; mu held.
func (h *volHealth) setBad(now time.Time, problem string) {
	if h.badSince.IsZero() {
		h.badSince = now
	}
	h.problem = problem
}

// healthLoop runs the checks until the process exits.
func (d *Driver) healthLoop() {
	slog.Info("checking dfuse mounts", "interval", d.cfg.Health.Interval,
		"timeout", d.cfg.Health.Timeout, "killAfter", d.cfg.Health.KillAfter)
	d.checkHealth(time.Now())
	for range time.Tick(d.cfg.Health.Interval) {
		d.checkHealth(time.Now())
	}
}

// stagedRecords returns the stage records of the volumes staged on this node.
func stagedRecords() []stageInfo {
	paths, _ := filepath.Glob(stagingGlob())
	var res []stageInfo
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var info stageInfo
		if json.Unmarshal(data, &info) == nil && info.VolumeID != "" && info.StagingPath != "" {
			res = append(res, info)
		}
	}
	return res
}

// podTargets returns the pods' mount points of this driver's volumes, by volume id, from the
// vol_data.json that kubelet keeps next to each of them.
func podTargets() map[string][]string {
	files, _ := filepath.Glob(filepath.Join(kubeletDir,
		"pods/*/volumes/kubernetes.io~csi/*/vol_data.json"))
	res := map[string][]string{}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var v struct {
			DriverName   string `json:"driverName"`
			VolumeHandle string `json:"volumeHandle"`
		}
		if json.Unmarshal(data, &v) == nil && v.DriverName == DriverName {
			res[v.VolumeHandle] = append(res[v.VolumeHandle], filepath.Join(filepath.Dir(f), "mount"))
		}
	}
	return res
}

func (d *Driver) checkHealth(now time.Time) {
	recs := stagedRecords()
	ents, err := allMounts()
	if err != nil {
		slog.Warn("health check: reading mountinfo", "err", err)
		return
	}
	top := map[string]mountEntry{} // topmost mount at each mount point
	for _, e := range ents {
		top[e.mountPoint] = e
	}
	targets := podTargets()

	t := d.health
	t.mu.Lock()
	seen := map[string]bool{}
	var toKill, toRestart []*volHealth
	var healthy []stageInfo
	for _, rec := range recs {
		seen[rec.VolumeID] = true
		h := t.vols[rec.VolumeID]
		if h == nil {
			h = &volHealth{volID: rec.VolumeID}
			t.vols[rec.VolumeID] = h
		}
		h.staging, h.args = rec.StagingPath, rec.DfuseArgs
		if d.cfg.MountMode == "process" && h.problem == "dfuse is gone" {
			toRestart = append(toRestart, h)
		}
		e, ok := top[rec.StagingPath]
		switch {
		case !ok || e.fsType != fuseType:
			// a mount pod restarting dfuse, or a dfuse that cannot mount (e.g. the network is
			// still down): nothing to probe and nothing to kill
			h.setBad(now, "staging path not mounted by dfuse")
			if d.cfg.MountMode == "process" {
				toRestart = append(toRestart, h)
			}
			continue
		case e.dev != h.dev:
			// another dfuse than at the last check: probe it afresh. A probe still blocked on
			// the old one is ignored when it returns.
			h.dev, h.devSince, h.gen = e.dev, now, h.gen+1
			h.probeStart, h.timedOut, h.probeGen = now, false, h.gen
			go d.probe(h, rec.StagingPath, statfs, h.gen)
		case h.probeStart.IsZero():
			h.probeStart, h.timedOut, h.probeGen = now, false, h.gen
			go d.probe(h, rec.StagingPath, statfs, h.gen)
		case now.Sub(h.probeStart) > d.cfg.Health.Timeout:
			if !h.timedOut {
				h.timedOut = true
				d.metrics.probeFailures.inc("timeout")
			}
			h.setBad(now, fmt.Sprintf("dfuse has not answered statfs for %s",
				now.Sub(h.probeStart).Round(time.Second)))
		}
		if h.badSince.IsZero() {
			healthy = append(healthy, rec)
			continue
		}
		ka := d.cfg.Health.KillAfter
		since := h.badSince
		for _, t := range []time.Time{h.lastKill, h.devSince} {
			if t.After(since) {
				since = t // a restarted or new dfuse gets as long again
			}
		}
		if ka > 0 && now.Sub(since) > ka && h.problem != "dfuse is gone" {
			h.lastKill = now
			toKill = append(toKill, h)
		}
	}
	for id := range t.vols {
		if !seen[id] {
			delete(t.vols, id)
		}
	}
	t.mu.Unlock()

	for _, h := range toKill {
		d.killDfuse(h)
	}
	for _, h := range toRestart {
		go d.restartDfuse(h)
	}
	for _, rec := range healthy {
		d.rebindTargets(rec, top, targets[rec.VolumeID])
	}
}

// probe statfs-es the staging path and records the result, unless the dfuse it probed (gen) has
// been replaced meanwhile.
func (d *Driver) probe(h *volHealth, staging string, statfs func(string, *syscall.Statfs_t) error,
	gen int) {
	var st syscall.Statfs_t
	err := statfs(staging, &st)
	now := time.Now()
	t := d.health
	t.mu.Lock()
	defer t.mu.Unlock()
	if gen != h.gen || gen != h.probeGen {
		return
	}
	h.probeStart = time.Time{}
	switch {
	case err == nil:
		if !h.badSince.IsZero() {
			slog.Info("dfuse answers again", "volume", h.volID, "after",
				now.Sub(h.badSince).Round(time.Second))
		}
		h.badSince, h.problem, h.probed = time.Time{}, "", true
		h.total = st.Blocks * uint64(st.Bsize)
		h.avail = st.Bavail * uint64(st.Bsize)
	case errors.Is(err, syscall.ENOTCONN):
		// dfuse exited: its mount pod (or restartDfuse) starts it again
		d.metrics.probeFailures.inc("dead")
		h.setBad(now, "dfuse is gone")
	default:
		d.metrics.probeFailures.inc("error")
		h.setBad(now, "statfs: "+err.Error())
	}
}

// killDfuse kills the dfuse processes serving the volume's staging path. The node service sees
// them, as it runs in the host's pid namespace.
func (d *Driver) killDfuse(h *volHealth) {
	pids := dfusePids(h.staging)
	slog.Warn("killing unresponsive dfuse", "volume", h.volID, "pids", pids, "problem", h.problem)
	for _, pid := range pids {
		if err := kill(pid, syscall.SIGKILL); err == nil {
			d.metrics.dfuseKills.inc()
		} else {
			slog.Warn("killing dfuse", "pid", pid, "err", err)
		}
	}
}

// dfusePids returns the processes whose command is dfuse with mountPoint among its arguments.
func dfusePids(mountPoint string) []int {
	dirs, _ := filepath.Glob(filepath.Join(procDir, "[0-9]*/cmdline"))
	var pids []int
	for _, f := range dirs {
		data, err := os.ReadFile(f)
		if err != nil || len(data) == 0 {
			continue
		}
		args := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")
		if filepath.Base(args[0]) != "dfuse" {
			continue
		}
		for _, a := range args[1:] {
			if a == mountPoint {
				var pid int
				fmt.Sscanf(filepath.Base(filepath.Dir(f)), "%d", &pid)
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids
}

// restartDfuse brings back the dfuse of a volume in mount mode process, where no mount pod does.
func (d *Driver) restartDfuse(h *volHealth) {
	unlock := d.locks.lock(h.volID)
	defer unlock()
	if err := d.ensureMounted(h.staging, h.volID, h.args, podResources{}); err != nil {
		slog.Error("restarting dfuse", "volume", h.volID, "err", err)
	}
}

// rebindTargets mounts the staging path again on the pods' targets that are still mounts of an
// earlier dfuse of the volume.
func (d *Driver) rebindTargets(rec stageInfo, top map[string]mountEntry, targets []string) {
	staging, ok := top[rec.StagingPath]
	if !ok {
		return
	}
	for _, target := range targets {
		e, ok := top[target]
		if !ok || e.fsType != fuseType || e.dev == staging.dev {
			continue
		}
		func() {
			unlock := d.locks.lock(rec.VolumeID)
			defer unlock()
			// still the same dead mount, now that we hold the volume?
			ents, err := mounts(target)
			if err != nil || len(ents) == 0 || ents[len(ents)-1].dev != e.dev {
				return
			}
			if err := d.mnt.unmountAll(target); err != nil {
				slog.Warn("unmounting a pod's dead mount", "target", target, "err", err)
				return
			}
			if err := d.mnt.bind(rec.StagingPath, target, e.readOnly()); err != nil {
				slog.Warn("mounting a pod's target again", "target", target, "err", err)
				return
			}
			d.metrics.rebound.inc()
			slog.Info("pod's mount now served by the new dfuse; its containers get it when they "+
				"restart", "volume", rec.VolumeID, "target", target)
		}()
	}
}

// volumeHealth returns a copy of the volume's health record.
func (d *Driver) volumeHealth(volID string) (volHealth, bool) {
	d.health.mu.Lock()
	defer d.health.mu.Unlock()
	h, ok := d.health.vols[volID]
	if !ok {
		return volHealth{}, false
	}
	return *h, true
}

// healthGauges is daos_csi_volume_healthy: 1 or 0 per staged volume.
func (d *Driver) healthGauges() map[string]float64 {
	d.health.mu.Lock()
	defer d.health.mu.Unlock()
	res := map[string]float64{}
	for id, h := range d.health.vols {
		if h.badSince.IsZero() {
			res[id] = 1
		} else {
			res[id] = 0
		}
	}
	return res
}

// NodeGetVolumeStats reports a volume's space from its last successful health probe, without
// touching the mount: a stat of a hung dfuse would block kubelet's call. kubelet turns it into
// the kubelet_volume_stats_* metrics. (CSI 1.13 has no volume condition here any more: its
// successor, VolumeHealth, is alpha.) Inline agent-socket volumes get the space of the agent
// directory's filesystem.
func (d *Driver) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volID, path := req.GetVolumeId(), req.GetVolumePath()
	if volID == "" || path == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and path are required")
	}
	if h, ok := d.volumeHealth(volID); ok {
		// a volume staged a moment ago: its first probe may be about to answer
		for i := 0; i < 20 && !h.probed && !h.probeStart.IsZero(); i++ {
			time.Sleep(100 * time.Millisecond)
			h, _ = d.volumeHealth(volID)
		}
		if !h.probed {
			return nil, status.Errorf(codes.Unavailable, "volume %s: no successful probe yet", volID)
		}
		return &csi.NodeGetVolumeStatsResponse{Usage: []*csi.VolumeUsage{{
			Unit: csi.VolumeUsage_BYTES, Total: int64(h.total), Available: int64(h.avail),
			Used: int64(h.total - h.avail)}}}, nil
	}
	ents, err := mounts(path)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reading mountinfo: %v", err)
	}
	if len(ents) == 0 {
		return nil, status.Errorf(codes.NotFound, "%s is not mounted", path)
	}
	if ents[len(ents)-1].fsType == fuseType {
		return nil, status.Errorf(codes.Unavailable, "volume %s is not checked on this node", volID)
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return nil, status.Errorf(codes.Internal, "statfs %s: %v", path, err)
	}
	total, avail := st.Blocks*uint64(st.Bsize), st.Bavail*uint64(st.Bsize)
	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{{Unit: csi.VolumeUsage_BYTES, Total: int64(total),
			Available: int64(avail), Used: int64(total - avail)}},
	}, nil
}
