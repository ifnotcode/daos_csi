package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// fakeNode lays out what the health checks read: kubelet's staging area with a stage record,
// pods' vol_data.json, mountinfo, and /proc with a dfuse process. statfs and kill are replaced.
type fakeNode struct {
	t       *testing.T
	root    string
	staging string
	volID   string
	targets []string

	mu      sync.Mutex
	block   chan struct{} // statfs blocks until closed, when not nil
	statErr error
	killed  []int
}

const testDfusePid = 4242

func newFakeNode(t *testing.T, pods int) *fakeNode {
	t.Helper()
	root := t.TempDir()
	n := &fakeNode{t: t, root: root, volID: "tank/pvc-1"}
	oldKubelet, oldMountinfo, oldProc, oldStatfs, oldKill := kubeletDir, mountinfoFile, procDir, statfs, kill
	t.Cleanup(func() {
		kubeletDir, mountinfoFile, procDir, statfs, kill = oldKubelet, oldMountinfo, oldProc, oldStatfs, oldKill
	})
	kubeletDir = filepath.Join(root, "kubelet")
	mountinfoFile = filepath.Join(root, "mountinfo")
	procDir = filepath.Join(root, "proc")

	n.staging = filepath.Join(stagingDir(), "abc", "globalmount")
	must(t, os.MkdirAll(n.staging, 0o755))
	rec, _ := json.Marshal(stageInfo{VolumeID: n.volID, StagingPath: n.staging,
		DfuseArgs: []string{"--pool", "tank", n.staging}})
	must(t, os.WriteFile(filepath.Join(filepath.Dir(n.staging), stageRecord), rec, 0o600))
	for i := 0; i < pods; i++ {
		dir := filepath.Join(kubeletDir, "pods", fmt.Sprintf("uid-%d", i), "volumes/kubernetes.io~csi/pvc-1")
		must(t, os.MkdirAll(filepath.Join(dir, "mount"), 0o755))
		must(t, os.WriteFile(filepath.Join(dir, "vol_data.json"),
			[]byte(`{"driverName":"csi.daos.io","volumeHandle":"`+n.volID+`"}`), 0o600))
		n.targets = append(n.targets, filepath.Join(dir, "mount"))
	}
	// an inline agent-socket volume of another driver is ignored
	other := filepath.Join(kubeletDir, "pods/uid-x/volumes/kubernetes.io~csi/agent")
	must(t, os.MkdirAll(other, 0o755))
	must(t, os.WriteFile(filepath.Join(other, "vol_data.json"),
		[]byte(`{"driverName":"other.csi","volumeHandle":"x"}`), 0o600))

	pidDir := filepath.Join(procDir, fmt.Sprint(testDfusePid))
	must(t, os.MkdirAll(pidDir, 0o755))
	must(t, os.WriteFile(filepath.Join(pidDir, "cmdline"),
		[]byte("/usr/bin/dfuse\x00--foreground\x00--pool\x00tank\x00"+n.staging+"\x00"), 0o600))
	other = filepath.Join(procDir, "7")
	must(t, os.MkdirAll(other, 0o755))
	must(t, os.WriteFile(filepath.Join(other, "cmdline"), []byte("sleep\x00"+n.staging+"\x00"), 0o600))

	statfs = func(path string, st *syscall.Statfs_t) error {
		n.mu.Lock()
		block, err := n.block, n.statErr
		n.mu.Unlock()
		if block != nil {
			<-block
		}
		st.Bsize, st.Blocks, st.Bavail = 1, 1000, 400
		return err
	}
	kill = func(pid int, _ syscall.Signal) error {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.killed = append(n.killed, pid)
		return nil
	}
	return n
}

// mountinfo writes the staging mount (device stagingDev) and the targets' mounts (their devices).
func (n *fakeNode) mountinfo(stagingDev string, targetDevs ...string) {
	lines := []string{"22 1 253:0 / / rw,relatime shared:1 - xfs /dev/vda1 rw"}
	if stagingDev != "" {
		lines = append(lines, fmt.Sprintf("95 22 %s / %s rw,nosuid,nodev,relatime shared:60 - fuse.daos dfuse rw",
			stagingDev, n.staging))
	}
	for i, dev := range targetDevs {
		lines = append(lines, fmt.Sprintf("%d 22 %s / %s ro,relatime shared:61 - fuse.daos dfuse rw",
			100+i, dev, n.targets[i]))
	}
	must(n.t, os.WriteFile(mountinfoFile, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func healthDriver(mnt mounter) *Driver {
	d := newDriver(Config{Node: true, NodeID: "node-1", MountMode: "pod",
		Health: healthConfig{Interval: time.Minute, Timeout: 30 * time.Second, KillAfter: 5 * time.Minute}})
	d.mnt = mnt
	return d
}

// waitProbe waits for the volume's probe in flight to finish.
func waitProbe(t *testing.T, d *Driver, volID string) volHealth {
	t.Helper()
	for i := 0; i < 200; i++ {
		if h, ok := d.volumeHealth(volID); ok && h.probeStart.IsZero() {
			return h
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("probe did not finish")
	return volHealth{}
}

func TestHealthyVolume(t *testing.T) {
	n := newFakeNode(t, 1)
	n.mountinfo("0:60", "0:60")
	mnt := newFakeMounter()
	d := healthDriver(mnt)
	d.checkHealth(time.Now())
	h := waitProbe(t, d, n.volID)
	if !h.badSince.IsZero() || !h.probed {
		t.Fatalf("healthy volume: %+v", h)
	}
	resp, err := d.NodeGetVolumeStats(context.Background(),
		&csi.NodeGetVolumeStatsRequest{VolumeId: n.volID, VolumePath: n.targets[0]})
	must(t, err)
	if resp.Usage[0].Total != 1000 || resp.Usage[0].Used != 600 {
		t.Errorf("stats %v", resp)
	}
	if len(mnt.binds) != 0 || len(n.killed) != 0 {
		t.Errorf("binds %v, kills %v", mnt.binds, n.killed)
	}
}

// A dfuse that stops answering: unhealthy after the timeout, and killed after kill-after.
func TestHungDfuseIsKilled(t *testing.T) {
	n := newFakeNode(t, 1)
	n.mountinfo("0:60", "0:60")
	n.block = make(chan struct{})
	defer close(n.block)
	d := healthDriver(newFakeMounter())
	t0 := time.Now()

	d.checkHealth(t0) // starts the probe, which hangs
	d.checkHealth(t0.Add(10 * time.Second))
	if h, _ := d.volumeHealth(n.volID); !h.badSince.IsZero() {
		t.Fatal("unhealthy before the timeout")
	}
	d.checkHealth(t0.Add(31 * time.Second))
	h, _ := d.volumeHealth(n.volID)
	if h.badSince.IsZero() || !strings.Contains(h.problem, "not answered") {
		t.Fatalf("not unhealthy after the timeout: %+v", h)
	}
	// never answered: no space to report, and the call must not block on the mount
	if _, err := d.NodeGetVolumeStats(context.Background(),
		&csi.NodeGetVolumeStatsRequest{VolumeId: n.volID, VolumePath: n.targets[0]}); err == nil {
		t.Error("stats of a never-probed volume")
	}
	var b strings.Builder
	_ = d.metrics.reg.write(&b)
	for _, want := range []string{`daos_csi_volume_healthy{volume="tank/pvc-1"} 0`,
		`daos_csi_health_probe_failures_total{reason="timeout"} 1`} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %q", want)
		}
	}

	d.checkHealth(t0.Add(4 * time.Minute))
	if len(n.killed) != 0 {
		t.Fatal("killed before kill-after")
	}
	d.checkHealth(t0.Add(6 * time.Minute))
	if len(n.killed) != 1 || n.killed[0] != testDfusePid {
		t.Fatalf("killed %v, want [%d] (only dfuse with this mount point)", n.killed, testDfusePid)
	}
	// not again right away: the restarted dfuse gets kill-after to come up
	d.checkHealth(t0.Add(7 * time.Minute))
	if len(n.killed) != 1 {
		t.Fatalf("killed again: %v", n.killed)
	}
}

func TestNoKillWhenDisabled(t *testing.T) {
	n := newFakeNode(t, 0)
	n.mountinfo("0:60")
	n.block = make(chan struct{})
	defer close(n.block)
	d := healthDriver(newFakeMounter())
	d.cfg.Health.KillAfter = 0
	t0 := time.Now()
	for _, s := range []time.Duration{0, time.Minute, time.Hour} {
		d.checkHealth(t0.Add(s))
	}
	if len(n.killed) != 0 {
		t.Fatalf("killed %v with kill-after 0", n.killed)
	}
}

// After dfuse was replaced (the staging path has a new device), the pods' targets that are still
// mounts of the old one are bind-mounted again, keeping read-only ones read-only.
func TestDeadTargetsRebound(t *testing.T) {
	n := newFakeNode(t, 2)
	n.mountinfo("0:61", "0:52", "0:61") // pod 0 on the old dfuse, pod 1 already on the new one
	mnt := newFakeMounter()
	d := healthDriver(mnt)
	d.checkHealth(time.Now())
	waitProbe(t, d, n.volID)
	if len(mnt.binds) != 1 || mnt.binds[0] != n.targets[0] {
		t.Fatalf("bind-mounted %v, want only %s", mnt.binds, n.targets[0])
	}
	var b strings.Builder
	_ = d.metrics.reg.write(&b)
	if !strings.Contains(b.String(), "daos_csi_targets_rebound_total 1") {
		t.Errorf("rebind not counted:\n%s", b.String())
	}
}

// A dfuse that is gone (ENOTCONN) is not killed: its mount pod restarts it.
func TestDeadDfuseNotKilled(t *testing.T) {
	n := newFakeNode(t, 0)
	n.mountinfo("0:60")
	n.statErr = syscall.ENOTCONN
	d := healthDriver(newFakeMounter())
	t0 := time.Now()
	d.checkHealth(t0)
	waitProbe(t, d, n.volID)
	d.checkHealth(t0.Add(time.Hour))
	if h, _ := d.volumeHealth(n.volID); h.problem != "dfuse is gone" {
		t.Errorf("problem %q", h.problem)
	}
	if len(n.killed) != 0 {
		t.Errorf("killed %v", n.killed)
	}
}

func TestUnstagedVolumeForgotten(t *testing.T) {
	n := newFakeNode(t, 0)
	n.mountinfo("0:60")
	d := healthDriver(newFakeMounter())
	d.checkHealth(time.Now())
	waitProbe(t, d, n.volID)
	must(t, os.Remove(filepath.Join(filepath.Dir(n.staging), stageRecord)))
	d.checkHealth(time.Now())
	if _, ok := d.volumeHealth(n.volID); ok {
		t.Error("health record kept after unstage")
	}
}

func TestThreadCount(t *testing.T) {
	for _, c := range []struct {
		limit string
		args  string
		want  int
	}{
		{"", "", 0},
		{"200m", "", 2},
		{"1", "", 2},
		{"1500m", "", 3},
		{"4", "--eq-count=2", 6},
		{"4", "-e 3", 7},
		{"4", "-e2", 6},
		{"4", "--thread-count=8", 0},
		{"4", "-t 8", 0},
		{"4", "-t8", 0},
	} {
		got := podResources{CPULimit: c.limit}.threadCount(strings.Fields(c.args))
		if got != c.want {
			t.Errorf("limit %q args %q: %d, want %d", c.limit, c.args, got, c.want)
		}
	}
}

func TestMountSpecThreads(t *testing.T) {
	d := newDriver(Config{Node: true, MountMode: "pod", MountPodResources: podResources{CPULimit: "2"}})
	args, res, err := d.mountSpec("tank", "vol", "/stage", map[string]string{ParamDfuseArgs: "--allow-idmap"})
	must(t, err)
	if got := strings.Join(args, " "); got !=
		"--multi-user --pool tank --container vol --allow-idmap --thread-count=3 /stage" {
		t.Errorf("args %q", got)
	}
	if res.CPULimit != "2" {
		t.Errorf("res %+v", res)
	}
	// process mode: no mount pod, no resources, dfuse sizes itself
	d = newDriver(Config{Node: true, MountMode: "process", MountPodResources: podResources{CPULimit: "2"}})
	args, _, _ = d.mountSpec("tank", "vol", "/stage", nil)
	if strings.Contains(strings.Join(args, " "), "thread-count") {
		t.Errorf("process mode args %q", args)
	}
}

func TestNodeInfoAndCapabilities(t *testing.T) {
	d := newDriver(Config{Node: true, NodeID: "n", MaxVolumesPerNode: 20,
		Health: healthConfig{Interval: time.Minute}})
	info, _ := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if info.MaxVolumesPerNode != 20 {
		t.Errorf("max volumes %d", info.MaxVolumesPerNode)
	}
	caps, _ := d.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	var names []string
	for _, c := range caps.Capabilities {
		names = append(names, c.GetRpc().GetType().String())
	}
	if got := strings.Join(names, " "); got != "STAGE_UNSTAGE_VOLUME GET_VOLUME_STATS" {
		t.Errorf("capabilities %s", got)
	}
	d = newDriver(Config{Node: true, NodeID: "n"})
	caps, _ = d.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if len(caps.Capabilities) != 1 {
		t.Errorf("without health checks: %v", caps.Capabilities)
	}
}

// After a long outage, the dfuse that the mount pod starts once the servers are back is not
// killed at once: it gets the whole kill-after from when it appeared, and the late answer of a
// probe of the old dfuse does not count for it.
func TestNewDfuseGetsFullKillWindow(t *testing.T) {
	n := newFakeNode(t, 0)
	n.mountinfo("0:60")
	n.block = make(chan struct{})
	d := healthDriver(newFakeMounter())
	t0 := time.Now()
	d.checkHealth(t0)                      // probe of dfuse 0:60 hangs
	d.checkHealth(t0.Add(time.Minute))     // unhealthy
	d.checkHealth(t0.Add(7 * time.Minute)) // killed
	if len(n.killed) != 1 {
		t.Fatalf("killed %v", n.killed)
	}
	// the mount pod's dfuse cannot mount while the servers are unreachable
	n.mountinfo("")
	d.checkHealth(t0.Add(20 * time.Minute))
	// the servers are back: a new dfuse mounts, its first probe is still connecting
	n.mountinfo("0:61")
	d.checkHealth(t0.Add(30 * time.Minute))
	d.checkHealth(t0.Add(30*time.Minute + 10*time.Second))
	if len(n.killed) != 1 {
		t.Fatalf("new dfuse killed right away: %v", n.killed)
	}
	// its probe answers; the old one's (released too) is ignored
	n.mu.Lock()
	n.statErr = nil
	n.mu.Unlock()
	close(n.block)
	h := waitProbe(t, d, n.volID)
	if !h.badSince.IsZero() {
		t.Fatalf("new dfuse not healthy: %+v", h)
	}
	d.checkHealth(t0.Add(40 * time.Minute))
	if len(n.killed) != 1 {
		t.Fatalf("killed again: %v", n.killed)
	}
}
