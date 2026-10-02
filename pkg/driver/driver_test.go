package driver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeMounter keeps mounts in memory. Mount changes take a little time, so that concurrent
// calls overlap.
type fakeMounter struct {
	mu     sync.Mutex
	state_ map[string]mountState // by path; fsType is always fuseType
	binds  []string              // targets bind-mounted, in order
	unmnts []string              // paths unmounted, in order
}

func newFakeMounter(staged ...string) *fakeMounter {
	f := &fakeMounter{state_: map[string]mountState{}}
	for _, p := range staged {
		f.state_[p] = mounted
	}
	return f
}

func (f *fakeMounter) state(path string) (mountState, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.state_[path]
	if st == notMounted {
		return notMounted, "", nil
	}
	return st, fuseType, nil
}

func (f *fakeMounter) bind(_, dst string, _ bool) error {
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state_[dst] = mounted
	f.binds = append(f.binds, dst)
	return nil
}

func (f *fakeMounter) unmountAll(path string) error {
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state_[path] != notMounted {
		f.unmnts = append(f.unmnts, path)
	}
	delete(f.state_, path)
	return nil
}

func testDriver(t *testing.T, mnt mounter) *Driver {
	t.Helper()
	d := newDriver(Config{Node: true, NodeID: "node-1", MountMode: "process"})
	d.mnt = mnt
	return d
}

var mountCap = &csi.VolumeCapability{
	AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
}

// Pods sharing a volume on a node publish and unpublish it at the same time: every call must
// act on its own target. (A driver that deduplicated such calls by volume id alone left pods
// without their mount, and mounts behind after the pods were gone.)
func TestConcurrentPodsOnOneVolume(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "globalmount")
	mnt := newFakeMounter(staging)
	d := testDriver(t, mnt)
	const pods = 8
	targets := make([]string, pods)
	for i := range targets {
		targets[i] = filepath.Join(t.TempDir(), "mount")
	}

	var wg sync.WaitGroup
	for _, tp := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
				VolumeId: "tank/vol", StagingTargetPath: staging, TargetPath: tp,
				VolumeCapability: mountCap,
			})
			if err != nil {
				t.Errorf("publish %s: %v", tp, err)
			}
		}()
	}
	wg.Wait()
	if len(mnt.binds) != pods {
		t.Fatalf("%d publishes succeeded, %d targets bind-mounted", pods, len(mnt.binds))
	}

	for _, tp := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
				VolumeId: "tank/vol", TargetPath: tp,
			})
			if err != nil {
				t.Errorf("unpublish %s: %v", tp, err)
			}
		}()
	}
	wg.Wait()
	if len(mnt.unmnts) != pods {
		t.Fatalf("%d unpublishes succeeded, %d targets unmounted", pods, len(mnt.unmnts))
	}
}

// A pod's mount whose dfuse died is replaced on the next publish, and counted.
func TestPublishReplacesStaleTarget(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "globalmount")
	target := filepath.Join(t.TempDir(), "mount")
	mnt := newFakeMounter(staging)
	mnt.state_[target] = staleMount
	d := testDriver(t, mnt)

	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "tank/vol", StagingTargetPath: staging, TargetPath: target,
		VolumeCapability: mountCap,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(mnt.unmnts) != 1 || len(mnt.binds) != 1 {
		t.Fatalf("unmounted %v, bind-mounted %v", mnt.unmnts, mnt.binds)
	}
	var b strings.Builder
	_ = d.metrics.reg.write(&b)
	if !strings.Contains(b.String(), `daos_csi_stale_mounts_total{where="target"} 1`) {
		t.Errorf("stale target not counted:\n%s", b.String())
	}
}

func TestPublishValidation(t *testing.T) {
	d := testDriver(t, newFakeMounter())
	block := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{}}}
	for name, req := range map[string]*csi.NodePublishVolumeRequest{
		"no target": {VolumeId: "tank/vol", StagingTargetPath: "/s", VolumeCapability: mountCap},
		"block": {VolumeId: "tank/vol", StagingTargetPath: "/s", TargetPath: "/t",
			VolumeCapability: block},
	} {
		_, err := d.NodePublishVolume(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
}

func TestKeyLocksSerializePerKey(t *testing.T) {
	k := newKeyLocks()
	var mu sync.Mutex
	inside, maxInside := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := k.lock("tank/vol")
			defer unlock()
			mu.Lock()
			inside++
			maxInside = max(maxInside, inside)
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
		}()
	}
	wg.Wait()
	if maxInside != 1 {
		t.Fatalf("%d holders of one key at once", maxInside)
	}

	// another key is not blocked
	unlock := k.lock("a")
	done := make(chan struct{})
	go func() { k.lock("b")(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lock on another key blocked")
	}
	unlock()
}

func TestParseVolumeID(t *testing.T) {
	for id, ok := range map[string]bool{
		"tank/vol": true, "tank": false, "/vol": false, "tank/": false, "tank/a/b": false,
	} {
		_, _, err := parseVolumeID(id)
		if (err == nil) != ok {
			t.Errorf("%q: err %v", id, err)
		}
	}
}

func TestDfuseArgs(t *testing.T) {
	got := strings.Join(dfuseArgs("tank", "vol", "/stage", []string{"--allow-idmap", "--disable-caching"}), " ")
	want := "--multi-user --pool tank --container vol --allow-idmap --disable-caching /stage"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestParseMountinfo(t *testing.T) {
	const info = `22 1 253:0 / / rw,relatime shared:1 - xfs /dev/vda1 rw
95 22 0:52 / /var/lib/kubelet/plugins/kubernetes.io/csi/csi.daos.io/abc/globalmount rw,nosuid,nodev,relatime shared:60 - fuse.daos dfuse rw,user_id=0,group_id=0,default_permissions,allow_other
96 22 0:52 / /mnt/with\040space rw master:60 - fuse.daos dfuse rw
bad line
`
	ents, err := parseMountinfo(strings.NewReader(info))
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 3 {
		t.Fatalf("got %d entries: %+v", len(ents), ents)
	}
	if ents[1].fsType != fuseType || ents[1].source != "dfuse" {
		t.Errorf("entry 1: %+v", ents[1])
	}
	if ents[2].mountPoint != "/mnt/with space" {
		t.Errorf("unescaped mount point: %q", ents[2].mountPoint)
	}
}

func TestMountPodSpec(t *testing.T) {
	d := newDriver(Config{Node: true, NodeID: "node-1", MountMode: "pod", MountImage: "img",
		DfuseBin: "dfuse", AgentDir: "/var/run/daos_agent"})
	spec := d.mountPodSpec("daos-mount-x", "/stage", "tank/vol", []string{"--pool", "tank", "/stage"},
		podResources{})
	s := spec["spec"].(map[string]any)
	// interception libraries in any pod can import dfuse's handles only from the host pid ns
	if s["hostPID"] != true || s["hostNetwork"] != true {
		t.Errorf("hostPID %v, hostNetwork %v", s["hostPID"], s["hostNetwork"])
	}
	if s["priorityClassName"] != "system-node-critical" || s["nodeName"] != "node-1" {
		t.Errorf("priorityClassName %v, nodeName %v", s["priorityClassName"], s["nodeName"])
	}
	cmd := s["containers"].([]any)[0].(map[string]any)["command"].([]string)
	if !strings.Contains(strings.Join(cmd, " "), "dfuse --foreground --pool tank /stage") {
		t.Errorf("command %q", cmd)
	}
	if mountPodName("node-1", "tank/vol") == mountPodName("node-2", "tank/vol") {
		t.Error("mount pod names of two nodes collide")
	}
}

func TestMetricsExposition(t *testing.T) {
	m := newDriverMetrics(func() float64 { return 2 }, nil)
	m.calls.inc("NodePublishVolume", "OK")
	m.calls.inc("NodePublishVolume", "OK")
	m.callTime.observe(300*time.Millisecond, "NodePublishVolume")
	var b strings.Builder
	if err := m.reg.write(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"# TYPE daos_csi_operations_total counter",
		`daos_csi_operations_total{method="NodePublishVolume",code="OK"} 2`,
		`daos_csi_operation_duration_seconds_bucket{method="NodePublishVolume",le="0.1"} 0`,
		`daos_csi_operation_duration_seconds_bucket{method="NodePublishVolume",le="0.5"} 1`,
		`daos_csi_operation_duration_seconds_bucket{method="NodePublishVolume",le="+Inf"} 1`,
		`daos_csi_operation_duration_seconds_count{method="NodePublishVolume"} 1`,
		"daos_csi_staged_volumes 2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}

// The interceptor counts calls by method and gRPC code.
func TestInstrument(t *testing.T) {
	d := testDriver(t, newFakeMounter())
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodeUnpublishVolume"}
	fail := func(context.Context, any) (any, error) { return nil, status.Error(codes.Internal, "x") }
	_, _ = d.instrument(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "tank/vol"},
		info, fail)
	var b strings.Builder
	_ = d.metrics.reg.write(&b)
	if !strings.Contains(b.String(), `daos_csi_operations_total{method="NodeUnpublishVolume",code="Internal"} 1`) {
		t.Errorf("call not counted:\n%s", b.String())
	}
}

func TestPodResources(t *testing.T) {
	def := podResources{CPURequest: "100m", CPULimit: "2", MemoryLimit: "4Gi"}
	r := def.withVolume(map[string]string{ParamCPULimit: "500m", ParamMemoryRequest: "1Gi", "pool": "x"})
	want := podResources{CPURequest: "100m", CPULimit: "500m", MemoryRequest: "1Gi", MemoryLimit: "4Gi"}
	if r != want {
		t.Fatalf("got %+v, want %+v", r, want)
	}
	if err := r.validate(); err != nil {
		t.Fatal(err)
	}
	spec := r.spec()
	if spec["limits"].(map[string]any)["cpu"] != "500m" ||
		spec["requests"].(map[string]any)["memory"] != "1Gi" {
		t.Errorf("spec %v", spec)
	}
	if (podResources{}).spec() != nil {
		t.Error("empty resources should give no spec")
	}
	// a limit alone reserves nothing (Kubernetes would default the request to the limit)
	s := (podResources{CPULimit: "1", MemoryLimit: "1Gi"}).spec()
	if req, _ := s["requests"].(map[string]any); req["cpu"] != "0" || req["memory"] != "0" {
		t.Errorf("a limit alone must get request 0: %v", s)
	}

	for _, bad := range []podResources{
		{CPULimit: "two"},
		{MemoryLimit: "1GB"},
		{CPURequest: "-1"},
		{CPURequest: "2", CPULimit: "1500m"},
		{MemoryRequest: "2Gi", MemoryLimit: "1Gi"},
	} {
		if err := bad.validate(); err == nil {
			t.Errorf("%+v: no error", bad)
		}
	}
	for _, ok := range []podResources{
		{CPURequest: "1500m", CPULimit: "2"},
		{CPURequest: "0.5", CPULimit: "500m"},
		{MemoryRequest: "1024Mi", MemoryLimit: "1Gi"},
		{MemoryRequest: "1G", MemoryLimit: "1Gi"},
	} {
		if err := ok.validate(); err != nil {
			t.Errorf("%+v: %v", ok, err)
		}
	}
}

func TestMountPodSpecResources(t *testing.T) {
	d := newDriver(Config{Node: true, NodeID: "node-1", MountMode: "pod", MountImage: "img"})
	spec := d.mountPodSpec("daos-mount-x", "/stage", "tank/vol", nil,
		podResources{CPULimit: "2", MemoryRequest: "256Mi"})
	c := spec["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	res, ok := c["resources"].(map[string]any)
	if !ok || res["limits"].(map[string]any)["cpu"] != "2" ||
		res["requests"].(map[string]any)["memory"] != "256Mi" {
		t.Errorf("resources %v", c["resources"])
	}
	spec = d.mountPodSpec("daos-mount-x", "/stage", "tank/vol", nil, podResources{})
	c = spec["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	if _, ok := c["resources"]; ok {
		t.Error("resources set without settings")
	}
}

// A bad resource parameter fails CreateVolume before the daos CLI runs.
func TestCreateVolumeRejectsBadResources(t *testing.T) {
	d := newDriver(Config{Controller: true, DaosBin: "/nonexistent/daos"})
	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-1", VolumeCapabilities: []*csi.VolumeCapability{mountCap},
		Parameters: map[string]string{ParamPool: "tank", ParamCPURequest: "4", ParamCPULimit: "2"},
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), ParamCPURequest) {
		t.Fatalf("got %v", err)
	}
}

// A mount pod that kubelet rejected (here OutOfcpu) is deleted, so that the next attempt creates it
// again, and the error says why.
func TestFailedMountPodIsReplaced(t *testing.T) {
	var deleted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, `{"metadata":{"name":"p","annotations":{"daos-csi/staging":"/stage"}},
				"status":{"phase":"Failed","reason":"OutOfcpu",
				"message":"Pod was rejected: Node didn't have enough resource: cpu"}}`)
		case http.MethodDelete:
			deleted = true
			fmt.Fprint(w, `{}`)
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tok, []byte("x"), 0o600)

	d := newDriver(Config{Node: true, NodeID: "node-1", MountMode: "pod", MountImage: "img"})
	d.mnt = newFakeMounter()
	d.kube = &kubeClient{base: srv.URL, namespace: "daos-csi", tokenFile: tok, http: srv.Client()}
	err := d.ensureMountPod("/stage", "tank/vol", nil, podResources{CPURequest: "64"})
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "OutOfcpu") {
		t.Fatalf("got %v", err)
	}
	if !deleted {
		t.Error("failed mount pod not deleted")
	}
}
