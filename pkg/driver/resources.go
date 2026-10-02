package driver

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Resources of the dfuse container in mount pods, as Kubernetes quantities ("500m", "2", "1Gi").
// The node plugin's flags give the defaults, and a volume can override each of them with the
// StorageClass parameters or PV volumeAttributes below. They are applied when a mount pod is
// created, i.e. when a volume is staged on a node; a running mount pod keeps its resources.
const (
	ParamCPURequest    = "dfuseCpuRequest"
	ParamCPULimit      = "dfuseCpuLimit"
	ParamMemoryRequest = "dfuseMemoryRequest"
	ParamMemoryLimit   = "dfuseMemoryLimit"
)

type podResources struct {
	CPURequest, CPULimit, MemoryRequest, MemoryLimit string
}

var resourceParams = []string{ParamCPURequest, ParamCPULimit, ParamMemoryRequest, ParamMemoryLimit}

func (r *podResources) field(param string) *string {
	switch param {
	case ParamCPURequest:
		return &r.CPURequest
	case ParamCPULimit:
		return &r.CPULimit
	case ParamMemoryRequest:
		return &r.MemoryRequest
	default:
		return &r.MemoryLimit
	}
}

// withVolume returns r with the volume's own settings applied over it.
func (r podResources) withVolume(volCtx map[string]string) podResources {
	for _, p := range resourceParams {
		if v := volCtx[p]; v != "" {
			*r.field(p) = v
		}
	}
	return r
}

// A plain decimal number with an optional SI or binary suffix. Kubernetes also accepts exponent
// forms (1e3), which nobody writes for CPU or memory.
var quantityRE = regexp.MustCompile(`^([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(m|k|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)?$`)

var quantitySuffix = map[string]float64{
	"": 1, "m": 1e-3, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18,
	"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40, "Pi": 1 << 50, "Ei": 1 << 60,
}

func parseQuantity(s string) (float64, error) {
	m := quantityRE.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("%q is not a quantity such as 500m, 2, 512Mi or 1Gi", s)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, err
	}
	return v * quantitySuffix[m[2]], nil
}

// validate checks the syntax and that no request exceeds its limit, so that a bad setting fails
// with a clear message when the volume is provisioned or staged rather than when the API server
// rejects the mount pod.
func (r podResources) validate() error {
	vals := map[string]float64{}
	for _, p := range resourceParams {
		s := *r.field(p)
		if s == "" {
			continue
		}
		v, err := parseQuantity(s)
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		vals[p] = v
	}
	for req, lim := range map[string]string{ParamCPURequest: ParamCPULimit,
		ParamMemoryRequest: ParamMemoryLimit} {
		rv, okR := vals[req]
		lv, okL := vals[lim]
		if okR && okL && rv > lv {
			return fmt.Errorf("%s %s is above %s %s", req, *r.field(req), lim, *r.field(lim))
		}
	}
	return nil
}

// spec is the container's "resources", or nil when nothing is set.
//
// A limit without a request gets a request of 0. Kubernetes would default the request to the
// limit, and a mount pod's request is taken from the node behind the scheduler's back: kubelet
// admits the pod with node name set and, as it is system-node-critical, evicts lower-priority pods
// to make room. A limit of 2 CPUs on 50 volumes would reserve 100. Set a request to reserve.
func (r podResources) spec() map[string]any {
	requests, limits := map[string]any{}, map[string]any{}
	set := func(m map[string]any, key, v string) {
		if v != "" {
			m[key] = v
		}
	}
	set(requests, "cpu", r.CPURequest)
	set(requests, "memory", r.MemoryRequest)
	if r.CPURequest == "" && r.CPULimit != "" {
		requests["cpu"] = "0"
	}
	if r.MemoryRequest == "" && r.MemoryLimit != "" {
		requests["memory"] = "0"
	}
	set(limits, "cpu", r.CPULimit)
	set(limits, "memory", r.MemoryLimit)
	res := map[string]any{}
	if len(requests) > 0 {
		res["requests"] = requests
	}
	if len(limits) > 0 {
		res["limits"] = limits
	}
	if len(res) == 0 {
		return nil
	}
	return res
}

// threadCount is the --thread-count to give dfuse under a CPU limit, or 0 to leave it alone: when
// there is no limit, or dfuseArgs sets it already. dfuse sizes its thread pool from the CPUs it
// may run on (up to 16, or its cpuset), not from a CFS quota, and more busy threads than the quota
// allows only get throttled. It gets one thread per CPU of the limit, rounded up, plus one per
// event queue (--eq-count, 1 by default), which dfuse takes out of the total.
func (r podResources) threadCount(dfuseArgs []string) int {
	if r.CPULimit == "" {
		return 0
	}
	cpus, err := parseQuantity(r.CPULimit)
	if err != nil || cpus <= 0 {
		return 0
	}
	eqs := 1
	for i, a := range dfuseArgs {
		switch {
		case a == "--thread-count" || a == "-t" || strings.HasPrefix(a, "--thread-count=") ||
			(strings.HasPrefix(a, "-t") && !strings.HasPrefix(a, "--")):
			return 0
		case strings.HasPrefix(a, "--eq-count="):
			eqs = atoiOr(strings.TrimPrefix(a, "--eq-count="), eqs)
		case (a == "--eq-count" || a == "-e") && i+1 < len(dfuseArgs):
			eqs = atoiOr(dfuseArgs[i+1], eqs)
		case strings.HasPrefix(a, "-e") && !strings.HasPrefix(a, "--") && len(a) > 2:
			eqs = atoiOr(a[2:], eqs)
		}
	}
	return int(math.Ceil(cpus)) + eqs
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}
	return def
}
