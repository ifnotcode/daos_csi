package driver

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// A minimal Prometheus registry (text exposition format 0.0.4), so that the driver keeps its
// small dependency set.

// Bucket bounds in seconds: CSI calls take milliseconds, a dfuse mount seconds, a stuck one
// minutes.
var durationBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120}

type metricKind int

const (
	counterKind metricKind = iota
	histogramKind
	gaugeFuncKind
	gaugeVecFuncKind
)

type metric struct {
	name, help string
	kind       metricKind
	labels     []string

	mu     sync.Mutex
	series map[string]*series        // by joined label values
	gauge  func() float64            // gaugeFuncKind: evaluated at scrape time
	gauges func() map[string]float64 // gaugeVecFuncKind: by the value of its one label
}

type series struct {
	values  []string
	count   float64 // counter value, or the number of observations
	sum     float64
	buckets []float64 // cumulative counts per durationBuckets bound
}

type registry struct {
	mu      sync.Mutex
	metrics []*metric
}

func (r *registry) add(m *metric) *metric {
	m.series = map[string]*series{}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
	return m
}

func (r *registry) counter(name, help string, labels ...string) *metric {
	return r.add(&metric{name: name, help: help, kind: counterKind, labels: labels})
}

func (r *registry) histogram(name, help string, labels ...string) *metric {
	return r.add(&metric{name: name, help: help, kind: histogramKind, labels: labels})
}

func (r *registry) gaugeFunc(name, help string, f func() float64) *metric {
	return r.add(&metric{name: name, help: help, kind: gaugeFuncKind, gauge: f})
}

func (r *registry) gaugeVecFunc(name, help, label string, f func() map[string]float64) *metric {
	return r.add(&metric{name: name, help: help, kind: gaugeVecFuncKind, labels: []string{label},
		gauges: f})
}

func (m *metric) get(values []string) *series {
	if len(values) != len(m.labels) {
		panic(fmt.Sprintf("metric %s: %d label values for %d labels", m.name, len(values), len(m.labels)))
	}
	key := strings.Join(values, "\x00")
	s, ok := m.series[key]
	if !ok {
		s = &series{values: append([]string(nil), values...)}
		if m.kind == histogramKind {
			s.buckets = make([]float64, len(durationBuckets))
		}
		m.series[key] = s
	}
	return s
}

func (m *metric) inc(values ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.get(values).count++
}

func (m *metric) observe(d time.Duration, values ...string) {
	v := d.Seconds()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.get(values)
	s.count++
	s.sum += v
	for i, b := range durationBuckets {
		if v <= b {
			s.buckets[i]++
		}
	}
}

func (r *registry) write(w io.Writer) error {
	r.mu.Lock()
	ms := append([]*metric(nil), r.metrics...)
	r.mu.Unlock()
	var b strings.Builder
	for _, m := range ms {
		typ := map[metricKind]string{counterKind: "counter", histogramKind: "histogram",
			gaugeFuncKind: "gauge", gaugeVecFuncKind: "gauge"}[m.kind]
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", m.name, m.help, m.name, typ)
		if m.kind == gaugeFuncKind {
			fmt.Fprintf(&b, "%s %s\n", m.name, formatFloat(m.gauge()))
			continue
		}
		if m.kind == gaugeVecFuncKind {
			vals := m.gauges()
			keys := make([]string, 0, len(vals))
			for k := range vals {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(&b, "%s%s %s\n", m.name, braces(labelPairs(m.labels, []string{k})),
					formatFloat(vals[k]))
			}
			continue
		}
		m.mu.Lock()
		keys := make([]string, 0, len(m.series))
		for k := range m.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := m.series[k]
			lbl := labelPairs(m.labels, s.values)
			if m.kind == counterKind {
				fmt.Fprintf(&b, "%s%s %s\n", m.name, braces(lbl), formatFloat(s.count))
				continue
			}
			for i, bound := range durationBuckets {
				fmt.Fprintf(&b, "%s_bucket%s %s\n", m.name,
					braces(append(lbl, `le="`+formatFloat(bound)+`"`)), formatFloat(s.buckets[i]))
			}
			fmt.Fprintf(&b, "%s_bucket%s %s\n", m.name, braces(append(lbl, `le="+Inf"`)),
				formatFloat(s.count))
			fmt.Fprintf(&b, "%s_sum%s %s\n", m.name, braces(lbl), formatFloat(s.sum))
			fmt.Fprintf(&b, "%s_count%s %s\n", m.name, braces(lbl), formatFloat(s.count))
		}
		m.mu.Unlock()
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func labelPairs(names, values []string) []string {
	res := make([]string, len(names))
	for i, n := range names {
		v := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(values[i])
		res[i] = n + `="` + v + `"`
	}
	return res
}

func braces(pairs []string) string {
	if len(pairs) == 0 {
		return ""
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return fmt.Sprintf("%g", v)
}

// The driver's metrics
type driverMetrics struct {
	reg        registry
	calls      *metric // CSI calls by method and gRPC code
	callTime   *metric
	dfuseMount *metric // dfuse mounts (staging) by result
	mountTime  *metric
	stale      *metric // stale FUSE mounts found and cleared, by where (staging, target)
	// health checks
	probeFailures *metric // by reason: timeout, error, dead
	dfuseKills    *metric
	rebound       *metric // pods' targets mounted again from a new dfuse
}

func newDriverMetrics(staged func() float64, healthy func() map[string]float64) *driverMetrics {
	m := &driverMetrics{}
	m.calls = m.reg.counter("daos_csi_operations_total", "CSI calls by method and result code.",
		"method", "code")
	m.callTime = m.reg.histogram("daos_csi_operation_duration_seconds",
		"Duration of CSI calls.", "method")
	m.dfuseMount = m.reg.counter("daos_csi_dfuse_mounts_total",
		"dfuse mounts of a staging path by result (ok, error).", "result")
	m.mountTime = m.reg.histogram("daos_csi_dfuse_mount_duration_seconds",
		"Time until a started dfuse serves its staging path.")
	m.stale = m.reg.counter("daos_csi_stale_mounts_total",
		"FUSE mounts whose dfuse had died, found and cleared, by where (staging, target).", "where")
	if staged != nil {
		// From mountinfo only: a stat of a hung dfuse mount would block the scrape.
		m.reg.gaugeFunc("daos_csi_staged_volumes",
			"dfuse mounts at this node's staging paths.", staged)
	}
	m.probeFailures = m.reg.counter("daos_csi_health_probe_failures_total",
		"Failed statfs probes of dfuse mounts, by reason (timeout, error, dead).", "reason")
	m.dfuseKills = m.reg.counter("daos_csi_dfuse_kills_total",
		"dfuse processes killed after being unresponsive for -health-kill-after.")
	m.rebound = m.reg.counter("daos_csi_targets_rebound_total",
		"Pods' mounts of a dead dfuse replaced by the volume's new dfuse.")
	if healthy != nil {
		m.reg.gaugeVecFunc("daos_csi_volume_healthy",
			"1 if the volume's dfuse answered its last health probe in time, else 0.", "volume",
			healthy)
	}
	return m
}
