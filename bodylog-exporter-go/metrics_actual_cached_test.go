package main

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// counterTotal sums every series of one counter family.
func counterTotal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, f := range fams {
		if f.GetName() == name {
			for _, m := range f.GetMetric() {
				sum += m.GetCounter().GetValue()
			}
		}
	}
	return sum
}

// Some sglang builds report a second token count, actual_usage, next to usage; the listener
// writes its cached_tokens to the details line as actual_cached_tokens. It gets its own counter:
// bodylog_cached_tokens_total keeps counting usage's figure (what clients and billing see).
func TestActualCachedTokensCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	tl := &tailer{m: newMetrics(reg)}
	line := func(extra string) string {
		return `{"ts":"2026-11-10T16:00:00+08:00","ts_end":"2026-11-10T16:00:01+08:00","request_id":"r",` +
			`"stream":true,"status":200,"backend":"10.0.0.4:8050","model":"kimi-k3","finish_reason":"stop",` +
			`"frt":0.5,"lct":1.0,"rt":1.5,"prompt_tokens":4096,"completion_tokens":10,"cached_tokens":2048` + extra + `}` + "\n"
	}
	tl.handleLine([]byte(line(`,"actual_cached_tokens":3072`)))
	tl.handleLine([]byte(line(`,"actual_cached_tokens":0`))) // reported, nothing cached
	tl.handleLine([]byte(line(``)))                          // a backend without actual_usage

	if got := counterTotal(t, reg, "bodylog_cached_tokens_total"); got != 3*2048 {
		t.Errorf("bodylog_cached_tokens_total = %v, want %v (usage's figure, every request)", got, 3*2048)
	}
	if got := counterTotal(t, reg, "bodylog_actual_cached_tokens_total"); got != 3072 {
		t.Errorf("bodylog_actual_cached_tokens_total = %v, want 3072 (only what actual_usage reported)", got)
	}
}
