package main

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// finishReasonCounts returns bodylog_finish_reason_total keyed by its
// finish_reason label. A reason that was never incremented is simply absent --
// which is the state the "none" bucket was in before this change, and the reason
// silent truncations could not be alerted on.
func finishReasonCounts(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range fams {
		if f.GetName() != "bodylog_finish_reason_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "finish_reason" {
					out[l.GetValue()] += m.GetCounter().GetValue()
				}
			}
		}
	}
	return out
}

func TestEmptyFinishReasonOnlyCountsFor200(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	tl := &tailer{m: m}

	line := func(status int, finish string) string {
		return `{"ts":"2026-09-23T22:48:53+08:00","ts_end":"2026-09-23T22:48:53+08:00",` +
			`"request_id":"r","stream":true,"status":` + itoa(status) +
			`,"backend":"10.0.0.4:8050","model":"kimi-k3","finish_reason":"` + finish +
			`","frt":0.5,"lct":1.0,"rt":1.5,"completion_tokens":10}`
	}

	tl.handleLine([]byte(line(200, "stop") + "\n"))
	tl.handleLine([]byte(line(200, "") + "\n"))  // the truncation: engine died mid-stream
	tl.handleLine([]byte(line(503, "") + "\n"))  // an error: no finish_reason is normal, status already says so
	tl.handleLine([]byte(line(499, "") + "\n"))  // client went away: likewise not a truncation

	got := finishReasonCounts(t, reg)
	if got["stop"] != 1 {
		t.Fatalf("stop = %v, want 1", got["stop"])
	}
	if got["none"] != 1 {
		t.Fatalf("none = %v, want exactly 1 (only the 200 with no finish_reason)", got["none"])
	}
	if got[""] != 0 {
		t.Fatalf("empty label = %v, want 0 -- the empty case must be labelled, not blank", got[""])
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

var _ = dto.MetricFamily{}
