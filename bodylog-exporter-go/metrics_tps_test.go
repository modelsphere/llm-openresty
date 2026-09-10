package main

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

type gatheredHistogram struct {
	labels map[string]string
	count  uint64
	sum    float64
}

func gatherHistogram(t *testing.T, reg *prometheus.Registry, name string) []gatheredHistogram {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		out := make([]gatheredHistogram, 0, len(mf.GetMetric()))
		for _, metric := range mf.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			h := metric.GetHistogram()
			out = append(out, gatheredHistogram{labels: labels, count: h.GetSampleCount(), sum: h.GetSampleSum()})
		}
		return out
	}
	return nil
}

func assertTPSLabels(t *testing.T, got map[string]string) {
	t.Helper()
	want := map[string]bool{"service": true, "route": true, "backend": true, "model": true}
	if len(got) != len(want) {
		t.Fatalf("labels = %v, want exactly service/route/backend/model", got)
	}
	for name := range got {
		if !want[name] {
			t.Errorf("unexpected TPS label %q in %v", name, got)
		}
	}
}

func TestTPSMetricsExposureFormulaAndFilters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	// Valid for both metrics: overall=20/2=10, decode=20/(1.5-.5)=20.
	m.observe(detailRecord{Status: 200, Backend: "10.0.0.1:8050", Model: "model-a", CompletionTokens: 20, Rt: 2, Lct: 1.5, Frt: .5})
	// Valid overall only: decode duration is exactly .3s and must be rejected.
	m.observe(detailRecord{Status: 200, Backend: "10.0.0.1:8050", Model: "model-a", CompletionTokens: 20, Rt: 2, Lct: .5, Frt: .2})
	// Valid decode only: rt is invalid, while lct-frt is valid.
	m.observe(detailRecord{Status: 200, Backend: "10.0.0.1:8050", Model: "model-a", CompletionTokens: 20, Rt: 0, Lct: 1, Frt: .5})
	// Non-2xx, zero completion, and negative decode duration must not be sampled.
	m.observe(detailRecord{Status: 500, Backend: "10.0.0.1:8050", Model: "model-a", CompletionTokens: 20, Rt: 2, Lct: 1.5, Frt: .5})
	m.observe(detailRecord{Status: 200, Backend: "10.0.0.1:8050", Model: "model-a", CompletionTokens: 0, Rt: 2, Lct: 1.5, Frt: .5})
	m.observe(detailRecord{Status: 200, Backend: "10.0.0.1:8050", Model: "model-a", CompletionTokens: 20, Rt: 0, Lct: .4, Frt: .5})

	overall := gatherHistogram(t, reg, "bodylog_overall_output_tok_per_second")
	if len(overall) != 1 {
		t.Fatalf("overall series = %d, want 1", len(overall))
	}
	assertTPSLabels(t, overall[0].labels)
	if overall[0].count != 2 || math.Abs(overall[0].sum-20) > 1e-9 {
		t.Errorf("overall count/sum = %d/%v, want 2/20", overall[0].count, overall[0].sum)
	}

	decode := gatherHistogram(t, reg, "bodylog_decode_output_tok_per_second")
	if len(decode) != 1 {
		t.Fatalf("decode series = %d, want 1", len(decode))
	}
	assertTPSLabels(t, decode[0].labels)
	if decode[0].count != 2 || math.Abs(decode[0].sum-60) > 1e-9 {
		t.Errorf("decode count/sum = %d/%v, want 2/60", decode[0].count, decode[0].sum)
	}
}

func TestTPSMetricsDoNotRepurposeLegacyMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	// New metrics accept any positive completion count and rt. The legacy metric
	// still keeps its historical completion>=16 and rt>=0.5 filter.
	m.observe(detailRecord{Status: 200, Backend: "10.0.0.2:8050", Model: "model-b", CompletionTokens: 1, Rt: 1, Lct: .8, Frt: .2})

	if got := gatherHistogram(t, reg, "bodylog_overall_output_tok_per_second"); len(got) != 1 || got[0].count != 1 {
		t.Fatalf("overall metric = %#v, want one sample", got)
	}
	if got := gatherHistogram(t, reg, "bodylog_decode_output_tok_per_second"); len(got) != 1 || got[0].count != 1 {
		t.Fatalf("decode metric = %#v, want one sample", got)
	}
	if got := gatherHistogram(t, reg, "bodylog_output_tok_per_second"); len(got) != 0 {
		t.Fatalf("legacy metric = %#v, want no sample for completion_tokens<16", got)
	}
}

func TestTPSStatusAndValueBoundaries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	base := func(status int64, completion int64, rt, lct, frt float64) {
		m.observe(detailRecord{
			Status: status, Backend: "10.0.0.3:8050", Model: "model-c",
			CompletionTokens: completion, Rt: rt, Lct: lct, Frt: frt,
		})
	}

	base(199, 10, 1, 1, 0)         // status other: reject both
	base(200, 10, 1, 1, 0)         // lower valid 2xx boundary
	base(299, 10, 1, 1, 0)         // upper valid 2xx boundary
	base(300, 10, 1, 1, 0)         // status other: reject both
	base(200, 10, -1, .5, .5)      // rt<0: reject overall, lct-frt<=0 rejects decode
	base(200, -1, 1, .5, .5)       // completion<0: reject both
	base(200, 10, 1, .5, .5)       // lct<=frt: overall only
	base(200, 10, 1, .5, .2)       // decode exactly .3s: overall only
	base(200, 10, 1, .5000001, .2) // decode .3000001s: both

	overall := gatherHistogram(t, reg, "bodylog_overall_output_tok_per_second")
	if len(overall) != 1 || overall[0].count != 5 || math.Abs(overall[0].sum-50) > 1e-9 {
		t.Fatalf("overall boundary result = %#v, want count=5 sum=50", overall)
	}
	decode := gatherHistogram(t, reg, "bodylog_decode_output_tok_per_second")
	wantDecodeSum := 20 + 10/.3000001
	if len(decode) != 1 || decode[0].count != 3 || math.Abs(decode[0].sum-wantDecodeSum) > 1e-9 {
		t.Fatalf("decode boundary result = %#v, want count=3 sum=%v", decode, wantDecodeSum)
	}
}

func TestDetailRecordJSONLineMapping(t *testing.T) {
	line := `{"ts":"2026-09-10T08:00:00Z","ts_end":"2026-09-10T08:00:02.500Z","request_id":"req-1","stream":true,"status":200,"backend":"10.0.0.4:8050","model":"model-json","finish_reason":"stop","frt":0.5,"lct":1.75,"rt":2.5,"req_bytes":123,"resp_bytes":456,"prompt_tokens":7,"completion_tokens":25,"cached_tokens":3,"total_tokens":35,"reasoning_tokens":2}`
	var decoded detailRecord
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Status != 200 || decoded.CompletionTokens != 25 || decoded.Frt != .5 || decoded.Lct != 1.75 || decoded.Rt != 2.5 {
		t.Fatalf("JSON mapping = %+v, lct/frt/rt or status/tokens incorrect", decoded)
	}

	// handleLine is the actual tail path: it calls json.Unmarshal then observe.
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	(&tailer{m: m}).handleLine([]byte(line + "\n"))
	overall := gatherHistogram(t, reg, "bodylog_overall_output_tok_per_second")
	decode := gatherHistogram(t, reg, "bodylog_decode_output_tok_per_second")
	if len(overall) != 1 || overall[0].count != 1 || math.Abs(overall[0].sum-10) > 1e-9 {
		t.Fatalf("JSON overall = %#v, want one sample at 10 tok/s", overall)
	}
	if len(decode) != 1 || decode[0].count != 1 || math.Abs(decode[0].sum-20) > 1e-9 {
		t.Fatalf("JSON decode = %#v, want one sample at 20 tok/s", decode)
	}
	assertTPSLabels(t, overall[0].labels)
}
