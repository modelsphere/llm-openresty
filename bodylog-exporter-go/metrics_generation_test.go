package main

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"testing"
)

func counterSum(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, family := range families {
		if family.GetName() == name {
			for _, metric := range family.GetMetric() {
				sum += metric.GetCounter().GetValue()
			}
		}
	}
	return sum
}
func TestGenerationRequestsSeparateDiscoveryWithoutChangingHTTPCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	m.observe(detailRecord{URI: "/v1/models", Method: "GET", Backend: "10.0.0.1:8080", Status: 200})
	families, _ := reg.Gather()
	found := false
	for _, f := range families {
		if f.GetName() == "bodylog_generation_requests_total" {
			found = true
		}
	}
	if !found || counterSum(t, reg, "bodylog_generation_requests_total") != 0 {
		t.Fatal("discovery must publish a scraped zero generation counter")
	}
	for _, raw := range []string{
		`{"method":"POST","uri":"/v1/chat/completions","status":200,"backend":"10.0.0.1:8080"}`,
		`{"method":"POST","uri":"/route/v1/completions?debug=true","status":400,"backend":"10.0.0.1:8080"}`,
		`{"method":"POST","uri":"/v1/messages","status":500,"backend":"10.0.0.1:8080"}`,
		`{"method":"GET","uri":"/v1/chat/completions","status":200,"backend":"10.0.0.1:8080"}`,
		`{"method":"POST","uri":"/tokenize","status":200,"backend":"10.0.0.1:8080"}`,
	} {
		var d detailRecord
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			t.Fatal(err)
		}
		m.observe(d)
	}
	if got := counterSum(t, reg, "bodylog_requests_total"); got != 6 {
		t.Fatalf("legacy HTTP count changed: %v", got)
	}
	if got := counterSum(t, reg, "bodylog_generation_requests_total"); got != 3 {
		t.Fatalf("generation count includes probes or drops failed/unknown-model generation: %v", got)
	}
}
func TestGenerationRequestPaths(t *testing.T) {
	for _, tc := range []struct {
		method, uri string
		want        bool
	}{
		{"POST", "/generate", true}, {"POST", "/model/v1/chat/completions", true},
		{"POST", "/v1/chat/completions/extra", false}, {"GET", "/v1/models", false},
		{"POST", "/%zz/v1/completions", false}, {"", "", false},
	} {
		if got := generationRequest(detailRecord{Method: tc.method, URI: tc.uri}); got != tc.want {
			t.Errorf("%s %s = %v", tc.method, tc.uri, got)
		}
	}
}

func TestGenerationMetricsHaveBaselineBeforeFirstShortBurst(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	m.observe(detailRecord{URI: "/v1/models", Method: "GET", Backend: "10.0.0.1:8080", Status: 200})
	want := []string{"bodylog_generation_requests_total", "bodylog_generation_prompt_tokens_total", "bodylog_generation_completion_tokens_total", "bodylog_generation_cached_tokens_total", "bodylog_generation_missing_usage_requests_total", "bodylog_generation_ttft_seconds", "bodylog_generation_overall_output_tok_per_second", "bodylog_generation_decode_output_tok_per_second"}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	baseline := map[string]bool{}
	for _, family := range families {
		for _, name := range want {
			if family.GetName() == name {
				baseline[name] = true
				for _, metric := range family.GetMetric() {
					if metric.GetCounter().GetValue() != 0 || metric.GetHistogram().GetSampleCount() != 0 {
						t.Fatal("probe generated token/latency sample", name)
					}
				}
			}
		}
	}
	for _, name := range want {
		if !baseline[name] {
			t.Fatal("first burst has no zero baseline", name)
		}
	}
	yes := true
	for _, model := range []string{"alias-a", "alias-b"} {
		m.observe(detailRecord{URI: "/v1/chat/completions", Method: "POST", Backend: "10.0.0.1:8080", Status: 200, Model: model, Stream: &yes, PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30, Frt: .2, Lct: 1, Rt: 1.1})
	}
	if got := counterSum(t, reg, "bodylog_generation_requests_total"); got != 2 {
		t.Fatal(got)
	}
	if got := counterSum(t, reg, "bodylog_generation_prompt_tokens_total"); got != 20 {
		t.Fatal(got)
	}
	if got := counterSum(t, reg, "bodylog_generation_completion_tokens_total"); got != 40 {
		t.Fatal(got)
	}
	for _, name := range []string{"bodylog_generation_ttft_seconds", "bodylog_generation_overall_output_tok_per_second", "bodylog_generation_decode_output_tok_per_second"} {
		hs := gatherHistogram(t, reg, name)
		if len(hs) != 1 || hs[0].count != 2 || len(hs[0].labels) != 3 {
			t.Fatalf("short burst/model aliases lost first samples: %s %+v", name, hs)
		}
	}
}

func TestGenerationMissingUsageIsExplicit(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	m.observe(detailRecord{URI: "/v1/chat/completions", Method: "POST", Backend: "10.0.0.1:8080", Status: 500})
	if counterSum(t, reg, "bodylog_generation_requests_total") != 1 || counterSum(t, reg, "bodylog_generation_missing_usage_requests_total") != 1 {
		t.Fatal("failed generation hidden as no traffic")
	}
	if counterSum(t, reg, "bodylog_generation_prompt_tokens_total") != 0 {
		t.Fatal("usage fabricated")
	}
	if hs := gatherHistogram(t, reg, "bodylog_generation_ttft_seconds"); len(hs) != 1 || hs[0].count != 0 {
		t.Fatal("failed request produced TTFT")
	}
}
