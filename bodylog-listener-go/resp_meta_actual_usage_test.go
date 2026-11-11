package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// Some sglang builds return a top-level actual_usage next to usage. openresty can strip it
// from the client response (postprocess strip_actual_usage), so the bodylog is the only place
// it survives: resp_meta must carry it, as it carries usage.
func TestRespMetaKeepsActualUsage(t *testing.T) {
	usage := `{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,"prompt_tokens_details":{"cached_tokens":2048,"cache_write_tokens":1792}}`
	actual := `{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,"prompt_tokens_details":{"cached_tokens":3072}}`
	cases := map[string]string{
		"stream": `data: {"id":"c1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n" +
			`data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
			`data: {"id":"c1","choices":[],"usage":` + usage + `,"actual_usage":` + actual + `}` + "\n\n" +
			"data: [DONE]\n\n",
		"json": `{"id":"c2","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":` +
			usage + `,"actual_usage":` + actual + `}`,
	}
	for name, body := range cases {
		meta := buildRespMeta(extractResp(body))
		if meta == nil {
			t.Fatalf("%s: no resp_meta", name)
		}
		b, _ := json.Marshal(meta)
		var got struct {
			Usage struct {
				Details struct {
					Cached int `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"usage"`
			Actual *struct {
				Prompt  int `json:"prompt_tokens"`
				Details struct {
					Cached int `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"actual_usage"`
		}
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Usage.Details.Cached != 2048 {
			t.Errorf("%s: usage.cached_tokens = %d, want 2048 (%s)", name, got.Usage.Details.Cached, b)
		}
		if got.Actual == nil {
			t.Fatalf("%s: resp_meta has no actual_usage: %s", name, b)
		}
		if got.Actual.Details.Cached != 3072 || got.Actual.Prompt != 4096 {
			t.Errorf("%s: actual_usage = %+v, want cached 3072 / prompt 4096", name, *got.Actual)
		}
	}

	// absent upstream -> absent in resp_meta (no empty key)
	meta := buildRespMeta(extractResp(`{"choices":[{"message":{"content":"x"},"finish_reason":"stop"}],"usage":` + usage + `}`))
	if _, ok := meta["actual_usage"]; ok {
		t.Errorf("actual_usage present though upstream sent none: %v", meta)
	}
}

// The details line (what bodylog-exporter reads) carries actual_usage's cached_tokens as
// actual_cached_tokens next to cached_tokens -- only when the response reported one.
func TestDetailActualCachedTokens(t *testing.T) {
	usage := `{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,"prompt_tokens_details":{"cached_tokens":2048}}`
	cases := []struct {
		name, extra string
		want        *int64
	}{
		{"reported", `,"actual_usage":{"prompt_tokens":4096,"prompt_tokens_details":{"cached_tokens":3072}}`, ptr(3072)},
		{"reported zero", `,"actual_usage":{"prompt_tokens_details":{"cached_tokens":0}}`, ptr(0)},
		{"null", `,"actual_usage":{"prompt_tokens_details":{"cached_tokens":null}}`, nil},
		{"no details", `,"actual_usage":{"prompt_tokens":4096}`, nil},
		{"absent", ``, nil},
	}
	for _, c := range cases {
		body := `{"choices":[{"message":{"content":"hi"},"finish_reason":"stop"}],"usage":` + usage + c.extra + `}`
		m := map[string]any{"status": float64(200), "resp_meta": buildRespMeta(extractResp(body))}
		d := extractDetail(m)
		if d.CachedTokens != 2048 {
			t.Errorf("%s: cached_tokens = %d, want 2048 (usage is unaffected)", c.name, d.CachedTokens)
		}
		switch {
		case c.want == nil && d.ActualCachedTokens != nil:
			t.Errorf("%s: actual_cached_tokens = %d, want absent", c.name, *d.ActualCachedTokens)
		case c.want != nil && (d.ActualCachedTokens == nil || *d.ActualCachedTokens != *c.want):
			t.Errorf("%s: actual_cached_tokens = %v, want %d", c.name, d.ActualCachedTokens, *c.want)
		}
		line, _ := json.Marshal(d)
		has := strings.Contains(string(line), `"actual_cached_tokens"`)
		if has != (c.want != nil) {
			t.Errorf("%s: details line has actual_cached_tokens=%v, want %v: %s", c.name, has, c.want != nil, line)
		}
	}
}

func ptr(v int64) *int64 { return &v }
