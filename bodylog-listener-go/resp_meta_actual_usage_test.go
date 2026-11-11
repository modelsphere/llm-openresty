package main

import (
	"encoding/json"
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

// THE PER-CHOICE PLACEMENT. Measured on a recent sglang build (2026-11): in a stream the frame
// carrying finish_reason holds the real usage AND actual_usage INSIDE choices[0], and the top level
// carries them only on the trailing "choices":[] frame -- which a client that did not send
// stream_options.include_usage never receives. Reading the top level alone left those streams with
// no token counts recorded at all, and openresty's strip_actual_usage removes the per-choice copy
// from the client response, so the bodylog is the only place the value can survive.
func TestRespMetaKeepsPerChoiceUsage(t *testing.T) {
	usage := `{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,"prompt_tokens_details":{"cached_tokens":2048}}`
	actual := `{"prompt_tokens":4096,"completion_tokens":128,"total_tokens":4224,"prompt_tokens_details":{"cached_tokens":3072}}`
	// No trailing usage frame: exactly what a client without include_usage sees.
	body := `data: {"id":"c3","choices":[{"index":0,"delta":{"content":"hi"},"usage":null}]}` + "\n\n" +
		`data: {"id":"c3","choices":[{"index":0,"delta":{},"finish_reason":"length","usage":` + usage +
		`,"actual_usage":` + actual + `}]}` + "\n\n" +
		"data: [DONE]\n\n"
	meta := buildRespMeta(extractResp(body))
	if meta == nil {
		t.Fatal("no resp_meta")
	}
	b, _ := json.Marshal(meta)
	var got struct {
		Usage *struct {
			Total   int `json:"total_tokens"`
			Details struct {
				Cached int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
		Actual *struct {
			Details struct {
				Cached int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"actual_usage"`
		FinishReason string `json:"finish_reason"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("resp_meta not valid JSON: %v", err)
	}
	if got.Usage == nil {
		t.Fatalf("per-choice usage was not recorded: %s", b)
	}
	if got.Usage.Total != 4224 || got.Usage.Details.Cached != 2048 {
		t.Errorf("per-choice usage wrong: %s", b)
	}
	if got.Actual == nil {
		t.Fatalf("per-choice actual_usage was not recorded: %s", b)
	}
	if got.Actual.Details.Cached != 3072 {
		t.Errorf("per-choice actual_usage wrong: %s", b)
	}
	// The "usage":null placeholder on the content frame must not overwrite the real numbers.
	if got.FinishReason != "length" {
		t.Errorf("finish_reason lost: %s", b)
	}
}
