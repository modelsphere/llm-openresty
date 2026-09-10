package main

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// prompt_bucket 落档与 native-only(无经典桶)的回归测试。
// 背景见 metrics.go 里 promptBucketBounds 的注释:档位边界一旦上线就不能改。

func TestPromptBucket(t *testing.T) {
	cases := []struct {
		tok  int64
		want string
	}{
		{0, "unknown"}, {1, "0000k_0001k"}, {1023, "0000k_0001k"}, {1024, "0001k_0002k"},
		{6 * 1024, "0006k_0008k"}, {6143, "0004k_0006k"},
		{12 * 1024, "0012k_0016k"}, {16 * 1024, "0016k_0020k"},
		{50 * 1024, "0048k_0064k"},
		{256 * 1024, "0256k_0384k"}, {1024 * 1024, "1024k_inf"},
		{2000 * 1024, "1024k_inf"}, {-5, "unknown"},
	}
	for _, c := range cases {
		if got := promptBucket(c.tok); got != c.want {
			t.Errorf("promptBucket(%d) = %s, want %s", c.tok, got, c.want)
		}
	}
	if n := len(promptBucketLabels); n != 26 {
		t.Errorf("档数 = %d, want 26", n)
	}
	t.Logf("首档=%s 末档=%s 档数=%d", promptBucketLabels[0], promptBucketLabels[25], len(promptBucketLabels))
}

func TestNativeOnly(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	st := true
	m.observe(detailRecord{
		Stream: &st, Status: 200, Backend: "1.2.3.4:8050", Model: "kimi-k2.6",
		Frt: 1.5, Rt: 20, PromptTokens: 7000, CompletionTokens: 100,
	})
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		n := mf.GetName()
		if n != "bodylog_ttft_seconds" && n != "bodylog_output_tok_per_second" && n != "bodylog_rt_seconds" &&
			n != "bodylog_overall_output_tok_per_second" && n != "bodylog_decode_output_tok_per_second" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			h := mm.GetHistogram()
			lbls := map[string]string{}
			for _, l := range mm.GetLabel() {
				lbls[l.GetName()] = l.GetValue()
			}
			t.Logf(n+": 经典桶数=%d  native schema=%d  native正桶段数=%d  prompt_bucket=%q",
				len(h.GetBucket()), h.GetSchema(), len(h.GetPositiveDelta()), lbls["prompt_bucket"])
			if len(h.GetBucket()) != 0 {
				t.Errorf("仍在发经典桶: %d 个", len(h.GetBucket()))
			}
			if h.GetSchema() == 0 {
				t.Errorf("%s: native schema=0, want non-zero native schema", n)
			}
			// 新 TPS 指标只允许 service/route/backend/model,不带 prompt_bucket。
			if n == "bodylog_overall_output_tok_per_second" || n == "bodylog_decode_output_tok_per_second" {
				if _, ok := lbls["prompt_bucket"]; ok {
					t.Errorf("%s: 不应暴露 prompt_bucket", n)
				}
				continue
			}
			// rt_seconds 不带 prompt_bucket(只去经典桶),其余两个必须落对档
			if n != "bodylog_rt_seconds" && lbls["prompt_bucket"] != "0006k_0008k" {
				t.Errorf("%s: prompt_bucket=%q, want 0006k_0008k", n, lbls["prompt_bucket"])
			}
		}
	}
}
