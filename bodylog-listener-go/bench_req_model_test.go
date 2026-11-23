package main

import (
	"strings"
	"testing"
)

// Worst case for the ingest path: a large body with "model" after the messages, so the scan
// covers all of it. Compared against extractStream's regex, which already scans every body.
func benchBody(n int) map[string]any {
	msg := strings.Repeat(`lorem ipsum \"quoted\" {not json} [x] `, n/40)
	body := `{"messages":[{"role":"user","content":"` + msg + `"}],"stream":true,"model":"kimi-k3"}`
	return map[string]any{"req_body": body}
}

func BenchmarkExtractReqModel1MB(b *testing.B) {
	m := benchBody(1 << 20)
	b.SetBytes(int64(len(m["req_body"].(string))))
	for i := 0; i < b.N; i++ {
		if extractReqModel(m) != "kimi-k3" {
			b.Fatal("wrong model")
		}
	}
}

func BenchmarkExtractStream1MB(b *testing.B) {
	m := benchBody(1 << 20)
	b.SetBytes(int64(len(m["req_body"].(string))))
	for i := 0; i < b.N; i++ {
		extractStream(m)
	}
}
