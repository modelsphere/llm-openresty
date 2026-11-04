//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const apiTimeout = 60 * time.Second

// The kind counterpart of test/test_path_routing_e2e.sh: the real image, the
// real standalone manifest shape, one route ("mock") whose only peer is
// modelsphere/mock-llmd, everything reached through the 8080 path dispatch.
var _ = Describe("openresty router", Ordered, Label("router"), func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpClusterState(CurrentSpecReport().FullText())
		}
	})

	It("answers /healthz on the admin port", func() {
		status, body := do(http.MethodGet, adminURL("/healthz"), nil)
		Expect(status).To(Equal(http.StatusOK))
		Expect(strings.TrimSpace(body)).To(Equal("ok"))
	})

	It("dispatches /mock/v1/chat/completions to the mock peer", func() {
		status, body := do(http.MethodPost, dispatchURL("/mock/v1/chat/completions"), chatBody(false))
		Expect(status).To(Equal(http.StatusOK), body)

		var resp struct {
			Model   string `json:"model"`
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		Expect(json.Unmarshal([]byte(body), &resp)).To(Succeed(), body)
		// The mock echoes the requested model; the request only succeeds if the
		// dispatch reached the per-route server and its balancer picked the peer.
		Expect(resp.Model).To(Equal("mock-llm"))
		Expect(resp.Choices).NotTo(BeEmpty())
	})

	It("serves the mock's /v1/models through the route", func() {
		status, body := do(http.MethodGet, dispatchURL("/mock/v1/models"), nil)
		Expect(status).To(Equal(http.StatusOK), body)

		var resp struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		Expect(json.Unmarshal([]byte(body), &resp)).To(Succeed(), body)
		Expect(resp.Data).To(HaveLen(1))
		// The id is the mock's MOCK_MODEL_IDS value: only the mock answers this.
		Expect(resp.Data[0].ID).To(Equal("mock-llm"))
	})

	It("streams SSE through the dispatch port", func() {
		status, body := do(http.MethodPost, dispatchURL("/mock/v1/chat/completions"), chatBody(true))
		Expect(status).To(Equal(http.StatusOK), body)
		chunks := 0
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "data:") {
				chunks++
			}
		}
		Expect(chunks).To(BeNumerically(">=", 2), body)
		Expect(body).To(ContainSubstring("data: [DONE]"))
	})

	It("answers 502 for an unknown route key and stays up", func() {
		status, _ := do(http.MethodPost, dispatchURL("/nosuch/v1/chat/completions"), chatBody(false))
		Expect(status).To(Equal(http.StatusBadGateway))

		status, body := do(http.MethodGet, adminURL("/healthz"), nil)
		Expect(status).To(Equal(http.StatusOK))
		Expect(strings.TrimSpace(body)).To(Equal("ok"))
	})

	It("reports the route on /mock/_health_status", func() {
		status, body := do(http.MethodGet, dispatchURL("/mock/_health_status"), nil)
		Expect(status).To(Equal(http.StatusOK), body)
		Expect(body).To(ContainSubstring("mock"))
	})
})

func dispatchURL(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", cfg.localPort, path)
}

func adminURL(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", cfg.adminPort, path)
}

func chatBody(stream bool) []byte {
	body, err := json.Marshal(map[string]any{
		"model":          "mock-llm",
		"stream":         stream,
		"stream_options": map[string]any{"include_usage": true},
		"max_tokens":     8,
		"messages":       []map[string]string{{"role": "user", "content": "hi"}},
	})
	Expect(err).NotTo(HaveOccurred())
	return body
}

func do(method, url string, body []byte) (int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	Expect(err).NotTo(HaveOccurred())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return resp.StatusCode, string(out)
}
