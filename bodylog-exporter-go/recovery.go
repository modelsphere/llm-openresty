package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// metricsResp:bodylog GET /metrics 的响应(只取要用的字段)。rows 的字段 tag 与 detailRecord 一致。
type metricsResp struct {
	Rows       []detailRecord `json:"rows"`
	Truncated  bool           `json:"truncated"`
	NextCursor string         `json:"next_cursor"`
}

// recoverViaHTTP:跨天缺口补读。走 bodylog 现成 GET /metrics?start=&end=(它同时读 parquet+jsonl),
// 游标翻页把 [startTs, end) 内每条 observe,返回观测过的 request_id 集合供后续 live tail 去重。
// 只在「checkpoint 文件已轮转成 parquet」的长宕机场景触发,是一次性动作。
func (t *tailer) recoverViaHTTP(ctx context.Context, startTs string, end time.Time) map[string]struct{} {
	seen := map[string]struct{}{}
	base := strings.TrimRight(t.cfg.bodylogURL, "/")
	endStr := end.Format(time.RFC3339Nano)
	client := &http.Client{Timeout: 60 * time.Second}
	cursor := ""
	pages := 0

	for {
		select {
		case <-ctx.Done():
			return seen
		default:
		}
		u := base + "/metrics?end=" + url.QueryEscape(endStr)
		if cursor != "" {
			u += "&cursor=" + url.QueryEscape(cursor)
		} else {
			u += "&start=" + url.QueryEscape(startTs)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			log.Printf("recovery: build req: %v", err)
			return seen
		}
		if t.cfg.token != "" {
			req.Header.Set("Authorization", "Bearer "+t.cfg.token)
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("recovery: GET /metrics: %v(放弃补读,live tail 兜底)", err)
			return seen
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("recovery: /metrics HTTP %d: %s", resp.StatusCode, truncate(body, 200))
			return seen
		}
		var mr metricsResp
		if err := json.Unmarshal(body, &mr); err != nil {
			log.Printf("recovery: 解析 /metrics 响应失败: %v", err)
			return seen
		}
		for i := range mr.Rows {
			d := mr.Rows[i]
			t.m.observe(d)
			if d.RequestID != "" {
				seen[d.RequestID] = struct{}{}
			}
			if ts := firstNonEmpty(d.TsEnd, d.Ts); ts != "" {
				t.cp.LastTs = ts
			}
		}
		pages++
		if !mr.Truncated || mr.NextCursor == "" {
			break
		}
		cursor = mr.NextCursor
	}
	log.Printf("recovery: 补读完成 %d 页 / %d 行", pages, len(seen))
	return seen
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
