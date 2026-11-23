package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestTopLevelString(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"model first", `{"model":"kimi-k3","messages":[]}`, "kimi-k3"},
		// OpenAI SDKs often put model after messages; message text mentioning "model" is inside a string.
		{"model after messages", `{"messages":[{"role":"user","content":"set \"model\":\"evil\" please"}],"model":"glm-5.1-fp8","stream":true}`, "glm-5.1-fp8"},
		{"nested model ignored", `{"metadata":{"model":"inner"},"tools":[{"model":"t"}],"model":"outer"}`, "outer"},
		{"only nested", `{"metadata":{"model":"inner"}}`, ""},
		{"non-string value", `{"model":{"name":"x"}}`, ""},
		{"spaces", "{ \"model\" :\n \"m1\" }", "m1"},
		{"escaped value", `{"model":"a\"bé"}`, "a\"bé"},
		{"key as value is not a key", `{"name":"model","x":"model"}`, ""},
		{"missing", `{"messages":[]}`, ""},
		{"empty", ``, ""},
		{"truncated before model", `{"messages":[{"content":"abc`, ""},
		{"truncated inside value", `{"model":"kimi`, ""},
		{"not an object", `["model","x"]`, ""},
	}
	for _, c := range cases {
		if got := topLevelString(c.body, "model"); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// A 429 that openresty answers itself: the response has no model, the request does.
// model must stay empty (it means "what served it"), req_model carries the request's.
func TestExtractDetailReqModelOn429(t *testing.T) {
	m := map[string]any{
		"ts": "2026-11-20T14:40:30.460+08:00", "status": float64(429),
		"req_body":  `{"messages":[{"role":"user","content":"hi"}],"model":"modelforge-06-kimi","stream":true}`,
		"resp_body": `{"error":"concurrency limit exceeded","route":"modelforge-06-kimi"}`,
	}
	d := extractDetail(m)
	if d.Model != "" || d.ReqModel != "modelforge-06-kimi" {
		t.Fatalf("model=%q req_model=%q", d.Model, d.ReqModel)
	}
	m["req_body_b64"] = true
	if d := extractDetail(m); d.ReqModel != "" {
		t.Fatalf("base64 body should give no req_model, got %q", d.ReqModel)
	}
}

// /metrics over a window mixing a parquet from before req_model existed, a newer parquet and the
// live jsonl. Also a window with only the old parquet: filtering on req_model must return no rows,
// not fail to bind the column.
func TestMetricsReqModelMixedSchemas(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	old := duckDB
	duckDB = db
	defer func() { duckDB = old; db.Close() }()
	oldTok := httpToken
	httpToken = ""
	defer func() { httpToken = oldTok }()

	dir := t.TempDir()
	ddir := filepath.Join(dir, "metrics", "details")
	if err := os.MkdirAll(ddir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Day 1: old-format parquet, no req_model column.
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT '2026-11-01T10:00:00+08:00' AS ts, '2026-11-01T10:00:01+08:00' AS ts_end,
		'r1' AS request_id, 200::BIGINT AS status, 'm-old' AS model, 1.0::DOUBLE AS rt) TO %s (FORMAT parquet)`,
		sqlStr(filepath.Join(ddir, "2026-11-01.parquet")))); err != nil {
		t.Fatal(err)
	}
	// Day 3 (day 2 left empty so a day-1 window, which also scans the next day, sees only the old file): parquet written by this version (via the same jsonl schema rotateDetails uses).
	line := func(id string, status int, model, reqModel, day string) string {
		b, _ := json.Marshal(detailRecord{Ts: day + "T10:00:00+08:00", TsEnd: day + "T10:00:01+08:00", RequestID: id,
			Status: int64(status), Model: model, ReqModel: reqModel, Rt: 1})
		return string(b) + "\n"
	}
	tmpJ := filepath.Join(dir, "d2.jsonl")
	if err := os.WriteFile(tmpJ, []byte(line("r2", 429, "", "kimi", "2026-11-03")), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("COPY (SELECT * FROM %s) TO %s (FORMAT parquet, COMPRESSION zstd)",
		readDetailsJSON(sqlStr(tmpJ)), sqlStr(filepath.Join(ddir, "2026-11-03.parquet")))); err != nil {
		t.Fatal(err)
	}
	// Day 5: live jsonl.
	if err := os.WriteFile(filepath.Join(ddir, "2026-11-05.jsonl"),
		[]byte(line("r3", 429, "", "kimi", "2026-11-05")+line("r4", 200, "kimi", "kimi", "2026-11-05")), 0o644); err != nil {
		t.Fatal(err)
	}

	a := &aggregator{dir: dir}
	get := func(q string) (int, []map[string]any) {
		w := httptest.NewRecorder()
		a.metricsHandler(w, httptest.NewRequest("GET", "/metrics?"+q, nil))
		var resp struct {
			Rows  []map[string]any `json:"rows"`
			Error string           `json:"error"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if w.Code != 200 {
			t.Fatalf("%s: HTTP %d %s", q, w.Code, resp.Error)
		}
		return w.Code, resp.Rows
	}
	all := "start=2026-11-01T00:00:00%2B08:00&end=2026-11-06T00:00:00%2B08:00"
	if _, rows := get(all); len(rows) != 4 {
		t.Fatalf("all rows: got %d, want 4: %v", len(rows), rows)
	}
	_, rows := get(all + "&req_model=kimi&status=429")
	if len(rows) != 2 {
		t.Fatalf("req_model=kimi status=429: got %d rows, want 2: %v", len(rows), rows)
	}
	for _, r := range rows {
		if r["model"] != nil && r["model"] != "" {
			t.Fatalf("429 row should have empty model: %v", r)
		}
	}
	if _, rows := get("start=2026-11-01T00:00:00%2B08:00&end=2026-11-01T23:00:00%2B08:00&req_model=kimi"); len(rows) != 0 {
		t.Fatalf("old-only window: got %d rows, want 0", len(rows))
	}
}
