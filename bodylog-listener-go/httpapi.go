// httpapi.go: bodylog-listener 的 HTTP 查询接口（端口 9998）。
//
//	GET /healthz                      存活探测（在 serveHTTP 内联）
//	GET /summary?minutes=N            最近 N 分钟 per-peer 分钟聚合
//	GET /metrics?start=&end=&...      时间窗内每条请求的 metrics 明细（读 metrics/details/*.{parquet,jsonl}）
//
// 与 main.go 同属 package main，共享 aggregator / detailRecord / sqlStr / get* 等定义。
package main

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// HTTP

type peerSummary struct {
	Peer             string `json:"peer"`
	Requests         int64  `json:"requests"`
	Status2xx        int64  `json:"status_2xx"`
	Status4xx        int64  `json:"status_4xx"`
	Status5xx        int64  `json:"status_5xx"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CachedTokens     int64  `json:"cached_tokens"` // vllm prefix cache 命中数
	CompletionTokens int64  `json:"completion_tokens"`
	TotalTokens      int64  `json:"total_tokens"`
	ReqBodyBytes     int64  `json:"req_body_bytes"`
	RespBodyBytes    int64  `json:"resp_body_bytes"`
	RTAvgMs          int64  `json:"rt_avg_ms"`
	RTMaxMs          int64  `json:"rt_max_ms"`
	// TTFT（first_chunk_t）：仅在请求带 first_chunk_t 字段时累计（流式响应必有；
	// 非流式响应近似等于 rt），FrtN 为实际带 frt 的请求数；FrtAvgMs 用 FrtN 做分母
	FrtAvgMs int64 `json:"frt_avg_ms"`
	FrtMaxMs int64 `json:"frt_max_ms"`
	FrtN     int64 `json:"frt_n"`
}

// authOK 校验 /summary、/metrics 的鉴权。httpToken 为空时不鉴权(opt-in)。
// 非空时接受 Authorization: Bearer <token> 或 ?token=<token>。用 ConstantTimeCompare 防时序侧信道。
func authOK(r *http.Request) bool {
	if httpToken == "" {
		return true
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		if subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(h, "Bearer ")), []byte(httpToken)) == 1 {
			return true
		}
	}
	if t := r.URL.Query().Get("token"); t != "" &&
		subtle.ConstantTimeCompare([]byte(t), []byte(httpToken)) == 1 {
		return true
	}
	return false
}

// writeUnauthorized 统一 401 响应。
func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": "unauthorized: missing/invalid token (Authorization: Bearer <token> or ?token=)"})
}

func (a *aggregator) summaryHandler(w http.ResponseWriter, r *http.Request) {
	if !authOK(r) {
		writeUnauthorized(w)
		return
	}
	minutes, _ := strconv.Atoi(r.URL.Query().Get("minutes"))
	if minutes <= 0 {
		minutes = 5
	}
	if a.archiveKeepMin > 0 && minutes > a.archiveKeepMin {
		minutes = a.archiveKeepMin
	}
	breakdown := r.URL.Query().Get("breakdown") == "true"
	// stream 过滤: ""=不过滤(默认,跨 stream 合并); true/1=只流式; false/0=只非流式。
	// 内部桶按 (minute,peer,stream) 分;默认必须合回 (minute,peer) 保证 buckets[] 一行一
	// (minute,peer)——monitor 按 (minute,peer) last-write-wins 消费,分裂会丢一半 TPM。
	streamFilter := r.URL.Query().Get("stream")

	now := time.Now().Unix()
	cutoff := now - int64(minutes)*60
	cutoff = cutoff - cutoff%60

	a.mu.RLock()
	var rows []bucket
	for _, b := range a.archive {
		if b.Minute >= cutoff {
			rows = append(rows, *b) // 拷贝快照，避免 RUnlock 后 archive 被并发更新
		}
	}
	for m, bk := range a.active {
		if m >= cutoff {
			for _, b := range bk {
				rows = append(rows, *b)
			}
		}
	}
	a.mu.RUnlock()

	if streamFilter != "" {
		wantKey := "false"
		if streamFilter == "true" || streamFilter == "1" {
			wantKey = "true"
		}
		f := rows[:0] // 原地过滤(rows 元素是值拷贝,安全)
		for _, b := range rows {
			if b.Stream == wantKey {
				f = append(f, b)
			}
		}
		rows = f
	}

	// per-peer aggregate
	perPeer := map[string]*bucket{}
	for i := range rows {
		b := &rows[i]
		p := perPeer[b.Peer]
		if p == nil {
			p = &bucket{Peer: b.Peer}
			perPeer[b.Peer] = p
		}
		p.Requests += b.Requests
		p.Status2xx += b.Status2xx
		p.Status4xx += b.Status4xx
		p.Status5xx += b.Status5xx
		p.PromptTok += b.PromptTok
		p.CachedTok += b.CachedTok
		p.ComplTok += b.ComplTok
		p.TotalTok += b.TotalTok
		p.ReqBytes += b.ReqBytes
		p.RespBytes += b.RespBytes
		p.RTSumMs += b.RTSumMs
		if b.RTMaxMs > p.RTMaxMs {
			p.RTMaxMs = b.RTMaxMs
		}
		p.FrtSumMs += b.FrtSumMs
		p.FrtN += b.FrtN
		if b.FrtMaxMs > p.FrtMaxMs {
			p.FrtMaxMs = b.FrtMaxMs
		}
	}
	peers := make([]peerSummary, 0, len(perPeer))
	for _, p := range perPeer {
		avg := int64(0)
		if p.Requests > 0 {
			avg = p.RTSumMs / p.Requests
		}
		frtAvg := int64(0)
		if p.FrtN > 0 {
			frtAvg = p.FrtSumMs / p.FrtN
		}
		peers = append(peers, peerSummary{
			Peer: p.Peer, Requests: p.Requests,
			Status2xx: p.Status2xx, Status4xx: p.Status4xx, Status5xx: p.Status5xx,
			PromptTokens: p.PromptTok, CachedTokens: p.CachedTok,
			CompletionTokens: p.ComplTok, TotalTokens: p.TotalTok,
			ReqBodyBytes: p.ReqBytes, RespBodyBytes: p.RespBytes,
			RTAvgMs: avg, RTMaxMs: p.RTMaxMs,
			FrtAvgMs: frtAvg, FrtMaxMs: p.FrtMaxMs, FrtN: p.FrtN,
		})
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Peer < peers[j].Peer })

	resp := map[string]any{
		"from":    time.Unix(cutoff, 0).Format(time.RFC3339),
		"to":      time.Now().Format(time.RFC3339),
		"minutes": minutes,
		"peers":   peers,
	}
	if breakdown {
		out := rows
		if streamFilter == "" {
			// 默认: 跨 stream 合并回 (minute,peer)，一行一 (minute,peer)(monitor 兼容)
			out = mergeBucketsByPeer(rows)
		}
		// 按 (Minute, Peer) 升序便于消费
		sort.Slice(out, func(i, j int) bool {
			if out[i].Minute != out[j].Minute {
				return out[i].Minute < out[j].Minute
			}
			return out[i].Peer < out[j].Peer
		})
		resp["buckets"] = out
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

// mergeBucketsByPeer 把按 (minute,peer,stream) 分裂的桶合并回 (minute,peer)(Stream 清空
// → json 省略),sum 各计数、max 取 RT/Frt 峰值。用于 /summary 默认 breakdown 输出,
// 保证 monitor 看到的 buckets[] 仍是一行一 (minute,peer)。
func mergeBucketsByPeer(rows []bucket) []bucket {
	mg := map[string]*bucket{}
	var order []*bucket
	for i := range rows {
		b := &rows[i]
		k := strconv.FormatInt(b.Minute, 10) + "\x00" + b.Peer
		e := mg[k]
		if e == nil {
			nb := *b
			nb.Stream = ""
			mg[k] = &nb
			order = append(order, &nb)
			continue
		}
		e.Requests += b.Requests
		e.Status2xx += b.Status2xx
		e.Status4xx += b.Status4xx
		e.Status5xx += b.Status5xx
		e.PromptTok += b.PromptTok
		e.CachedTok += b.CachedTok
		e.ComplTok += b.ComplTok
		e.TotalTok += b.TotalTok
		e.ReqBytes += b.ReqBytes
		e.RespBytes += b.RespBytes
		e.RTSumMs += b.RTSumMs
		if b.RTMaxMs > e.RTMaxMs {
			e.RTMaxMs = b.RTMaxMs
		}
		e.FrtSumMs += b.FrtSumMs
		e.FrtN += b.FrtN
		if b.FrtMaxMs > e.FrtMaxMs {
			e.FrtMaxMs = b.FrtMaxMs
		}
	}
	out := make([]bucket, len(order))
	for i, p := range order {
		out[i] = *p
	}
	return out
}

func (a *aggregator) serveHTTP(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/summary", a.summaryHandler)
	mux.HandleFunc("/metrics", a.metricsHandler)
	log.Printf("HTTP listening on %s (try /summary?minutes=5 | /metrics?start=..&end=..)", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("HTTP server err: %v", err)
	}
}

// parseTimeParam 接受 RFC3339（带或不带时区）或 Unix 秒（10 位整数）。无时区按 time.Local。
func parseTimeParam(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty")
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05", s, time.Local); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("bad time %q (want RFC3339 or unix seconds)", s)
}

// metricsHandler: GET /metrics?start=&end=[&model=&peer=&status=&min_frt=&limit=]
//
// 返回时间窗内每条请求的 metrics 明细（JSON 数组，非流式）。历史天读 metrics/details/<date>.parquet，
// 当天读 live <date>.jsonl，两支用 UNION ALL BY NAME 合并（DuckDB 没有单函数同时吃两种格式）。
// 硬上限 metricsMaxRows 条：多查 1 条探测，超限只回前 N 条 + truncated=true。
// 时间窗 [start,end) 与排序/游标统一按【结束时刻】，与 /summary 的结束时刻分桶口径一致：
// 优先 ts_end 列，历史无 ts_end 的行按 ts + rt 现算（见 endTs / rowEndTs）。
func (a *aggregator) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if !authOK(r) {
		writeUnauthorized(w)
		return
	}
	writeErr := func(code int, msg string) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
	}
	if duckDB == nil {
		writeErr(http.StatusServiceUnavailable, "duckdb unavailable (built without CGO/go-duckdb?)")
		return
	}
	q := r.URL.Query()
	end, err := parseTimeParam(q.Get("end"))
	if err != nil {
		writeErr(http.StatusBadRequest, "bad end: "+err.Error())
		return
	}
	// 续传游标(keyset 分页):cursor = base64url("<ts>|<request_id>")，由上一段截断响应的
	// next_cursor 给出。带 cursor 时下界用 (ts,request_id) 严格大于元组 → 精确续取、零重叠、
	// 无需按 request_id 去重;此时 start 可省(用 cursor 的 ts 做文件按天枚举的下界)。
	cursorTs, cursorID, hasCursor, err := parseCursor(q.Get("cursor"))
	if err != nil {
		writeErr(http.StatusBadRequest, "bad cursor: "+err.Error())
		return
	}
	var lo time.Time // 文件按天枚举的下界
	if hasCursor {
		if lo, err = parseTimeParam(cursorTs); err != nil {
			writeErr(http.StatusBadRequest, "bad cursor ts: "+err.Error())
			return
		}
	} else {
		if lo, err = parseTimeParam(q.Get("start")); err != nil {
			writeErr(http.StatusBadRequest, "bad start (or pass cursor): "+err.Error())
			return
		}
		if !end.After(lo) {
			writeErr(http.StatusBadRequest, "end must be after start")
			return
		}
	}
	limit := metricsMaxRows
	if v := q.Get("limit"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 && n < limit {
			limit = n
		}
	}

	// 选文件：按 local 日期枚举 [lo,end] 覆盖的天，每天优先 parquet 否则 live jsonl
	ddir := filepath.Join(a.dir, "metrics", "details")
	var parquets, jsonls []string
	d := time.Date(lo.In(time.Local).Year(), lo.In(time.Local).Month(), lo.In(time.Local).Day(), 0, 0, 0, 0, time.Local)
	endDay := end.In(time.Local)
	for !d.After(endDay) {
		date := d.Format("2006-01-02")
		pq := filepath.Join(ddir, date+".parquet")
		js := filepath.Join(ddir, date+".jsonl")
		if _, err := os.Stat(pq); err == nil {
			parquets = append(parquets, pq)
		} else if _, err := os.Stat(js); err == nil {
			jsonls = append(jsonls, js)
		}
		d = d.AddDate(0, 0, 1)
	}
	if len(parquets) == 0 && len(jsonls) == 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "truncated": false, "rows": []any{}})
		return
	}

	// 拼 SQL：两支子查询 UNION ALL BY NAME（容忍列序/缺列差异）
	fileList := func(paths []string) string {
		qs := make([]string, len(paths))
		for i, p := range paths {
			qs[i] = sqlStr(p)
		}
		return "[" + strings.Join(qs, ",") + "]"
	}
	var branches []string
	if len(parquets) > 0 {
		branches = append(branches, "SELECT * FROM read_parquet("+fileList(parquets)+")")
	}
	if len(jsonls) > 0 {
		branches = append(branches, "SELECT * FROM "+readDetailsJSON(fileList(jsonls)))
	}
	src := strings.Join(branches, " UNION ALL BY NAME ")

	// 过滤/排序/游标统一按【结束时刻】(与 /summary 的结束时刻分桶口径一致)，已是 TIMESTAMPTZ。
	// endTs：优先 ts_end 列；历史数据无 ts_end(UNION ALL BY NAME 填 NULL)→ 按 ts + rt 现算
	// 结束时刻；rt 也缺则 +0=ts。否则 NULL 比较会把旧记录全部排除/乱序。
	// 注：本版 DuckDB 无 +(TIMESTAMPTZ, INTERVAL) 重载，故走 epoch 秒空间：
	//   to_timestamp(epoch_us(ts)/1e6 + rt) —— tz 安全、保留亚秒，与 Go 侧 rowEndTs 一致。
	// 明细文件按完成日期切，按天枚举 [lo,end] 边界与结束时刻落点对齐。
	// 上界 end 开区间;下界——带 cursor 用 (endTs,request_id) 元组严格大于(keyset，零重叠)，
	// 否则用 start 闭区间。再叠可选 model/peer/status/min_frt。
	const endTs = "COALESCE(ts_end::TIMESTAMPTZ, to_timestamp(epoch_us(ts::TIMESTAMPTZ) / 1000000.0 + COALESCE(rt, 0)))"
	where := []string{
		fmt.Sprintf("%s < TIMESTAMPTZ %s", endTs, sqlStr(end.Format(time.RFC3339Nano))),
	}
	if hasCursor {
		where = append(where, fmt.Sprintf(
			"(%s, COALESCE(request_id,'')) > (TIMESTAMPTZ %s, %s)",
			endTs, sqlStr(cursorTs), sqlStr(cursorID)))
	} else {
		where = append(where, fmt.Sprintf("%s >= TIMESTAMPTZ %s", endTs, sqlStr(lo.Format(time.RFC3339Nano))))
	}
	if v := q.Get("model"); v != "" {
		where = append(where, "model = "+sqlStr(v))
	}
	if v := q.Get("peer"); v != "" {
		where = append(where, "(backend = "+sqlStr(v)+" OR peer = "+sqlStr(v)+")")
	}
	if v := q.Get("status"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			where = append(where, fmt.Sprintf("status = %d", n))
		} else {
			writeErr(http.StatusBadRequest, "bad status (want int)")
			return
		}
	}
	if v := q.Get("min_frt"); v != "" {
		if f, e := strconv.ParseFloat(v, 64); e == nil {
			where = append(where, fmt.Sprintf("frt >= %g", f))
		} else {
			writeErr(http.StatusBadRequest, "bad min_frt (want number, seconds)")
			return
		}
	}
	// stream 过滤：true/1 只流式;false/0 只非流式;null/unknown 只「未传 stream」的请求。
	// 明细里 stream 为 BOOLEAN(未传=NULL)。
	if v := q.Get("stream"); v != "" {
		switch v {
		case "true", "1":
			where = append(where, "stream = true")
		case "false", "0":
			where = append(where, "stream = false")
		case "null", "unknown":
			where = append(where, "stream IS NULL")
		default:
			writeErr(http.StatusBadRequest, "bad stream (want true/false/null)")
			return
		}
	}

	// ORDER BY (endTs, request_id) 给出确定的全序，使 keyset 游标能精确切分(endTs 不唯一)
	sqlText := fmt.Sprintf(
		"SELECT * FROM (%s) WHERE %s ORDER BY %s, COALESCE(request_id,'') LIMIT %d",
		src, strings.Join(where, " AND "), endTs, limit+1)

	rows, err := duckDB.QueryContext(r.Context(), sqlText)
	if err != nil {
		log.Printf("/metrics query err: %v", err)
		writeErr(http.StatusInternalServerError, "query failed: "+err.Error())
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		writeErr(http.StatusInternalServerError, "columns: "+err.Error())
		return
	}
	out := make([]map[string]any, 0, 256)
	truncated := false
	for rows.Next() {
		if len(out) >= limit {
			truncated = true
			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			writeErr(http.StatusInternalServerError, "scan: "+err.Error())
			return
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}
		out = append(out, row)
	}

	resp := map[string]any{"count": len(out), "truncated": truncated, "rows": out}
	if truncated && len(out) > 0 {
		// 结果按 (endTs,request_id) 升序，截断时回本段覆盖的 [returned_from, returned_to] +
		// next_cursor。游标键必须与 SQL 侧 COALESCE(ts_end,ts) 一致 → 用 rowEndTs 取有效结束
		// 时刻(ts_end 缺失回退 ts)。下一段用 cursor=next_cursor&end=<同 end> 精确续取(零重叠)。
		last := out[len(out)-1]
		from := rowEndTs(out[0])
		to := rowEndTs(last)
		resp["returned_from"] = from
		resp["returned_to"] = to
		resp["next_cursor"] = makeCursor(to, anyStr(last["request_id"]))
		resp["hint"] = fmt.Sprintf("命中上限 %d 条(按 ts_end,request_id 升序截断);本段覆盖 [%s, %s];下一段用 cursor=<next_cursor>&end=<同 end> 精确续取(零重叠、无需去重),或加 model/peer/status/min_frt 过滤收窄", limit, from, to)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

// rowEndTs 取行的有效结束时刻字符串 = ts_end，缺失(历史数据无该列→nil/空)则按 ts + rt 现算。
// 必须与 SQL 侧 endTs = COALESCE(ts_end, to_timestamp(epoch_us(ts)/1e6 + rt)) 一致(同在秒空间
// 加 rt)，否则 next_cursor 与 ORDER BY 键错位会导致分页跳行/重复。
func rowEndTs(row map[string]any) string {
	if v := row["ts_end"]; v != nil {
		if s := tsString(v); s != "" {
			return s
		}
	}
	tsStr := tsString(row["ts"])
	if tsStr == "" {
		return tsStr
	}
	rt := getFloat(row["rt"])
	if rt <= 0 {
		return tsStr
	}
	t, err := parseTimeParam(tsStr)
	if err != nil {
		return tsStr
	}
	return t.Add(time.Duration(rt * float64(time.Second))).Format(time.RFC3339Nano)
}

// tsString 把 ts 列值统一成字符串(保留亚秒精度):DuckDB 可能把 ts 推断成 VARCHAR(原样字符串)
// 或 TIMESTAMP/TIMESTAMPTZ(scan 成 time.Time)，两种都归一化。用 RFC3339Nano 保留毫秒/微秒，
// 保证 next_cursor 的 ts 与原行精确一致、回灌时元组比较不丢精度。
func tsString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case time.Time:
		return x.Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%v", v)
}

// anyStr 把任意列值取成字符串(nil → "")，用于取 request_id 拼游标。
func anyStr(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// makeCursor / parseCursor: keyset 游标 = base64url("<ts>|<request_id>")。
// base64url 不含 URL 特殊字符(无需再为 ts 里的 '+' 做 %2B)，可直接当 query 值回灌。
func makeCursor(ts, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(ts + "|" + id))
}

func parseCursor(s string) (ts, id string, ok bool, err error) {
	if s == "" {
		return "", "", false, nil
	}
	b, e := base64.RawURLEncoding.DecodeString(s)
	if e != nil {
		return "", "", false, fmt.Errorf("not base64url")
	}
	i := strings.IndexByte(string(b), '|')
	if i < 0 {
		return "", "", false, fmt.Errorf("malformed (want <ts>|<request_id>)")
	}
	return string(b[:i]), string(b[i+1:]), true, nil
}
