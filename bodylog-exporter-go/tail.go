package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type tailer struct {
	cfg config
	m   *metrics

	cp      checkpoint // 实时 checkpoint(Path/Offset/LastTs)
	curPath string     // 当前 tail 的文件
	offset  int64      // 当前字节偏移

	// 跨天补读后的去重:HTTP 补读观测过的 request_id 放进 seen,
	// 随后 re-scan 今天文件时跳过重叠;追到 live EOF 即丢弃 seen 释放内存。
	seen        map[string]struct{}
	dedupActive bool
}

func newTailer(cfg config, m *metrics) *tailer { return &tailer{cfg: cfg, m: m} }

// detailsPathFor:按【固定时区 loc】算 <date>.jsonl,与 bodylog 的 time.Local(北京)命名对齐(M3)。
func detailsPathFor(dir string, day time.Time, loc *time.Location) string {
	return filepath.Join(dir, day.In(loc).Format("2006-01-02")+".jsonl")
}

// dateFromPath:从 details/<YYYY-MM-DD>.jsonl 文件名反解出当天 00:00(loc),恢复起点用(H1)。
func dateFromPath(p string, loc *time.Location) (time.Time, bool) {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, ".jsonl")
	t, err := time.ParseInLocation("2006-01-02", base, loc)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func fileExists(p string) bool {
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// run:决定起点(续读 / 跨天补读 / 从今天头),然后进入 tail 主循环。阻塞直到 ctx 结束。
func (t *tailer) run(ctx context.Context) {
	today := detailsPathFor(t.cfg.detailsDir, time.Now(), t.cfg.dateLoc)

	if cp, ok := loadCheckpoint(t.cfg.checkpointPath); ok {
		t.cp = cp
		switch {
		case fileExists(cp.Path):
			// 短宕机 / 同文件仍在:从 offset 续读(最常见,完全无损)
			t.curPath, t.offset = cp.Path, cp.Offset
			log.Printf("resume: %s @ offset %d", cp.Path, cp.Offset)
		case t.cfg.bodylogURL != "":
			// 长宕机:checkpoint 文件已轮转成 parquet 删掉 → HTTP 补读缺口。
			// H1:文件不按 ts 排序(backfill 追写老 ts),cp.LastTs 是最后观测行的 ts 而非最小,
			//     用它当 start 会漏 ts<LastTs 的未观测行。改用【checkpoint 文件当天 00:00 - 1 天】做保守下界:
			//     过度拉取安全(request_id 去重 + counter 重启归零),绝不漏。
			start := t.recoveryStart(cp)
			end := time.Now().Add(-t.cfg.recoveryLag)
			seen := t.recoverViaHTTP(ctx, start, end)
			t.seen, t.dedupActive = seen, true
			t.m.recovers.Inc()
			log.Printf("recovery: 文件 %s 已轮转,HTTP 保守补读 %d 行(from %s);切今日文件去重续读", cp.Path, len(seen), start)
			t.curPath, t.offset = today, 0
		default:
			log.Printf("warn: checkpoint 文件 %s 已消失且无 BODYLOG_URL 无法补读,从今日文件头开始(可能丢该缺口)", cp.Path)
			t.curPath, t.offset = today, 0
		}
	} else {
		// 首次启动:tail 今天文件(从头,纳入今天已有数据)
		t.curPath, t.offset = today, 0
		log.Printf("start fresh: %s", today)
	}

	t.tailLoop(ctx)
}

// recoveryStart:保守恢复下界。优先 checkpoint 文件名当天 00:00 减 1 天(覆盖跨午夜 backfill);
// 文件名解析不出则回退 cp.LastTs(尽力),仍不行用 now-48h 兜底。
func (t *tailer) recoveryStart(cp checkpoint) string {
	if d, ok := dateFromPath(cp.Path, t.cfg.dateLoc); ok {
		return d.AddDate(0, 0, -1).Format(time.RFC3339)
	}
	if cp.LastTs != "" {
		return cp.LastTs
	}
	return time.Now().Add(-48 * time.Hour).Format(time.RFC3339)
}

func (t *tailer) tailLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			t.persist()
			return
		default:
		}

		today := detailsPathFor(t.cfg.detailsDir, time.Now(), t.cfg.dateLoc)

		// 跨天:当前 tail 的是过去某天文件 → 先把它读到 EOF 收干净,再切今天(offset 归零)。
		if t.curPath != today {
			// M2:文件已被轮转/删除(过去某天)→ 无法再读其尾巴,显式记潜在丢失并切今天;
			//     别把"文件不存在"当成 EOF 静默切。
			if !fileExists(t.curPath) {
				log.Printf("day roll: %s 已消失(轮转/删除)→ 切 %s;若 exporter 落后超一天可能丢其未读尾巴", t.curPath, today)
				t.curPath, t.offset = today, 0
				t.dropDedup()
				t.persist()
				continue
			}
			// 文件在:读到干净 EOF(atEOF)才切;瞬时读错误(atEOF=false)则本轮不切,下轮重试。
			n, atEOF := t.readFrom(t.curPath)
			if n == 0 && atEOF {
				log.Printf("day roll: %s 读毕 → 切 %s", t.curPath, today)
				t.curPath, t.offset = today, 0
				t.dropDedup()
				t.persist()
			}
			continue
		}

		// 今天文件:follow。无新数据(EOF 或瞬时读不到)都等一下再读(catch backfill append)。
		if n, atEOF := t.readFrom(t.curPath); n == 0 {
			if atEOF {
				t.dropDedup() // 追到 live EOF → 丢 seen(补读去重窗口结束)
			}
			select {
			case <-ctx.Done():
				t.persist()
				return
			case <-time.After(t.cfg.followInterval):
			}
		}
	}
}

// readFrom:从 t.offset 起把 path 里【完整的行】读完并 observe;半行(无结尾 \n)不推进,留待下次。
// 返回 (本次 observe 的完整行数, atEOF)。atEOF 仅在"文件已打开、干净读到 io.EOF"时为 true;
// 打开/seek 失败返回 (0,false) —— 让调用方区分「真读完」与「暂时读不到」,别把错误当 EOF 静默切天(M2)。
func (t *tailer) readFrom(path string) (int, bool) {
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("open %s: %v", path, err)
		}
		return 0, false
	}
	defer f.Close()
	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		log.Printf("seek %s @%d: %v", path, t.offset, err)
		return 0, false
	}
	r := bufio.NewReaderSize(f, 64*1024)
	count := 0
	atEOF := false
	for {
		line, err := r.ReadBytes('\n')
		if err == nil { // 完整一行
			t.handleLine(line)
			t.offset += int64(len(line))
			count++
			if count%2000 == 0 {
				t.persist() // 追赶大文件时别太频繁写 checkpoint
			}
			continue
		}
		if err == io.EOF {
			atEOF = true // 半行/无更多数据:不推进,下次再读(append 补齐后自然读到)
			break
		}
		log.Printf("read %s: %v", path, err)
		break
	}
	if count > 0 {
		t.persist()
		t.m.offset.Set(float64(t.offset))
	}
	return count, atEOF
}

func (t *tailer) handleLine(line []byte) {
	var d detailRecord
	if err := json.Unmarshal(line, &d); err != nil {
		// 坏行:offset 已由 readFrom 推进(跳过),不重试,避免卡死
		log.Printf("bad jsonl line(skip): %v", err)
		return
	}
	if t.dedupActive && d.RequestID != "" {
		if _, ok := t.seen[d.RequestID]; ok {
			return // 已被 HTTP 补读观测过
		}
	}
	t.m.observe(d)
	if ts := firstNonEmpty(d.TsEnd, d.Ts); ts != "" {
		t.cp.LastTs = ts
	}
}

func (t *tailer) dropDedup() {
	if t.dedupActive {
		t.dedupActive = false
		t.seen = nil
	}
}

func (t *tailer) persist() {
	t.cp.Path, t.cp.Offset = t.curPath, t.offset
	if err := saveCheckpoint(t.cfg.checkpointPath, t.cp); err != nil {
		log.Printf("save checkpoint: %v", err)
	}
}
