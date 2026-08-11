package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
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

func detailsPathFor(dir string, day time.Time) string {
	return filepath.Join(dir, day.Format("2006-01-02")+".jsonl")
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
	today := detailsPathFor(t.cfg.detailsDir, time.Now())

	if cp, ok := loadCheckpoint(t.cfg.checkpointPath); ok {
		t.cp = cp
		switch {
		case fileExists(cp.Path):
			// 短宕机 / 同文件仍在:从 offset 续读(最常见,完全无损)
			t.curPath, t.offset = cp.Path, cp.Offset
			log.Printf("resume: %s @ offset %d", cp.Path, cp.Offset)
		case t.cfg.bodylogURL != "" && cp.LastTs != "":
			// 长宕机:checkpoint 文件已轮转成 parquet 删掉 → HTTP 补读缺口 [LastTs, now-lag]
			end := time.Now().Add(-t.cfg.recoveryLag)
			seen := t.recoverViaHTTP(ctx, cp.LastTs, end)
			t.seen, t.dedupActive = seen, true
			t.m.recovers.Inc()
			log.Printf("recovery: 文件 %s 已轮转,HTTP 补读 %d 行(from %s);切今日文件去重续读", cp.Path, len(seen), cp.LastTs)
			t.curPath, t.offset = today, 0
		default:
			log.Printf("warn: checkpoint 文件 %s 已消失且无法 HTTP 补读(BODYLOG_URL/LastTs 缺),从今日文件头开始(可能丢该缺口)", cp.Path)
			t.curPath, t.offset = today, 0
		}
	} else {
		// 首次启动:tail 今天文件(从头,纳入今天已有数据)
		t.curPath, t.offset = today, 0
		log.Printf("start fresh: %s", today)
	}

	t.tailLoop(ctx)
}

func (t *tailer) tailLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			t.persist()
			return
		default:
		}

		today := detailsPathFor(t.cfg.detailsDir, time.Now())

		// 跨天:当前 tail 的是过去某天文件 → 先把它读到 EOF 收干净,再切今天(offset 归零)。
		if t.curPath != today {
			if n := t.readFrom(t.curPath); n == 0 {
				log.Printf("day roll: %s 读毕 → 切 %s", t.curPath, today)
				t.curPath, t.offset = today, 0
				t.dropDedup() // 跨天视为已追平
				t.persist()
			}
			continue
		}

		// 今天文件:follow。EOF 时等一下再读(catch backfill append)。
		if n := t.readFrom(t.curPath); n == 0 {
			t.dropDedup() // 追到 live EOF → 丢 seen(补读去重窗口结束)
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
// 返回本次 observe 的完整行数(0 = 已到 EOF/无新数据)。offset 每完整行推进,周期性持久化。
func (t *tailer) readFrom(path string) int {
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("open %s: %v", path, err)
		}
		return 0
	}
	defer f.Close()
	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		log.Printf("seek %s @%d: %v", path, t.offset, err)
		return 0
	}
	r := bufio.NewReaderSize(f, 64*1024)
	count := 0
	for {
		line, err := r.ReadBytes('\n')
		if err == nil { // 完整一行
			t.handleLine(line)
			t.offset += int64(len(line))
			count++
			if count%500 == 0 {
				t.persist()
			}
			continue
		}
		if err == io.EOF {
			break // 半行:不推进,下次再读(append 补齐后自然读到)
		}
		log.Printf("read %s: %v", path, err)
		break
	}
	if count > 0 {
		t.persist()
		t.m.offset.Set(float64(t.offset))
	}
	return count
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
