package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

// checkpoint:offset 持久化。只存【已 observe 到的位置】,绝不超前 —— 重启最坏重读几行
// (counter 重启归零,Prometheus rate() 识别 reset,不重复计),绝不跳过未读行。
type checkpoint struct {
	Path   string `json:"path"`    // 当前 tail 的文件绝对路径(details/<date>.jsonl)
	Offset int64  `json:"offset"`  // 已 observe 到的字节偏移
	LastTs string `json:"last_ts"` // 最后 observe 行的 ts_end(空则 ts)原始串,跨天补读的 start 参数
}

func loadCheckpoint(p string) (checkpoint, bool) {
	b, err := os.ReadFile(p)
	if err != nil {
		return checkpoint{}, false
	}
	var c checkpoint
	if err := json.Unmarshal(b, &c); err != nil {
		log.Printf("warn: checkpoint %s 解析失败(%v),忽略", p, err)
		return checkpoint{}, false
	}
	return c, c.Path != ""
}

// saveCheckpoint:原子写(temp + rename),避免崩溃写坏 checkpoint。
func saveCheckpoint(p string, c checkpoint) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}
