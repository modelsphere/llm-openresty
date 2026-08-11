// bodylog-exporter:把 bodylog-listener 落的【每请求明细 jsonl】翻译成 Prometheus 指标。
//
// 设计要点(详见 plans:bodylog Prometheus exporter B-tail):
//   - 不改 bodylog-listener;按【字节 offset tail append-only 的 details/<date>.jsonl】,逐行 observe。
//   - 补发/迟到记录也是 append 到文件末尾 → tail 每行恰好读一次,与 endTs 早晚无关 → 正常态零丢。
//   - counter/histogram 增量 observe(每请求 O(1),不存原始值)→ Prometheus rate()/histogram_quantile() 正确。
//   - 崩溃靠文件当持久缓冲:重启从 checkpoint offset 续读;跨天 jsonl→parquet 轮转的缺口走 bodylog /metrics HTTP 补读。
//
// 部署:裸机 systemd(读本地 metrics/details)或 k8s sidecar(共享卷)。自身 /metrics 免鉴权(聚合无正文)。
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envOrInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("warn: %s=%q 非整数,用默认 %d", k, os.Getenv(k), def)
	}
	return def
}

type config struct {
	detailsDir     string         // BODYLOG_DIR/metrics/details
	listen         string         // exporter 监听地址
	checkpointPath string         // offset 持久化文件
	bodylogURL     string         // bodylog HTTP base(仅跨天缺口补读用),空=不补读
	token          string         // bodylog /metrics 鉴权(补读用)
	recoveryLag    time.Duration  // 补读上界 = now - lag(等 flush 落定)
	followInterval time.Duration  // tail 到 EOF 后的轮询间隔
	dateLoc        *time.Location // 文件按天命名的时区(必须与 bodylog 一致,默认北京)
}

func loadConfig() config {
	// M3:bodylog 按 time.Local(北京)命名 <date>.jsonl;容器默认 UTC 会算错"今天"文件名。
	// 固定 date 时区 = DATE_TZ(默认 Asia/Shanghai);无 tzdata 时回退 +08:00 定偏移。
	tzName := envOr("DATE_TZ", "Asia/Shanghai")
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		log.Printf("warn: 加载时区 %s 失败(%v),回退固定 +08:00", tzName, err)
		loc = time.FixedZone("CST", 8*3600)
	}
	return config{
		detailsDir:     envOr("BODYLOG_DETAILS_DIR", "/data/bodylog/metrics/details"),
		listen:         envOr("EXPORTER_LISTEN", ":9110"),
		checkpointPath: envOr("CHECKPOINT_PATH", "/var/lib/bodylog-exporter/checkpoint.json"),
		bodylogURL:     envOr("BODYLOG_URL", ""),
		token:          envOr("BODYLOG_HTTP_TOKEN", ""),
		recoveryLag:    time.Duration(envOrInt("RECOVERY_LAG_SECONDS", 10)) * time.Second,
		followInterval: time.Duration(envOrInt("FOLLOW_INTERVAL_MS", 300)) * time.Millisecond,
		dateLoc:        loc,
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[bodylog-exporter] ")
	cfg := loadConfig()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("OK")) })
	srv := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("HTTP on %s (/metrics /healthz);details dir=%s", cfg.listen, cfg.detailsDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	newTailer(cfg, m).run(ctx) // 阻塞直到收到信号

	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	log.Printf("stopped")
}
