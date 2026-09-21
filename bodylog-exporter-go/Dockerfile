# bodylog-exporter:tail 明细 jsonl → Prometheus 指标。纯 Go(CGO off)静态二进制。
# 构建上下文 = 本目录(bodylog-exporter-go/)。CI 打 tag 触发(见 openresty/.gitlab-ci.yml build:exporter)。
# GOPROXY 默认腾讯镜像(public-buildx runner 可达),与 Dockerfile.bodylog 一致。
ARG GO_BASE=golang:1.24-bookworm
FROM ${GO_BASE} AS build
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOSUMDB=off GOTOOLCHAIN=local
ARG GOPROXY=https://mirrors.tencent.com/go/,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -ldflags="-s -w" -o /bodylog-exporter .

ARG RUNTIME_BASE=debian:12-slim
FROM ${RUNTIME_BASE}
COPY --from=build /bodylog-exporter /usr/local/bin/bodylog-exporter
EXPOSE 9110
ENTRYPOINT ["/usr/local/bin/bodylog-exporter"]
