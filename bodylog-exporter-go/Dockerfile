# bodylog-exporter:tail 明细 jsonl → Prometheus 指标。纯 Go(CGO off)静态二进制。
# 构建上下文 = 本目录(bodylog-exporter-go/)。CI 打 tag 触发(见 openresty/.gitlab-ci.yml build:exporter)。
# GOPROXY 默认腾讯镜像(public-buildx runner 可达),与 Dockerfile.bodylog 一致。
# 两个 base 的 ARG 都必须声明在【任何 FROM 之前】。ARG 一旦写在某个 FROM 之后,
# 就只属于那个 build stage;而第二个 FROM 是在【全局作用域】解析的,取不到它 ——
# RUNTIME_BASE 曾写在下面第二个 FROM 的紧邻上方,于是解析成空串,报
# "base name (${RUNTIME_BASE}) should not be blank"。
# 内网 CI 一直没暴露这个问题,因为它每次都传 --build-arg RUNTIME_BASE=<harbor>,
# 把作用域错误盖住了;不传覆盖值时才会现形。Dockerfile.bodylog 的写法是对的。
ARG GO_BASE=golang:1.24-bookworm
ARG RUNTIME_BASE=debian:12-slim
FROM ${GO_BASE} AS build
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOSUMDB=off GOTOOLCHAIN=local
ARG GOPROXY=https://mirrors.tencent.com/go/,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -ldflags="-s -w" -o /bodylog-exporter .

FROM ${RUNTIME_BASE}
COPY --from=build /bodylog-exporter /usr/local/bin/bodylog-exporter
EXPOSE 9110
ENTRYPOINT ["/usr/local/bin/bodylog-exporter"]
