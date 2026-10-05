# bodylog-exporter: tails the detail jsonl and exports Prometheus metrics. A static
# pure-Go binary (CGO off). Build context = this directory (bodylog-exporter-go/).
# GOPROXY defaults to a regional mirror, as in Dockerfile.bodylog; override it with
# --build-arg (the release workflow passes the upstream proxy).
# Both base-image ARGs must be declared before ANY FROM. An ARG written after a FROM
# belongs to that build stage only, while the second FROM is resolved in the global
# scope and cannot see it: RUNTIME_BASE once sat right above the second FROM, resolved
# to an empty string and failed with "base name (${RUNTIME_BASE}) should not be
# blank". A build that always passed --build-arg RUNTIME_BASE hid the mistake; it only
# shows without the override. Dockerfile.bodylog does it right.
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
