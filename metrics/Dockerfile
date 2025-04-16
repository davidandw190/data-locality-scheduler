FROM golang:1.23 AS builder
WORKDIR /app
COPY metrics-exporter.go .
COPY go.mod .
COPY go.sum .
RUN go build -o metrics-exporter metrics-exporter.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/metrics-exporter .
ENTRYPOINT ["/app/metrics-exporter"]