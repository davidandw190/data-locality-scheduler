FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o webhook ./cmd/webhook.go

FROM alpine:3.21
RUN apk --no-cache add ca-certificates && \
    mkdir -p /etc/webhook/certs
COPY --from=builder /app/webhook /usr/local/bin/webhook
ENTRYPOINT ["/usr/local/bin/webhook"]