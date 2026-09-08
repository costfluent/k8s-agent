# Build stage
FROM golang:1.23-alpine AS builder

ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files first for caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.Version=${VERSION} -X main.GitCommit=${GIT_COMMIT} -X main.BuildTime=${BUILD_TIME}" \
    -o /costfluent-agent \
    ./cmd/agent

# Runtime stage
FROM scratch

# Copy CA certificates for HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary
COPY --from=builder /costfluent-agent /costfluent-agent

# Create data directory
VOLUME /data

# Expose metrics port
EXPOSE 9010

USER 1000:1000

ENTRYPOINT ["/costfluent-agent"]
