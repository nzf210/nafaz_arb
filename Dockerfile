# Stage 1: Build binary
FROM docker.io/library/golang:alpine AS builder

WORKDIR /build

# Copy dependency files and download modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY internal/ internal/

# Build static binary without CGO
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o nafaz-scanner ./cmd/nafaz-scanner

# Stage 2: Minimal runtime image
FROM docker.io/library/alpine:3.20

ARG APP_UID=1000
ARG APP_GID=1000

# ca-certificates required for HTTPS requests to exchange APIs and Telegram
RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S -g ${APP_GID} appgroup \
    && adduser -S -u ${APP_UID} -G appgroup appuser

WORKDIR /app

# Copy compiled binary from builder stage
COPY --from=builder /build/nafaz-scanner /app/nafaz-scanner

USER appuser

# Expose default HTTP API port
EXPOSE 8080

ENTRYPOINT ["/app/nafaz-scanner"]
CMD ["-config", "/app/config.json"]
