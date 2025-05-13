# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 go build -o extract main.go && \
    chmod +x extract

# Runtime stage
FROM gcr.io/distroless/static-debian12

WORKDIR /app
COPY --from=builder /build/extract /app/extract
COPY --from=builder /build/.env /app/.env

CMD ["/app/extract"]