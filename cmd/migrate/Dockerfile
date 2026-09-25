FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/backfill ./cmd/migrate

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/backfill /usr/local/bin/backfill
ENTRYPOINT ["/usr/local/bin/backfill"]
CMD ["-h"]
