FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /meshflow ./cmd/meshflow/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates
COPY --from=builder /meshflow /usr/local/bin/meshflow

ENTRYPOINT ["meshflow", "node", "start"]
