# --- STAGE 1: Compilazione ---
FROM golang:alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/loadgen_bin ./cmd/loadgen/*.go

# --- STAGE 2: Runner ---
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copia il binario compilato
COPY --from=builder /app/loadgen_bin .

# Directory di lavoro in cui verranno salvati i file CSV
VOLUME ["/app/output"]

ENTRYPOINT ["./loadgen_bin"]