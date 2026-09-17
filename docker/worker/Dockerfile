# --- STAGE 1: Compilazione del binario ---
FROM golang:alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Compila il binario del worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/worker_bin ./cmd/worker/*.go

# --- STAGE 2: Immagine finale ---
FROM alpine:3.19

WORKDIR /root/

COPY --from=builder /app/worker_bin .

ENTRYPOINT ["./worker_bin"]