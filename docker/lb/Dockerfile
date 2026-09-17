# --- STAGE 1: Compilazione del binario ---
FROM golang:alpine AS builder

# Installa git e certificati necessari per scaricare i moduli gRPC/Go
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copia i descrittori delle dipendenze e scaricale in cache
COPY go.mod go.sum ./
RUN go mod download

# Copia l'intero codice sorgente
COPY .. .

# Compila il binario del LB in modo statico (CGO_ENABLED=0 disattiva dipendenze C esterne)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/lb_bin ./cmd/lb/*.go

# --- STAGE 2: Immagine finale ultraleggera ---
FROM alpine:3.19

# Installa i certificati SSL (necessari per chiamare le API HTTPS esterne come Electricity Maps)
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copia il binario compilato dallo stadio precedente
COPY --from=builder /app/lb_bin .

# Il comando di avvio di default quando il container parte
ENTRYPOINT ["./lb_bin"]