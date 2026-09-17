# Green Load Balancer

A distributed, carbon-aware HTTP Load Balancer cluster in Go.

## Features
- Bully Election algorithm for distributed leader consensus via gRPC.
- Dynamic carbon intensity integration (Electricity Maps API).
- Real-time power metrics polling with hardware fallback.
- Routing policies: Round Robin, Least Connections, and Green Top-K.

## Setup
1. Copy `.env.example` to `.env` and set your API keys.
2. Run cluster: `docker compose up --build -d`
