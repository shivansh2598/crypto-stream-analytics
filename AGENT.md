# AGENT.md

## Project Overview

Build a production-style real-time analytics platform for cryptocurrency market data.

The goal is to learn and demonstrate the integration of:

- Binance WebSocket API
- Apache Kafka
- Apache Flink
- StarRocks
- Apache Superset
- Docker Compose
- Kubernetes (later phase)

The project should be structured as an open-source portfolio project with clean architecture, documentation, and reproducible local setup.

---

# Architecture

```
Binance WebSocket
       ↓
Market Data Producer
       ↓
   Kafka Topic
       ↓
  Apache Flink
       ↓
   StarRocks
       ↓
Superset Dashboard
```

Deployment (Local):

```
Docker Compose
├── Kafka
├── Flink JobManager
├── Flink TaskManager
├── StarRocks
└── Superset
```

Future Deployment:

```
Kubernetes
├── StarRocks
├── Kafka
├── Flink
└── Superset
``` 

---

# MVP Scope

Track only:

- BTCUSDT
- ETHUSDT
- SOLUSDT

Implement only:

1. Live trade ingestion
2. Kafka publishing
3. Flink VWAP calculation
4. StarRocks storage
5. Dashboard visualization

Avoid adding additional analytics until MVP is complete.

---

# Repository Structure

```
crypto-stream-analytics/
├── services/
│   ├── market-data-producer/
│   └── flink-jobs/
├── infrastructure/
│   ├── docker-compose/
│   └── kubernetes/
├── schemas/
│   ├── trade-event.json
│   └── vwap-event.json
├── docs/
│   ├── architecture.md
│   ├── local-development.md
│   └── deployment.md
├── scripts/
├── docker-compose.yml
└── README.md
``` 

---

# Service Responsibilities

## Market Data Producer

Language: Python

Responsibilities:

- Connect to Binance WebSocket
- Subscribe to:
  - btcusdt@trade
  - ethusdt@trade
  - solusdt@trade
- Normalize events
- Publish to Kafka

No analytics or business logic should exist here.

Output Topic:

`crypto_trades`

---

## Kafka

Purpose:

- Event transport layer
- Decouple ingestion from processing

Topic:

`crypto_trades`

Partition Key:

`symbol`

---

## Flink Job

Input:

`crypto_trades`

Responsibilities:

- Consume trade events
- Calculate rolling VWAP per symbol

Formula:

```
VWAP = Σ(price × quantity) / Σ(quantity)
```

Output:

`crypto_vwap`

---

## StarRocks

Tables:

### crypto_trades

Stores raw trade events.

### crypto_vwap

Stores latest VWAP metrics.

---

## Superset

Create a dashboard showing:

- Symbol
- Current Price
- Current VWAP
- Difference %

Only one dashboard is required for MVP.

---

# Event Contracts

## Trade Event

```json
{
  "symbol": "BTCUSDT",
  "price": 105432.11,
  "quantity": 0.25,
  "trade_time": 1717920000
}
```

---

## VWAP Event

```json
{
  "symbol": "BTCUSDT",
  "current_price": 105432.11,
  "vwap": 105120.44,
  "delta_percent": 0.29,
  "timestamp": 1717920000
}
```

---

# Milestones

## Milestone 1

Repository bootstrap.

Deliverables:

- Folder structure
- README
- Docker Compose skeleton

---

## Milestone 2

Binance → Kafka

Success Criteria:

- Kafka receives live trade events

---

## Milestone 3

Kafka → Flink

Success Criteria:

- Flink consumes and logs trade events

---

## Milestone 4

VWAP Computation

Success Criteria:

- Flink calculates VWAP correctly

---

## Milestone 5

Flink → StarRocks

Success Criteria:

- VWAP metrics persisted

---

## Milestone 6

StarRocks → Superset

Success Criteria:

- Dashboard displays VWAP metrics

---

## Milestone 7

Kubernetes Deployment

Success Criteria:

- StarRocks deployed on Kubernetes
- Documentation updated

---

# Engineering Principles

- Keep services small and focused.
- Prefer configuration over hardcoding.
- Document every component.
- Use Docker Compose for local development.
- Treat Kubernetes as a deployment concern, not a business logic concern.
- Complete MVP before adding new analytics.

---

# Future Enhancements

After MVP completion:

- Top gainers/losers
- Volume spikes
- Trade velocity
- Alert generation
- Multiple Kafka topics
- Flink checkpointing
- Kubernetes deployment of all services
- Monitoring and observability
- CI/CD pipeline