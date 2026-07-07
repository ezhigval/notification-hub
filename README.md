# notification-hub

![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)
[![CI](https://github.com/ezhigval/notification-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/ezhigval/notification-hub/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-blue)
![Tier](https://img.shields.io/badge/tier-middle-5319e7)

Unified notification delivery: email, push, SMS (mock). Kafka priority queues, exponential retry, DLQ, Redis dedup.

## Quick start

```bash
make docker-up && sleep 3 && make seed

curl -s -X POST localhost:8088/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{
    "template_name":"order-shipped",
    "recipient":"user@example.com",
    "priority":"high",
    "variables":{"Name":"Valentin","OrderID":"42"}
  }' | jq

# poll status
curl -s localhost:8088/api/v1/notifications/1 | jq .status
curl -s localhost:8088/api/v1/notifications/1/attempts | jq
```

Force retry path: use recipient `fail-email@x.com` — fails twice then lands in DLQ after 3 attempts.

## API

| Method | Path | Notes |
|--------|------|-------|
| POST | `/api/v1/templates` | register template |
| GET | `/api/v1/templates` | list |
| POST | `/api/v1/notifications` | `Idempotency-Key` header |
| GET | `/api/v1/notifications/{id}` | status |
| GET | `/api/v1/notifications/{id}/attempts` | delivery log |

## Architecture

```
HTTP ──► PG + Redis dedup ──► Kafka (high/normal/low topics)
                                    │
                              Worker pool
                                    ├── template render
                                    ├── channel adapter (mock)
                                    ├── retry + backoff (PG scheduler)
                                    └── DLQ topic after max attempts
```

## Priority topics

- `notifications.high` — transactional alerts
- `notifications.normal` — default
- `notifications.low` — marketing batch

Separate consumer readers per topic; same consumer group.

## Stack

Go 1.25 · chi · PostgreSQL · Redis · Kafka · text/template · [go-toolkit](https://github.com/ezhigval/go-toolkit)

Port **8088** · MIT
