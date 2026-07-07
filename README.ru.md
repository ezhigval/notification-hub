# notification-hub

![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)
[![CI](https://github.com/ezhigval/notification-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/ezhigval/notification-hub/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-blue)
![Tier](https://img.shields.io/badge/tier-middle-5319e7)

**[English](README.md)** · Русский

Единая доставка уведомлений: email, push, SMS (mock). Очереди Kafka по приоритетам, экспоненциальный retry, DLQ, дедупликация в Redis.

## Быстрый старт

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

# проверить статус
curl -s localhost:8088/api/v1/notifications/1 | jq .status
curl -s localhost:8088/api/v1/notifications/1/attempts | jq
```

Путь с retry: получатель `fail-email@x.com` — два сбоя, затем DLQ после 3 попыток.

## API

| Метод | Путь | Примечания |
|--------|------|------------|
| POST | `/api/v1/templates` | зарегистрировать шаблон |
| GET | `/api/v1/templates` | список |
| POST | `/api/v1/notifications` | заголовок `Idempotency-Key` |
| GET | `/api/v1/notifications/{id}` | статус |
| GET | `/api/v1/notifications/{id}/attempts` | лог доставки |

## Архитектура

```
HTTP ──► PG + Redis dedup ──► Kafka (high/normal/low topics)
                                    │
                              Worker pool
                                    ├── template render
                                    ├── channel adapter (mock)
                                    ├── retry + backoff (PG scheduler)
                                    └── DLQ topic after max attempts
```

## Топики по приоритету

- `notifications.high` — транзакционные алерты
- `notifications.normal` — по умолчанию
- `notifications.low` — маркетинговые рассылки

Отдельные consumer reader на топик; одна consumer group.

## Стек

Go 1.25 · chi · PostgreSQL · Redis · Kafka · text/template · [go-toolkit](https://github.com/ezhigval/go-toolkit)

Порт **8088** · MIT
