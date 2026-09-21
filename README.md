# Payment Process Nebus

Асинхронный микросервис процессинга платежей для компании «Небус».

## Архитектура

```
┌─────────┐  POST /payments  ┌─────┐  outbox  ┌──────────┐  payments.new  ┌──────────┐
│  Client ├─────────────────►│ API ├─────────►│ Outbox   ├──────────────►│ Consumer │
└────┬────┘                  └──┬──┘          │ Publisher│               └─────┬────┘
     │  GET /payments/{id}      │              └──────────┘                     │
     │◄─────────────────────────┘                                              │
     │                                                                         │
     │◄──────────────── webhook notification ──────────────────────────────────┘
```

- **API** — FastAPI, принимает запросы, сохраняет Payment + OutboxEvent в одной транзакции
- **Outbox Publisher** — фоновый процесс, публикует события из таблицы outbox_events в RabbitMQ
- **Consumer** — FastStream worker, обрабатывает платёж (эмуляция 2-5 сек), обновляет статус, отправляет webhook

### Гарантии

| Механизм | Реализация |
|---|---|
| Outbox pattern | Payment + OutboxEvent в одной транзакции, отдельный publisher |
| Idempotency | Уникальный `Idempotency-Key` header, дедупликация по нему |
| Dead Letter Queue | `payments.dlx` exchange + `payments.dlq` queue |
| Retry (webhook) | Экспоненциальная задержка, до 3 попыток |
| Retry (consumer) | `REJECT_ON_ERROR` → сообщение уходит в DLQ через Dead Letter Exchange |

## Запуск

```bash
cp .env.example .env  # при необходимости отредактировать значения
docker compose up --build -d
```

Сервисы:
- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672 (guest/guest)

## Примеры API

### Создание платежа

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: my-unique-key-123" \
  -d '{
    "amount": 1500.00,
    "currency": "RUB",
    "description": "Оплата заказа #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://example.com/webhook"
  }'
```

Ответ (202 Accepted):
```json
{
  "payment_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2026-04-21T12:00:00Z"
}
```

### Получение информации о платеже

```bash
curl http://localhost:8000/api/v1/payments/550e8400-e29b-41d4-a716-446655440000 \
  -H "X-API-Key: secret-api-key"
```

### Проверка здоровья

```bash
curl http://localhost:8000/health
```

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | Подключение к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | Подключение к RabbitMQ |
| `API_KEY` | `secret-api-key` | Статический API ключ |

## Стек

- Python 3.11+
- FastAPI + Pydantic v2
- SQLAlchemy 2.0 (async)
- PostgreSQL 16
- RabbitMQ 3.13 (aio-pika)
- FastStream (consumer)
- Alembic (миграции)
- Docker + Docker Compose
