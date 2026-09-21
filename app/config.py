from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "secret-api-key"

    outbox_poll_interval_sec: float = 1.0
    outbox_batch_size: int = 50

    webhook_max_retries: int = 3
    webhook_timeout_sec: float = 10.0

    model_config = {"env_file": ".env"}


settings = Settings()
