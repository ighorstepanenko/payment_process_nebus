from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.logging_config import setup_logging

setup_logging()

app = FastAPI(title="Payment Process Nebus", version="0.1.0")
app.include_router(payments_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
