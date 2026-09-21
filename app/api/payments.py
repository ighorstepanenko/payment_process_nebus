import uuid

from fastapi import APIRouter, Header, HTTPException, status

from app.api.deps import AuthDep, SessionDep
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.schemas import PaymentCreate, PaymentDetail, PaymentEvent, PaymentResponse

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[AuthDep])


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentResponse)
async def create_payment(
    body: PaymentCreate,
    session: SessionDep,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
) -> PaymentResponse:
    payment_repo = PaymentRepository(session)
    outbox_repo = OutboxRepository(session)

    existing = await payment_repo.get_by_idempotency_key(idempotency_key)
    if existing is not None:
        return PaymentResponse(
            payment_id=existing.id,
            status=existing.status.value,
            created_at=existing.created_at,
        )

    payment = await payment_repo.create(
        amount=body.amount,
        currency=body.currency.value,
        description=body.description,
        metadata=body.metadata,
        idempotency_key=idempotency_key,
        webhook_url=body.webhook_url,
    )

    event_payload = PaymentEvent(
        payment_id=payment.id,
        amount=body.amount,
        currency=body.currency.value,
        description=body.description,
        metadata=body.metadata,
        webhook_url=body.webhook_url,
        idempotency_key=idempotency_key,
    )

    await outbox_repo.create(
        aggregate_id=payment.id,
        event_type="payment.new",
        payload=event_payload.model_dump(mode="json"),
    )

    await session.commit()

    return PaymentResponse(
        payment_id=payment.id,
        status=payment.status.value,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentDetail:
    payment_repo = PaymentRepository(session)
    payment = await payment_repo.get_by_id(payment_id)

    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")

    return PaymentDetail(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status.value,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
