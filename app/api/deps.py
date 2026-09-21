from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_session


async def verify_api_key(x_api_key: Annotated[str | None, Header()] = None) -> str:
    if x_api_key is None or x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    return x_api_key


AuthDep = Depends(verify_api_key)
SessionDep = Annotated[AsyncSession, Depends(get_session)]
