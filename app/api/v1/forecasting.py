from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.base import reflect_existing_table
from app.database.session import Database

forecast_router = APIRouter()

@forecast_router.get("/")
async def get_forecast_data(session: AsyncSession = Depends(Database.get_async_session)):
    expenses_table = await reflect_existing_table("expenses")
    result = await session.execute(select(expenses_table))
    rows = result.fetchall()
    data = [
        dict(row._mapping)
        for row in rows
    ]
    return {"data": data}