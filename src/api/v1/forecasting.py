import decimal
import holidays
from collections import defaultdict
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.dependencies import get_forecasting_service
from src.services.forecasting import ForecastingService
from src.database.base import reflect_existing_table
from src.database.session import Database

forecast_router = APIRouter()


@forecast_router.get("/")
async def get_forecast_data(
    session: AsyncSession = Depends(Database.get_async_session),
    forecasting_service: ForecastingService = Depends(get_forecasting_service)
):
    expenses_table = await reflect_existing_table("expenses")
    categories_table = await reflect_existing_table("categories")

    join_stmt = (
        select(
            expenses_table.c.date_spent,
            expenses_table.c.amount,
        )
        .select_from(
            expenses_table.join(
                categories_table,
                expenses_table.c.category_id == categories_table.c.id
            )
        )
        .where(categories_table.c.name == "Transportation")
        .order_by(expenses_table.c.date_spent)
    )

    result = await session.execute(join_stmt)
    rows = result.fetchall()

    aggregation = defaultdict(float)
    min_date = None
    max_date = None
    for row in rows:
        amount_as_float = float(row.amount) if isinstance(row.amount, decimal.Decimal) else row.amount
        date_value = row.date_spent.date() if isinstance(row.date_spent, datetime) else datetime.strptime(str(row.date_spent), "%Y-%m-%d").date()

        aggregation[date_value] += amount_as_float
        if min_date is None or date_value < min_date:
            min_date = date_value
        if max_date is None or date_value > max_date:
            max_date = date_value

    all_dates = forecasting_service.build_date_range(min_date, max_date)

    ph_holidays = holidays.country_holidays('PH')
    data = forecasting_service.fill_date_gaps_with_properties(all_dates, aggregation, ph_holidays)

    return {"data": data}
