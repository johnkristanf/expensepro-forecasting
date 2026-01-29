import decimal
from collections import defaultdict
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.base import reflect_existing_table
from src.database.session import Database

forecast_router = APIRouter()


@forecast_router.get("/")
async def get_forecast_data(
    session: AsyncSession = Depends(Database.get_async_session),
):
    expenses_table = await reflect_existing_table("expenses")
    categories_table = await reflect_existing_table("categories")

    # Join expenses with categories, select only created_at and amount from expense, and category name; filter Transportation only
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

    # Aggregate total amount per date (YYYY-MM-DD)
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

    # Get all following dates from min to max date
    if min_date and max_date:
        all_dates = []
        current = min_date
        while current <= max_date:
            all_dates.append(current)
            current = current + timedelta(days=1)
    else:
        all_dates = []

    # Fill in missing dates, especially Sundays, with amount 0, and add is_weekend property
    data = []
    for date in sorted(all_dates):
        amount = aggregation.get(date, 0.0)
        is_weekend = 1 if date.weekday() == 6 else 0  # Sunday is 6
        data.append({
            "date": date,
            "amount": amount,
            "is_weekend": is_weekend
        })

    return {"data": data}
