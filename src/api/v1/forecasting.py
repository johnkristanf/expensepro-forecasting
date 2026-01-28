from collections import defaultdict
import decimal
    
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
    for row in rows:
        amount_as_float = float(row.amount) if isinstance(row.amount, decimal.Decimal) else row.amount
        aggregation[row.date_spent] += amount_as_float

    data = [
        {"date": date, "amount": amount}
        for date, amount in sorted(aggregation.items())
    ]

    return {"data": data}
