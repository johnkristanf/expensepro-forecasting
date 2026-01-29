import decimal
import holidays
import pandas as pd

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
    forecasting_service: ForecastingService = Depends(get_forecasting_service),
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
                categories_table, expenses_table.c.category_id == categories_table.c.id
            )
        )
        .where(categories_table.c.name == "Transportation")
        .order_by(expenses_table.c.date_spent)
    )

    result = await session.execute(join_stmt)
    rows = result.fetchall()

    import os

    df = pd.DataFrame(rows, columns=["date_spent", "amount"])

    df["date_spent"] = pd.to_datetime(df["date_spent"]).dt.date
    df["amount"] = df["amount"].astype(float)

    daily_expense = df.groupby("date_spent", as_index=False).agg(
        total_amount=("amount", "sum")
    )

    full_date_range = pd.date_range(
        start=daily_expense["date_spent"].min(),
        end=daily_expense["date_spent"].max(),
        freq="D",
    )

    # Fill missing dates
    daily_expense = (
        daily_expense.set_index("date_spent")
        .reindex(full_date_range)
        .rename_axis("date_spent")
        .reset_index()
    )

    # Calendar Features
    daily_expense["is_weekend"] = (daily_expense["date_spent"].dt.weekday == 6).astype(
        int
    )

    ph_holidays = holidays.country_holidays(
        country="PH", years=daily_expense["date_spent"].dt.year.unique()
    )

    date_as_date = daily_expense["date_spent"].dt.date
    daily_expense["is_holiday"] = date_as_date.isin(ph_holidays).astype(int)


    # Fill missing dates with 0 amount, indicating missing data -> no transportation or absent day, instead of unknown
    daily_expense["total_amount"] = daily_expense["total_amount"].fillna(0.0)

    daily_expense["is_absent_day"] = (
        (daily_expense["is_weekend"] == 0)
        & (daily_expense["is_holiday"] == 0)
        & (daily_expense["total_amount"] == 0)
    ).astype(int)

    # Set IQR value to non-zeros to avoid large drop in 1st quantile
    iqr_base = daily_expense[
        (daily_expense["is_weekend"] == 0)
        & (daily_expense["is_holiday"] == 0)
        & (daily_expense["total_amount"] > 0)
    ]

    iqr_cap_amount = forecasting_service.get_iqr_capped_amount(iqr_base)
    daily_expense["total_amount_capped"] = daily_expense["total_amount"].clip(
        lower=0, upper=iqr_cap_amount
    )
    
    # Optional features, for business explaination purposes
    daily_expense["holiday_name"] = date_as_date.map(ph_holidays)

    output_dir = "exports"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "transportation_forecast.csv")
    daily_expense.to_csv(output_path, index=False)

    return {
        "message": "Export successful",
        "path": output_path,
        "iqr_cap_amount": iqr_cap_amount,
    }
