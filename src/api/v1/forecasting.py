import os
import numpy as np
import pandas as pd


from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.dependencies import get_forecasting_service
from src.services.forecasting import ForecastingService
from src.database.base import reflect_existing_table
from src.database.session import Database
from src.utils.data import get_ph_holidays

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

    # Fill missing dates with 0 amount, indicating missing data -> no transportation or absent day, instead of unknown
    daily_expense["total_amount"] = daily_expense["total_amount"].fillna(0.0)

    # Calendar Features (Holiday & Weekends)
    daily_expense["weekend"] = (daily_expense["date_spent"].dt.weekday == 6).astype(int)

    ph_holidays = get_ph_holidays(years=daily_expense["date_spent"].dt.year.unique())

    date_as_date = daily_expense["date_spent"].dt.date
    daily_expense["holiday"] = date_as_date.isin(ph_holidays).astype(int)
    daily_expense["holiday_name"] = date_as_date.map(ph_holidays)

    # Behavior Frequency Features (Work or Absent)
    daily_expense["has_spend"] = (daily_expense["total_amount"] > 0).astype(int)

    daily_expense["rolling_mean_7d"] = (
        daily_expense["total_amount"].rolling(7, min_periods=1).mean()
    )

    daily_expense["rolling_mean_14d"] = (
        daily_expense["total_amount"].rolling(14, min_periods=1).mean()
    )

    daily_expense["rolling_mean_30d"] = (
        daily_expense["total_amount"].rolling(30, min_periods=1).mean()
    )


    daily_expense["borrowed_money"] = (
        (
            (daily_expense["weekend"] == 0)
            & (daily_expense["holiday"] == 0)
            & (daily_expense["has_spend"] == 0)
        )
    ).astype(int)

    daily_expense["avg_spend_per_active_day_30d"] = daily_expense[
        "total_amount"
    ].rolling(30).sum() / daily_expense["has_spend"].rolling(30).sum().replace(
        0, np.nan
    )

    # Outlier Flags
    Q1 = daily_expense["total_amount"].quantile(0.25)
    Q3 = daily_expense["total_amount"].quantile(0.75)
    IQR = Q3 - Q1

    upper = Q3 + 1.5 * IQR

    daily_expense["is_outlier"] = (daily_expense["total_amount"] > upper).astype(int)

    output_dir = "exports"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "transportation_forecast.csv")
    daily_expense.to_csv(output_path, index=False)

    return {
        "message": "Export successful",
        "path": output_path,
    }
