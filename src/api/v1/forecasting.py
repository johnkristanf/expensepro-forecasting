import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


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

    # Behavior Frequency Features (Spent or Borrowed)
    daily_expense["has_spend"] = (daily_expense["total_amount"] > 0).astype(int)

    daily_expense["borrowed_money"] = (
        (
            (daily_expense["weekend"] == 0)
            & (daily_expense["holiday"] == 0)
            & (daily_expense["has_spend"] == 0)
        )
    ).astype(int)


    # Outlier Flags
    upper = forecasting_service.get_iqr_upper_bound(daily_expense, "total_amount")
    daily_expense["is_outlier"] = (daily_expense["total_amount"] > upper).astype(int)
    
    # Rolling means
    daily_expense["rolling_mean_7d"] = (
        daily_expense["total_amount"].rolling(7, min_periods=1).mean()
    )

    daily_expense["rolling_mean_14d"] = (
        daily_expense["total_amount"].rolling(14, min_periods=1).mean()
    )

    daily_expense["rolling_mean_30d"] = (
        daily_expense["total_amount"].rolling(30, min_periods=1).mean()
    )

    # Average spend
    daily_expense["avg_spend_per_active_day_30d"] = daily_expense[
        "total_amount"
    ].rolling(30, min_periods=1).sum() / daily_expense["has_spend"].rolling(
        30, min_periods=1
    ).sum().replace(
        0, np.nan
    )
    
    
    df = daily_expense.copy()
    df["year_month"] = df["date_spent"].dt.to_period("M")
    df["month_name"] = df["date_spent"].dt.strftime("%B")

    monthly = df.groupby("year_month").agg(
        month_name=("month_name", "first"),
        total_amount=("total_amount", "sum"),                     # TARGET
        active_days=("has_spend", "sum"),
        rolling_mean_30d=("rolling_mean_30d", "mean"),
        avg_spend_intensity=("avg_spend_per_active_day_30d", "mean"),
        weekend_ratio=("weekend", "mean"),
        holiday_days=("holiday", "sum"),
    )
    
    # output_dir = "exports"
    # os.makedirs(output_dir, exist_ok=True)
    # output_path = os.path.join(output_dir, "monthly_transportation.csv")
    # monthly.to_csv(output_path, index=False)
    
    monthly["lag_1m"] = monthly["total_amount"].shift(1).fillna(0)
    monthly["lag_2m"] = monthly["total_amount"].shift(2).fillna(0)
    monthly["lag_3m"] = monthly["total_amount"].shift(3).fillna(0)


    # output_dir = "exports"
    # os.makedirs(output_dir, exist_ok=True)
    # output_path = os.path.join(output_dir, "monthly_transportation_with_lag.csv")
    # monthly.to_csv(output_path, index=False)
    
    plt.rcParams["figure.figsize"] = (8, 4)

    plt.plot(monthly.index.astype(str), monthly["total_amount"], marker="o")
    plt.title("Total Amount Over Time")
    plt.xlabel("Month")
    plt.ylabel("Total Amount")
    plt.grid(True)
    plt.show()


    return {
        "message": "Export successful",
    }
