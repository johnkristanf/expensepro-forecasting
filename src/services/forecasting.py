from collections import defaultdict
from datetime import datetime, timedelta

from pandas import DataFrame


class ForecastingService:
    def aggregate_expenses_by_date(self, rows):
        """
        Aggregates amounts by date from the given rows.
        Returns aggregation dict, min_date, and max_date.
        """
        aggregation = defaultdict(float)
        min_date = None
        max_date = None
        for row in rows:
            amount_as_float = (
                float(row.amount)
                if isinstance(row.amount, (float, int))
                else float(row.amount)
            )
            date_value = (
                row.date_spent.date()
                if isinstance(row.date_spent, datetime)
                else datetime.strptime(str(row.date_spent), "%Y-%m-%d").date()
            )

            aggregation[date_value] += amount_as_float
            if min_date is None or date_value < min_date:
                min_date = date_value
            if max_date is None or date_value > max_date:
                max_date = date_value
        return aggregation, min_date, max_date

    def build_date_range(self, min_date, max_date):
        """
        Return a list of dates from min_date to max_date inclusive.
        """
        if min_date is None or max_date is None:
            return []
        all_dates = []
        current = min_date
        while current <= max_date:
            all_dates.append(current)
            current += timedelta(days=1)
        return all_dates

    def fill_date_gaps_with_properties(self, all_dates, aggregation, holidays_provider):
        """
        If a date is missing from aggregation, fill with zero.
        Adds 'is_weekend', 'is_holiday' for each date.
        Uses the DI-provided holidays_provider.
        """
        data = []
        for date in sorted(all_dates):
            amount = aggregation.get(date, 0.0)
            is_weekend = 1 if date.weekday() == 6 else 0
            is_holiday = 1 if date in holidays_provider else 0
            data.append(
                {
                    "date": date,
                    "amount": amount,
                    "is_weekend": is_weekend,
                    "is_holiday": is_holiday,
                }
            )
        return data

    def get_iqr_capped_amount(self, df: DataFrame):
        Q1 = df["total_amount"].quantile(0.25)
        Q3 = df["total_amount"].quantile(0.75)
        IQR = Q3 - Q1
        print("Q1: " + str(Q1))
        print("Q3: " + str(Q3))
        print("IQR: " + str(IQR))

        cap_amount = Q3 + 1.5 * IQR
        return cap_amount