from __future__ import annotations

from datetime import date, timedelta


class CalendarMonthService:
    @staticmethod
    def month_start(value: date) -> date:
        return value.replace(day=1)

    def month_end(self, month_start_value: date) -> date:
        return self.next_month(month_start_value) - timedelta(days=1)

    @staticmethod
    def next_month(value: date) -> date:
        year = value.year + (1 if value.month == 12 else 0)
        month = 1 if value.month == 12 else value.month + 1
        return date(year, month, 1)
