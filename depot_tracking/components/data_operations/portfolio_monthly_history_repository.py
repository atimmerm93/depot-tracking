from __future__ import annotations

from datetime import date

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional

from .models import PortfolioMonthlyHistoryModel
from ...core.models import PortfolioMonthlyHistory


class PortfolioMonthlyHistoryRepository(BaseDao):

    @transactional
    def get_by_month(self, month_date: date) -> PortfolioMonthlyHistoryModel | None:
        row = self._session.query(PortfolioMonthlyHistory).filter(
            PortfolioMonthlyHistory.month_date == month_date
        ).one_or_none()
        if row is None:
            return None
        return PortfolioMonthlyHistoryModel(
            id=row.id,
            month_date=row.month_date,
            month_end_date=row.month_end_date,
            invested_amount_eur=float(row.invested_amount_eur),
            portfolio_value_eur=float(row.portfolio_value_eur),
            portfolio_profit_eur=float(row.portfolio_profit_eur),
            source=row.source,
        )
