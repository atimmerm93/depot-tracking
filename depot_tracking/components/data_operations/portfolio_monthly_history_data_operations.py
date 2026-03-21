from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import select

from .models import PortfolioMonthlyHistoryModel, PortfolioMonthlyHistoryWriteModel
from ...core.models import PortfolioMonthlyHistory


class PortfolioMonthlyHistoryDataOperations(BaseDao):
    @transactional
    def upsert(self, payload: PortfolioMonthlyHistoryWriteModel) -> tuple[PortfolioMonthlyHistoryModel, bool]:
        row = self._session.execute(
            select(PortfolioMonthlyHistory).where(PortfolioMonthlyHistory.month_date == payload.month_date)
        ).scalar_one_or_none()

        created = False
        if row is None:
            row = PortfolioMonthlyHistory(month_date=payload.month_date)
            self._session.add(row)
            created = True

        row.month_end_date = payload.month_end_date
        row.invested_amount_eur = float(payload.invested_amount_eur)
        row.portfolio_value_eur = float(payload.portfolio_value_eur)
        row.portfolio_profit_eur = float(payload.portfolio_profit_eur)
        row.source = payload.source
        self._session.flush()

        return (
            PortfolioMonthlyHistoryModel(
                id=row.id,
                month_date=row.month_date,
                month_end_date=row.month_end_date,
                invested_amount_eur=float(row.invested_amount_eur),
                portfolio_value_eur=float(row.portfolio_value_eur),
                portfolio_profit_eur=float(row.portfolio_profit_eur),
                source=row.source,
            ),
            created,
        )
