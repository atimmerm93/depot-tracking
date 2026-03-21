from __future__ import annotations

from datetime import datetime

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import func, select

from ...core.models import AssetValue
from .models import AssetValueModel


class AssetValueRepository(BaseDao):
    @transactional
    def get_by_product_and_source(self, *, product_id: int, source: str) -> AssetValueModel | None:
        row = self._session.execute(
            select(AssetValue).where(
                AssetValue.product_id == product_id,
                AssetValue.source == source,
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        return self._to_model(row)

    @transactional
    def get_latest_eur_value_as_of(self, *, product_id: int, as_of: datetime) -> float | None:
        value = self._session.scalar(
            select(AssetValue.value)
            .where(
                AssetValue.product_id == product_id,
                func.upper(AssetValue.currency) == "EUR",
                AssetValue.recorded_at <= as_of,
            )
            .order_by(AssetValue.id.desc())
            .limit(1)
        )
        return float(value) if value is not None else None

    @staticmethod
    def _to_model(row: AssetValue) -> AssetValueModel:
        return AssetValueModel(
            id=row.id,
            product_id=row.product_id,
            recorded_at=row.recorded_at,
            value=float(row.value),
            currency=row.currency,
            source=row.source,
        )
