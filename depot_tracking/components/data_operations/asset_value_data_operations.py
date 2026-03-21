from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import select

from .models import AssetValueModel, AssetValueWriteModel
from ...core.models import AssetValue


class AssetValueDataOperations(BaseDao):

    @transactional
    def create(self, payload: AssetValueWriteModel) -> AssetValueModel:
        row = AssetValue(
            product_id=payload.product_id,
            value=float(payload.value),
            currency=payload.currency,
            source=payload.source,
        )
        if payload.recorded_at is not None:
            row.recorded_at = payload.recorded_at
        self._session.add(row)
        self._session.flush()
        return self._to_model(row)

    @transactional
    def upsert_by_product_and_source(self, payload: AssetValueWriteModel) -> tuple[AssetValueModel, bool]:
        row = self._session.execute(
            select(AssetValue).where(
                AssetValue.product_id == payload.product_id,
                AssetValue.source == payload.source,
            )
        ).scalar_one_or_none()

        created = False
        if row is None:
            row = AssetValue(
                product_id=payload.product_id,
                source=payload.source,
            )
            self._session.add(row)
            created = True

        row.value = float(payload.value)
        row.currency = payload.currency
        if payload.recorded_at is not None:
            row.recorded_at = payload.recorded_at

        self._session.flush()
        return self._to_model(row), created

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
