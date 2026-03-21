from __future__ import annotations

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import func, select

from .models import HoldingSnapshotModel
from ...core.models import HoldingSnapshot


class HoldingSnapshotRepository(BaseDao):

    @transactional
    def exists_by_product_and_source_hash(self, *, product_id: int, source_hash: str) -> bool:
        holding_snapshot = self._session.query(HoldingSnapshot).join(HoldingSnapshot.source_document).where(
            HoldingSnapshot.product_id == product_id,
            HoldingSnapshot.source_document.has(file_hash=source_hash),
        ).one_or_none()
        return holding_snapshot is not None

    @transactional
    def list_all(self) -> list[HoldingSnapshotModel]:
        rows = self._session.execute(select(HoldingSnapshot)).scalars().all()
        return [self._to_model(row) for row in rows]

    @transactional
    def list_earliest_per_product(self) -> list[HoldingSnapshotModel]:
        earliest_snapshot_subq = (
            select(
                HoldingSnapshot.product_id,
                func.min(HoldingSnapshot.snapshot_date).label("first_snapshot_date"),
            )
            .group_by(HoldingSnapshot.product_id)
            .subquery()
        )
        rows = self._session.execute(
            select(HoldingSnapshot)
            .join(
                earliest_snapshot_subq,
                (HoldingSnapshot.product_id == earliest_snapshot_subq.c.product_id)
                & (HoldingSnapshot.snapshot_date == earliest_snapshot_subq.c.first_snapshot_date),
            )
        ).scalars().all()
        return [self._to_model(row) for row in rows]

    @staticmethod
    def _to_model(row: HoldingSnapshot) -> HoldingSnapshotModel:
        return HoldingSnapshotModel(
            id=row.id,
            product_id=row.product_id,
            source_document_id=row.source_document_id,
            snapshot_date=row.snapshot_date,
            quantity=float(row.quantity),
            snapshot_price=float(row.snapshot_price) if row.snapshot_price is not None else None,
            source_file=row.source_document.file_path,
            source_hash=row.source_document.file_hash,
        )
