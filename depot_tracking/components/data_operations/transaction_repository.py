from __future__ import annotations

from datetime import date

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import case, func, select

from .models import ProductModel, TransactionModel
from ...core.models import Product, SourceDocument, Transaction, TransactionType


class TransactionRepository(BaseDao):

    @transactional
    def exists_by_source_hash(self, source_hash: str) -> bool:
        row = self._session.execute(
            select(Transaction.id)
            .join(SourceDocument, Transaction.source_document_id == SourceDocument.id)
            .where(SourceDocument.file_hash == source_hash)
        ).first()
        return row is not None

    @transactional
    def get_by_source_hash(self, source_hash: str) -> TransactionModel | None:
        row = self._session.execute(
            select(Transaction)
            .join(SourceDocument, Transaction.source_document_id == SourceDocument.id)
            .where(SourceDocument.file_hash == source_hash)
        ).scalar_one_or_none()
        if row is None:
            return None
        return self._to_model(row)

    @transactional
    def get_first_transaction_date(self) -> date | None:
        return self._session.scalar(select(func.min(Transaction.transaction_date)))

    @transactional
    def list_all(self) -> list[TransactionModel]:
        rows = self._session.execute(select(Transaction)).scalars().all()
        return [self._to_model(row) for row in rows]

    @transactional
    def list_for_product(self, product_id: int) -> list[TransactionModel]:
        rows = self._session.execute(select(Transaction).where(Transaction.product_id == product_id)).scalars().all()
        return [self._to_model(row) for row in rows]

    @transactional
    def list_by_product_and_type(self, *, product_id: int, tx_type: TransactionType) -> list[TransactionModel]:
        rows = self._session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == tx_type,
            )
        ).scalars().all()
        return [self._to_model(row) for row in rows]

    @transactional
    def list_by_product_and_type_with_source_prefix(
            self,
            *,
            product_id: int,
            tx_type: TransactionType,
            prefix: str,
    ) -> list[TransactionModel]:
        rows = self._session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == tx_type,
            )
        ).scalars().all()
        # Prefix selection is evaluated in-memory to avoid vendor-specific SQL string behavior.
        return [item for item in (self._to_model(row) for row in rows) if item.source_file.startswith(prefix)]

    @transactional
    def list_by_product_with_source_prefixes(self, *, product_id: int, prefixes: tuple[str, ...]) -> list[
        TransactionModel]:
        rows = self._session.execute(select(Transaction).where(Transaction.product_id == product_id)).scalars().all()
        return [
            item
            for item in (self._to_model(row) for row in rows)
            if any(item.source_file.startswith(prefix) for prefix in prefixes)
        ]

    @transactional
    def list_legacy_split_repairs(self) -> list[TransactionModel]:
        rows = self._session.execute(
            select(Transaction).where(Transaction.type.in_([TransactionType.BUY, TransactionType.SELL]))
        ).scalars().all()
        return [item for item in (self._to_model(row) for row in rows) if item.source_file.startswith("repair_split:")]

    @transactional
    def list_split_transactions_by_prefix(
            self,
            *,
            product_id: int,
            tx_date: date,
            source_prefix: str,
    ) -> list[TransactionModel]:
        rows = self._session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == TransactionType.SPLIT,
                Transaction.transaction_date == tx_date,
            )
        ).scalars().all()
        return [item for item in (self._to_model(row) for row in rows) if item.source_file.startswith(source_prefix)]

    @transactional
    def sum_signed_quantity_until(self, *, product_id: int, until_date: date) -> float:
        quantity_expr = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, Transaction.quantity),
                (Transaction.type == TransactionType.SELL, -Transaction.quantity),
                (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                else_=0.0,
            )
        )
        value = self._session.scalar(
            select(func.coalesce(quantity_expr, 0.0)).where(
                Transaction.product_id == product_id,
                Transaction.transaction_date <= until_date,
            )
        )
        return float(value or 0.0)

    @transactional
    def sum_signed_quantity_before(self, *, product_id: int, before_date: date) -> float:
        quantity_expr = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, Transaction.quantity),
                (Transaction.type == TransactionType.SELL, -Transaction.quantity),
                (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                else_=0.0,
            )
        )
        value = self._session.scalar(
            select(func.coalesce(quantity_expr, 0.0)).where(
                Transaction.product_id == product_id,
                Transaction.transaction_date < before_date,
            )
        )
        return float(value or 0.0)

    @transactional
    def get_buy_basis(self, *, product_id: int, until_date: date | None = None) -> tuple[float, float]:
        statement = select(
            func.coalesce(
                func.sum(
                    case(
                        (Transaction.type == TransactionType.BUY, Transaction.gross_amount + Transaction.costs),
                        (Transaction.type == TransactionType.SPLIT, Transaction.gross_amount),
                        else_=0.0,
                    )
                ),
                0.0,
            ),
            func.coalesce(
                func.sum(
                    case(
                        (Transaction.type == TransactionType.BUY, Transaction.quantity),
                        (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                        else_=0.0,
                    )
                ),
                0.0,
            ),
        ).where(Transaction.product_id == product_id)
        if until_date is not None:
            statement = statement.where(Transaction.transaction_date <= until_date)

        row = self._session.execute(statement).first()
        if row is None:
            return 0.0, 0.0
        return float(row[0] or 0.0), float(row[1] or 0.0)

    @transactional
    def get_net_cashflow_until(self, *, as_of: date) -> float:
        net_cashflow_expr = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, -(Transaction.gross_amount + Transaction.costs)),
                (Transaction.type == TransactionType.SELL, (Transaction.gross_amount - Transaction.costs)),
                (Transaction.type == TransactionType.ERTRAGSABRECHNUNG, (Transaction.gross_amount - Transaction.costs)),
                (Transaction.type == TransactionType.SPLIT, -Transaction.gross_amount),
                else_=0.0,
            )
        )
        value = self._session.scalar(
            select(func.coalesce(net_cashflow_expr, 0.0)).where(Transaction.transaction_date <= as_of))
        return float(value or 0.0)

    @transactional
    def list_open_positions(self, *, as_of: date | None = None) -> list[tuple[ProductModel, float]]:
        quantity_open = func.sum(
            case(
                (Transaction.type == TransactionType.BUY, Transaction.quantity),
                (Transaction.type == TransactionType.SELL, -Transaction.quantity),
                (Transaction.type == TransactionType.SPLIT, Transaction.quantity),
                else_=0.0,
            )
        ).label("quantity_open")

        stmt = (
            select(Product, quantity_open)
            .join(Transaction, Transaction.product_id == Product.id)
            .group_by(Product.id)
            .having(quantity_open > 0)
        )
        if as_of is not None:
            stmt = stmt.where(Transaction.transaction_date <= as_of)

        rows = self._session.execute(stmt).all()
        return [
            (
                ProductModel(id=product.id, wkn=product.wkn, isin=product.isin, name=product.name,
                             ticker=product.ticker),
                float(quantity or 0.0),
            )
            for product, quantity in rows
        ]

    @transactional
    def get_latest_non_unknown_bank_for_product(self, product_id: int) -> str:
        row = self._session.execute(
            select(Transaction.bank)
            .where(Transaction.product_id == product_id, Transaction.bank != "UNKNOWN")
            .order_by(Transaction.id.desc())
            .limit(1)
        ).first()
        return str(row[0]) if row and row[0] else "UNKNOWN"

    @transactional
    def exists_exact_buy(
            self,
            *,
            product_id: int,
            transaction_date: date,
            quantity: float,
            gross_amount: float,
            costs: float,
            bank: str,
            source_file: str,
    ) -> bool:
        rows = self._session.execute(
            select(Transaction).where(
                Transaction.product_id == product_id,
                Transaction.type == TransactionType.BUY,
                Transaction.transaction_date == transaction_date,
                Transaction.quantity == float(quantity),
                Transaction.gross_amount == float(gross_amount),
                Transaction.costs == float(costs),
                Transaction.bank == bank,
            )
        ).scalars().all()
        return any(self._to_model(row).source_file == source_file for row in rows)

    @staticmethod
    def _to_model(row: Transaction) -> TransactionModel:
        return TransactionModel(
            id=row.id,
            product_id=row.product_id,
            type=row.type,
            transaction_date=row.transaction_date,
            quantity=float(row.quantity),
            gross_amount=float(row.gross_amount),
            costs=float(row.costs),
            currency=row.currency,
            bank=row.bank,
            source_document_id=row.source_document_id,
            source_file=row.source_document.file_path,
            source_hash=row.source_document.file_hash,
        )
