from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from sqlalchemy import Date, DateTime, Enum, Float, ForeignKey, String, Text, UniqueConstraint, select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class TransactionType(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    ERTRAGSABRECHNUNG = "ERTRAGSABRECHNUNG"
    SPLIT = "SPLIT"


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    wkn: Mapped[str] = mapped_column(String(6), unique=True, index=True)
    isin: Mapped[str | None] = mapped_column(String(12), unique=True, nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ticker: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        server_onupdate=func.current_timestamp(),
    )

    transactions: Mapped[list[Transaction]] = relationship(back_populates="product", cascade="all, delete-orphan")
    asset_values: Mapped[list[AssetValue]] = relationship(back_populates="product", cascade="all, delete-orphan")
    holding_snapshots: Mapped[list[HoldingSnapshot]] = relationship(back_populates="product", cascade="all, delete-orphan")


class SourceDocument(Base):
    __tablename__ = "source_documents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    file_path: Mapped[str] = mapped_column(Text)
    file_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    transactions: Mapped[list[Transaction]] = relationship(back_populates="source_document")
    holding_snapshots: Mapped[list[HoldingSnapshot]] = relationship(back_populates="source_document")
    processed_files: Mapped[list[ProcessedFile]] = relationship(back_populates="source_document")


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="RESTRICT"), index=True)
    source_document_id: Mapped[int] = mapped_column(
        ForeignKey("source_documents.id", ondelete="RESTRICT"), index=True, unique=True
    )
    type: Mapped[TransactionType] = mapped_column(
        Enum(TransactionType, native_enum=False, validate_strings=True, name="transaction_type"),
        index=True,
    )
    transaction_date: Mapped[date] = mapped_column(Date, index=True)
    quantity: Mapped[float] = mapped_column(Float, default=0)
    gross_amount: Mapped[float] = mapped_column(Float)
    costs: Mapped[float] = mapped_column(Float, default=0)
    currency: Mapped[str] = mapped_column(String(3), default="EUR")
    bank: Mapped[str] = mapped_column(String(32), default="UNKNOWN", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    product: Mapped[Product] = relationship(back_populates="transactions")
    source_document: Mapped[SourceDocument] = relationship(back_populates="transactions", lazy="joined")

    def __init__(self, **kwargs: object) -> None:
        source_file = kwargs.pop("source_file", None)
        source_hash = kwargs.pop("source_hash", None)
        for key, value in kwargs.items():
            setattr(self, key, value)
        if source_file is not None or source_hash is not None:
            if source_file is None or source_hash is None:
                raise ValueError("source_file and source_hash must be provided together")
            self.source_document = SourceDocument(file_path=str(source_file), file_hash=str(source_hash))

    @hybrid_property
    def source_file(self) -> str:
        return self.source_document.file_path

    @source_file.setter
    def source_file(self, value: str) -> None:
        if self.source_document is None:
            self.source_document = SourceDocument(file_path=str(value), file_hash="")
            return
        self.source_document.file_path = str(value)

    @source_file.expression
    def source_file(cls):  # type: ignore[no-untyped-def]
        return select(SourceDocument.file_path).where(SourceDocument.id == cls.source_document_id).scalar_subquery()

    @hybrid_property
    def source_hash(self) -> str:
        return self.source_document.file_hash

    @source_hash.setter
    def source_hash(self, value: str) -> None:
        if self.source_document is None:
            self.source_document = SourceDocument(file_path="", file_hash=str(value))
            return
        self.source_document.file_hash = str(value)

    @source_hash.expression
    def source_hash(cls):  # type: ignore[no-untyped-def]
        return select(SourceDocument.file_hash).where(SourceDocument.id == cls.source_document_id).scalar_subquery()


class AssetValue(Base):
    __tablename__ = "asset_values"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), index=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp(), index=True)
    value: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(3), default="EUR")
    source: Mapped[str] = mapped_column(String(255))

    product: Mapped[Product] = relationship(back_populates="asset_values")


class ProcessedFile(Base):
    __tablename__ = "processed_files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_document_id: Mapped[int] = mapped_column(
        ForeignKey("source_documents.id", ondelete="CASCADE"), unique=True, index=True
    )
    processed_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    parser_version: Mapped[str] = mapped_column(String(32), default="v1")

    source_document: Mapped[SourceDocument] = relationship(back_populates="processed_files", lazy="joined")

    def __init__(self, **kwargs: object) -> None:
        file_path = kwargs.pop("file_path", None)
        file_hash = kwargs.pop("file_hash", None)
        for key, value in kwargs.items():
            setattr(self, key, value)
        if file_path is not None or file_hash is not None:
            if file_path is None or file_hash is None:
                raise ValueError("file_path and file_hash must be provided together")
            self.source_document = SourceDocument(file_path=str(file_path), file_hash=str(file_hash))

    @hybrid_property
    def file_path(self) -> str:
        return self.source_document.file_path

    @file_path.setter
    def file_path(self, value: str) -> None:
        if self.source_document is None:
            self.source_document = SourceDocument(file_path=str(value), file_hash="")
            return
        self.source_document.file_path = str(value)

    @file_path.expression
    def file_path(cls):  # type: ignore[no-untyped-def]
        return select(SourceDocument.file_path).where(SourceDocument.id == cls.source_document_id).scalar_subquery()

    @hybrid_property
    def file_hash(self) -> str:
        return self.source_document.file_hash

    @file_hash.setter
    def file_hash(self, value: str) -> None:
        if self.source_document is None:
            self.source_document = SourceDocument(file_path="", file_hash=str(value))
            return
        self.source_document.file_hash = str(value)

    @file_hash.expression
    def file_hash(cls):  # type: ignore[no-untyped-def]
        return select(SourceDocument.file_hash).where(SourceDocument.id == cls.source_document_id).scalar_subquery()


class PortfolioMonthlyHistory(Base):
    __tablename__ = "portfolio_monthly_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    month_date: Mapped[date] = mapped_column(Date, unique=True, index=True)
    month_end_date: Mapped[date] = mapped_column(Date)
    invested_amount_eur: Mapped[float] = mapped_column(Float, default=0.0)
    portfolio_value_eur: Mapped[float] = mapped_column(Float, default=0.0)
    portfolio_profit_eur: Mapped[float] = mapped_column(Float, default=0.0)
    source: Mapped[str] = mapped_column(String(32), default="computed")
    recorded_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp(), index=True)


@dataclass(frozen=True)
class ParsedTransaction:
    tx_type: TransactionType
    wkn: str
    isin: str | None
    product_name: str | None
    transaction_date: date
    quantity: float
    gross_amount: float
    costs: float


class HoldingSnapshot(Base):
    __tablename__ = "holding_snapshots"
    __table_args__ = (UniqueConstraint("product_id", "source_document_id", name="uq_holding_snapshots_product_source"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), index=True)
    source_document_id: Mapped[int] = mapped_column(ForeignKey("source_documents.id", ondelete="CASCADE"), index=True)
    snapshot_date: Mapped[date] = mapped_column(Date, index=True)
    quantity: Mapped[float] = mapped_column(Float)
    snapshot_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    product: Mapped[Product] = relationship(back_populates="holding_snapshots")
    source_document: Mapped[SourceDocument] = relationship(back_populates="holding_snapshots", lazy="joined")

    def __init__(self, **kwargs: object) -> None:
        source_file = kwargs.pop("source_file", None)
        source_hash = kwargs.pop("source_hash", None)
        for key, value in kwargs.items():
            setattr(self, key, value)
        if source_file is not None or source_hash is not None:
            if source_file is None or source_hash is None:
                raise ValueError("source_file and source_hash must be provided together")
            self.source_document = SourceDocument(file_path=str(source_file), file_hash=str(source_hash))

    @hybrid_property
    def source_file(self) -> str:
        return self.source_document.file_path

    @source_file.setter
    def source_file(self, value: str) -> None:
        if self.source_document is None:
            self.source_document = SourceDocument(file_path=str(value), file_hash="")
            return
        self.source_document.file_path = str(value)

    @source_file.expression
    def source_file(cls):  # type: ignore[no-untyped-def]
        return select(SourceDocument.file_path).where(SourceDocument.id == cls.source_document_id).scalar_subquery()

    @hybrid_property
    def source_hash(self) -> str:
        return self.source_document.file_hash

    @source_hash.setter
    def source_hash(self, value: str) -> None:
        if self.source_document is None:
            self.source_document = SourceDocument(file_path="", file_hash=str(value))
            return
        self.source_document.file_hash = str(value)

    @source_hash.expression
    def source_hash(cls):  # type: ignore[no-untyped-def]
        return select(SourceDocument.file_hash).where(SourceDocument.id == cls.source_document_id).scalar_subquery()


@dataclass(frozen=True)
class ParsedHolding:
    wkn: str
    isin: str | None
    product_name: str | None
    quantity: float
    snapshot_price: float | None = None
