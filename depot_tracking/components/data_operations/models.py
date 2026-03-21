from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict

from ...core.models import TransactionType


class SourceDocumentModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    file_path: str
    file_hash: str
    created_at: datetime | None = None


class SourceDocumentWriteModel(BaseModel):
    file_path: str
    file_hash: str


class ProductModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    wkn: str
    isin: str | None = None
    name: str | None = None
    ticker: str | None = None


class ProductUpsertModel(BaseModel):
    wkn: str
    isin: str | None = None
    name: str | None = None


class ProductTickerUpdateModel(BaseModel):
    product_id: int
    ticker: str


class TransactionModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    product_id: int
    type: TransactionType
    transaction_date: date
    quantity: float
    gross_amount: float
    costs: float
    currency: str
    bank: str
    source_document_id: int
    source_file: str
    source_hash: str


class TransactionWriteModel(BaseModel):
    product_id: int
    type: TransactionType
    transaction_date: date
    quantity: float
    gross_amount: float
    costs: float = 0.0
    currency: str = "EUR"
    bank: str = "UNKNOWN"
    source_file: str
    source_hash: str


class TransactionUpdateModel(BaseModel):
    transaction_id: int
    type: TransactionType | None = None
    quantity: float | None = None
    gross_amount: float | None = None
    costs: float | None = None


class HoldingSnapshotModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    product_id: int
    source_document_id: int
    snapshot_date: date
    quantity: float
    snapshot_price: float | None = None
    source_file: str
    source_hash: str


class HoldingSnapshotWriteModel(BaseModel):
    product_id: int
    snapshot_date: date
    quantity: float
    snapshot_price: float | None = None
    source_file: str
    source_hash: str


class ProcessedFileModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    source_document_id: int
    parser_version: str
    file_path: str
    file_hash: str


class ProcessedFileWriteModel(BaseModel):
    file_path: str
    file_hash: str
    parser_version: str


class AssetValueModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    product_id: int
    recorded_at: datetime | None = None
    value: float
    currency: str
    source: str


class AssetValueWriteModel(BaseModel):
    product_id: int
    value: float
    currency: str = "EUR"
    source: str
    recorded_at: datetime | None = None


class PortfolioMonthlyHistoryModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    month_date: date
    month_end_date: date
    invested_amount_eur: float
    portfolio_value_eur: float
    portfolio_profit_eur: float
    source: str


class PortfolioMonthlyHistoryWriteModel(BaseModel):
    month_date: date
    month_end_date: date
    invested_amount_eur: float
    portfolio_value_eur: float
    portfolio_profit_eur: float
    source: str = "computed"
