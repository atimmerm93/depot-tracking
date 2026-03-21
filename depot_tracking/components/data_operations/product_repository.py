from __future__ import annotations

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.transactional_decorator import transactional
from sqlalchemy import select

from .models import ProductModel
from ...core.models import Product


class ProductRepository(BaseDao):

    @transactional
    def get_by_id(self, product_id: int) -> ProductModel | None:
        product = self._session.query(Product).where(Product.id == product_id).one_or_none()
        if product is None:
            return None
        return self._to_model(product)

    @transactional
    def get_by_wkn(self, wkn: str) -> ProductModel | None:
        row = self._session.execute(select(Product).where(Product.wkn == wkn)).scalar_one_or_none()
        if row is None:
            return None
        return self._to_model(row)

    @transactional
    def find_by_wkn_or_isin(self, *, wkn: str, isin: str | None) -> ProductModel | None:
        statement = select(Product).where(Product.wkn == wkn)
        if isin:
            statement = select(Product).where((Product.wkn == wkn) | (Product.isin == isin))
        row = self._session.execute(statement).scalar_one_or_none()
        if row is None:
            return None
        return self._to_model(row)

    @staticmethod
    def _to_model(row: Product) -> ProductModel:
        return ProductModel(
            id=row.id,
            wkn=row.wkn,
            isin=row.isin,
            name=row.name,
            ticker=row.ticker,
        )
