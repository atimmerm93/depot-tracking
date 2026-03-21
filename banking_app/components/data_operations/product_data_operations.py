import re

from di_unit_of_work.base_dao import BaseDao
from di_unit_of_work.session_provider import SessionProvider
from di_unit_of_work.transactional_decorator import transactional

from .models import ProductModel, ProductTickerUpdateModel, ProductUpsertModel
from ..shared import IdentifierCanonicalizer
from ...core.models import Product


class ProductDataOperations(BaseDao):
    def __init__(self, session_provider: SessionProvider, identifier_canonicalizer: IdentifierCanonicalizer) -> None:
        super().__init__(session_provider)
        self._identifier_canonicalizer = identifier_canonicalizer

    @transactional
    def upsert(self, payload: ProductUpsertModel) -> ProductModel:
        resolved_wkn, resolved_isin = self._identifier_canonicalizer.canonicalize(
            wkn=payload.wkn,
            isin=payload.isin,
        )

        statement = self._session.query(Product).where(Product.wkn == resolved_wkn)
        if resolved_isin:
            statement = self._session.query(Product).where((Product.wkn == resolved_wkn) | (Product.isin == resolved_isin))

        product = statement.one_or_none()
        if product is None:
            product = Product(wkn=resolved_wkn, isin=resolved_isin, name=payload.name)
            self._add_to_db(product)
            return ProductModel(id=product.id, wkn=product.wkn, isin=product.isin, name=product.name,
                                ticker=product.ticker)

        if not product.isin and resolved_isin:
            product.isin = resolved_isin
        if not product.name and payload.name:
            product.name = payload.name
        if product.wkn != resolved_wkn and not (
                resolved_isin
                and product.isin == resolved_isin
                and product.wkn
                and re.fullmatch(r"TR[0-9A-F]{4}", resolved_wkn) is not None
        ):
            product.wkn = resolved_wkn

        self._session.flush()
        return ProductModel(id=product.id, wkn=product.wkn, isin=product.isin, name=product.name, ticker=product.ticker)

    @transactional
    def update_ticker(self, payload: ProductTickerUpdateModel) -> None:
        row = self._session.get(Product, payload.product_id)
        if row is None:
            return
        if row.ticker == payload.ticker:
            return
        row.ticker = payload.ticker
        self._session.flush()
