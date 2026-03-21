from __future__ import annotations

from pathlib import Path

import pytest

from banking_app.components.ingestion.parsing.trade_republic_pdf_parser import TradeRepublicPdfTransactionParser
from banking_app.core.parser_errors import UnsupportedPdfDocument


TRADE_REPUBLIC_DEPOTAUSZUG_TEXT = """
Trade Republic Bank GmbH
DEPOTAUSZUG
zum 19.02.2026
POSITIONEN
STK. / NOMINALE WERTPAPIERBEZEICHNUNG KURS PRO STÜCK KURSWERT IN EUR
80,00 Stk. Vanguard FTSE All-World U.ETF
Reg. Shs USD Acc. oN
ISIN: IE00BK5BQT80
Wertpapierrechnung in Deutschland
Lagerland: Vereinigtes Königreich
150,16
19.02.2026
12.012,80
ANZAHL POSITIONEN: 1 12.012,8 EUR
"""


def test_parse_trade_republic_depotauszug_holdings(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = TradeRepublicPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: TRADE_REPUBLIC_DEPOTAUSZUG_TEXT)

    snapshot_date, holdings = parser.parse_depotauszug_holdings(Path("Wertpapiere.pdf"))

    assert snapshot_date.isoformat() == "2026-02-19"
    assert len(holdings) == 1
    assert holdings[0].wkn == "A2PKXG"
    assert holdings[0].isin == "IE00BK5BQT80"
    assert holdings[0].quantity == pytest.approx(80.0)
    assert holdings[0].snapshot_price == pytest.approx(150.16)


def test_parse_trade_republic_depotauszug_as_transaction_is_unsupported(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = TradeRepublicPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: TRADE_REPUBLIC_DEPOTAUSZUG_TEXT)

    with pytest.raises(UnsupportedPdfDocument):
        parser.parse(Path("Wertpapiere.pdf"))
