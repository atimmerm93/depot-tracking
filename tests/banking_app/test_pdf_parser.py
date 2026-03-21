from __future__ import annotations

from pathlib import Path

import pytest

from banking_app.core.models import TransactionType
from banking_app.components.ingestion.parsing.pdf_parser import INGPdfParser, UnsupportedPdfDocument


BUY_TEXT = """
Wertpapierabrechnung Kauf aus Sparplan
Ordernummer 445273212.001
ISIN (WKN) IE00B5L8K969 (A1C1H5)
Wertpapierbezeichnung iShs VII-MSCI EM Asia U.ETF
Reg. Shares USD (Acc) o.N.
Nominale Stück 4,71143
Kurs EUR 212,25
Ausführungstag / -zeit 02.02.2026 um 09:10:24 Uhr
Kurswert EUR 1.000,00
Endbetrag zu Ihren Lasten EUR 1.000,00
Valuta 04.02.2026
"""


ERTRAGS_TEXT = """
Vorabpauschale
ISIN (WKN) IE00BK5BQT80 (A2PKXG)
Wertpapierbezeichnung Vanguard FTSE All-World U.ETF
Reg. Shs USD Acc. oN
Nominale 525,63629 Stück
Ex-Tag 02.01.2026
Gesamtbetrag zu Ihren Lasten EUR - 216,21
Valuta 02.01.2026
"""


UNKNOWN_TEXT = """
Depotinformation
ISIN (WKN) IE00BK5BQT80 (A2PKXG)
Nominale 100,00 Stück
"""

FILENAME_FALLBACK_TEXT = """
ISIN (WKN) CH0008680370 (900998)
Wertpapierbezeichnung iShares Example ETF
Nominale 0,10234 Stück
Valuta 25.03.2020
Gesamtbetrag zu Ihren Lasten EUR - 5,78
"""

DEPOTAUSZUG_TEXT = """
Depotauszug per 30.09.2024
35 Stück Advanced Micro Devices Inc. 147,00 EUR 5.145,00 EUR
Registered Shares DL -,01
ISIN (WKN): US0079031078 (863186)

91,19769 Stück iShs-MSCI World UCITS ETF 71,05 EUR 6.479,60 EUR
Registered Shares USD (Dist)oN
ISIN (WKN): IE00B0M62Q58 (A0HGV0)
"""


def test_parse_buy_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = INGPdfParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: BUY_TEXT)

    tx = parser.parse(Path("sample_buy_20260203.pdf"))

    assert tx.tx_type == TransactionType.BUY
    assert tx.wkn == "A1C1H5"
    assert tx.isin == "IE00B5L8K969"
    assert tx.quantity == pytest.approx(4.71143)
    assert tx.gross_amount == pytest.approx(1000.0)
    assert tx.costs == pytest.approx(0.0)


def test_parse_ertragsabrechnung_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = INGPdfParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: ERTRAGS_TEXT)

    tx = parser.parse(Path("sample_ertrags_20260119.pdf"))

    assert tx.tx_type == TransactionType.ERTRAGSABRECHNUNG
    assert tx.wkn == "A2PKXG"
    assert tx.isin == "IE00BK5BQT80"
    assert tx.quantity == pytest.approx(525.63629)
    assert tx.gross_amount == pytest.approx(-216.21)
    assert tx.costs == pytest.approx(0.0)


def test_parse_unsupported_document_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = INGPdfParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: UNKNOWN_TEXT)

    with pytest.raises(UnsupportedPdfDocument):
        parser.parse(Path("unsupported.pdf"))


def test_parse_ertragsabrechnung_detected_from_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = INGPdfParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: FILENAME_FALLBACK_TEXT)

    tx = parser.parse(Path("Direkt_Depot_8013529518_Ertragsabrechnung_CH0008680370_20250409.pdf"))

    assert tx.tx_type == TransactionType.ERTRAGSABRECHNUNG


def test_parse_depotauszug_holdings(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = INGPdfParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: DEPOTAUSZUG_TEXT)

    snapshot_date, holdings = parser.parse_depotauszug_holdings(Path("Direkt_Depot_8013529518_Depotauszug_20241015.pdf"))

    assert snapshot_date.isoformat() == "2024-09-30"
    assert len(holdings) == 2
    assert holdings[0].wkn == "863186"
    assert holdings[0].quantity == pytest.approx(35.0)
    assert holdings[0].snapshot_price == pytest.approx(147.0)
    assert holdings[1].wkn == "A0HGV0"
    assert holdings[1].quantity == pytest.approx(91.19769)
    assert holdings[1].snapshot_price == pytest.approx(71.05)
