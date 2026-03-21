from __future__ import annotations

from pathlib import Path

import pytest

from depot_tracking.components.ingestion.parsing.consors_pdf_parser import ConsorsPdfTransactionParser
from depot_tracking.core.models import TransactionType
from depot_tracking.core.parser_errors import UnsupportedPdfDocument

CONSORS_SELL_TEXT = """
Depotnummer DE191528929F
Wertpapierbezeichnung WKNISIN ADVANCEDMIC.DEV.DL-,01 863186 US0079031078
ORDERABRECHNUNGVERKAUF
Einheit Umsatz ST 0,23400
Kurswert 52,74 EUR
Kapitalertragssteuer 11,80 EUR
Solidaritätszuschlag 0,64 EUR
Datum:29.10.2025
"""

CONSORS_BUY_TEXT = """
Wertpapierbezeichnung WKNISIN VANG.FTSEDEV.W.U.ETFDLA A2PLS9 IE00BK5BQV03
ORDERABRECHNUNGKAUF
Einheit Umsatz ST 45,00000
Kurswert 3.988,80 EUR
Handelsplatzkosten 2,95 EUR
Provision 10,95 EUR
Grundgebühr 2,00 EUR
Datum:09.04.2025
"""

CONSORS_BUY_OLD_LAYOUT_TEXT = """
ORDERABRECHNUNG KAUF
Wertpapierbezeichnung WKNISIN BAYER AG NA O.N. BAY001 DE000BAY0017
Einheit Umsatz ST 13,00000
Kurswert EUR 1.508,24
Provision EUR 5,00
Grundgebühr EUR 4,95
Eig. Spesen EUR 1,95
Wert 16.01.2017 EUR 1.520,14 zulasten Konto-Nr. 8258975005
"""

CONSORS_SELL_OLD_LAYOUT_TEXT = """
ORDERABRECHNUNG VERKAUF
Wertpapierbezeichnung WKNISIN ETFS OIL SEC.DZ06/UN.OIL B A0KRKM DE000A0KRKM3
Einheit Umsatz ST 70,00000
Kurswert EUR 1.631,07
Provision EUR 5,00
Grundgebühr EUR 4,95
Wert 27.12.2016 EUR 1.621,12 zugunsten Konto-Nr. 8258975005
"""

CONSORS_ERTRAG_TEXT = """
Depotnummer DE191528929F
Wertpapierbezeichnung WKNISIN NVIDIACORP.RegisteredSharesDL-,001 918422 US67066G1040
DIVIDENDENGUTSCHRIFT
BruttoinEUR 0,09EUR
QuellensteuerinEUR 0,02EUR
Datum:08.01.2026
"""

CONSORS_ERTRAG_OLD_EUR_TEXT = """
Depotnummer: 0448609135
Dividendengutschrift
Wertpapierbezeichnung WKN ISIN
JUNGHEINRICH AG INHABER-VORZUGSAKT.O.ST.O.N. 621993 DE0006219934
Bestand 60 Stück
Dividende pro Stück 0,48 EUR
Schlusstag 27.08.2020
Brutto 28,80 EUR
Netto zugunsten IBAN DE76701204008258975005 28,80 EUR
Valuta 01.09.2020
"""

CONSORS_ERTRAG_OLD_FX_TEXT = """
DIVIDENDENGUTSCHRIFT
ST 30,00000 WKN: 855681 INTEL CORP.
ZINS-/DIVIDENDENSATZ 0,272500 USD
SCHLUSSTAG PER 05.11.2017 EX-TAG 06.11.2017
BRUTTO USD 8,18
QUST 15,00000 % EUR 1,03
USD 1,23 USD 6,95
UMGER.ZUM DEV.-KURS 1,193200 EUR 5,82
WERT 01.12.2017
ZU GUNSTEN KONTO-NR. 8258 975 005 / IBAN DE76701204008258975005
ANRECHENBARE AUSLAEND. QUELLENSTEUER EUR 1,03
KAPST-PFLICHTIGER KAPITALERTRAG EUR 6,86
"""

CONSORS_DEPOTAUSZUG_TEXT = """
Jahresdepotauszug Wertpapiere per 31.12.2025
ST10,00NVIDIACORP.RegisteredSharesDL-,001GirosammelverwahrungDeutschland918422160,36EUR1,001.603,60
ST9,23400ADVANCEDMICRODEVICESINC.RegisteredSharesDL-,01GirosammelverwahrungDeutschland863186136,84EUR1,001.263,16
ANZAHLPOSTEN
"""

CONSORS_DEPOTAUSZUG_OLD_LAYOUT_TEXT = """
WERTPAPIER-JAHRESDEPOTAUSZUG per 31.12.2019
ST60,00JUNGHEINRICHAG21,58EUR1.294,80INHABER-VORZUGSAKT.O.ST.O.N.6219931,00Girosammelverwahrung
ST42,00ADVANCEDMICRODEVICESINC.41,215EUR1.731,03RegisteredSharesDL-,018631861,00Girosammelverwahrung
ST10,00NVIDIACORP.210,95EUR2.109,50RegisteredSharesDL-,0019184221,00Girosammelverwahrung
ANZAHLPOSTEN5GESAMTKURSWERTEUR7.361,43
"""

CONSORS_DEPOTAUSZUG_COMPACT_QUARTERLY_TEXT = """
QUARTALSDEPOTAUSZUG WERTPAPIERE per 31.12.2018
ST30,000,00000INTELCORP.RegisteredSharesDL-,00185568140,160000EUR1,001.204,80
ST34,000,00000ADVANCEDMICRODEVICESINC.RegisteredSharesDL-,0186318615,460000EUR1,00525,64
ST10,00NVIDIACORP.RegisteredSharesDL-,001918422132,30USD1,1451.155,46
ANZAHLPOSTEN3GESAMTKURSWERTEUR2.885,90
"""

CONSORS_DEPOTAUSZUG_EMPTY_TEXT = """
Seite Jahresdepotauszug per 31.12.2016 Depot 0448609135 1
Währung Anzahl/Nennwert Zinssatz % Wertpapier Kennnummer Wertpapierkurs Kurswert Verwahrart Devisenkurs in EUR
Zum angegebenen Stichtag waren für Sie keine Bestände verbucht.
"""


def test_parse_consors_sell_uses_isin_near_wkn(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_SELL_TEXT)

    tx = parser.parse(Path("VERKAUF_448609135_ord349930432_001_wkn863186_dat20251029_id1.pdf"))

    assert tx.tx_type == TransactionType.SELL
    assert tx.wkn == "863186"
    assert tx.isin == "US0079031078"
    assert tx.transaction_date.isoformat() == "2025-10-29"
    assert tx.quantity == pytest.approx(0.234)
    assert tx.gross_amount == pytest.approx(52.74)
    assert tx.costs == pytest.approx(12.44)


def test_parse_consors_buy(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_BUY_TEXT)

    tx = parser.parse(Path("KAUF_448609135_ord329952789_001_wknA2PLS9_dat20250409_id1.pdf"))

    assert tx.tx_type == TransactionType.BUY
    assert tx.wkn == "A2PLS9"
    assert tx.isin == "IE00BK5BQV03"
    assert tx.transaction_date.isoformat() == "2025-04-09"
    assert tx.quantity == pytest.approx(45.0)
    assert tx.gross_amount == pytest.approx(3988.8)
    assert tx.costs == pytest.approx(15.9)


def test_parse_consors_buy_old_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_BUY_OLD_LAYOUT_TEXT)

    tx = parser.parse(Path("KAUF_448609135_ord106248808_001_wknBAY001_dat20170112_id1.pdf"))

    assert tx.tx_type == TransactionType.BUY
    assert tx.wkn == "BAY001"
    assert tx.isin == "DE000BAY0017"
    assert tx.transaction_date.isoformat() == "2017-01-12"
    assert tx.quantity == pytest.approx(13.0)
    assert tx.gross_amount == pytest.approx(1508.24)
    assert tx.costs == pytest.approx(11.90)


def test_parse_consors_sell_old_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_SELL_OLD_LAYOUT_TEXT)

    tx = parser.parse(Path("VERKAUF_448609135_ord105535355_001_wknA0KRKM_dat20161222_id1.pdf"))

    assert tx.tx_type == TransactionType.SELL
    assert tx.wkn == "A0KRKM"
    assert tx.isin == "DE000A0KRKM3"
    assert tx.transaction_date.isoformat() == "2016-12-22"
    assert tx.quantity == pytest.approx(70.0)
    assert tx.gross_amount == pytest.approx(1631.07)
    assert tx.costs == pytest.approx(9.95)


def test_parse_consors_dividend(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_ERTRAG_TEXT)

    tx = parser.parse(Path("DIVIDENDENGUTSCHRIFT_448609135_wkn918422_dat20260108_id1.pdf"))

    assert tx.tx_type == TransactionType.ERTRAGSABRECHNUNG
    assert tx.wkn == "918422"
    assert tx.isin == "US67066G1040"
    assert tx.transaction_date.isoformat() == "2026-01-08"
    assert tx.quantity == pytest.approx(0.0)
    assert tx.gross_amount == pytest.approx(0.09)
    assert tx.costs == pytest.approx(0.02)


def test_parse_consors_dividend_old_eur_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_ERTRAG_OLD_EUR_TEXT)

    tx = parser.parse(Path("DIVIDENDENGUTSCHRIFT_448609135_wkn621993_dat20200901_id1.pdf"))

    assert tx.tx_type == TransactionType.ERTRAGSABRECHNUNG
    assert tx.wkn == "621993"
    assert tx.isin == "DE0006219934"
    assert tx.gross_amount == pytest.approx(28.80)
    assert tx.costs == pytest.approx(0.0)


def test_parse_consors_dividend_old_fx_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_ERTRAG_OLD_FX_TEXT)

    tx = parser.parse(Path("DIVIDENDENGUTSCHRIFT_448609135_wkn855681_dat20171201_id1.pdf"))

    assert tx.tx_type == TransactionType.ERTRAGSABRECHNUNG
    assert tx.wkn == "855681"
    assert tx.isin is None
    assert tx.gross_amount == pytest.approx(6.86)
    assert tx.costs == pytest.approx(1.03)


def test_parse_consors_depotauszug_holdings(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_DEPOTAUSZUG_TEXT)

    snapshot_date, holdings = parser.parse_depotauszug_holdings(
        Path("JAHRESDEPOTAUSZUG_WERTPAPIERE_448609135_dat20251231_id1.pdf")
    )

    assert snapshot_date.isoformat() == "2025-12-31"
    assert len(holdings) == 2
    assert holdings[0].wkn == "918422"
    assert holdings[0].quantity == pytest.approx(10.0)
    assert holdings[0].snapshot_price == pytest.approx(160.36)
    assert holdings[1].wkn == "863186"
    assert holdings[1].quantity == pytest.approx(9.234)
    assert holdings[1].snapshot_price == pytest.approx(136.84)


def test_parse_consors_depotauszug_holdings_old_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_DEPOTAUSZUG_OLD_LAYOUT_TEXT)

    snapshot_date, holdings = parser.parse_depotauszug_holdings(
        Path("WERTPAPIER-JAHRESDEPOTAUSZUG_448609135_dat20191231_id832771734.pdf")
    )

    assert snapshot_date.isoformat() == "2019-12-31"
    assert len(holdings) == 3
    assert holdings[0].wkn == "621993"
    assert holdings[0].quantity == pytest.approx(60.0)
    assert holdings[0].snapshot_price == pytest.approx(21.58)
    assert holdings[1].wkn == "863186"
    assert holdings[1].quantity == pytest.approx(42.0)
    assert holdings[1].snapshot_price == pytest.approx(41.215)
    assert holdings[2].wkn == "918422"
    assert holdings[2].quantity == pytest.approx(10.0)
    assert holdings[2].snapshot_price == pytest.approx(210.95)


def test_parse_consors_depotauszug_holdings_compact_quarterly_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_DEPOTAUSZUG_COMPACT_QUARTERLY_TEXT)

    snapshot_date, holdings = parser.parse_depotauszug_holdings(
        Path("QUARTALSDEPOTAUSZUG_WERTPAPIERE_448609135_dat20181231_id748888859.pdf")
    )

    assert snapshot_date.isoformat() == "2018-12-31"
    assert len(holdings) == 3
    assert holdings[0].wkn == "855681"
    assert holdings[0].quantity == pytest.approx(30.0)
    assert holdings[0].snapshot_price == pytest.approx(40.16)
    assert holdings[1].wkn == "863186"
    assert holdings[1].quantity == pytest.approx(34.0)
    assert holdings[1].snapshot_price == pytest.approx(15.46)
    assert holdings[2].wkn == "918422"
    assert holdings[2].quantity == pytest.approx(10.0)
    assert holdings[2].snapshot_price == pytest.approx(115.546)


def test_parse_consors_depotauszug_holdings_empty_returns_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: CONSORS_DEPOTAUSZUG_EMPTY_TEXT)

    snapshot_date, holdings = parser.parse_depotauszug_holdings(
        Path("WERTPAPIER-JAHRESDEPOTAUSZUG_448609135_dat20161231_id599737075.pdf")
    )

    assert snapshot_date.isoformat() == "2016-12-31"
    assert holdings == []


def test_parse_consors_unsupported_document(monkeypatch: pytest.MonkeyPatch) -> None:
    parser = ConsorsPdfTransactionParser()
    monkeypatch.setattr(parser, "_extract_text", lambda _path: "KONTOAUSZUG VERRECHNUNGSKONTO")

    with pytest.raises(UnsupportedPdfDocument):
        parser.parse(Path("KONTOAUSZUG_VERRECHNUNGSKONTO_448609135_dat20260131_id1.pdf"))
