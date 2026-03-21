from __future__ import annotations

import time
from pathlib import Path

from depot_tracking.applications.download.download_service.consors.consors_selenium_downloader import \
    ConsorsSeleniumInboxDownloader, \
    RowCandidate
from depot_tracking.applications.download.download_service.download_support import SeleniumDownloadSupport
from depot_tracking.applications.download.download_service.selenium_shared import SeleniumDownloadConfig


def test_extract_doc_type_recognizes_requested_consors_types(tmp_path: Path) -> None:
    downloader = ConsorsSeleniumInboxDownloader(
        SeleniumDownloadConfig(download_dir=tmp_path, debugger_address="127.0.0.1:9222"),
        support=SeleniumDownloadSupport(),
    )

    assert downloader._extract_doc_type("VERKAUF NVIDIA CORP 18.02.2026") == "verkauf"
    assert downloader._extract_doc_type("JAHRESDEPOTAUSZUG WERTPAPIERE 31.12.2025") in {
        "jahresdepotauszug",
        "jahresdepotauszug wertpapiere",
    }
    assert downloader._extract_doc_type("QUARTALSDEPOTAUSZUG WERTPAPIERE 30.09.2025") in {
        "quartalsdepotauszug",
        "quartalsdepotauszug wertpapiere",
    }
    assert downloader._extract_doc_type("KAUF VANG FTSE 09.04.2025") == "kauf"
    assert downloader._extract_doc_type("DIVIDENDENGUTSCHRIFT ASML 18.02.2026") == "dividendengutschrift"
    assert downloader._extract_doc_type("KONTOAUSZUG VERRECHNUNGSKONTO") is None


class _FakeControl:
    def __init__(self, *, text: str = "", attrs: dict[str, str] | None = None) -> None:
        self.text = text
        self._attrs = attrs or {}

    def get_attribute(self, name: str) -> str:
        return self._attrs.get(name, "")


def test_score_download_control_prefers_download_markers() -> None:
    control = _FakeControl(
        text="",
        attrs={
            "aria-label": "Download",
            "href": "/download?id=123",
            "class": "action-download",
        },
    )
    assert ConsorsSeleniumInboxDownloader._score_download_control(control) > 0


def test_score_download_control_rejects_archive_markers() -> None:
    control = _FakeControl(
        text="Archivieren",
        attrs={
            "aria-label": "Archivieren",
            "class": "action-archive",
        },
    )
    assert ConsorsSeleniumInboxDownloader._score_download_control(control) < 0


def test_score_download_control_ignores_empty_non_download_controls() -> None:
    control = _FakeControl(
        text="",
        attrs={
            "class": "checkbox-control",
            "aria-label": "",
            "href": "",
        },
    )
    assert ConsorsSeleniumInboxDownloader._score_download_control(control) == 0


def test_find_changed_pdf_detects_overwritten_existing_file(tmp_path: Path) -> None:
    downloader = ConsorsSeleniumInboxDownloader(
        SeleniumDownloadConfig(download_dir=tmp_path, debugger_address="127.0.0.1:9222"),
        support=SeleniumDownloadSupport(),
    )
    existing_file = tmp_path / "DIVIDENDENGUTSCHRIFT_foo.pdf"
    existing_file.write_bytes(b"%PDF-1.4\nold\n")
    before = downloader._snapshot_pdf_states()

    time.sleep(0.01)
    existing_file.write_bytes(b"%PDF-1.4\nnew-content\n")

    changed = downloader._find_changed_pdf(before)
    assert changed is not None
    assert changed.resolve() == existing_file.resolve()


def test_build_row_signature_ignores_ungelesen_marker() -> None:
    sig_a = ConsorsSeleniumInboxDownloader._build_row_signature(
        "dividendengutschrift",
        "12.05.2022 ASML HOLDING N.V. Ungelesen DIVIDENDENGUTSCHRIFT",
    )
    sig_b = ConsorsSeleniumInboxDownloader._build_row_signature(
        "dividendengutschrift",
        "12.05.2022 ASML HOLDING N.V. DIVIDENDENGUTSCHRIFT",
    )
    assert sig_a == sig_b


def test_build_row_text_snippet_normalizes_row() -> None:
    snippet = ConsorsSeleniumInboxDownloader._build_row_text_snippet(
        "12.05.2022   ASML HOLDING N.V.   Ungelesen   DIVIDENDENGUTSCHRIFT"
    )
    assert "ungelesen" not in snippet
    assert snippet.startswith("12.05.2022 asml holding n.v.")


def test_select_archive_handle_scoring_prefers_real_archive_tab(tmp_path: Path) -> None:
    downloader = ConsorsSeleniumInboxDownloader(
        SeleniumDownloadConfig(download_dir=tmp_path, debugger_address="127.0.0.1:9222"),
        support=SeleniumDownloadSupport(),
    )

    class _Body:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Switch:
        def __init__(self, drv) -> None:
            self._drv = drv

        def window(self, handle: str) -> None:
            self._drv._current = handle

    class _Driver:
        def __init__(self) -> None:
            self.window_handles = ["devtools", "view_source", "archive"]
            self._current = "devtools"
            self.switch_to = _Switch(self)
            self.data = {
                "devtools": {
                    "url": "devtools://devtools/bundled/devtools_app.html",
                    "title": "DevTools",
                    "body": "Elements Console",
                },
                "view_source": {
                    "url": "https://www.consorsbank.de/web/Mein-Konto-und-Depot/Online-Archiv/Kontobezogene-Dokumente",
                    "title": "",
                    "body": "Zeilenumbruch <!doctype html> <html lang='de'>",
                },
                "archive": {
                    "url": "https://www.consorsbank.de/web/Mein-Konto-und-Depot/Online-Archiv/Kontobezogene-Dokumente",
                    "title": "Online-Archiv | Consorsbank",
                    "body": "Willkommen Alexander Timmermann",
                },
            }

        @property
        def current_window_handle(self) -> str:
            return self._current

        @property
        def current_url(self) -> str:
            return self.data[self._current]["url"]

        @property
        def title(self) -> str:
            return self.data[self._current]["title"]

        def find_element(self, by, value):
            assert by is not None and value is not None
            return _Body(self.data[self._current]["body"])

    driver = _Driver()
    selected = downloader._select_archive_handle(driver)
    assert selected == "archive"


def test_extract_row_document_url_prefers_row_anchor(tmp_path: Path) -> None:
    downloader = ConsorsSeleniumInboxDownloader(
        SeleniumDownloadConfig(download_dir=tmp_path, debugger_address="127.0.0.1:9222"),
        support=SeleniumDownloadSupport(),
    )

    class _Anchor:
        def __init__(self, href: str, text: str) -> None:
            self._href = href
            self.text = text

        def get_attribute(self, name: str) -> str:
            if name == "href":
                return self._href
            return ""

    class _Row:
        def find_elements(self, by, value):
            assert by is not None and value is not None
            return [
                _Anchor(
                    "/web-document-service/api/users/x/documents/content/v1?documentId=abc",
                    "JAHRESDEPOTAUSZUG WERTPAPIERE PER 31.12.2025",
                ),
                _Anchor("/foo/bar", "Other"),
            ]

    class _Driver:
        current_url = "https://www.consorsbank.de/web/Mein-Konto-und-Depot/Online-Archiv/Kontobezogene-Dokumente"

        def find_elements(self, by, value):
            assert by is not None and value is not None
            return []

    row = RowCandidate(
        doc_type="jahresdepotauszug",
        signature="sig",
        row_text="31.12.2025 JAHRESDEPOTAUSZUG WERTPAPIERE PER 31.12.2025 JAHRESDEPOTAUSZUG WERTPAPIERE",
        row_element=_Row(),
    )
    url = downloader._extract_row_document_url(_Driver(), row)
    assert url is not None
    assert "web-document-service/api" in url
