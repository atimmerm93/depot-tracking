from __future__ import annotations

from pathlib import Path

import depot_tracking.applications.download.gateway as download_gateway
from depot_tracking.applications.cli import main


def test_cli_main_init_db_uses_di_overridden_db_path(tmp_path: Path) -> None:
    db_path = tmp_path / "di_app.sqlite"

    exit_code = main(["--db-path", str(db_path), "init-db"])

    assert exit_code == 0
    assert db_path.exists()


def test_cli_main_download_docs_routes_via_di_gateway(monkeypatch, tmp_path: Path) -> None:
    called: dict[str, object] = {}

    class _FakeDownloaderFactory:
        def __init__(self, *, config, support) -> None:
            called["download_dir"] = config.download_dir
            called["support"] = support

        def download_documents_for_bank(self, bank: str) -> dict[str, int]:
            called["bank"] = bank
            return {"found": 2, "downloaded": 2, "skipped": 0, "errors": 0}

    monkeypatch.setattr(download_gateway, "SeleniumInboxDownloaderFactory", _FakeDownloaderFactory)

    exit_code = main(
        [
            "--db-path",
            str(tmp_path / "unused.sqlite"),
            "download-docs",
            "--bank",
            "ing",
            "--download-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 0
    assert called["bank"] == "ing"
    assert called["download_dir"] == tmp_path
