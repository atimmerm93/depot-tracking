from __future__ import annotations

from pathlib import Path

import pytest

import banking_app.applications.download.download_service.downloader_factory as downloader_factory
from banking_app.applications.download.download_service.download_support import SeleniumDownloadSupport
from banking_app.applications.download.download_service.selenium_shared import SeleniumDownloadConfig


@pytest.fixture
def config(tmp_path: Path) -> SeleniumDownloadConfig:
    return SeleniumDownloadConfig(debugger_address="127.0.0.1:9222", download_dir=tmp_path)


def test_build_routes_to_ing(config: SeleniumDownloadConfig) -> None:
    factory = downloader_factory.SeleniumInboxDownloaderFactory(
        config=config,
        support=SeleniumDownloadSupport(),
    )

    downloader = factory.build("ing")

    assert isinstance(downloader, downloader_factory.IngSeleniumInboxDownloader)
    assert downloader.config is config


def test_build_routes_to_consors(config: SeleniumDownloadConfig) -> None:
    factory = downloader_factory.SeleniumInboxDownloaderFactory(
        config=config,
        support=SeleniumDownloadSupport(),
    )

    downloader = factory.build("consors")

    assert isinstance(downloader, downloader_factory.ConsorsSeleniumInboxDownloader)
    assert downloader.config is config


def test_build_rejects_unknown(config: SeleniumDownloadConfig) -> None:
    factory = downloader_factory.SeleniumInboxDownloaderFactory(
        config=config,
        support=SeleniumDownloadSupport(),
    )

    with pytest.raises(ValueError):
        factory.build("foo")
