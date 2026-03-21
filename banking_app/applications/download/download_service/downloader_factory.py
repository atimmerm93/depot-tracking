from __future__ import annotations

from banking_app.applications.download.download_service.consors.consors_selenium_downloader import \
    ConsorsSeleniumInboxDownloader
from banking_app.applications.download.download_service.ing.ing_selenium_downloader import IngSeleniumInboxDownloader
from .download_support import SeleniumDownloadSupport
from .selenium_shared import SeleniumDownloadConfig

SUPPORTED_DOWNLOADER_BANKS = ("ing", "consors")


class SeleniumInboxDownloaderFactory:
    def __init__(
            self,
            *,
            config: SeleniumDownloadConfig,
            support: SeleniumDownloadSupport,
    ) -> None:
        self._config = config
        self._support = support

    def build(self, bank: str) -> IngSeleniumInboxDownloader | ConsorsSeleniumInboxDownloader:
        selected_bank = (bank or "").strip().lower()
        if selected_bank not in SUPPORTED_DOWNLOADER_BANKS:
            raise ValueError(
                f"Unsupported bank '{bank}'. Expected one of: {', '.join(SUPPORTED_DOWNLOADER_BANKS)}"
            )
        if selected_bank == "ing":
            return IngSeleniumInboxDownloader(self._config, support=self._support)
        return ConsorsSeleniumInboxDownloader(self._config, support=self._support)

    def download_documents_for_bank(self, bank: str) -> dict[str, int]:
        return self.build(bank).run()
