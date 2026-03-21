from .download_service import SeleniumDownloadConfig, SeleniumDownloadSupport, SeleniumInboxDownloaderFactory


class SeleniumDownloaderGateway:
    def __init__(self, support: SeleniumDownloadSupport) -> None:
        self._support = support

    def download_documents_for_bank(self, bank: str, config: SeleniumDownloadConfig) -> dict[str, int]:
        factory = SeleniumInboxDownloaderFactory(config=config, support=self._support)
        return factory.download_documents_for_bank(bank)
