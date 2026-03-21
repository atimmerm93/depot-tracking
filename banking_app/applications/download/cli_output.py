class DownloadingCliOutput:
    @staticmethod
    def print_error(message: str) -> None:
        print(f"[SELENIUM][ERROR] {message}")

    @staticmethod
    def print_summary(stats: dict[str, int]) -> None:
        print(
            "Selenium download summary: "
            f"found={stats['found']}, downloaded={stats['downloaded']}, "
            f"skipped={stats['skipped']}, errors={stats['errors']}"
        )
