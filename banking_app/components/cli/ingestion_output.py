class IngestionCliOutput:
    @staticmethod
    def print_ingest_stats(stats: dict[str, int]) -> None:
        print(
            "Ingest summary: "
            f"seen={stats['seen']}, ingested={stats['ingested']}, "
            f"skipped={stats['skipped']}, errors={stats['errors']}"
        )

    @staticmethod
    def print_dedupe_stats(stats: dict[str, int]) -> None:
        print(
            "Dedup summary: "
            f"files_seen={stats['files_seen']}, files_removed={stats['files_removed']}, "
            f"tx_removed={stats['tx_removed']}, snapshots_removed={stats['snapshots_removed']}, "
            f"processed_removed={stats['processed_removed']}"
        )
