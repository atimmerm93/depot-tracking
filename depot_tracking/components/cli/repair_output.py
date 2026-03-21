class RepairCliOutput:
    @staticmethod
    def print_infer_stats(stats: dict[str, int]) -> None:
        print(
            "Inferred buys summary: "
            f"snapshots={stats['snapshots']}, inferred={stats['inferred']}, "
            f"skipped={stats['skipped']}, errors={stats['errors']}"
        )

    @staticmethod
    def print_repair_stats(stats: dict[str, int]) -> None:
        print(
            "Repair summary: "
            f"applied={stats.get('applied', 0)}, "
            f"skipped={stats.get('skipped', 0)}, "
            f"errors={stats.get('errors', 0)}"
        )
