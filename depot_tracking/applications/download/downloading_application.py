import argparse
from pathlib import Path

from python_di_application.application import Application
from python_di_application.di_container import DIContainer, Dependency

from depot_tracking.applications.di import resolve_application
from depot_tracking.applications.download.cli_output import DownloadingCliOutput
from depot_tracking.applications.download.download_service.download_support import SeleniumDownloadSupport
from depot_tracking.applications.download.download_service.selenium_shared import SeleniumDownloadConfig
from depot_tracking.applications.download.gateway import SeleniumDownloaderGateway


class DownloadingApplication(Application):
    def __init__(self, downloader: SeleniumDownloaderGateway, output: DownloadingCliOutput) -> None:
        self._downloader = downloader
        self._output = output

    @classmethod
    def _default_container(cls) -> DIContainer:
        container = DIContainer()
        container.register_dependencies(
            dependencies_types_with_kwargs=[
                Dependency(dependency_type=SeleniumDownloaderGateway),
                Dependency(dependency_type=SeleniumDownloadSupport),
                Dependency(dependency_type=DownloadingCliOutput),
                Dependency(dependency_type=cls),
            ]
        )
        return container

    @classmethod
    def _build(cls, container: DIContainer) -> tuple[DIContainer, "DownloadingApplication"]:
        return resolve_application(container, cls)

    def run(self, args: argparse.Namespace) -> int:
        if args.command != "download-docs":
            raise SystemExit("Unsupported command")

        config = SeleniumDownloadConfig(
            debugger_address=args.debugger_address,
            download_dir=Path(args.download_dir),
            state_file=Path(args.state_file) if args.state_file else None,
            reset_state=args.reset_state,
            max_documents=args.max_documents,
        )
        try:
            stats = self._downloader.download_documents_for_bank(args.bank, config)
        except ValueError as exc:
            self._output.print_error(str(exc))
            return 1
        except RuntimeError as exc:
            self._output.print_error(str(exc))
            return 1

        self._output.print_summary(stats)
        return 0 if stats["errors"] == 0 else 1
