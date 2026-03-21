"""Top-level application entrypoints."""

from banking_app.applications.download.downloading_application import DownloadingApplication
from banking_app.applications.ingestion.ingestion_application import IngestionApplication
from banking_app.applications.portfolio.portfolio_application import PortfolioApplication
from banking_app.applications.repair.repair_application import RepairApplication
from banking_app.applications.workflow.workflow_application import WorkflowApplication

__all__ = ["DownloadingApplication", "IngestionApplication", "PortfolioApplication", "RepairApplication", "WorkflowApplication"]
