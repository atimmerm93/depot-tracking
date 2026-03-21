from pathlib import Path

from di_unit_of_work.session_aspect import SessionAspect
from di_unit_of_work.session_cache import SessionCache
from di_unit_of_work.session_factory.sqlite_session_factory import SqlLiteConfig, SQLiteSessionFactory
from di_unit_of_work.session_provider import SessionProvider
from python_di_application.application import Application
from python_di_application.di_container import DIContainer, Dependency

from banking_app.components.shared import (
    BankClassifier,
    CalendarMonthService,
    IdentifierCanonicalizer,
    RepairRulesLoader,
    SourceDocumentNormalizer,
)
from banking_app.config import BankingAppConfig, ParserConfig
from banking_app.core.models import Base


def register_session_dependencies(container: DIContainer) -> None:
    container.register_dependencies(
        dependencies_types_with_kwargs=[
            Dependency(dependency_type=SessionCache),
            Dependency(dependency_type=SessionAspect),
            Dependency(dependency_type=SessionProvider),
            Dependency(dependency_type=SQLiteSessionFactory),
        ]
    )


def register_default_instances(container: DIContainer) -> None:
    container.register_instance(instance_obj=BankingAppConfig(db_path=Path("banking.sqlite")))
    container.register_instance(instance_obj=SqlLiteConfig(path="banking.sqlite", metadata=Base.metadata))
    container.register_instance(instance_obj=ParserConfig(bank_hint="auto"))


def register_shared_dependencies(container: DIContainer) -> None:
    container.register_dependencies(
        dependencies_types_with_kwargs=[
            Dependency(dependency_type=IdentifierCanonicalizer),
            Dependency(dependency_type=BankClassifier),
            Dependency(dependency_type=SourceDocumentNormalizer),
            Dependency(dependency_type=CalendarMonthService),
            Dependency(dependency_type=RepairRulesLoader),
        ]
    )


def resolve_application[T: Application](container: DIContainer, app_cls: type[T]) -> tuple[DIContainer, T]:
    return container, container.resolve_dependency(dependency_type=app_cls)
