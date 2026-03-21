from pathlib import Path

from di_unit_of_work.session_factory.sqlite_session_factory import SqlLiteConfig, SQLiteSessionFactory
from sqlalchemy import text

from depot_tracking.core.db import initialize_database
from depot_tracking.core.models import Base


def test_sqlite_session_factory_creates_working_session(tmp_path: Path) -> None:
    db_path = tmp_path / "session_factory.sqlite"
    initialize_database(db_path)

    session_factory = SQLiteSessionFactory(SqlLiteConfig(path=str(db_path), metadata=Base.metadata))

    with session_factory() as session:
        row = session.execute(text("SELECT 1")).first()

    assert row is not None
    assert row[0] == 1
