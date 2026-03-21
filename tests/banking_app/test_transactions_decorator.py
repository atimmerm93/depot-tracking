from dataclasses import dataclass

import pytest
from di_unit_of_work.session_aspect import SessionAspect
from di_unit_of_work.session_cache import SessionCache
from di_unit_of_work.transactional_decorator import transactional
from python_di_application.di_container import DIContainer, Dependency


@dataclass
class _SessionTracker:
    created: int = 0
    entered: int = 0
    exited: int = 0
    committed: int = 0
    rolled_back: int = 0


class _FakeSession:
    def __init__(self, tracker: _SessionTracker) -> None:
        self._tracker = tracker
        self._tracker.created += 1

    def __enter__(self) -> "_FakeSession":
        self._tracker.entered += 1
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._tracker.exited += 1
        return False

    def commit(self) -> None:
        self._tracker.committed += 1

    def rollback(self) -> None:
        self._tracker.rolled_back += 1


class _FakeSessionFactory:
    def __init__(self, tracker: _SessionTracker) -> None:
        self._tracker = tracker

    def __call__(self) -> _FakeSession:
        return _FakeSession(tracker=self._tracker)


class _ProbeService:
    def __init__(self, session_cache: SessionCache) -> None:
        self._session_cache = session_cache
        self.calls = 0

    @transactional
    def outer(self) -> str:
        assert self._session_cache.get_current_session() is not None
        self.calls += 1
        return self.inner()

    @transactional
    def inner(self) -> str:
        assert self._session_cache.get_current_session() is not None
        self.calls += 1
        return "ok"

    @transactional
    def fail(self) -> None:
        assert self._session_cache.get_current_session() is not None
        self.calls += 1
        raise RuntimeError("boom")


def _build_container(session_factory: _FakeSessionFactory) -> DIContainer:
    container = DIContainer()
    container.register_dependencies(
        dependencies_types_with_kwargs=[
            Dependency(dependency_type=SessionCache),
            Dependency(dependency_type=SessionAspect, session_factory=session_factory),
            Dependency(dependency_type=_ProbeService),
        ]
    )
    # Ensure instances exist before post-init wrapper application.
    container.resolve_dependency(dependency_type=SessionAspect)
    container.resolve_dependency(dependency_type=_ProbeService)
    container.apply_post_init_wrappers()
    return container


def test_transactions_decorator_opens_and_commits_when_no_active_session() -> None:
    tracker = _SessionTracker()
    container = _build_container(_FakeSessionFactory(tracker=tracker))

    service = container.resolve_dependency(dependency_type=_ProbeService)
    session_cache = container.resolve_dependency(dependency_type=SessionCache)

    result = service.outer()

    assert result == "ok"
    assert service.calls == 2
    assert tracker.created == 1
    assert tracker.entered == 1
    assert tracker.exited == 1
    assert tracker.committed == 1
    assert tracker.rolled_back == 0
    assert session_cache.get_current_session() is None


def test_transactions_decorator_reuses_existing_session_without_commit_or_rollback() -> None:
    tracker = _SessionTracker()
    container = _build_container(_FakeSessionFactory(tracker=tracker))

    service = container.resolve_dependency(dependency_type=_ProbeService)
    session_cache = container.resolve_dependency(dependency_type=SessionCache)

    existing_session = _FakeSession(tracker=tracker)
    token = session_cache.set_current_session(existing_session)
    try:
        result = service.outer()
    finally:
        session_cache.reset_to_token(token)

    assert result == "ok"
    assert service.calls == 2
    assert tracker.created == 1  # only the manually created session
    assert tracker.entered == 0
    assert tracker.exited == 0
    assert tracker.committed == 0
    assert tracker.rolled_back == 0


def test_transactions_decorator_rolls_back_on_error() -> None:
    tracker = _SessionTracker()
    container = _build_container(_FakeSessionFactory(tracker=tracker))

    service = container.resolve_dependency(dependency_type=_ProbeService)
    session_cache = container.resolve_dependency(dependency_type=SessionCache)

    with pytest.raises(RuntimeError, match="boom"):
        service.fail()

    assert service.calls == 1
    assert tracker.created == 1
    assert tracker.entered == 1
    assert tracker.exited == 1
    assert tracker.committed == 0
    assert tracker.rolled_back == 1
    assert session_cache.get_current_session() is None
