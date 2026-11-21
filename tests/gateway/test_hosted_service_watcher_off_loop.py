"""The hosted-room watcher must not read profile files on the gateway event loop.

``_hosted_room_worker_watcher`` calls ``_ensure_hosted_service`` for every serving authority once a
second. It used to enter the sync ``owner_scope``, which reads the profile ``.env`` and
``config.yaml`` on the loop thread; on a swapping host one read blocked the loop for minutes and the
liveness watchdog restarted the gateway (every worker then lost its owner).
"""

import asyncio
import threading
from types import SimpleNamespace

import pytest


@pytest.fixture
def profile_home(tmp_path):
    home = tmp_path / "profile"
    home.mkdir()
    (home / ".env").write_text("TERMINAL_ENV=local\n")
    (home / "config.yaml").write_text("terminal:\n  backend: local\n")
    return home


def _authority(home, service):
    runner = SimpleNamespace(config=SimpleNamespace(multiplex_profiles=True),
                             session_control_server=object())
    authority = SimpleNamespace(runner=runner, profile_id=str(home), hosted_room_service=service,
                                epoch=1, db=SimpleNamespace(db_path=str(home / "state.db")))
    return runner, authority


class _EnvReads:
    """Records which thread each ``load_env_file`` call ran on."""

    def __init__(self, monkeypatch, before=None):
        from agent import secret_scope

        self.threads = []
        real = secret_scope.load_env_file

        def wrapper(*args, **kwargs):
            self.threads.append(threading.get_ident())
            if before is not None:
                before()
            return real(*args, **kwargs)

        monkeypatch.setattr(secret_scope, "load_env_file", wrapper)

    def on_loop(self, loop_thread):
        return [t for t in self.threads if t == loop_thread]


@pytest.mark.asyncio
async def test_steady_state_tick_reads_nothing_on_loop(profile_home, monkeypatch):
    from gateway.session_hosted_service import _ensure_hosted_service

    runner, authority = _authority(profile_home, SimpleNamespace(_transport_installed=True))
    reads = _EnvReads(monkeypatch)
    for _ in range(10):
        await _ensure_hosted_service(runner, authority)
    assert reads.on_loop(threading.get_ident()) == [], "load_env_file called on loop thread"
    assert reads.threads == []


@pytest.mark.asyncio
async def test_loop_not_blocked_by_slow_env_read(profile_home, monkeypatch):
    """A profile read that stalls must stall a worker thread, not the loop.

    The read waits until the loop has ticked; on the loop thread the ticker cannot run, so the read
    times out and the observed loop gap is the full wait.
    """
    from gateway.session_hosted_service import _ensure_hosted_service

    loop = asyncio.get_running_loop()
    ticked = threading.Event()
    gaps = []

    def stall_until_loop_ticks():
        ticked.wait(timeout=5.0)

    runner, authority = _authority(profile_home, None)
    _EnvReads(monkeypatch, before=stall_until_loop_ticks)
    _stub_creation(monkeypatch)

    async def ticker():
        last = loop.time()
        for _ in range(5):
            await asyncio.sleep(0)
            now = loop.time()
            gaps.append(now - last)
            last = now
            ticked.set()

    started = asyncio.Event()

    async def run_ticker():
        started.set()
        await ticker()

    tick = asyncio.create_task(run_ticker())
    await started.wait()  # the ticker has taken its first timestamp before the read can stall the loop
    await _ensure_hosted_service(runner, authority)
    await tick
    assert max(gaps) < 1.0, f"event loop blocked for {max(gaps):.2f}s by a profile read"


def _stub_creation(monkeypatch, record=None):
    """Replace service construction/import/transport so only scope handling is under test."""
    import gateway.hosted_rooms_gateway_import as importer
    import gateway.session_hosted_service as hosted
    import gateway.session_hosted_transport as transport

    record = record if record is not None else {}

    class FakeService:
        _transport_installed = False

        def __init__(self, authority, loop):
            from hermes_constants import get_hermes_home

            record["service_home"] = get_hermes_home()

        def attest(self, *a, **k):
            return None

    monkeypatch.setattr(importer, "import_standalone_rooms", lambda *a, **k: None)
    monkeypatch.setattr(hosted, "CanonicalHostedRoomService", FakeService)

    def install(server, authority, loop, attest=None):
        from hermes_constants import get_hermes_home

        record["transport_home"] = get_hermes_home()

    monkeypatch.setattr(transport, "install_hosted_transport", install)
    return record


@pytest.mark.asyncio
async def test_first_creation_reads_off_loop_and_keeps_owner_scope(profile_home, monkeypatch):
    from gateway.session_hosted_service import _ensure_hosted_service

    runner, authority = _authority(profile_home, None)
    record = _stub_creation(monkeypatch)
    reads = _EnvReads(monkeypatch)

    await _ensure_hosted_service(runner, authority)

    assert reads.on_loop(threading.get_ident()) == [], "load_env_file called on loop thread"
    assert record["service_home"] == profile_home
    assert record["transport_home"] == profile_home
    assert authority.hosted_room_service._transport_installed is True
