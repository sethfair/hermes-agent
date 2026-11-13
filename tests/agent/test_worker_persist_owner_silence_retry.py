"""Worker persistence retries transport uncertainty without duplicating receipts."""
import json
import threading

import pytest


class RetryRPC:
    def __init__(self):
        self.calls = []

    def __call__(self, method, **params):
        self.calls.append((method, params))
        if len(self.calls) == 1:
            raise TimeoutError("worker_receipt_timeout")
        return {"value": "committed"}


def make_store(tmp_path, rpc):
    from agent.runtime_session_store import RuntimeSessionStore

    return RuntimeSessionStore(
        rpc,
        {"execution_id": "exec", "session_id": "session", "epoch": 1, "generation": 1},
        tmp_path,
    )


@pytest.mark.parametrize("failure", [TimeoutError(), TimeoutError("timed out while waiting for handshake response"), ConnectionRefusedError("refused")])
def test_redial_transport_errors_retry_same_receipt(tmp_path, monkeypatch, failure):
    import agent.runtime_session_store as module

    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 8.0)
    monkeypatch.setattr(module, "_sleep_or_cancel", lambda cancel, delay: None)
    monkeypatch.setattr(module, "_monotonic", iter([0.0, 0.0, 0.1, 0.1]).__next__)

    class RPC:
        def __init__(self):
            self.calls = []

        def __call__(self, method, **params):
            self.calls.append((method, params))
            if len(self.calls) == 1:
                raise failure
            return {"value": "committed"}

    rpc = RPC()
    store = make_store(tmp_path, rpc)
    assert store._apply("operation", {"payload": "same"}) == {"value": "committed"}
    assert len(rpc.calls) == 2
    assert [call[1]["sequence"] for call in rpc.calls] == [1, 1]
    assert [call[1]["payload"] for call in rpc.calls] == [{"payload": "same"}] * 2


def test_transport_timeout_retries_same_receipt(tmp_path, monkeypatch):
    import agent.runtime_session_store as module

    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 8.0)
    monkeypatch.setattr(module, "_sleep_or_cancel", lambda cancel, delay: None)
    import itertools
    monkeypatch.setattr(module, "_monotonic", itertools.chain([0.0, 0.0, 0.1], itertools.repeat(0.1)).__next__)
    rpc = RetryRPC()
    store = make_store(tmp_path, rpc)

    assert store._apply("operation", {"payload": "same"}) == {"value": "committed"}
    assert store.failure is None
    assert store.journal["pending"] == []
    assert [call[1]["sequence"] for call in rpc.calls] == [1, 1]
    assert [call[1]["payload"] for call in rpc.calls] == [{"payload": "same"}] * 2


def test_unknown_persistence_error_is_fatal(tmp_path, monkeypatch):
    from agent.runtime_session_store import RuntimeSessionStore, WorkerPersistenceError

    class RPC:
        def __init__(self):
            self.calls = 0

        def __call__(self, method, **params):
            self.calls += 1
            raise WorkerPersistenceError("stale_epoch")

    rpc = RPC()
    store = make_store(tmp_path, rpc)
    with pytest.raises(WorkerPersistenceError, match="stale_epoch"):
        store._apply("operation", {})
    assert rpc.calls == 1
    assert store.journal["pending"]


@pytest.mark.parametrize("reason", ["stale_epoch", "stale_generation", "admission_conflict", "permission_denied"])
def test_owner_fence_errors_are_fatal_without_retry(tmp_path, reason):
    from agent.runtime_session_store import WorkerPersistenceError

    class RPC:
        calls = 0

        def __call__(self, method, **params):
            self.calls += 1
            raise WorkerPersistenceError(reason)

    rpc = RPC()
    store = make_store(tmp_path, rpc)
    with pytest.raises(WorkerPersistenceError, match=reason):
        store._apply("operation", {})
    assert rpc.calls == 1
    assert store.journal["pending"]


def test_timeout_then_fence_error_stops_on_fence(tmp_path, monkeypatch):
    from agent.runtime_session_store import WorkerPersistenceError
    import agent.runtime_session_store as module

    monkeypatch.setattr(module, "_sleep_or_cancel", lambda cancel, delay: None)
    monkeypatch.setattr(module, "_monotonic", iter([0.0, 0.1, 0.2, 0.3]).__next__)

    class RPC:
        calls = 0

        def __call__(self, method, **params):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("worker_receipt_timeout")
            raise WorkerPersistenceError("stale_epoch")

    rpc = RPC()
    store = make_store(tmp_path, rpc)
    with pytest.raises(WorkerPersistenceError, match="stale_epoch"):
        store._apply("operation", {})
    assert rpc.calls == 2
    assert store.journal["pending"]


def test_budget_exhaustion_keeps_last_transport_error_and_journal(tmp_path, monkeypatch):
    import agent.runtime_session_store as module

    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 25.0)
    monkeypatch.setattr(module, "_sleep_or_cancel", lambda cancel, delay: None)
    monkeypatch.setattr(module, "_monotonic", iter([0.0, 10.0, 20.0, 30.0]).__next__)

    class RPC:
        calls = 0

        def __call__(self, method, **params):
            self.calls += 1
            raise TimeoutError("handshake timeout")

    rpc = RPC()
    store = make_store(tmp_path, rpc)
    with pytest.raises(TimeoutError, match="handshake timeout"):
        store._apply("operation", {})
    assert rpc.calls == 2
    assert store.failure == "handshake timeout"
    assert store.journal["pending"]


def test_startup_replay_uses_stricter_budget(tmp_path, monkeypatch):
    import agent.runtime_session_store as module
    from agent.runtime_session_store import RuntimeSessionStore

    scope = {"execution_id": "exec", "session_id": "session", "epoch": 1, "generation": 1}
    outbox = tmp_path / "outbox"
    outbox.mkdir()
    (outbox / "pending.json").write_text(json.dumps({
        "scope": scope,
        "next_sequence": 2,
        "pending": [{"sequence": 1, "operation": "operation", "payload": {}}],
    }))
    monkeypatch.setattr(module, "STARTUP_REPLAY_BUDGET_S", 3.0)
    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 180.0)
    monkeypatch.setattr(module, "_sleep_or_cancel", lambda cancel, delay: None)
    monkeypatch.setattr(module, "_monotonic", iter([0.0, 2.0, 4.0, 5.0]).__next__)

    class RPC:
        calls = 0

        def __call__(self, method, **params):
            self.calls += 1
            raise TimeoutError("worker_receipt_timeout")

    rpc = RPC()
    with pytest.raises(TimeoutError, match="worker_receipt_timeout"):
        RuntimeSessionStore(rpc, scope, outbox)
    assert rpc.calls == 1


def test_owner_silence_budget_matches_kanban_wait_budget(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HOME", str(tmp_path))
    from agent.runtime_session_store import OWNER_SILENCE_BUDGET_S
    from hermes_cli.kanban_worker_client import OWNER_WAIT_S

    assert OWNER_SILENCE_BUDGET_S == OWNER_WAIT_S


def test_cancelled_retry_preserves_pending_entry(tmp_path, monkeypatch):
    import agent.runtime_session_store as module
    from agent.runtime_session_store import WorkerPersistenceError

    cancel = threading.Event()

    class RPC:
        def __init__(self):
            self.calls = 0

        def __call__(self, method, **params):
            self.calls += 1
            cancel.set()
            raise TimeoutError("worker_receipt_timeout")

    monkeypatch.setattr(module, "_sleep_or_cancel", lambda event, delay: event.wait(0))
    rpc = RPC()
    store = make_store(tmp_path, rpc)
    store.cancel = cancel
    with pytest.raises(WorkerPersistenceError, match="worker_cancelled"):
        store._apply("operation", {})
    assert rpc.calls == 1
    assert store.journal["pending"]

