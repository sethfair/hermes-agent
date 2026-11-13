"""Worker persistence retries transport uncertainty without duplicating receipts."""
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

