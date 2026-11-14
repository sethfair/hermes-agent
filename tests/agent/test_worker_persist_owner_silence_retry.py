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
    from pathlib import Path
    import sys
    import types
    real_is_file = Path.is_file
    monkeypatch.setattr(Path, "is_file", lambda path: False
                        if "hermes-update-pull" in str(path) else real_is_file(path))
    monkeypatch.setitem(sys.modules, "hermes_bootstrap", types.ModuleType("hermes_bootstrap"))
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


def test_cancel_during_real_receive_returns_without_waiting(tmp_path):
    from agent.runtime_session_store import _OwnerLink, WorkerPersistenceError
    import time
    release = threading.Event()

    class Socket:
        def recv(self):
            release.wait()

    link = _OwnerLink(Socket())
    cancel = threading.Event()
    timer = threading.Timer(0.05, cancel.set)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(WorkerPersistenceError, match="worker_cancelled"):
            link.receive(5, cancel=cancel)
    finally:
        timer.cancel()
        release.set()
    assert time.monotonic() - started < 1


def test_cancel_during_backoff_stops_retry_and_preserves_first_attempt(tmp_path, monkeypatch):
    import time
    from agent import retry_utils
    from agent.runtime_session_store import WorkerPersistenceError

    monkeypatch.setattr(retry_utils, "jittered_backoff", lambda *args, **kwargs: 1.0)
    cancel = threading.Event()
    calls = []

    class RPC:
        def __call__(self, method, **params):
            calls.append(params)
            if len(calls) == 1:
                threading.Timer(0.05, cancel.set).start()
                raise TimeoutError("worker_receipt_timeout")
            return {"value": "committed"}

    store = make_store(tmp_path, RPC())
    store.cancel = cancel
    started = time.monotonic()
    with pytest.raises(WorkerPersistenceError, match="worker_cancelled"):
        store._apply("operation", {})
    assert time.monotonic() - started < 1
    assert len(calls) == 1
    assert store.journal["pending"]


def test_preset_cancel_still_allows_first_attempt(tmp_path):
    cancel = threading.Event()
    cancel.set()
    calls = []

    def rpc(method, **params):
        calls.append(params)
        return {"value": "committed"}

    store = make_store(tmp_path, rpc)
    store.cancel = cancel
    assert store._apply("operation", {}) == {"value": "committed"}
    assert len(calls) == 1


def test_real_receipt_handler_commits_one_row_for_retried_lost_ack(tmp_path):
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    db = SessionDB(tmp_path / "state.db")
    db.create_session("s", "cli")
    epoch = begin_runtime_epoch(db, instance_id="owner")
    scope = {"epoch": epoch, "execution_id": "worker", "session_id": "s", "generation": 0}
    register_worker_execution(db, **scope, kind="compute", adoption_secret="private")
    calls = [0]

    def rpc(method, **params):
        result = mutate_worker_execution(db, **params)
        calls[0] += 1
        if calls[0] == 1:
            raise TimeoutError("lost_ack")
        return result

    try:
        store = RuntimeSessionStore(rpc, scope, tmp_path / "outbox")
        import agent.runtime_session_store as module
        old = module.OWNER_SILENCE_BUDGET_S
        module.OWNER_SILENCE_BUDGET_S = 0
        try:
            with pytest.raises(TimeoutError):
                store._apply("usage.main", {"input_tokens": 1})
            store._outbox_owner.close()
            reopened = RuntimeSessionStore(lambda method, **params: mutate_worker_execution(db, **params),
                                           scope, tmp_path / "outbox")
        finally:
            module.OWNER_SILENCE_BUDGET_S = old
        receipts = db._read_one("SELECT COUNT(*) FROM worker_receipts")
        assert receipts is not None and receipts[0] == 1
        session = db.get_session("s")
        assert session is not None and session["input_tokens"] == 1
        reopened.close()
    finally:
        db.close()


def test_real_receipt_handler_retries_with_nonzero_budget(tmp_path, monkeypatch):
    """The real worker.persist path retries a committed, lost reply (not just replay)."""
    import agent.runtime_session_store as module
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 2.0)
    db = SessionDB(tmp_path / "state.db")
    db.create_session("s", "cli")
    epoch = begin_runtime_epoch(db, instance_id="owner")
    scope = {"epoch": epoch, "execution_id": "worker", "session_id": "s", "generation": 0}
    register_worker_execution(db, **scope, kind="compute", adoption_secret="private")
    calls = 0

    def rpc(method, **params):
        nonlocal calls
        result = mutate_worker_execution(db, **params)
        calls += 1
        if calls == 1:
            raise TimeoutError("worker_receipt_timeout")
        return result

    try:
        store = RuntimeSessionStore(rpc, scope, tmp_path / "outbox")
        assert store._apply("usage.main", {"input_tokens": 1})
        assert calls == 2
        assert db._read_one("SELECT COUNT(*) FROM worker_receipts")[0] == 1
        assert db.get_session("s")["input_tokens"] == 1
        assert store.journal["pending"] == []
        store.close()
    finally:
        db.close()


def test_real_receipt_handler_late_commit_race_is_one_effect(tmp_path, monkeypatch):
    """A late first commit racing the retry still resolves one durable receipt."""
    import agent.runtime_session_store as module
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 2.0)
    db = SessionDB(tmp_path / "state.db")
    db.create_session("s", "cli")
    epoch = begin_runtime_epoch(db, instance_id="owner")
    scope = {"epoch": epoch, "execution_id": "worker", "session_id": "s", "generation": 0}
    register_worker_execution(db, **scope, kind="compute", adoption_secret="private")
    late = []
    late_threads = []
    calls = 0

    def rpc(method, **params):
        nonlocal calls
        calls += 1
        if calls == 1:
            def commit_late():
                late.append(mutate_worker_execution(db, **params))
            thread = threading.Thread(target=commit_late)
            thread.start()
            late_threads.append(thread)
            raise TimeoutError("worker_receipt_timeout")
        result = mutate_worker_execution(db, **params)
        return result

    try:
        store = RuntimeSessionStore(rpc, scope, tmp_path / "outbox")
        result = store._apply("usage.main", {"input_tokens": 1})
        for thread in late_threads:
            thread.join(timeout=2)
        assert result
        assert len(late) == 1
        assert db._read_one("SELECT COUNT(*) FROM worker_receipts")[0] == 1
        assert db.get_session("s")["input_tokens"] == 1
        store.close()
    finally:
        db.close()


def test_owner_transition_requires_adoption_before_replay(tmp_path, monkeypatch):
    """A replacement epoch fences the unchanged retry; verified adoption then replays it once."""
    import agent.runtime_session_store as module
    from agent.runtime_session_store import RuntimeSessionStore, WorkerPersistenceError
    from hermes_state import SessionDB
    from hermes_state_runtime import (adopt_worker_execution, begin_runtime_epoch,
                                      mutate_worker_execution, register_worker_execution)

    monkeypatch.setattr(module, "OWNER_SILENCE_BUDGET_S", 0.0)
    path, outbox = tmp_path / "state.db", tmp_path / "outbox"
    db = SessionDB(path)
    db.create_session("s", "cli")
    first = begin_runtime_epoch(db, instance_id="first")
    scope = {"epoch": first, "execution_id": "worker", "session_id": "s", "generation": 0}
    register_worker_execution(db, **scope, kind="compute", adoption_secret="private")

    def lost(method, **params):
        mutate_worker_execution(db, **params)
        raise TimeoutError("lost_ack")

    try:
        store = RuntimeSessionStore(lost, scope, outbox)
        with pytest.raises(TimeoutError, match="lost_ack"):
            store._apply("usage.main", {"input_tokens": 1})
        store._outbox_owner.close()  # retain the journal without flushing the failed store
        second = begin_runtime_epoch(db, instance_id="second")
        successor = dict(scope, epoch=second)
        adopted = adopt_worker_execution(db, epoch=second, execution_id="worker", session_id="s",
                                         generation=0, adoption_secret="private")

        def fenced(method, **params):
            return mutate_worker_execution(db, **params)

        with pytest.raises(WorkerPersistenceError, match="outbox_scope_mismatch"):
            RuntimeSessionStore(fenced, successor, outbox, adopted=None)
        assert db._read_one("SELECT COUNT(*) FROM worker_receipts")[0] == 1
        reopened = RuntimeSessionStore(fenced, successor, outbox, adopted=adopted)
        try:
            assert reopened.journal["pending"] == []
            assert db._read_one("SELECT COUNT(*) FROM worker_receipts")[0] == 1
            assert db.get_session("s")["input_tokens"] == 1
        finally:
            reopened.close()
    finally:
        db.close()


def test_cohort_of_five_stores_recover_same_owner(tmp_path):
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    db = SessionDB(tmp_path / "state.db")
    epoch = begin_runtime_epoch(db, instance_id="owner")
    calls = {}

    def rpc(method, **params):
        key = params["execution_id"]
        calls[key] = calls.get(key, 0) + 1
        result = mutate_worker_execution(db, **params)
        if calls[key] == 1:
            raise TimeoutError("worker_receipt_timeout")
        return result

    stores = []
    try:
        for i in range(5):
            session_id = f"s{i}"
            db.create_session(session_id, "cli")
            scope = {"execution_id": str(i), "session_id": session_id, "epoch": epoch, "generation": 0}
            register_worker_execution(db, **scope, kind="compute", adoption_secret="private")
            stores.append(RuntimeSessionStore(rpc, scope, tmp_path / str(i)))
        for i, store in enumerate(stores):
            assert store._apply("usage.main", {"input_tokens": 1})
            assert store.journal["pending"] == []
        assert db._read_one("SELECT COUNT(*) FROM worker_receipts")[0] == 5
    finally:
        for store in stores:
            store.close()
        db.close()


def test_execute_wires_store_and_rpc_cancel_to_controls(monkeypatch, tmp_path):
    """Execute's production wiring is exercised before agent construction begins."""
    import sys
    import types
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    from pathlib import Path
    real_is_file = Path.is_file
    monkeypatch.setattr(Path, "is_file", lambda path: False if "hermes-update-pull" in str(path) else real_is_file(path))
    monkeypatch.setitem(sys.modules, "hermes_bootstrap", types.ModuleType("hermes_bootstrap"))
    import agent.managed_worker as managed
    import agent.runtime_session_store as runtime

    class FakeRPC:
        cancel = None
        instance = None

        def __init__(self, home):
            self.home = home
            self.cancel = object()
            type(self).instance = self

        def __call__(self, method, **params):
            return {"owner_epoch": params.get("epoch", 1)}

    class FakeStore:
        cancel = None
        instance = None

        def __init__(self, rpc, scope, outbox, adopted=None):
            self.rpc = rpc
            self.cancel = object()
            type(self).instance = self

        def close(self):
            pass

    class Policy:
        skills_prompt = ""
        source = "tool"
        ignore_rules = True
        platform = "tool"
        model = provider = base_url = None
        toolsets = ()
        max_turns = 1
        pass_session_id = False
        reasoning_config = {}

        def config(self):
            return {}

    class Channel:
        def send(self, *args, **kwargs):
            pass

    monkeypatch.setattr(managed, "bind_worker_policy", lambda frame: None)
    monkeypatch.setattr(runtime, "WorkerRPC", FakeRPC)
    monkeypatch.setattr(runtime, "RuntimeSessionStore", FakeStore)
    monkeypatch.setattr(managed, "outbox_dir", lambda home, execution: tmp_path)
    monkeypatch.setattr(managed, "discover_profile_mcp", lambda policy: None)
    def stop_before_agent(*args, **kwargs):
        assert FakeStore.instance is not None and FakeRPC.instance is not None
        assert FakeStore.instance.cancel is FakeRPC.instance.cancel
        raise AssertionError("wired")

    monkeypatch.setattr(managed, "_construct_agent", stop_before_agent)
    monkeypatch.setattr("tools.approval.register_gateway_notify", lambda *args: None)
    monkeypatch.setattr("tools.approval.unregister_gateway_notify", lambda *args: None)
    monkeypatch.setattr("gateway.session_kanban.bind_worker_context", lambda frame: None)
    monkeypatch.setattr("gateway.session_policy.restore_policy", lambda frame: Policy())
    monkeypatch.setattr("gateway.session_policy.register_worker_hooks", lambda policy: None)
    monkeypatch.setattr("gateway.session_policy.policy_scope", lambda policy, home: __import__("contextlib").nullcontext())
    monkeypatch.setattr("gateway.session_worker_construct.construct_kwargs", lambda frame: {})
    monkeypatch.setattr("gateway.session_worker_construct.checkpoint_agent_kwargs", lambda policy: {})
    monkeypatch.setattr("gateway.session_worker_construct.routing_agent_kwargs", lambda *args: {})
    frame = {"scope": {"execution_id": "e", "session_id": "s", "epoch": 1, "generation": 1},
             "home": str(tmp_path), "policy": {}, "route": "r", "api_key": None,
             "user_id": "u", "chat_id": "c", "text": "x"}
    with pytest.raises(AssertionError) as exc:
        managed.execute(frame, Channel())
    assert str(exc.value) == "wired"
