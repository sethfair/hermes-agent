"""The managed-worker loss token must survive the worker/gateway boundary."""

from types import SimpleNamespace

from agent import managed_worker
from agent.worker_errors import failure_reason as worker_failure_reason
from hermes_cli.kanban_owner_recovery import is_host_side_loss


def test_receipt_timeout_uses_fixed_host_loss_token():
    assert worker_failure_reason(TimeoutError("worker_receipt_timeout")) == "worker_receipt_timeout"


def test_pipe_eof_uses_fixed_host_loss_token():
    assert worker_failure_reason(EOFError("anything")) == "managed_worker_pipe_closed"


def test_generic_worker_exception_still_uses_failure_token():
    assert worker_failure_reason(RuntimeError("worker_receipt_timeout")) == "managed_worker_failed"


def test_worker_main_emits_host_loss_token_without_exception_text(monkeypatch):
    sent = []

    class Channel:
        stream = SimpleNamespace(close=lambda: None)

        def send(self, kind, **payload):
            sent.append((kind, payload))

    monkeypatch.setattr(managed_worker, "WorkerChannel", lambda _stream: Channel())
    monkeypatch.setattr(managed_worker.os, "fdopen", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(managed_worker.os, "dup2", lambda *_args: None)
    monkeypatch.setattr(managed_worker, "hello", dict)
    monkeypatch.setattr(managed_worker, "read_frame", lambda _stream: {})
    monkeypatch.setattr(managed_worker, "validate_bootstrap", lambda _frame: {})
    monkeypatch.setattr(managed_worker, "execute", lambda _frame, _channel: (_ for _ in ()).throw(
        TimeoutError("worker_receipt_timeout")))

    assert managed_worker.main() == 1
    assert sent[-1] == ("error", {"reason": "worker_receipt_timeout"})
    assert is_host_side_loss("managed worker lost: worker_receipt_timeout")
