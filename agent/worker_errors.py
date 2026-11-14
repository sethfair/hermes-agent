"""Stable reason tokens shared by the managed worker and its gateway owner."""

from __future__ import annotations


WORKER_RECEIPT_TIMEOUT = "worker_receipt_timeout"
MANAGED_WORKER_PIPE_CLOSED = "managed_worker_pipe_closed"
MANAGED_WORKER_FAILED = "managed_worker_failed"


def failure_reason(exc: BaseException) -> str:
    """Map worker-side failures to bounded, non-sensitive gateway reason tokens."""
    if isinstance(exc, TimeoutError) and str(exc) == WORKER_RECEIPT_TIMEOUT:
        return WORKER_RECEIPT_TIMEOUT
    if isinstance(exc, EOFError):
        return MANAGED_WORKER_PIPE_CLOSED
    return MANAGED_WORKER_FAILED
