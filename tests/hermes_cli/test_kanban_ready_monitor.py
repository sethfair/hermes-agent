"""Behavioral contracts for aged-Ready monitoring."""

from hermes_cli import kanban_ready_monitor as monitor


def test_ready_monitor_generation_exposes_frozen_classifier_api():
    assert monitor.READY_ENTRY_CLASSIFIERS
    assert callable(monitor.ready_generation)
    assert callable(monitor.canonical_guard_fingerprint)
    assert callable(monitor.classify_ready_state)
