"""Tests for the S90/P1-extension: alert_manager dedup backed by the shared store.

alert_manager's subject dedup was an in-memory per-process dict, wiped on every
worker restart -- the same root cause P1 fixed for the bridge cooldown, but for
the NON-bridge alert sources (auto_qc, data_enrichment, data_matrix_monitor)
that call send_alert directly. It now reads/writes through the shared,
fail-open AlertCooldownStore, with the in-memory dict as the fallback.
"""

from __future__ import annotations

import time

import pytest

import alert_manager
from alert_cooldown_store import AlertCooldownStore, InMemoryCooldownBackend


def test_dedup_roundtrip_with_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        alert_manager, "_cooldown_store", AlertCooldownStore(InMemoryCooldownBackend())
    )
    assert alert_manager._dedup_last_sent("Subj A") == 0.0  # never sent
    alert_manager._dedup_mark_sent("Subj A")
    last = alert_manager._dedup_last_sent("Subj A")
    assert last > 0 and (time.time() - last) < 5
    assert alert_manager._dedup_last_sent("Subj B") == 0.0  # distinct key independent


def test_dedup_fallback_without_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(alert_manager, "_cooldown_store", None)
    with alert_manager._lock:
        alert_manager._dedup_cache.clear()
    assert alert_manager._dedup_last_sent("X") == 0.0
    alert_manager._dedup_mark_sent("X")
    assert alert_manager._dedup_last_sent("X") > 0


class _ExplodingBackend:
    name = "boom"

    def get_last_fired(self, key: str):
        raise RuntimeError("store down")

    def record_fired(self, key: str, ts: float) -> None:
        raise RuntimeError("store down")

    def active_count(self) -> int:
        return -1


def test_dedup_fail_open_when_store_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        alert_manager, "_cooldown_store", AlertCooldownStore(_ExplodingBackend())
    )
    # A store outage must not suppress (-> 0.0 means "not recently sent" -> send),
    # and must never raise out of the alert path.
    assert alert_manager._dedup_last_sent("Subj") == 0.0
    alert_manager._dedup_mark_sent("Subj")  # must not raise
