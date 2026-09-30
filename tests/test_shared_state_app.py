"""In-process checks of app.py's wiring onto shared_state.py.

The cross-process behaviour is proven with real server processes in
tests/test_shared_state_multiprocess.py; these pin the single-process edges
of the same wiring (sweeps, id validation, TTL on read, kill switch).
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Iterator

import pytest

import app
from tests.sweep_driver import run_one_sweep


@pytest.fixture()
def slot_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point every shared-state store at a private instance directory."""
    monkeypatch.setattr(app._generate_slots, "_slot_dir", str(tmp_path))
    for var in ("NOVA_STATE_SUPABASE_URL", "NOVA_STATE_SUPABASE_KEY", "SUPABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    with app._shared_plans_lock:
        saved = dict(app._shared_plans)
        app._shared_plans.clear()
    try:
        yield tmp_path
    finally:
        with app._shared_plans_lock:
            app._shared_plans.clear()
            app._shared_plans.update(saved)


def _share_entry(created_at: float) -> dict:
    return {"plan_data": {"a": 1}, "client": "Edge Co", "created_at": created_at, "est_bytes": 80}


def test_stale_job_timeout_is_published_to_the_shared_record(
    slot_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep marks a >10-min 'processing' job failed and drops it from the
    dict in the same pass; without publishing, every later poll read the
    shared record and saw 'processing' for 24h."""
    job_id = uuid.uuid4().hex
    with app._generation_jobs_lock:
        app._generation_jobs[job_id] = {
            "status": "processing",
            "progress_pct": 40,
            "status_message": "Synthesizing...",
            "created": time.time() - 700,
            "result_bytes": None,
            "error": None,
            "_session_token": "",
        }
    app._mirror_job(job_id)
    run_one_sweep(monkeypatch, loop=app._cleanup_generation_jobs)
    with app._generation_jobs_lock:
        assert job_id not in app._generation_jobs
    record = app._job_record_get(job_id)
    assert record["status"] == "failed"
    assert "timed out" in record["error"]


def test_mirror_rewrite_keeps_an_ack_recorded_by_another_worker(slot_dir: Path) -> None:
    job_id = uuid.uuid4().hex
    with app._generation_jobs_lock:
        app._generation_jobs[job_id] = {
            "status": "completed",
            "progress_pct": 100,
            "created": time.time(),
            "result_bytes": b"PK\x03\x04zip",
            "result_filename": "p.zip",
            "result_content_type": "application/zip",
            "_session_token": "tok",
        }
    try:
        app._mirror_job(job_id)
        record = app._job_record_get(job_id)
        app._job_record_store.put(job_id, {**record, "_qa_acknowledged_by": "b@joveo.com"})
        app._mirror_job(job_id)  # this worker never saw the ack
        assert app._job_record_get(job_id)["_qa_acknowledged_by"] == "b@joveo.com"
        # The ZIP went to the blob store, never into the JSON record.
        raw = (slot_dir / f"job_{job_id}.json").read_bytes()
        assert b"zip" not in raw.replace(b"p.zip", b"").replace(b"application/zip", b"")
        assert app._job_result_blobs.get(job_id) == b"PK\x03\x04zip"
    finally:
        with app._generation_jobs_lock:
            app._generation_jobs.pop(job_id, None)


def test_shared_plan_get_rejects_malformed_ids_before_any_io(slot_dir: Path) -> None:
    for bad in ("../../etc/passwd", "a" * 21, "a" * 23, "", "abc.def", "ABCDEF12"):
        assert app._shared_plan_get(bad) is None
    assert not (slot_dir / "shared_state").exists()


def test_share_ttl_is_enforced_on_read_on_every_layer(slot_dir: Path) -> None:
    fresh, stale = app._new_share_id(), app._new_share_id()
    app._shared_plan_put(fresh, _share_entry(time.time()))
    app._shared_plan_put(stale, _share_entry(time.time() - app._SHARED_PLANS_TTL - 5))
    assert app._shared_plan_get(fresh)["client"] == "Edge Co"
    assert app._shared_plan_get(stale) is None  # memory copy, before any sweep
    with app._shared_plans_lock:
        app._shared_plans.clear()
    assert app._shared_plan_get(fresh)["client"] == "Edge Co"  # file layer
    assert app._shared_plan_get(stale) is None


def test_legacy_share_ids_stay_readable_from_memory(slot_dir: Path) -> None:
    legacy = uuid.uuid4().hex[:8]
    with app._shared_plans_lock:
        app._shared_plans[legacy] = _share_entry(time.time())
    assert app._shared_plan_get(legacy)["client"] == "Edge Co"


def test_kill_switch_restores_dict_only_behaviour(
    slot_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NOVA_SHARED_STATE", "0")
    share_id = app._new_share_id()
    app._shared_plan_put(share_id, _share_entry(time.time()))
    assert app._shared_plan_get(share_id)["client"] == "Edge Co"
    assert not (slot_dir / "shared_state").exists()
    with app._shared_plans_lock:
        app._shared_plans.clear()
    assert app._shared_plan_get(share_id) is None


def test_oversized_plan_result_stays_memory_only(slot_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan_id = uuid.uuid4().hex
    monkeypatch.setattr(app, "_extract_plan_json", lambda _d: {"blob": "x" * (300 * 1024)})
    app._store_plan_result(plan_id, {})
    try:
        assert app._plan_result_lookup(plan_id)["data"]["blob"]
        assert not list((slot_dir / "shared_state" / "plan_results").glob("*.json"))
    finally:
        with app._plan_results_lock:
            app._plan_results_store.pop(plan_id, None)


def test_sheets_url_reaches_the_shared_copy_even_when_evicted_locally(slot_dir: Path) -> None:
    plan_id = uuid.uuid4().hex
    app._plan_result_store.put(plan_id, {"data": {"channels": []}, "created": time.time()})
    app._plan_result_set_sheets_url(plan_id, "https://sheet")
    assert app._plan_result_store.get(plan_id)["sheets_url"] == "https://sheet"
    with app._plan_results_lock:
        assert plan_id not in app._plan_results_store


def test_share_ids_minted_are_128_bit() -> None:
    ids = {app._new_share_id() for _ in range(200)}
    assert len(ids) == 200
    assert all(app._SHARE_ID_RE.fullmatch(i) and len(i) == 22 for i in ids)
