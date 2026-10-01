"""In-process checks of app.py's wiring onto shared_state.py.

The cross-process behaviour is proven with real server processes in
tests/test_shared_state_multiprocess.py; these pin the single-process edges
of the same wiring (sweeps, id validation, TTL on read, kill switch).
"""

from __future__ import annotations

import hashlib
import json
import os
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


def _legacy_snapshot(job: dict) -> dict:
    """The exact whitelist origin/main's _mirror_job wrote (b2764e0..caa3c47)."""
    snap = {
        "status": job.get("status"),
        "progress_pct": job.get("progress_pct"),
        "status_message": job.get("status_message"),
        "created": job.get("created"),
        "error": job.get("error"),
        "result_filename": job.get("result_filename"),
        "result_content_type": job.get("result_content_type"),
        "bundle_qa": job.get("bundle_qa"),
    }
    if job.get("_session_token"):
        snap["_session_token_sha256"] = hashlib.sha256(
            job["_session_token"].encode("utf-8")
        ).hexdigest()
    return snap


def test_kill_switch_mirror_file_is_byte_identical_to_the_legacy_mirror(
    slot_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NOVA_SHARED_STATE=0 must still write prod's job mirror -- same path,
    same bytes (json.dump default formatting), same whitelist -- and nothing
    this change added (ack field, ZIP blob, shared_state dir)."""
    monkeypatch.setenv("NOVA_SHARED_STATE", "0")
    job_id = uuid.uuid4().hex
    job = {
        "status": "completed",
        "progress_pct": 100,
        "status_message": "Complete",
        "created": time.time(),
        "error": None,
        "result_bytes": b"PK\x03\x04zip",
        "result_filename": "Plan.zip",
        "result_content_type": "application/zip",
        "bundle_qa": {"qa_status": "critical", "critical_count": 1, "codes": ["x"]},
        "_session_token": "tok",
        "_qa_acknowledged_by": "someone@joveo.com",
    }
    with app._generation_jobs_lock:
        app._generation_jobs[job_id] = job
    try:
        app._mirror_job(job_id)
        written = (slot_dir / f"job_{job_id}.json").read_bytes()
        assert written == json.dumps(_legacy_snapshot(job)).encode("utf-8")
        assert not (slot_dir / "shared_state").exists()
    finally:
        with app._generation_jobs_lock:
            app._generation_jobs.pop(job_id, None)


def test_kill_switch_keeps_the_legacy_mirror_sweep(
    slot_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NOVA_SHARED_STATE", "0")
    stale = slot_dir / f"job_{uuid.uuid4().hex}.json"
    fresh = slot_dir / f"job_{uuid.uuid4().hex}.json"
    lock = slot_dir / "generate_slot_0.lock"
    for f in (stale, fresh, lock):
        f.write_text("{}")
    old = time.time() - app._GENERATION_JOB_EXPIRY_SECONDS - 60
    os.utime(stale, (old, old))
    os.utime(lock, (old, old))
    run_one_sweep(monkeypatch, loop=app._cleanup_generation_jobs)
    assert not stale.exists() and fresh.exists() and lock.exists()


def test_share_feedback_is_bounded_per_share_and_per_comment(slot_dir: Path) -> None:
    share_id = app._new_share_id()
    for i in range(app._PLAN_FEEDBACK_PER_SHARE + 10):
        count = app._plan_feedback_add(share_id, f"n{i}" + "N" * 500, f"c{i}" + "x" * 5000)
    assert count == app._PLAN_FEEDBACK_PER_SHARE
    items = app._plan_feedback_list(share_id)
    assert len(items) == app._PLAN_FEEDBACK_PER_SHARE
    assert items[0]["comment"].startswith("c10") and items[-1]["comment"].startswith("c59")
    assert all(len(i["comment"]) <= app._PLAN_FEEDBACK_COMMENT_MAX for i in items)
    assert all(len(i["name"]) <= app._PLAN_FEEDBACK_NAME_MAX for i in items)
    with app._plan_feedback_lock:
        assert len(app._plan_feedback[share_id]) == app._PLAN_FEEDBACK_PER_SHARE
        app._plan_feedback.pop(share_id, None)
        app._plan_feedback_ts.pop(share_id, None)
    # Another worker (empty dict) sees the same bounded list from the shared layer.
    assert len(app._plan_feedback_list(share_id)) == app._PLAN_FEEDBACK_PER_SHARE


def test_corrupt_feedback_records_render_nothing_rather_than_crash(slot_dir: Path) -> None:
    share_id = app._new_share_id()
    app._plan_feedback_store.put(
        share_id,
        {
            "created": time.time(),
            "items": [
                {"name": "ok", "comment": "fine", "created_at": time.time()},
                {"name": "bad", "comment": "no time", "created_at": "yesterday"},
                "not-a-dict",
                {"name": ["list"], "comment": {"x": 1}, "created_at": 5},
            ],
        },
    )
    items = app._plan_feedback_list(share_id)
    assert [i["name"] for i in items] == ["ok", "['list']"]
    assert all(isinstance(i["comment"], str) for i in items)


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
