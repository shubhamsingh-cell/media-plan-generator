"""The Slack "Download" button must still work the next day.

Every button in #nova-media-plans pointed at /api/jobs/<id>, which dies with
the 24h job expiry -- and the "permanent" Google Drive copy has never uploaded
in prod (service accounts have no Drive quota). Prod logs, 2026-10-10 22:43
UTC: the RTI International button returned 404 "expired" 26h after the plan
was generated, though the S47 Supabase copy of the ZIP was still there.

The button now carries an HMAC token (/api/plans/<id>/download?t=...) that
reads the Supabase copy for SLACK_DOWNLOAD_LINK_DAYS. The bare job link keeps
its 24h bound -- only someone holding the Slack message's token gets longer.
"""

from __future__ import annotations

import secrets
import uuid

import pytest

import slack_plan_notifier as spn
from tests.test_multiprocess_serving import (  # noqa: F401  (fixtures)
    _http_get,
    _install_fake_supabase,
    _supabase_row,
    app_module,
    isolated_slot_dir,
    live_server,
)

DAY = 86400


@pytest.fixture()
def link_key(monkeypatch) -> str:
    key = secrets.token_hex(16)
    monkeypatch.setenv("SESSION_SIGNING_SECRET", key)
    monkeypatch.delenv("SLACK_DOWNLOAD_LINK_DAYS", raising=False)
    return key


def _job_id() -> str:
    return uuid.uuid4().hex


def test_slack_button_uses_signed_long_lived_link(link_key: str) -> None:
    job_id = _job_id()
    blocks = spn._build_plan_notification_blocks(
        {"job_id": job_id, "client_name": "RTI International", "filename": "RTI.zip"}
    )
    urls = [
        el["url"] for b in blocks if b.get("type") == "actions" for el in b["elements"]
    ]
    assert len(urls) == 1
    assert f"/api/plans/{job_id}/download?t=" in urls[0]
    token = urls[0].rsplit("t=", 1)[1]
    assert spn.verify_slack_download_token(job_id, token)


def test_no_key_falls_back_to_job_link(monkeypatch) -> None:
    for name in ("SESSION_SIGNING_SECRET", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_KEY"):
        monkeypatch.delenv(name, raising=False)
    job_id = _job_id()
    assert spn.slack_download_url(job_id).endswith(f"/api/jobs/{job_id}")


def test_token_is_bound_to_one_job(link_key: str) -> None:
    a, b = _job_id(), _job_id()
    assert not spn.verify_slack_download_token(b, spn.slack_download_token(a))
    assert not spn.verify_slack_download_token(a, "")


def test_signed_link_serves_plan_older_than_24h(
    live_server: int, isolated_slot_dir: str, monkeypatch, link_key: str
) -> None:
    """The reported case: a day-old plan, still in Supabase, must download."""
    job_id = _job_id()
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=26 * 3600)])
    token = spn.slack_download_token(job_id)

    status, headers, body = _http_get(
        live_server, f"/api/plans/{job_id}/download?t={token}"
    )
    assert status == 200, body[:300]
    assert body == b"PK\x03\x04supabase-copy"
    assert "attachment" in headers.get("Content-Disposition", "")


def test_bare_job_link_still_expires_at_24h(
    live_server: int, isolated_slot_dir: str, monkeypatch, link_key: str
) -> None:
    """Widening the Slack route must not widen the bearer job link."""
    job_id = _job_id()
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=26 * 3600)])
    status, _h, body = _http_get(live_server, f"/api/jobs/{job_id}")
    assert status == 404
    assert b"supabase-copy" not in body


def test_signed_link_rejects_bad_token(
    live_server: int, isolated_slot_dir: str, monkeypatch, link_key: str
) -> None:
    job_id = _job_id()
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=60)])
    for path in (
        f"/api/plans/{job_id}/download",
        f"/api/plans/{job_id}/download?t=" + "0" * 32,
        f"/api/plans/{job_id}/download?t={spn.slack_download_token(_job_id())}",
        # non-ASCII: compare_digest raises TypeError on it -> was a 500
        f"/api/plans/{job_id}/download?t=%C3%A9",
        f"/api/plans/{job_id}/download?t=" + "%C3%A9" * 16,
    ):
        status, headers, body = _http_get(live_server, path)
        assert status == 403, (path, status)
        assert b"supabase-copy" not in body
        assert headers.get("Content-Type", "").startswith("text/html")


def test_signed_link_past_its_window_shows_a_page_not_json(
    live_server: int, isolated_slot_dir: str, monkeypatch, link_key: str
) -> None:
    job_id = _job_id()
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=31 * DAY)])
    status, headers, body = _http_get(
        live_server,
        f"/api/plans/{job_id}/download?t={spn.slack_download_token(job_id)}",
    )
    assert status == 404
    assert headers.get("Content-Type", "").startswith("text/html")
    assert b"expired" in body and b"supabase-copy" not in body


def test_malformed_job_id_rejected(
    live_server: int, isolated_slot_dir: str, link_key: str
) -> None:
    status, _h, _b = _http_get(live_server, "/api/plans/not-a-job/download?t=x")
    assert status == 400


def test_link_days_env_is_bounded(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_DOWNLOAD_LINK_DAYS", "7")
    assert spn.slack_download_link_seconds() == 7 * DAY
    monkeypatch.setenv("SLACK_DOWNLOAD_LINK_DAYS", "99999")
    assert spn.slack_download_link_seconds() == 365 * DAY
    monkeypatch.setenv("SLACK_DOWNLOAD_LINK_DAYS", "junk")
    assert spn.slack_download_link_seconds() == 30 * DAY


def test_expired_job_link_opened_in_a_browser_shows_a_page(
    live_server: int, isolated_slot_dir: str
) -> None:
    """Old Slack buttons (bare /api/jobs/<id>) opened after 24h showed raw
    JSON in the tab; a person gets a page, the wizard's poll keeps JSON."""
    job_id = _job_id()
    status, headers, body = _http_get(
        live_server,
        f"/api/jobs/{job_id}",
        headers={"Accept": "text/html,application/xhtml+xml,*/*;q=0.8"},
    )
    assert status == 404
    assert headers.get("Content-Type", "").startswith("text/html")
    assert b"expired" in body

    status, headers, body = _http_get(live_server, f"/api/jobs/{job_id}")
    assert status == 404
    assert headers.get("Content-Type", "").startswith("application/json")
