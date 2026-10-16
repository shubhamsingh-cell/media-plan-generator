"""Slack plan-download links must survive a signing-key change.

Every link posted to #nova-media-plans is valid for SLACK_DOWNLOAD_LINK_DAYS
(30). Production minted them with SUPABASE_SERVICE_ROLE_KEY because
SESSION_SIGNING_SECRET was unset; setting SESSION_SIGNING_SECRET (to turn
Saved Plans on) moves the *minting* key to the new secret. Verifying against
that single key silently 403'd every outstanding link.

Minting still uses the first configured key; verification accepts a token
minted with ANY currently configured candidate key, so a rotation keeps old
links alive for their remaining lifetime. A key that is no longer configured
at all stops verifying (that is how a leaked key is actually retired).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import uuid

import pytest

import slack_plan_notifier as spn
from tests.test_multiprocess_serving import (  # noqa: F401  (fixtures)
    _http_get,
    _install_fake_supabase,
    _supabase_row,
    isolated_slot_dir,
    live_server,
)

_KEY_ENVS = ("SESSION_SIGNING_SECRET", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_KEY")


@pytest.fixture(autouse=True)
def _clean_key_env(monkeypatch) -> None:
    for name in (*_KEY_ENVS, "SUPABASE_ANON_KEY"):
        monkeypatch.delenv(name, raising=False)


def _job_id() -> str:
    return uuid.uuid4().hex


def _mint_with(monkeypatch, job_id: str, **env: str) -> str:
    """Mint a token with exactly `env` configured (all key envs reset first)."""
    for name in _KEY_ENVS:
        monkeypatch.delenv(name, raising=False)
    for name, val in env.items():
        monkeypatch.setenv(name, val)
    return spn.slack_download_token(job_id)


def test_old_token_survives_session_secret_being_set(monkeypatch) -> None:
    """The prod case: links minted with the service-role key must still verify
    after SESSION_SIGNING_SECRET is set, and new links use the new secret."""
    job_id = _job_id()
    old_key = secrets.token_hex(16)
    new_key = secrets.token_hex(16)

    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", old_key)
    old_token = spn.slack_download_token(job_id)
    assert old_token and spn.verify_slack_download_token(job_id, old_token)

    monkeypatch.setenv("SESSION_SIGNING_SECRET", new_key)
    new_token = spn.slack_download_token(job_id)

    assert spn.verify_slack_download_token(job_id, old_token), "old link 403'd"
    assert new_token and new_token != old_token, "minting must move to the new key"
    assert spn.verify_slack_download_token(job_id, new_token)
    # The button the next Slack message posts carries the NEW token.
    assert spn.slack_download_url(job_id).endswith(f"/download?t={new_token}")


def test_token_from_a_key_no_longer_configured_is_rejected(monkeypatch) -> None:
    job_id = _job_id()
    retired = secrets.token_hex(16)
    current = secrets.token_hex(16)

    retired_token = _mint_with(monkeypatch, job_id, SUPABASE_SERVICE_ROLE_KEY=retired)
    assert spn.verify_slack_download_token(job_id, retired_token)

    # Rotate: the old key is removed from the environment entirely.
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.setenv("SESSION_SIGNING_SECRET", current)
    assert not spn.verify_slack_download_token(job_id, retired_token)
    assert spn.verify_slack_download_token(job_id, spn.slack_download_token(job_id))


def test_minting_uses_the_first_configured_key(monkeypatch) -> None:
    job_id = _job_id()
    a, b, c = (secrets.token_hex(16) for _ in range(3))
    only_a = _mint_with(monkeypatch, job_id, SESSION_SIGNING_SECRET=a)
    only_b = _mint_with(monkeypatch, job_id, SUPABASE_SERVICE_ROLE_KEY=b)
    only_c = _mint_with(monkeypatch, job_id, SUPABASE_KEY=c)
    assert len({only_a, only_b, only_c}) == 3

    # All three set -> mint with the first (SESSION_SIGNING_SECRET) ...
    for name, val in zip(_KEY_ENVS, (a, b, c)):
        monkeypatch.setenv(name, val)
    assert spn.slack_download_token(job_id) == only_a
    # ... but a token minted with any of the three still verifies.
    for token in (only_a, only_b, only_c):
        assert spn.verify_slack_download_token(job_id, token)

    # First key absent -> falls through to the next set one, as before.
    monkeypatch.delenv("SESSION_SIGNING_SECRET")
    assert spn.slack_download_token(job_id) == only_b
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY")
    assert spn.slack_download_token(job_id) == only_c


def test_blank_and_duplicate_keys_do_not_widen_acceptance(monkeypatch) -> None:
    job_id = _job_id()
    key = secrets.token_hex(16)
    monkeypatch.setenv("SESSION_SIGNING_SECRET", "   ")  # whitespace == unset
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", key)
    monkeypatch.setenv("SUPABASE_KEY", key)  # same value twice
    token = spn.slack_download_token(job_id)
    assert token == _mint_with(monkeypatch, job_id, SUPABASE_SERVICE_ROLE_KEY=key)
    monkeypatch.setenv("SESSION_SIGNING_SECRET", "   ")
    monkeypatch.setenv("SUPABASE_KEY", key)
    assert spn.verify_slack_download_token(job_id, token)
    # A blank key must never verify a token "signed" with the empty key.
    msg = f"nova-slack-plan-download:v1:{job_id}".encode("utf-8")
    empty_key_token = hmac.new(b"", msg, hashlib.sha256).hexdigest()[:32]
    assert not spn.verify_slack_download_token(job_id, empty_key_token)


def test_token_stays_bound_to_one_job_across_a_rotation(monkeypatch) -> None:
    a_job, b_job = _job_id(), _job_id()
    old_key = secrets.token_hex(16)
    old_token = _mint_with(monkeypatch, a_job, SUPABASE_SERVICE_ROLE_KEY=old_key)
    # Old key still configured, plus a new secret: the old token opens only
    # the job it was minted for.
    monkeypatch.setenv("SESSION_SIGNING_SECRET", secrets.token_hex(16))
    assert spn.verify_slack_download_token(a_job, old_token)
    assert not spn.verify_slack_download_token(b_job, old_token)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        None,
        "   ",
        "z" * 32,  # right length, not hex
        "g" + "0" * 31,
        "0" * 31,  # too short
        "0" * 33,  # too long
        "0" * 64,  # a full SHA-256 hexdigest, not our 32-char truncation
        "é" * 16,  # non-ASCII: compare_digest raises TypeError on it
        "é" + "0" * 31,
    ],
)
def test_malformed_tokens_rejected_with_every_key_configured(monkeypatch, bad) -> None:
    for name in _KEY_ENVS:
        monkeypatch.setenv(name, secrets.token_hex(16))
    assert spn.verify_slack_download_token(_job_id(), bad) is False


def test_uppercase_hex_token_still_accepted(monkeypatch) -> None:
    """Pre-existing behaviour (tokens are lower-cased before the compare),
    pinned so the rotation rewrite does not silently change it."""
    job_id = _job_id()
    monkeypatch.setenv("SESSION_SIGNING_SECRET", secrets.token_hex(16))
    token = spn.slack_download_token(job_id)
    assert spn.verify_slack_download_token(job_id, token.upper())


def test_no_key_configured_mints_nothing_and_verifies_nothing(monkeypatch) -> None:
    job_id = _job_id()
    assert spn.slack_download_token(job_id) == ""
    assert spn.verify_slack_download_token(job_id, "0" * 32) is False
    assert spn.verify_slack_download_token(job_id, "") is False
    # A token minted earlier is useless once every key is gone.
    token = _mint_with(
        monkeypatch, job_id, SESSION_SIGNING_SECRET=secrets.token_hex(16)
    )
    assert token
    monkeypatch.delenv("SESSION_SIGNING_SECRET")
    assert spn.verify_slack_download_token(job_id, token) is False


def test_empty_job_id_never_mints_or_verifies(monkeypatch) -> None:
    monkeypatch.setenv("SESSION_SIGNING_SECRET", secrets.token_hex(16))
    assert spn.slack_download_token("") == ""
    assert spn.verify_slack_download_token("", "0" * 32) is False


def test_link_minted_before_secret_was_set_survives_rotation_over_http(
    live_server: int, isolated_slot_dir: str, monkeypatch
) -> None:
    """Prod minted every link with SUPABASE_SERVICE_ROLE_KEY (no
    SESSION_SIGNING_SECRET). Setting the secret must not 403 those links
    through the real route; a link whose key was removed must still 403."""
    job_id = _job_id()
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=26 * 3600)])

    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", secrets.token_hex(16))
    old_token = spn.slack_download_token(job_id)
    monkeypatch.setenv("SESSION_SIGNING_SECRET", secrets.token_hex(16))
    assert spn.slack_download_token(job_id) != old_token

    path = f"/api/plans/{job_id}/download?t={old_token}"
    status, _h, body = _http_get(live_server, path)
    assert status == 200, body[:300]
    assert body == b"PK\x03\x04supabase-copy"

    # Retire the old key entirely: its link is dead.
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY")
    status, _h, body = _http_get(live_server, path)
    assert status == 403
    assert b"supabase-copy" not in body


def _jwt(role: str) -> str:
    """A Supabase-shaped JWT (unsigned payload is all the code reads), built at
    runtime so no secret-shaped literal sits in source."""

    def seg(obj: dict) -> str:
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    sig = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    return ".".join(
        (seg({"alg": "HS256", "typ": "JWT"}), seg({"role": role, "ref": "x"}), sig)
    )


def _raw_token(key: str, job_id: str) -> str:
    msg = f"nova-slack-plan-download:v1:{job_id}".encode("utf-8")
    return hmac.new(key.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:32]


def test_anon_jwt_alone_never_signs_or_verifies(monkeypatch) -> None:
    job_id = _job_id()
    anon = _jwt("anon")
    monkeypatch.setenv("SUPABASE_KEY", anon)
    assert spn.slack_download_token(job_id) == ""
    # Someone holding the public key can compute this token; it must not open.
    assert spn.verify_slack_download_token(job_id, _raw_token(anon, job_id)) is False
    # With nothing signable, the Slack button falls back to the 24h job link.
    assert spn.slack_download_url(job_id).endswith(f"/api/jobs/{job_id}")


def test_anon_jwt_is_skipped_and_service_key_is_used(monkeypatch) -> None:
    job_id = _job_id()
    anon = _jwt("anon")
    service = _jwt("service_role")
    monkeypatch.setenv("SUPABASE_KEY", anon)
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", service)
    token = spn.slack_download_token(job_id)
    assert token == _raw_token(service, job_id)
    assert spn.verify_slack_download_token(job_id, token)
    assert not spn.verify_slack_download_token(job_id, _raw_token(anon, job_id))


def test_anon_jwt_in_a_higher_priority_slot_is_still_skipped(monkeypatch) -> None:
    """Order must not matter: the anon key is excluded wherever it sits."""
    job_id = _job_id()
    anon = _jwt("anon")
    service = _jwt("service_role")
    monkeypatch.setenv("SESSION_SIGNING_SECRET", anon)
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", service)
    assert spn.slack_download_token(job_id) == _raw_token(service, job_id)
    assert not spn.verify_slack_download_token(job_id, _raw_token(anon, job_id))


def test_value_equal_to_supabase_anon_key_is_skipped(monkeypatch) -> None:
    """The /api/config key may be an opaque (non-JWT) publishable key: matched
    by equality with SUPABASE_ANON_KEY, not by parsing."""
    job_id = _job_id()
    public = "sb_publishable_" + secrets.token_hex(12)
    private = secrets.token_hex(16)
    monkeypatch.setenv("SUPABASE_ANON_KEY", public)
    monkeypatch.setenv("SUPABASE_KEY", public)
    assert spn.slack_download_token(job_id) == ""
    assert spn.verify_slack_download_token(job_id, _raw_token(public, job_id)) is False

    monkeypatch.setenv("SESSION_SIGNING_SECRET", private)
    assert spn.slack_download_token(job_id) == _raw_token(private, job_id)
    assert not spn.verify_slack_download_token(job_id, _raw_token(public, job_id))


def test_non_anon_and_malformed_jwts_are_not_treated_as_public(monkeypatch) -> None:
    job_id = _job_id()
    for value in (
        _jwt("service_role"),
        _jwt("authenticated"),
        "aaa.bbb.ccc",  # three segments, payload not JSON
        "only.two",
        "a.%%%.c",  # undecodable payload
        "a.W10.c",  # payload is valid JSON but a list, not an object
        secrets.token_hex(16),
    ):
        monkeypatch.setenv("SUPABASE_KEY", value)
        token = spn.slack_download_token(job_id)
        assert token == _raw_token(value, job_id), value
        assert spn.verify_slack_download_token(job_id, token), value
