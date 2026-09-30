"""Regression tests: API keys / tokens never reach a log handler.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 note): WARNING
lines printed full API keys embedded in URL query strings (Adzuna ``app_key``,
FRED ``api_key``). urllib / requests exception strings echo the URL as well.
``log_redaction.SecretRedactingFilter`` (installed by
``monitoring.configure_logging``) must scrub the formatted message AND the
formatted exception text.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Iterator

import pytest

from log_redaction import (
    REDACTED,
    SecretRedactingFilter,
    install_log_redaction,
    redact_secrets,
)

# Fake secrets, deliberately long enough that any leaked fragment is obvious.
# Built from low-entropy pieces so secret scanners do not flag the fixtures.
ADZUNA_KEY = "FAKEADZUNAKEY" + "A" * 19
FRED_KEY = "FAKEFREDKEY" + "B" * 21
GEMINI_KEY = "FAKEGEMINIKEY" + "C" * 26
CENSUS_KEY = "FAKECENSUSKEY" + "D" * 12
JWT = ".".join(
    ("FAKEJWTHEADER" + "E" * 8, "FAKEJWTPAYLOAD" + "F" * 8, "FAKESIG" + "G" * 8)
)

# Real URL shapes found in the codebase (api_enrichment.py, market_signals.py,
# data_matrix_monitor.py, llm_router.py, vector_search.py).
ADZUNA_URL = (
    "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=a1b2c3d4"
    f"&app_key={ADZUNA_KEY}&results_per_page=50&what=registered%20nurse"
)
FRED_URL = (
    "https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE"
    f"&api_key={FRED_KEY}&file_type=json"
)
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"gemini-embedding-2:batchEmbedContents?key={GEMINI_KEY}"
)
GEMINI_SSE_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"gemini-2.0-flash:streamGenerateContent?alt=sse&key={GEMINI_KEY}"
)
CENSUS_URL = (
    "https://api.census.gov/data/2022/acs/acs5?get=B01003_001E&for=place:*"
    f"&in=state:42&key={CENSUS_KEY}"
)

ALL_URLS_WITH_SECRET = [
    (ADZUNA_URL, ADZUNA_KEY),
    (FRED_URL, FRED_KEY),
    (GEMINI_URL, GEMINI_KEY),
    (GEMINI_SSE_URL, GEMINI_KEY),
    (CENSUS_URL, CENSUS_KEY),
]


# ---------------------------------------------------------------------------
# 1. redact_secrets on the real URL shapes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("url,secret", ALL_URLS_WITH_SECRET)
def test_real_url_shapes_lose_the_key_but_keep_the_rest(url: str, secret: str) -> None:
    out = redact_secrets(url)
    assert secret not in out
    assert REDACTED in out
    # host / path / non-secret params survive so the line stays diagnosable
    assert out.split("?")[0] == url.split("?")[0]


def test_adzuna_keeps_app_id_and_other_params() -> None:
    out = redact_secrets(ADZUNA_URL)
    assert "app_id=a1b2c3d4" in out
    assert f"app_key={REDACTED}" in out
    assert "results_per_page=50" in out and "what=registered%20nurse" in out


def test_fred_keeps_series_id_and_file_type() -> None:
    out = redact_secrets(FRED_URL)
    assert "series_id=UNRATE" in out and "file_type=json" in out
    assert f"api_key={REDACTED}" in out


# ---------------------------------------------------------------------------
# 2. urllib / requests exception strings
# ---------------------------------------------------------------------------


def test_requests_max_retries_exception_text() -> None:
    text = (
        "HTTPSConnectionPool(host='api.stlouisfed.org', port=443): Max retries "
        "exceeded with url: /fred/series/observations?series_id=UNRATE"
        f"&api_key={FRED_KEY}&file_type=json (Caused by ReadTimeoutError("
        "\"HTTPSConnectionPool(host='api.stlouisfed.org', port=443): Read timed "
        'out. (read timeout=10)"))'
    )
    out = redact_secrets(text)
    assert FRED_KEY not in out
    assert "series_id=UNRATE" in out and "read timeout=10" in out


def test_requests_http_error_text() -> None:
    text = f"503 Server Error: Service Unavailable for url: {ADZUNA_URL}"
    out = redact_secrets(text)
    assert ADZUNA_KEY not in out
    assert out.startswith("503 Server Error: Service Unavailable for url: https://")


def test_urllib_error_strings_are_left_alone() -> None:
    for text in (
        "HTTP Error 401: Unauthorized",
        "<urlopen error [Errno 8] nodename nor servname provided, or not known>",
        "<urlopen error timed out>",
        "HTTP Error 400: Bad Request",
    ):
        assert redact_secrets(text) == text


def test_quoted_url_inside_an_exception_repr() -> None:
    text = f"URLError('{FRED_URL}')"
    out = redact_secrets(text)
    assert FRED_KEY not in out
    assert out.endswith("')")


# ---------------------------------------------------------------------------
# 3. Headers, dict/JSON reprs, bearer tokens, URL userinfo
# ---------------------------------------------------------------------------


def test_authorization_and_api_key_header_lines() -> None:
    out = redact_secrets(f"Authorization: Bearer {JWT}")
    assert JWT not in out and out == f"Authorization: {REDACTED}"
    out = redact_secrets("X-API-Key: supersecretvalue123")
    assert "supersecretvalue123" not in out and out == f"X-API-Key: {REDACTED}"
    out = redact_secrets("x-api-key=supersecretvalue123")
    assert "supersecretvalue123" not in out


def test_headers_dict_repr() -> None:
    text = (
        "request failed headers={'Authorization': 'Bearer abc.def.ghi', "
        "'X-API-Key': 'k1secret', 'Accept': 'application/json'}"
    )
    out = redact_secrets(text)
    assert "abc.def.ghi" not in out and "k1secret" not in out
    assert "'Accept': 'application/json'" in out


def test_json_body_repr() -> None:
    text = '{"api_key": "sk-live-abcdef", "password":"hunter2", "q": "nurse"}'
    out = redact_secrets(text)
    assert "sk-live-abcdef" not in out and "hunter2" not in out
    assert '"q": "nurse"' in out
    assert json.loads(out)["q"] == "nurse"  # still valid JSON


def test_params_dict_repr_adzuna_style() -> None:
    text = f"params={{'app_id': 'a1b2', 'app_key': '{ADZUNA_KEY}', 'what': 'nurse'}}"
    out = redact_secrets(text)
    assert ADZUNA_KEY not in out
    assert "'app_id': 'a1b2'" in out and "'what': 'nurse'" in out


def test_stray_bearer_token() -> None:
    out = redact_secrets(f"upstream rejected token Bearer {JWT} for user")
    assert JWT not in out and "for user" in out


@pytest.mark.parametrize(
    "name",
    [
        "key",
        "api_key",
        "apikey",
        "app_key",
        "token",
        "access_token",
        "secret",
        "password",
        "authorization",
        "X-API-Key",
    ],
)
def test_every_brief_listed_name_is_redacted_as_a_query_param(name: str) -> None:
    out = redact_secrets(f"https://h.example/p?a=1&{name}=VALUE123456&b=2")
    assert "VALUE123456" not in out
    assert "a=1" in out and "b=2" in out


def test_url_userinfo_password() -> None:
    out = redact_secrets(
        "connect failed: postgresql://postgres:pw123abc@db.host:5432/app"
    )
    assert "pw123abc" not in out
    assert "postgres:" in out and "@db.host:5432/app" in out


# ---------------------------------------------------------------------------
# 4. No false positives on look-alikes / non-secrets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "LLM Router: response appears truncated (output_tokens=300 == max_tokens=300)",
        "monkey=1 turkey=2",
        "series_id=UNRATE file_type=json",
        "contact user@joveo.com about https://example.com:443/path?x=1",
        "token_count=5 keyboard=us",
        # Supabase PostgREST column filters on the cache table's `key` column
        "GET /rest/v1/nova_cache?key=eq.plan%3Aabc123&select=data,expires_at",
        "GET /rest/v1/nova_cache?key=in.(a,b,c)&select=key,data",
        "https://api.x.com:443?email=a@b.com",
    ],
)
def test_benign_text_is_unchanged(text: str) -> None:
    assert redact_secrets(text) == text


def test_clean_message_returns_same_object_fast_path() -> None:
    s = "Pipeline complete in 35.4s enrichment=20.01s"
    assert redact_secrets(s) is s


def test_redaction_is_idempotent() -> None:
    once = redact_secrets(f"{ADZUNA_URL} Authorization: Bearer {JWT} {{'token': 'zz'}}")
    assert redact_secrets(once) == once
    assert "]]" not in once


# ---------------------------------------------------------------------------
# 5. The logging.Filter itself, through real handlers
# ---------------------------------------------------------------------------


@pytest.fixture()
def captured() -> Iterator[tuple[logging.Logger, io.StringIO, logging.StreamHandler]]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    install_log_redaction(handler)
    lg = logging.getLogger("test_log_redaction.capture")
    lg.handlers = [handler]
    lg.propagate = False
    lg.setLevel(logging.DEBUG)
    yield lg, stream, handler
    lg.handlers = []


def test_filter_redacts_percent_args(captured) -> None:  # type: ignore[no-untyped-def]
    lg, stream, _ = captured
    lg.warning("FRED fetch failed: %s", FRED_URL)
    lg.warning("Adzuna HTTP 503 for %s (attempt %d)", ADZUNA_URL, 2)
    text = stream.getvalue()
    assert FRED_KEY not in text and ADZUNA_KEY not in text
    assert "attempt 2" in text and f"api_key={REDACTED}" in text


def test_filter_redacts_preformatted_message(captured) -> None:  # type: ignore[no-untyped-def]
    lg, stream, _ = captured
    lg.warning(f"request to {GEMINI_SSE_URL} failed")
    assert GEMINI_KEY not in stream.getvalue()


def test_filter_redacts_exception_text_in_traceback(captured) -> None:  # type: ignore[no-untyped-def]
    lg, stream, _ = captured
    try:
        raise RuntimeError(
            f"503 Server Error: Service Unavailable for url: {ADZUNA_URL}"
        )
    except RuntimeError:
        lg.error("Adzuna call failed", exc_info=True)
    text = stream.getvalue()
    assert ADZUNA_KEY not in text
    assert "Traceback (most recent call last)" in text, "traceback must still be logged"
    assert "RuntimeError: 503 Server Error" in text


def test_filter_handles_non_string_msg_objects(captured) -> None:  # type: ignore[no-untyped-def]
    lg, stream, _ = captured
    lg.error(ValueError(f"bad url {FRED_URL}"))
    assert FRED_KEY not in stream.getvalue()


def test_filter_never_drops_records_and_never_raises() -> None:
    f = SecretRedactingFilter()
    rec = logging.LogRecord(
        "n", logging.INFO, __file__, 1, "x %d %d", ("only-one",), None
    )
    assert f.filter(rec) is True  # bad args must not raise out of logging
    rec2 = logging.LogRecord("n", logging.INFO, __file__, 1, "plain", None, None)
    assert f.filter(rec2) is True and rec2.getMessage() == "plain"


def test_install_is_idempotent() -> None:
    handler = logging.StreamHandler(io.StringIO())
    install_log_redaction(handler)
    install_log_redaction(handler)
    assert sum(isinstance(x, SecretRedactingFilter) for x in handler.filters) == 1


# ---------------------------------------------------------------------------
# 6. Wired where the app configures logging (monitoring.configure_logging)
# ---------------------------------------------------------------------------


@pytest.fixture()
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_filters = {
        n: logging.getLogger(n).filters[:]
        for n in ("gunicorn.access", "gunicorn.error")
    }
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    for n, flt in saved_filters.items():
        logging.getLogger(n).filters[:] = flt


@pytest.mark.parametrize("json_format", [True, False])
def test_configure_logging_installs_the_filter_on_the_root_handler(
    restore_logging: None, json_format: bool
) -> None:
    import monitoring

    monitoring.configure_logging("INFO", json_format=json_format)
    root = logging.getLogger()
    handler = root.handlers[0]
    assert any(isinstance(f, SecretRedactingFilter) for f in handler.filters)
    stream = io.StringIO()
    handler.setStream(stream)

    child = logging.getLogger("api_enrichment")  # propagates to root like prod
    child.warning("FRED read timed out for %s", FRED_URL)
    try:
        raise OSError(f"403 Client Error: Forbidden for url: {ADZUNA_URL}")
    except OSError:
        child.error("Adzuna failed", exc_info=True)
    out = stream.getvalue()
    assert FRED_KEY not in out and ADZUNA_KEY not in out
    assert "series_id=UNRATE" in out
    assert "403 Client Error" in out, "exception type/message still logged"
    if json_format:
        # every emitted line is still valid single-line JSON with the exception
        lines = [ln for ln in out.splitlines() if ln.strip()]
        parsed = [json.loads(ln) for ln in lines]
        assert any("exception" in p for p in parsed)


def test_configure_logging_scrubs_gunicorn_loggers(restore_logging: None) -> None:
    import monitoring

    monitoring.configure_logging("INFO", json_format=False)
    for name in ("gunicorn.access", "gunicorn.error"):
        assert any(
            isinstance(f, SecretRedactingFilter)
            for f in logging.getLogger(name).filters
        ), name
    # access-log style record: format string + dict args (gunicorn's shape)
    stream = io.StringIO()
    h = logging.StreamHandler(stream)
    lg = logging.getLogger("gunicorn.access")
    saved, saved_prop = lg.handlers[:], lg.propagate
    lg.handlers, lg.propagate = [h], False
    try:
        lg.info(
            '%(h)s "%(r)s" %(s)s',
            {"h": "1.2.3.4", "r": "GET /api/x?api_key=LEAKME123 HTTP/1.1", "s": 200},
        )
    finally:
        lg.handlers, lg.propagate = saved, saved_prop
    assert "LEAKME123" not in stream.getvalue()
    assert "GET /api/x?api_key=[REDACTED] HTTP/1.1" in stream.getvalue()
