#!/usr/bin/env python3
"""Secret redaction for log output (stdlib only).

Prod telemetry 2026-10-01: WARNING lines printed full API keys embedded in URL
query strings (Adzuna ``app_key``, FRED ``api_key``, Gemini ``key`` ...), and
exception strings from urllib / requests echo the request URL too. This module
provides a cheap ``logging.Filter`` that rewrites the *formatted* message and
the formatted exception text so the secret never reaches a handler.

What is redacted (values only; the parameter/header name stays so the line is
still diagnosable):

* query / form / ``k=v`` parameters named key, api_key, api-key, apikey,
  app_key, token, access_token, refresh_token, id_token, secret,
  client_secret, password, passwd, authorization, x-api-key (and a few
  vendor spellings) -- ``?app_key=abc123`` -> ``?app_key=[REDACTED]``
* the same names as quoted dict / JSON keys -- ``{'api_key': 'abc'}``
* ``Authorization:`` / ``X-API-Key:`` style header lines and stray
  ``Bearer <token>`` strings
* the password in URL userinfo -- ``postgres://user:pw@host`` ->
  ``postgres://user:[REDACTED]@host``

PostgREST column filters such as ``?key=eq.cache-key-1`` (the Supabase cache
table is literally keyed by a column named ``key``) are NOT secrets and are
left intact so cache/upsert failures stay diagnosable.

Cheap by construction: all patterns are precompiled, and a single trigger
regex short-circuits the (vast majority of) records that contain none of the
sensitive words. Hostile-input safe: every pattern is linear-time (verified on
1 MB adversarial lines in tests/test_log_redaction_redos.py) and at most
``_MAX_SCAN_CHARS`` characters are scanned -- an oversized message is truncated
with a marker rather than scanned (or emitted) unbounded.
"""

from __future__ import annotations

import logging
import re
from typing import Any

REDACTED = "[REDACTED]"

# Parameter / header / dict-key names whose VALUE is a secret. Longest names
# first so the alternation prefers "api_key" over "key" at the same position.
_SECRET_NAMES = (
    r"x-goog-api-key|x-nova-api-key|x-api-key|subscription-key|registrationkey|"
    r"client_secret|access_token|refresh_token|id_token|authorization|"
    r"api[_-]?key|app[_-]?key|password|passwd|secret|token|key"
)

# Values that are not secrets even under a "key" parameter: PostgREST column
# filter operators (``key=eq.foo``, ``key=in.(a,b)``).
_POSTGREST_OP = r"(?:eq|neq|gt|gte|lt|lte|like|ilike|in|is|not|cs|cd)\."
# Already-redacted marker: keeps the filter idempotent (double-filtering a
# record, or several handlers each running it, never produces ``]]``).
_NOT_REDACTED = r"(?!\[REDACTED\])"

# Cheap pre-check: if none of these appear, no pattern below can match.
_TRIGGER = re.compile(
    r"key|token|secret|passw|authoriz|bearer|://[^/\s]*@", re.IGNORECASE
)

# name=value where name is not the tail of a longer identifier.
_PARAM = re.compile(
    rf"(?<![A-Za-z0-9_])(?P<name>{_SECRET_NAMES})(?P<eq>=)"
    rf"{_NOT_REDACTED}(?!{_POSTGREST_OP})(?P<val>[^&\s\"'<>#)\]}},;]+)",
    re.IGNORECASE,
)

# 'name': 'value' / "name": "value" (dict repr and JSON)
_QUOTED = re.compile(
    rf"(?P<q>[\"'])(?P<name>{_SECRET_NAMES})(?P=q)(?P<sep>\s*:\s*)"
    rf"(?P<vq>[\"']){_NOT_REDACTED}(?P<val>.*?)(?P=vq)",
    re.IGNORECASE,
)

# Authorization: Bearer xyz / X-API-Key: xyz (header lines, not quoted dicts)
_HEADER = re.compile(
    r"(?<![A-Za-z0-9_])(?P<name>proxy-authorization|authorization|x-goog-api-key|"
    r"x-nova-api-key|x-api-key|api-key)(?P<sep>\s*:\s*)"
    rf"(?:(?:bearer|basic|token)\s+)?{_NOT_REDACTED}(?P<val>[^\s,;\"'}}\]]+)",
    re.IGNORECASE,
)

# Stray "Bearer <token>" anywhere
_BEARER = re.compile(
    rf"\b(?P<scheme>bearer)\s+{_NOT_REDACTED}[A-Za-z0-9._~+/=\-]{{8,}}",
    re.IGNORECASE,
)

# scheme://user:password@host -- password only.
#
# Anchored on the literal "://" and deliberately NOT preceded by a scheme matcher
# such as ``[A-Za-z][A-Za-z0-9+.\-]*``: tried at every position of a long run of
# scheme characters that prefix costs O(n) each, i.e. O(n^2) overall (10 KB 0.12 s,
# 80 KB 7 s -- an unauthenticated request body could freeze a worker). Starting at
# "://" is linear (``user`` and ``pw`` both exclude "/", so scans never overlap)
# and it also redacts odd schemes the old prefix could miss.
_USERINFO = re.compile(rf"(?P<head>://[^/\s:@]+:){_NOT_REDACTED}(?P<pw>[^@\s/?#]+)@")

# redact_secrets() scans at most this many characters; the rest of an oversized
# message is dropped (replaced by a marker), never emitted unscanned. Bounds the
# filter's CPU per log record no matter what a client managed to get logged.
_MAX_SCAN_CHARS = 65536


def redact_secrets(text: str) -> str:
    """Return ``text`` with secret values replaced by ``[REDACTED]``.

    Idempotent and safe on any string. Returns the input object unchanged
    (no copy) when nothing sensitive could be present. Only the first
    ``_MAX_SCAN_CHARS`` characters are scanned; anything longer is truncated
    (redact-by-truncation) and ends with a ``[truncated N chars]`` marker, so a
    secret in the unscanned tail can never be emitted and CPU stays bounded.
    """
    if not text:
        return text
    if len(text) > _MAX_SCAN_CHARS:
        dropped = len(text) - _MAX_SCAN_CHARS
        head = _redact_bounded(text[:_MAX_SCAN_CHARS])
        return f"{head}...[truncated {dropped} chars]"
    return _redact_bounded(text)


def _redact_bounded(text: str) -> str:
    """Apply every pattern to ``text`` (already capped by redact_secrets)."""
    if not _TRIGGER.search(text):
        return text
    text = _USERINFO.sub(rf"\g<head>{REDACTED}@", text)
    text = _QUOTED.sub(
        lambda m: f"{m.group('q')}{m.group('name')}{m.group('q')}"
        f"{m.group('sep')}{m.group('vq')}{REDACTED}{m.group('vq')}",
        text,
    )
    text = _HEADER.sub(rf"\g<name>\g<sep>{REDACTED}", text)
    text = _PARAM.sub(rf"\g<name>\g<eq>{REDACTED}", text)
    text = _BEARER.sub(rf"\g<scheme> {REDACTED}", text)
    return text


class SecretRedactingFilter(logging.Filter):
    """Redact secrets from a record's formatted message and exception text.

    Attach to a *handler* (a logger-level filter would miss records that
    propagate up from child loggers). Always returns True: it rewrites, it
    never drops a record.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            msg: Any = record.msg
            if record.args:
                message = record.getMessage()
            elif isinstance(msg, str):
                message = msg
            else:
                message = str(msg)
            cleaned = redact_secrets(message)
            if cleaned is not message and cleaned != message:
                record.msg = cleaned
                record.args = None
            if record.exc_info and record.exc_info[0] is not None:
                exc_text = record.exc_text
                if not exc_text:
                    exc_text = logging.Formatter().formatException(record.exc_info)
                cleaned_exc = redact_secrets(exc_text)
                if cleaned_exc != exc_text or not record.exc_text:
                    # The standard Formatter reuses a pre-set exc_text;
                    # StructuredJsonFormatter prefers it as well.
                    record.exc_text = cleaned_exc
        except Exception:  # never let log scrubbing break logging
            logging.getLogger(__name__).debug("log redaction failed", exc_info=True)
        return True


_FILTER = SecretRedactingFilter()


def install_log_redaction(target: "logging.Handler | logging.Logger") -> None:
    """Attach the shared redaction filter to a handler or logger (idempotent).

    Prefer a handler: a logger-level filter only sees records logged directly
    to that logger, not records propagated from its children.
    """
    if not any(isinstance(f, SecretRedactingFilter) for f in target.filters):
        target.addFilter(_FILTER)
