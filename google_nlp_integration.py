"""Google Cloud Natural Language API integration for sentiment, entities, classification.

Uses service account from GOOGLE_SLIDES_CREDENTIALS_B64 (shared with Sheets/Slides).
Endpoints: language.googleapis.com/v2/documents:{analyzeSentiment,analyzeEntities,classifyText}
Free tier: 5,000 units/month.
"""

from __future__ import annotations

import calendar
import json
import logging
import re
import ssl
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_NLP_BASE = "https://language.googleapis.com/v2/documents"
_MONTHLY_FREE_UNITS = 5000
_usage_lock = threading.Lock()
_usage: Dict[str, Any] = {
    "total_calls": 0,
    "month_start": 0.0,
    "monthly_units": 0,
    "errors": 0,
    "last_call_ts": 0.0,
}


def _get_access_token() -> Optional[str]:
    """Reuse the cached OAuth2 token from sheets_export."""
    try:
        from sheets_export import _get_access_token as _tok

        return _tok()
    except ImportError:
        logger.error("sheets_export module not found; cannot obtain token")
        return None


def _reset_monthly_counter() -> None:
    """Reset monthly counter if a new calendar month has started."""
    now = datetime.now(timezone.utc)
    ms = calendar.timegm(now.replace(day=1, hour=0, minute=0, second=0).timetuple())
    if _usage["month_start"] < ms:
        _usage["month_start"] = ms
        _usage["monthly_units"] = 0


def _track(units: int = 1, error: bool = False) -> None:
    """Thread-safe usage/error tracking."""
    with _usage_lock:
        _reset_monthly_counter()
        if error:
            _usage["errors"] += 1
        else:
            _usage["total_calls"] += 1
            _usage["monthly_units"] += units
            _usage["last_call_ts"] = time.time()


def _nlp_request(endpoint: str, text: str) -> Optional[Dict[str, Any]]:
    """Make authenticated POST to Cloud NL API. Returns parsed JSON or None."""
    token = _get_access_token()
    if not token:
        logger.warning("No access token available for NLP API")
        return None
    body = {"document": {"type": "PLAIN_TEXT", "content": text}, "encodingType": "UTF8"}
    req = urllib.request.Request(
        f"{_NLP_BASE}{endpoint}",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        _track()
        return data
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logger.error(
            "NLP API %s HTTP %s: %s", endpoint, exc.code, err_body, exc_info=True
        )
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("NLP API %s failed: %s", endpoint, exc, exc_info=True)
    _track(error=True)
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def analyze_sentiment(text: str) -> dict:
    """Return {score: float, magnitude: float, label: positive/negative/neutral}."""
    fallback: dict = {"score": 0.0, "magnitude": 0.0, "label": "neutral", "error": True}
    if not (text or "").strip():
        return fallback
    data = _nlp_request(":analyzeSentiment", text)
    if not data:
        return fallback
    s = data.get("documentSentiment") or {}
    score = float(s.get("score") or 0.0)
    mag = float(s.get("magnitude") or 0.0)
    label = "positive" if score > 0.25 else ("negative" if score < -0.25 else "neutral")
    return {"score": score, "magnitude": mag, "label": label}


def extract_entities(text: str) -> List[Dict[str, Any]]:
    """Return [{name, type, salience, metadata}] for people, orgs, locations, skills."""
    if not (text or "").strip():
        return []
    data = _nlp_request(":analyzeEntities", text)
    if not data:
        return []
    keep = {
        "PERSON",
        "ORGANIZATION",
        "LOCATION",
        "EVENT",
        "WORK_OF_ART",
        "CONSUMER_GOOD",
        "OTHER",
    }
    results: List[Dict[str, Any]] = []
    for e in data.get("entities") or []:
        etype = e.get("type") or "UNKNOWN"
        if etype not in keep:
            continue
        results.append(
            {
                "name": e.get("name") or "",
                "type": etype,
                "salience": float(e.get("salience") or 0.0),
                "metadata": e.get("metadata") or {},
            }
        )
    results.sort(key=lambda x: x["salience"], reverse=True)
    return results


def classify_content(text: str) -> List[Dict[str, Any]]:
    """Return [{category, confidence}] content categories."""
    if not (text or "").strip():
        return []
    data = _nlp_request(":classifyText", text)
    if not data:
        return []
    results = [
        {
            "category": c.get("name") or "",
            "confidence": float(c.get("confidence") or 0.0),
        }
        for c in data.get("categories") or []
    ]
    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results


def _compute_readability(text: str) -> float:
    """Flesch-Kincaid-inspired readability score 0-10 (higher = more readable). Stdlib only."""
    sents = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]
    words = text.split()
    if not words or not sents:
        return 5.0
    avg_sent = len(words) / len(sents)
    avg_syl = sum(
        max(len(re.findall(r"[aeiouy]+", w.lower())), 1) for w in words
    ) / len(words)
    fk = 0.39 * avg_sent + 11.8 * avg_syl - 15.59
    return round(max(0.0, min(10.0, 10.0 - fk / 2.0)), 1)


def analyze_job_posting_quality(posting_text: str) -> dict:
    """Combined analysis: sentiment + entities + readability + quality_rating (1-10)."""
    text = (posting_text or "").strip()
    if not text:
        return {
            "sentiment": {"score": 0.0, "magnitude": 0.0, "label": "neutral"},
            "entities": [],
            "categories": [],
            "readability_score": 0.0,
            "quality_rating": 1,
            "quality_factors": ["Empty posting text"],
        }

    sentiment = analyze_sentiment(text)
    entities = extract_entities(text)
    categories = classify_content(text)
    readability = _compute_readability(text)

    factors: List[str] = []
    score = 5.0
    # Sentiment
    if sentiment["label"] == "positive":
        score += 1.0
        factors.append("Positive tone")
    elif sentiment["label"] == "negative":
        score -= 1.5
        factors.append("Negative tone may deter applicants")
    # Entity richness
    ec = len(entities)
    if ec >= 5:
        score += 1.5
        factors.append(f"Rich entity content ({ec} entities)")
    elif ec >= 2:
        score += 0.5
        factors.append(f"Moderate entity content ({ec} entities)")
    else:
        score -= 0.5
        factors.append("Low entity content; add skills, locations, company details")
    # Length
    wc = len(text.split())
    if 150 <= wc <= 800:
        score += 1.0
        factors.append(f"Good length ({wc} words)")
    elif wc < 80:
        score -= 1.0
        factors.append(f"Too short ({wc} words); aim for 150-800")
    elif wc > 1200:
        score -= 0.5
        factors.append(f"Verbose ({wc} words); consider trimming")
    # Readability
    if readability >= 6.0:
        score += 1.0
        factors.append(f"Good readability ({readability}/10)")
    elif readability < 4.0:
        score -= 1.0
        factors.append(f"Low readability ({readability}/10); simplify language")

    return {
        "sentiment": sentiment,
        "entities": entities,
        "categories": categories,
        "readability_score": readability,
        "quality_rating": max(1, min(10, round(score))),
        "quality_factors": factors,
    }


def get_status() -> dict:
    """Health check with usage tracking and free-tier remaining."""
    token = _get_access_token()
    with _usage_lock:
        _reset_monthly_counter()
        return {
            "configured": token is not None,
            "total_calls": _usage["total_calls"],
            "monthly_units": _usage["monthly_units"],
            "monthly_free_limit": _MONTHLY_FREE_UNITS,
            "monthly_remaining": max(0, _MONTHLY_FREE_UNITS - _usage["monthly_units"]),
            "errors": _usage["errors"],
            "last_call_ts": _usage["last_call_ts"],
        }
