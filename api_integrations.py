"""
api_integrations.py -- Unified API Integration Module for Nova AI Suite.

Provides structured access to 8 external data APIs used by the recruitment
advertising platform for labor market intelligence, economic indicators,
occupational data, and federal job listings.

Integrated APIs:
    1. FRED (Federal Reserve Economic Data) -- Unemployment, CPI, GDP, LFPR
    2. Adzuna -- Job market search, salary histograms, top companies
    3. Jooble -- International job aggregator (69 countries)
    4. O*NET -- Occupational skills, technology, related occupations
    5. BEA (Bureau of Economic Analysis) -- State GDP, income, employment
    6. Census (US Census Bureau) -- Population, income, education, demographics
    7. USAJobs -- Federal job listings, hiring paths
    8. BLS (Bureau of Labor Statistics) -- OES, projections, QCEW, CPI

All API calls:
    - Use only stdlib (urllib.request, json, os) -- no external deps
    - Have per-call timeouts (10s default)
    - Are cached in-memory with configurable TTL (1h default)
    - Return None on failure (never raise)
    - Log errors with exc_info=True
    - Read API keys from environment variables

Usage:
    from api_integrations import fred, adzuna, onet, bls

    unemployment = fred.get_unemployment_rate()
    jobs = adzuna.search_jobs("Software Engineer", "us")
    skills = onet.get_skills("15-1252.00")
    oes = bls.get_occupational_employment("15-1252")
"""

from __future__ import annotations

import base64
import http.client
import json
import logging
import os
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

# Persistent HTTPS connection pool -- reuses TCP+TLS across same-host calls
try:
    from http_pool import pooled_request as _pooled_request

    _HAS_POOL = True
except ImportError:
    _HAS_POOL = False

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# UPSTASH REDIS L2 CACHE (persistent across deploys)
# ═══════════════════════════════════════════════════════════════════════════════
try:
    from upstash_cache import cache_get as _redis_get, cache_set as _redis_set

    _redis_available = True
except ImportError:
    _redis_get = _redis_set = None  # type: ignore[assignment]
    _redis_available = False
    logger.info(
        "upstash_cache not available; L2 Redis cache disabled for api_integrations"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED CACHE (L1 in-memory + L2 Upstash Redis)
# ═══════════════════════════════════════════════════════════════════════════════

_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 3600  # 1 hour default
_CACHE_MAX_SIZE = 500  # Max L1 entries -- evict oldest by timestamp when exceeded
_REDIS_TTL_DEFAULT = 86400  # 24 hours for most APIs in Redis

# Real-time API prefixes get shorter Redis TTL (1 hour)
_REALTIME_PREFIXES = ("fred:", "adzuna:jobs:", "jooble:")


def _get_redis_ttl(key: str) -> int:
    """Determine Redis TTL based on data freshness requirements.

    Args:
        key: Cache key string.

    Returns:
        TTL in seconds (3600 for real-time data, 86400 for reference data).
    """
    for prefix in _REALTIME_PREFIXES:
        if key.startswith(prefix):
            return 3600  # 1 hour for real-time data
    return _REDIS_TTL_DEFAULT


def _get_cached(key: str, ttl: int = _CACHE_TTL) -> Any | None:
    """Return cached value from L1 (memory) or L2 (Redis).

    Checks in-memory cache first. On miss, checks Upstash Redis.
    If found in Redis, promotes to in-memory cache for fast subsequent access.

    Args:
        key: Cache key string.
        ttl: Time-to-live in seconds for L1 cache. Defaults to _CACHE_TTL.

    Returns:
        Cached value or None if missing/expired in both layers.
    """
    # L1: in-memory cache
    entry = _cache.get(key)
    if entry is not None:
        timestamp, value = entry
        if time.time() - timestamp > ttl:
            _cache.pop(key, None)
        else:
            return value

    # L2: Upstash Redis
    if _redis_available and _redis_get:
        try:
            redis_val = _redis_get(f"api:{key}")
            if redis_val is not None:
                # Promote to L1
                _cache[key] = (time.time(), redis_val)
                return redis_val
        except Exception as redis_err:
            logger.debug(f"Redis L2 get failed for {key}: {redis_err}")

    return None


def _set_cached(key: str, value: Any) -> None:
    """Store a value in both L1 (memory) and L2 (Redis) caches.

    Args:
        key: Cache key string.
        value: Any serializable value to cache.
    """
    # L1: in-memory -- enforce size cap by evicting oldest entries
    if len(_cache) >= _CACHE_MAX_SIZE and key not in _cache:
        # Evict oldest entries (by timestamp) to make room
        sorted_keys = sorted(_cache, key=lambda k: _cache[k][0])
        for stale_key in sorted_keys[: len(_cache) - _CACHE_MAX_SIZE + 1]:
            _cache.pop(stale_key, None)
    _cache[key] = (time.time(), value)

    # L2: Upstash Redis (fire-and-forget, non-blocking)
    if _redis_available and _redis_set:
        try:
            redis_ttl = _get_redis_ttl(key)
            _redis_set(
                f"api:{key}", value, ttl_seconds=redis_ttl, category="api_integrations"
            )
        except Exception as redis_err:
            logger.debug(f"Redis L2 set failed for {key}: {redis_err}")


def clear_cache() -> None:
    """Clear the entire in-memory L1 cache. Redis L2 cache expires via TTL."""
    _cache.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_DEFAULT_TIMEOUT = 10  # seconds

# Secure SSL context using system CA bundle (default behavior)
_ssl_ctx = ssl.create_default_context()
# check_hostname=True and verify_mode=CERT_REQUIRED are the defaults —
# explicitly set for clarity and to prevent accidental regression.
_ssl_ctx.check_hostname = True
_ssl_ctx.verify_mode = ssl.CERT_REQUIRED

# Separate unverified context for self-signed cert APIs only.
# Used as a per-call fallback — never applied globally.
_ssl_ctx_unverified = ssl.create_default_context()
_ssl_ctx_unverified.check_hostname = False
_ssl_ctx_unverified.verify_mode = ssl.CERT_NONE


def _http_get(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """Perform an HTTP GET and return parsed JSON.

    Uses pooled HTTPS connections when available (saves ~100-200ms per call
    by reusing TCP + TLS handshakes for same-host requests).

    Args:
        url: Fully-qualified URL to fetch.
        headers: Optional HTTP headers dict.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON (dict or list) or None on any failure.
    """
    if _HAS_POOL:
        return _http_get_pooled(url, headers=headers, timeout=timeout)
    return _http_get_urllib(url, headers=headers, timeout=timeout)


def _http_get_pooled(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """HTTP GET via persistent connection pool."""
    try:
        resp = _pooled_request(
            url, method="GET", headers=headers or {}, timeout=timeout, ssl_ctx=_ssl_ctx
        )
        if resp.status >= 400:
            if resp.status in (401, 403):
                logger.warning(f"HTTP {resp.status} for {url} (auth/credential issue)")
            elif resp.status == 429:
                logger.warning(f"HTTP 429 for {url} (rate limited)")
            else:
                logger.warning(f"HTTP {resp.status} for {url}")
            return None
        raw = resp.read().decode("utf-8")
        return json.loads(raw)
    except ssl.SSLError:
        logger.warning(
            f"SSL verification failed for {url}, retrying without verification"
        )
        try:
            resp = _pooled_request(
                url,
                method="GET",
                headers=headers or {},
                timeout=timeout,
                ssl_ctx=_ssl_ctx_unverified,
            )
            if resp.status >= 400:
                if resp.status in (401, 403):
                    logger.warning(
                        f"HTTP {resp.status} for {url} (auth/credential issue, unverified SSL)"
                    )
                else:
                    logger.warning(f"HTTP {resp.status} for {url} (unverified SSL)")
                return None
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
        except (http.client.HTTPException, OSError, TimeoutError) as exc:
            logger.warning(f"HTTP GET failed for {url} (unverified retry): {exc}")
            return None
    except json.JSONDecodeError:
        logger.warning(f"JSON decode error for {url}")
        return None
    except TimeoutError:
        logger.warning(f"Timeout fetching {url}")
        return None
    except (http.client.HTTPException, OSError) as exc:
        logger.warning(f"HTTP GET failed for {url}: {exc}")
        return None


def _http_get_urllib(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """HTTP GET via urllib (fallback when pool unavailable)."""
    try:
        req = urllib.request.Request(url, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.URLError as ssl_exc:
            if "CERTIFICATE_VERIFY_FAILED" in str(ssl_exc) or "SSL" in str(ssl_exc):
                logger.warning(
                    f"SSL verification failed for {url}, retrying without verification"
                )
                req = urllib.request.Request(url, headers=headers or {})
                with urllib.request.urlopen(
                    req, timeout=timeout, context=_ssl_ctx_unverified
                ) as resp:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
            raise
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            logger.warning(f"HTTP {exc.code} for {url} (auth/credential issue)")
        elif exc.code == 429:
            logger.warning(f"HTTP 429 for {url} (rate limited)")
        else:
            logger.warning(f"HTTP {exc.code} for {url}")
        return None
    except urllib.error.URLError as exc:
        logger.warning(f"URL error for {url}: {exc.reason}")
        return None
    except json.JSONDecodeError:
        logger.warning(f"JSON decode error for {url}")
        return None
    except TimeoutError:
        logger.warning(f"Timeout fetching {url}")
        return None
    except OSError:
        logger.warning(f"OS error fetching {url}")
        return None


def _http_post(
    url: str,
    data: dict,
    headers: dict[str, str] | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """Perform an HTTP POST with JSON body and return parsed JSON.

    Uses pooled HTTPS connections when available.

    Args:
        url: Fully-qualified URL to post to.
        data: Dict to JSON-encode as the request body.
        headers: Optional HTTP headers dict.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON (dict or list) or None on any failure.
    """
    body = json.dumps(data).encode("utf-8")
    hdrs: dict[str, str] = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)

    if _HAS_POOL:
        return _http_post_pooled(url, body=body, headers=hdrs, timeout=timeout)
    return _http_post_urllib(url, body=body, headers=hdrs, timeout=timeout)


def _http_post_pooled(
    url: str,
    body: bytes,
    headers: dict[str, str],
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """HTTP POST via persistent connection pool."""
    try:
        resp = _pooled_request(
            url,
            method="POST",
            body=body,
            headers=headers,
            timeout=timeout,
            ssl_ctx=_ssl_ctx,
        )
        if resp.status >= 400:
            logger.warning(f"HTTP {resp.status} for POST {url}")
            return None
        raw = resp.read().decode("utf-8")
        return json.loads(raw)
    except ssl.SSLError:
        logger.warning(
            f"SSL verification failed for POST {url}, retrying without verification"
        )
        try:
            resp = _pooled_request(
                url,
                method="POST",
                body=body,
                headers=headers,
                timeout=timeout,
                ssl_ctx=_ssl_ctx_unverified,
            )
            if resp.status >= 400:
                logger.warning(f"HTTP {resp.status} for POST {url} (unverified SSL)")
                return None
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
        except (http.client.HTTPException, OSError, TimeoutError) as exc:
            logger.warning(f"HTTP POST failed for {url} (unverified retry): {exc}")
            return None
    except json.JSONDecodeError:
        logger.warning(f"JSON decode error for POST {url}")
        return None
    except TimeoutError:
        logger.warning(f"Timeout posting to {url}")
        return None
    except (http.client.HTTPException, OSError) as exc:
        logger.warning(f"HTTP POST failed for {url}: {exc}")
        return None


def _http_post_urllib(
    url: str,
    body: bytes,
    headers: dict[str, str],
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """HTTP POST via urllib (fallback when pool unavailable)."""
    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.URLError as ssl_exc:
            if "CERTIFICATE_VERIFY_FAILED" in str(ssl_exc) or "SSL" in str(ssl_exc):
                logger.warning(
                    f"SSL verification failed for POST {url}, retrying without verification"
                )
                req = urllib.request.Request(
                    url, data=body, headers=headers, method="POST"
                )
                with urllib.request.urlopen(
                    req, timeout=timeout, context=_ssl_ctx_unverified
                ) as resp:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
            raise
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            logger.warning(f"HTTP {exc.code} for POST {url} (auth/credential issue)")
        else:
            logger.warning(f"HTTP {exc.code} for POST {url}")
        return None
    except urllib.error.URLError as exc:
        logger.warning(f"URL error for POST {url}: {exc.reason}")
        return None
    except json.JSONDecodeError:
        logger.warning(f"JSON decode error for POST {url}")
        return None
    except TimeoutError:
        logger.warning(f"Timeout posting to {url}")
        return None
    except OSError:
        logger.warning(f"OS error posting to {url}")
        return None


def _http_get_basic_auth(
    url: str,
    username: str,
    password: str,
    headers: dict[str, str] | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict | list | None:
    """Perform an HTTP GET with Basic Auth and return parsed JSON.

    Args:
        url: Fully-qualified URL to fetch.
        username: Basic auth username.
        password: Basic auth password.
        headers: Optional additional HTTP headers.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON or None on any failure.
    """
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    hdrs = {"Authorization": f"Basic {credentials}"}
    if headers:
        hdrs.update(headers)
    return _http_get(url, headers=hdrs, timeout=timeout)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. FRED (Federal Reserve Economic Data)
# ═══════════════════════════════════════════════════════════════════════════════


class FREDClient:
    """Client for the Federal Reserve Economic Data (FRED) API.

    Provides access to unemployment, CPI, GDP, and labor force participation
    data at national and state levels.

    Env var: FRED_API_KEY
    Docs: https://fred.stlouisfed.org/docs/api/fred/
    """

    BASE_URL = "https://api.stlouisfed.org/fred"

    # Series IDs
    SERIES_UNRATE = "UNRATE"  # National unemployment rate
    SERIES_CPI = "CPIAUCSL"  # Consumer Price Index
    SERIES_GDP = "GDP"  # Gross Domestic Product
    SERIES_CIVPART = "CIVPART"  # Civilian Labor Force Participation Rate

    def __init__(self) -> None:
        """Initialize FRED client with API key from environment."""
        self.api_key = os.environ.get("FRED_API_KEY") or ""

    def _is_configured(self) -> bool:
        """Check if API key is available."""
        return bool(self.api_key)

    def _build_series_url(self, series_id: str, limit: int = 12) -> str:
        """Build a FRED series observations URL.

        Args:
            series_id: FRED series identifier.
            limit: Max number of observations to return.

        Returns:
            Fully-qualified URL string.
        """
        params = urllib.parse.urlencode(
            {
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": limit,
            }
        )
        return f"{self.BASE_URL}/series/observations?{params}"

    @staticmethod
    def _validate_series_id(series_id: str) -> bool:
        """Validate a FRED series ID format before making API call.

        Checks that the series ID contains only alphanumeric characters
        and is a reasonable length (1-30 chars). For LAUS series, validates
        the expected 20-character format.

        Args:
            series_id: FRED series identifier to validate.

        Returns:
            True if the series ID appears valid.
        """
        if not series_id or len(series_id) > 30:
            return False
        if not series_id.replace("_", "").isalnum():
            return False
        # LAUS series must be exactly 20 chars: LAUST + 2-digit FIPS + 13 digits
        if series_id.startswith("LAUST"):
            if len(series_id) != 20:
                return False
            # After LAUST, rest must be digits
            if not series_id[5:].isdigit():
                return False
        return True

    def _fetch_series(self, series_id: str, limit: int = 12) -> dict | None:
        """Fetch a FRED series with caching.

        Args:
            series_id: FRED series identifier.
            limit: Max observations.

        Returns:
            Parsed observations dict or None.
        """
        if not self._is_configured():
            logger.warning("FRED_API_KEY not set, skipping FRED request")
            return None

        if not self._validate_series_id(series_id):
            logger.warning(
                f"Invalid FRED series ID format: {series_id} (len={len(series_id)}), skipping"
            )
            return None

        cache_key = f"fred:{series_id}:{limit}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = self._build_series_url(series_id, limit)
        data = _http_get(url)
        if data is None:
            return None

        observations = data.get("observations")
        if observations is None:
            logger.warning(f"FRED response missing 'observations' for {series_id}")
            return None

        result = {
            "series_id": series_id,
            "count": len(observations),
            "observations": [
                {
                    "date": obs.get("date") or "",
                    "value": obs.get("value") or "",
                }
                for obs in observations
            ],
        }
        _set_cached(cache_key, result)
        return result

    def get_unemployment_rate(self, state_code: str | None = None) -> dict | None:
        """Get unemployment rate data (national or by state).

        Args:
            state_code: Two-letter US state abbreviation (e.g., 'CA').
                        None for national rate.

        Returns:
            Dict with series_id, count, and observations list, or None.
        """
        if state_code:
            # State unemployment: LAUST{FIPS}0000000000003
            fips = _state_to_fips(state_code.upper())
            if not fips:
                logger.warning(f"Unknown state code: {state_code}")
                return None
            series_id = f"LAUST{fips}0000000000003"
        else:
            series_id = self.SERIES_UNRATE
        return self._fetch_series(series_id)

    def get_cpi_data(self, months: int = 12) -> dict | None:
        """Get Consumer Price Index (CPI-U) data.

        Args:
            months: Number of recent months to retrieve.

        Returns:
            Dict with CPI observations or None.
        """
        return self._fetch_series(self.SERIES_CPI, limit=months)

    def get_gdp_growth(self) -> dict | None:
        """Get GDP growth data (last 8 quarters).

        Returns:
            Dict with GDP observations or None.
        """
        return self._fetch_series(self.SERIES_GDP, limit=8)

    def get_labor_force_participation(
        self, state_code: str | None = None
    ) -> dict | None:
        """Get labor force participation rate.

        Args:
            state_code: Two-letter state code for state-level data.
                        None for national rate.

        Returns:
            Dict with participation rate observations or None.
        """
        if state_code:
            fips = _state_to_fips(state_code.upper())
            if not fips:
                logger.warning(f"Unknown state code: {state_code}")
                return None
            # State LFPR series follows a different pattern
            series_id = f"LAUST{fips}0000000000006"
        else:
            series_id = self.SERIES_CIVPART
        return self._fetch_series(series_id)

    def get_u6_rate(self) -> dict | None:
        """Get U-6 unemployment rate (broader measure including underemployment).

        Returns:
            Dict with U6RATE observations or None.
        """
        return self._fetch_series("U6RATE")

    def get_jolts_data(self, industry: str = "total") -> dict | None:
        """Get JOLTS job openings, hires, separations by industry.

        Args:
            industry: Industry key. One of: total, manufacturing,
                      healthcare, tech, retail, construction.

        Returns:
            Dict with JOLTS series data keyed by metric name, or None.
        """
        JOLTS_SERIES: dict[str, dict[str, str]] = {
            "total": {
                "openings": "JTSJOL",
                "hires": "JTSHIL",
                "separations": "JTSTSL",
                "quits": "JTSQUL",
            },
            "manufacturing": {"openings": "JTS3000JOL"},
            "healthcare": {"openings": "JTS6200JOL"},
            "tech": {"openings": "JTS5100JOL"},
            "retail": {"openings": "JTS4400JOL"},
            "construction": {"openings": "JTS2300JOL"},
        }

        if not self._is_configured():
            logger.warning("FRED_API_KEY not set, skipping JOLTS request")
            return None

        industry_key = industry.lower().strip()
        series_map = JOLTS_SERIES.get(industry_key)
        if series_map is None:
            # Fall back to total if unknown industry
            series_map = JOLTS_SERIES["total"]

        cache_key = f"fred:jolts:{industry_key}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        result: dict[str, Any] = {"industry": industry_key, "metrics": {}}
        for metric_name, series_id in series_map.items():
            try:
                data = self._fetch_series(series_id, limit=12)
                if data and data.get("observations"):
                    obs = data["observations"]
                    values = []
                    for o in obs:
                        try:
                            values.append(float(o["value"]))
                        except (ValueError, KeyError):
                            continue
                    result["metrics"][metric_name] = {
                        "series_id": series_id,
                        "latest_value": values[0] if values else None,
                        "latest_date": obs[0].get("date") or "" if obs else "",
                        "trend_3m": (
                            round(values[0] - values[2], 2)
                            if len(values) >= 3
                            else None
                        ),
                        "observations": obs[:6],
                    }
            except Exception as exc:
                logger.error(
                    f"JOLTS fetch failed for {series_id}: {exc}", exc_info=True
                )
                continue

        if result["metrics"]:
            _set_cached(cache_key, result)
            return result
        return None

    def get_labor_market_summary(self) -> dict | None:
        """Get a comprehensive labor market summary: unemployment, U6, LFPR, JOLTS.

        Returns:
            Dict with combined labor market indicators, or None.
        """
        cache_key = "fred:labor_market_summary"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        summary: dict[str, Any] = {"source": "FRED", "indicators": {}}

        # Unemployment rate
        try:
            unemp = self.get_unemployment_rate()
            if unemp and unemp.get("observations"):
                obs = unemp["observations"]
                summary["indicators"]["unemployment_rate"] = {
                    "value": obs[0].get("value") or "",
                    "date": obs[0].get("date") or "",
                    "series_id": "UNRATE",
                }
        except Exception as exc:
            logger.error(f"Labor summary UNRATE failed: {exc}", exc_info=True)

        # U-6 rate
        try:
            u6 = self.get_u6_rate()
            if u6 and u6.get("observations"):
                obs = u6["observations"]
                summary["indicators"]["u6_rate"] = {
                    "value": obs[0].get("value") or "",
                    "date": obs[0].get("date") or "",
                    "series_id": "U6RATE",
                    "description": "Total unemployed + marginally attached + part-time for economic reasons",
                }
        except Exception as exc:
            logger.error(f"Labor summary U6RATE failed: {exc}", exc_info=True)

        # Labor force participation
        try:
            lfpr = self.get_labor_force_participation()
            if lfpr and lfpr.get("observations"):
                obs = lfpr["observations"]
                summary["indicators"]["labor_force_participation"] = {
                    "value": obs[0].get("value") or "",
                    "date": obs[0].get("date") or "",
                    "series_id": "CIVPART",
                }
        except Exception as exc:
            logger.error(f"Labor summary CIVPART failed: {exc}", exc_info=True)

        # JOLTS total
        try:
            jolts = self.get_jolts_data("total")
            if jolts and jolts.get("metrics"):
                summary["indicators"]["jolts"] = jolts["metrics"]
        except Exception as exc:
            logger.error(f"Labor summary JOLTS failed: {exc}", exc_info=True)

        if summary["indicators"]:
            _set_cached(cache_key, summary)
            return summary
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 1b. REMOTEOK (Remote Job Market Intelligence -- zero auth)
# ═══════════════════════════════════════════════════════════════════════════════

# Disk cache for RemoteOK (24hr TTL, avoids hammering the API)
_REMOTEOK_DISK_CACHE_DIR = Path(tempfile.gettempdir()) / "nova_remoteok_cache"
_REMOTEOK_DISK_CACHE_TTL = 86400  # 24 hours


def _remoteok_disk_cache_get(key: str) -> Any | None:
    """Read a cached value from disk if it exists and is not expired.

    Args:
        key: Cache key (sanitized to filesystem-safe name).

    Returns:
        Cached value or None if missing/expired.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    cache_file = _REMOTEOK_DISK_CACHE_DIR / f"{safe_key}.json"
    try:
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < _REMOTEOK_DISK_CACHE_TTL:
                return data.get("value")
            # Expired -- remove
            cache_file.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError, KeyError) as exc:
        logger.debug(f"RemoteOK disk cache read error for {key}: {exc}")
    return None


def _remoteok_disk_cache_set(key: str, value: Any) -> None:
    """Write a value to disk cache.

    Args:
        key: Cache key.
        value: JSON-serializable value.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    try:
        _REMOTEOK_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = _REMOTEOK_DISK_CACHE_DIR / f"{safe_key}.json"
        cache_file.write_text(
            json.dumps({"ts": time.time(), "value": value}),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.debug(f"RemoteOK disk cache write error for {key}: {exc}")


class RemoteOKClient:
    """Remote job market intelligence from RemoteOK (zero auth).

    Fetches the full JSON feed from remoteok.io/api and provides
    search, salary stats, and trending skills analysis.

    No API key required. Data is cached to disk for 24 hours.
    Docs: https://remoteok.io/api
    """

    BASE_URL = "https://remoteok.io/api"

    def _fetch_all_jobs(self) -> list[dict]:
        """Fetch the full RemoteOK job feed with disk caching.

        Returns:
            List of job dicts from the API, or empty list on failure.
        """
        cache_key = "remoteok_all_jobs"

        # L1: in-memory
        cached_mem = _get_cached(cache_key, ttl=3600)
        if cached_mem is not None:
            return cached_mem

        # L2: disk cache (24hr)
        cached_disk = _remoteok_disk_cache_get(cache_key)
        if cached_disk is not None:
            _set_cached(cache_key, cached_disk)
            return cached_disk

        # Fetch from API
        try:
            data = _http_get(
                self.BASE_URL,
                headers={"User-Agent": "NovaAISuite/1.0"},
                timeout=15,
            )
            if data is None:
                return []
            # RemoteOK returns a JSON array; first element is metadata
            if isinstance(data, list) and len(data) > 1:
                jobs = [j for j in data[1:] if isinstance(j, dict)]
            elif isinstance(data, list):
                jobs = []
            else:
                jobs = []

            if jobs:
                _set_cached(cache_key, jobs)
                _remoteok_disk_cache_set(cache_key, jobs)
            return jobs
        except Exception as exc:
            logger.error(f"RemoteOK fetch failed: {exc}", exc_info=True)
            return []

    def search_jobs(self, query: str, limit: int = 20) -> list[dict]:
        """Search remote job listings by keyword.

        Args:
            query: Search keyword (matched against title, company, tags).
            limit: Max results to return. Defaults to 20.

        Returns:
            List of dicts with: title, company, salary_min, salary_max,
            tags, location, date, url.
        """
        cache_key = f"remoteok:search:{query.lower().strip()}:{limit}"
        cached = _get_cached(cache_key, ttl=3600)
        if cached is not None:
            return cached

        all_jobs = self._fetch_all_jobs()
        query_lower = query.lower().strip()
        matches: list[dict] = []

        for job in all_jobs:
            searchable = " ".join(
                [
                    job.get("position") or "",
                    job.get("company") or "",
                    " ".join(job.get("tags") or []),
                    job.get("description") or "",
                ]
            ).lower()

            if query_lower in searchable:
                matches.append(
                    {
                        "title": job.get("position") or "",
                        "company": job.get("company") or "",
                        "salary_min": job.get("salary_min") or None,
                        "salary_max": job.get("salary_max") or None,
                        "tags": job.get("tags") or [],
                        "location": job.get("location") or "Remote",
                        "date": job.get("date") or "",
                        "url": job.get("url") or "",
                    }
                )
                if len(matches) >= limit:
                    break

        _set_cached(cache_key, matches)
        return matches

    def get_salary_stats(self, role: str) -> dict:
        """Get salary statistics for a role from remote job listings.

        Aggregates salary_min and salary_max across matching listings to
        compute median, 25th percentile, 75th percentile, and sample size.

        Args:
            role: Job title to search for (e.g., 'Software Engineer').

        Returns:
            Dict with median, p25, p75, sample_size, companies, and source.
        """
        cache_key = f"remoteok:salary:{role.lower().strip()}"
        cached = _get_cached(cache_key, ttl=3600)
        if cached is not None:
            return cached

        all_jobs = self._fetch_all_jobs()
        role_lower = role.lower().strip()
        salaries: list[float] = []
        companies: set[str] = set()

        for job in all_jobs:
            position = (job.get("position") or "").lower()
            if role_lower not in position:
                continue

            sal_min = job.get("salary_min")
            sal_max = job.get("salary_max")
            company = job.get("company") or ""

            if sal_min and sal_max:
                try:
                    avg = (float(sal_min) + float(sal_max)) / 2
                    salaries.append(avg)
                    if company:
                        companies.add(company)
                except (ValueError, TypeError):
                    continue
            elif sal_min:
                try:
                    salaries.append(float(sal_min))
                    if company:
                        companies.add(company)
                except (ValueError, TypeError):
                    continue

        if not salaries:
            result = {
                "role": role,
                "sample_size": 0,
                "source": "RemoteOK",
                "note": "No salary data found for this role in remote listings",
            }
            _set_cached(cache_key, result)
            return result

        salaries.sort()
        n = len(salaries)
        result = {
            "role": role,
            "median": round(salaries[n // 2]),
            "p25": round(salaries[n // 4]) if n >= 4 else round(salaries[0]),
            "p75": round(salaries[3 * n // 4]) if n >= 4 else round(salaries[-1]),
            "sample_size": n,
            "companies": sorted(companies)[:15],
            "source": "RemoteOK",
        }
        _set_cached(cache_key, result)
        return result

    def get_trending_skills(self, limit: int = 20) -> list[dict]:
        """Get most common skills/tags from recent remote jobs.

        Counts tag frequencies across all current listings.

        Args:
            limit: Max tags to return. Defaults to 20.

        Returns:
            List of dicts with: tag, count, percentage.
        """
        cache_key = f"remoteok:trending:{limit}"
        cached = _get_cached(cache_key, ttl=3600)
        if cached is not None:
            return cached

        all_jobs = self._fetch_all_jobs()
        tag_counts: dict[str, int] = {}
        total_jobs = len(all_jobs)

        for job in all_jobs:
            tags = job.get("tags") or []
            for tag in tags:
                if isinstance(tag, str) and tag.strip():
                    tag_lower = tag.strip().lower()
                    tag_counts[tag_lower] = tag_counts.get(tag_lower, 0) + 1

        sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
        result = [
            {
                "tag": tag,
                "count": count,
                "percentage": (
                    round(count / total_jobs * 100, 1) if total_jobs > 0 else 0
                ),
            }
            for tag, count in sorted_tags[:limit]
        ]

        _set_cached(cache_key, result)
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# 2. ADZUNA (Job Market Data)
# ═══════════════════════════════════════════════════════════════════════════════


class AdzunaClient:
    """Client for the Adzuna Job Search API.

    Provides job search, salary histograms, top companies, and job counts
    across multiple countries.

    Env vars: ADZUNA_APP_ID, ADZUNA_APP_KEY
    Docs: https://developer.adzuna.com/
    """

    BASE_URL = "https://api.adzuna.com/v1/api/jobs"

    # Supported country codes
    SUPPORTED_COUNTRIES = {
        "us",
        "gb",
        "ca",
        "au",
        "de",
        "fr",
        "in",
        "nl",
        "br",
        "pl",
        "ru",
        "za",
        "nz",
        "sg",
        "at",
        "ch",
        "it",
        "es",
    }

    def __init__(self) -> None:
        """Initialize Adzuna client with credentials from environment."""
        self.app_id = os.environ.get("ADZUNA_APP_ID") or ""
        self.app_key = os.environ.get("ADZUNA_APP_KEY") or ""

    def _is_configured(self) -> bool:
        """Check if both app_id and app_key are set."""
        return bool(self.app_id and self.app_key)

    def _build_url(
        self,
        country: str,
        endpoint: str,
        params: dict[str, str | int] | None = None,
    ) -> str:
        """Build an Adzuna API URL.

        Args:
            country: Two-letter country code.
            endpoint: API endpoint path (e.g., 'search/1').
            params: Additional query parameters.

        Returns:
            Fully-qualified URL.
        """
        country = (
            country.lower() if country.lower() in self.SUPPORTED_COUNTRIES else "us"
        )
        base_params: dict[str, str | int] = {
            "app_id": self.app_id,
            "app_key": self.app_key,
        }
        if params:
            base_params.update(params)
        qs = urllib.parse.urlencode(base_params)
        return f"{self.BASE_URL}/{country}/{endpoint}?{qs}"

    def search_jobs(
        self,
        role: str,
        location: str = "us",
        page: int = 1,
        results_per_page: int = 10,
    ) -> dict | None:
        """Search for job listings.

        Args:
            role: Job title or keyword to search for.
            location: Country code (e.g., 'us', 'gb').
            page: Page number (1-indexed).
            results_per_page: Number of results per page (max 50).

        Returns:
            Dict with results list, count, and mean salary, or None.
        """
        if not self._is_configured():
            logger.warning("Adzuna credentials not set")
            return None

        cache_key = f"adzuna:search:{role}:{location}:{page}:{results_per_page}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = self._build_url(
            location,
            f"search/{page}",
            {"what": role, "results_per_page": results_per_page},
        )
        data = _http_get(url)
        if data is None:
            return None

        result = {
            "count": data.get("count") or 0,
            "mean": data.get("mean") or 0,
            "results": [
                {
                    "title": r.get("title") or "",
                    "company": (r.get("company") or {}).get("display_name") or "",
                    "location": (r.get("location") or {}).get("display_name") or "",
                    "salary_min": r.get("salary_min") or 0,
                    "salary_max": r.get("salary_max") or 0,
                    "created": r.get("created") or "",
                    "redirect_url": r.get("redirect_url") or "",
                }
                for r in (data.get("results") or [])
            ],
        }
        _set_cached(cache_key, result)
        return result

    def get_salary_histogram(self, role: str, location: str = "us") -> dict | None:
        """Get salary histogram for a role.

        Args:
            role: Job title or keyword.
            location: Country code.

        Returns:
            Dict with histogram buckets or None.
        """
        if not self._is_configured():
            logger.warning("Adzuna credentials not set")
            return None

        cache_key = f"adzuna:histogram:{role}:{location}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = self._build_url(location, "histogram", {"what": role})
        data = _http_get(url)
        if data is None:
            return None

        result = {"histogram": data.get("histogram") or {}}
        _set_cached(cache_key, result)
        return result

    def get_top_companies(self, role: str, location: str = "us") -> dict | None:
        """Get top companies hiring for a role.

        Args:
            role: Job title or keyword.
            location: Country code.

        Returns:
            Dict with leaderboard of companies or None.
        """
        if not self._is_configured():
            logger.warning("Adzuna credentials not set")
            return None

        cache_key = f"adzuna:companies:{role}:{location}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = self._build_url(location, "top_companies", {"what": role})
        data = _http_get(url)
        if data is None:
            return None

        result = {
            "leaderboard": [
                {
                    "canonical_name": item.get("canonical_name") or "",
                    "count": item.get("count") or 0,
                }
                for item in (data.get("leaderboard") or [])
            ]
        }
        _set_cached(cache_key, result)
        return result

    def get_job_count(self, role: str, location: str = "us") -> dict | None:
        """Get total job count for a role in a country.

        Args:
            role: Job title or keyword.
            location: Country code.

        Returns:
            Dict with total count or None.
        """
        if not self._is_configured():
            logger.warning("Adzuna credentials not set")
            return None

        cache_key = f"adzuna:count:{role}:{location}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        # Use search endpoint page 1 with 1 result to get count
        url = self._build_url(
            location, "search/1", {"what": role, "results_per_page": 1}
        )
        data = _http_get(url)
        if data is None:
            return None

        result = {"count": data.get("count") or 0, "role": role, "location": location}
        _set_cached(cache_key, result)
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. JOOBLE (Job Aggregator)
# ═══════════════════════════════════════════════════════════════════════════════


class JoobleClient:
    """Client for the Jooble Job Search API.

    Provides international job search across 69 countries via POST requests.
    Subject to 500 request/day limit.

    Env var: JOOBLE_API_KEY
    Docs: https://jooble.org/api/about
    """

    BASE_URL = "https://jooble.org/api"

    def __init__(self) -> None:
        """Initialize Jooble client with API key from environment."""
        self.api_key = os.environ.get("JOOBLE_API_KEY") or ""
        self._request_count = 0
        self._request_day: str = ""
        self._DAILY_LIMIT = 500

    def _is_configured(self) -> bool:
        """Check if API key is set."""
        return bool(self.api_key)

    def _check_rate_limit(self) -> bool:
        """Check and update daily rate limit.

        Returns:
            True if within limit, False if exceeded.
        """
        today = time.strftime("%Y-%m-%d")
        if self._request_day != today:
            self._request_day = today
            self._request_count = 0
        if self._request_count >= self._DAILY_LIMIT:
            logger.warning(f"Jooble daily limit reached ({self._DAILY_LIMIT} requests)")
            return False
        return True

    def search_jobs(
        self,
        keywords: str,
        location: str = "",
        page: int = 1,
    ) -> dict | None:
        """Search for jobs on Jooble.

        Args:
            keywords: Search keywords (job title, skills, etc.).
            location: Location string (city, state, country).
            page: Page number (1-indexed).

        Returns:
            Dict with jobs list and totalCount, or None.
        """
        if not self._is_configured():
            logger.warning("JOOBLE_API_KEY not set")
            return None

        if not self._check_rate_limit():
            return None

        cache_key = f"jooble:search:{keywords}:{location}:{page}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/{self.api_key}"
        payload = {
            "keywords": keywords,
            "location": location,
            "page": str(page),
        }

        data = _http_post(url, payload)
        self._request_count += 1

        if data is None:
            return None

        result = {
            "totalCount": data.get("totalCount") or 0,
            "jobs": [
                {
                    "title": job.get("title") or "",
                    "location": job.get("location") or "",
                    "snippet": job.get("snippet") or "",
                    "salary": job.get("salary") or "",
                    "source": job.get("source") or "",
                    "type": job.get("type") or "",
                    "link": job.get("link") or "",
                    "company": job.get("company") or "",
                    "updated": job.get("updated") or "",
                }
                for job in (data.get("jobs") or [])
            ],
        }
        _set_cached(cache_key, result)
        return result

    @property
    def requests_remaining(self) -> int:
        """Return estimated remaining daily requests."""
        today = time.strftime("%Y-%m-%d")
        if self._request_day != today:
            return self._DAILY_LIMIT
        return max(0, self._DAILY_LIMIT - self._request_count)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. O*NET (Occupational Data)
# ═══════════════════════════════════════════════════════════════════════════════


class ONetClient:
    """Client for O*NET Web Services API.

    Provides occupational data including skills, technology requirements,
    related occupations, and detailed occupation profiles.

    Env vars: ONET_USERNAME (default 'joveo'), ONET_API_KEY or ONET_PASSWORD (password)
    Docs: https://services.onetcenter.org/reference/
    """

    BASE_URL = "https://services.onetcenter.org/ws"

    def __init__(self) -> None:
        """Initialize O*NET client with credentials from environment."""
        self.username = os.environ.get("ONET_USERNAME") or "joveo"
        self.password = (
            os.environ.get("ONET_API_KEY") or os.environ.get("ONET_PASSWORD") or ""
        )
        self._auth_failed = False  # Track persistent auth failures

    def _is_configured(self) -> bool:
        """Check if API credentials are available."""
        return bool(self.password)

    def _fetch(self, path: str) -> dict | list | None:
        """Fetch an O*NET endpoint with Basic Auth and caching.

        Args:
            path: API path (e.g., '/search?keyword=nurse').

        Returns:
            Parsed JSON or None.
        """
        if not self._is_configured():
            logger.warning("ONET_API_KEY not set, skipping O*NET request")
            return None

        if self._auth_failed:
            # Don't keep hitting a 401 -- skip until restart
            return None

        cache_key = f"onet:{path}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}{path}"
        headers = {"Accept": "application/json"}
        data = _http_get_basic_auth(url, self.username, self.password, headers=headers)
        if data is None:
            # Track consecutive failures -- likely auth issue if first call fails
            if not hasattr(self, "_consecutive_failures"):
                self._consecutive_failures = 0
            self._consecutive_failures += 1
            if self._consecutive_failures >= 2:
                logger.warning(
                    "O*NET API returned None %d times -- disabling for this session. "
                    "Check ONET_USERNAME and ONET_API_KEY env vars.",
                    self._consecutive_failures,
                )
                self._auth_failed = True
            return None
        self._consecutive_failures = 0
        _set_cached(cache_key, data)
        return data

    def search_occupations(self, keyword: str) -> list[dict] | None:
        """Search for occupations by keyword.

        Args:
            keyword: Search term (e.g., 'software engineer').

        Returns:
            List of occupation dicts with code and title, or None.
        """
        encoded = urllib.parse.quote(keyword)
        data = self._fetch(f"/online/search?keyword={encoded}")
        if data is None:
            return None

        occupations = data.get("occupation") or []
        return [
            {
                "code": occ.get("code") or "",
                "title": occ.get("title") or "",
                "relevance_score": occ.get("relevance_score") or 0,
            }
            for occ in occupations
        ]

    def get_occupation_details(self, soc_code: str) -> dict | None:
        """Get detailed occupation profile.

        Args:
            soc_code: O*NET-SOC code (e.g., '15-1252.00').

        Returns:
            Dict with occupation details or None.
        """
        data = self._fetch(f"/online/occupations/{soc_code}")
        if data is None:
            return None

        return {
            "code": data.get("code") or "",
            "title": data.get("title") or "",
            "description": data.get("description") or "",
            "sample_of_reported_titles": data.get("sample_of_reported_titles") or {},
            "updated": data.get("updated") or "",
        }

    def get_skills(self, soc_code: str) -> list[dict] | None:
        """Get skills required for an occupation.

        Args:
            soc_code: O*NET-SOC code (e.g., '15-1252.00').

        Returns:
            List of skill dicts with name and importance, or None.
        """
        data = self._fetch(f"/online/occupations/{soc_code}/summary/skills")
        if data is None:
            return None

        elements = data.get("element") or []
        return [
            {
                "id": elem.get("id") or "",
                "name": elem.get("name") or "",
                "description": elem.get("description") or "",
                "score": (elem.get("score") or {}).get("value") or 0,
            }
            for elem in elements
        ]

    def get_technology_skills(self, soc_code: str) -> list[dict] | None:
        """Get technology skills for an occupation.

        Args:
            soc_code: O*NET-SOC code (e.g., '15-1252.00').

        Returns:
            List of technology skill dicts or None.
        """
        data = self._fetch(f"/online/occupations/{soc_code}/summary/technology_skills")
        if data is None:
            return None

        categories = data.get("category") or []
        results: list[dict] = []
        for cat in categories:
            cat_title = cat.get("title") or {}
            cat_name = (
                cat_title.get("name") or ""
                if isinstance(cat_title, dict)
                else str(cat_title)
            )
            examples = cat.get("example") or []
            for ex in examples:
                results.append(
                    {
                        "category": cat_name,
                        "name": (
                            ex.get("name") or "" if isinstance(ex, dict) else str(ex)
                        ),
                        "hot_technology": (
                            ex.get("hot_technology") or False
                            if isinstance(ex, dict)
                            else False
                        ),
                    }
                )
        return results

    def get_related_occupations(self, soc_code: str) -> list[dict] | None:
        """Get occupations related by task/skill similarity (v2.0).

        Uses the /details/ endpoint for richer similarity data including
        fitness scores compared to the /summary/ endpoint.

        Args:
            soc_code: O*NET-SOC code (e.g., '15-1252.00').

        Returns:
            List of related occupation dicts with code, title, and
            similarity score, or None.
        """
        data = self._fetch(
            f"/online/occupations/{soc_code}/details/related_occupations"
        )
        if data is None:
            # Fallback to summary endpoint if details unavailable
            data = self._fetch(
                f"/online/occupations/{soc_code}/summary/related_occupations"
            )
        if data is None:
            return None

        occupations = data.get("occupation") or []
        return [
            {
                "code": occ.get("code") or "",
                "title": occ.get("title") or "",
                "fitness": occ.get("fitness") or occ.get("relevance_score") or 0,
            }
            for occ in occupations
        ]

    def get_knowledge(self, soc_code: str) -> list[dict] | None:
        """Get knowledge requirements for an occupation (v2.0).

        Args:
            soc_code: O*NET-SOC code (e.g., '15-1252.00').

        Returns:
            List of knowledge area dicts with name, description, and
            importance score (1-5), or None.
        """
        data = self._fetch(f"/online/occupations/{soc_code}/summary/knowledge")
        if data is None:
            return None

        elements = data.get("element") or []
        return [
            {
                "id": elem.get("id") or "",
                "name": elem.get("name") or "",
                "description": elem.get("description") or "",
                "score": (elem.get("score") or {}).get("value") or 0,
            }
            for elem in elements
        ]

    def search_my_next_move(self, keyword: str) -> list[dict] | None:
        """Search the My Next Move career explorer (v2.0).

        My Next Move provides career exploration data optimized for
        career changers and job seekers, with simpler descriptions
        and career pathway information.

        Args:
            keyword: Search term (e.g., 'data analyst').

        Returns:
            List of career dicts with code, title, and tags, or None.
        """
        encoded = urllib.parse.quote(keyword)
        data = self._fetch(f"/mnm/search?keyword={encoded}")
        if data is None:
            return None

        careers = data.get("career") or []
        return [
            {
                "code": c.get("code") or "",
                "title": c.get("title") or "",
                "tags": c.get("tags") or {},
            }
            for c in careers
        ]

    def get_skills_profile(self, soc_code: str) -> dict | None:
        """Get complete skills + knowledge profile with importance scores (v2.0).

        Combines skills, technology skills, knowledge areas, and related
        occupations into a single comprehensive profile for an occupation.

        Args:
            soc_code: O*NET-SOC code (e.g., '15-1252.00').

        Returns:
            Dict with skills, technology_skills, knowledge, and
            related_occupations, or None if all calls fail.
        """
        skills = self.get_skills(soc_code)
        tech_skills = self.get_technology_skills(soc_code)
        knowledge = self.get_knowledge(soc_code)
        related = self.get_related_occupations(soc_code)

        # Return None only if everything failed
        if all(x is None for x in [skills, tech_skills, knowledge, related]):
            return None

        profile: dict[str, Any] = {"soc_code": soc_code}
        if skills is not None:
            profile["skills"] = skills
        if tech_skills is not None:
            profile["technology_skills"] = tech_skills
        if knowledge is not None:
            profile["knowledge"] = knowledge
        if related is not None:
            profile["related_occupations"] = related
        return profile


# ═══════════════════════════════════════════════════════════════════════════════
# 5. BEA (Bureau of Economic Analysis)
# ═══════════════════════════════════════════════════════════════════════════════

# Disk cache for BEA (7-day TTL -- BEA data updates quarterly)
_BEA_DISK_CACHE_DIR = Path(tempfile.gettempdir()) / "nova_bea_cache"
_BEA_DISK_CACHE_TTL = 604800  # 7 days in seconds


def _bea_disk_cache_get(key: str) -> Any | None:
    """Read a cached BEA value from disk if it exists and is not expired.

    Args:
        key: Cache key (sanitized to filesystem-safe name).

    Returns:
        Cached value or None if missing/expired.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    cache_file = _BEA_DISK_CACHE_DIR / f"{safe_key}.json"
    try:
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < _BEA_DISK_CACHE_TTL:
                return data.get("value")
            # Expired -- remove
            cache_file.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError, KeyError) as exc:
        logger.debug(f"BEA disk cache read error for {key}: {exc}")
    return None


def _bea_disk_cache_set(key: str, value: Any) -> None:
    """Write a BEA value to disk cache (7-day TTL).

    Args:
        key: Cache key.
        value: JSON-serializable value.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    try:
        _BEA_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = _BEA_DISK_CACHE_DIR / f"{safe_key}.json"
        cache_file.write_text(
            json.dumps({"ts": time.time(), "value": value}),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.debug(f"BEA disk cache write error for {key}: {exc}")


# State FIPS codes for all 50 states + DC
STATE_FIPS: dict[str, str] = {
    "Alabama": "01000",
    "Alaska": "02000",
    "Arizona": "04000",
    "Arkansas": "05000",
    "California": "06000",
    "Colorado": "08000",
    "Connecticut": "09000",
    "Delaware": "10000",
    "District of Columbia": "11000",
    "Florida": "12000",
    "Georgia": "13000",
    "Hawaii": "15000",
    "Idaho": "16000",
    "Illinois": "17000",
    "Indiana": "18000",
    "Iowa": "19000",
    "Kansas": "20000",
    "Kentucky": "21000",
    "Louisiana": "22000",
    "Maine": "23000",
    "Maryland": "24000",
    "Massachusetts": "25000",
    "Michigan": "26000",
    "Minnesota": "27000",
    "Mississippi": "28000",
    "Missouri": "29000",
    "Montana": "30000",
    "Nebraska": "31000",
    "Nevada": "32000",
    "New Hampshire": "33000",
    "New Jersey": "34000",
    "New Mexico": "35000",
    "New York": "36000",
    "North Carolina": "37000",
    "North Dakota": "38000",
    "Ohio": "39000",
    "Oklahoma": "40000",
    "Oregon": "41000",
    "Pennsylvania": "42000",
    "Rhode Island": "44000",
    "South Carolina": "45000",
    "South Dakota": "46000",
    "Tennessee": "47000",
    "Texas": "48000",
    "Utah": "49000",
    "Vermont": "50000",
    "Virginia": "51000",
    "Washington": "53000",
    "West Virginia": "54000",
    "Wisconsin": "55000",
    "Wyoming": "56000",
}

# Reverse lookup: abbreviation -> full name
_STATE_ABBREV_TO_NAME: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


def _resolve_state_fips(state: str) -> str | None:
    """Resolve a state name or abbreviation to its FIPS code.

    Args:
        state: State name (e.g., 'California') or abbreviation (e.g., 'CA').

    Returns:
        FIPS code string (e.g., '06000') or None if not found.
    """
    if not state:
        return None
    state_clean = state.strip()
    # Try direct name match (case-insensitive)
    for name, fips in STATE_FIPS.items():
        if name.lower() == state_clean.lower():
            return fips
    # Try abbreviation
    full_name = _STATE_ABBREV_TO_NAME.get(state_clean.upper())
    if full_name:
        return STATE_FIPS.get(full_name)
    return None


class BEAClient:
    """Client for the Bureau of Economic Analysis (BEA) API.

    Provides regional economic data: state GDP by industry, per capita personal
    income, employment by industry, and metro area income data.

    Uses a 3-tier cache: L1 in-memory (1h) + L2 Upstash Redis (24h) + L3 disk (7 days).
    BEA data updates quarterly, so aggressive caching is appropriate.

    Env var: BEA_API_KEY
    Docs: https://apps.bea.gov/api/
    """

    BASE_URL = "https://apps.bea.gov/api/data/"

    def __init__(self) -> None:
        """Initialize BEA client with API key from environment."""
        self._api_key = os.environ.get("BEA_API_KEY") or ""
        self._enabled = bool(self._api_key)

    def _is_configured(self) -> bool:
        """Check if API key is set."""
        return self._enabled

    def _fetch(self, params: dict[str, str], cache_suffix: str = "") -> dict | None:
        """Fetch BEA data with 3-tier caching (memory + Redis + disk).

        Args:
            params: Query parameters for the BEA API.
            cache_suffix: Optional suffix for readable cache key.

        Returns:
            Parsed BEAAPI.Results dict or None on failure.
        """
        if not self._is_configured():
            logger.warning("BEA_API_KEY not set -- skipping BEA request")
            return None

        # Inject common params
        params["UserID"] = self._api_key
        params["method"] = "GetData"
        params["ResultFormat"] = "JSON"

        cache_key = f"bea:{cache_suffix or json.dumps(params, sort_keys=True)}"

        # L1 + L2: in-memory + Redis
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        # L3: disk cache (7-day TTL)
        disk_cached = _bea_disk_cache_get(cache_key)
        if disk_cached is not None:
            # Promote to L1 + L2
            _set_cached(cache_key, disk_cached)
            return disk_cached

        try:
            qs = urllib.parse.urlencode(params)
            url = f"{self.BASE_URL}?{qs}"
            data = _http_get(url, timeout=15)
            if data is None:
                return None

            # BEA wraps everything in BEAAPI.Results
            bea_api = data.get("BEAAPI") or {}
            results = bea_api.get("Results") or {}
            if not results:
                logger.warning("BEA response missing BEAAPI.Results")
                return None

            # Store in all 3 cache tiers
            _set_cached(cache_key, results)
            _bea_disk_cache_set(cache_key, results)
            return results

        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            logger.error(f"BEA API request failed: {exc}", exc_info=True)
            return None

    def get_regional_gdp(
        self, state_fips: str, industry: str = "ALL", year: str = "LAST5"
    ) -> dict | None:
        """Get GDP by industry for a specific state.

        Args:
            state_fips: State FIPS code (e.g., '06000' for California).
            industry: Industry filter ('ALL' for all industries).
            year: Year specification ('LAST5', 'LAST10', or specific year).

        Returns:
            Dict with state GDP data by industry or None.
        """
        return self._fetch(
            {
                "DataSetName": "Regional",
                "TableName": "CAGDP2",
                "LineCode": "1",
                "GeoFips": state_fips,
                "Year": year,
            },
            cache_suffix=f"gdp:{state_fips}:{industry}:{year}",
        )

    def get_personal_income(self, state_fips: str, year: str = "LAST5") -> dict | None:
        """Get per capita personal income by state.

        Args:
            state_fips: State FIPS code (e.g., '36000' for New York).
            year: Year specification.

        Returns:
            Dict with per capita personal income data or None.
        """
        return self._fetch(
            {
                "DataSetName": "Regional",
                "TableName": "CAINC1",
                "LineCode": "3",
                "GeoFips": state_fips,
                "Year": year,
            },
            cache_suffix=f"income:{state_fips}:{year}",
        )

    def get_employment_by_industry(
        self, state_fips: str, year: str = "LAST5"
    ) -> dict | None:
        """Get employment counts by industry for a state.

        Args:
            state_fips: State FIPS code.
            year: Year specification.

        Returns:
            Dict with employment data by industry or None.
        """
        return self._fetch(
            {
                "DataSetName": "Regional",
                "TableName": "CAEMP25N",
                "LineCode": "10",
                "GeoFips": state_fips,
                "Year": year,
            },
            cache_suffix=f"employment:{state_fips}:{year}",
        )

    def get_metro_income(self, metro_fips: str, year: str = "LAST5") -> dict | None:
        """Get income data for a metro area (MSA).

        Args:
            metro_fips: Metro area FIPS code (e.g., '12420' for Austin-Round Rock).
            year: Year specification.

        Returns:
            Dict with metro area income data or None.
        """
        return self._fetch(
            {
                "DataSetName": "Regional",
                "TableName": "CAINC1",
                "LineCode": "3",
                "GeoFips": metro_fips,
                "Year": year,
            },
            cache_suffix=f"metro_income:{metro_fips}:{year}",
        )

    def get_gdp_by_state_all(self, year: str = "LAST5") -> dict | None:
        """Get GDP for all states (aggregate view).

        Args:
            year: Year specification.

        Returns:
            Dict with all-state GDP data or None.
        """
        return self._fetch(
            {
                "DataSetName": "Regional",
                "TableName": "CAGDP2",
                "LineCode": "1",
                "GeoFips": "STATE",
                "Year": year,
            },
            cache_suffix=f"gdp_all_states:{year}",
        )

    def get_personal_income_all_states(self, year: str = "LAST5") -> dict | None:
        """Get personal income for all states.

        Args:
            year: Year specification.

        Returns:
            Dict with all-state income data or None.
        """
        return self._fetch(
            {
                "DataSetName": "Regional",
                "TableName": "CAINC1",
                "LineCode": "3",
                "GeoFips": "STATE",
                "Year": year,
            },
            cache_suffix=f"income_all_states:{year}",
        )

    def query_regional_economics(
        self,
        state: str = "",
        metro_fips: str = "",
        metric_type: str = "all",
    ) -> dict:
        """High-level tool: query regional economics for Nova AI.

        Combines GDP, income, and employment data into a single enriched response
        for media planning context.

        Args:
            state: State name or abbreviation (e.g., 'California' or 'CA').
            metro_fips: Metro area FIPS code (e.g., '12420' for Austin).
            metric_type: 'gdp', 'income', 'employment', or 'all'.

        Returns:
            Dict with economic data, trends, and media planning context.
        """
        result: dict[str, Any] = {
            "source": "bea_regional_economics",
            "api": "Bureau of Economic Analysis",
        }

        if not self._is_configured():
            result["error"] = "BEA API not configured (BEA_API_KEY missing)"
            return result

        # Resolve state
        state_fips: str | None = None
        state_name = ""
        if state:
            state_fips = _resolve_state_fips(state)
            if not state_fips:
                result["error"] = f"Unknown state: '{state}'"
                result["hint"] = (
                    "Use full state name (e.g., 'California') or abbreviation (e.g., 'CA')"
                )
                return result
            # Find display name
            for name, fips in STATE_FIPS.items():
                if fips == state_fips:
                    state_name = name
                    break

        result["state"] = state_name or state
        result["state_fips"] = state_fips or ""

        # Fetch requested metrics
        metrics_to_fetch = (
            ["gdp", "income", "employment"] if metric_type == "all" else [metric_type]
        )

        for metric in metrics_to_fetch:
            try:
                if metric == "gdp" and state_fips:
                    gdp_data = self.get_regional_gdp(state_fips)
                    if gdp_data:
                        result["gdp"] = self._extract_data_rows(gdp_data, limit=20)

                elif metric == "income" and state_fips:
                    income_data = self.get_personal_income(state_fips)
                    if income_data:
                        result["personal_income"] = self._extract_data_rows(
                            income_data, limit=10
                        )

                elif metric == "employment" and state_fips:
                    emp_data = self.get_employment_by_industry(state_fips)
                    if emp_data:
                        result["employment"] = self._extract_data_rows(
                            emp_data, limit=20
                        )

            except (TypeError, KeyError, ValueError) as exc:
                logger.error(
                    f"BEA data extraction error for {metric}: {exc}",
                    exc_info=True,
                )
                result[f"{metric}_error"] = str(exc)

        # Metro income if requested
        if metro_fips:
            try:
                metro_data = self.get_metro_income(metro_fips)
                if metro_data:
                    result["metro_income"] = self._extract_data_rows(
                        metro_data, limit=10
                    )
                    result["metro_fips"] = metro_fips
            except (TypeError, KeyError, ValueError) as exc:
                logger.error(f"BEA metro income error: {exc}", exc_info=True)
                result["metro_error"] = str(exc)

        return result

    @staticmethod
    def _extract_data_rows(results: dict, limit: int = 20) -> list[dict[str, str]]:
        """Extract data rows from BEA Results payload.

        Args:
            results: BEA Results dict (contains 'Data' key).
            limit: Maximum number of rows to return.

        Returns:
            List of dicts with cleaned data rows.
        """
        data_rows = results.get("Data") or []
        if not isinstance(data_rows, list):
            return []
        cleaned: list[dict[str, str]] = []
        for row in data_rows[:limit]:
            if not isinstance(row, dict):
                continue
            cleaned.append(
                {
                    "geo": row.get("GeoName") or "",
                    "year": row.get("TimePeriod") or "",
                    "value": row.get("DataValue") or "",
                    "unit": row.get("UNIT_MULT_DESC") or row.get("CL_UNIT") or "",
                    "description": row.get("Description") or "",
                }
            )
        return cleaned


# ═══════════════════════════════════════════════════════════════════════════════
# 6. CENSUS (US Census Bureau)
# ═══════════════════════════════════════════════════════════════════════════════

# Disk cache for Census data (30-day TTL -- Census ACS updates annually)
_CENSUS_DISK_CACHE_DIR = Path(tempfile.gettempdir()) / "nova_census_cache"
_CENSUS_DISK_CACHE_TTL = 86400 * 30  # 30 days


def _census_disk_cache_get(key: str) -> Any | None:
    """Read a cached Census value from disk if it exists and is not expired.

    Args:
        key: Cache key (sanitized to filesystem-safe name).

    Returns:
        Cached value or None if missing/expired.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    cache_file = _CENSUS_DISK_CACHE_DIR / f"{safe_key}.json"
    try:
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < _CENSUS_DISK_CACHE_TTL:
                return data.get("value")
            cache_file.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError, KeyError) as exc:
        logger.debug(f"Census disk cache read error for {key}: {exc}")
    return None


def _census_disk_cache_set(key: str, value: Any) -> None:
    """Write a Census value to disk cache.

    Args:
        key: Cache key.
        value: JSON-serializable value.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    try:
        _CENSUS_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = _CENSUS_DISK_CACHE_DIR / f"{safe_key}.json"
        cache_file.write_text(
            json.dumps({"ts": time.time(), "value": value}),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.debug(f"Census disk cache write error for {key}: {exc}")


# County FIPS codes for the top 50 US metro areas (primary county)
_METRO_COUNTY_FIPS: dict[str, tuple[str, str]] = {
    "new york": ("36", "061"),
    "los angeles": ("06", "037"),
    "chicago": ("17", "031"),
    "dallas": ("48", "113"),
    "houston": ("48", "201"),
    "washington": ("11", "001"),
    "philadelphia": ("42", "101"),
    "miami": ("12", "086"),
    "atlanta": ("13", "121"),
    "boston": ("25", "025"),
    "phoenix": ("04", "013"),
    "san francisco": ("06", "075"),
    "riverside": ("06", "065"),
    "detroit": ("26", "163"),
    "seattle": ("53", "033"),
    "minneapolis": ("27", "053"),
    "san diego": ("06", "073"),
    "tampa": ("12", "057"),
    "denver": ("08", "031"),
    "st louis": ("29", "510"),
    "baltimore": ("24", "510"),
    "orlando": ("12", "095"),
    "charlotte": ("37", "119"),
    "san antonio": ("48", "029"),
    "portland": ("41", "051"),
    "sacramento": ("06", "067"),
    "pittsburgh": ("42", "003"),
    "austin": ("48", "453"),
    "las vegas": ("32", "003"),
    "cincinnati": ("39", "061"),
    "kansas city": ("29", "095"),
    "columbus": ("39", "049"),
    "indianapolis": ("18", "097"),
    "cleveland": ("39", "035"),
    "san jose": ("06", "085"),
    "nashville": ("47", "037"),
    "virginia beach": ("51", "810"),
    "providence": ("44", "007"),
    "milwaukee": ("55", "079"),
    "jacksonville": ("12", "031"),
    "memphis": ("47", "157"),
    "oklahoma city": ("40", "109"),
    "raleigh": ("37", "183"),
    "richmond": ("51", "760"),
    "new orleans": ("22", "071"),
    "louisville": ("21", "111"),
    "salt lake city": ("49", "035"),
    "hartford": ("09", "003"),
    "buffalo": ("36", "029"),
    "birmingham": ("01", "073"),
}


class CensusClient:
    """Client for the US Census Bureau API.

    Provides population, income, education, workforce demographics,
    commuting patterns, and industry employment from the American
    Community Survey (ACS) 5-year estimates and Current Population Survey.

    Env var: CENSUS_API_KEY
    Docs: https://www.census.gov/data/developers.html
    """

    BASE_URL = "https://api.census.gov/data"

    # ACS 5-year estimates year (update as new data releases)
    ACS_YEAR = "2023"

    def __init__(self) -> None:
        """Initialize Census client with API key from environment."""
        self.api_key = os.environ.get("CENSUS_API_KEY") or ""

    def _is_configured(self) -> bool:
        """Check if API key is set."""
        return bool(self.api_key)

    def _fetch_acs(
        self,
        variables: str,
        geo: str = "state:*",
        extra_params: dict[str, str] | None = None,
    ) -> dict | None:
        """Fetch ACS 5-year estimate data with 3-tier caching (memory/Redis/disk).

        Args:
            variables: Comma-separated Census variable codes.
            geo: Geographic filter string (e.g., 'state:*').
            extra_params: Additional query parameters.

        Returns:
            Dict with headers and data rows, or None.
        """
        if not self._is_configured():
            logger.warning("CENSUS_API_KEY not set")
            return None

        cache_key = f"census:{variables}:{geo}"

        # L1+L2: memory + Redis (24h TTL -- Census data updates annually)
        cached = _get_cached(cache_key, ttl=86400)
        if cached is not None:
            return cached

        # L3: disk cache (30-day TTL for Census data)
        cached_disk = _census_disk_cache_get(cache_key)
        if cached_disk is not None:
            _set_cached(cache_key, cached_disk)
            return cached_disk

        params: dict[str, str] = {
            "get": variables,
            "for": geo,
            "key": self.api_key,
        }
        if extra_params:
            params.update(extra_params)

        qs = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}/{self.ACS_YEAR}/acs/acs5?{qs}"
        data = _http_get(url)
        if data is None or not isinstance(data, list) or len(data) < 2:
            return None

        headers = data[0]
        rows = data[1:]
        result: dict[str, Any] = {
            "headers": headers,
            "data": rows,
            "count": len(rows),
        }
        _set_cached(cache_key, result)
        _census_disk_cache_set(cache_key, result)
        return result

    def _fetch_cps(
        self,
        variables: str,
        geo: str = "state:*",
    ) -> dict | None:
        """Fetch Current Population Survey (CPS) Basic Monthly data.

        Args:
            variables: Comma-separated CPS variable codes.
            geo: Geographic filter string.

        Returns:
            Dict with headers and data rows, or None.
        """
        if not self._is_configured():
            logger.warning("CENSUS_API_KEY not set")
            return None

        cache_key = f"census_cps:{variables}:{geo}"
        cached = _get_cached(cache_key, ttl=86400)
        if cached is not None:
            return cached

        cached_disk = _census_disk_cache_get(cache_key)
        if cached_disk is not None:
            _set_cached(cache_key, cached_disk)
            return cached_disk

        params: dict[str, str] = {
            "get": variables,
            "for": geo,
            "key": self.api_key,
        }
        qs = urllib.parse.urlencode(params)
        # CPS Basic Monthly -- January 2024
        url = f"{self.BASE_URL}/2024/cps/basic/jan?{qs}"
        data = _http_get(url)
        if data is None or not isinstance(data, list) or len(data) < 2:
            return None

        result: dict[str, Any] = {
            "headers": data[0],
            "data": data[1:],
            "count": len(data) - 1,
        }
        _set_cached(cache_key, result)
        _census_disk_cache_set(cache_key, result)
        return result

    def get_population_by_state(self) -> dict | None:
        """Get total population by state.

        Returns:
            Dict with state population data or None.
        """
        # B01003_001E = Total Population
        return self._fetch_acs("NAME,B01003_001E")

    def get_median_income_by_state(self) -> dict | None:
        """Get median household income by state.

        Returns:
            Dict with state income data or None.
        """
        # B19013_001E = Median Household Income
        return self._fetch_acs("NAME,B19013_001E")

    def get_education_by_state(self) -> dict | None:
        """Get educational attainment by state (bachelor's degree or higher).

        Returns:
            Dict with education data or None.
        """
        # B15003_022E = Bachelor's degree, B15003_001E = Total population 25+
        return self._fetch_acs(
            "NAME,B15003_001E,B15003_022E,B15003_023E,B15003_024E,B15003_025E"
        )

    def get_workforce_demographics(self, state_fips: str | None = None) -> dict | None:
        """Get workforce demographics (employment status by age/sex).

        Args:
            state_fips: Two-digit state FIPS code (e.g., '06' for CA).
                        None for all states.

        Returns:
            Dict with workforce demographic data or None.
        """
        # B23001_001E = Total, B23001_006E = In labor force (male 16-19)
        # Using broader employment status variables
        variables = "NAME,B23025_001E,B23025_002E,B23025_003E,B23025_004E,B23025_005E,B23025_006E,B23025_007E"
        if state_fips:
            geo = f"state:{state_fips}"
        else:
            geo = "state:*"
        return self._fetch_acs(variables, geo=geo)

    def get_labor_force(self, state_fips: str) -> dict | None:
        """Get labor force statistics from Current Population Survey.

        Variables: PEMLR (employment status), PRTAGE (age),
                   PESEX (sex), GTMETSTA (metro status).

        Args:
            state_fips: Two-digit state FIPS code (e.g., '06' for CA).

        Returns:
            Dict with CPS labor force data or None.
        """
        return self._fetch_cps(
            "PEMLR,PRTAGE,PESEX,GTMETSTA",
            geo=f"state:{state_fips}",
        )

    def get_demographics(self, state_fips: str, county_fips: str = "") -> dict | None:
        """Get population demographics from ACS 5-year estimates.

        Variables: B01001_001E (total pop), B19013_001E (median income),
                   B15003_022E (bachelor's degree), B23025_002E (in labor force).

        Args:
            state_fips: Two-digit state FIPS code.
            county_fips: Three-digit county FIPS code (optional).

        Returns:
            Dict with demographic data or None.
        """
        variables = "NAME,B01001_001E,B19013_001E,B15003_022E,B23025_002E"
        if county_fips:
            geo = f"county:{county_fips}&in=state:{state_fips}"
        else:
            geo = f"state:{state_fips}"
        return self._fetch_acs(variables, geo=geo)

    def get_workforce_education(self, state_fips: str) -> dict | None:
        """Get educational attainment of the workforce.

        Uses B15003 table: total 25+ pop, high school, some college,
        associates, bachelor's, master's, professional, doctorate.

        Args:
            state_fips: Two-digit state FIPS code.

        Returns:
            Dict with education level data or None.
        """
        variables = (
            "NAME,B15003_001E,"  # Total pop 25+
            "B15003_017E,B15003_018E,"  # High school diploma, GED
            "B15003_019E,B15003_020E,"  # Some college <1yr, 1yr+
            "B15003_021E,"  # Associate's
            "B15003_022E,"  # Bachelor's
            "B15003_023E,"  # Master's
            "B15003_024E,"  # Professional
            "B15003_025E"  # Doctorate
        )
        return self._fetch_acs(variables, geo=f"state:{state_fips}")

    def get_commute_data(self, state_fips: str) -> dict | None:
        """Get commuting patterns including remote work percentage.

        Uses B08006 (means of transportation to work) and
        B08301 (work from home) variables.

        Args:
            state_fips: Two-digit state FIPS code.

        Returns:
            Dict with commute/remote work data or None.
        """
        variables = "NAME,B08006_001E,B08006_017E,B08301_001E,B08301_021E"
        return self._fetch_acs(variables, geo=f"state:{state_fips}")

    def get_industry_employment(self, state_fips: str) -> dict | None:
        """Get employment by industry from ACS.

        Uses C24030 table (industry by occupation for the civilian
        employed population 16+).

        Args:
            state_fips: Two-digit state FIPS code.

        Returns:
            Dict with industry employment data or None.
        """
        variables = (
            "NAME,C24030_001E,C24030_002E,C24030_029E,"
            "C24030_036E,C24030_043E,C24030_050E"
        )
        return self._fetch_acs(variables, geo=f"state:{state_fips}")

    def get_workforce_profile(self, state: str, city: str = "") -> dict[str, Any]:
        """Build a comprehensive workforce profile for a state/metro area.

        Combines demographics, education, commute, and industry data
        into a single enriched profile for Nova AI tool consumption.

        Args:
            state: Two-letter state abbreviation (e.g., 'CA').
            city: Optional city name for metro-level county data.

        Returns:
            Dict with workforce profile or error info.
        """
        fips = _state_to_fips(state.upper()) if state else None
        if not fips:
            return {"error": f"Unknown state: {state}", "source": "Census-ACS"}

        profile: dict[str, Any] = {
            "state": state.upper(),
            "source": "Census-ACS",
            "acs_year": self.ACS_YEAR,
        }

        # --- Demographics (state or county level) ---
        county_fips = ""
        city_lower = city.lower().strip() if city else ""
        if city_lower and city_lower in _METRO_COUNTY_FIPS:
            metro_state, county_fips = _METRO_COUNTY_FIPS[city_lower]
            if metro_state != fips:
                county_fips = ""  # State mismatch; fall back to state-level

        if city_lower:
            profile["city"] = city.strip().title()

        try:
            demo = self.get_demographics(fips, county_fips)
            if demo and demo.get("data"):
                row = demo["data"][0]
                headers = demo["headers"]
                idx = {h: i for i, h in enumerate(headers)}
                total_pop = int(row[idx.get("B01001_001E", 0)] or 0)
                median_income = int(row[idx.get("B19013_001E", 0)] or 0)
                labor_force = int(row[idx.get("B23025_002E", 0)] or 0)
                profile["population"] = total_pop
                profile["median_household_income"] = median_income
                profile["labor_force_size"] = labor_force
                if total_pop > 0:
                    profile["labor_force_participation_pct"] = round(
                        labor_force / total_pop * 100, 1
                    )
        except Exception as exc:
            logger.error(f"Census demographics fetch failed: {exc}", exc_info=True)

        # --- Education levels ---
        try:
            edu = self.get_workforce_education(fips)
            if edu and edu.get("data"):
                row = edu["data"][0]
                headers = edu["headers"]
                idx = {h: i for i, h in enumerate(headers)}
                total_25 = int(row[idx.get("B15003_001E", 0)] or 0)
                bachelors = int(row[idx.get("B15003_022E", 0)] or 0)
                masters = int(row[idx.get("B15003_023E", 0)] or 0)
                professional = int(row[idx.get("B15003_024E", 0)] or 0)
                doctorate = int(row[idx.get("B15003_025E", 0)] or 0)
                associates = int(row[idx.get("B15003_021E", 0)] or 0)
                hs_diploma = int(row[idx.get("B15003_017E", 0)] or 0)
                ged = int(row[idx.get("B15003_018E", 0)] or 0)

                bachelors_plus = bachelors + masters + professional + doctorate
                if total_25 > 0:
                    profile["education"] = {
                        "population_25_plus": total_25,
                        "high_school_pct": round(
                            (hs_diploma + ged) / total_25 * 100, 1
                        ),
                        "associates_pct": round(associates / total_25 * 100, 1),
                        "bachelors_plus_pct": round(bachelors_plus / total_25 * 100, 1),
                        "graduate_pct": round(
                            (masters + professional + doctorate) / total_25 * 100, 1
                        ),
                    }
        except Exception as exc:
            logger.error(f"Census education fetch failed: {exc}", exc_info=True)

        # --- Remote work / commute ---
        try:
            commute = self.get_commute_data(fips)
            if commute and commute.get("data"):
                row = commute["data"][0]
                headers = commute["headers"]
                idx = {h: i for i, h in enumerate(headers)}
                total_workers = int(row[idx.get("B08006_001E", 0)] or 0)
                wfh = int(row[idx.get("B08006_017E", 0)] or 0)
                if total_workers > 0:
                    profile["remote_work_pct"] = round(wfh / total_workers * 100, 1)
                    profile["total_workers_16_plus"] = total_workers
        except Exception as exc:
            logger.error(f"Census commute fetch failed: {exc}", exc_info=True)

        # --- Industry mix ---
        try:
            industry = self.get_industry_employment(fips)
            if industry and industry.get("data"):
                row = industry["data"][0]
                headers = industry["headers"]
                idx = {h: i for i, h in enumerate(headers)}
                total_emp = int(row[idx.get("C24030_001E", 0)] or 0)
                if total_emp > 0:
                    profile["industry_mix"] = {
                        "total_employed": total_emp,
                        "mgmt_business_science_arts_pct": round(
                            int(row[idx.get("C24030_002E", 0)] or 0) / total_emp * 100,
                            1,
                        ),
                        "service_pct": round(
                            int(row[idx.get("C24030_029E", 0)] or 0) / total_emp * 100,
                            1,
                        ),
                        "sales_office_pct": round(
                            int(row[idx.get("C24030_036E", 0)] or 0) / total_emp * 100,
                            1,
                        ),
                        "construction_maintenance_pct": round(
                            int(row[idx.get("C24030_043E", 0)] or 0) / total_emp * 100,
                            1,
                        ),
                        "production_transportation_pct": round(
                            int(row[idx.get("C24030_050E", 0)] or 0) / total_emp * 100,
                            1,
                        ),
                    }
        except Exception as exc:
            logger.error(f"Census industry fetch failed: {exc}", exc_info=True)

        # Build a human-readable summary line
        parts: list[str] = []
        if "labor_force_size" in profile:
            lf = profile["labor_force_size"]
            if lf >= 1_000_000:
                parts.append(f"{lf / 1_000_000:.1f}M labor force")
            else:
                parts.append(f"{lf:,} labor force")
        if "education" in profile:
            bp = profile["education"].get("bachelors_plus_pct", 0)
            parts.append(f"{bp}% with bachelor's+")
        if "remote_work_pct" in profile:
            parts.append(f"{profile['remote_work_pct']}% remote workers")
        if "median_household_income" in profile:
            inc = profile["median_household_income"]
            parts.append(f"${inc:,} median income")

        geo_label = city.strip().title() if city else state.upper()
        if parts:
            profile["summary"] = f"{geo_label} metro has {', '.join(parts)}"

        return profile


# ═══════════════════════════════════════════════════════════════════════════════
# 7. USAJOBS (Federal Job Listings)
# ═══════════════════════════════════════════════════════════════════════════════

# Disk cache for USAJobs (6hr TTL -- federal listings update daily)
_USAJOBS_DISK_CACHE_DIR = Path(tempfile.gettempdir()) / "nova_usajobs_cache"
_USAJOBS_DISK_CACHE_TTL = 21600  # 6 hours


def _usajobs_disk_cache_get(key: str) -> Any | None:
    """Read a cached USAJobs value from disk if it exists and is not expired.

    Args:
        key: Cache key string.

    Returns:
        Cached value or None if missing/expired.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    cache_file = _USAJOBS_DISK_CACHE_DIR / f"{safe_key}.json"
    try:
        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < _USAJOBS_DISK_CACHE_TTL:
                return data.get("value")
            cache_file.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError, KeyError) as exc:
        logger.debug(f"USAJobs disk cache read error for {key}: {exc}")
    return None


def _usajobs_disk_cache_set(key: str, value: Any) -> None:
    """Write a USAJobs value to disk cache.

    Args:
        key: Cache key string.
        value: Value to cache.
    """
    safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in key)
    try:
        _USAJOBS_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = _USAJOBS_DISK_CACHE_DIR / f"{safe_key}.json"
        cache_file.write_text(
            json.dumps({"ts": time.time(), "value": value}),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.debug(f"USAJobs disk cache write error for {key}: {exc}")


class USAJobsClient:
    """Client for the USAJobs.gov API.

    Provides federal job search, salary by grade, agency aggregation,
    security clearance filtering, and hiring path information.

    Env vars: USAJOBS_API_KEY, USAJOBS_EMAIL
    Docs: https://developer.usajobs.gov/API-Reference
    """

    BASE_URL = "https://data.usajobs.gov/api"

    def __init__(self) -> None:
        """Initialize USAJobs client with credentials from environment."""
        self.api_key = os.environ.get("USAJOBS_API_KEY") or ""
        self.email = os.environ.get("USAJOBS_EMAIL") or "shubhamsingh@joveo.com"

    def _is_configured(self) -> bool:
        """Check if API key is set (email has a fallback)."""
        return bool(self.api_key)

    @staticmethod
    def _parse_job_item(item: dict) -> dict:
        """Parse a single SearchResultItem into a normalized job dict.

        Args:
            item: Raw USAJobs SearchResultItem.

        Returns:
            Normalized job dict with title, org, salary, grade, clearance, etc.
        """
        desc = item.get("MatchedObjectDescriptor") or {}
        user_area = desc.get("UserArea") or {}
        details = user_area.get("Details") or {}
        remun_list = desc.get("PositionRemuneration") or [{}]
        remun = remun_list[0] if remun_list else {}

        # Extract security clearance from QualificationSummary or details
        clearance = details.get("SecurityClearance") or ""
        if not clearance:
            qual = details.get("QualificationSummary") or ""
            for lvl in ("top secret", "secret", "confidential", "public trust"):
                if lvl in qual.lower():
                    clearance = lvl.title()
                    break

        return {
            "position_title": desc.get("PositionTitle") or "",
            "organization": desc.get("OrganizationName") or "",
            "department": desc.get("DepartmentName") or "",
            "location": desc.get("PositionLocationDisplay") or "",
            "salary_min": remun.get("MinimumRange") or "",
            "salary_max": remun.get("MaximumRange") or "",
            "rate_interval": remun.get("RateIntervalCode") or "",
            "grade": details.get("LowGrade") or "",
            "high_grade": details.get("HighGrade") or "",
            "security_clearance": clearance,
            "hiring_path": details.get("HiringPath") or [],
            "url": desc.get("PositionURI") or "",
            "control_number": desc.get("PositionID") or "",
            "open_date": desc.get("PositionStartDate") or "",
            "close_date": desc.get("PositionEndDate") or "",
        }

    def _get_headers(self) -> dict[str, str]:
        """Build required USAJobs request headers.

        Returns:
            Dict with Authorization-Key and User-Agent headers.
        """
        return {
            "Authorization-Key": self.api_key,
            "User-Agent": self.email,
            "Host": "data.usajobs.gov",
        }

    def search_jobs(
        self,
        keyword: str,
        location: str = "",
        salary_min: int = 0,
        results_per_page: int = 25,
    ) -> dict | None:
        """Search federal job listings.

        Args:
            keyword: Job keyword or title.
            location: Location filter (city, state).
            salary_min: Minimum salary filter (USD).
            results_per_page: Number of results (max 500).

        Returns:
            Dict with count and parsed job list, or None on failure.
        """
        if not self._is_configured():
            logger.warning("USAJOBS_API_KEY not set")
            return None

        cache_key = (
            f"usajobs:search:{keyword}:{location}:{salary_min}:{results_per_page}"
        )

        # L1: in-memory
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        # L2: disk (6hr TTL)
        cached_disk = _usajobs_disk_cache_get(cache_key)
        if cached_disk is not None:
            _set_cached(cache_key, cached_disk)
            return cached_disk

        params: dict[str, str | int] = {
            "Keyword": keyword,
            "ResultsPerPage": results_per_page,
        }
        if location:
            params["LocationName"] = location
        if salary_min > 0:
            params["RemunerationMinimumAmount"] = salary_min

        qs = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}/Search?{qs}"
        try:
            data = _http_get(url, headers=self._get_headers())
        except Exception as exc:
            logger.error("USAJobs search_jobs failed: %s", exc, exc_info=True)
            return None

        if data is None:
            return None

        search_result = data.get("SearchResult") or {}
        search_result_count = search_result.get("SearchResultCount") or 0
        items = search_result.get("SearchResultItems") or []

        result = {
            "count": search_result_count,
            "jobs": [self._parse_job_item(item) for item in items],
        }
        _set_cached(cache_key, result)
        _usajobs_disk_cache_set(cache_key, result)
        return result

    def get_job_count(self, keyword: str, location: str = "") -> int:
        """Get total federal job count for a role/location.

        Args:
            keyword: Job keyword or title.
            location: Optional location filter.

        Returns:
            Total count of matching federal jobs (0 on failure).
        """
        result = self.search_jobs(keyword, location=location, results_per_page=1)
        if result is None:
            return 0
        return result.get("count") or 0

    def get_salary_by_grade(self, grade: str = "GS-13") -> dict | None:
        """Get federal jobs for a specific GS grade to determine salary ranges.

        Args:
            grade: GS grade string (e.g., 'GS-13', 'GS-15').

        Returns:
            Dict with grade, sample salary range, and job count, or None.
        """
        if not self._is_configured():
            logger.warning("USAJOBS_API_KEY not set")
            return None

        grade_num = grade.replace("GS-", "").replace("gs-", "").strip()

        cache_key = f"usajobs:grade:{grade_num}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        cached_disk = _usajobs_disk_cache_get(cache_key)
        if cached_disk is not None:
            _set_cached(cache_key, cached_disk)
            return cached_disk

        params: dict[str, str | int] = {
            "PayGradeLow": grade_num,
            "PayGradeHigh": grade_num,
            "ResultsPerPage": 10,
        }
        qs = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}/Search?{qs}"
        try:
            data = _http_get(url, headers=self._get_headers())
        except Exception as exc:
            logger.error("USAJobs get_salary_by_grade failed: %s", exc, exc_info=True)
            return None

        if data is None:
            return None

        search_result = data.get("SearchResult") or {}
        items = search_result.get("SearchResultItems") or []
        count = search_result.get("SearchResultCount") or 0

        min_salaries: list[float] = []
        max_salaries: list[float] = []
        for item in items:
            desc = item.get("MatchedObjectDescriptor") or {}
            remun_list = desc.get("PositionRemuneration") or [{}]
            remun = remun_list[0] if remun_list else {}
            try:
                s_min = float(remun.get("MinimumRange") or 0)
                s_max = float(remun.get("MaximumRange") or 0)
                if s_min > 0:
                    min_salaries.append(s_min)
                if s_max > 0:
                    max_salaries.append(s_max)
            except (ValueError, TypeError):
                pass

        result = {
            "grade": f"GS-{grade_num}",
            "total_jobs": count,
            "salary_range_low": min(min_salaries) if min_salaries else 0,
            "salary_range_high": max(max_salaries) if max_salaries else 0,
            "avg_salary_min": (
                sum(min_salaries) / len(min_salaries) if min_salaries else 0
            ),
            "avg_salary_max": (
                sum(max_salaries) / len(max_salaries) if max_salaries else 0
            ),
        }
        _set_cached(cache_key, result)
        _usajobs_disk_cache_set(cache_key, result)
        return result

    def get_agencies_hiring(self, keyword: str, location: str = "") -> list[dict]:
        """Get top agencies hiring for a role, aggregated from search results.

        Args:
            keyword: Job keyword or title.
            location: Optional location filter.

        Returns:
            List of agency dicts sorted by job count (descending).
        """
        cache_key = f"usajobs:agencies:{keyword}:{location}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        result = self.search_jobs(keyword, location=location, results_per_page=100)
        if not result or not result.get("jobs"):
            return []

        agency_counts: dict[str, dict[str, Any]] = {}
        for job in result["jobs"]:
            org = job.get("organization") or "Unknown"
            dept = job.get("department") or ""
            if org not in agency_counts:
                agency_counts[org] = {"agency": org, "department": dept, "count": 0}
            agency_counts[org]["count"] += 1

        agencies = sorted(
            agency_counts.values(), key=lambda x: x["count"], reverse=True
        )
        _set_cached(cache_key, agencies)
        return agencies

    def get_security_clearance_jobs(
        self,
        clearance_level: str = "secret",
        keyword: str = "",
        location: str = "",
    ) -> dict | None:
        """Search jobs requiring a specific security clearance level.

        Args:
            clearance_level: Clearance type ('secret', 'top secret', 'public trust').
            keyword: Optional job keyword filter.
            location: Optional location filter.

        Returns:
            Dict with count, jobs, and clearance breakdown, or None.
        """
        if not self._is_configured():
            logger.warning("USAJOBS_API_KEY not set")
            return None

        cache_key = f"usajobs:clearance:{clearance_level}:{keyword}:{location}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        cached_disk = _usajobs_disk_cache_get(cache_key)
        if cached_disk is not None:
            _set_cached(cache_key, cached_disk)
            return cached_disk

        params: dict[str, str | int] = {
            "SecurityClearanceRequired": "1",
            "ResultsPerPage": 50,
        }
        if keyword:
            params["Keyword"] = keyword
        if location:
            params["LocationName"] = location

        qs = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}/Search?{qs}"
        try:
            data = _http_get(url, headers=self._get_headers())
        except Exception as exc:
            logger.error("USAJobs clearance search failed: %s", exc, exc_info=True)
            return None

        if data is None:
            return None

        search_result = data.get("SearchResult") or {}
        items = search_result.get("SearchResultItems") or []
        total = search_result.get("SearchResultCount") or 0

        all_jobs = [self._parse_job_item(item) for item in items]
        clearance_lower = clearance_level.lower()

        clearance_breakdown: dict[str, int] = {}
        filtered_jobs: list[dict] = []
        for job in all_jobs:
            cl = job.get("security_clearance") or "Unspecified"
            clearance_breakdown[cl] = clearance_breakdown.get(cl, 0) + 1
            if clearance_lower in cl.lower():
                filtered_jobs.append(job)

        result_data = {
            "total_clearance_jobs": total,
            "clearance_level_filter": clearance_level,
            "matched_count": len(filtered_jobs),
            "clearance_breakdown": clearance_breakdown,
            "jobs": filtered_jobs[:25],
        }
        _set_cached(cache_key, result_data)
        _usajobs_disk_cache_set(cache_key, result_data)
        return result_data

    def get_job_details(self, control_number: str) -> dict | None:
        """Get detailed information about a specific federal job.

        Args:
            control_number: USAJobs position control number.

        Returns:
            Dict with full job details or None.
        """
        if not self._is_configured():
            logger.warning("USAJOBS_API_KEY not set")
            return None

        cache_key = f"usajobs:detail:{control_number}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/Search?ControlNumber={control_number}"
        try:
            data = _http_get(url, headers=self._get_headers())
        except Exception as exc:
            logger.error("USAJobs get_job_details failed: %s", exc, exc_info=True)
            return None

        if data is None:
            return None

        search_result = data.get("SearchResult") or {}
        items = search_result.get("SearchResultItems") or []
        if not items:
            return None

        descriptor = items[0].get("MatchedObjectDescriptor") or {}
        user_area = descriptor.get("UserArea") or {}
        details = user_area.get("Details") or {}
        result = {
            "position_title": descriptor.get("PositionTitle") or "",
            "organization": descriptor.get("OrganizationName") or "",
            "department": descriptor.get("DepartmentName") or "",
            "job_summary": details.get("JobSummary") or "",
            "who_may_apply": (details.get("WhoMayApply") or {}).get("Name") or "",
            "position_location": descriptor.get("PositionLocationDisplay") or "",
            "salary_min": (descriptor.get("PositionRemuneration") or [{}])[0].get(
                "MinimumRange"
            )
            or "",
            "salary_max": (descriptor.get("PositionRemuneration") or [{}])[0].get(
                "MaximumRange"
            )
            or "",
            "open_date": descriptor.get("PositionStartDate") or "",
            "close_date": descriptor.get("PositionEndDate") or "",
            "url": descriptor.get("PositionURI") or "",
        }
        _set_cached(cache_key, result)
        _usajobs_disk_cache_set(cache_key, result)
        return result

    def get_hiring_paths(self) -> list[dict] | None:
        """Get available federal hiring paths.

        Returns:
            List of hiring path dicts or None.
        """
        if not self._is_configured():
            logger.warning("USAJOBS_API_KEY not set")
            return None

        cache_key = "usajobs:hiring_paths"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/codelist/hiringpaths"
        try:
            data = _http_get(url, headers=self._get_headers())
        except Exception as exc:
            logger.error("USAJobs get_hiring_paths failed: %s", exc, exc_info=True)
            return None

        if data is None:
            return None

        code_list = data.get("CodeList") or []
        if not code_list:
            return None

        valid_values = (code_list[0].get("ValidValue") or []) if code_list else []
        result = [
            {
                "code": v.get("Code") or "",
                "value": v.get("Value") or "",
                "is_disabled": v.get("IsDisabled") or "",
            }
            for v in valid_values
        ]
        _set_cached(cache_key, result)
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# 8. BLS (Bureau of Labor Statistics)
# ═══════════════════════════════════════════════════════════════════════════════


class BLSClient:
    """Client for the Bureau of Labor Statistics (BLS) API v2.

    Provides occupational employment statistics (OES), employment projections,
    QCEW data, and CPI series.

    Env var: BLS_API_KEY
    Docs: https://www.bls.gov/developers/
    """

    BASE_URL = "https://api.bls.gov/publicAPI/v2"

    def __init__(self) -> None:
        """Initialize BLS client with API key from environment."""
        self.api_key = os.environ.get("BLS_API_KEY") or ""

    def _is_configured(self) -> bool:
        """Check if API key is set."""
        return bool(self.api_key)

    def _fetch_series(
        self,
        series_ids: list[str],
        start_year: str | None = None,
        end_year: str | None = None,
    ) -> dict | None:
        """Fetch one or more BLS time series.

        Args:
            series_ids: List of BLS series IDs.
            start_year: Start year (e.g., '2020'). Defaults to 2 years ago.
            end_year: End year. Defaults to current year.

        Returns:
            Dict with series data or None.
        """
        if not self._is_configured():
            logger.warning("BLS_API_KEY not set")
            return None

        if not start_year:
            start_year = str(int(time.strftime("%Y")) - 2)
        if not end_year:
            end_year = time.strftime("%Y")

        cache_key = f"bls:{','.join(series_ids)}:{start_year}:{end_year}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/timeseries/data/"
        payload = {
            "seriesid": series_ids,
            "startyear": start_year,
            "endyear": end_year,
            "registrationkey": self.api_key,
        }
        data = _http_post(url, payload)
        if data is None:
            return None

        status = data.get("status") or ""
        if status != "REQUEST_SUCCEEDED":
            msg = data.get("message") or []
            logger.warning(f"BLS request failed: {status} -- {msg}")
            return None

        results = data.get("Results") or {}
        series_list = results.get("series") or []

        result = {
            "status": status,
            "series": [
                {
                    "seriesID": s.get("seriesID") or "",
                    "data": [
                        {
                            "year": d.get("year") or "",
                            "period": d.get("period") or "",
                            "periodName": d.get("periodName") or "",
                            "value": d.get("value") or "",
                        }
                        for d in (s.get("data") or [])
                    ],
                }
                for s in series_list
            ],
        }
        _set_cached(cache_key, result)
        return result

    def get_occupational_employment(self, soc_code: str) -> dict | None:
        """Get Occupational Employment and Wage Statistics (OES) data.

        Args:
            soc_code: SOC code without dots (e.g., '151252' for Software Devs).

        Returns:
            Dict with employment and wage data or None.
        """
        # OES series: OEUM{area}{industry}{soc_code}{datatype}
        # National: area=003600000, industry=000000
        soc_clean = soc_code.replace("-", "").replace(".", "")
        # Employment: datatype 01, Mean wage: 04, Median wage: 13
        series_ids = [
            f"OEUM003600000000000{soc_clean}01",  # Employment
            f"OEUM003600000000000{soc_clean}04",  # Mean hourly wage
            f"OEUM003600000000000{soc_clean}13",  # Median hourly wage
        ]
        return self._fetch_series(series_ids)

    def get_employment_projections(self, soc_code: str) -> dict | None:
        """Get employment projections data.

        Args:
            soc_code: SOC code (e.g., '15-1252' or '151252').

        Returns:
            Dict with projections data or None.
        """
        soc_clean = soc_code.replace("-", "").replace(".", "")
        # EP series: Employment projections
        # Base year employment, projected employment, change
        series_ids = [
            f"EUBM00{soc_clean}0001",  # Base year employment
        ]
        return self._fetch_series(series_ids)

    def get_qcew_data(
        self,
        area_code: str,
        industry_code: str = "10",
    ) -> dict | None:
        """Get Quarterly Census of Employment and Wages (QCEW) data.

        Args:
            area_code: FIPS area code (e.g., 'US000' for national).
            industry_code: Industry code (default '10' for all industries).

        Returns:
            Dict with QCEW data or None.
        """
        # QCEW series: ENU{area}{ownership}{industry}{size}{datatype}
        # Private ownership (5), all sizes (0)
        series_ids = [
            f"ENU{area_code}5{industry_code}05",  # Average weekly wage
            f"ENU{area_code}5{industry_code}01",  # Employment
        ]
        return self._fetch_series(series_ids)

    def get_cpi_series(
        self,
        area: str = "0000",
        item: str = "SA0",
    ) -> dict | None:
        """Get Consumer Price Index series from BLS.

        Args:
            area: CPI area code (e.g., '0000' for national).
            item: CPI item code (e.g., 'SA0' for all items).

        Returns:
            Dict with CPI data or None.
        """
        # CPI-U series: CUUR{area}{item}
        series_ids = [f"CUUR{area}{item}"]
        return self._fetch_series(series_ids)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: STATE FIPS MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

_STATE_FIPS: dict[str, str] = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "PR": "72",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}


def _state_to_fips(state_code: str) -> str | None:
    """Convert a two-letter state abbreviation to FIPS code.

    Args:
        state_code: Two-letter state abbreviation (e.g., 'CA').

    Returns:
        Two-digit FIPS code string or None if not found.
    """
    return _STATE_FIPS.get(state_code.upper())


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL CLIENT INSTANCES
# ═══════════════════════════════════════════════════════════════════════════════

fred = FREDClient()
adzuna = AdzunaClient()
jooble = JoobleClient()
onet = ONetClient()
bea = BEAClient()
census = CensusClient()
usajobs = USAJobsClient()
bls = BLSClient()
remoteok = RemoteOKClient()


# ═══════════════════════════════════════════════════════════════════════════════
# DIAGNOSTIC: TEST ALL APIs
# ═══════════════════════════════════════════════════════════════════════════════


def test_all_apis() -> dict[str, bool]:
    """Make minimal test calls to each API and return pass/fail status.

    Each API is tested with the lightest possible request. A True result
    means the API responded successfully; False means it failed (missing
    credentials, network error, or bad response).

    Returns:
        Dict mapping API name to boolean pass/fail.
    """
    results: dict[str, bool] = {}

    # 1. FRED
    try:
        r = fred.get_gdp_growth()
        results["fred"] = r is not None and bool(r.get("observations"))
    except Exception as exc:
        logger.warning("FRED test failed: %s", exc)
        results["fred"] = False

    # 2. Adzuna
    try:
        r = adzuna.get_job_count("engineer", "us")
        results["adzuna"] = r is not None and (r.get("count") or 0) > 0
    except Exception as exc:
        logger.warning("Adzuna test failed: %s", exc)
        results["adzuna"] = False

    # 3. Jooble
    try:
        r = jooble.search_jobs("developer", "New York")
        results["jooble"] = r is not None and (r.get("totalCount") or 0) > 0
    except Exception as exc:
        logger.warning("Jooble test failed: %s", exc)
        results["jooble"] = False

    # 4. O*NET
    try:
        r = onet.search_occupations("software")
        results["onet"] = r is not None and len(r) > 0
    except Exception as exc:
        logger.warning("O*NET test failed: %s", exc)
        results["onet"] = False

    # 5. BEA
    try:
        r = bea.get_gdp_by_state(year="2022")
        results["bea"] = r is not None
    except Exception as exc:
        logger.warning("BEA test failed: %s", exc)
        results["bea"] = False

    # 6. Census
    try:
        r = census.get_population_by_state()
        results["census"] = r is not None and (r.get("count") or 0) > 0
    except Exception as exc:
        logger.warning("Census test failed: %s", exc)
        results["census"] = False

    # 7. USAJobs
    try:
        r = usajobs.search_jobs("engineer", results_per_page=1)
        results["usajobs"] = r is not None and (r.get("count") or 0) > 0
    except Exception as exc:
        logger.warning("USAJobs test failed: %s", exc)
        results["usajobs"] = False

    # 8. BLS
    try:
        r = bls.get_cpi_series()
        results["bls"] = r is not None and bool(r.get("series"))
    except Exception as exc:
        logger.warning("BLS test failed: %s", exc)
        results["bls"] = False

    # 9. RemoteOK (no auth required)
    try:
        r = remoteok.get_trending_skills(limit=5)
        results["remoteok"] = isinstance(r, list) and len(r) > 0
    except Exception as exc:
        logger.warning("RemoteOK test failed: %s", exc)
        results["remoteok"] = False

    # 10. FRED JOLTS (uses existing FRED key)
    try:
        r = fred.get_jolts_data("total")
        results["fred_jolts"] = r is not None and bool(r.get("metrics"))
    except Exception as exc:
        logger.warning("FRED JOLTS test failed: %s", exc)
        results["fred_jolts"] = False

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info(f"API integration test: {passed}/{total} passed")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE: GET ALL CONFIGURED APIs
# ═══════════════════════════════════════════════════════════════════════════════


def get_api_status() -> dict[str, dict[str, Any]]:
    """Get configuration status of all API clients.

    Returns:
        Dict mapping API name to status info (configured, env_vars).
    """
    return {
        "fred": {
            "configured": fred._is_configured(),
            "env_vars": ["FRED_API_KEY"],
        },
        "adzuna": {
            "configured": adzuna._is_configured(),
            "env_vars": ["ADZUNA_APP_ID", "ADZUNA_APP_KEY"],
        },
        "jooble": {
            "configured": jooble._is_configured(),
            "env_vars": ["JOOBLE_API_KEY"],
            "requests_remaining": jooble.requests_remaining,
        },
        "onet": {
            "configured": onet._is_configured(),
            "env_vars": ["ONET_USERNAME", "ONET_API_KEY", "ONET_PASSWORD"],
        },
        "bea": {
            "configured": bea._is_configured(),
            "env_vars": ["BEA_API_KEY"],
        },
        "census": {
            "configured": census._is_configured(),
            "env_vars": ["CENSUS_API_KEY"],
        },
        "usajobs": {
            "configured": usajobs._is_configured(),
            "env_vars": ["USAJOBS_API_KEY", "USAJOBS_EMAIL"],
        },
        "bls": {
            "configured": bls._is_configured(),
            "env_vars": ["BLS_API_KEY"],
        },
        "remoteok": {
            "configured": True,
            "env_vars": [],
            "note": "No API key required",
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# RESILIENCE: CROSS-REFERENCING FALLBACKS
# ═══════════════════════════════════════════════════════════════════════════════

import threading as _fb_threading

_fallback_lock = _fb_threading.Lock()


def _call_with_fallback(
    primary_fn: Any,
    fallback_fn: Any,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Try primary function, fall back on failure.

    Thread-safe utility that attempts the primary callable first.
    If it returns None or raises, tries the fallback callable with
    the same arguments.

    Args:
        primary_fn: Primary callable to try first.
        fallback_fn: Fallback callable if primary fails.
        *args: Positional args passed to both callables.
        **kwargs: Keyword args passed to both callables.

    Returns:
        Result from whichever callable succeeds, or None if both fail.
    """
    try:
        result = primary_fn(*args, **kwargs)
        if result is not None:
            return result
    except Exception as e:
        logger.warning(
            "Primary API call failed (%s), trying fallback: %s",
            primary_fn.__name__ if hasattr(primary_fn, "__name__") else "unknown",
            e,
        )

    try:
        result = fallback_fn(*args, **kwargs)
        if result is not None:
            logger.info(
                "Fallback API call succeeded (%s)",
                fallback_fn.__name__ if hasattr(fallback_fn, "__name__") else "unknown",
            )
            return result
    except Exception as e:
        logger.error(
            "Fallback API call also failed (%s): %s",
            fallback_fn.__name__ if hasattr(fallback_fn, "__name__") else "unknown",
            e,
            exc_info=True,
        )

    return None


# ── Job Search Fallbacks: Adzuna <-> Jooble ────────────────────────────────


def search_jobs_resilient(
    role: str,
    location: str = "us",
    page: int = 1,
) -> dict | None:
    """Search jobs with automatic failover between Adzuna and Jooble.

    If Adzuna fails, falls back to Jooble. If Jooble fails, falls back
    to Adzuna. Normalizes results to a common format.

    Args:
        role: Job title or keyword.
        location: Country code or location string.
        page: Page number.

    Returns:
        Normalized job search results dict, or None if both fail.
    """

    def _try_adzuna() -> dict | None:
        result = adzuna.search_jobs(role, location, page=page)
        if result is None:
            return None
        return {
            "source": "adzuna",
            "count": result.get("count") or 0,
            "results": result.get("results") or [],
        }

    def _try_jooble() -> dict | None:
        result = jooble.search_jobs(role, location)
        if result is None:
            return None
        # Normalize Jooble format to match Adzuna's
        normalized_results = [
            {
                "title": j.get("title") or "",
                "company": j.get("company") or "",
                "location": j.get("location") or "",
                "salary_min": 0,
                "salary_max": 0,
                "created": j.get("updated") or "",
                "redirect_url": j.get("link") or "",
            }
            for j in (result.get("jobs") or [])
        ]
        return {
            "source": "jooble",
            "count": result.get("totalCount") or 0,
            "results": normalized_results,
        }

    return _call_with_fallback(_try_adzuna, _try_jooble)


# ── Federal Job Fallback: USAJobs -> Adzuna (government filter) ────────────


def search_federal_jobs_resilient(
    keyword: str,
    location: str = "",
) -> dict | None:
    """Search federal jobs with fallback to Adzuna government jobs.

    Args:
        keyword: Job keyword or title.
        location: Location filter.

    Returns:
        Job search results dict, or None if both fail.
    """

    def _try_usajobs() -> dict | None:
        return usajobs.search_jobs(keyword, location)

    def _try_adzuna_gov() -> dict | None:
        # Adzuna with government keyword filter as fallback
        result = adzuna.search_jobs(
            f"{keyword} government federal",
            "us",
            page=1,
            results_per_page=10,
        )
        if result is None:
            return None
        return {
            "source": "adzuna_gov_fallback",
            "count": result.get("count") or 0,
            "jobs": result.get("results") or [],
        }

    return _call_with_fallback(_try_usajobs, _try_adzuna_gov)


# ── Economic Data Cross-Reference: FRED <-> BLS <-> BEA ───────────────────


def get_unemployment_resilient(state_code: str | None = None) -> dict | None:
    """Get unemployment data with cross-API fallback.

    Tier 1: FRED (primary source for unemployment)
    Tier 2: BLS (alternative time series)

    Args:
        state_code: Two-letter state abbreviation or None for national.

    Returns:
        Unemployment data dict, or None if both fail.
    """

    def _try_fred() -> dict | None:
        return fred.get_unemployment_rate(state_code)

    def _try_bls() -> dict | None:
        # BLS CPS series for unemployment: LNS14000000 (national)
        result = bls._fetch_series(["LNS14000000"])
        if result is None:
            return None
        return {
            "source": "bls_fallback",
            "series_id": "LNS14000000",
            "data": result,
        }

    return _call_with_fallback(_try_fred, _try_bls)


def get_cpi_resilient() -> dict | None:
    """Get CPI data with cross-API fallback.

    Tier 1: FRED CPI series
    Tier 2: BLS CPI series

    Returns:
        CPI data dict, or None if both fail.
    """

    def _try_fred() -> dict | None:
        return fred.get_cpi_data()

    def _try_bls() -> dict | None:
        return bls.get_cpi_series()

    return _call_with_fallback(_try_fred, _try_bls)


def get_gdp_resilient(state_level: bool = False) -> dict | None:
    """Get GDP data with cross-API fallback.

    Tier 1: FRED (national) or BEA (state-level)
    Tier 2: BEA (national fallback) or FRED (state approximation)

    Args:
        state_level: If True, return state-level data.

    Returns:
        GDP data dict, or None if both fail.
    """
    if state_level:

        def _try_bea() -> dict | None:
            return bea.get_gdp_by_state()

        def _try_fred_fallback() -> dict | None:
            return fred.get_gdp_growth()

        return _call_with_fallback(_try_bea, _try_fred_fallback)
    else:

        def _try_fred_gdp() -> dict | None:
            return fred.get_gdp_growth()

        def _try_bea_fallback() -> dict | None:
            return bea.get_gdp_by_state(year="2024")

        return _call_with_fallback(_try_fred_gdp, _try_bea_fallback)


# ── O*NET Disk Cache v2.0 (30-day TTL, occupational data changes infrequently)

_onet_cache_file = Path(__file__).resolve().parent / "data" / "onet_cache.json"
_onet_local_cache: dict[str, Any] = {}
_onet_cache_loaded = False
_ONET_DISK_CACHE_TTL = 30 * 86400  # 30 days in seconds


def _load_onet_local_cache() -> None:
    """Load O*NET local cache from disk (one-time at first access)."""
    global _onet_cache_loaded, _onet_local_cache
    if _onet_cache_loaded:
        return

    with _fallback_lock:
        if _onet_cache_loaded:
            return
        if _onet_cache_file.exists():
            try:
                with open(_onet_cache_file, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                # Migrate old flat cache to timestamped format if needed
                if raw and isinstance(raw, dict):
                    first_val = next(iter(raw.values()), None)
                    if isinstance(first_val, dict) and "ts" in first_val:
                        _onet_local_cache = raw
                    else:
                        # Old format: wrap each entry with current timestamp
                        _onet_local_cache = {
                            k: {"ts": time.time(), "data": v} for k, v in raw.items()
                        }
                else:
                    _onet_local_cache = {}
                logger.info(
                    "Loaded O*NET local cache: %d entries", len(_onet_local_cache)
                )
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load O*NET local cache: %s", e)
        _onet_cache_loaded = True


def _save_onet_local_cache() -> None:
    """Persist O*NET local cache to disk."""
    with _fallback_lock:
        try:
            _onet_cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(_onet_cache_file, "w", encoding="utf-8") as f:
                json.dump(_onet_local_cache, f, ensure_ascii=False)
        except OSError as e:
            logger.warning("Failed to save O*NET local cache: %s", e)


def _onet_disk_get(cache_key: str) -> Any | None:
    """Read from disk cache with 30-day TTL check.

    Args:
        cache_key: Cache key string.

    Returns:
        Cached data or None if expired/missing.
    """
    entry = _onet_local_cache.get(cache_key)
    if entry is None:
        return None
    if not isinstance(entry, dict) or "ts" not in entry:
        return entry  # Legacy format -- return as-is
    if time.time() - entry["ts"] > _ONET_DISK_CACHE_TTL:
        logger.debug("O*NET disk cache expired for %s", cache_key)
        return None
    return entry.get("data")


def _onet_disk_set(cache_key: str, data: Any) -> None:
    """Write to disk cache with timestamp.

    Args:
        cache_key: Cache key string.
        data: Data to cache.
    """
    _onet_local_cache[cache_key] = {"ts": time.time(), "data": data}
    _save_onet_local_cache()


def get_onet_skills_resilient(soc_code: str) -> list[dict] | None:
    """Get O*NET skills with local cache fallback.

    Tier 1: O*NET API (live data)
    Tier 2: Local disk cache (data/onet_cache.json, 30-day TTL)

    Args:
        soc_code: O*NET-SOC code (e.g., '15-1252.00').

    Returns:
        List of skill dicts, or None if both fail.
    """
    _load_onet_local_cache()
    cache_key = f"skills:{soc_code}"

    # Try live API first
    result = onet.get_skills(soc_code)
    if result is not None:
        _onet_disk_set(cache_key, result)
        return result

    # Fallback to disk cache
    cached = _onet_disk_get(cache_key)
    if cached is not None:
        logger.info("O*NET skills served from disk cache for %s", soc_code)
        return cached

    return None


def get_onet_tech_skills_resilient(soc_code: str) -> list[dict] | None:
    """Get O*NET technology skills with local cache fallback.

    Args:
        soc_code: O*NET-SOC code.

    Returns:
        List of tech skill dicts, or None if both fail.
    """
    _load_onet_local_cache()
    cache_key = f"tech_skills:{soc_code}"

    result = onet.get_technology_skills(soc_code)
    if result is not None:
        _onet_disk_set(cache_key, result)
        return result

    cached = _onet_disk_get(cache_key)
    if cached is not None:
        logger.info("O*NET tech skills served from disk cache for %s", soc_code)
        return cached

    return None


def get_onet_knowledge_resilient(soc_code: str) -> list[dict] | None:
    """Get O*NET knowledge requirements with local cache fallback (v2.0).

    Args:
        soc_code: O*NET-SOC code.

    Returns:
        List of knowledge area dicts, or None if both fail.
    """
    _load_onet_local_cache()
    cache_key = f"knowledge:{soc_code}"

    result = onet.get_knowledge(soc_code)
    if result is not None:
        _onet_disk_set(cache_key, result)
        return result

    cached = _onet_disk_get(cache_key)
    if cached is not None:
        logger.info("O*NET knowledge served from disk cache for %s", soc_code)
        return cached

    return None


def get_onet_related_resilient(soc_code: str) -> list[dict] | None:
    """Get O*NET related occupations with local cache fallback (v2.0).

    Args:
        soc_code: O*NET-SOC code.

    Returns:
        List of related occupation dicts, or None if both fail.
    """
    _load_onet_local_cache()
    cache_key = f"related:{soc_code}"

    result = onet.get_related_occupations(soc_code)
    if result is not None:
        _onet_disk_set(cache_key, result)
        return result

    cached = _onet_disk_get(cache_key)
    if cached is not None:
        logger.info("O*NET related occupations served from disk cache for %s", soc_code)
        return cached

    return None


def get_onet_skills_profile_resilient(soc_code: str) -> dict | None:
    """Get complete O*NET skills profile with local cache fallback (v2.0).

    Combines skills, tech skills, knowledge, and related occupations
    with per-component disk caching.

    Args:
        soc_code: O*NET-SOC code.

    Returns:
        Combined profile dict, or None if all components fail.
    """
    skills = get_onet_skills_resilient(soc_code)
    tech_skills = get_onet_tech_skills_resilient(soc_code)
    knowledge = get_onet_knowledge_resilient(soc_code)
    related = get_onet_related_resilient(soc_code)

    if all(x is None for x in [skills, tech_skills, knowledge, related]):
        return None

    profile: dict[str, Any] = {"soc_code": soc_code}
    if skills is not None:
        profile["skills"] = skills
    if tech_skills is not None:
        profile["technology_skills"] = tech_skills
    if knowledge is not None:
        profile["knowledge"] = knowledge
    if related is not None:
        profile["related_occupations"] = related
    return profile


# ── Demographics Cross-Reference: Census <-> BEA ──────────────────────────


def get_income_data_resilient(state_fips: str | None = None) -> dict | None:
    """Get income data with Census -> BEA fallback.

    Tier 1: Census median household income
    Tier 2: BEA personal income by state

    Args:
        state_fips: Two-digit state FIPS code or None for all states.

    Returns:
        Income data dict, or None if both fail.
    """

    def _try_census() -> dict | None:
        return census.get_median_income_by_state()

    def _try_bea() -> dict | None:
        return bea.get_personal_income_by_state()

    return _call_with_fallback(_try_census, _try_bea)
