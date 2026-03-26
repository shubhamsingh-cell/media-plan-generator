"""Edge-First LLM Routing -- geographic routing for lowest latency.

Routes LLM requests to the nearest provider cluster based on user
geographic region detected from request headers.  Thread-safe with
per-region-provider latency tracking.

Region detection priority:
  1. CF-IPCountry header (Cloudflare)
  2. X-Forwarded-For -> IP geolocation heuristic
  3. Accept-Language header
  4. Fallback to US-West (Render.com default)

Fallback chain: nearest region -> adjacent regions -> global providers.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Region definitions
# ---------------------------------------------------------------------------


class Region(str, Enum):
    """Geographic regions for LLM provider clusters."""

    US_WEST = "us-west"
    US_EAST = "us-east"
    EU = "eu"
    ASIA = "asia"
    GLOBAL = "global"


# ---------------------------------------------------------------------------
# Provider -> Region mapping
# ---------------------------------------------------------------------------

PROVIDER_REGION_MAP: dict[str, Region] = {
    # US-West
    "together": Region.US_WEST,
    "siliconflow": Region.US_WEST,
    "cerebras": Region.US_WEST,
    "nvidia_nim": Region.US_WEST,
    "cloudflare": Region.US_WEST,
    # US-East
    "openai": Region.US_EAST,
    "anthropic": Region.US_EAST,
    "groq": Region.US_EAST,
    # EU
    "mistral": Region.EU,
    "huggingface": Region.EU,
    # Asia
    "zhipu": Region.ASIA,
    "xiaomi_mimo": Region.ASIA,
    # Global (anycast / multi-region)
    "openrouter": Region.GLOBAL,
    "openrouter_1": Region.GLOBAL,
    "openrouter_2": Region.GLOBAL,
    "openrouter_3": Region.GLOBAL,
    "openrouter_4": Region.GLOBAL,
    "openrouter_5": Region.GLOBAL,
    "openrouter_6": Region.GLOBAL,
    "openrouter_7": Region.GLOBAL,
    "sambanova": Region.GLOBAL,
}

# Reverse map: region -> list of providers
REGION_PROVIDERS: dict[Region, list[str]] = defaultdict(list)
for _prov, _reg in PROVIDER_REGION_MAP.items():
    REGION_PROVIDERS[_reg].append(_prov)

# ---------------------------------------------------------------------------
# Region adjacency -- ordered by geographic proximity
# ---------------------------------------------------------------------------

REGION_FALLBACK_CHAIN: dict[Region, list[Region]] = {
    Region.US_WEST: [
        Region.US_WEST,
        Region.US_EAST,
        Region.GLOBAL,
        Region.ASIA,
        Region.EU,
    ],
    Region.US_EAST: [
        Region.US_EAST,
        Region.US_WEST,
        Region.GLOBAL,
        Region.EU,
        Region.ASIA,
    ],
    Region.EU: [Region.EU, Region.US_EAST, Region.GLOBAL, Region.US_WEST, Region.ASIA],
    Region.ASIA: [
        Region.ASIA,
        Region.GLOBAL,
        Region.US_WEST,
        Region.EU,
        Region.US_EAST,
    ],
    Region.GLOBAL: [
        Region.GLOBAL,
        Region.US_WEST,
        Region.US_EAST,
        Region.EU,
        Region.ASIA,
    ],
}

# ---------------------------------------------------------------------------
# Country -> Region mapping (ISO 3166-1 alpha-2)
# ---------------------------------------------------------------------------

_COUNTRY_REGION: dict[str, Region] = {}

_US_WEST_STATES_COUNTRIES = [
    "US",  # default US -> west (Render is US-West)
]
_US_EAST_COUNTRIES = [
    "CA",
    "BR",
    "AR",
    "CO",
    "MX",
    "CL",
    "PE",
    "VE",
]
_EU_COUNTRIES = [
    "GB",
    "DE",
    "FR",
    "IT",
    "ES",
    "NL",
    "BE",
    "SE",
    "NO",
    "DK",
    "FI",
    "PL",
    "CZ",
    "AT",
    "CH",
    "IE",
    "PT",
    "RO",
    "HU",
    "BG",
    "HR",
    "SK",
    "SI",
    "LT",
    "LV",
    "EE",
    "LU",
    "MT",
    "CY",
    "GR",
    "IS",
    "UA",
    "RU",
    "TR",
    "IL",
    "ZA",
    "NG",
    "KE",
    "EG",
    "MA",
    "SA",
    "AE",
    "QA",
    "KW",
    "BH",
    "OM",
]
_ASIA_COUNTRIES = [
    "CN",
    "JP",
    "KR",
    "IN",
    "SG",
    "HK",
    "TW",
    "TH",
    "VN",
    "MY",
    "ID",
    "PH",
    "AU",
    "NZ",
    "PK",
    "BD",
    "LK",
    "MM",
    "KH",
    "LA",
    "MN",
    "NP",
]

for _cc in _US_WEST_STATES_COUNTRIES:
    _COUNTRY_REGION[_cc] = Region.US_WEST
for _cc in _US_EAST_COUNTRIES:
    _COUNTRY_REGION[_cc] = Region.US_EAST
for _cc in _EU_COUNTRIES:
    _COUNTRY_REGION[_cc] = Region.EU
for _cc in _ASIA_COUNTRIES:
    _COUNTRY_REGION[_cc] = Region.ASIA

# Language prefix -> Region (fallback for Accept-Language detection)
_LANG_REGION: dict[str, Region] = {
    "en": Region.US_WEST,
    "es": Region.US_EAST,
    "pt": Region.US_EAST,
    "fr": Region.EU,
    "de": Region.EU,
    "it": Region.EU,
    "nl": Region.EU,
    "sv": Region.EU,
    "da": Region.EU,
    "pl": Region.EU,
    "ru": Region.EU,
    "tr": Region.EU,
    "ar": Region.EU,
    "he": Region.EU,
    "zh": Region.ASIA,
    "ja": Region.ASIA,
    "ko": Region.ASIA,
    "hi": Region.ASIA,
    "th": Region.ASIA,
    "vi": Region.ASIA,
    "id": Region.ASIA,
    "ms": Region.ASIA,
}


# ---------------------------------------------------------------------------
# IP-based region heuristic (lightweight, no external dependency)
# ---------------------------------------------------------------------------


def _region_from_ip(ip_str: str) -> Optional[Region]:
    """Best-effort region guess from first octet of IPv4.

    This is a rough heuristic -- production deployments should use
    Cloudflare's CF-IPCountry or a proper GeoIP database.
    """
    try:
        first_octet = int(ip_str.strip().split(".")[0])
    except (ValueError, IndexError, AttributeError):
        return None

    # Very rough geographic allocation of IPv4 first-octet blocks
    if first_octet in range(3, 76):
        return Region.US_WEST
    if first_octet in range(76, 130):
        return Region.US_EAST
    if first_octet in range(130, 195):
        return Region.EU
    if first_octet >= 195:
        return Region.ASIA
    return None


# ---------------------------------------------------------------------------
# Latency tracker (thread-safe)
# ---------------------------------------------------------------------------


@dataclass
class _LatencySample:
    total_ms: float = 0.0
    count: int = 0
    min_ms: float = float("inf")
    max_ms: float = 0.0
    last_updated: float = 0.0


class LatencyTracker:
    """Thread-safe per-region-provider latency tracker with rolling stats."""

    def __init__(self, max_samples: int = 500) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, _LatencySample] = {}
        self._max_samples = max_samples

    def _key(self, region: Region, provider: str) -> str:
        return f"{region.value}:{provider}"

    def record(self, region: Region, provider: str, latency_ms: float) -> None:
        """Record a latency observation for a region-provider pair."""
        key = self._key(region, provider)
        with self._lock:
            sample = self._data.get(key)
            if sample is None:
                sample = _LatencySample()
                self._data[key] = sample

            # Rolling average with cap
            if sample.count >= self._max_samples:
                sample.total_ms = sample.total_ms * 0.5
                sample.count = sample.count // 2

            sample.total_ms += latency_ms
            sample.count += 1
            sample.min_ms = min(sample.min_ms, latency_ms)
            sample.max_ms = max(sample.max_ms, latency_ms)
            sample.last_updated = time.time()

    def get_avg(self, region: Region, provider: str) -> Optional[float]:
        """Get average latency in ms for a region-provider pair."""
        key = self._key(region, provider)
        with self._lock:
            sample = self._data.get(key)
            if sample is None or sample.count == 0:
                return None
            return round(sample.total_ms / sample.count, 2)

    def get_stats(self) -> dict[str, dict[str, float | int]]:
        """Get all latency stats for health endpoint."""
        with self._lock:
            result: dict[str, dict[str, float | int]] = {}
            for key, sample in self._data.items():
                if sample.count == 0:
                    continue
                result[key] = {
                    "avg_ms": round(sample.total_ms / sample.count, 2),
                    "min_ms": round(sample.min_ms, 2),
                    "max_ms": round(sample.max_ms, 2),
                    "samples": sample.count,
                    "last_updated": round(sample.last_updated, 2),
                }
            return result


# Module-level singleton
_latency_tracker = LatencyTracker()


# ---------------------------------------------------------------------------
# Region detection
# ---------------------------------------------------------------------------


def detect_user_region(headers: dict[str, str]) -> Region:
    """Detect user geographic region from HTTP request headers.

    Detection priority:
      1. CF-IPCountry (Cloudflare)
      2. X-Forwarded-For IP heuristic
      3. Accept-Language
      4. Default to US-West

    Args:
        headers: HTTP request headers (case-insensitive keys recommended).

    Returns:
        Detected Region enum value.
    """
    # Normalize header keys to lowercase for case-insensitive lookup
    h = {k.lower(): v for k, v in headers.items()}

    # 1. Cloudflare CF-IPCountry
    cf_country = (h.get("cf-ipcountry") or "").strip().upper()
    if cf_country and cf_country != "XX":
        region = _COUNTRY_REGION.get(cf_country)
        if region is not None:
            logger.debug(f"Edge routing: CF-IPCountry={cf_country} -> {region.value}")
            return region

    # 2. X-Forwarded-For IP heuristic
    xff = h.get("x-forwarded-for") or ""
    if xff:
        # Take the first (client) IP
        client_ip = xff.split(",")[0].strip()
        region = _region_from_ip(client_ip)
        if region is not None:
            logger.debug(f"Edge routing: XFF IP={client_ip} -> {region.value}")
            return region

    # 3. Accept-Language
    accept_lang = h.get("accept-language") or ""
    if accept_lang:
        # Parse first language tag: "en-US,en;q=0.9" -> "en"
        primary_lang = accept_lang.split(",")[0].split(";")[0].strip()
        lang_prefix = primary_lang.split("-")[0].lower()
        region = _LANG_REGION.get(lang_prefix)
        if region is not None:
            logger.debug(
                f"Edge routing: Accept-Language={primary_lang} -> {region.value}"
            )
            return region

    # 4. Default
    logger.debug("Edge routing: no geo signal, defaulting to us-west")
    return Region.US_WEST


# ---------------------------------------------------------------------------
# Routing stats tracker (thread-safe)
# ---------------------------------------------------------------------------


class _RoutingStats:
    """Track routing decisions for observability."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._region_hits: dict[str, int] = defaultdict(int)
        self._provider_selections: dict[str, int] = defaultdict(int)
        self._fallback_count: int = 0
        self._total_routes: int = 0
        self._start_time: float = time.time()

    def record_route(
        self,
        user_region: Region,
        selected_provider: str,
        was_fallback: bool,
    ) -> None:
        """Record a routing decision."""
        with self._lock:
            self._region_hits[user_region.value] += 1
            self._provider_selections[selected_provider] += 1
            self._total_routes += 1
            if was_fallback:
                self._fallback_count += 1

    def get_stats(self) -> dict:
        """Return routing stats snapshot."""
        with self._lock:
            return {
                "total_routes": self._total_routes,
                "fallback_rate": (
                    round(self._fallback_count / max(1, self._total_routes), 4)
                ),
                "region_distribution": dict(self._region_hits),
                "top_providers": dict(
                    sorted(
                        self._provider_selections.items(),
                        key=lambda x: x[1],
                        reverse=True,
                    )[:10]
                ),
                "uptime_seconds": round(time.time() - self._start_time, 2),
            }


_routing_stats = _RoutingStats()


# ---------------------------------------------------------------------------
# Core routing logic
# ---------------------------------------------------------------------------


def get_optimal_providers(
    user_region: Region,
    available_providers: list[str],
    *,
    max_results: int = 10,
) -> list[str]:
    """Return providers ranked by proximity to user region.

    Walks the fallback chain for the user's region (nearest -> furthest),
    and within each region bucket, sorts by average observed latency
    (lowest first).  Global providers are injected as universal fallback.

    Args:
        user_region: Detected user region.
        available_providers: List of provider names currently available.
        max_results: Maximum number of providers to return.

    Returns:
        Ordered list of provider names, best first.
    """
    available_set = set(available_providers)
    chain = REGION_FALLBACK_CHAIN.get(user_region, list(Region))
    ranked: list[str] = []
    seen: set[str] = set()

    for region in chain:
        region_providers = REGION_PROVIDERS.get(region, [])
        # Filter to available and unseen
        candidates = [
            p for p in region_providers if p in available_set and p not in seen
        ]

        # Sort by observed latency within the region bucket
        def _latency_sort_key(p: str) -> float:
            avg = _latency_tracker.get_avg(user_region, p)
            if avg is not None:
                return avg
            # No data yet -- assume baseline per region distance
            return _estimated_latency(user_region, region)

        candidates.sort(key=_latency_sort_key)

        for p in candidates:
            ranked.append(p)
            seen.add(p)
            if len(ranked) >= max_results:
                break
        if len(ranked) >= max_results:
            break

    # Record stats for top pick
    if ranked:
        was_fallback = PROVIDER_REGION_MAP.get(ranked[0]) != user_region
        _routing_stats.record_route(user_region, ranked[0], was_fallback)

    return ranked[:max_results]


def _estimated_latency(user_region: Region, provider_region: Region) -> float:
    """Estimated baseline latency (ms) between two regions.

    Used as a tiebreaker when no observed latency data exists.
    """
    if user_region == provider_region:
        return 20.0

    # Cross-region estimates (rough, in ms)
    _CROSS_LATENCY: dict[tuple[Region, Region], float] = {
        (Region.US_WEST, Region.US_EAST): 60.0,
        (Region.US_WEST, Region.EU): 130.0,
        (Region.US_WEST, Region.ASIA): 150.0,
        (Region.US_EAST, Region.EU): 90.0,
        (Region.US_EAST, Region.ASIA): 200.0,
        (Region.EU, Region.ASIA): 180.0,
    }

    key = (user_region, provider_region)
    reverse_key = (provider_region, user_region)

    if key in _CROSS_LATENCY:
        return _CROSS_LATENCY[key]
    if reverse_key in _CROSS_LATENCY:
        return _CROSS_LATENCY[reverse_key]

    # Global providers -- moderate baseline
    if provider_region == Region.GLOBAL or user_region == Region.GLOBAL:
        return 80.0

    return 100.0  # Unknown pair


# ---------------------------------------------------------------------------
# Convenience: single-call from request headers
# ---------------------------------------------------------------------------


def route_request(
    headers: dict[str, str],
    available_providers: list[str],
    *,
    max_results: int = 10,
) -> tuple[Region, list[str]]:
    """Detect region and return ranked providers in one call.

    Args:
        headers: HTTP request headers.
        available_providers: Currently available provider names.
        max_results: Max providers to return.

    Returns:
        Tuple of (detected_region, ranked_provider_list).
    """
    region = detect_user_region(headers)
    ranked = get_optimal_providers(region, available_providers, max_results=max_results)
    return region, ranked


def record_provider_latency(
    user_region: Region,
    provider: str,
    latency_ms: float,
) -> None:
    """Record observed latency for a provider from a user region.

    Call this after each LLM request completes to improve future routing.

    Args:
        user_region: The region the request originated from.
        provider: The provider name that handled the request.
        latency_ms: Observed round-trip latency in milliseconds.
    """
    _latency_tracker.record(user_region, provider, latency_ms)


# ---------------------------------------------------------------------------
# Health / stats endpoint
# ---------------------------------------------------------------------------


def get_edge_routing_stats() -> dict:
    """Return edge routing stats for /api/health integration.

    Returns:
        Dict with routing stats, latency data, and region configuration.
    """
    return {
        "enabled": True,
        "regions": {r.value: REGION_PROVIDERS[r] for r in Region},
        "routing": _routing_stats.get_stats(),
        "latency": _latency_tracker.get_stats(),
        "fallback_chains": {
            r.value: [fr.value for fr in chain]
            for r, chain in REGION_FALLBACK_CHAIN.items()
        },
        "total_providers_mapped": len(PROVIDER_REGION_MAP),
    }
