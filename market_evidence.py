"""Strict provenance checks for client-visible current-market evidence."""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, FrozenSet, Optional, Set


CONTRACT = "market_evidence_v1"
CANONICAL_PROVIDER_IDS: Dict[str, FrozenSet[str]] = {
    "job_postings": frozenset({"adzuna", "jooble"}),
    "search_volume": frozenset({"google_ads"}),
    "talent_pool": frozenset({"linkedin"}),
    "trend": frozenset({"google_trends"}),
}
CANONICAL_PROVIDER_LABELS: Dict[str, str] = {
    "adzuna": "Adzuna",
    "jooble": "Jooble API",
    "google_ads": "Google Ads",
    "linkedin": "LinkedIn",
    "google_trends": "Google Trends",
}
_TEMPERATURE_COMPONENTS = ("job_postings", "talent_pool")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def valid_observation_date(value: Any, *, today: Optional[date] = None) -> bool:
    """Return whether value is a real, non-future ISO calendar date."""
    if not isinstance(value, str) or not _ISO_DATE_RE.fullmatch(value):
        return False
    try:
        observed = date.fromisoformat(value)
    except ValueError:
        return False
    return observed <= (today or date.today())


def observation_is_valid(
    metric: str,
    status: Any,
    provider_id: Any,
    observed_at: Any,
    *,
    today: Optional[date] = None,
) -> bool:
    """Validate one raw observation against its metric's canonical provider."""
    allowed = CANONICAL_PROVIDER_IDS.get(metric)
    return (
        status == "observed"
        and isinstance(provider_id, str)
        and allowed is not None
        and provider_id in allowed
        and valid_observation_date(observed_at, today=today)
    )


def canonical_market_source_label(metric: str, provider_id: Any) -> str:
    """Return the display label only for a canonical provider of this metric."""
    allowed = CANONICAL_PROVIDER_IDS.get(metric)
    if (
        not isinstance(provider_id, str)
        or allowed is None
        or provider_id not in allowed
    ):
        return ""
    return CANONICAL_PROVIDER_LABELS.get(provider_id, "")


def _metric_block(role_data: Any, metric: str) -> Optional[dict]:
    if not isinstance(role_data, dict):
        return None
    evidence = role_data.get("market_evidence")
    if not isinstance(evidence, dict) or evidence.get("contract") != CONTRACT:
        return None
    metrics = evidence.get("metrics")
    block = metrics.get(metric) if isinstance(metrics, dict) else None
    return block if isinstance(block, dict) else None


def _basic_metric_is_observed(
    role_data: Any, metric: str, *, today: Optional[date] = None
) -> bool:
    block = _metric_block(role_data, metric)
    allowed = CANONICAL_PROVIDER_IDS.get(metric)
    if block is None or allowed is None or block.get("status") != "observed":
        return False
    provider_ids = block.get("provider_ids")
    observed_at = block.get("observed_at")
    if not isinstance(provider_ids, list) or not provider_ids:
        return False
    if not isinstance(observed_at, list) or not observed_at:
        return False
    if any(not isinstance(provider_id, str) for provider_id in provider_ids):
        return False
    if any(not isinstance(value, str) for value in observed_at):
        return False
    if len(provider_ids) != len(set(provider_ids)):
        return False
    if len(observed_at) != len(set(observed_at)):
        return False
    if not set(provider_ids).issubset(allowed):
        return False
    return all(valid_observation_date(value, today=today) for value in observed_at)


def market_metric_is_observed(
    role_data: Any, metric: str, *, today: Optional[date] = None
) -> bool:
    """Validate an observed metric in the normalized market-evidence contract."""
    if metric != "market_temperature":
        return _basic_metric_is_observed(role_data, metric, today=today)

    temperature = _metric_block(role_data, metric)
    if temperature is None or temperature.get("status") != "observed":
        return False
    if not all(
        _basic_metric_is_observed(role_data, component, today=today)
        for component in _TEMPERATURE_COMPONENTS
    ):
        return False

    provider_ids = temperature.get("provider_ids")
    observed_at = temperature.get("observed_at")
    if not isinstance(provider_ids, list) or not provider_ids:
        return False
    if not isinstance(observed_at, list) or not observed_at:
        return False
    if any(not isinstance(provider_id, str) for provider_id in provider_ids):
        return False
    if any(not isinstance(value, str) for value in observed_at):
        return False
    component_providers: Set[str] = set()
    component_dates: Set[str] = set()
    for component in _TEMPERATURE_COMPONENTS:
        block = _metric_block(role_data, component) or {}
        component_providers.update(block.get("provider_ids") or [])
        component_dates.update(block.get("observed_at") or [])
    return (
        set(provider_ids) == component_providers and set(observed_at) == component_dates
    )


def observed_market_provider_ids(role_data: Any) -> Set[str]:
    """Return canonical providers from valid observed component metrics."""
    providers: Set[str] = set()
    for metric in CANONICAL_PROVIDER_IDS:
        if market_metric_is_observed(role_data, metric):
            block = _metric_block(role_data, metric) or {}
            providers.update(block.get("provider_ids") or [])
    return providers


def market_source_detail_is_valid(role_data: Any, metric: str, detail: Any) -> bool:
    """Validate a display source against the role's observed metric evidence."""
    if not market_metric_is_observed(role_data, metric) or not isinstance(detail, dict):
        return False
    provider_id = detail.get("provider_id")
    observed_at = detail.get("observed_at")
    block = _metric_block(role_data, metric) or {}
    return (
        bool(canonical_market_source_label(metric, provider_id))
        and observation_is_valid(metric, "observed", provider_id, observed_at)
        and provider_id in (block.get("provider_ids") or [])
        and observed_at in (block.get("observed_at") or [])
    )


def validated_market_source_details(
    role_data: Any, metric: str, details: Any
) -> list[dict]:
    """Return valid details only when they cover the metric's full evidence set."""
    if not isinstance(details, list) or not details:
        return []
    valid_details = []
    for detail in details:
        if not market_source_detail_is_valid(role_data, metric, detail):
            continue
        normalized = dict(detail)
        normalized["source"] = canonical_market_source_label(
            metric, normalized["provider_id"]
        )
        valid_details.append(normalized)
    if not valid_details:
        return []
    block = _metric_block(role_data, metric) or {}
    required_providers = set(block.get("provider_ids") or [])
    required_dates = set(block.get("observed_at") or [])
    covered_providers = {detail["provider_id"] for detail in valid_details}
    covered_dates = {detail["observed_at"] for detail in valid_details}
    if covered_providers != required_providers or covered_dates != required_dates:
        return []
    return valid_details
