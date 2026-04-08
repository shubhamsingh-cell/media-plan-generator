"""Real-Time Market Signal Ingestion System.

Continuously ingests and processes market signals (job posting volumes,
CPC changes, salary shifts, channel performance) to keep recommendations
fresh.  Signals are computed on-demand with a 5-minute cache to avoid
redundant work on rapid successive API calls.

Signal types:
    CPCChange, DemandShift, SalaryUpdate, ChannelPerformance,
    SeasonalTrend, CompetitorActivity
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import statistics
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "data"
_LIVE_MARKET_FILE = _DATA_DIR / "live_market_data.json"
_CHANNEL_BENCH_FILE = _DATA_DIR / "channel_benchmarks_live.json"
_CACHE_TTL_SECONDS = 300  # 5-minute cache
_CPC_CHANGE_THRESHOLD = 0.10  # 10% shift triggers signal
_DEMAND_CHANGE_THRESHOLD = 0.15  # 15% change triggers signal
_SALARY_CHANGE_THRESHOLD = 0.08  # 8% salary shift triggers signal

# ---------------------------------------------------------------------------
# Signal Types
# ---------------------------------------------------------------------------


class SignalType(str, Enum):
    """Market signal categories."""

    CPC_CHANGE = "CPCChange"
    DEMAND_SHIFT = "DemandShift"
    SALARY_UPDATE = "SalaryUpdate"
    CHANNEL_PERFORMANCE = "ChannelPerformance"
    SEASONAL_TREND = "SeasonalTrend"
    COMPETITOR_ACTIVITY = "CompetitorActivity"


class SignalSeverity(str, Enum):
    """Signal severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Signal:
    """A market signal representing a detected change or trend."""

    signal_type: str
    severity: str
    title: str
    description: str
    channel: str = ""
    role_family: str = ""
    location: str = ""
    metric_name: str = ""
    current_value: float = 0.0
    previous_value: float = 0.0
    change_pct: float = 0.0
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize signal to dictionary."""
        return asdict(self)


# ---------------------------------------------------------------------------
# Baseline Historical Data (reference for change detection)
# ---------------------------------------------------------------------------

_HISTORICAL_CPC_BASELINES: Dict[str, Dict[str, float]] = {
    "indeed": {"min": 0.20, "max": 4.50, "typical": 1.20},
    "linkedin": {"min": 2.50, "max": 7.50, "typical": 4.50},
    "ziprecruiter": {"min": 0.60, "max": 5.00, "typical": 2.00},
    "glassdoor": {"min": 1.20, "max": 5.50, "typical": 2.50},
    "monster": {"min": 0.40, "max": 3.50, "typical": 1.50},
    "careerbuilder": {"min": 0.60, "max": 2.80, "typical": 1.40},
}

_HISTORICAL_INDUSTRY_CPC: Dict[str, float] = {
    "technology": 0.70,
    "healthcare": 1.60,
    "retail": 0.30,
    "finance": 1.10,
    "manufacturing": 0.55,
    "hospitality": 0.28,
    "engineering": 1.40,
    "cybersecurity": 2.20,
    "data_science": 1.80,
}

_HISTORICAL_COST_PER_HIRE: Dict[str, float] = {
    "technology": 5800,
    "healthcare": 8500,
    "retail": 2500,
    "finance": 5200,
    "manufacturing": 4000,
    "hospitality": 2500,
    "engineering": 5800,
    "cybersecurity": 9200,
    "data_science": 9200,
    "overall": 4500,
}

_SEASONAL_PATTERNS: Dict[int, Dict[str, float]] = {
    1: {"demand_multiplier": 1.15, "label": "New Year hiring surge"},
    2: {"demand_multiplier": 1.10, "label": "Q1 budget activation"},
    3: {"demand_multiplier": 1.05, "label": "Spring hiring ramp"},
    4: {"demand_multiplier": 1.00, "label": "Steady state"},
    5: {"demand_multiplier": 0.95, "label": "Pre-summer slowdown"},
    6: {"demand_multiplier": 0.90, "label": "Summer slowdown begins"},
    7: {"demand_multiplier": 0.85, "label": "Peak summer lull"},
    8: {"demand_multiplier": 0.90, "label": "Back-to-school recovery"},
    9: {"demand_multiplier": 1.10, "label": "Fall hiring push"},
    10: {"demand_multiplier": 1.05, "label": "Q4 planning hires"},
    11: {"demand_multiplier": 0.95, "label": "Thanksgiving slowdown"},
    12: {"demand_multiplier": 0.80, "label": "Holiday freeze"},
}


# ---------------------------------------------------------------------------
# Data Loading (thread-safe)
# ---------------------------------------------------------------------------

_data_lock = threading.Lock()


def _load_json_file(filepath: Path) -> Optional[Dict[str, Any]]:
    """Load and parse a JSON file, returning None on failure."""
    try:
        if not filepath.exists():
            logger.warning("Market signals data file not found: %s", filepath)
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", filepath, e, exc_info=True)
        return None
    except OSError as e:
        logger.error("Failed to read %s: %s", filepath, e, exc_info=True)
        return None


def _load_market_data() -> Optional[Dict[str, Any]]:
    """Load live market data from data/live_market_data.json."""
    return _load_json_file(_LIVE_MARKET_FILE)


def _load_channel_benchmarks() -> Optional[Dict[str, Any]]:
    """Load channel benchmarks from data/channel_benchmarks_live.json."""
    return _load_json_file(_CHANNEL_BENCH_FILE)


# ---------------------------------------------------------------------------
# Signal Computation
# ---------------------------------------------------------------------------


def _pct_change(current: float, previous: float) -> float:
    """Compute percentage change, safe against zero division."""
    if previous == 0:
        return 0.0 if current == 0 else 100.0
    return (current - previous) / abs(previous)


def _severity_from_change(change_pct: float) -> str:
    """Map absolute percentage change to severity level."""
    abs_change = abs(change_pct)
    if abs_change >= 0.30:
        return SignalSeverity.CRITICAL.value
    if abs_change >= 0.20:
        return SignalSeverity.HIGH.value
    if abs_change >= 0.10:
        return SignalSeverity.MEDIUM.value
    return SignalSeverity.LOW.value


def _compute_cpc_signals(market_data: Dict[str, Any]) -> List[Signal]:
    """Detect significant CPC changes across job boards."""
    signals: List[Signal] = []
    job_boards = market_data.get("job_boards") or {}

    for board_key, board_data in job_boards.items():
        baseline = _HISTORICAL_CPC_BASELINES.get(board_key)
        if not baseline:
            continue

        cpc_range = board_data.get("cpc_range") or {}
        current_min = cpc_range.get("min", 0.0)
        current_max = cpc_range.get("max", 0.0)
        if current_min == 0.0 and current_max == 0.0:
            # Try alternate field names (careerbuilder format)
            current_min = board_data.get("avg_cpc_min", 0.0)
            current_max = board_data.get("avg_cpc_max", 0.0)

        if current_min == 0.0 and current_max == 0.0:
            continue

        current_typical = (current_min + current_max) / 2.0
        baseline_typical = baseline["typical"]
        change = _pct_change(current_typical, baseline_typical)

        if abs(change) >= _CPC_CHANGE_THRESHOLD:
            direction = "increased" if change > 0 else "decreased"
            board_name = board_data.get("board_name") or board_key.title()
            signals.append(
                Signal(
                    signal_type=SignalType.CPC_CHANGE.value,
                    severity=_severity_from_change(change),
                    title=f"CPC {direction} on {board_name}",
                    description=(
                        f"{board_name} CPC {direction} by {abs(change)*100:.1f}% "
                        f"(${baseline_typical:.2f} -> ${current_typical:.2f})"
                    ),
                    channel=board_key,
                    metric_name="cpc_typical",
                    current_value=round(current_typical, 2),
                    previous_value=round(baseline_typical, 2),
                    change_pct=round(change * 100, 1),
                    metadata={
                        "current_range": [current_min, current_max],
                        "baseline_range": [baseline["min"], baseline["max"]],
                    },
                )
            )

    return signals


def _compute_demand_signals(market_data: Dict[str, Any]) -> List[Signal]:
    """Detect demand shifts from industry benchmark changes."""
    signals: List[Signal] = []
    benchmarks = market_data.get("industry_benchmarks") or {}

    for industry, bench_data in benchmarks.items():
        if industry == "overall":
            continue

        current_cpc = bench_data.get("avg_cpc", 0.0)
        historical_cpc = _HISTORICAL_INDUSTRY_CPC.get(industry, 0.0)
        if current_cpc == 0.0 or historical_cpc == 0.0:
            continue

        change = _pct_change(current_cpc, historical_cpc)
        if abs(change) >= _DEMAND_CHANGE_THRESHOLD:
            direction = "rising" if change > 0 else "falling"
            signals.append(
                Signal(
                    signal_type=SignalType.DEMAND_SHIFT.value,
                    severity=_severity_from_change(change),
                    title=f"Demand {direction} in {industry.replace('_', ' ').title()}",
                    description=(
                        f"Industry CPC for {industry.replace('_', ' ')} shifted "
                        f"{abs(change)*100:.1f}% ({direction}), indicating "
                        f"{'increased competition' if change > 0 else 'softening demand'}"
                    ),
                    role_family=industry,
                    metric_name="industry_avg_cpc",
                    current_value=round(current_cpc, 2),
                    previous_value=round(historical_cpc, 2),
                    change_pct=round(change * 100, 1),
                    metadata={
                        "apply_rate_pct": bench_data.get("apply_rate_pct", 0.0),
                        "cost_per_hire": bench_data.get("avg_cost_per_hire", 0),
                    },
                )
            )

    return signals


def _compute_salary_signals(market_data: Dict[str, Any]) -> List[Signal]:
    """Detect cost-per-hire shifts that indicate salary market changes."""
    signals: List[Signal] = []
    benchmarks = market_data.get("industry_benchmarks") or {}

    for industry, bench_data in benchmarks.items():
        current_cph = bench_data.get("avg_cost_per_hire", 0)
        historical_cph = _HISTORICAL_COST_PER_HIRE.get(industry, 0)
        if current_cph == 0 or historical_cph == 0:
            continue

        change = _pct_change(float(current_cph), float(historical_cph))
        if abs(change) >= _SALARY_CHANGE_THRESHOLD:
            direction = "rising" if change > 0 else "falling"
            signals.append(
                Signal(
                    signal_type=SignalType.SALARY_UPDATE.value,
                    severity=_severity_from_change(change),
                    title=f"Cost-per-hire {direction} for {industry.replace('_', ' ').title()}",
                    description=(
                        f"Cost-per-hire for {industry.replace('_', ' ')} shifted "
                        f"{abs(change)*100:.1f}% "
                        f"(${historical_cph:,.0f} -> ${current_cph:,.0f})"
                    ),
                    role_family=industry,
                    metric_name="avg_cost_per_hire",
                    current_value=float(current_cph),
                    previous_value=float(historical_cph),
                    change_pct=round(change * 100, 1),
                )
            )

    return signals


def _compute_channel_performance_signals(
    market_data: Dict[str, Any],
    channel_benchmarks: Optional[Dict[str, Any]],
) -> List[Signal]:
    """Evaluate channel performance from job board and benchmark data."""
    signals: List[Signal] = []
    job_boards = market_data.get("job_boards") or {}

    for board_key, board_data in job_boards.items():
        cpc_range = board_data.get("cpc_range") or {}
        cpa_range = board_data.get("cpa_estimate") or {}
        cpc_min = cpc_range.get("min") or board_data.get("avg_cpc_min", 0.0)
        cpc_max = cpc_range.get("max") or board_data.get("avg_cpc_max", 0.0)
        cpa_min = cpa_range.get("min", 0.0)
        cpa_max = cpa_range.get("max", 0.0)

        if cpc_min == 0.0 and cpc_max == 0.0:
            continue

        board_name = board_data.get("board_name") or board_key.title()
        has_free = board_data.get("posting_cost", {}).get("free_option", False)
        cpc_mid = (cpc_min + cpc_max) / 2.0

        # Score channels: lower CPC + free option = better value
        value_score = 100.0
        if cpc_mid > 3.0:
            value_score -= 30
        elif cpc_mid > 1.5:
            value_score -= 15
        if not has_free:
            value_score -= 10
        if cpa_max > 50:
            value_score -= 20
        elif cpa_max > 30:
            value_score -= 10

        signals.append(
            Signal(
                signal_type=SignalType.CHANNEL_PERFORMANCE.value,
                severity=(
                    SignalSeverity.HIGH.value
                    if value_score >= 70
                    else (
                        SignalSeverity.MEDIUM.value
                        if value_score >= 50
                        else SignalSeverity.LOW.value
                    )
                ),
                title=f"{board_name} performance assessment",
                description=(
                    f"{board_name}: CPC ${cpc_min:.2f}-${cpc_max:.2f}, "
                    f"CPA ${cpa_min:.0f}-${cpa_max:.0f}, "
                    f"{'free tier available' if has_free else 'paid only'} "
                    f"(value score: {value_score:.0f}/100)"
                ),
                channel=board_key,
                metric_name="channel_value_score",
                current_value=round(value_score, 1),
                metadata={
                    "cpc_range": [cpc_min, cpc_max],
                    "cpa_range": [cpa_min, cpa_max],
                    "free_option": has_free,
                    "model": board_data.get("model")
                    or board_data.get("pricing_model")
                    or "",
                },
            )
        )

    return signals


def _compute_seasonal_signals() -> List[Signal]:
    """Detect seasonal trends based on current month."""
    signals: List[Signal] = []
    now = datetime.datetime.now(datetime.timezone.utc)
    current_month = now.month
    current_pattern = _SEASONAL_PATTERNS.get(current_month)
    if not current_pattern:
        return signals

    multiplier = current_pattern["demand_multiplier"]
    label = current_pattern["label"]

    severity = SignalSeverity.LOW.value
    if multiplier >= 1.10:
        severity = SignalSeverity.HIGH.value
    elif multiplier <= 0.85:
        severity = SignalSeverity.HIGH.value
    elif abs(multiplier - 1.0) >= 0.05:
        severity = SignalSeverity.MEDIUM.value

    direction = "above" if multiplier > 1.0 else "below" if multiplier < 1.0 else "at"

    signals.append(
        Signal(
            signal_type=SignalType.SEASONAL_TREND.value,
            severity=severity,
            title=f"Seasonal factor: {label}",
            description=(
                f"Current month ({now.strftime('%B')}) shows demand "
                f"{abs(multiplier - 1.0)*100:.0f}% {direction} baseline. "
                f"Pattern: {label}."
            ),
            metric_name="seasonal_demand_multiplier",
            current_value=round(multiplier, 2),
            previous_value=1.0,
            change_pct=round((multiplier - 1.0) * 100, 1),
            metadata={
                "month": current_month,
                "month_name": now.strftime("%B"),
                "next_month_multiplier": _SEASONAL_PATTERNS.get(
                    (current_month % 12) + 1, {}
                ).get("demand_multiplier", 1.0),
            },
        )
    )

    return signals


def _compute_competitor_signals(market_data: Dict[str, Any]) -> List[Signal]:
    """Detect competitor activity from pricing model shifts and trends."""
    signals: List[Signal] = []
    trends = market_data.get("trends") or []

    for trend in trends:
        title = trend.get("title") or ""
        summary = trend.get("summary") or ""
        source = trend.get("source") or ""

        if not title:
            continue

        # Classify trend severity by keywords
        severity = SignalSeverity.MEDIUM.value
        title_lower = title.lower()
        if any(
            w in title_lower for w in ("sharply", "surge", "significantly", "premium")
        ):
            severity = SignalSeverity.HIGH.value
        elif any(w in title_lower for w in ("slight", "moderate", "stable")):
            severity = SignalSeverity.LOW.value

        signals.append(
            Signal(
                signal_type=SignalType.COMPETITOR_ACTIVITY.value,
                severity=severity,
                title=title,
                description=summary[:200] if len(summary) > 200 else summary,
                metadata={
                    "source": source,
                    "date": trend.get("date") or "",
                },
            )
        )

    return signals


# ---------------------------------------------------------------------------
# Volatility Index
# ---------------------------------------------------------------------------


def _compute_volatility_index(signals: List[Signal]) -> Dict[str, Any]:
    """Compute market volatility index (0-100) from active signals.

    Higher score = more volatile / more change detected.
    """
    if not signals:
        return {
            "index": 0,
            "label": "stable",
            "description": "No significant market movements detected",
            "contributing_factors": [],
        }

    severity_weights = {
        SignalSeverity.LOW.value: 5,
        SignalSeverity.MEDIUM.value: 15,
        SignalSeverity.HIGH.value: 30,
        SignalSeverity.CRITICAL.value: 50,
    }

    weighted_sum = sum(severity_weights.get(s.severity, 5) for s in signals)

    # Normalize: cap at 100
    raw_index = min(100, weighted_sum)

    # Factor in absolute change percentages
    change_values = [abs(s.change_pct) for s in signals if s.change_pct != 0.0]
    avg_change = statistics.mean(change_values) if change_values else 0.0

    # Blend: 60% signal-count-based, 40% change-magnitude-based
    magnitude_score = min(100, avg_change * 3)
    final_index = round(raw_index * 0.6 + magnitude_score * 0.4)
    final_index = max(0, min(100, final_index))

    if final_index >= 70:
        label = "highly_volatile"
        desc = "Significant market disruption -- multiple major shifts detected"
    elif final_index >= 45:
        label = "moderately_volatile"
        desc = "Notable market changes -- adjustments to strategy recommended"
    elif final_index >= 20:
        label = "slightly_volatile"
        desc = "Minor market shifts -- monitor but no immediate action needed"
    else:
        label = "stable"
        desc = "Market conditions relatively stable"

    # Top contributing factors
    contributing = sorted(
        [
            {
                "signal": s.title,
                "severity": s.severity,
                "change_pct": s.change_pct,
            }
            for s in signals
            if s.severity in (SignalSeverity.HIGH.value, SignalSeverity.CRITICAL.value)
        ],
        key=lambda x: abs(x.get("change_pct", 0)),
        reverse=True,
    )[:5]

    return {
        "index": final_index,
        "label": label,
        "description": desc,
        "signal_count": len(signals),
        "avg_change_pct": round(avg_change, 1),
        "contributing_factors": contributing,
        "computed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Trending Channels
# ---------------------------------------------------------------------------


def _compute_trending_channels(signals: List[Signal]) -> List[Dict[str, Any]]:
    """Identify channels with rising performance or notable changes."""
    channel_scores: Dict[str, Dict[str, Any]] = {}

    for s in signals:
        if not s.channel:
            continue

        if s.channel not in channel_scores:
            channel_scores[s.channel] = {
                "channel": s.channel,
                "signals": [],
                "total_change": 0.0,
                "direction": "stable",
                "value_score": 0.0,
            }

        entry = channel_scores[s.channel]
        entry["signals"].append(s.title)
        entry["total_change"] += s.change_pct

        if s.signal_type == SignalType.CHANNEL_PERFORMANCE.value:
            entry["value_score"] = s.current_value

    trending = []
    for ch_key, ch_data in channel_scores.items():
        total_change = ch_data["total_change"]
        if total_change > 0:
            direction = "rising"
        elif total_change < 0:
            direction = "falling"
        else:
            direction = "stable"

        trending.append(
            {
                "channel": ch_key,
                "direction": direction,
                "total_change_pct": round(total_change, 1),
                "value_score": round(ch_data["value_score"], 1),
                "signal_count": len(ch_data["signals"]),
                "signals": ch_data["signals"][:3],
            }
        )

    # Sort: rising channels first, then by absolute change magnitude
    trending.sort(
        key=lambda x: (
            0 if x["direction"] == "rising" else 1,
            -abs(x["total_change_pct"]),
        )
    )
    return trending


# ---------------------------------------------------------------------------
# Cached Signal Engine (singleton, thread-safe)
# ---------------------------------------------------------------------------


class MarketSignalEngine:
    """Thread-safe market signal engine with 5-minute cache."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._signals: List[Signal] = []
        self._volatility: Dict[str, Any] = {}
        self._trending: List[Dict[str, Any]] = []
        self._last_computed: float = 0.0
        self._signal_history: List[Dict[str, Any]] = []
        self._history_lock = threading.Lock()
        self._computation_count: int = 0

    def _is_cache_valid(self) -> bool:
        """Check if cached signals are still fresh."""
        return (time.time() - self._last_computed) < _CACHE_TTL_SECONDS

    def _refresh_if_needed(self) -> None:
        """Recompute signals if cache has expired."""
        if self._is_cache_valid():
            return

        with self._lock:
            # Double-check after acquiring lock
            if self._is_cache_valid():
                return

            try:
                self._compute_all_signals()
            except Exception as e:
                logger.error("Failed to compute market signals: %s", e, exc_info=True)

    def _compute_all_signals(self) -> None:
        """Load data and compute all signal types."""
        start_time = time.time()
        market_data = _load_market_data()
        channel_benchmarks = _load_channel_benchmarks()

        if market_data is None:
            logger.warning("No market data available for signal computation")
            self._last_computed = time.time()
            return

        all_signals: List[Signal] = []

        # Compute each signal type with error isolation
        try:
            all_signals.extend(_compute_cpc_signals(market_data))
        except (KeyError, TypeError, ValueError) as e:
            logger.error("CPC signal computation failed: %s", e, exc_info=True)

        try:
            all_signals.extend(_compute_demand_signals(market_data))
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Demand signal computation failed: %s", e, exc_info=True)

        try:
            all_signals.extend(_compute_salary_signals(market_data))
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Salary signal computation failed: %s", e, exc_info=True)

        try:
            all_signals.extend(
                _compute_channel_performance_signals(market_data, channel_benchmarks)
            )
        except (KeyError, TypeError, ValueError) as e:
            logger.error(
                f"Channel performance signal computation failed: {e}", exc_info=True
            )

        try:
            all_signals.extend(_compute_seasonal_signals())
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Seasonal signal computation failed: %s", e, exc_info=True)

        try:
            all_signals.extend(_compute_competitor_signals(market_data))
        except (KeyError, TypeError, ValueError) as e:
            logger.error("Competitor signal computation failed: %s", e, exc_info=True)

        self._signals = all_signals
        self._volatility = _compute_volatility_index(all_signals)
        self._trending = _compute_trending_channels(all_signals)
        self._last_computed = time.time()
        self._computation_count += 1

        # Append to history (keep last 1000 entries)
        with self._history_lock:
            self._signal_history.append(
                {
                    "computed_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "signal_count": len(all_signals),
                    "volatility_index": self._volatility.get("index", 0),
                    "computation_ms": round((time.time() - start_time) * 1000, 1),
                }
            )
            if len(self._signal_history) > 1000:
                self._signal_history = self._signal_history[-500:]

        elapsed = round((time.time() - start_time) * 1000, 1)
        logger.info(
            f"Market signals computed: {len(all_signals)} signals, "
            f"volatility={self._volatility.get('index', 0)}, "
            f"elapsed={elapsed}ms"
        )

    # -- Public API --

    def get_active_signals(
        self,
        role_family: Optional[str] = None,
        location: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get active market signals, optionally filtered.

        Args:
            role_family: Filter signals by industry/role family (e.g. 'technology').
            location: Filter signals by location (currently reserved for future use).

        Returns:
            List of signal dictionaries sorted by severity.
        """
        self._refresh_if_needed()
        signals = self._signals

        if role_family:
            role_lower = role_family.lower().strip()
            signals = [
                s
                for s in signals
                if (
                    s.role_family.lower() == role_lower
                    or not s.role_family  # include global signals
                )
            ]

        if location:
            loc_lower = location.lower().strip()
            signals = [
                s
                for s in signals
                if (s.location.lower() == loc_lower or not s.location)
            ]

        severity_order = {
            SignalSeverity.CRITICAL.value: 0,
            SignalSeverity.HIGH.value: 1,
            SignalSeverity.MEDIUM.value: 2,
            SignalSeverity.LOW.value: 3,
        }
        signals_sorted = sorted(
            signals, key=lambda s: severity_order.get(s.severity, 99)
        )

        return [s.to_dict() for s in signals_sorted]

    def get_market_volatility(self) -> Dict[str, Any]:
        """Get the current market volatility index.

        Returns:
            Dict with index (0-100), label, description, and contributing factors.
        """
        self._refresh_if_needed()
        return self._volatility

    def get_trending_channels(self) -> List[Dict[str, Any]]:
        """Get channels with notable performance changes.

        Returns:
            List of channel trend dicts sorted by direction and magnitude.
        """
        self._refresh_if_needed()
        return self._trending

    def get_signal_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent signal computation history.

        Args:
            hours: Number of hours of history to return.

        Returns:
            List of computation snapshots.
        """
        self._refresh_if_needed()
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=hours
        )
        cutoff_iso = cutoff.isoformat()

        with self._history_lock:
            return [
                entry
                for entry in self._signal_history
                if (entry.get("computed_at") or "") >= cutoff_iso
            ]

    def get_signal_stats(self) -> Dict[str, Any]:
        """Get signal engine stats for /api/health.

        Returns:
            Dict with engine status, signal counts, and cache info.
        """
        self._refresh_if_needed()

        by_type: Dict[str, int] = {}
        by_severity: Dict[str, int] = {}
        for s in self._signals:
            by_type[s.signal_type] = by_type.get(s.signal_type, 0) + 1
            by_severity[s.severity] = by_severity.get(s.severity, 0) + 1

        return {
            "status": "ok",
            "active_signals": len(self._signals),
            "by_type": by_type,
            "by_severity": by_severity,
            "volatility_index": self._volatility.get("index", 0),
            "volatility_label": self._volatility.get("label", "unknown"),
            "trending_channels": len(self._trending),
            "cache_age_seconds": (
                round(time.time() - self._last_computed, 1)
                if self._last_computed > 0
                else None
            ),
            "computation_count": self._computation_count,
            "cache_ttl_seconds": _CACHE_TTL_SECONDS,
            "data_files": {
                "live_market_data": _LIVE_MARKET_FILE.exists(),
                "channel_benchmarks": _CHANNEL_BENCH_FILE.exists(),
            },
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_engine: Optional[MarketSignalEngine] = None
_engine_lock = threading.Lock()


def _get_engine() -> MarketSignalEngine:
    """Get or create the singleton MarketSignalEngine."""
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = MarketSignalEngine()
    return _engine


# ---------------------------------------------------------------------------
# Public API (module-level convenience functions)
# ---------------------------------------------------------------------------


def get_active_signals(
    role_family: Optional[str] = None,
    location: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get active market signals, optionally filtered by role family or location.

    Args:
        role_family: Filter by industry/role family (e.g. 'technology', 'healthcare').
        location: Filter by location (reserved for future use).

    Returns:
        List of signal dictionaries sorted by severity (critical first).
    """
    return _get_engine().get_active_signals(role_family=role_family, location=location)


def get_market_volatility() -> Dict[str, Any]:
    """Get the current market volatility index (0-100).

    Returns:
        Dict with index, label, description, contributing_factors, and metadata.
    """
    return _get_engine().get_market_volatility()


def get_trending_channels() -> List[Dict[str, Any]]:
    """Get channels with rising or notable performance changes.

    Returns:
        List of trending channel dicts with direction, change_pct, and signals.
    """
    return _get_engine().get_trending_channels()


def get_signal_history(hours: int = 24) -> List[Dict[str, Any]]:
    """Get recent signal computation history.

    Args:
        hours: Number of hours of history to return (default 24).

    Returns:
        List of computation snapshots with timestamps and counts.
    """
    return _get_engine().get_signal_history(hours=hours)


def get_signal_stats() -> Dict[str, Any]:
    """Get signal engine stats for /api/health integration.

    Returns:
        Dict with engine status, signal counts, cache info, and data file status.
    """
    return _get_engine().get_signal_stats()


# ---------------------------------------------------------------------------
# S48: CPC Trend Tracker + Job Posting Volume Tracker
# ---------------------------------------------------------------------------
# Live Adzuna/Jooble integration with benchmark fallbacks and 1-hour cache.

import hashlib
import ssl
import urllib.error
import urllib.parse
import urllib.request

_CPC_CACHE: Dict[str, Any] = {}
_VOLUME_CACHE: Dict[str, Any] = {}
_CPC_CACHE_LOCK = threading.Lock()
_VOLUME_CACHE_LOCK = threading.Lock()
_TRACKER_CACHE_TTL = 3600  # 1 hour

# Platform-level CPC benchmarks from recruitment_benchmarks_comprehensive_2026.json
_PLATFORM_CPC_BENCHMARKS: Dict[str, Dict[str, float]] = {
    "indeed": {"min": 0.25, "max": 1.50, "median": 0.92},
    "linkedin": {"min": 1.50, "max": 8.00, "median": 5.26},
    "ziprecruiter": {"min": 0.80, "max": 3.50, "median": 1.35},
    "glassdoor": {"min": 5.00, "max": 150.00, "median": 12.00},
    "facebook": {"min": 0.50, "max": 2.11, "median": 1.11},
    "google_ads": {"min": 2.00, "max": 5.00, "median": 3.20},
}

_INDUSTRY_CPC_BENCHMARKS: Dict[str, Dict[str, float]] = {
    "technology": {"indeed": 1.15, "linkedin": 6.50, "ziprecruiter": 1.80},
    "healthcare": {"indeed": 0.90, "linkedin": 4.00, "ziprecruiter": 1.40},
    "finance": {"indeed": 1.00, "linkedin": 5.50, "ziprecruiter": 1.60},
    "retail": {"indeed": 0.55, "linkedin": 3.50, "ziprecruiter": 1.00},
    "logistics": {"indeed": 0.70, "linkedin": 3.80, "ziprecruiter": 1.10},
    "hospitality": {"indeed": 0.45, "linkedin": 3.20, "ziprecruiter": 0.90},
    "manufacturing": {"indeed": 0.65, "linkedin": 3.60, "ziprecruiter": 1.05},
    "engineering": {"indeed": 1.10, "linkedin": 6.00, "ziprecruiter": 1.70},
    "marketing": {"indeed": 0.85, "linkedin": 5.00, "ziprecruiter": 1.30},
    "education": {"indeed": 0.60, "linkedin": 4.50, "ziprecruiter": 1.15},
}

# Posting volume baselines by industry (weekly averages for trend comparison)
_POSTING_VOLUME_BASELINES: Dict[str, int] = {
    "technology": 145000,
    "healthcare": 210000,
    "finance": 85000,
    "retail": 180000,
    "logistics": 95000,
    "hospitality": 120000,
    "manufacturing": 65000,
    "engineering": 42000,
    "marketing": 38000,
    "education": 72000,
    "construction": 55000,
    "default": 50000,
}


def _tracker_cache_key(prefix: str, params: str) -> str:
    """Generate a deterministic cache key for tracker functions."""
    raw = f"{prefix}:{params}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _get_tracker_cached(
    cache: Dict[str, Any], lock: threading.Lock, key: str
) -> Optional[Dict[str, Any]]:
    """Check in-memory tracker cache. Returns None on miss or expiry."""
    with lock:
        if key in cache:
            entry = cache[key]
            if time.time() - entry.get("ts", 0) < _TRACKER_CACHE_TTL:
                return entry.get("data")
            cache.pop(key, None)
    return None


def _set_tracker_cached(
    cache: Dict[str, Any], lock: threading.Lock, key: str, data: Dict[str, Any]
) -> None:
    """Store data in tracker cache with timestamp."""
    with lock:
        # Evict oldest if cache grows beyond 200 entries
        if len(cache) >= 200:
            sorted_keys = sorted(cache.keys(), key=lambda k: cache[k].get("ts", 0))
            for k in sorted_keys[:40]:
                cache.pop(k, None)
        cache[key] = {"ts": time.time(), "data": data}


def _classify_industry(role: str) -> str:
    """Map a role string to an industry key for benchmark lookup."""
    rl = role.lower()
    kw_map = {
        "software": "technology",
        "developer": "technology",
        "devops": "technology",
        "data": "technology",
        "cloud": "technology",
        "ai": "technology",
        "ml": "technology",
        "engineer": "engineering",
        "nurse": "healthcare",
        "doctor": "healthcare",
        "physician": "healthcare",
        "therapist": "healthcare",
        "medical": "healthcare",
        "accountant": "finance",
        "analyst": "finance",
        "financial": "finance",
        "banking": "finance",
        "warehouse": "logistics",
        "driver": "logistics",
        "supply chain": "logistics",
        "teacher": "education",
        "professor": "education",
        "instructor": "education",
        "sales": "retail",
        "retail": "retail",
        "cashier": "retail",
        "marketing": "marketing",
        "content": "marketing",
        "seo": "marketing",
        "chef": "hospitality",
        "hotel": "hospitality",
        "restaurant": "hospitality",
        "construction": "manufacturing",
        "mechanic": "manufacturing",
        "welder": "manufacturing",
    }
    for kw, ind in kw_map.items():
        if kw in rl:
            return ind
    return "default"


def _adzuna_live_cpc(role: str, country: str = "us") -> Optional[Dict[str, Any]]:
    """Fetch live CPC-related data from Adzuna search endpoint.

    Returns dict with posting count and mean salary, or None on failure.
    """
    app_id = os.environ.get("ADZUNA_APP_ID") or ""
    app_key = os.environ.get("ADZUNA_APP_KEY") or ""
    if not app_id or not app_key:
        return None

    params = urllib.parse.urlencode(
        {
            "app_id": app_id,
            "app_key": app_key,
            "what": role,
            "results_per_page": "1",
            "content-type": "application/json",
        }
    )
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1?{params}"

    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url, method="GET")
        req.add_header("User-Agent", "NovaAISuite/1.0")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            return {
                "count": data.get("count") or 0,
                "mean_salary": data.get("mean"),
            }
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        logger.warning("Adzuna CPC fetch failed for %s: %s", role, exc)
        return None
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        logger.warning("Adzuna CPC parse failed for %s: %s", role, exc)
        return None


def _jooble_live_count(role: str, location: str) -> Optional[int]:
    """Fetch job posting count from Jooble API.

    Returns totalCount or None on failure.
    """
    api_key = (os.environ.get("JOOBLE_API_KEY") or "").strip()
    if not api_key:
        return None

    url = f"https://jooble.org/api/{api_key}"
    payload = json.dumps(
        {
            "keywords": role,
            "location": location,
            "page": 1,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            return data.get("totalCount") or 0
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        logger.warning(
            "Jooble count fetch failed for %s in %s: %s", role, location, exc
        )
        return None
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        logger.warning(
            "Jooble count parse failed for %s in %s: %s", role, location, exc
        )
        return None


def _generate_cpc_alert(trends: Dict[str, str], current_cpc: Dict[str, float]) -> str:
    """Generate a smart alert string based on CPC trends."""
    alerts: List[str] = []
    for platform, trend_str in trends.items():
        try:
            pct = float(trend_str.replace("%", "").replace("+", ""))
        except (ValueError, AttributeError):
            continue
        if pct > 10:
            alerts.append(
                f"{platform.title()} CPC up {trend_str} -- consider shifting budget"
            )
        elif pct < -10:
            alerts.append(
                f"{platform.title()} CPC down {trend_str} -- opportunity to increase spend"
            )

    # Seasonal check
    month = datetime.datetime.now().month
    seasonal = _SEASONAL_PATTERNS.get(month, {})
    label = seasonal.get("label", "")
    mult = seasonal.get("demand_multiplier", 1.0)
    if mult > 1.05:
        alerts.append(f"Seasonal pattern: {label} (demand +{int((mult-1)*100)}%)")
    elif mult < 0.90:
        alerts.append(f"Seasonal pattern: {label} (demand {int((mult-1)*100)}%)")

    if not alerts:
        return "CPC trends within normal range across all platforms."
    return " | ".join(alerts)


def _generate_volume_signal(change_pct: float, role: str, locations: List[str]) -> str:
    """Generate a market signal string from posting volume change."""
    loc_str = ", ".join(locations[:3]) if locations else "all markets"
    if change_pct > 15:
        return (
            f"Hiring surge detected for {role} in {loc_str} "
            f"(+{change_pct:.1f}% week-over-week). "
            f"Expect CPC increase in 2-4 weeks."
        )
    elif change_pct > 5:
        return (
            f"Moderate hiring uptick for {role} in {loc_str} "
            f"(+{change_pct:.1f}% week-over-week)."
        )
    elif change_pct < -10:
        return (
            f"Market cooling detected for {role} in {loc_str} "
            f"({change_pct:.1f}% week-over-week). "
            f"Opportunity to reduce CPC bids."
        )
    elif change_pct < -5:
        return (
            f"Slight demand softening for {role} in {loc_str} "
            f"({change_pct:.1f}% week-over-week)."
        )
    return f"Stable posting volume for {role} in {loc_str}."


def get_cpc_trends(
    role: str,
    industry: str = "",
    locations: Optional[List[str]] = None,
    timeframe: str = "30d",
) -> Dict[str, Any]:
    """Track CPC bid price trends across platforms for a given role.

    Fetches live data from Adzuna when available, compares against stored
    benchmarks, and returns trend direction with smart alerts.

    Args:
        role: Job title to track (e.g. "Software Engineer").
        industry: Industry vertical for benchmark lookup.
        locations: Target locations (used for Adzuna country routing).
        timeframe: Trend window -- "7d", "30d", or "90d" (default "30d").

    Returns:
        Dict with current_cpc, benchmark_cpc, trend, alert, forecast_30d,
        and last_updated fields.
    """
    if not role:
        return {"error": "Role is required", "current_cpc": {}, "trend": {}}

    locations = locations or []
    cache_key = _tracker_cache_key(
        "cpc_trends",
        f"{role}|{industry}|{'|'.join(locations)}|{timeframe}",
    )
    cached = _get_tracker_cached(_CPC_CACHE, _CPC_CACHE_LOCK, cache_key)
    if cached is not None:
        return cached

    # Determine industry for benchmark lookup
    ind = (industry or "").lower().strip()
    if not ind or ind not in _INDUSTRY_CPC_BENCHMARKS:
        ind = _classify_industry(role)

    # Benchmark CPC by platform
    ind_benchmarks = _INDUSTRY_CPC_BENCHMARKS.get(ind, {})
    benchmark_cpc: Dict[str, float] = {
        "indeed": ind_benchmarks.get(
            "indeed", _PLATFORM_CPC_BENCHMARKS["indeed"]["median"]
        ),
        "linkedin": ind_benchmarks.get(
            "linkedin", _PLATFORM_CPC_BENCHMARKS["linkedin"]["median"]
        ),
        "ziprecruiter": ind_benchmarks.get(
            "ziprecruiter", _PLATFORM_CPC_BENCHMARKS["ziprecruiter"]["median"]
        ),
    }

    # Try live Adzuna data
    country = "us"
    if locations:
        loc0 = locations[0].lower()
        country_map = {
            "uk": "gb",
            "united kingdom": "gb",
            "canada": "ca",
            "australia": "au",
            "germany": "de",
            "france": "fr",
            "india": "in",
            "brazil": "br",
        }
        for k, v in country_map.items():
            if k in loc0:
                country = v
                break

    live_data = _adzuna_live_cpc(role, country)

    # Compute current CPC estimates
    # Adzuna gives posting count which correlates with competition / CPC pressure
    current_cpc: Dict[str, float] = {}
    if live_data and live_data.get("count", 0) > 0:
        posting_count = live_data["count"]
        # Competition-adjusted CPC: more postings = higher CPC pressure
        # Apply a multiplier based on competition relative to baseline
        baseline_count = _POSTING_VOLUME_BASELINES.get(ind, 50000)
        competition_ratio = min(posting_count / max(baseline_count, 1), 2.0)
        for platform, bench_val in benchmark_cpc.items():
            # Scale CPC by competition ratio (capped at 2x baseline)
            adjusted = round(bench_val * (0.85 + 0.3 * competition_ratio), 2)
            current_cpc[platform] = adjusted
    else:
        # Fallback: apply seasonal multiplier to benchmarks
        month = datetime.datetime.now().month
        seasonal_mult = _SEASONAL_PATTERNS.get(month, {}).get("demand_multiplier", 1.0)
        for platform, bench_val in benchmark_cpc.items():
            current_cpc[platform] = round(bench_val * seasonal_mult, 2)

    # Compute trends
    trends: Dict[str, str] = {}
    for platform in benchmark_cpc:
        curr = current_cpc.get(platform, 0)
        bench = benchmark_cpc.get(platform, 0)
        if bench > 0:
            pct_change = ((curr - bench) / bench) * 100
            sign = "+" if pct_change >= 0 else ""
            trends[platform] = f"{sign}{pct_change:.0f}%"
        else:
            trends[platform] = "N/A"

    # 30-day forecast: project current trend forward with dampening
    forecast_30d: Dict[str, float] = {}
    for platform in current_cpc:
        curr = current_cpc[platform]
        bench = benchmark_cpc.get(platform, curr)
        if bench > 0:
            trend_factor = (curr - bench) / bench
            # Dampen forecast: assume 60% of current trend continues
            projected = curr * (1 + trend_factor * 0.6)
            forecast_30d[platform] = round(projected, 2)
        else:
            forecast_30d[platform] = curr

    alert = _generate_cpc_alert(trends, current_cpc)

    result: Dict[str, Any] = {
        "role": role,
        "industry": ind,
        "current_cpc": current_cpc,
        "benchmark_cpc": {k: round(v, 2) for k, v in benchmark_cpc.items()},
        "trend": trends,
        "alert": alert,
        "forecast_30d": forecast_30d,
        "timeframe": timeframe,
        "data_source": (
            "Adzuna API + benchmarks"
            if live_data
            else "Curated benchmarks (Adzuna unavailable)"
        ),
        "last_updated": datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d"
        ),
    }

    _set_tracker_cached(_CPC_CACHE, _CPC_CACHE_LOCK, cache_key, result)
    return result


def get_posting_volume(
    role: str,
    locations: Optional[List[str]] = None,
    timeframe: str = "7d",
) -> Dict[str, Any]:
    """Track job posting volume and market activity for a given role.

    Uses Adzuna as primary source and Jooble as secondary to get current
    posting volumes, then compares against historical baselines.

    Args:
        role: Job title to track (e.g. "Software Engineer").
        locations: Target locations for volume tracking.
        timeframe: Comparison window -- "7d" or "30d" (default "7d").

    Returns:
        Dict with current_postings, previous_period, change, trend,
        market_signal, competitor_indicator, and by_location fields.
    """
    if not role:
        return {"error": "Role is required", "current_postings": 0}

    locations = locations or []
    cache_key = _tracker_cache_key(
        "posting_volume",
        f"{role}|{'|'.join(locations)}|{timeframe}",
    )
    cached = _get_tracker_cached(_VOLUME_CACHE, _VOLUME_CACHE_LOCK, cache_key)
    if cached is not None:
        return cached

    industry = _classify_industry(role)
    baseline = _POSTING_VOLUME_BASELINES.get(industry, 50000)

    # Apply seasonal adjustment to baseline for fair comparison
    month = datetime.datetime.now().month
    seasonal_mult = _SEASONAL_PATTERNS.get(month, {}).get("demand_multiplier", 1.0)
    adjusted_baseline = int(baseline * seasonal_mult)

    # Try live Adzuna count
    country = "us"
    if locations:
        loc0 = locations[0].lower()
        country_map = {
            "uk": "gb",
            "united kingdom": "gb",
            "canada": "ca",
            "australia": "au",
            "germany": "de",
            "france": "fr",
            "india": "in",
            "brazil": "br",
        }
        for k, v in country_map.items():
            if k in loc0:
                country = v
                break

    adzuna_data = _adzuna_live_cpc(role, country)
    adzuna_count = (adzuna_data or {}).get("count", 0)

    # Try Jooble as secondary source
    jooble_count = 0
    if locations:
        for loc in locations[:3]:
            jc = _jooble_live_count(role, loc)
            if jc is not None:
                jooble_count += jc
    elif not adzuna_count:
        # No locations and no Adzuna -- try Jooble with empty location
        jc = _jooble_live_count(role, "")
        if jc is not None:
            jooble_count = jc

    # Determine best current count
    if adzuna_count > 0 and jooble_count > 0:
        current_postings = max(adzuna_count, jooble_count)
        data_source = "Adzuna + Jooble APIs"
    elif adzuna_count > 0:
        current_postings = adzuna_count
        data_source = "Adzuna API"
    elif jooble_count > 0:
        current_postings = jooble_count
        data_source = "Jooble API"
    else:
        # Fallback to baseline with seasonal adjustment
        current_postings = adjusted_baseline
        data_source = "Curated benchmarks (APIs unavailable)"

    # Compute change vs previous period (baseline represents typical volume)
    previous_period = adjusted_baseline
    if previous_period > 0:
        change_pct = ((current_postings - previous_period) / previous_period) * 100
    else:
        change_pct = 0.0

    # Determine trend direction
    if change_pct > 15:
        trend = "surging"
    elif change_pct > 5:
        trend = "increasing"
    elif change_pct > -5:
        trend = "stable"
    elif change_pct > -15:
        trend = "declining"
    else:
        trend = "contracting"

    # Competitor indicator
    if current_postings > baseline * 1.2:
        competitor_indicator = "high_activity"
    elif current_postings > baseline * 0.8:
        competitor_indicator = "normal_activity"
    else:
        competitor_indicator = "low_activity"

    # Per-location breakdown (if Jooble data available)
    by_location: Dict[str, int] = {}
    if locations:
        for loc in locations[:5]:
            jc = _jooble_live_count(role, loc)
            if jc is not None and jc > 0:
                by_location[loc] = jc
            elif adzuna_count > 0:
                # Estimate proportional share
                by_location[loc] = max(1, adzuna_count // max(len(locations), 1))

    market_signal = _generate_volume_signal(change_pct, role, locations)

    sign = "+" if change_pct >= 0 else ""
    result: Dict[str, Any] = {
        "role": role,
        "current_postings": current_postings,
        "previous_period": previous_period,
        "change": f"{sign}{change_pct:.1f}%",
        "trend": trend,
        "market_signal": market_signal,
        "competitor_indicator": competitor_indicator,
        "by_location": by_location,
        "industry": industry,
        "seasonal_context": _SEASONAL_PATTERNS.get(month, {}).get("label", ""),
        "data_source": data_source,
        "timeframe": timeframe,
        "last_updated": datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d"
        ),
    }

    _set_tracker_cached(_VOLUME_CACHE, _VOLUME_CACHE_LOCK, cache_key, result)
    return result
