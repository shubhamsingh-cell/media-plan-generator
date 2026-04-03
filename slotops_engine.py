"""SlotOps Engine -- LinkedIn Slot Optimization for Nova AI Suite (Product #6).

ROI-weighted slot allocation, rotation scheduling, and performance prediction
backed by 88,954 job postings across 73 countries and 885 title families.
"""

from __future__ import annotations
import csv, io, json, logging, math, threading, time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_baselines: dict[str, Any] = {}
_optimize_count: int = 0
_predict_count: int = 0
_schedule_count: int = 0
_total_latency_ms: float = 0.0
_BASELINE_PATH = Path(__file__).resolve().parent / "data" / "slotops_baseline_data.json"


# -- Data Models -------------------------------------------------------------
@dataclass
class SlotConfig:
    """Configuration for the slot pool and rotation behaviour."""

    total_slots: int = 500
    rotation_cycles_per_day: int = 6
    peak_window_hours: int = 4
    countries: list[str] = field(default_factory=list)


@dataclass
class Job:
    """A single job posting to be optimized."""

    job_id: str = ""
    title: str = ""
    standardized_title: str = ""
    country: str = ""
    location: str = ""
    industry: str = ""
    company: str = ""
    priority: int = 3
    language: str = "English"


@dataclass
class Slot:
    """Represents one LinkedIn advertising slot."""

    slot_id: int = 0
    status: str = "idle"
    current_job: Optional[Job] = None
    country: str = ""
    window_start_utc: float = 0.0
    window_end_utc: float = 0.0


@dataclass
class RotationSchedule:
    """One rotation window for a country within the daily cycle."""

    country: str = ""
    window_start_utc: float = 0.0
    window_end_utc: float = 0.0
    slots_allocated: int = 0
    jobs_in_window: list[str] = field(default_factory=list)


# -- Baseline Loader ---------------------------------------------------------
def load_baselines(path: Path | None = None) -> dict[str, Any]:
    """Load slotops_baseline_data.json; thread-safe, cached after first call."""
    global _baselines
    if _baselines:
        return _baselines
    resolved = path or _BASELINE_PATH
    with _lock:
        if _baselines:
            return _baselines
        try:
            with open(resolved, "r", encoding="utf-8") as fh:
                _baselines = json.load(fh)
            logger.info(f"SlotOps baselines loaded from {resolved}")
        except FileNotFoundError:
            logger.error(f"Baseline file not found: {resolved}", exc_info=True)
            _baselines = {}
        except json.JSONDecodeError as exc:
            logger.error(f"Invalid JSON in baseline file: {exc}", exc_info=True)
            _baselines = {}
    return _baselines


# -- Internal Helpers --------------------------------------------------------
def _safe_avg(data: dict | None, metric: str) -> float:
    """Extract avg from a nested baseline metric dict."""
    if not data:
        return 0.0
    bucket = data.get(metric)
    return float(bucket.get("avg") or 0) if isinstance(bucket, dict) else 0.0


def _country_bl(country: str) -> dict | None:
    return (load_baselines().get("country_baselines") or {}).get(country)


def _title_bl(title: str) -> dict | None:
    return (load_baselines().get("title_baselines") or {}).get(title)


def _cross_bl(country: str, title: str) -> dict | None:
    return (load_baselines().get("country_title_cross") or {}).get(f"{country}|{title}")


def _norm01(value: float, mx: float) -> float:
    return min(max(value / mx, 0.0), 1.0) if mx > 0 else 0.0


def _max_rate(section: str) -> float:
    """Highest avg apply rate across all entries in a baseline section."""
    best = 1.0
    for entry in (load_baselines().get(section) or {}).values():
        r = _safe_avg(entry, "apply_rate")
        if r > best:
            best = r
    return best


def _best_dow(country: str) -> tuple[str, float]:
    """Return (best_day_name, best_apply_rate) from dow_patterns."""
    dow = (load_baselines().get("dow_patterns") or {}).get(country) or {}
    best, rate = "", 0.0
    for d, v in dow.items():
        r = v.get("avg_apply_rate") or 0
        if r > rate:
            best, rate = d, r
    return best, rate


def _jobs_from_dicts(raw: list[dict]) -> list[Job]:
    """Convert plain dicts to Job instances."""
    return [
        Job(
            job_id=str(r.get("job_id") or ""),
            title=str(r.get("title") or ""),
            standardized_title=str(r.get("standardized_title") or r.get("title") or ""),
            country=str(r.get("country") or ""),
            location=str(r.get("location") or ""),
            industry=str(r.get("industry") or ""),
            company=str(r.get("company") or ""),
            priority=int(r.get("priority") or 3),
            language=str(r.get("language") or "English"),
        )
        for r in raw
    ]


def _config_from_dict(raw: dict) -> SlotConfig:
    """Build SlotConfig from plain dict."""
    return SlotConfig(
        total_slots=int(raw.get("total_slots") or 500),
        rotation_cycles_per_day=int(raw.get("rotation_cycles_per_day") or 6),
        peak_window_hours=int(raw.get("peak_window_hours") or 4),
        countries=list(raw.get("countries") or []),
    )


def _auto_countries(jobs: list[Job], config: SlotConfig) -> list[str]:
    """Return config.countries or derive from job list."""
    return config.countries or list({j.country for j in jobs if j.country})


def _avg_or_fallback(a: float, b: float) -> float:
    """Average two values; if one is zero, use the other."""
    return (a + b) / 2 if (a and b) else (a or b)


# -- Core Functions ----------------------------------------------------------
def calculate_slot_allocation(
    countries: list[str], total_slots: int, jobs: list[Job]
) -> dict[str, int]:
    """ROI-weighted slot allocation: slots = total * (jobs_in_country * apply_rate) / total_weight."""
    if not countries or total_slots <= 0:
        return {}
    jbc: dict[str, int] = {}
    for j in jobs:
        if j.country in countries:
            jbc[j.country] = jbc.get(j.country, 0) + 1
    roi = {
        c: jbc.get(c, 0) * (_safe_avg(_country_bl(c), "apply_rate") or 1.0)
        for c in countries
    }
    tw = sum(roi.values())
    if tw <= 0:
        per = total_slots // len(countries)
        return {c: per for c in countries}
    raw = {c: total_slots * (roi[c] / tw) for c in countries}
    alloc = {c: int(math.floor(v)) for c, v in raw.items()}
    rem = total_slots - sum(alloc.values())
    by_frac = sorted(countries, key=lambda c: raw[c] - math.floor(raw[c]), reverse=True)
    for i in range(rem):
        alloc[by_frac[i % len(by_frac)]] += 1
    return alloc


def score_job(job: Job) -> float:
    """Score 0-100: 40% country rate, 25% title rate, 20% priority, 15% cross rate."""
    mc = _max_rate("country_baselines")
    mt = _max_rate("title_baselines")
    cs = _norm01(_safe_avg(_country_bl(job.country), "apply_rate"), mc)
    ts = _norm01(_safe_avg(_title_bl(job.standardized_title), "apply_rate"), mt)
    ps = min(max(job.priority, 1), 5) / 5.0
    cross = _cross_bl(job.country, job.standardized_title)
    xs = _norm01(_safe_avg(cross, "apply_rate"), max(mc, mt, 1.0)) if cross else 0.0
    return round((0.40 * cs + 0.25 * ts + 0.20 * ps + 0.15 * xs) * 100, 2)


def build_rotation_schedule(
    jobs: list[Job], config: SlotConfig
) -> list[RotationSchedule]:
    """Build 24h rotation schedule ordered by UTC peak windows."""
    tz_peaks = load_baselines().get("timezone_peaks") or {}
    countries = _auto_countries(jobs, config)
    alloc = calculate_slot_allocation(countries, config.total_slots, jobs)
    peaks: list[tuple[str, float, float]] = []
    for c in countries:
        utc_hrs = (tz_peaks.get(c) or {}).get("utc_peak_hours") or []
        s = min(utc_hrs) if utc_hrs else 8.0
        peaks.append((c, s, s + config.peak_window_hours))
    peaks.sort(key=lambda x: x[1])
    scored: dict[str, list[tuple[float, Job]]] = {}
    for j in jobs:
        scored.setdefault(j.country, []).append((score_job(j), j))
    for v in scored.values():
        v.sort(key=lambda x: x[0], reverse=True)
    schedule: list[RotationSchedule] = []
    for c, s, e in peaks:
        n = alloc.get(c, 0)
        if n <= 0:
            continue
        ids = [j.job_id for _, j in scored.get(c, [])[:n]]
        schedule.append(RotationSchedule(c, round(s, 2), round(e % 24, 2), n, ids))
    return schedule


def predict_performance(job: Job) -> dict[str, Any]:
    """Predict apply rate, views, applications, best day/month, confidence."""
    bl = load_baselines()
    cross = _cross_bl(job.country, job.standardized_title)
    cb, tb = _country_bl(job.country), _title_bl(job.standardized_title)
    if cross:
        ar = _safe_avg(cross, "apply_rate")
        vw = _safe_avg(cross, "views")
        ap = _safe_avg(cross, "applications")
        ss = cross.get("sample_size") or 0
    else:
        ar = _avg_or_fallback(_safe_avg(cb, "apply_rate"), _safe_avg(tb, "apply_rate"))
        vw = _avg_or_fallback(_safe_avg(cb, "views"), _safe_avg(tb, "views"))
        ap = _avg_or_fallback(
            _safe_avg(cb, "applications"), _safe_avg(tb, "applications")
        )
        ss = min((cb or {}).get("sample_size", 0), (tb or {}).get("sample_size", 0))
    best_day, _ = _best_dow(job.country)
    season = (bl.get("seasonality") or {}).get(job.country) or {}
    best_mo, best_mr = "", 0.0
    for mk, mv in season.items():
        r = mv.get("avg_apply_rate") or 0
        if r > best_mr:
            best_mr, best_mo = r, mk
    conf = (
        0.95
        if ss >= 100
        else 0.85 if ss >= 50 else 0.70 if ss >= 20 else 0.50 if ss >= 5 else 0.30
    )
    return {
        "job_id": job.job_id,
        "country": job.country,
        "standardized_title": job.standardized_title,
        "expected_apply_rate": round(ar, 2),
        "expected_views": round(vw, 1),
        "expected_applications": round(ap, 1),
        "best_day_of_week": best_day,
        "best_month": best_mo,
        "confidence_score": conf,
        "sample_size": ss,
        "data_source": "cross" if cross else "averaged",
    }


def get_optimal_posting_time(country: str) -> dict[str, Any]:
    """Return peak hours (morning + evening), best day, timezone info."""
    tz = (load_baselines().get("timezone_peaks") or {}).get(country) or {}
    lp = tz.get("local_peak_hours") or []
    best_day, best_rate = _best_dow(country)
    return {
        "country": country,
        "timezone": tz.get("timezone") or "Unknown",
        "timezone_label": tz.get("timezone_label") or "",
        "utc_offset": tz.get("utc_offset") or 0,
        "morning_peak_hours_local": [h for h in lp if h < 12],
        "evening_peak_hours_local": [h for h in lp if h >= 12],
        "utc_morning_window": tz.get("utc_morning_window") or "",
        "utc_evening_window": tz.get("utc_evening_window") or "",
        "best_day_of_week": best_day,
        "best_day_apply_rate": round(best_rate, 2),
    }


def generate_schedule_csv(schedule: list[RotationSchedule]) -> str:
    """Generate CSV string from rotation schedule for export."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "Country",
            "Window Start (UTC)",
            "Window End (UTC)",
            "Slots Allocated",
            "Jobs Count",
            "Job IDs",
        ]
    )
    for e in schedule:
        w.writerow(
            [
                e.country,
                f"{e.window_start_utc:.2f}",
                f"{e.window_end_utc:.2f}",
                e.slots_allocated,
                len(e.jobs_in_window),
                ";".join(e.jobs_in_window[:50]),
            ]
        )
    return buf.getvalue()


def get_dashboard_data(jobs: list[Job], config: SlotConfig) -> dict[str, Any]:
    """Return complete dashboard: slots, schedule, metrics, predictions."""
    t0 = time.monotonic()
    schedule = build_rotation_schedule(jobs, config)
    countries = _auto_countries(jobs, config)
    alloc = calculate_slot_allocation(countries, config.total_slots, jobs)
    top = sorted(jobs, key=score_job, reverse=True)[:20]
    preds = [predict_performance(j) for j in top]
    active = sum(s.slots_allocated for s in schedule)
    queued = len(jobs) - sum(len(s.jobs_in_window) for s in schedule)
    slots: list[dict] = []
    sid = 1
    for e in schedule:
        for jid in e.jobs_in_window:
            if sid > config.total_slots:
                break
            slots.append(
                {
                    "slot_id": sid,
                    "status": "active",
                    "job_id": jid,
                    "country": e.country,
                    "window_start_utc": e.window_start_utc,
                    "window_end_utc": e.window_end_utc,
                }
            )
            sid += 1
        if sid > config.total_slots:
            break
    ms = round((time.monotonic() - t0) * 1000, 1)
    return {
        "summary": {
            "total_slots": config.total_slots,
            "active_slots": min(active, config.total_slots),
            "queued_jobs": max(queued, 0),
            "countries_active": len(alloc),
            "rotation_cycles_per_day": config.rotation_cycles_per_day,
            "computation_ms": ms,
        },
        "allocation": alloc,
        "schedule": [asdict(s) for s in schedule],
        "top_predictions": preds,
        "slots": slots[:100],
    }


# -- API Handlers ------------------------------------------------------------
def _parse_jobs_config(body: dict) -> tuple[list[Job], SlotConfig]:
    """Shared parser for optimize/schedule/export handlers."""
    raw_jobs = body.get("jobs") or []
    if not raw_jobs:
        raise ValueError("No jobs provided")
    jobs = _jobs_from_dicts(raw_jobs)
    config = _config_from_dict(body.get("config") or {})
    if not config.countries:
        config.countries = _auto_countries(jobs, config)
    return jobs, config


def _timed(counter_name: str, t0: float) -> float:
    """Record latency and bump counter; returns elapsed ms."""
    global _optimize_count, _predict_count, _schedule_count, _total_latency_ms
    ms = round((time.monotonic() - t0) * 1000, 1)
    with _lock:
        if counter_name == "optimize":
            _optimize_count += 1
        elif counter_name == "predict":
            _predict_count += 1
        elif counter_name == "schedule":
            _schedule_count += 1
        _total_latency_ms += ms
    return ms


def handle_slotops_optimize(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/optimize -- Run full optimization."""
    t0 = time.monotonic()
    try:
        jobs, config = _parse_jobs_config(body)
        alloc = calculate_slot_allocation(config.countries, config.total_slots, jobs)
        sched = build_rotation_schedule(jobs, config)
        scored = sorted(
            ((score_job(j), j) for j in jobs), key=lambda x: x[0], reverse=True
        )
        top = [
            {"job_id": j.job_id, "title": j.title, "country": j.country, "score": s}
            for s, j in scored[:30]
        ]
        ms = _timed("optimize", t0)
        return {
            "ok": True,
            "allocation": alloc,
            "schedule": [asdict(s) for s in sched],
            "top_jobs": top,
            "total_jobs": len(jobs),
            "countries": config.countries,
            "total_slots": config.total_slots,
            "computation_ms": ms,
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except (TypeError, KeyError) as exc:
        logger.error(f"SlotOps optimize error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


def handle_slotops_predict(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/predict -- Predict job performance (single or batch)."""
    t0 = time.monotonic()
    try:
        single, batch = body.get("job"), body.get("jobs")
        if single:
            jobs = _jobs_from_dicts([single])
        elif batch:
            jobs = _jobs_from_dicts(batch)
        else:
            return {"ok": False, "error": "Provide 'job' or 'jobs' in request body"}
        preds = [predict_performance(j) for j in jobs]
        ms = _timed("predict", t0)
        return {
            "ok": True,
            "predictions": preds,
            "count": len(preds),
            "computation_ms": ms,
        }
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"SlotOps predict error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


def handle_slotops_schedule(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/schedule -- Get rotation schedule with posting times."""
    t0 = time.monotonic()
    try:
        jobs, config = _parse_jobs_config(body)
        sched = build_rotation_schedule(jobs, config)
        ptimes = {c: get_optimal_posting_time(c) for c in config.countries}
        ms = _timed("schedule", t0)
        return {
            "ok": True,
            "schedule": [asdict(s) for s in sched],
            "optimal_posting_times": ptimes,
            "total_windows": len(sched),
            "computation_ms": ms,
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except (TypeError, KeyError) as exc:
        logger.error(f"SlotOps schedule error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


def handle_slotops_export(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/export -- Export schedule as CSV."""
    try:
        jobs, config = _parse_jobs_config(body)
        sched = build_rotation_schedule(jobs, config)
        return {
            "ok": True,
            "csv": generate_schedule_csv(sched),
            "rows": len(sched),
            "format": "text/csv",
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except (TypeError, KeyError) as exc:
        logger.error(f"SlotOps export error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


def handle_slotops_dashboard(query_params: dict[str, Any]) -> dict[str, Any]:
    """GET /api/slotops/dashboard -- Baseline-derived dashboard data."""
    try:
        bl = load_baselines()
        cp = str(query_params.get("countries") or "")
        ts = int(query_params.get("total_slots") or 500)
        ad = bl.get("slot_rotation") or {}
        ar = ad.get("allocations") or []
        requested = (
            [c.strip() for c in cp.split(",") if c.strip()]
            if cp
            else [a["country"] for a in ar[:20]]
        )
        tz_peaks = bl.get("timezone_peaks") or {}
        alloc: dict[str, int] = {}
        entries: list[dict] = []
        for e in ar:
            c = e.get("country") or ""
            if c not in requested:
                continue
            scaled = int(
                (e.get("slot_allocation_roi") or 0)
                * (ts / max(ad.get("total_slots", 500), 1))
            )
            alloc[c] = scaled
            uh = (tz_peaks.get(c) or {}).get("utc_peak_hours") or []
            s = min(uh) if uh else 8.0
            entries.append(
                {
                    "country": c,
                    "window_start_utc": round(s, 2),
                    "window_end_utc": round((s + 4) % 24, 2),
                    "slots_allocated": scaled,
                    "avg_apply_rate": e.get("avg_apply_rate") or 0,
                    "avg_views": e.get("avg_views") or 0,
                    "job_count": e.get("job_count") or 0,
                }
            )
        entries.sort(key=lambda x: x["window_start_utc"])
        return {
            "ok": True,
            "summary": {
                "total_slots": ts,
                "countries_active": len(alloc),
                "total_jobs_in_baselines": ad.get("total_jobs") or 0,
                "data_source": "baselines",
            },
            "allocation": alloc,
            "schedule": entries,
        }
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"SlotOps dashboard error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


_SECTION_MAP = {
    "countries": "country_baselines",
    "titles": "title_baselines",
    "cross": "country_title_cross",
    "dow": "dow_patterns",
    "seasonality": "seasonality",
    "duration": "posting_duration",
    "industries": "industry_performance",
    "timezones": "timezone_peaks",
    "companies": "company_performance",
}


def handle_slotops_baselines(query_params: dict[str, Any]) -> dict[str, Any]:
    """GET /api/slotops/baselines -- Lookup baseline performance data."""
    try:
        bl = load_baselines()
        country = str(query_params.get("country") or "")
        title = str(query_params.get("title") or "")
        section = str(query_params.get("section") or "")
        if country and title:
            return {
                "ok": True,
                "country": country,
                "title": title,
                "cross_data": _cross_bl(country, title),
                "country_baseline": _country_bl(country),
                "title_baseline": _title_bl(title),
                "optimal_posting_time": get_optimal_posting_time(country),
            }
        if country:
            return {
                "ok": True,
                "country": country,
                "baseline": _country_bl(country),
                "dow_patterns": (bl.get("dow_patterns") or {}).get(country),
                "seasonality": (bl.get("seasonality") or {}).get(country),
                "industry_performance": (bl.get("industry_performance") or {}).get(
                    country
                ),
                "optimal_posting_time": get_optimal_posting_time(country),
            }
        if title:
            return {"ok": True, "title": title, "baseline": _title_bl(title)}
        if section and section in _SECTION_MAP:
            data = bl.get(_SECTION_MAP[section]) or {}
            if isinstance(data, dict) and len(data) > 50:
                return {
                    "ok": True,
                    "section": section,
                    "total_entries": len(data),
                    "sample_keys": list(data.keys())[:50],
                    "note": "Use ?country= or ?title= for specific lookups",
                }
            return {"ok": True, "section": section, "data": data}
        meta = bl.get("metadata") or {}
        return {
            "ok": True,
            "metadata": meta,
            "available_sections": list(_SECTION_MAP.keys()),
            "country_count": len(bl.get("country_baselines") or {}),
            "title_count": len(bl.get("title_baselines") or {}),
            "cross_count": len(bl.get("country_title_cross") or {}),
        }
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"SlotOps baselines error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# LLM-Powered Insights (Gemini 3.1 Flash Lite -- free tier)
# ---------------------------------------------------------------------------

_llm_available = False


def _lazy_llm():
    """Lazy-load the LLM router for generating natural language insights."""
    global _llm_available
    try:
        from llm_router import call_llm, GEMINI_FLASH_LITE

        _llm_available = True
        return call_llm, GEMINI_FLASH_LITE
    except ImportError:
        logger.warning("llm_router not available; SlotOps LLM insights disabled")
        return None, None


def generate_optimization_report(
    schedule: list[dict], allocation: dict[str, int], top_jobs: list[dict]
) -> str:
    """Generate a natural language optimization report using Gemini 3.1 Flash Lite.

    Args:
        schedule: Rotation schedule entries.
        allocation: Country -> slot count mapping.
        top_jobs: Top scored jobs with their scores.

    Returns:
        Markdown-formatted report string, or empty string if LLM unavailable.
    """
    call_fn, provider = _lazy_llm()
    if not call_fn:
        return ""
    try:
        countries_summary = ", ".join(
            f"{k}: {v} slots"
            for k, v in sorted(allocation.items(), key=lambda x: -x[1])[:10]
        )
        top_jobs_summary = "\n".join(
            f"- {j.get('title', 'N/A')} in {j.get('country', 'N/A')}: score {j.get('score', 0):.1f}"
            for j in top_jobs[:10]
        )
        prompt = f"""Analyze this LinkedIn job slot optimization and write a concise executive summary (150 words max):

Slot Allocation: {countries_summary}
Total Slots: {sum(allocation.values())}
Rotation Windows: {len(schedule)} country-timezone windows per day

Top Scored Jobs:
{top_jobs_summary}

Focus on: why this allocation maximizes apply rates, which countries are overweighted and why, and the key scheduling insight (timezone rotation benefit). Be specific with numbers."""

        result = call_fn(
            messages=[{"role": "user", "content": prompt}],
            system_prompt="You are a recruitment advertising optimization analyst. Be concise, data-driven, no fluff.",
            max_tokens=300,
            task_type="summarization",
            force_provider=provider or "",
            use_cache=True,
        )
        return result.get("text") or ""
    except Exception as exc:
        logger.error(f"SlotOps LLM report generation failed: {exc}", exc_info=True)
        return ""


def handle_slotops_insights(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/insights -- Generate LLM-powered optimization report.

    Uses Gemini 3.1 Flash Lite (free) to produce natural language analysis
    of slot allocation and rotation schedule.
    """
    t0 = time.monotonic()
    try:
        jobs_raw = body.get("jobs") or []
        config_raw = body.get("config") or {}
        jobs = _jobs_from_dicts(jobs_raw) if jobs_raw else []
        config = _config_from_dict(config_raw)
        load_baselines()

        allocation = calculate_slot_allocation(
            config.countries, config.total_slots, jobs
        )
        schedule_objs = build_rotation_schedule(jobs, config)
        schedule = [
            {
                "country": s.country,
                "window_start_utc": s.window_start_utc,
                "window_end_utc": s.window_end_utc,
                "slots_allocated": s.slots_allocated,
            }
            for s in schedule_objs
        ]
        scored = sorted(
            [
                {"title": j.title, "country": j.country, "score": score_job(j)}
                for j in jobs
            ],
            key=lambda x: -x["score"],
        )

        report = generate_optimization_report(schedule, allocation, scored)
        ms = round((time.monotonic() - t0) * 1000, 1)

        if not report:
            return {
                "ok": False,
                "error": "LLM insights not available (Gemini API key may be missing)",
                "computation_ms": ms,
            }
        return {
            "ok": True,
            "report": report,
            "computation_ms": ms,
            "model": "gemini-3.1-flash-lite-preview",
        }
    except Exception as exc:
        logger.error(f"SlotOps insights error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}
