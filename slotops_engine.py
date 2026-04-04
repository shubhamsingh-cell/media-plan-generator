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
_INDUSTRY_BENCH_PATH = (
    Path(__file__).resolve().parent / "data" / "linkedin_industry_benchmarks.json"
)
_industry_benchmarks: dict[str, Any] = {}

# -- Constants ---------------------------------------------------------------
EASY_APPLY_LIFT: float = 2.3
BEST_POSTING_DAYS: list[str] = ["Tuesday", "Wednesday", "Thursday"]
REFRESH_CADENCE_DAYS: list[int] = [8, 9, 10]

_FUNCTION_DEMAND: dict[str, float] = {
    "engineering": 0.95,
    "software": 0.95,
    "data science": 0.90,
    "product": 0.85,
    "design": 0.80,
    "marketing": 0.75,
    "sales": 0.70,
    "finance": 0.70,
    "operations": 0.65,
    "hr": 0.60,
    "human resources": 0.60,
    "admin": 0.40,
    "customer service": 0.50,
    "legal": 0.55,
    "healthcare": 0.85,
    "it": 0.80,
    "consulting": 0.75,
    "research": 0.80,
}

_WORKPLACE_SCORES: dict[str, float] = {
    "remote": 10.0,
    "hybrid": 7.0,
    "on-site": 5.0,
    "onsite": 5.0,
}

_DOW_SCORES: dict[str, float] = {
    "monday": 6.0,
    "tuesday": 10.0,
    "wednesday": 10.0,
    "thursday": 10.0,
    "friday": 3.0,
    "saturday": 8.0,
    "sunday": 5.0,
}

COUNTRY_CODE_MAP: dict[str, str] = {
    "GB": "United Kingdom",
    "US": "United States",
    "CA": "Canada",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "IN": "India",
    "SG": "Singapore",
    "NZ": "New Zealand",
    "IE": "Ireland",
    "BR": "Brazil",
    "MX": "Mexico",
    "NL": "Netherlands",
    "JP": "Japan",
    "CN": "China",
    "IL": "Israel",
    "AE": "United Arab Emirates",
    "SA": "Saudi Arabia",
    "TW": "Taiwan",
    "ID": "Indonesia",
    "TH": "Thailand",
    "MY": "Malaysia",
    "KR": "South Korea",
    "IT": "Italy",
    "ES": "Spain",
    "PL": "Poland",
    "CH": "Switzerland",
    "AT": "Austria",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "BE": "Belgium",
    "PT": "Portugal",
    "GR": "Greece",
    "CZ": "Czechia",
    "RO": "Romania",
    "HU": "Hungary",
    "CO": "Colombia",
    "AR": "Argentina",
    "CL": "Chile",
    "PE": "Peru",
}

_NICHE_KEYWORDS: list[str] = [
    "driver",
    "cleaner",
    "housekeeper",
    "lifeguard",
    "demi chef",
    "commis chef",
    "food and beverage",
]

_HIGH_PERFORMER_KEYWORDS: list[str] = [
    "software engineer",
    "data analyst",
    "business analyst",
    "content specialist",
    "medical doctor",
    "consultant",
    "writer",
    "project manager",
    "product manager",
]


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
    application_method: str = "ATS"  # "LinkedIn" or "ATS"
    workplace_type: str = "Remote"  # "Remote", "Hybrid", "On-site"
    function: str = ""  # Job function category


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


def load_industry_benchmarks() -> dict[str, Any]:
    """Load external LinkedIn industry benchmarks; thread-safe, cached."""
    global _industry_benchmarks
    if _industry_benchmarks:
        return _industry_benchmarks
    with _lock:
        if _industry_benchmarks:
            return _industry_benchmarks
        try:
            with open(_INDUSTRY_BENCH_PATH, "r", encoding="utf-8") as fh:
                _industry_benchmarks = json.load(fh)
            logger.info("LinkedIn industry benchmarks loaded")
        except FileNotFoundError:
            logger.warning("Industry benchmarks file not found; using defaults")
            _industry_benchmarks = {}
        except json.JSONDecodeError as exc:
            logger.error(f"Invalid JSON in industry benchmarks: {exc}", exc_info=True)
            _industry_benchmarks = {}
    return _industry_benchmarks


def get_industry_context(
    industry: str, function: str, country: str, workplace: str
) -> dict[str, Any]:
    """Get relevant external benchmarks for a job based on its attributes.

    Args:
        industry: Job industry (e.g., 'Technology, Information and Media').
        function: Job function (e.g., 'Engineering').
        country: Job country (e.g., 'United States').
        workplace: Workplace type (e.g., 'Remote').

    Returns:
        Dict with industry, function, region, workplace, and seniority benchmarks.
    """
    ib = load_industry_benchmarks()
    if not ib:
        return {}

    # Map industry to key
    ind_lower = (industry or "").lower()
    ind_map = {
        "technology": "technology",
        "tech": "technology",
        "information": "technology",
        "software": "technology",
        "it": "technology",
        "healthcare": "healthcare",
        "health": "healthcare",
        "medical": "healthcare",
        "pharma": "healthcare",
        "finance": "finance_banking",
        "banking": "finance_banking",
        "insurance": "finance_banking",
        "financial": "finance_banking",
        "staffing": "staffing_recruiting",
        "recruiting": "staffing_recruiting",
        "manufacturing": "manufacturing",
        "industrial": "manufacturing",
        "retail": "retail_consumer",
        "consumer": "retail_consumer",
        "ecommerce": "retail_consumer",
        "professional": "professional_services",
        "consulting": "professional_services",
        "education": "education",
        "academic": "education",
        "government": "government_nonprofit",
        "nonprofit": "government_nonprofit",
        "logistics": "logistics_transportation",
        "transport": "logistics_transportation",
    }
    ind_key = None
    for keyword, key in ind_map.items():
        if keyword in ind_lower:
            ind_key = key
            break

    # Map function to key
    fn_lower = (function or "").lower()
    fn_map = {
        "engineering": "engineering_software",
        "software": "engineering_software",
        "sales": "sales",
        "marketing": "marketing",
        "operations": "operations",
        "human resources": "human_resources",
        "hr": "human_resources",
        "finance": "finance_accounting",
        "accounting": "finance_accounting",
        "data": "data_science_analytics",
        "analytics": "data_science_analytics",
        "customer": "customer_service",
        "design": "design_creative",
    }
    fn_key = None
    for keyword, key in fn_map.items():
        if keyword in fn_lower:
            fn_key = key
            break

    # Map country to region
    region_map = {
        "United States": "north_america",
        "Canada": "north_america",
        "Mexico": "north_america",
        "United Kingdom": "europe",
        "Germany": "europe",
        "France": "europe",
        "Netherlands": "europe",
        "Ireland": "europe",
        "Spain": "europe",
        "Italy": "europe",
        "Sweden": "europe",
        "Poland": "europe",
        "Belgium": "europe",
        "Switzerland": "europe",
        "Austria": "europe",
        "Denmark": "europe",
        "Norway": "europe",
        "Finland": "europe",
        "Portugal": "europe",
        "Czech Republic": "europe",
        "Romania": "europe",
        "India": "asia_pacific",
        "Australia": "asia_pacific",
        "Singapore": "asia_pacific",
        "Japan": "asia_pacific",
        "South Korea": "asia_pacific",
        "New Zealand": "asia_pacific",
        "Philippines": "asia_pacific",
        "Malaysia": "asia_pacific",
        "Indonesia": "asia_pacific",
        "Thailand": "asia_pacific",
        "Vietnam": "asia_pacific",
        "China": "asia_pacific",
        "Hong Kong": "asia_pacific",
        "Taiwan": "asia_pacific",
        "Brazil": "latin_america",
        "Argentina": "latin_america",
        "Colombia": "latin_america",
        "Chile": "latin_america",
        "Peru": "latin_america",
        "Saudi Arabia": "middle_east_africa",
        "UAE": "middle_east_africa",
        "United Arab Emirates": "middle_east_africa",
        "South Africa": "middle_east_africa",
        "Egypt": "middle_east_africa",
        "Nigeria": "middle_east_africa",
        "Qatar": "middle_east_africa",
        "Kuwait": "middle_east_africa",
    }
    region_key = region_map.get(country)

    wp_key = (workplace or "remote").lower().replace("-", "").replace(" ", "")
    if wp_key == "onsite":
        wp_key = "onsite"

    by_ind = (ib.get("by_industry") or {}).get(ind_key or "") or {}
    by_fn = (ib.get("by_job_function") or {}).get(fn_key or "") or {}
    by_region = (ib.get("by_region") or {}).get(region_key or "") or {}
    by_wp = (ib.get("by_workplace_type") or {}).get(wp_key or "") or {}
    platform = ib.get("linkedin_platform_benchmarks") or {}
    tips = ib.get("posting_optimization_tips") or {}
    seasonal = ib.get("seasonal_patterns") or {}
    algo = ib.get("linkedin_algorithm_insights") or {}
    costs = ib.get("cost_benchmarks_2025") or {}

    return {
        "industry": by_ind,
        "industry_key": ind_key,
        "function": by_fn,
        "function_key": fn_key,
        "region": by_region,
        "region_key": region_key,
        "workplace": by_wp,
        "workplace_key": wp_key,
        "platform": platform,
        "tips": tips,
        "seasonal": seasonal,
        "algorithm": algo,
        "costs": costs,
    }


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


_US_STATE_CODES = frozenset(
    [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "DC",
    ]
)
_CA_PROVINCE_CODES = frozenset(
    ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"]
)


def _resolve_country(raw_country: str, state: str = "", cl_market: str = "") -> str:
    """Map 2-letter ISO codes, US state codes, and CL Market to full country names."""
    code = (raw_country or "").strip().upper()
    if code in COUNTRY_CODE_MAP:
        return COUNTRY_CODE_MAP[code]
    if raw_country and raw_country.strip():
        return raw_country.strip()
    # Infer from state code
    state_code = (state or "").strip().upper()
    if state_code in _US_STATE_CODES:
        return "United States"
    if state_code in _CA_PROVINCE_CODES:
        return "Canada"
    # Infer from CL Market (e.g., "east tx, tx, us")
    market = (cl_market or "").strip().lower()
    if market.endswith(", us") or ", us," in market:
        return "United States"
    if market.endswith(", ca") or ", ca," in market:
        return "Canada"
    if market.endswith(", uk") or ", uk," in market:
        return "United Kingdom"
    return ""


def _jobs_from_dicts(raw: list[dict]) -> list[Job]:
    """Convert plain dicts to Job instances. Handles real user Excel formats:
    country may be 2-letter code, city used for location, company from tab name.
    """
    jobs: list[Job] = []
    for r in raw:
        country_raw = str(r.get("country") or "")
        state_raw = str(r.get("state") or "")
        cl_market = str(r.get("CL Market") or r.get("cl_market") or "")
        country = _resolve_country(country_raw, state_raw, cl_market)
        location = str(r.get("city") or r.get("location") or "")
        company = str(r.get("company") or r.get("client") or r.get("_sheet_name") or "")
        title = str(r.get("title") or "")
        job_id = str(
            r.get("job_id")
            or r.get("referencenumber")
            or r.get("reference_number")
            or ""
        )
        description = str(r.get("description") or "")
        workplace = str(r.get("workplace_type") or r.get("type") or "Remote")
        status_on_li = str(
            r.get("Status On LinkedIn") or r.get("status_on_linkedin") or ""
        )
        jobs.append(
            Job(
                job_id=job_id,
                title=title,
                standardized_title=str(r.get("standardized_title") or title),
                country=country,
                location=location,
                industry=str(r.get("industry") or ""),
                company=company,
                priority=int(r.get("priority") or 3),
                language=str(r.get("language") or "English"),
                application_method=str(r.get("application_method") or "ATS"),
                workplace_type=workplace,
                function=str(r.get("function") or ""),
            )
        )
    return jobs


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
    """8-factor scoring (max 100): country(15) function(15) tier(10) easyApply(20) workplace(10) title(10) history(10) dow(10)."""
    mc, mt = _max_rate("country_baselines"), _max_rate("title_baselines")
    country_s = _norm01(_safe_avg(_country_bl(job.country), "apply_rate"), mc) * 15.0
    fn_s = _FUNCTION_DEMAND.get((job.function or "").lower().strip(), 0.5) * 15.0
    tier_s = {1: 10.0, 2: 8.0, 3: 6.0, 4: 4.0, 5: 2.0}.get(
        min(max(job.priority, 1), 5), 6.0
    )
    ea_s = 20.0 if (job.application_method or "").lower() == "linkedin" else 0.0
    wp_s = _WORKPLACE_SCORES.get((job.workplace_type or "remote").lower().strip(), 5.0)
    title_s = (
        _norm01(_safe_avg(_title_bl(job.standardized_title), "apply_rate"), mt) * 10.0
    )
    cross = _cross_bl(job.country, job.standardized_title)
    hist_s = (
        _norm01(_safe_avg(cross, "apply_rate"), max(mc, mt, 1.0)) * 10.0
        if cross
        else 0.0
    )
    best_day, _ = _best_dow(job.country)
    dow_s = _DOW_SCORES.get(best_day.lower(), 5.0)
    return round(
        min(country_s + fn_s + tier_s + ea_s + wp_s + title_s + hist_s + dow_s, 100.0),
        2,
    )


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
    # Normalize title for better baseline matching
    norm_title = normalize_job_title(job.standardized_title or job.title)
    if norm_title and norm_title != job.standardized_title:
        job = Job(**{**job.__dict__, "standardized_title": norm_title})
    cross = _cross_bl(job.country, job.standardized_title)
    cb, tb = _country_bl(job.country), _title_bl(job.standardized_title)
    if cross:
        ar, vw, ap = (
            _safe_avg(cross, "apply_rate"),
            _safe_avg(cross, "views"),
            _safe_avg(cross, "applications"),
        )
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
    # Easy Apply vs ATS split from detailed benchmarks
    ea_ats = bl.get("easy_apply_vs_ats") or {}
    is_easy_apply = (job.application_method or "").lower() == "linkedin"
    method_key = "easy_apply" if is_easy_apply else "ats"
    # Try title+country+method, then title+method, then country+method
    ea_data = (
        ea_ats.get(f"{job.standardized_title}|{job.country}|{method_key}")
        or ea_ats.get(f"{job.standardized_title}||{method_key}")
        or ea_ats.get(f"|{job.country}|{method_key}")
    )
    if ea_data:
        ar = ea_data.get("avg_apply_rate") or ar
        vw = ea_data.get("avg_views") or vw
        ap = ea_data.get("avg_applications") or ap
        ss = max(ss, ea_data.get("count") or 0)
    # Also get the opposite method for comparison
    opp_key = "ats" if is_easy_apply else "easy_apply"
    opp_data = ea_ats.get(
        f"{job.standardized_title}|{job.country}|{opp_key}"
    ) or ea_ats.get(f"{job.standardized_title}||{opp_key}")
    return {
        "job_id": job.job_id,
        "country": job.country,
        "standardized_title": job.standardized_title,
        "application_method": job.application_method,
        "expected_apply_rate": round(ar, 2),
        "expected_views": round(vw, 1),
        "expected_apply_clicks": round(
            (
                ea_data.get("avg_apply_clicks", vw * ar / 100)
                if ea_data
                else vw * ar / 100
            ),
            1,
        ),
        "expected_applications": round(ap, 1),
        "easy_apply_comparison": (
            {
                "current_method": (
                    "Easy Apply" if is_easy_apply else "ATS (external site)"
                ),
                "current_applications": round(ap, 1),
                "alternative_applications": (
                    round(opp_data.get("avg_applications", 0), 1) if opp_data else None
                ),
                "alternative_rate": (
                    round(opp_data.get("avg_apply_rate", 0), 2) if opp_data else None
                ),
                "lift_available": (
                    round(opp_data["avg_apply_rate"] / ar, 1)
                    if opp_data and ar > 0 and not is_easy_apply
                    else None
                ),
                "recommendation": (
                    "Already using Easy Apply -- optimal"
                    if is_easy_apply
                    else f"Switch to Easy Apply for ~{EASY_APPLY_LIFT}x lift in apply rate"
                ),
            }
            if opp_data or is_easy_apply
            else None
        ),
        "best_day_of_week": best_day,
        "best_month": best_mo,
        "confidence_score": conf,
        "sample_size": ss,
        "data_source": "cross" if cross else "averaged",
        "optimal_timing": get_optimal_posting_time(job.country),
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
    preds = [
        predict_performance(j) for j in sorted(jobs, key=score_job, reverse=True)[:20]
    ]
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
    """POST /api/slotops/optimize -- Run full optimization with predictions per job."""
    t0 = time.monotonic()
    try:
        jobs, config = _parse_jobs_config(body)
        alloc = calculate_slot_allocation(config.countries, config.total_slots, jobs)
        sched = build_rotation_schedule(jobs, config)
        scored = sorted(
            ((score_job(j), j) for j in jobs), key=lambda x: x[0], reverse=True
        )
        # Enrich top jobs with predictions + external benchmarks
        top = []
        total_views = 0.0
        total_apps = 0.0
        for s, j in scored[:30]:
            pred = predict_performance(j)
            ext = get_industry_context(
                j.industry, j.function, j.country, j.workplace_type
            )
            ind_bench = ext.get("industry") or {}
            verdict = "GO" if s >= 70 else "CAUTION" if s >= 50 else "HOLD"
            entry = {
                "job_id": j.job_id,
                "title": j.title,
                "country": j.country,
                "score": s,
                "verdict": verdict,
                "expected_views": round(pred.get("expected_views", 0)),
                "expected_clicks": round(pred.get("expected_apply_clicks", 0)),
                "expected_applications": round(pred.get("expected_applications", 0)),
                "expected_apply_rate": pred.get("expected_apply_rate", 0),
                "best_day": pred.get("best_day_of_week", ""),
                "easy_apply_lift": (
                    round(
                        (pred.get("easy_apply_comparison") or {}).get("lift_available")
                        or 0,
                        1,
                    )
                ),
                "industry_avg_ar": ind_bench.get("avg_apply_rate_pct", 0),
                "industry_competition": ind_bench.get("competition_level", ""),
                "vs_industry": (
                    round(
                        pred.get("expected_apply_rate", 0)
                        - ind_bench.get("avg_apply_rate_pct", 0),
                        1,
                    )
                    if ind_bench.get("avg_apply_rate_pct")
                    else None
                ),
            }
            top.append(entry)
            total_views += pred.get("expected_views", 0)
            total_apps += pred.get("expected_applications", 0)

        # Portfolio-level summary
        portfolio = {
            "total_predicted_views": round(total_views),
            "total_predicted_applications": round(total_apps),
            "avg_apply_rate": (
                round(total_apps / total_views * 100, 1) if total_views > 0 else 0
            ),
            "go_count": sum(1 for t in top if t["verdict"] == "GO"),
            "caution_count": sum(1 for t in top if t["verdict"] == "CAUTION"),
            "hold_count": sum(1 for t in top if t["verdict"] == "HOLD"),
            "easy_apply_opportunity": sum(
                1 for t in top if t.get("easy_apply_lift", 0) > 0
            ),
        }

        ms = _timed("optimize", t0)
        return {
            "ok": True,
            "allocation": alloc,
            "schedule": [asdict(s) for s in sched],
            "top_jobs": top,
            "portfolio_summary": portfolio,
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


def handle_slotops_predict_analysis(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/predict-analysis -- LLM expert analysis for a single job prediction.

    Takes prediction data + job context and generates actionable strategic recommendation
    using all available data: baselines, Easy Apply benchmarks, seasonality, timing, and
    Joveo account performance data.
    """
    t0 = time.monotonic()
    call_fn, provider = _lazy_llm()
    if not call_fn:
        return {"ok": False, "error": "LLM not available", "analysis": ""}

    try:
        job_data = body.get("job") or {}
        prediction = body.get("prediction") or {}
        score_total = body.get("score_total", 0)

        country = job_data.get("country") or prediction.get("country") or "Unknown"
        title = (
            job_data.get("title") or prediction.get("standardized_title") or "Unknown"
        )
        method = (
            job_data.get("application_method")
            or prediction.get("application_method")
            or "ATS"
        )
        workplace = job_data.get("workplace_type") or "Remote"
        industry = job_data.get("industry") or ""
        company = job_data.get("company") or ""

        ar = prediction.get("expected_apply_rate", 0)
        views = prediction.get("expected_views", 0)
        apps = prediction.get("expected_applications", 0)
        clicks = prediction.get("expected_apply_clicks", 0)
        conf = prediction.get("confidence_score", 0)
        sample = prediction.get("sample_size", 0)
        best_day = prediction.get("best_day_of_week", "")
        best_month = prediction.get("best_month", "")
        data_src = prediction.get("data_source", "")

        comp = prediction.get("easy_apply_comparison") or {}
        timing = prediction.get("optimal_timing") or {}

        # Build country context
        bl = load_baselines()
        cb = _country_bl(country)
        country_avg_ar = _safe_avg(cb, "apply_rate") if cb else 0
        country_avg_views = _safe_avg(cb, "views") if cb else 0

        # Build seasonality context
        season = (bl.get("seasonality") or {}).get(country) or {}
        season_str = ""
        if season:
            top_months = sorted(
                season.items(), key=lambda x: -(x[1].get("avg_apply_rate") or 0)
            )[:3]
            season_str = ", ".join(
                f"{m}: {d.get('avg_apply_rate', 0):.1f}% AR" for m, d in top_months
            )

        # External industry benchmarks context
        ext = get_industry_context(
            industry, job_data.get("function", ""), country, workplace
        )
        ind_bench = ext.get("industry") or {}
        fn_bench = ext.get("function") or {}
        region_bench = ext.get("region") or {}
        wp_bench = ext.get("workplace") or {}
        platform_bench = ext.get("platform") or {}
        ea_bench = platform_bench.get("easy_apply_vs_ats") or {}

        # Joveo benchmark context
        joveo_str = "Joveo accounts combined: 66.3M views, 11M clicks, 16.6% avg AR (above 12.6% industry benchmark)"

        # Build external benchmarks section
        ext_section = ""
        if ind_bench:
            ext_section += f"\nINDUSTRY BENCHMARKS ({ext.get('industry_key', 'unknown').replace('_', ' ').title()}):"
            ext_section += f"\n- Industry avg apply rate: {ind_bench.get('avg_apply_rate_pct', 'N/A')}%"
            ext_section += (
                f"\n- Industry avg views: {ind_bench.get('avg_views', 'N/A')}"
            )
            ext_section += f"\n- Industry avg applications: {ind_bench.get('avg_applications', 'N/A')}"
            ext_section += (
                f"\n- Competition level: {ind_bench.get('competition_level', 'N/A')}"
            )
            ext_section += f"\n- Avg time to fill: {ind_bench.get('avg_time_to_fill_days', 'N/A')} days"
            ext_section += (
                f"\n- Avg CPA: ${ind_bench.get('avg_cost_per_application_usd', 'N/A')}"
            )
            if ind_bench.get("notes"):
                ext_section += f"\n- Key insight: {ind_bench['notes']}"
        if fn_bench:
            ext_section += f"\n\nFUNCTION BENCHMARKS ({ext.get('function_key', '').replace('_', ' ').title()}):"
            ext_section += f"\n- Function avg apply rate: {fn_bench.get('avg_apply_rate_pct', 'N/A')}%"
            ext_section += f"\n- Supply/demand ratio: {fn_bench.get('candidate_supply_demand_ratio', 'N/A')}"
            ext_section += f"\n- Avg time to fill: {fn_bench.get('avg_time_to_fill_days', 'N/A')} days"
            if fn_bench.get("recommended_channels"):
                ext_section += f"\n- Recommended channels: {', '.join(fn_bench['recommended_channels'][:4])}"
        if region_bench:
            ext_section += f"\n\nREGION BENCHMARKS ({ext.get('region_key', '').replace('_', ' ').title()}):"
            ext_section += f"\n- Region avg apply rate: {region_bench.get('avg_apply_rate_pct', 'N/A')}%"
            ext_section += (
                f"\n- Avg CPC: ${region_bench.get('avg_cost_per_click_usd', 'N/A')}"
            )
            ext_section += f"\n- Mobile application %: {region_bench.get('mobile_application_pct', 'N/A')}%"
            if region_bench.get("best_days"):
                ext_section += f"\n- Best days: {', '.join(region_bench['best_days'])}"
        if wp_bench:
            ext_section += f"\n\nWORKPLACE ({workplace}):"
            ext_section += f"\n- Workplace avg apply rate: {wp_bench.get('avg_apply_rate_pct', 'N/A')}%"
            ext_section += f"\n- Volume vs on-site: {wp_bench.get('application_volume_vs_onsite', 'N/A')}x"
        if ea_bench:
            ext_section += f"\n\nPLATFORM BENCHMARKS (LinkedIn overall):"
            ext_section += f"\n- Easy Apply avg AR: {ea_bench.get('easy_apply_avg_apply_rate_pct', 'N/A')}%"
            ext_section += f"\n- ATS redirect avg AR: {ea_bench.get('ats_redirect_avg_apply_rate_pct', 'N/A')}%"
            ext_section += f"\n- Easy Apply completion rate: {ea_bench.get('easy_apply_completion_rate_pct', 'N/A')}%"
            ext_section += f"\n- ATS redirect completion rate: {ea_bench.get('ats_redirect_completion_rate_pct', 'N/A')}%"
            ext_section += f"\n- Mobile Easy Apply: {ea_bench.get('mobile_easy_apply_rate_pct', 'N/A')}%"

        prompt = f"""You are a LinkedIn recruitment advertising expert at Joveo with access to both internal performance data (108K+ jobs) and external industry benchmarks from Appcast, Recruitics, LinkedIn Talent Solutions, SHRM, Gem, and Greenhouse.

Analyze this job posting prediction and give a strategic recommendation grounded in ALL data sources.

JOB DETAILS:
- Title: {title}
- Country: {country}
- Application Method: {method} ({"Easy Apply -- candidates apply directly on LinkedIn" if method == "LinkedIn" else "ATS -- candidates are redirected to external career site, 65% drop-off rate"})
- Workplace: {workplace}
- Industry: {industry or "Not specified"}
- Company: {company or "Not specified"}

PREDICTED PERFORMANCE (based on {sample} similar historical jobs from Joveo data):
- Expected Views: {views:.0f}
- Expected Apply Clicks: {clicks:.0f} (candidates who click Apply)
- Expected Applications: {apps:.0f} (candidates who complete application)
- Click-to-Application rate: {(apps/clicks*100) if clicks > 0 else 0:.0f}%
- Expected Apply Rate: {ar}%
- Slot Priority Score: {score_total}/100
- Confidence: {conf*100:.0f}%

COUNTRY BENCHMARKS ({country}, from Joveo 108K database):
- Country avg apply rate: {country_avg_ar:.1f}%
- Country avg views: {country_avg_views:.0f}
- This job vs country avg: {"+" if ar > country_avg_ar else ""}{ar - country_avg_ar:.1f}% difference
{ext_section}

EASY APPLY vs ATS (from Joveo database):
- Current method: {comp.get("current_method", method)}
- Current applications: {comp.get("current_applications", apps):.0f}
- Alternative method applications: {comp.get("alternative_applications", "N/A")}
- Alternative rate: {comp.get("alternative_rate", "N/A")}%
- Lift available: {comp.get("lift_available", "N/A")}x
- {comp.get("recommendation", "")}

OPTIMAL TIMING:
- Best posting day: {best_day}
- Best month: {best_month}
- Peak hours (local): Morning {timing.get("morning_peak_hours_local", [])}, Evening {timing.get("evening_peak_hours_local", [])}
- Timezone: {timing.get("timezone_label", "")} ({timing.get("timezone", "")})
- Top seasonal months: {season_str or "No seasonality data"}

JOVEO BENCHMARK:
- {joveo_str}

Write a 3-paragraph strategic recommendation:
1. **Verdict:** Should they post this job on LinkedIn? Compare against BOTH Joveo data AND external industry benchmarks. Be direct with specific numbers.
2. **Optimization:** What specific changes would improve performance? Quantify impact using industry data. Include CPA estimates if relevant.
3. **Timing & Strategy:** When to post, ideal rotation cadence, and channel mix recommendation (LinkedIn alone or combine with other channels?).

Be data-driven, specific, and actionable. Cross-reference internal Joveo data with external industry benchmarks. No generic advice. Max 250 words."""

        result = call_fn(
            messages=[{"role": "user", "content": prompt}],
            system_prompt="Senior LinkedIn recruitment advertising strategist at Joveo. You have access to both internal Joveo performance data (108K+ jobs) and external industry benchmarks from Appcast, Recruitics, LinkedIn Talent Solutions, SHRM, Gem, Greenhouse, and iCIMS. Cross-reference all sources for the most accurate recommendation. Data-driven, concise, actionable. Always cite specific numbers and their source.",
            max_tokens=600,
            task_type="summarization",
            force_provider=provider or "",
            use_cache=True,
        )
        analysis_text = result.get("text") or ""
        ms = _timed("predict-analysis", t0)
        return {
            "ok": True,
            "analysis": analysis_text,
            "computation_ms": ms,
        }
    except Exception as exc:
        logger.error(f"SlotOps predict-analysis error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc), "analysis": ""}


def handle_slotops_command_analysis(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/command-analysis -- LLM portfolio analysis for Command Center.

    Generates a strategic summary analyzing the entire uploaded job portfolio,
    identifying patterns, risks, and optimization opportunities across all jobs.
    """
    t0 = time.monotonic()
    call_fn, provider = _lazy_llm()
    if not call_fn:
        return {"ok": False, "error": "LLM not available", "analysis": ""}

    try:
        portfolio = body.get("portfolio") or {}
        top_jobs = body.get("top_jobs") or []
        countries = body.get("countries") or []
        total_jobs = body.get("total_jobs", 0)
        total_slots = body.get("total_slots", 0)

        # Build job summary for LLM
        job_lines = []
        for j in top_jobs[:15]:
            job_lines.append(
                f"  - {j.get('title', '?')} | {j.get('country', '?')} | "
                f"Score: {j.get('score', 0)}/100 | Verdict: {j.get('verdict', '?')} | "
                f"Views: {j.get('expected_views', 0)} | Apps: {j.get('expected_applications', 0)} | "
                f"AR: {j.get('expected_apply_rate', 0)}% | "
                f"vs Industry: {j.get('vs_industry', 'N/A')}%"
            )
        jobs_str = "\n".join(job_lines)

        # Load external benchmarks for context
        ib = load_industry_benchmarks()
        platform = ib.get("linkedin_platform_benchmarks", {}).get("overall", {})

        prompt = f"""You are a senior LinkedIn recruitment advertising strategist at Joveo analyzing a client's full job portfolio.

PORTFOLIO OVERVIEW:
- Total jobs: {total_jobs}
- Total slots: {total_slots}
- Countries active: {', '.join(countries[:10])}
- GO (score 70+): {portfolio.get('go_count', 0)} jobs
- CAUTION (50-69): {portfolio.get('caution_count', 0)} jobs
- HOLD (<50): {portfolio.get('hold_count', 0)} jobs
- Total predicted views: {portfolio.get('total_predicted_views', 0):,}
- Total predicted applications: {portfolio.get('total_predicted_applications', 0):,}
- Portfolio avg apply rate: {portfolio.get('avg_apply_rate', 0)}%
- Easy Apply opportunity: {portfolio.get('easy_apply_opportunity', 0)} jobs not using Easy Apply

LINKEDIN PLATFORM BENCHMARKS:
- Platform avg AR: {platform.get('avg_apply_rate_pct', 11.2)}%
- Platform avg views/posting: {platform.get('avg_views_per_posting', 150)}
- Platform avg apps/posting: {platform.get('avg_applications_per_posting', 12)}
- Easy Apply adoption: {platform.get('easy_apply_adoption_pct', 65)}%

TOP JOBS (ranked by score):
{jobs_str}

Write a concise portfolio analysis (3 sections, max 200 words):
1. **Portfolio Health**: Overall performance vs LinkedIn platform benchmarks. Key strengths and weaknesses.
2. **Top 3 Actions**: Most impactful changes to make TODAY, with specific job references and expected impact.
3. **Slot Strategy**: How to optimize the {total_slots} slots across {len(countries)} countries for maximum ROI.

Be specific. Reference actual job titles and countries. Quantify impact."""

        result = call_fn(
            messages=[{"role": "user", "content": prompt}],
            system_prompt="Senior LinkedIn recruitment strategist at Joveo. Analyzing a client portfolio of job postings. Data-driven, specific, actionable. Reference individual jobs by name.",
            max_tokens=500,
            task_type="summarization",
            force_provider=provider or "",
            use_cache=True,
        )
        analysis_text = result.get("text") or ""
        ms = round((time.monotonic() - t0) * 1000, 1)
        return {
            "ok": True,
            "analysis": analysis_text,
            "computation_ms": ms,
        }
    except Exception as exc:
        logger.error(f"SlotOps command-analysis error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc), "analysis": ""}


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
        country, title = str(query_params.get("country") or ""), str(
            query_params.get("title") or ""
        )
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


def handle_slotops_upload(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/upload -- Score jobs, assign slots, generate 30-day rotation."""
    t0 = time.monotonic()
    try:
        raw_jobs = body.get("jobs") or []
        if not raw_jobs:
            return {"ok": False, "error": "No jobs provided in upload"}
        config = _config_from_dict(body.get("config") or {})
        jobs = _jobs_from_dicts(raw_jobs)
        if not config.countries:
            config.countries = _auto_countries(jobs, config)
        scored = sorted(
            [(score_job(j), j) for j in jobs], key=lambda x: x[0], reverse=True
        )
        top_jobs = [
            {
                "job_id": j.job_id,
                "title": j.title,
                "country": j.country,
                "score": s,
                "application_method": j.application_method,
                "workplace_type": j.workplace_type,
                "function": j.function,
            }
            for s, j in scored
        ]
        alloc = calculate_slot_allocation(config.countries, config.total_slots, jobs)
        schedule = build_rotation_schedule(jobs, config)
        cycle_len = sum(REFRESH_CADENCE_DAYS) // len(REFRESH_CADENCE_DAYS)
        rotation_plan: list[dict[str, Any]] = []
        for idx, (sc, j) in enumerate(scored):
            sd = (idx * cycle_len) % 30 + 1
            dur = REFRESH_CADENCE_DAYS[idx % len(REFRESH_CADENCE_DAYS)]
            rotation_plan.append(
                {
                    "job_id": j.job_id,
                    "title": j.title,
                    "score": sc,
                    "start_day": sd,
                    "end_day": min(sd + dur - 1, 30),
                    "duration_days": dur,
                }
            )
        ms = _timed("optimize", t0)
        return {
            "ok": True,
            "total_jobs": len(jobs),
            "scored_jobs": top_jobs,
            "allocation": alloc,
            "schedule": [asdict(s) for s in schedule],
            "rotation_30day": rotation_plan,
            "countries": config.countries,
            "total_slots": config.total_slots,
            "computation_ms": ms,
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except (TypeError, KeyError) as exc:
        logger.error(f"SlotOps upload error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


def generate_daily_actions(
    day: int, schedule: list[dict[str, Any]], jobs: list[dict[str, Any]]
) -> dict[str, Any]:
    """For day 1-30, return go_live/take_down/keep_live lists and day-type flags."""
    try:
        go_live, take_down, keep_live = (
            [],
            [],
            [],
        )  # type: list[str], list[str], list[str]
        for entry in schedule:
            jid = str(entry.get("job_id") or "")
            start, end = int(entry.get("start_day") or 1), int(
                entry.get("end_day") or 30
            )
            if start == day:
                go_live.append(jid)
            elif end == day:
                take_down.append(jid)
            elif start < day < end:
                keep_live.append(jid)
        dow_names = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        dow_name = dow_names[(day - 1) % 7]
        return {
            "day": day,
            "day_of_week": dow_name,
            "go_live": go_live,
            "take_down": take_down,
            "keep_live": keep_live,
            "go_live_count": len(go_live),
            "take_down_count": len(take_down),
            "keep_live_count": len(keep_live),
            "is_prime_day": dow_name in BEST_POSTING_DAYS,
            "is_weekend": dow_name in ("Saturday", "Sunday"),
            "is_ops_day": dow_name == "Monday",
        }
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(
            f"generate_daily_actions error for day {day}: {exc}", exc_info=True
        )
        return {"day": day, "error": str(exc)}


def generate_quick_wins(
    jobs: list[dict[str, Any]], baselines: dict[str, Any] | None = None
) -> list[dict[str, str]]:
    """Analyze jobs and return actionable recommendations with type, message, impact."""
    try:
        bl = baselines or load_baselines()
        recs: list[dict[str, str]] = []
        ats_count = sum(
            1 for j in jobs if (j.get("application_method") or "ATS").upper() == "ATS"
        )
        if ats_count:
            recs.append(
                {
                    "type": "easy_apply",
                    "impact": "high",
                    "message": f"Switch {ats_count} ATS jobs to Easy Apply = {round((EASY_APPLY_LIFT - 1) * 100)}% lift in apply rates",
                }
            )
        recs.append(
            {
                "type": "posting_timing",
                "impact": "medium",
                "message": f"Post on {', '.join(BEST_POSTING_DAYS)} for best apply rates (up to 40% better than Friday)",
            }
        )
        country_rates: dict[str, float] = {}
        for j in jobs:
            c = j.get("country") or ""
            if c:
                r = _safe_avg(_country_bl(c), "apply_rate")
                if r > country_rates.get(c, 0):
                    country_rates[c] = r
        top_c = sorted(country_rates.items(), key=lambda x: x[1], reverse=True)[:3]
        if top_c:
            top_set = {c for c, _ in top_c}
            n = sum(1 for j in jobs if (j.get("country") or "") in top_set)
            recs.append(
                {
                    "type": "high_value_countries",
                    "impact": "high",
                    "message": f"{n} jobs in {', '.join(c for c, _ in top_c)} are high-value -- prioritize these slots",
                }
            )
        stale_n = sum(
            1 for j in jobs if int(j.get("days_live") or 0) > max(REFRESH_CADENCE_DAYS)
        )
        if stale_n:
            recs.append(
                {
                    "type": "refresh_stale",
                    "impact": "medium",
                    "message": f"Refresh {stale_n} jobs that are > {max(REFRESH_CADENCE_DAYS)} days old",
                }
            )
        non_remote = sum(
            1
            for j in jobs
            if (j.get("workplace_type") or "").lower() not in ("remote", "")
        )
        if non_remote:
            recs.append(
                {
                    "type": "workplace_optimization",
                    "impact": "medium",
                    "message": f"{non_remote} jobs are not Remote -- remote roles get 2x more applies on LinkedIn",
                }
            )
        return recs
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"generate_quick_wins error: {exc}", exc_info=True)
        return [{"type": "error", "message": str(exc), "impact": "none"}]


# -- API Wrappers for daily-actions and quick-wins ----------------------------


def handle_slotops_daily_actions(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/daily-actions -- Get go-live/take-down lists for a given day."""
    try:
        day = int(body.get("day") or 1)
        raw_jobs = body.get("jobs") or []
        jobs_as_dicts = raw_jobs if raw_jobs else []
        config = _config_from_dict(body.get("config") or {})
        if not config.countries and raw_jobs:
            config.countries = list(
                {(j.get("country") or "") for j in raw_jobs if j.get("country")}
            )
        jobs = _jobs_from_dicts(raw_jobs)
        sched = build_rotation_schedule(jobs, config)
        schedule_dicts = [
            {
                "country": s.country,
                "slots_allocated": s.slots_allocated,
                "jobs_in_window": s.jobs_in_window,
            }
            for s in sched
        ]
        result = generate_daily_actions(day, schedule_dicts, jobs_as_dicts)
        return {"ok": True, **result}
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"handle_slotops_daily_actions error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}


def handle_slotops_quick_wins(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/quick-wins -- Data-driven optimization recommendations."""
    try:
        raw_jobs = body.get("jobs") or []
        wins = generate_quick_wins(raw_jobs, load_baselines())
        return {"ok": True, "recommendations": wins, "count": len(wins)}
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"handle_slotops_quick_wins error: {exc}", exc_info=True)
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


_title_norm_cache: dict[str, str] = {}


def normalize_job_title(raw_title: str) -> str:
    """Normalize a job title to match our baseline standardized titles using Gemini Flash Lite.

    Uses LLM to fuzzy-match user input (e.g., 'software dev', 'Sr. SWE', 'full stack developer')
    to our 962 baseline standardized titles. Falls back to basic string matching if LLM unavailable.
    Results are cached to avoid repeated LLM calls.
    """
    if not raw_title:
        return ""
    raw_clean = raw_title.strip()
    if raw_clean in _title_norm_cache:
        return _title_norm_cache[raw_clean]

    bl = load_baselines()
    all_titles = list((bl.get("title_baselines") or {}).keys())

    # Fast exact match
    for t in all_titles:
        if t.lower() == raw_clean.lower():
            _title_norm_cache[raw_clean] = t
            return t

    raw_lower = raw_clean.lower()

    # Keyword alias map for common variations
    _ALIASES: dict[str, str] = {
        "software dev": "Software Engineer",
        "swe": "Software Engineer",
        "sde": "Software Engineer",
        "full stack": "Full Stack Engineer",
        "frontend": "Frontend Engineer",
        "backend": "Backend Engineer",
        "data science": "Data Scientist",
        "ml engineer": "Machine Learning Engineer",
        "devops": "DevOps Engineer",
        "qa": "Quality Assurance Engineer",
        "hr": "Human Resources",
        "pm": "Project Manager",
        "product manager": "Product Manager",
        "customer support": "Customer Support Specialist",
        "customer service": "Customer Service Representative",
        "nurse": "Registered Nurse",
        "rn": "Registered Nurse",
        "doctor": "Medical Doctor",
        "truck driver": "Truck Driver",
        "driver": "Driver",
        "warehouse": "Warehouse Associate",
    }
    for alias, target in _ALIASES.items():
        if alias in raw_lower:
            if target in all_titles:
                _title_norm_cache[raw_clean] = target
                return target

    # Fast substring match
    raw_lower = raw_clean.lower()
    for t in all_titles:
        if raw_lower in t.lower() or t.lower() in raw_lower:
            _title_norm_cache[raw_clean] = t
            return t

    # LLM-based fuzzy matching
    call_fn, provider = _lazy_llm()
    if call_fn:
        try:
            sample_titles = [
                t
                for t in all_titles
                if any(w in t.lower() for w in raw_lower.split()[:2])
            ][:30]
            if not sample_titles:
                sample_titles = all_titles[:50]
            prompt = (
                f"Match this job title to the closest standardized title from the list.\n"
                f'Input: "{raw_clean}"\n'
                f"Standardized titles: {', '.join(sample_titles)}\n"
                f"Reply with ONLY the matching title, nothing else. If no good match, reply with the closest one."
            )
            result = call_fn(
                messages=[{"role": "user", "content": prompt}],
                system_prompt="You are a job title normalizer. Reply with only the matched title.",
                max_tokens=50,
                task_type="classification",
                force_provider=provider or "",
                use_cache=True,
            )
            matched = (result.get("text") or "").strip().strip('"').strip("'")
            if matched and matched in all_titles:
                _title_norm_cache[raw_clean] = matched
                logger.info(f"LLM title normalization: '{raw_clean}' -> '{matched}'")
                return matched
            # Check if LLM returned something close
            for t in all_titles:
                if matched.lower() == t.lower():
                    _title_norm_cache[raw_clean] = t
                    return t
        except Exception as exc:
            logger.error(f"LLM title normalization failed: {exc}", exc_info=True)

    # Fallback: return as-is
    _title_norm_cache[raw_clean] = raw_clean
    return raw_clean


def generate_optimization_report(
    schedule: list[dict], allocation: dict[str, int], top_jobs: list[dict]
) -> str:
    """Generate natural language optimization report via Gemini Flash Lite. Returns markdown or empty string."""
    call_fn, provider = _lazy_llm()
    if not call_fn:
        return ""
    try:
        cs = ", ".join(
            f"{k}: {v} slots"
            for k, v in sorted(allocation.items(), key=lambda x: -x[1])[:10]
        )
        tj = "\n".join(
            f"- {j.get('title', 'N/A')} in {j.get('country', 'N/A')}: score {j.get('score', 0):.1f}"
            for j in top_jobs[:10]
        )
        prompt = (
            f"Analyze this LinkedIn job slot optimization (150 words max):\n"
            f"Allocation: {cs}\nTotal Slots: {sum(allocation.values())}\n"
            f"Rotation Windows: {len(schedule)} per day\nTop Jobs:\n{tj}\n"
            f"Focus: allocation rationale, overweighted countries, timezone rotation benefit."
        )
        result = call_fn(
            messages=[{"role": "user", "content": prompt}],
            system_prompt="Recruitment advertising optimization analyst. Concise, data-driven.",
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
    """POST /api/slotops/insights -- LLM-powered optimization report (Gemini Flash Lite)."""
    t0 = time.monotonic()
    try:
        jobs = _jobs_from_dicts(body.get("jobs") or []) if body.get("jobs") else []
        config = _config_from_dict(body.get("config") or {})
        load_baselines()
        allocation = calculate_slot_allocation(
            config.countries, config.total_slots, jobs
        )
        sched_objs = build_rotation_schedule(jobs, config)
        schedule = [
            {
                "country": s.country,
                "window_start_utc": s.window_start_utc,
                "window_end_utc": s.window_end_utc,
                "slots_allocated": s.slots_allocated,
            }
            for s in sched_objs
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


# ---------------------------------------------------------------------------
# Job Eligibility Analysis & Slot Planning (108K-job baselines)
# ---------------------------------------------------------------------------


def _eligibility_score(job_dict: dict[str, Any]) -> tuple[int, list[str]]:
    """Score a single job for LinkedIn eligibility (0-100). Returns (score, reasons)."""
    score = 0
    reasons: list[str] = []
    title_lower = str(job_dict.get("title") or "").lower().strip()
    country_raw = str(job_dict.get("country") or "")
    country = _resolve_country(country_raw)
    description = str(job_dict.get("description") or "")
    workplace = (
        str(job_dict.get("workplace_type") or job_dict.get("type") or "")
        .lower()
        .strip()
    )

    # 1. Country has baseline data with good apply rate (>5%): +20
    cb = _country_bl(country)
    country_rate = _safe_avg(cb, "apply_rate") if cb else 0.0
    if country_rate > 5.0:
        score += 20
        reasons.append(f"Country {country} has {country_rate:.1f}% avg apply rate")
    elif cb:
        reasons.append(
            f"Country {country} apply rate {country_rate:.1f}% below 5% threshold"
        )

    # 2. Title matches known high-performer: +20
    is_high = any(kw in title_lower for kw in _HIGH_PERFORMER_KEYWORDS)
    if is_high:
        score += 20
        reasons.append(f"High-performing title match")

    # 3. Remote or can be posted as Remote: +15
    if workplace in ("remote", "hybrid", ""):
        score += 15
        reasons.append("Remote/hybrid eligible")

    # 4. Country has timezone with peak hours: +10
    tz_peaks = (load_baselines().get("timezone_peaks") or {}).get(country)
    if tz_peaks and tz_peaks.get("utc_peak_hours"):
        score += 10
        reasons.append("Timezone rotation available")

    # 5. Title has Easy Apply benchmarks >15% apply rate: +15
    ea_ats = load_baselines().get("easy_apply_vs_ats") or {}
    norm_title = normalize_job_title(title_lower)
    ea_key = f"{norm_title}||easy_apply"
    ea_data = ea_ats.get(ea_key)
    if ea_data and (ea_data.get("avg_apply_rate") or 0) > 15.0:
        score += 15
        reasons.append(
            f"Easy Apply benchmark {ea_data['avg_apply_rate']:.1f}% apply rate"
        )

    # 6. Not a niche/local role: +10
    is_niche = any(kw in title_lower for kw in _NICHE_KEYWORDS)
    if not is_niche:
        score += 10
        reasons.append("Not a niche/local role")
    else:
        reasons.append(f"Niche/local role detected -- may not perform on LinkedIn")

    # 7. Has description: +10
    if description.strip():
        score += 10
        reasons.append("Has job description")
    else:
        reasons.append("Missing job description")

    return min(score, 100), reasons


def analyze_job_eligibility(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze LinkedIn eligibility for a list of jobs based on 108K-job baselines.

    Each job receives a LinkedIn Eligibility Score (0-100) and classification:
    >= 60 ELIGIBLE, 40-59 CONDITIONAL, < 40 NOT RECOMMENDED.
    """
    load_baselines()
    eligible: list[dict] = []
    conditional: list[dict] = []
    not_recommended: list[dict] = []
    all_scored: list[dict] = []

    for idx, job in enumerate(jobs):
        score, reasons = _eligibility_score(job)
        title = str(job.get("title") or "")
        country_raw = str(job.get("country") or "")
        country = _resolve_country(country_raw)
        company = str(
            job.get("company") or job.get("client") or job.get("_sheet_name") or ""
        )
        job_id = str(
            job.get("job_id")
            or job.get("referencenumber")
            or job.get("reference_number")
            or f"job_{idx}"
        )

        if score >= 60:
            classification = "ELIGIBLE"
        elif score >= 40:
            classification = "CONDITIONAL"
        else:
            classification = "NOT RECOMMENDED"

        entry = {
            "job_id": job_id,
            "title": title,
            "country": country,
            "company": company,
            "score": score,
            "classification": classification,
            "reasons": reasons,
        }
        all_scored.append(entry)
        if classification == "ELIGIBLE":
            eligible.append(entry)
        elif classification == "CONDITIONAL":
            conditional.append(entry)
        else:
            not_recommended.append(entry)

    # Aggregations
    by_client: dict[str, int] = {}
    by_country: dict[str, int] = {}
    for e in eligible:
        c = e["company"] or "Unknown"
        by_client[c] = by_client.get(c, 0) + 1
        co = e["country"] or "Unknown"
        by_country[co] = by_country.get(co, 0) + 1

    return {
        "total_jobs_analyzed": len(jobs),
        "eligible": len(eligible),
        "conditional": len(conditional),
        "not_recommended": len(not_recommended),
        "eligible_by_client": dict(sorted(by_client.items(), key=lambda x: -x[1])),
        "eligible_by_country": dict(sorted(by_country.items(), key=lambda x: -x[1])),
        "scored_jobs": all_scored,
        "eligible_jobs": eligible,
        "conditional_jobs": conditional,
        "not_recommended_jobs": not_recommended,
    }


def generate_slot_plan(
    eligible_jobs: list[dict[str, Any]], total_slots: int = 501
) -> dict[str, Any]:
    """Generate a 30-day slot rotation plan for eligible jobs.

    Groups by country + timezone, allocates slots ROI-weighted from baselines,
    builds daily rotation respecting Tue-Thu prime days, no new posts Friday,
    Monday is ops day.
    """
    load_baselines()
    if not eligible_jobs:
        return {
            "total_eligible": 0,
            "slot_allocation": {},
            "rotation_plan": [],
            "utilization_rate": 0.0,
            "recommendations": ["No eligible jobs to plan."],
        }

    # Build Job objects for slot allocation
    job_objs = _jobs_from_dicts(eligible_jobs)
    countries = list({j.country for j in job_objs if j.country})
    alloc = calculate_slot_allocation(countries, total_slots, job_objs)

    # Score and sort
    scored = sorted(
        [(score_job(j), j, ej) for j, ej in zip(job_objs, eligible_jobs)],
        key=lambda x: x[0],
        reverse=True,
    )

    # Build 30-day rotation plan
    cycle_len = sum(REFRESH_CADENCE_DAYS) // len(REFRESH_CADENCE_DAYS)
    dow_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    rotation_plan: list[dict[str, Any]] = []
    daily_live: dict[int, list[str]] = {d: [] for d in range(1, 31)}

    for idx, (sc, j, ej) in enumerate(scored):
        start_day = (idx * cycle_len) % 30 + 1
        dur = REFRESH_CADENCE_DAYS[idx % len(REFRESH_CADENCE_DAYS)]
        end_day = min(start_day + dur - 1, 30)
        dow_name = dow_names[(start_day - 1) % 7]

        # No new posts on Friday; shift to Thursday
        if dow_name == "Friday" and start_day > 1:
            start_day -= 1
            end_day = min(start_day + dur - 1, 30)

        jid = str(ej.get("job_id") or ej.get("referencenumber") or f"job_{idx}")
        rotation_plan.append(
            {
                "job_id": jid,
                "title": j.title,
                "country": j.country,
                "company": j.company,
                "score": sc,
                "start_day": start_day,
                "end_day": end_day,
                "duration_days": dur,
            }
        )
        for d in range(start_day, end_day + 1):
            if d <= 30:
                daily_live[d].append(jid)

    # Daily summary
    daily_summary: list[dict[str, Any]] = []
    for d in range(1, 31):
        dow_name = dow_names[(d - 1) % 7]
        live_count = len(daily_live[d])
        daily_summary.append(
            {
                "day": d,
                "day_of_week": dow_name,
                "jobs_live": live_count,
                "slot_utilization": (
                    round(min(live_count / total_slots, 1.0), 4)
                    if total_slots > 0
                    else 0.0
                ),
                "is_prime_day": dow_name in BEST_POSTING_DAYS,
                "is_ops_day": dow_name == "Monday",
            }
        )

    avg_util = (
        sum(ds["slot_utilization"] for ds in daily_summary) / 30.0
        if daily_summary
        else 0.0
    )

    # Recommendations
    recs: list[str] = []
    n_elig = len(eligible_jobs)
    cycles_needed = max(1, math.ceil(n_elig / total_slots))
    recs.append(
        f"{n_elig} eligible jobs can be served by {total_slots} slots "
        f"with {cycles_needed} rotation cycle{'s' if cycles_needed > 1 else ''}"
    )
    top_country = max(alloc.items(), key=lambda x: x[1]) if alloc else ("N/A", 0)
    cb = _country_bl(top_country[0])
    top_rate = _safe_avg(cb, "apply_rate") if cb else 0.0
    top_sample = (cb or {}).get("sample_size", 0)
    recs.append(
        f"{top_country[0]} gets {top_country[1]} slots "
        f"(highest ROI: {top_rate:.2f}% apply rate, {top_sample:,}+ baseline jobs)"
    )
    recs.append("Post on Tuesday-Thursday for best results")
    recs.append("Monday is ops day -- use for monitoring, not new launches")
    recs.append("Avoid launching new posts on Friday (lowest engagement)")

    return {
        "total_eligible": n_elig,
        "total_slots": total_slots,
        "slot_allocation": alloc,
        "rotation_plan": rotation_plan,
        "daily_summary": daily_summary,
        "utilization_rate": round(avg_util, 4),
        "recommendations": recs,
    }


def handle_slotops_analyze(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/slotops/analyze -- Job eligibility analysis and slot planning.

    Accepts:
        jobs: list[dict] -- parsed from Excel
        total_slots: int -- default 501
        include_conditional: bool -- include CONDITIONAL jobs in the slot plan
    """
    t0 = time.monotonic()
    try:
        raw_jobs = body.get("jobs") or []
        if not raw_jobs:
            return {"ok": False, "error": "No jobs provided"}
        total_slots = int(body.get("total_slots") or 501)
        include_conditional = bool(body.get("include_conditional", False))

        # Step 1: Eligibility analysis
        eligibility = analyze_job_eligibility(raw_jobs)

        # Step 2: Build slot plan from eligible (+ conditional if requested)
        plan_jobs = list(eligibility["eligible_jobs"])
        if include_conditional:
            plan_jobs.extend(eligibility["conditional_jobs"])

        # Re-build dicts for slot planning (need original dict fields for _jobs_from_dicts)
        plan_dicts: list[dict[str, Any]] = []
        scored_map = {e["job_id"]: e for e in eligibility["scored_jobs"]}
        for pj in plan_jobs:
            jid = pj["job_id"]
            # Find original dict
            orig = next(
                (
                    r
                    for r in raw_jobs
                    if str(
                        r.get("job_id")
                        or r.get("referencenumber")
                        or r.get("reference_number")
                        or ""
                    )
                    == jid
                ),
                pj,
            )
            plan_dicts.append(orig)

        slot_plan = generate_slot_plan(plan_dicts, total_slots)

        ms = round((time.monotonic() - t0) * 1000, 1)
        return {
            "ok": True,
            "total_jobs_analyzed": eligibility["total_jobs_analyzed"],
            "eligible": eligibility["eligible"],
            "conditional": eligibility["conditional"],
            "not_recommended": eligibility["not_recommended"],
            "eligible_by_client": eligibility["eligible_by_client"],
            "eligible_by_country": eligibility["eligible_by_country"],
            "slot_allocation": slot_plan["slot_allocation"],
            "rotation_plan": slot_plan["rotation_plan"],
            "daily_summary": slot_plan["daily_summary"],
            "utilization_rate": slot_plan["utilization_rate"],
            "recommendations": slot_plan["recommendations"],
            "include_conditional": include_conditional,
            "total_slots": total_slots,
            "computation_ms": ms,
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except (TypeError, KeyError) as exc:
        logger.error(f"SlotOps analyze error: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc)}
