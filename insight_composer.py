"""Client-specific prose fragments shared by the deck and workbook generators.

Every function here interpolates the concrete facts it was given (competitor
name, role, city, industry) rather than emitting generic filler -- the
shipped bug this replaces was boilerplate text that read identically
regardless of which competitor or role was actually being discussed.
"""

from __future__ import annotations

import hashlib

# ---------------------------------------------------------------------------
# Counter-strategy prose
# ---------------------------------------------------------------------------
# Skeletons are keyed by competitor_type "bucket". Each bucket has >=4
# skeletons so a deterministic hash of (competitor, role) picks between them
# without ever producing byte-identical text for two different competitors
# (or roles) that land in the same bucket.
_SKELETON_BANKS: dict[str, tuple[str, ...]] = {
    "staffing_agency": (
        "{competitor} is actively staffing {angle} through third-party "
        "agencies — compress time-to-offer so direct-hire terms win before "
        "an agency placement locks the candidate in.",
        "Expect {competitor} to route {angle} through contract-to-hire "
        "channels; lead with permanent-role stability and a clear "
        "total-comp comparison to counter it.",
        "{competitor}'s agency pipeline for {angle} moves fast — a "
        "same-week interview slot and pre-approved offer band are the "
        "clearest levers to beat it to the candidate.",
        "To out-compete {competitor} on {angle}, tighten the direct-hire "
        "funnel: pre-screened shortlists and a 48-hour offer turnaround "
        "beat a typical agency placement cycle.",
        "{competitor} typically wins {angle} on availability, not loyalty "
        "— a standing direct-hire offer with no re-application step "
        "converts agency-sourced candidates before their next assignment.",
        "Where {competitor} places {angle} on short-term assignments, "
        "position this role's permanence and benefits eligibility as the "
        "upgrade path out of temp work.",
        "{competitor}'s markup on {angle} placements leaves room to "
        "compete on take-home pay alone — lead with a transparent, "
        "higher net-pay comparison.",
        "Candidates {competitor} places as {angle} rarely get a real "
        "career conversation — a named hiring manager and a clear growth "
        "track are the differentiator here.",
        "{competitor} re-markets the same {angle} pool across multiple "
        "clients — speed to first contact after application determines "
        "who gets there first.",
        "Because {competitor} earns a fee per {angle} placement regardless "
        "of fit, expect volume over precision — win on a tighter, "
        "better-matched shortlist instead.",
    ),
    "direct_employer": (
        "{competitor} is hiring {angle} directly, so brand and total-comp "
        "clarity matter more than speed alone — lead with a concrete "
        "growth-path story.",
        "Expect {competitor} to keep pressure on {angle}; a faster "
        "interview-to-offer cycle is the clearest lever to counter a peer "
        "employer with a similar offer.",
        "{competitor}'s employer brand is visible to {angle} — "
        "differentiate on schedule flexibility or sign-on incentives where "
        "the base comp is comparable.",
        "Against {competitor} for {angle}, win on process: a single-visit "
        "interview loop and a same-week offer close the gap a bigger brand "
        "name can otherwise cover.",
        "{competitor} competes for {angle} on name recognition first — "
        "a candidate-facing comp calculator makes the pay comparison "
        "concrete instead of assumed.",
        "Where {competitor}'s hiring process for {angle} runs multiple "
        "rounds, a compressed two-touch loop wins candidates unwilling to "
        "wait out a slower employer.",
        "{competitor} and this role draw from the same {angle} pool — "
        "referral incentives and manager-led outreach reach candidates "
        "before a generic job-board listing does.",
        "If {competitor} is slow to post openings for {angle}, an "
        "always-on requisition with rolling interviews captures candidates "
        "during that gap.",
        "{competitor}'s retention pitch to {angle} leans on tenure — "
        "counter with a faster path to responsibility for candidates "
        "who don't want to wait years for it.",
        "Expect {competitor} to match base pay for {angle} but not "
        "schedule flexibility — make flexibility the headline, not a "
        "footnote.",
    ),
    "gig_platform": (
        "{competitor} pulls {angle} toward flexible, app-based work — "
        "lead with schedule control and guaranteed hours to counter the "
        "platform's flexibility pitch.",
        "Expect {competitor} to undercut on time-to-first-shift for "
        "{angle}; match it with a same-day onboarding path where possible.",
        "{competitor}'s gig model appeals to {angle} who want "
        "predictability elsewhere — benefits and a fixed schedule are the "
        "counter-offer.",
        "To out-compete {competitor} for {angle}, emphasize what the "
        "platform can't: benefits eligibility, career progression, and "
        "shift predictability.",
        "{competitor} offers {angle} instant sign-up but no guaranteed "
        "income floor — a stated minimum weekly pay is the concrete "
        "counter.",
        "Where {competitor} leaves {angle} to self-schedule around thin "
        "demand, a set roster with reliable hours reads as the more "
        "stable option.",
        "{competitor}'s per-task pay for {angle} has no ceiling on "
        "downside — a guaranteed hourly floor plus upside removes that "
        "risk.",
        "Candidates who try {competitor} as {angle} often churn back "
        "within weeks — time outreach to former gig workers who already "
        "know the gap.",
        "{competitor} gives {angle} no path to a W-2 role — make the "
        "conversion-to-permanent option explicit and easy to find.",
        "Against {competitor} for {angle}, sign-on plus a first-week pay "
        "guarantee beats a platform's pay-per-task uncertainty.",
    ),
    "default": (
        "{competitor} is actively competing for {angle} — sharpen offer "
        "cadence and speed-to-contact to stay ahead of it.",
        "Expect {competitor} to keep pressure on {angle}; a faster "
        "interview-to-offer cycle is the clearest lever to counter it.",
        "{competitor}'s presence in this pool means {angle} "
        "have options — lead with total-comp clarity and a same-week "
        "interview slot.",
        "To out-compete {competitor} for {angle}, tighten the funnel: "
        "pre-screened shortlists and a 48-hour offer turnaround.",
        "{competitor} is a known name to {angle} in this market — a "
        "specific, verifiable comp figure beats a generic brand "
        "impression.",
        "Where {competitor} is slower to respond to {angle}, first-contact "
        "speed alone can decide who gets the candidate.",
        "{competitor}'s reputation with {angle} is untested here — "
        "candidate reviews and a named team contact build trust faster "
        "than brand alone.",
        "Against {competitor} for {angle}, a same-week site visit or "
        "shadow shift gives candidates something a job posting can't.",
        "{competitor} likely draws from the same {angle} channels this "
        "plan targets — differentiated creative on those same channels "
        "avoids losing the impression entirely.",
        "To stay ahead of {competitor} on {angle}, keep the offer window "
        "short — a candidate weighing two open offers usually takes the "
        "one that resolves first.",
    ),
}


def _pick_index(key: str, n: int) -> int:
    """Deterministic (process-stable) index in [0, n) from a hash of ``key``.

    Uses sha256 rather than the builtin ``hash()`` because Python randomizes
    str hashing per-process (PYTHONHASHSEED) -- this must be reproducible
    across runs and processes.
    """
    if n <= 0:
        return 0
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % n


def compose_counter_strategy(competitor: str, ctx: dict | None = None) -> str:
    """One sentence of competitor-specific counter-strategy prose.

    ``ctx`` (all optional): role, city, industry, intensity, competitor_type.
    Always interpolates the competitor name and at least one role- or
    city-specific angle. Phrasing varies deterministically by a hash of
    (competitor, role) across >=4 skeletons per bucket, so two competitors
    in the same bucket never render byte-identical text.
    """
    ctx = ctx or {}
    name = str(competitor or "").strip() or "This competitor"
    role = str(ctx.get("role") or "").strip()
    city = str(ctx.get("city") or "").strip()
    industry = str(ctx.get("industry") or "").strip()
    intensity = str(ctx.get("intensity") or "").strip().lower()
    competitor_type = str(ctx.get("competitor_type") or "").strip().lower()

    # The angle is always a PLURAL noun phrase headed by "candidates" so every
    # skeleton can treat it as a plural subject/object without re-suffixing
    # ("...talent in Boston candidates have options" was the shipped bug).
    if role and city:
        angle = f"{role} candidates in {city}"
    elif role:
        angle = f"{role} candidates"
    elif city:
        angle = f"candidates in {city}"
    elif industry:
        angle = f"{industry} candidates"
    else:
        angle = "candidates in this pool"

    bank = _SKELETON_BANKS.get(competitor_type, _SKELETON_BANKS["default"])
    # ``ordinal`` (the competitor's position in the rendered list) selects
    # the skeleton DIRECTLY (idx = ordinal % len(bank)) rather than merely
    # offsetting a per-name hash. A hash-plus-offset scheme still let two
    # DIFFERENT competitors' independently-hashed base indices coincide
    # (observed: 5 competitors, hash bases collided such that positions 1
    # and 2 -- adjacent -- landed on the same skeleton despite the offset).
    # Pure ordinal indexing guarantees every position from 0..len(bank)-1
    # gets a distinct skeleton, which is what "adjacent competitors never
    # share a skeleton" actually requires; each bank now carries >=10
    # skeletons to cover the largest rendered competitor list (excel_v2's
    # Competitor Analysis table caps at 10 rows). Falls back to the
    # per-name hash only when no ordinal is supplied (caller doesn't know
    # its position in a list), preserving prior behavior for that case.
    ordinal = ctx.get("ordinal")
    if isinstance(ordinal, (int, float)) and not isinstance(ordinal, bool):
        idx = int(ordinal) % len(bank)
    else:
        idx = _pick_index(f"{name}|{role}|{city}", len(bank))
    sentence = bank[idx].format(competitor=name, angle=angle)

    if intensity in ("high", "aggressive", "elevated", "severe"):
        sentence += (
            f" {name} has been especially aggressive here recently — "
            "treat this as a priority lane."
        )
    return sentence


# ---------------------------------------------------------------------------
# Role requirement callouts
# ---------------------------------------------------------------------------
# Small, curated, non-fabricated regulatory notes. Empty list when nothing
# curated matches -- this is intentionally NOT a general-purpose generator.
def role_requirements_callout(industry: str, roles: list[str]) -> list[str]:
    """Curated regulatory/certification callouts for specific
    industry + role combinations. Factual and generic-regulatory only --
    never fabricates a number or a jurisdiction-specific claim."""
    industry_norm = (industry or "").strip().lower().replace("_", " ")
    roles_l = [str(r).strip().lower() for r in (roles or [])]
    callouts: list[str] = []

    is_fuel_logistics = any(
        kw in industry_norm
        for kw in ("propane", "fuel", "hazmat", "logistics", "trucking")
    )
    has_cdl_role = any(
        any(kw in r for kw in ("cdl", "driver", "truck")) for r in roles_l
    )
    if is_fuel_logistics and has_cdl_role:
        callouts.append(
            "Hazmat (H) and Tanker (N) endorsements required -- target "
            "endorsement-holding CDL pools"
        )

    is_healthcare = "healthcare" in industry_norm or "medical" in industry_norm
    has_nurse_role = any(
        any(kw in r for kw in ("nurse", "rn", "lpn", "nursing")) for r in roles_l
    )
    if is_healthcare and has_nurse_role:
        callouts.append(
            "Nursing roles require active state licensure -- prioritize "
            "compact-state license holders for multi-state mobility"
        )

    is_senior_living = any(
        kw in industry_norm
        for kw in ("senior living", "assisted living", "memory care")
    )
    has_caregiver_role = any(
        any(kw in r for kw in ("caregiver", "care giver", "cna")) for r in roles_l
    )
    if is_senior_living and has_caregiver_role:
        callouts.append(
            "Memory-care certification (or willingness to obtain) "
            "strengthens candidates for dementia/Alzheimer's-unit "
            "assignments"
        )

    return callouts


# ---------------------------------------------------------------------------
# Geography rationale
# ---------------------------------------------------------------------------
def geography_rationale(location: str, city_data: dict | None) -> str:
    """One honest sentence explaining why ``location`` was selected, built
    only from fields actually present in ``city_data``. Never fabricates a
    number -- falls back to a generic, honest sentence when no data is
    available."""
    loc = str(location or "").strip() or "This market"

    if not city_data or not isinstance(city_data, dict):
        return f"{loc} was selected by client footprint."

    parts: list[str] = []

    unemployment = city_data.get("unemployment")
    if unemployment is None:
        unemployment = city_data.get("unemployment_rate")
    if isinstance(unemployment, (int, float)) and not isinstance(unemployment, bool):
        parts.append(f"a {unemployment:g}% unemployment rate")

    difficulty = city_data.get("hiring_difficulty") or city_data.get("difficulty")
    if isinstance(difficulty, str) and difficulty.strip():
        parts.append(f"{difficulty.strip().lower()} hiring difficulty")

    supply_tier = city_data.get("supply_tier") or city_data.get("tier")
    if isinstance(supply_tier, str) and supply_tier.strip():
        parts.append(f"a {supply_tier.strip().lower()} talent-supply tier")

    salary_index = city_data.get("salary_index")
    if isinstance(salary_index, (int, float)) and not isinstance(salary_index, bool):
        parts.append(f"a salary index of {salary_index:g}")

    if not parts:
        return f"{loc} was selected by client footprint."

    if len(parts) == 1:
        detail = parts[0]
    elif len(parts) == 2:
        detail = f"{parts[0]} and {parts[1]}"
    else:
        detail = ", ".join(parts[:-1]) + f", and {parts[-1]}"

    return f"{loc} was prioritized based on {detail}."


__all__ = [
    "compose_counter_strategy",
    "role_requirements_callout",
    "geography_rationale",
]
