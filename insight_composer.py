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
#
# uber_shipped_2026_07_23 fix (unsourced-competitor-claim wave): the shipped
# Uber bundle asserted specific, never-observed hiring BEHAVIOUR about named
# third parties as fact -- "Marriott is actively competing for commercial cab
# driver candidates in UK", "Expect Hilton to keep pressure on...". No
# enrichment backs any of that; the only thing actually known about a
# fallback-sourced competitor is that it is a real employer of the relevant
# TYPE (staffing agency / direct employer / gig platform) in this space, so
# it is a *plausible* competitor for the same talent pool -- a presence/
# capability claim that is true regardless of what this specific employer
# is or isn't doing right now. Every skeleton below leads with that framing
# ("{competitor} is a <type> ... and a plausible/likely competitor for
# {angle}") instead of asserting a specific action, and keeps the advice
# half of each sentence (counter-strategies are recommendations, not
# third-party claims) reworded to be unconditional -- it does not depend on
# the named competitor actually having done the thing the old sentence
# asserted.
_SKELETON_BANKS: dict[str, tuple[str, ...]] = {
    "staffing_agency": (
        "{competitor} operates as a staffing agency and is a plausible "
        "competitor for {angle} — compress time-to-offer so direct-hire "
        "terms can win before a third-party placement locks a candidate in.",
        "{competitor} is a staffing agency that can route {angle} through "
        "contract-to-hire channels; lead with permanent-role stability and "
        "a clear total-comp comparison as the counter.",
        "{competitor} is a staffing agency, and agency pipelines for "
        "{angle} can move fast — a same-week interview slot and "
        "pre-approved offer band are the clearest levers to beat one to "
        "the candidate.",
        "{competitor} is a plausible staffing-agency competitor for "
        "{angle} — tighten the direct-hire funnel with pre-screened "
        "shortlists and a 48-hour offer turnaround to beat a typical "
        "agency placement cycle.",
        "{competitor} is a staffing agency, and agency placements "
        "typically compete on availability, not loyalty — a standing "
        "direct-hire offer with no re-application step converts "
        "agency-sourced candidates before their next assignment.",
        "{competitor} is a staffing agency that can place {angle} on "
        "short-term assignments — position this role's permanence and "
        "benefits eligibility as the upgrade path out of temp work.",
        "{competitor} is a staffing agency, and agency markup on {angle} "
        "placements typically leaves room to compete on take-home pay "
        "alone — lead with a transparent, higher net-pay comparison.",
        "{competitor} is a staffing agency, and candidates placed through "
        "an agency often don't get a real career conversation — a named "
        "hiring manager and a clear growth track are the differentiator "
        "here.",
        "{competitor} is a staffing agency, and agencies typically "
        "re-market the same {angle} pool across multiple clients — speed "
        "to first contact after application determines who gets there "
        "first.",
        "{competitor} is a staffing agency, and agencies typically earn a "
        "fee per {angle} placement regardless of fit — expect volume over "
        "precision, and win on a tighter, better-matched shortlist "
        "instead.",
    ),
    "direct_employer": (
        "{competitor} is a direct employer in this space and a plausible "
        "competitor for {angle} — brand and total-comp clarity matter more "
        "than speed alone, so lead with a concrete growth-path story.",
        "{competitor} is a direct employer and a plausible peer competitor "
        "for {angle} — a faster interview-to-offer cycle is the clearest "
        "lever against a similarly positioned employer.",
        "{competitor} is a well-known employer brand in this market, "
        "visible to {angle} — differentiate on schedule flexibility or "
        "sign-on incentives where base comp is comparable.",
        "{competitor} is a plausible direct-employer competitor for "
        "{angle} — win on process: a single-visit interview loop and a "
        "same-week offer close the gap a bigger brand name can otherwise "
        "cover.",
        "{competitor} is a likely competitor for {angle} on name "
        "recognition alone — a candidate-facing comp calculator makes the "
        "pay comparison concrete instead of assumed.",
        "{competitor} is a direct employer, and a multi-round hiring "
        "process is common at that scale — a compressed two-touch loop "
        "wins candidates unwilling to wait out a slower process.",
        "{competitor} is a plausible competitor for the same {angle} pool "
        "— referral incentives and manager-led outreach reach candidates "
        "before a generic job-board listing does.",
        "{competitor} is a direct employer for {angle} — an always-on "
        "requisition with rolling interviews captures candidates during "
        "any gap between a competitor's postings.",
        "{competitor} is a direct employer, and tenure-based retention "
        "pitches are common among peers at that scale — counter with a "
        "faster path to responsibility for candidates who don't want to "
        "wait years for it.",
        "{competitor} is a plausible competitor for {angle}; base pay may "
        "be comparable, but schedule flexibility is not guaranteed — make "
        "flexibility the headline, not a footnote.",
    ),
    "gig_platform": (
        "{competitor} is a gig-work platform and a plausible draw for "
        "{angle} toward flexible, app-based work — lead with schedule "
        "control and guaranteed hours to counter the platform's "
        "flexibility pitch.",
        "{competitor} is a gig-work platform, and time-to-first-shift is "
        "typically fast on that model for {angle} — match it with a "
        "same-day onboarding path where possible.",
        "{competitor} is a gig-work platform whose model can appeal to "
        "{angle} — benefits and a fixed schedule are the counter-offer "
        "for those who want predictability instead.",
        "{competitor} is a gig-work platform and a plausible competitor "
        "for {angle} — emphasize what the platform can't offer: benefits "
        "eligibility, career progression, and shift predictability.",
        "{competitor} is a gig-work platform: instant sign-up but no "
        "guaranteed income floor is standard for that model — a stated "
        "minimum weekly pay is the concrete counter for {angle}.",
        "{competitor} is a gig-work platform, and self-scheduling around "
        "thin demand is typical for {angle} on that model — a set roster "
        "with reliable hours reads as the more stable option.",
        "{competitor} is a gig-work platform, and per-task pay on that "
        "model has no ceiling on downside for {angle} — a guaranteed "
        "hourly floor plus upside removes that risk.",
        "{competitor} is a gig-work platform, and gig work generally sees "
        "candidates churn back to traditional roles within weeks — time "
        "outreach to former gig workers who already know the gap.",
        "{competitor} is a gig-work platform, and that model gives "
        "{angle} no path to a W-2 role — make the conversion-to-permanent "
        "option explicit and easy to find.",
        "{competitor} is a plausible gig-platform competitor for {angle} "
        "— sign-on plus a first-week pay guarantee beats a platform's "
        "pay-per-task uncertainty.",
    ),
    "default": (
        "{competitor} is a plausible competitor for {angle} — sharpen "
        "offer cadence and speed-to-contact to stay ahead of it.",
        "{competitor} is a plausible competitor for {angle}; a faster "
        "interview-to-offer cycle is the clearest lever regardless.",
        "{competitor}'s presence in this pool means {angle} "
        "have options — lead with total-comp clarity and a same-week "
        "interview slot.",
        "{competitor} is a plausible competitor for {angle} — tighten the "
        "funnel with pre-screened shortlists and a 48-hour offer "
        "turnaround to out-compete it.",
        "{competitor} is a known name to {angle} in this market — a "
        "specific, verifiable comp figure beats a generic brand "
        "impression.",
        "{competitor} is a plausible competitor for {angle} — "
        "first-contact speed alone can decide who gets the candidate.",
        "{competitor}'s reputation with {angle} is untested here — "
        "candidate reviews and a named team contact build trust faster "
        "than brand alone.",
        "{competitor} is a plausible competitor for {angle} — a same-week "
        "site visit or shadow shift gives candidates something a job "
        "posting can't.",
        "{competitor} is a plausible competitor on the same {angle} "
        "channels this plan targets — differentiated creative on those "
        "same channels avoids losing the impression entirely.",
        "{competitor} is a plausible competitor for {angle} — keep the "
        "offer window short, since a candidate weighing two open offers "
        "usually takes the one that resolves first.",
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
        # uber_shipped_2026_07_23 fix: "{name} has been especially
        # aggressive here recently" asserted specific, never-observed
        # recent behaviour by the named competitor. The only thing actually
        # known is this lane's own intensity classification -- state that
        # instead of a claim about what the named company has been doing.
        sentence += (
            " This lane is flagged high-intensity — treat it as a "
            "priority for offer speed."
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
