"""Envelope generator: ~40 decks stressing the corners of ppt_generator.

Every deck goes through the REAL pipeline -- ``budget_engine.
calculate_budget_allocation`` feeds ``data["_budget_allocation"]`` exactly as
app.py wires it -- and uses REAL channel keys (programmatic_dsp,
global_boards, niche_boards, regional_boards, social_media,
employer_branding, apac_regional, emea_regional). Display names are never
used: they silently fall through to ppt_generator's 3-channel fallback,
which would make every deck below identical and defeat the whole exercise.

Axes covered: channel count (1/2/4/6/8), a US-only vs. international-market
split (APAC/EMEA channels only apply off a US-only plan), an 80-char client
name, a 70-char role title, 0/3/6/12 synthesized competitors (short vs. long
names), micro ($5,000) and macro ($10,000,000) budgets, a typed non-USD
budget ("£2,000,000"), a bare-number budget needing market inference, 8 mixed
markets, 12 roles, two industries, an "Ongoing" duration, a 5-platform
synthesized deep-intelligence bundle (drives the slide-5 benchmark table AND
the funnel strip), the GBP+synthesized combo, and a kitchen-sink of
everything at once.

Usage:
    python3 scripts/deck_qa/matrix.py <outdir>

Prints a table of name / ok / bytes and writes ``<outdir>/NN_<shape>.pptx``.
"""

import argparse
import os
import sys

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import budget_engine  # noqa: E402
import ppt_generator as ppt  # noqa: E402

LONG_COMPETITORS = [
    "Universal Health Services Behavioral Division",
    "Encompass Health Rehabilitation Hospital Group",
    "Select Medical Critical Illness Recovery Holdings",
    "Kindred Healthcare Transitional Care Systems",
    "Genesis Healthcare Post-Acute Recovery Network",
    "Ernest Health Specialty Rehabilitation Group",
    "Life Care Centers of America Skilled Nursing Division",
    "HealthSouth Corporation Outpatient Therapy Systems",
    "Amedisys Home Health and Hospice Holdings",
    "LHC Group Community-Based Care Alliance",
    "National HealthCare Corporation Long-Term Care Systems",
    "Brookdale Senior Living Rehabilitation Services",
]
SHORT_COMPETITORS = ["HCA", "CVS", "Tenet", "Ascension", "Providence", "Kaiser"]
LONG_CLIENT = (
    "Consolidated Transcontinental Healthcare & Rehabilitation Partners International"
)
LONG_ROLE_TITLE = (
    "Senior Registered Nurse Clinical Care Coordinator and Patient Experience Lead"
)

CHANNEL_SETS = {
    1: ["programmatic_dsp"],
    2: ["programmatic_dsp", "social_media"],
    3: ["programmatic_dsp", "global_boards", "social_media"],
    4: ["programmatic_dsp", "global_boards", "niche_boards", "social_media"],
    5: [
        "programmatic_dsp",
        "global_boards",
        "niche_boards",
        "social_media",
        "regional_boards",
    ],
    6: [
        "programmatic_dsp",
        "global_boards",
        "niche_boards",
        "social_media",
        "regional_boards",
        "employer_branding",
    ],
    7: [
        "programmatic_dsp",
        "global_boards",
        "niche_boards",
        "social_media",
        "regional_boards",
        "employer_branding",
        "apac_regional",
    ],
    8: [
        "programmatic_dsp",
        "global_boards",
        "niche_boards",
        "social_media",
        "regional_boards",
        "employer_branding",
        "apac_regional",
        "emea_regional",
    ],
}


def _channel_pcts(n):
    keys = CHANNEL_SETS[n]
    pct = round(100.0 / len(keys), 2)
    pcts = {k: pct for k in keys}
    # rebalance rounding so the total is exactly 100
    drift = 100.0 - sum(pcts.values())
    pcts[keys[0]] += drift
    return pcts


def _channel_categories(n):
    return {k: True for k in CHANNEL_SETS[n]}


def _synth_competitors(names):
    if not names:
        return {}
    return {
        "competitive_intelligence": {
            "company_profile": {
                "name": "Mercy Health Partners",
                "industry_sector": "Healthcare",
                "employee_count": "25,000-50,000",
            },
            "competitors": {
                n: {"employee_count": "10,000+", "hiring_velocity": "high"}
                for n in names
            },
        }
    }


def _synth_deep_platforms():
    """5-platform deep-intelligence bundle + job market demand -- drives both
    the slide-5 benchmark table and the funnel strip."""
    platforms = {}
    long_names = [
        "Programmatic Demand-Side Platform Network",
        "Global Professional Job Board Consortium",
        "Niche Healthcare Talent Marketplace Alliance",
        "Regional Community Job Board Federation",
        "Employer Branding & Career Site Syndication Network",
    ]
    for i, name in enumerate(long_names, 1):
        platforms[f"platform_{i}"] = {
            "platform_name": name,
            "CPC": round(1.2 + i * 0.3, 2),
            "CPA": round(18 + i * 4, 2),
            "estimated_reach": 50000 * i,
            "fit_score": round(0.6 + i * 0.05, 2),
            "deep_intelligence": {
                "monthly_visitors": 1_000_000 * i,
                "best_for": [
                    "Registered Nurse",
                    "Clinical Coordinator",
                    "Care Manager",
                ],
            },
        }
    return {
        "ad_platform_analysis": platforms,
        "job_market_demand": {
            "Registered Nurse": {
                "total_postings": 12000,
                "avg_salary": 78000,
                "market_temperature": "hot",
                "posting_sources": ["Adzuna", "Jooble"],
            }
        },
    }


def _merge_synth(*parts):
    out = {}
    for p in parts:
        for k, v in p.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k].update(v)
            else:
                out[k] = v
    return out


def _base_plan(**over):
    data = {
        "client_name": "Mercy Health Partners",
        "industry": "healthcare",
        "industry_label": "Healthcare",
        "budget": "$150,000",
        "budget_period": "campaign",
        "campaign_duration": "3 months",
        "campaign_start_month": 9,
        "hire_volume": "90 hires",
        "work_environment": "onsite",
        "locations": [{"city": "Dallas", "state": "TX", "country": "United States"}],
        "roles": [{"title": "Registered Nurse", "count": 40, "tier": "mid"}],
        "target_roles": ["Registered Nurse"],
        "campaign_goals": ["volume_hiring"],
        "channel_categories": _channel_categories(4),
        "experience_level": "mid",
    }
    data.update(over)
    return data


def _with_budget_allocation(data, n_channels=4, collar_type="white"):
    """Run the REAL budget_engine call and attach the result, exactly the way
    app.py wires ``data["_budget_allocation"]`` before ppt_generator sees it."""
    roles = data.get("roles") or [
        {"title": "Registered Nurse", "count": 40, "tier": "mid"}
    ]
    locs_raw = data.get("locations") or []
    locations = []
    for loc in locs_raw:
        if isinstance(loc, dict):
            locations.append(
                {
                    "city": loc.get("city") or "",
                    "state": loc.get("state") or "",
                    "country": loc.get("country") or "",
                }
            )
        else:
            # best-effort "City, State, Country" split
            parts = [p.strip() for p in str(loc).split(",")]
            city = parts[0] if parts else ""
            country = parts[-1] if len(parts) > 1 else ""
            state = parts[1] if len(parts) > 2 else ""
            locations.append({"city": city, "state": state, "country": country})

    try:
        budget_val = float(
            str(data.get("budget", "0")).replace(",", "").lstrip("$£€¥").strip() or 0
        )
    except ValueError:
        budget_val = 150000.0

    try:
        result = budget_engine.calculate_budget_allocation(
            total_budget=budget_val,
            roles=roles,
            locations=locations,
            industry=data.get("industry", "healthcare"),
            channel_percentages=_channel_pcts(n_channels),
            synthesized_data=data.get("_synthesized"),
            collar_type=collar_type,
            campaign_start_month=data.get("campaign_start_month", 9),
        )
        data["_budget_allocation"] = result
    except (TypeError, ValueError, KeyError, ZeroDivisionError) as exc:
        print(
            f"  [matrix] budget_engine failed for a deck: {exc} -- omitting _budget_allocation"
        )
    return data


# ---------------------------------------------------------------------------
# The envelope
# ---------------------------------------------------------------------------
def build_envelope():
    decks = []

    def add(shape, n_channels=4, collar_type="white", **over):
        data = _base_plan(channel_categories=_channel_categories(n_channels), **over)
        data = _with_budget_allocation(
            data, n_channels=n_channels, collar_type=collar_type
        )
        decks.append((shape, data))

    # 1. control: healthcare Dallas USD baseline
    add("control_healthcare_dallas_usd")

    # 2. channel counts 1/2/4/6 (Dallas)
    for n in (1, 2, 4, 6):
        add(f"channels_{n}_dallas", n_channels=n)

    # 3. all-8 channels (London + Singapore)
    add(
        "channels_8_london_singapore",
        n_channels=8,
        locations=[
            {"city": "London", "state": "", "country": "United Kingdom"},
            {"city": "Singapore", "state": "", "country": "Singapore"},
        ],
    )

    # 4. 80-char client name
    add("client_name_80char", client_name=LONG_CLIENT)

    # 5. 70-char role title
    add(
        "role_title_70char",
        roles=[{"title": LONG_ROLE_TITLE, "count": 40, "tier": "mid"}],
        target_roles=[LONG_ROLE_TITLE],
    )

    # 6. competitors: 0 / 3 short / 6 short / 12 long
    add("competitors_0", _synthesized={})
    add("competitors_3_short", _synthesized=_synth_competitors(SHORT_COMPETITORS[:3]))
    add("competitors_6_short", _synthesized=_synth_competitors(SHORT_COMPETITORS))
    add("competitors_12_long", _synthesized=_synth_competitors(LONG_COMPETITORS))

    # 7. budgets: micro and macro
    add("budget_micro_5000", budget="$5,000")
    add("budget_macro_10000000", budget="$10,000,000")

    # 8. typed non-USD budget with London
    add(
        "budget_gbp_typed_london",
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
    )

    # 9. bare-number budget, market must infer currency
    add(
        "budget_bare_number_london",
        budget="2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
    )

    # 10. 8 mixed markets
    add(
        "markets_8_mixed",
        locations=[
            {"city": "Dallas", "state": "TX", "country": "United States"},
            {"city": "London", "state": "", "country": "United Kingdom"},
            {"city": "Singapore", "state": "", "country": "Singapore"},
            {"city": "Toronto", "state": "", "country": "Canada"},
            {"city": "Sydney", "state": "", "country": "Australia"},
            {"city": "Dublin", "state": "", "country": "Ireland"},
            {"city": "Berlin", "state": "", "country": "Germany"},
            {"city": "Tokyo", "state": "", "country": "Japan"},
        ],
    )

    # 11. 12 roles
    add(
        "roles_12",
        roles=[
            {"title": f"Registered Nurse Specialty {i}", "count": 10 + i, "tier": "mid"}
            for i in range(12)
        ],
    )

    # 12. two industries
    add(
        "industry_general_entry_level",
        industry="general_entry_level",
        industry_label="General",
    )
    add("industry_technology", industry="technology", industry_label="Technology")

    # 13. "Ongoing" duration
    add("duration_ongoing", campaign_duration="Ongoing")

    # 14. 5-platform deep synthesized bundle (funnel strip)
    add("synthesized_5platform_deep", _synthesized=_synth_deep_platforms())

    # 15. GBP + synthesized combo
    add(
        "gbp_plus_synthesized",
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
        _synthesized=_merge_synth(
            _synth_deep_platforms(), _synth_competitors(SHORT_COMPETITORS)
        ),
    )

    # 16. kitchen sink
    add(
        "kitchen_sink",
        n_channels=8,
        client_name=LONG_CLIENT,
        budget="£2,000,000",
        campaign_duration="Ongoing",
        roles=[
            {
                "title": LONG_ROLE_TITLE if i == 0 else f"Role {i}",
                "count": 10 + i,
                "tier": "mid",
            }
            for i in range(12)
        ],
        locations=[
            {"city": "London", "state": "", "country": "United Kingdom"},
            {"city": "Singapore", "state": "", "country": "Singapore"},
        ],
        _synthesized=_merge_synth(
            _synth_deep_platforms(), _synth_competitors(LONG_COMPETITORS)
        ),
    )

    # --- extra combinations, filling the envelope out toward ~40 ---
    add("channels_3_dallas", n_channels=3)
    add("channels_5_dallas", n_channels=5)
    add("channels_7_dallas", n_channels=7)
    add(
        "client_name_80char_gbp",
        client_name=LONG_CLIENT,
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
    )
    add(
        "competitors_12_long_gbp",
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
        _synthesized=_synth_competitors(LONG_COMPETITORS),
    )
    add(
        "competitors_6_short_technology",
        industry="technology",
        industry_label="Technology",
        _synthesized=_synth_competitors(SHORT_COMPETITORS),
    )
    add(
        "budget_micro_5000_gbp",
        budget="£5,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
    )
    add(
        "budget_macro_10000000_roles12",
        budget="$10,000,000",
        roles=[
            {"title": f"Role {i}", "count": 10 + i, "tier": "mid"} for i in range(12)
        ],
    )
    add(
        "markets_3_us_uk_sg",
        locations=[
            {"city": "Dallas", "state": "TX", "country": "United States"},
            {"city": "London", "state": "", "country": "United Kingdom"},
            {"city": "Singapore", "state": "", "country": "Singapore"},
        ],
    )
    add(
        "duration_ongoing_roles12",
        campaign_duration="Ongoing",
        roles=[
            {"title": f"Role {i}", "count": 10 + i, "tier": "mid"} for i in range(12)
        ],
    )
    add(
        "role_title_70char_gbp",
        roles=[{"title": LONG_ROLE_TITLE, "count": 40, "tier": "mid"}],
        target_roles=[LONG_ROLE_TITLE],
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
    )
    add(
        "bare_number_disagreeing_markets",
        budget="2,000,000",
        locations=[
            {"city": "Dallas", "state": "TX", "country": "United States"},
            {"city": "London", "state": "", "country": "United Kingdom"},
        ],
    )
    add(
        "industry_technology_gbp",
        industry="technology",
        industry_label="Technology",
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
    )
    add(
        "synthesized_5platform_deep_dallas_technology",
        industry="technology",
        industry_label="Technology",
        _synthesized=_synth_deep_platforms(),
    )
    add(
        "client_name_80char_roles12",
        client_name=LONG_CLIENT,
        roles=[
            {"title": f"Role {i}", "count": 10 + i, "tier": "mid"} for i in range(12)
        ],
    )
    add(
        "competitors_0_gbp",
        budget="£2,000,000",
        locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
        _synthesized={},
    )

    return decks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("outdir")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    decks = build_envelope()
    print(f"{'name':<45} {'ok':<5} {'bytes':>10}")
    for i, (shape, data) in enumerate(decks, 1):
        name = f"{i:02d}_{shape}"
        try:
            blob = ppt.generate_pptx(data)
            ok = True
            n_bytes = len(blob)
            with open(os.path.join(args.outdir, f"{name}.pptx"), "wb") as f:
                f.write(blob)
        except (
            TypeError,
            ValueError,
            KeyError,
            AttributeError,
            ZeroDivisionError,
        ) as exc:
            ok = False
            n_bytes = 0
            print(f"  [matrix] {name} FAILED: {exc}")
        print(f"{name:<45} {str(ok):<5} {n_bytes:>10}")


if __name__ == "__main__":
    main()
