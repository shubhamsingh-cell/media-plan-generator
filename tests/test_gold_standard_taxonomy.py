"""Tests for the gold_standard.py data-quality remediation.

Covers:
    1. Role taxonomy expansion -- frontline/healthcare/logistics keyword
       coverage in classify_difficulty(), the "cook" -> executive/10.0
       substring-collision bug (and the token-boundary fix), and the
       "unclassified_default" -> Professional/~5.0 safety net.
    2. Salary fallback tier differentiation in enrich_city_level_data().
    3. _ordered_salary_band() percentile-ordering invariant.
    4. confidence_summary().
    5. effective_work_model() / ONSITE_REQUIRED_KEYWORDS.

No production code outside gold_standard.py is touched or imported here
beyond stdlib + gold_standard itself.
"""

from __future__ import annotations

import gold_standard as gs


# ---------------------------------------------------------------------------
# 1a. classify_difficulty -- role taxonomy coverage + the "cook" bug
# ---------------------------------------------------------------------------


def test_cook_is_not_misclassified_as_executive():
    """Regression test for the verified prod defect: 'cook' was classified
    Executive / 10.0 difficulty / 'retained search' because the seniority
    keyword "coo" (Chief Operating Officer) is a raw substring of "cook".
    """
    results = gs.classify_difficulty({"target_roles": ["Cook"]})
    assert len(results) == 1
    row = results[0]
    assert row["seniority_level"] != "executive"
    assert row["complexity_score"] < 9.0
    assert "retained search" not in row["description"].lower()
    assert row["role_profile_matched"] is True
    assert row["classification_source"] == "role_profile_match"


def test_coo_substring_collision_family_not_misclassified():
    """Any title that merely *contains* the "coo" substring (not as its own
    word) must not be pulled into the executive bucket by the seniority
    keyword fallback.
    """
    # "Scoop Technician" contains the raw substring "coo" (s-C-O-O-p) and
    # is not in _ROLE_DIFFICULTY_MAP, so it exercises the seniority-keyword
    # fallback path directly.
    results = gs.classify_difficulty({"target_roles": ["Scoop Technician"]})
    row = results[0]
    assert row["seniority_level"] != "executive"
    assert row["complexity_score"] < 9.0


def test_frontline_service_role_keyword_coverage():
    """Frontline/hourly service roles resolve to a real role-type profile
    (not the generic fallback) and never land in an executive/leadership
    tier.
    """
    titles = [
        "Cook",
        "Line Cook",
        "Dishwasher",
        "Housekeeper",
        "Housekeeping",
        "Server",
        "Waitstaff",
        "Caregiver",
        "Memory Care Aide",
        "Janitor",
        "Custodian",
    ]
    results = gs.classify_difficulty({"target_roles": titles})
    assert len(results) == len(titles)
    for row in results:
        assert row["role_profile_matched"] is True, row["role_title"]
        assert row["seniority_level"] not in ("director", "executive"), row
        assert row["complexity_score"] <= 6.0, row


def test_healthcare_role_keyword_coverage():
    """CNA / RN / LPN / nurse / charge nurse all resolve to a licensed
    healthcare profile, not the generic fallback.
    """
    titles = ["CNA", "RN", "LPN", "Nurse", "Charge Nurse", "Shift/Charge Nurse"]
    results = gs.classify_difficulty({"target_roles": titles})
    for row in results:
        assert row["role_profile_matched"] is True, row["role_title"]
        assert row["seniority_level"] != "executive", row


def test_logistics_role_keyword_coverage():
    """Driver / CDL driver / warehouse associate / forklift resolve to a
    role-type profile.
    """
    titles = [
        "Driver",
        "CDL Driver",
        "Delivery Driver",
        "Warehouse Associate",
        "Forklift",
    ]
    results = gs.classify_difficulty({"target_roles": titles})
    for row in results:
        assert row["role_profile_matched"] is True, row["role_title"]
        assert row["seniority_level"] != "executive", row


def test_maintenance_technician_keyword_coverage():
    results = gs.classify_difficulty({"target_roles": ["Maintenance Technician"]})
    row = results[0]
    assert row["role_profile_matched"] is True
    assert row["seniority_level"] != "executive"


# ---------------------------------------------------------------------------
# 1b. Unclassified-title safety net
# ---------------------------------------------------------------------------


def test_unmatched_title_defaults_to_professional_not_executive():
    """A bare title with no role-profile match and no seniority keyword
    match must default to the middle 'Professional' tier (~5.0 difficulty)
    -- never to executive/leadership or 10.0 difficulty.
    """
    results = gs.classify_difficulty(
        {"target_roles": ["Totally Unknown Widget Fabricator"]}
    )
    row = results[0]
    assert row["role_profile_matched"] is False
    assert row["classification_source"] == "unclassified_default"
    assert row["seniority_level"] != "executive"
    assert row["tier_label"] == "Professional"
    assert row["complexity_score"] == 5.0
    assert row["confidence"] == "estimated"


def test_seniority_keyword_match_still_works_for_real_titles():
    """A title with no role-type profile match (direct or fuzzy) but a
    genuine whole-word seniority keyword still classifies via the keyword
    fallback path -- title words are deliberately domain-nonsense so they
    can't collide with _ROLE_DIFFICULTY_MAP's fuzzy word-overlap fallback.
    """
    results = gs.classify_difficulty({"target_roles": ["Zylo Intern"]})
    row = results[0]
    assert row["role_profile_matched"] is False
    assert row["classification_source"] == "seniority_keyword_match"
    assert row["seniority_level"] == "intern"

    results2 = gs.classify_difficulty({"target_roles": ["Head of Zylo Fabrication"]})
    row2 = results2[0]
    assert row2["role_profile_matched"] is False
    assert row2["classification_source"] == "seniority_keyword_match"
    assert row2["seniority_level"] == "director"


def test_role_profile_match_carries_benchmark_confidence():
    results = gs.classify_difficulty({"target_roles": ["Software Engineer"]})
    row = results[0]
    assert row["confidence"] == "benchmark"
    assert row["classification_source"] == "role_profile_match"


# ---------------------------------------------------------------------------
# 2. Token-boundary matching helpers
# ---------------------------------------------------------------------------


def test_token_boundary_match_rejects_embedded_substring():
    assert gs._token_boundary_match("coo", "cook") is False
    assert gs._token_boundary_match("coo", "our new coo starts monday") is True
    assert gs._token_boundary_match("rn", "warn") is False
    assert gs._token_boundary_match("rn", "the rn on shift") is True


def test_match_token_alias_whole_word_only():
    assert gs._match_token_alias("rn") == "registered nurse"
    assert gs._match_token_alias("staff rn needed") == "registered nurse"
    assert gs._match_token_alias("warning label") is None  # not a standalone "rn" token
    assert gs._match_token_alias("lpn") == "licensed practical nurse"
    assert gs._match_token_alias("software engineer") is None


# ---------------------------------------------------------------------------
# 3. Salary fallback -- tier differentiation (S: dishwasher/housekeeper/
#    memory-care-aide were priced on byte-identical bands as nurses)
# ---------------------------------------------------------------------------


def _city_role_salary(role_titles: list[str], location: str = "New York, NY") -> dict:
    data = {"locations": [location], "target_roles": role_titles}
    city_data = gs.enrich_city_level_data(data)
    city_name = next(iter(city_data))
    return city_data[city_name]["per_role_salary"]


def test_nurse_dishwasher_housekeeper_no_longer_identical():
    per_role = _city_role_salary(
        ["Nurse", "Dishwasher", "Housekeeper", "Memory Care Aide"]
    )
    medians = {role: sal["median"] for role, sal in per_role.items()}
    # All four must resolve to *different* medians -- the original defect
    # priced them on byte-identical bands.
    assert len(set(medians.values())) == len(medians), medians
    # Licensed clinical role must clearly out-earn the frontline roles.
    assert medians["Nurse"] > medians["Dishwasher"]
    assert medians["Nurse"] > medians["Housekeeper"]
    assert medians["Nurse"] > medians["Memory Care Aide"]


def test_nurse_salary_is_realistic_not_halved():
    """Regression test: nurse median in NY was $45,540 (~half of a real RN
    salary) because bare 'nurse' fell through to the flat generic fallback.
    """
    per_role = _city_role_salary(["Nurse"], location="New York, NY")
    nurse_median = per_role["Nurse"]["median"]
    assert nurse_median >= 70_000, nurse_median
    assert per_role["Nurse"]["source"] == "Industry Benchmark"
    assert per_role["Nurse"]["confidence"] == "benchmark"


def test_generic_fallback_tier_multiplier_differentiates_unmatched_roles():
    """Roles with NO _ROLE_SALARY_RANGES keyword match (so they hit the
    generic_enrichment fallback) must still differentiate by tier instead
    of collapsing to one flat band per city.
    """
    per_role = _city_role_salary(
        ["Platform Engineer", "Warehouse", "Heavy Equipment Operator"],
        location="Chicago, IL",
    )
    for role, sal in per_role.items():
        assert sal["source"] == "generic_enrichment", (role, sal)
        assert sal["confidence"] == "estimated"

    medians = {role: sal["median"] for role, sal in per_role.items()}
    assert len(set(medians.values())) == len(medians), medians
    # professional (Platform Engineer, 1.0x) must out-earn frontline
    # (Warehouse, 0.55x) on the same generic city base.
    assert medians["Platform Engineer"] > medians["Warehouse"]


def test_per_role_salary_rows_satisfy_percentile_ordering():
    per_role = _city_role_salary(
        ["Nurse", "Dishwasher", "Platform Engineer", "Unclassified Widget Role"]
    )
    for role, sal in per_role.items():
        assert sal["min"] <= sal["p25"] <= sal["median"] <= sal["p75"] <= sal["max"], (
            role,
            sal,
        )


# ---------------------------------------------------------------------------
# 4. _lookup_role_tier
# ---------------------------------------------------------------------------


def test_lookup_role_tier_frontline():
    tier, source = gs._lookup_role_tier("Dishwasher")
    assert tier == "frontline"
    assert source == "role_profile_match"


def test_lookup_role_tier_licensed_clinical():
    tier, source = gs._lookup_role_tier("Registered Nurse")
    assert tier == "licensed_clinical"
    assert source == "role_profile_match"


def test_lookup_role_tier_skilled_trade():
    tier, source = gs._lookup_role_tier("Electrician")
    assert tier == "skilled_trade"
    assert source == "role_profile_match"


def test_lookup_role_tier_unclassified_defaults_professional():
    tier, source = gs._lookup_role_tier("Completely Made Up Job Title")
    assert tier == "professional"
    assert source == "unclassified_default"


# ---------------------------------------------------------------------------
# 5. _ordered_salary_band -- percentile ordering invariant
# ---------------------------------------------------------------------------


def test_ordered_salary_band_well_ordered_input_preserved():
    band = gs._ordered_salary_band(50_000, 60_000, 70_000, 80_000, 90_000)
    assert band == {
        "min": 50_000.0,
        "p25": 60_000.0,
        "median": 70_000.0,
        "p75": 80_000.0,
        "max": 90_000.0,
    }


def test_ordered_salary_band_fixes_p25_below_min_and_p75_above_max():
    """Regression test for the verified prod defect: P25 $75,440 < Min
    $92,000 and P75 > Max on nurse/shift-nurse/housekeeper rows.
    """
    band = gs._ordered_salary_band(
        min_v=92_000, p25_v=75_440, median_v=95_000, p75_v=130_000, max_v=100_000
    )
    assert band["min"] <= band["p25"] <= band["median"] <= band["p75"] <= band["max"]


def test_ordered_salary_band_inverted_percentiles_rederived_from_median():
    # p25 above median, p75 below median -- both must be re-derived.
    band = gs._ordered_salary_band(
        min_v=40_000, p25_v=999_000, median_v=70_000, p75_v=1, max_v=90_000
    )
    assert band["min"] <= band["p25"] <= band["median"] <= band["p75"] <= band["max"]


def test_ordered_salary_band_zero_median_collapses_to_zero():
    band = gs._ordered_salary_band(10_000, 20_000, 0, 40_000, 50_000)
    assert band == {"min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0}


def test_ordered_salary_band_handles_non_numeric_input():
    band = gs._ordered_salary_band("bad", None, 50_000, "also bad", -5)
    assert band["min"] <= band["p25"] <= band["median"] <= band["p75"] <= band["max"]
    assert band["median"] == 50_000.0


def test_ordered_salary_band_negative_median_collapses_to_zero():
    band = gs._ordered_salary_band(-10, -5, -1, 0, 5)
    assert band == {"min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0}


# ---------------------------------------------------------------------------
# 6. confidence_summary
# ---------------------------------------------------------------------------


def test_confidence_summary_counts_and_percentages():
    rows = [
        {"confidence": "benchmark"},
        {"confidence": "benchmark"},
        {"confidence": "estimated"},
        {"confidence": "estimated"},
        {"confidence": "estimated"},
        {"other": "no confidence key"},
    ]
    summary = gs.confidence_summary(rows)
    assert summary["total_rows"] == 6
    assert summary["benchmark_count"] == 2
    assert summary["estimated_count"] == 3
    assert summary["unclassified_count"] == 1
    assert summary["pct_benchmark"] == round(100 * 2 / 6, 1)
    assert summary["pct_estimated"] == round(100 * 3 / 6, 1)


def test_confidence_summary_empty_input():
    summary = gs.confidence_summary([])
    assert summary["total_rows"] == 0
    assert summary["pct_benchmark"] == 0.0
    assert summary["pct_estimated"] == 0.0


def test_confidence_summary_tolerates_malformed_rows():
    summary = gs.confidence_summary(
        [None, "not a dict", 42, {"confidence": "benchmark"}]
    )
    assert summary["total_rows"] == 1
    assert summary["benchmark_count"] == 1


def test_classify_difficulty_and_salary_rows_feed_confidence_summary():
    """End-to-end smoke: classify_difficulty() row confidences roll up
    sensibly through confidence_summary().
    """
    rows = gs.classify_difficulty(
        {"target_roles": ["Cook", "Software Engineer", "Totally Unknown Role"]}
    )
    summary = gs.confidence_summary(rows)
    assert summary["total_rows"] == 3
    assert summary["benchmark_count"] == 2  # Cook + Software Engineer
    assert summary["estimated_count"] == 1  # Totally Unknown Role


# ---------------------------------------------------------------------------
# 7. effective_work_model / ONSITE_REQUIRED_KEYWORDS
# ---------------------------------------------------------------------------


def test_effective_work_model_corrects_remote_for_care_roles():
    model, note = gs.effective_work_model("Remote", ["Caregiver"])
    assert model == "On-site"
    assert note is not None
    assert "on-site" in note.lower()


def test_effective_work_model_corrects_remote_for_culinary_roles():
    model, note = gs.effective_work_model("Remote", ["Line Cook"])
    assert model == "On-site"
    assert note is not None


def test_effective_work_model_leaves_genuinely_remote_roles_alone():
    model, note = gs.effective_work_model("Remote", ["Software Engineer"])
    assert model == "Remote"
    assert note is None


def test_effective_work_model_leaves_non_remote_models_alone():
    model, note = gs.effective_work_model("Hybrid", ["Cook"])
    assert model == "Hybrid"
    assert note is None

    model2, note2 = gs.effective_work_model("On-site", ["Nurse"])
    assert model2 == "On-site"
    assert note2 is None


def test_effective_work_model_handles_empty_stated_value():
    model, note = gs.effective_work_model("", ["Software Engineer"])
    assert model == "Not specified"
    assert note is None


def test_onsite_required_keywords_cover_the_named_roles():
    expected_subset = {
        "caregiver",
        "nurse",
        "cook",
        "housekeeper",
        "driver",
        "warehouse",
        "maintenance",
        "dishwasher",
        "server",
    }
    assert expected_subset.issubset(gs.ONSITE_REQUIRED_KEYWORDS)


# ---------------------------------------------------------------------------
# 8. Percentile ordering invariant against real _ROLE_SALARY_RANGES data
# ---------------------------------------------------------------------------


def test_all_role_salary_ranges_produce_ordered_bands():
    """Sweep every keyword in _ROLE_SALARY_RANGES through the same band
    construction enrich_city_level_data uses, confirming the invariant
    holds for the entire table (not just the specific defect rows).
    """
    for keyword, (lo, hi) in gs._ROLE_SALARY_RANGES.items():
        median = (lo + hi) / 2.0
        band = gs._ordered_salary_band(lo, median * 0.90, median, median * 1.12, hi)
        assert (
            band["min"] <= band["p25"] <= band["median"] <= band["p75"] <= band["max"]
        ), (
            keyword,
            band,
        )


def test_no_role_difficulty_entry_defaults_to_executive_tier_by_accident():
    """Sanity sweep: every explicit _ROLE_DIFFICULTY_MAP entry has a tier
    drawn from the known set (frontline/professional/licensed_clinical/
    skilled_trade) -- guards against a future entry being added without
    tier coverage and silently defaulting through _lookup_role_tier.
    """
    valid_tiers = {"frontline", "professional", "licensed_clinical", "skilled_trade"}
    for role, profile in gs._ROLE_DIFFICULTY_MAP.items():
        assert profile.get("tier") in valid_tiers, (role, profile.get("tier"))
