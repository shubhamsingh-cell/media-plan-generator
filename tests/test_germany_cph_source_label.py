"""Regression: the Germany local cost-per-hire source is labelled as its page
states it (numbers verifier round 2, 2026-10-01, item 4).

Pre-fix (branch at bef6754): the €6,000-€25,000 Eurojob-Consulting range
was labelled "Eurojob-Consulting 2025 (Germany, all industries)" on the
deck's sources line and in the workbook, while the cited page (re-fetched
2026-10-01) says: "The total cost of hiring one employee ranges from €6,000
to €25,000, depending on the seniority, location, and industry." The page
carries no date either.
"""

from __future__ import annotations

import intl_benchmark_lookup as ibl
import budget_engine as be

_LABEL = "Eurojob-Consulting, Germany: varies by seniority, location and industry"


def test_lookup_names_the_scope_the_page_states():
    local = ibl.get_local_cph_benchmark("technology", "germany", "EUR", 1.13551)
    assert local is not None
    assert local["source_names"] == [_LABEL], local["source_names"]
    assert "all industries" not in " ".join(local["source_names"])


def test_resolver_carries_the_label_to_deck_and_workbook():
    cph = be.resolve_industry_cph(
        "tech_engineering",
        usd_per_local=1.13551,
        plan_currency="EUR",
        intl_cpc_basis={"basis": "local", "matched_countries": ["germany"]},
    )
    assert cph["basis"] == "local_kb"
    assert cph["source_names"] == [_LABEL]
