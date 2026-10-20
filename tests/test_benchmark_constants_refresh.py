"""Regression: benchmark constants refreshed with sources, internally
coherent, and single-valued per platform (audit 2026-10-01, F §2 / §3.7 /
§4.6-§4.8). Sources were re-fetched 2026-10-01:

* LocaliQ 2026 Search Advertising Benchmarks (updated 2026-06-01), Career &
  Employment: Google CPC $5.81 (its CPL $67.36 is held out as unverified --
  numbers verifier round 2; the Google CPA stays 48.0).
* LocaliQ 2026 Facebook Advertising Benchmarks (updated 2026-09-23), Career &
  Employment, leads objective: CPC $0.73, CPL $12.30, CVR 5.38%.
* SHRM 2025 Benchmarking press release (2025-10-15): nonexecutive average
  cost per hire $5,475.
* NSI 2026 National Health Care Retention & RN Staffing Report (March 2026):
  RN time to fill 78 days.
* ECB reference rates 2026-09-30 (via Frankfurter): USD per unit INR
  0.0104351, GBP 1.32864, EUR 1.13551, JPY 0.00636943, BRL 0.192208.

Pre-fix (f99beef) values: Google 2.90 / 48.0 (the 48.0 was WordStream's
2017-18 figure), Meta 1.86 / 32.0, COST_PER_HIRE overall 4750 labelled
"SHRM 2026", RN time to fill 30 days, FX INR 0.012 / GBP 1.27 / EUR 1.09 /
JPY 0.0067 / BRL 0.19 with no date; LinkedIn / Programmatic / CareerBuilder
stated CPAs 39-144% off their own cpc / apply_rate; channel_recommender
carried a second CPC table (Meta 1.20, Google 2.50, ZipRecruiter 0.95).
"""

from __future__ import annotations

import pytest

import benchmark_registry as br
import budget_engine as be
import channel_recommender as cr
import gold_standard as gs
import intl_benchmark_lookup as ibl
import ppt_generator as ppt


class TestRegistryRefresh:
    @pytest.mark.parametrize("key", ["google_ads", "google_search"])
    def test_google(self, key):
        # CPC refreshed; CPA unverified -- held at 48.0 (numbers verifier
        # round 2: the source's $67.36 CPL duplicates its Health & Fitness row)
        b = br.CHANNEL_BENCHMARKS[key]
        assert (b["cpc"], b["cpa"]) == (5.81, 48.0)

    @pytest.mark.parametrize("key", ["meta_facebook", "meta"])
    def test_meta(self, key):
        b = br.CHANNEL_BENCHMARKS[key]
        assert (b["cpc"], b["cpa"], b["apply_rate"]) == (0.73, 12.30, 0.0538)

    def test_overall_cost_per_hire_is_shrm_2025(self):
        assert br.COST_PER_HIRE["overall"] == 5475.0
        assert br.get_cost_per_hire("overall") == 5475.0  # live overlay too
        src = br._load_live_data()["industry_benchmarks"]["overall"][
            "avg_cost_per_hire_source"
        ]
        assert src.startswith("SHRM 2025 average (mean)"), src

    @pytest.mark.parametrize(
        "key", ["meta_facebook", "meta", "linkedin", "programmatic", "careerbuilder"]
    )
    def test_stated_cpa_matches_cpc_over_apply_rate(self, key):
        b = br.CHANNEL_BENCHMARKS[key]
        implied = b["cpc"] / b["apply_rate"]
        assert abs(b["cpa"] - implied) / implied <= 0.15, (key, b["cpa"], implied)


class TestOnePlatformOneCpc:
    def test_channel_recommender_uses_registry_cpcs(self):
        for platform, rkey in cr._REGISTRY_CPC_KEYS.items():
            assert cr._CHANNEL_CPC[platform] == br.get_channel_benchmark(rkey)["cpc"], (
                platform
            )


class TestTimeToFill:
    @pytest.mark.parametrize("title", ["Registered Nurse", "Staff Nurse"])
    def test_rn(self, title):
        assert gs._lookup_role_difficulty(title)["avg_ttf_days"] == 78


class TestApplyRates:
    @staticmethod
    def _gb(industry, role, city, state, collar=""):
        res = be.calculate_budget_allocation(
            total_budget=250_000.0,
            roles=[{"title": role, "count": 1, "tier": "Professional"}],
            locations=[{"city": city, "state": state, "country": "United States"}],
            industry=industry,
            channel_percentages={"global_boards": 60, "programmatic_dsp": 40},
            knowledge_base=None,
            collar_type=collar,
        )
        return res["channel_allocations"]["global_boards"]

    def test_healthcare_job_boards_use_kb_rate(self):
        # Restored 2026-10-18 (CPA-realism C1). The 2026-10-01 hold called the
        # healthcare rate unverified. The 3.22% (Appcast 2026, lowest of 15
        # occupational groups; tech 7.14%) rests on two SECONDARY quotes,
        # jobscore.com/articles/apply-rate and recruitingheadlines.com: the
        # primary report is gated and Appcast's public healthcare wording is
        # qualitative (appcast.io/the-rising-cost-of-recruiting prints tech
        # 7.14% and the 5.19% overall rate). The KB carries it as 3.2. The
        # held-against "$9.73 RN median" was the Welding row of
        # joveo_cpa_benchmarks_2026.json (RN: $82.57 gross). The KB healthcare
        # rate describes healthcare's mixed collar ("both").
        gb = self._gb("healthcare_medical", "Registered Nurse", "Dallas", "TX", "both")
        assert gb["apply_rate"] == 0.032 and gb["apply_rate_source"] == "kb_industry"

    def test_collar_uplift_still_applies_on_top(self):
        gb = self._gb(
            "healthcare_medical", "Housekeeper", "Dallas", "TX", "blue_collar"
        )
        assert gb["apply_rate"] == pytest.approx(0.032 * 1.4, abs=1e-4)

    def test_other_channels_move_by_the_same_factor(self):
        """Uniform re-level: channel ranking unchanged (programmatic's table
        rate is 0.06 vs job boards' 0.08; both scale by 0.032 / 0.08)."""
        res = be.calculate_budget_allocation(
            total_budget=250_000.0,
            roles=[{"title": "Registered Nurse", "count": 1, "tier": "Professional"}],
            locations=[{"city": "Dallas", "state": "TX", "country": "United States"}],
            industry="healthcare_medical",
            channel_percentages={"global_boards": 60, "programmatic_dsp": 40},
            knowledge_base=None,
            collar_type="both",
        )
        prog = res["channel_allocations"]["programmatic_dsp"]
        assert prog["apply_rate"] == pytest.approx(0.06 * 0.032 / 0.08, abs=1e-4)

    @pytest.mark.parametrize(
        "role", ["Registered Nurse", "Certified Nursing Assistant"]
    )
    @pytest.mark.parametrize("budget", [2_500, 9_000, 15_000, 28_000, 60_000, 250_000])
    def test_healthcare_relevel_moves_applications_and_cpa_only(
        self, monkeypatch, role, budget
    ):
        """The re-level is about APPLICATIONS and CPA. Hires, the hires range,
        cost per hire, the floor flag and the channel dollars must be exactly
        what the generic table rates give, at EVERY budget (the owner-level
        contract: hires = media budget / the assumed all-in industry cost per
        hire). When the funnel ran on the re-levelled rate a small plan's
        funnel fell under the floor-priced count and its hires went to 0."""

        def run():
            return be.calculate_budget_allocation(
                total_budget=float(budget),
                roles=[{"title": role, "count": 1, "tier": "Professional"}],
                locations=[
                    {"city": "Columbus", "state": "OH", "country": "United States"}
                ],
                industry="healthcare_medical",
                channel_percentages={
                    "global_boards": 30,
                    "niche_boards": 25,
                    "programmatic_dsp": 25,
                    "regional_boards": 12,
                    "social_media": 8,
                },
                knowledge_base=None,
                collar_type="both",
            )

        relevelled = run()
        monkeypatch.delitem(be._JOB_BOARD_APPLY_RATE_KB_KEY, "healthcare_medical")
        generic = run()
        r, g = relevelled["total_projected"], generic["total_projected"]
        for key in ("hires", "hires_low", "hires_high", "cost_per_hire", "clicks"):
            assert r[key] == g[key], (budget, role, key, r[key], g[key])
        assert (
            relevelled["metadata"]["cph_floor_applied"]
            == generic["metadata"]["cph_floor_applied"]
        )
        rc, gc = relevelled["channel_allocations"], generic["channel_allocations"]
        assert set(rc) == set(gc)
        for name in gc:
            for key in (
                "dollar_amount",
                "percentage",
                "projected_clicks",
                "projected_hires",
            ):
                assert rc[name][key] == gc[name][key], (budget, role, name, key)
        # ... and applications / CPA are the part that moved
        assert r["applications"] < g["applications"]
        assert r["cost_per_application"] > g["cost_per_application"]
        assert {c["apply_rate_source"] for c in rc.values()} == {"kb_industry"}

    def test_healthcare_relevel_is_us_only(self):
        # the KB rates are US (Appcast) figures: a non-US healthcare plan keeps
        # the generic table (intl_cpc_basis is set for non-US locale plans)
        res = be.calculate_budget_allocation(
            total_budget=250_000.0,
            roles=[{"title": "Registered Nurse", "count": 1, "tier": "Professional"}],
            locations=[{"city": "London", "state": "", "country": "United Kingdom"}],
            industry="healthcare_medical",
            channel_percentages={"global_boards": 100},
            knowledge_base=None,
            collar_type="both",
        )
        gb = res["channel_allocations"]["global_boards"]
        assert gb["apply_rate_source"] != "kb_industry"

    def test_trades_job_boards_use_kb_manufacturing_rate(self):
        gb = self._gb(
            "blue_collar_trades", "Machine Operator", "Cleveland", "OH", "blue_collar"
        )
        assert gb["apply_rate"] == 0.045 and gb["apply_rate_source"] == "kb_industry"

    def test_tech_unchanged(self):
        gb = self._gb("tech_engineering", "Software Developer", "Austin", "TX")
        assert gb["apply_rate_source"] == "base_table"


class TestFxRates:
    @pytest.mark.parametrize(
        "country,currency,rate",
        [
            ("India", "INR", 0.0104351),
            ("United Kingdom", "GBP", 1.32864),
            ("Germany", "EUR", 1.13551),
            ("France", "EUR", 1.13551),
            ("Japan", "JPY", 0.00636943),
            ("Brazil", "BRL", 0.192208),
        ],
    )
    def test_refreshed_rate_carries_its_as_of(self, country, currency, rate):
        basis = ibl.get_locale_cpc_basis([country], currency)
        assert basis["usd_per_local"] == rate
        assert basis["usd_rate_as_of"] == "2026-09-30"
        assert basis["usd_rate_source_short"] == "ECB"

    def test_deck_note_prints_the_rate_and_its_date(self):
        data = {
            "locations": ["Bangalore, India"],
            "budget": "₹20,000,000",
            "_budget_allocation": {
                "metadata": {
                    "industry_cph": {
                        "basis": "local_kb",
                        "fx": {
                            "usd_per_local": 0.0104351,
                            "as_of": "2026-09-30",
                            "source_short": "ECB",
                        },
                    }
                }
            },
        }
        ppt._set_active_currency(data)
        try:
            note = ppt._currency_basis_note(data)
        finally:
            ppt._set_active_currency({"locations": ["United States"]})
        assert "US$1 = ₹95.83 (ECB, 2026-09-30)" in note, note
