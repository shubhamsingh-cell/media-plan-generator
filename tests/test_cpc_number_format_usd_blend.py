"""Regression tests for DEFECT B (2026-09-24, adversarial review of an
unpushed release): ``excel_v2._is_localized`` / ``_cpc_number_format``
treated ANY ``cpc_source`` starting with "intl_" as a genuinely-localized
CPC figure -- including ``"intl_usd_blend:*"``
(``intl_benchmark_lookup.get_locale_cpc_basis``'s multi-country/
mismatched-currency basis), which is built entirely from the dataset's own
``cpc_usd`` figures and is NEVER converted into the plan's own currency.
A GBP plan spanning London + Sydney therefore rendered its USD-blend CPCs
(e.g. 1.22 / 4.34) with a "£" symbol as if they were real local numbers.

Fix: local only when ``cpc_source`` starts with "intl_local" (the basis IS
the plan's own currency) or contains "->" (an explicit, dataset-rate
conversion budget_engine's unit-coherence fix appends, e.g.
"synthesized->GBP"). Everything else on a non-USD plan renders with the
fixed-USD format (``FMT_USD2``), matching how the figure was actually
computed. This also routes "Recommended Channels (Vetted)" and "Channel
Recommendations" through the same per-channel ``_cpc_number_format`` (they
previously always used the active-currency format unconditionally), so
every CPC cell for the same channel carries the same symbol across sheets.

Runs under pytest, or standalone: ``python3 tests/test_cpc_number_format_usd_blend.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest  # noqa: E402

import excel_v2  # noqa: E402


@pytest.fixture(autouse=True)
def _gbp_active_currency():
    """Every test in this file runs as a non-USD (GBP) plan."""
    excel_v2._set_active_currency({"currency_code": "GBP"})
    yield
    excel_v2._set_active_currency({"currency_code": "USD"})


class TestCpcNumberFormatUsdBlend:
    def test_intl_usd_blend_source_is_not_localized(self):
        """The core defect: a multi-country USD-blend basis (e.g. a GBP
        plan spanning UK + Australia) must render with the fixed-USD
        format, not the plan's "£" symbol."""
        fmt = excel_v2._cpc_number_format({"cpc_source": "intl_usd_blend:uk,australia"})
        assert fmt == excel_v2.FMT_USD2
        assert "£" not in fmt

    def test_intl_local_source_is_localized(self):
        """A genuine single-market local basis (the market's own currency
        equals the plan's currency) is real local calibration -- keep the
        plan's own currency symbol."""
        fmt = excel_v2._cpc_number_format({"cpc_source": "intl_local:uk"})
        assert fmt == excel_v2._usd2_fmt()
        assert "£" in fmt

    def test_dataset_rate_converted_source_is_localized(self):
        """A "->CUR" suffix means budget_engine's unit-coherence fix
        actually divided by the dataset's own usd_per_local rate -- a real
        conversion, not a relabel. Keep the plan's own currency symbol."""
        fmt = excel_v2._cpc_number_format({"cpc_source": "synthesized->GBP"})
        assert fmt == excel_v2._usd2_fmt()
        assert "£" in fmt

    def test_unconverted_us_cascade_source_is_not_localized(self):
        """Plain US-cascade tiers (static_benchmark/live_benchmark/
        trend_engine/knowledge_base/synthesized, no intl/-> marker) were
        never converted -- must render fixed-USD."""
        for src in (
            "static_benchmark",
            "live_benchmark",
            "trend_engine",
            "knowledge_base",
            "synthesized",
            "",
        ):
            fmt = excel_v2._cpc_number_format({"cpc_source": src})
            assert fmt == excel_v2.FMT_USD2, f"cpc_source={src!r} got {fmt!r}"

    def test_usd_plan_always_uses_active_currency_format(self):
        """On a USD plan, active-currency format IS the USD format
        regardless of cpc_source -- no behavior change for US plans."""
        excel_v2._set_active_currency({"currency_code": "USD"})
        for src in ("intl_usd_blend:uk,australia", "static_benchmark", ""):
            fmt = excel_v2._cpc_number_format({"cpc_source": src})
            assert fmt == excel_v2._usd2_fmt()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
