"""Regression: a generation must not leave its plan currency on the thread.

generate_pptx / generate_excel_v2 store the plan currency in a
thread-local (``_currency_tls``) so every money figure renders in the
plan's own symbol. Before 2026-09-24 neither restored it on exit, so a GBP
plan left GBP behind on that thread:

* in tests, tests/test_ppt_geometry_matrix.py (which renders GBP decks)
  made six tests in tests/test_ppt_w3b_bundle_quality.py print "£25K"
  where they expect "$25K" whenever it ran first;
* in production, the threading HTTP server reuses request threads, so
  anything that later read the active currency on that thread without
  setting it first inherited the previous client's currency.

Contract: each generator restores the thread's PRIOR value on exit,
whether it returns or raises.
"""

from __future__ import annotations

import logging

import pytest

import excel_v2
import ppt_generator as ppt

_GBP_PLAN = {
    "client_name": "London NHS Trust",
    "industry": "healthcare_medical",
    "budget": "£250,000",
    "locations": ["London, UK"],
    "roles": ["Registered Nurse"],
}


@pytest.fixture(autouse=True)
def _clean_threads():
    for mod in (ppt, excel_v2):
        if hasattr(mod._currency_tls, "code"):
            del mod._currency_tls.code
    yield
    for mod in (ppt, excel_v2):
        if hasattr(mod._currency_tls, "code"):
            del mod._currency_tls.code


def _gbp():
    return {k: (list(v) if isinstance(v, list) else v) for k, v in _GBP_PLAN.items()}


class TestPptCurrencyScope:
    def test_gbp_deck_does_not_leave_gbp_on_thread(self):
        logging.disable(logging.CRITICAL)
        try:
            ppt.generate_pptx(_gbp())
        finally:
            logging.disable(logging.NOTSET)
        assert ppt._get_active_currency() == "USD"
        assert not hasattr(ppt._currency_tls, "code")

    def test_prior_value_is_restored_not_reset(self):
        ppt._currency_tls.code = "EUR"
        logging.disable(logging.CRITICAL)
        try:
            ppt.generate_pptx(_gbp())
        finally:
            logging.disable(logging.NOTSET)
        assert ppt._get_active_currency() == "EUR"

    def test_restored_when_generation_raises(self, monkeypatch):
        def _boom(data):
            ppt._set_active_currency(data)
            raise RuntimeError("mid-generation failure")

        monkeypatch.setattr(ppt, "_generate_pptx_scoped", _boom)
        with pytest.raises(RuntimeError):
            ppt.generate_pptx(_gbp())
        assert ppt._get_active_currency() == "USD"


class TestExcelCurrencyScope:
    def test_gbp_workbook_does_not_leave_gbp_on_thread(self):
        logging.disable(logging.CRITICAL)
        try:
            excel_v2.generate_excel_v2(_gbp())
        finally:
            logging.disable(logging.NOTSET)
        assert excel_v2._get_active_currency() == "USD"
        assert not hasattr(excel_v2._currency_tls, "code")

    def test_restored_when_generation_raises(self, monkeypatch):
        def _boom(data, *a, **k):
            excel_v2._set_active_currency(data)
            raise RuntimeError("mid-generation failure")

        monkeypatch.setattr(excel_v2, "_generate_excel_v2_scoped", _boom)
        with pytest.raises(RuntimeError):
            excel_v2.generate_excel_v2(_gbp())
        assert excel_v2._get_active_currency() == "USD"
