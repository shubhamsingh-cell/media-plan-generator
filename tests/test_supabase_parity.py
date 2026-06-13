"""Tests for supabase_parity (S89 #9 dual-read parity audit).

The diff logic is pure; the I/O goes through supabase_data internals, which we
monkeypatch so everything stays offline + deterministic.
"""

import supabase_data as sd
import supabase_parity as sp


# ── pure diff_rows ──────────────────────────────────────────────────────────
def test_diff_rows_in_parity():
    rows = [{"channel": "Indeed", "industry": "tech", "cpc": 1.5, "cpa": 20.0, "pricing_model": "cpc"}]
    out = sp.diff_rows(rows, list(rows), ["channel", "industry"], ["cpc", "cpa", "pricing_model"])
    assert out["matched"] == 1
    assert out["only_supabase"] == 0 and out["only_json"] == 0
    assert out["value_mismatches"] == 0


def test_diff_rows_value_mismatch_and_float_tolerance():
    sup = [{"channel": "Indeed", "industry": "tech", "cpc": 1.5000001, "cpa": 20.0}]
    jsn = [{"channel": "indeed", "industry": "TECH", "cpc": 1.5, "cpa": 25.0}]  # case-insensitive key match
    out = sp.diff_rows(sup, jsn, ["channel", "industry"], ["cpc", "cpa"])
    assert out["matched"] == 1  # keys match case-insensitively
    assert out["value_mismatches"] == 1  # cpa differs; cpc within float tolerance
    assert out["value_mismatch_sample"][0]["fields"].get("cpa")
    assert "cpc" not in out["value_mismatch_sample"][0]["fields"]


def test_diff_rows_only_sides():
    sup = [{"channel": "A", "industry": "x"}, {"channel": "B", "industry": "x"}]
    jsn = [{"channel": "B", "industry": "x"}, {"channel": "C", "industry": "x"}]
    out = sp.diff_rows(sup, jsn, ["channel", "industry"], [])
    assert out["matched"] == 1 and out["only_supabase"] == 1 and out["only_json"] == 1


# ── verdict classification ──────────────────────────────────────────────────
def test_verdict_supabase_only():
    assert sp._verdict({"supabase_count": 5, "json_count": 0}) == "supabase_only"


def test_verdict_json_only():
    assert sp._verdict({"supabase_count": 0, "json_count": 5}) == "json_only"


def test_verdict_in_parity_vs_diverged():
    assert sp._verdict({"supabase_count": 10, "json_count": 10, "matched": 10, "value_mismatches": 0}) == "in_parity"
    assert sp._verdict({"supabase_count": 10, "json_count": 10, "matched": 2, "value_mismatches": 0}) == "diverged"


# ── audit_domain (mocked I/O) ───────────────────────────────────────────────
def test_audit_domain_in_parity(monkeypatch):
    row = {"channel": "Indeed", "industry": "tech", "cpc": 1.5, "cpa": 20.0, "pricing_model": "cpc"}
    monkeypatch.setattr(sd, "_query_supabase", lambda table, params: [dict(row)])
    monkeypatch.setattr(sd, "_fallback_channel_benchmarks", lambda c="", i="": [dict(row)])
    out = sp.audit_domain("channel_benchmarks")
    assert out["domain"] == "channel_benchmarks"
    assert out["verdict"] == "in_parity"
    assert out["matched"] == 1 and out["value_mismatches"] == 0


def test_audit_domain_supabase_only_is_flagged(monkeypatch):
    # salary_data's JSON fallback returns [] -> single point of failure
    monkeypatch.setattr(sd, "_query_supabase", lambda table, params: [{"role": "RN", "location": "TX"}])
    monkeypatch.setattr(sd, "_fallback_salary_data", lambda r="", l="": [])
    out = sp.audit_domain("salary_data")
    assert out["verdict"] == "supabase_only"
    assert "single point of failure" in out["warning"]


def test_audit_domain_never_raises_on_supabase_error(monkeypatch):
    def _boom(table, params):
        raise RuntimeError("network down")
    monkeypatch.setattr(sd, "_query_supabase", _boom)
    monkeypatch.setattr(sd, "_fallback_channel_benchmarks", lambda c="", i="": [])
    out = sp.audit_domain("channel_benchmarks")
    assert out["verdict"] == "no_data"
    assert "network down" in out.get("supabase_error", "")


def test_audit_domain_unknown():
    assert sp.audit_domain("nope")["verdict"] == "error"


# ── aggregate ───────────────────────────────────────────────────────────────
def test_run_parity_audit_aggregate(monkeypatch):
    monkeypatch.setattr(sd, "_query_supabase", lambda table, params: [{"channel": "A", "industry": "x", "cpc": 1, "cpa": 2, "pricing_model": "cpc"}])
    monkeypatch.setattr(sd, "_fallback_channel_benchmarks", lambda c="", i="": [{"channel": "A", "industry": "x", "cpc": 1, "cpa": 2, "pricing_model": "cpc"}])
    monkeypatch.setattr(sd, "_fallback_salary_data", lambda r="", l="": [])
    out = sp.run_parity_audit(["channel_benchmarks", "salary_data"])
    assert out["domains_audited"] == 2
    assert "channel_benchmarks" not in out["supabase_only_domains"]
    assert "salary_data" in out["supabase_only_domains"]
    assert out["verdict_counts"].get("in_parity") == 1
    assert out["cutover_ready"] is True  # only diffable domain (channel) is in parity
