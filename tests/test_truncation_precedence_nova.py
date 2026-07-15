"""nova.py follow-up: `X or [][:N]` operator-precedence no-ops.

Python precedence: slicing binds tighter than `or`, so
``rv.get("key_findings") or [][:3]`` slices the empty-list LITERAL
(``[][:3] == []``) and the intended cap is silently a no-op -- the
un-capped value flows straight through. Same defect class as
8fc9a4f (excel_v2.py / data_synthesizer.py); this pass covers the 7
occurrences verified in nova.py on 2026-07-15:

  1. Nova._query_white_papers            "top_findings" (was uncapped)
  2. Nova._query_external_benchmarks     "top_findings" (was uncapped)
  3. Nova._query_client_plans            industry-filter "roles" (was
     uncapped; only the `isinstance(..., list)` branch of the
     conditional expression was affected -- the dict-keys fallback
     branch already sliced correctly and is unchanged)
  4. Nova._query_client_plans            "key_patterns" (was uncapped)
  5. Nova._chat_rule_based               Guidewire "key_themes" (was
     uncapped, rendered straight into the chat response text)
  6. Nova._chat_rule_based               Guidewire theme "points" (was
     uncapped, rendered straight into the chat response text)
  7. _format_channel_response            "channels" (module-level
     formatter, was uncapped, rendered straight into chat text)

All seven are consumers that either feed an LLM tool-call result
(1-4, serialized via ``json.dumps`` back into the Nova conversation)
or build user-facing chat response text directly (5-7). Restoring the
cap shrinks output for over-cap inputs; nothing downstream in nova.py
depends on the uncapped length (grepped -- these keys aren't read
elsewhere), so this is a pure bug fix, not a behavior change other
code relies on.

Uses ``Nova.__new__(Nova)`` to get a real bound-method instance
without running the heavy ``__init__`` (KB load), matching the
pattern in tests/test_nova_keystone.py.

Runs under pytest, or standalone: ``python3 tests/test_truncation_precedence_nova.py``.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import nova  # noqa: E402


def _nova():
    """A real Nova instance without the heavy __init__ (KB load)."""
    return nova.Nova.__new__(nova.Nova)


# ===========================================================================
# Guard: the precedence pattern must not reappear in nova.py
# ===========================================================================
def test_slice_precedence_pattern_absent_from_nova():
    """`or [][...]` / `or ""[...]` / `or ''[...]` always slice the empty
    LITERAL -- any occurrence is a bug by construction. Guards nova.py,
    the file audited in this pass."""
    pattern = re.compile(r"""or\s+(""|''|\[\])\s*\[""")
    src = (PROJECT_ROOT / "nova.py").read_text(encoding="utf-8")
    hits = [
        f"nova.py:{i}"
        for i, line in enumerate(src.splitlines(), 1)
        if pattern.search(line)
    ]
    assert not hits, f"slice-binds-to-literal precedence bug at: {hits}"


# ===========================================================================
# Site 1: Nova._query_white_papers "top_findings" (was uncapped)
# ===========================================================================
def test_white_papers_top_findings_capped_at_three():
    n = _nova()
    n._data_cache = {
        "white_papers": {
            "reports": {
                "r1": {
                    "title": "Great Recruitment Report",
                    "publisher": "Pub",
                    "year": 2024,
                    "key_findings": [f"finding {i}" for i in range(10)],
                }
            }
        }
    }
    out = n._query_white_papers({"search_term": "great"})
    top = out["results"][0]["top_findings"]
    assert len(top) == 3, "the 3-item cap is still a no-op"
    assert top == ["finding 0", "finding 1", "finding 2"]
    # finding_count still reports the true (uncapped) total.
    assert out["results"][0]["finding_count"] == 10


def test_white_papers_top_findings_short_input_unchanged():
    n = _nova()
    n._data_cache = {
        "white_papers": {
            "reports": {
                "r1": {
                    "title": "Small Report",
                    "publisher": "Pub",
                    "year": 2024,
                    "key_findings": ["only one finding"],
                }
            }
        }
    }
    out = n._query_white_papers({"search_term": "small"})
    assert out["results"][0]["top_findings"] == ["only one finding"]


def test_white_papers_top_findings_missing_key_is_empty():
    n = _nova()
    n._data_cache = {
        "white_papers": {
            "reports": {"r1": {"title": "No Findings Report", "publisher": "Pub"}}
        }
    }
    out = n._query_white_papers({"search_term": "no findings"})
    assert out["results"][0]["top_findings"] == []


# ===========================================================================
# Site 2: Nova._query_external_benchmarks "top_findings" (was uncapped)
# ===========================================================================
def test_external_benchmarks_top_findings_capped_at_three():
    n = _nova()
    n._data_cache = {
        "external_benchmarks": {
            "reports": {
                "r1": {
                    "title": "Bench Report",
                    "publisher": "Pub",
                    "year": 2024,
                    "key_findings": [f"finding {i}" for i in range(10)],
                }
            },
            "aggregated_benchmarks": {},
        }
    }
    out = n._query_external_benchmarks({"search_term": "bench"})
    top = out["report_matches"][0]["top_findings"]
    assert len(top) == 3, "the 3-item cap is still a no-op"
    assert top == ["finding 0", "finding 1", "finding 2"]


def test_external_benchmarks_top_findings_none_key_is_empty():
    n = _nova()
    n._data_cache = {
        "external_benchmarks": {
            "reports": {
                "r1": {
                    "title": "Bench Report",
                    "publisher": "Pub",
                    "year": 2024,
                    "key_findings": None,
                }
            },
            "aggregated_benchmarks": {},
        }
    }
    out = n._query_external_benchmarks({"search_term": "bench"})
    assert out["report_matches"][0]["top_findings"] == []


# ===========================================================================
# Site 3: Nova._query_client_plans industry-filter "roles" (was uncapped)
# ===========================================================================
def _client_plans_cache(roles):
    return {
        "client_media_plans": {
            "plans": {
                "p1": {
                    "client": "Acme",
                    "industry": "logistics_supply_chain",
                    "regions": ["US"],
                    "roles": roles,
                    "hiring_volume": 100,
                }
            },
            "aggregate_patterns": {
                "total_unique_channels_identified": 50,
                "key_patterns": [],
            },
            "industries_covered": ["logistics_supply_chain"],
        }
    }


def test_client_plans_roles_list_capped_at_five():
    n = _nova()
    n._data_cache = _client_plans_cache([f"role{i}" for i in range(9)])
    out = n._query_client_plans({"industry": "logistics_supply_chain"})
    roles = out["matching_plans"][0]["roles"]
    assert len(roles) == 5, "the 5-item cap on the list branch is still a no-op"
    assert roles == ["role0", "role1", "role2", "role3", "role4"]


def test_client_plans_roles_short_list_unchanged():
    n = _nova()
    n._data_cache = _client_plans_cache(["CDL Driver", "Warehouse Associate"])
    out = n._query_client_plans({"industry": "logistics_supply_chain"})
    roles = out["matching_plans"][0]["roles"]
    assert roles == ["CDL Driver", "Warehouse Associate"]


def test_client_plans_roles_missing_key_is_empty():
    n = _nova()
    cache = _client_plans_cache(None)
    # Drop the key entirely rather than leaving it None, to also exercise
    # the isinstance(..., list) -> False fallback path (dict-keys branch),
    # which was never affected by the precedence bug and must stay intact.
    del cache["client_media_plans"]["plans"]["p1"]["roles"]
    n._data_cache = cache
    out = n._query_client_plans({"industry": "logistics_supply_chain"})
    roles = out["matching_plans"][0]["roles"]
    assert roles == [], "missing roles should resolve to the empty dict-keys fallback"


def test_client_plans_roles_dict_fallback_branch_unaffected():
    """The `isinstance(pv.get("roles"), list)` False branch
    (`list(pv.get("roles", {}).keys())[:5]`) never had the precedence bug
    and must still cap correctly -- guards against a regression while
    fixing the sibling True branch."""
    n = _nova()
    roles_dict = {f"role{i}": "details" for i in range(9)}
    n._data_cache = _client_plans_cache(roles_dict)
    out = n._query_client_plans({"industry": "logistics_supply_chain"})
    roles = out["matching_plans"][0]["roles"]
    assert len(roles) == 5
    assert roles == [f"role{i}" for i in range(5)]


# ===========================================================================
# Site 4: Nova._query_client_plans "key_patterns" (was uncapped)
# ===========================================================================
def test_client_plans_key_patterns_capped_at_five():
    n = _nova()
    n._data_cache = _client_plans_cache(["Driver"])
    n._data_cache["client_media_plans"]["aggregate_patterns"]["key_patterns"] = [
        f"pattern{i}" for i in range(9)
    ]
    out = n._query_client_plans({})
    kp = out["key_patterns"]
    assert len(kp) == 5, "the 5-item cap is still a no-op"
    assert kp == [f"pattern{i}" for i in range(5)]


def test_client_plans_key_patterns_missing_is_empty():
    n = _nova()
    n._data_cache = _client_plans_cache(["Driver"])
    # aggregate_patterns has no "key_patterns" key at all.
    del n._data_cache["client_media_plans"]["aggregate_patterns"]["key_patterns"]
    out = n._query_client_plans({})
    assert out["key_patterns"] == []


# ===========================================================================
# Sites 5 & 6: Nova._chat_rule_based Guidewire "key_themes" / "points"
# (were uncapped, rendered straight into the chat response text)
# ===========================================================================
def _guidewire_cache(theme_count, points_per_theme):
    return {
        "linkedin_guidewire": {
            "executive_summary": {
                "headline": "Guidewire hiring is trending up",
                "key_themes": [
                    {
                        "theme": f"Theme {i}",
                        "points": [f"point{i}-{j}" for j in range(points_per_theme)],
                    }
                    for i in range(theme_count)
                ],
            },
            "document_metadata": {"peer_companies": []},
        }
    }


def test_guidewire_key_themes_capped_at_three():
    n = _nova()
    n._data_cache = _guidewire_cache(theme_count=6, points_per_theme=1)
    out = n._chat_rule_based("Tell me about guidewire linkedin hiring")
    resp = out["response"]
    theme_headers = [line for line in resp.split("\n") if line.startswith("*Theme")]
    assert len(theme_headers) == 3, "the 3-theme cap is still a no-op"
    assert theme_headers == ["*Theme 0*", "*Theme 1*", "*Theme 2*"]
    # Theme 3+ must not appear anywhere in the response.
    assert "Theme 5" not in resp


def test_guidewire_theme_points_capped_at_three():
    n = _nova()
    n._data_cache = _guidewire_cache(theme_count=1, points_per_theme=6)
    out = n._chat_rule_based("Tell me about guidewire linkedin hiring")
    resp = out["response"]
    point_lines = [line for line in resp.split("\n") if line.startswith("- point0-")]
    assert len(point_lines) == 3, "the 3-point cap is still a no-op"
    assert "point0-5" not in resp


def test_guidewire_short_input_unchanged():
    n = _nova()
    n._data_cache = _guidewire_cache(theme_count=1, points_per_theme=1)
    out = n._chat_rule_based("Tell me about guidewire linkedin hiring")
    resp = out["response"]
    assert "*Theme 0*" in resp
    assert "- point0-0" in resp


def test_guidewire_missing_key_themes_is_empty():
    n = _nova()
    n._data_cache = {
        "linkedin_guidewire": {
            "executive_summary": {"headline": "Guidewire hiring intel"},
            "document_metadata": {"peer_companies": []},
        }
    }
    out = n._chat_rule_based("Tell me about guidewire linkedin hiring")
    resp = out["response"]
    assert "*Theme" not in resp


# ===========================================================================
# Site 7: _format_channel_response "channels" (was uncapped)
# ===========================================================================
def test_format_channel_response_niche_channels_capped_at_twelve():
    data = {
        "niche_industry_channels": {
            "industry": "logistics",
            "channels": [f"chan{i}" for i in range(20)],
        }
    }
    out = nova._format_channel_response(data, "logistics")
    channel_lines = [line for line in out.split("\n") if line.startswith("- chan")]
    assert len(channel_lines) == 12, "the 12-item cap is still a no-op"
    assert channel_lines[-1] == "- chan11"
    assert "chan19" not in out


def test_format_channel_response_niche_channels_short_input_unchanged():
    data = {
        "niche_industry_channels": {
            "industry": "logistics",
            "channels": ["chan0", "chan1"],
        }
    }
    out = nova._format_channel_response(data, "logistics")
    assert "- chan0" in out
    assert "- chan1" in out


def test_format_channel_response_missing_channels_is_empty():
    data = {"niche_industry_channels": {"industry": "logistics"}}
    out = nova._format_channel_response(data, "logistics")
    channel_lines = [line for line in out.split("\n") if line.startswith("- chan")]
    assert channel_lines == []


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc}")
    if _failures:
        sys.exit(1)
