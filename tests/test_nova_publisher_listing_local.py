"""Nova's publisher listing follows the supply repository for the market.

2026-11-03 chat sweep: "best job boards in Poland for software developers"
listed the whole global Tech category (Technojobs, DEV.BG, Engineering
Careers.ca, Dice ...) because a category filter ignored the country; "best
job boards in UAE for nurses" listed only US healthcare boards; and a
country listing came back in file order instead of the local-relevance order
the media plan uses (channel_funding's score).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import channel_funding  # noqa: E402
import nova  # noqa: E402
import publisher_filter  # noqa: E402


@pytest.fixture(scope="module")
def nv():
    return nova.Nova()


def _in_network(name: str, country: str) -> bool:
    return (
        publisher_filter.board_supply_record(name, country)["state"]
        == publisher_filter.SUPPLY_IN_NETWORK
    )


def test_category_with_country_lists_only_boards_in_network_there(nv) -> None:
    res = nv._query_publishers({"country": "Poland", "category": "Tech"})
    pubs = res.get("publishers") or []
    assert pubs, res
    assert all(_in_network(p, "Poland") for p in pubs), pubs
    for foreign in ("Technojobs", "DEV.BG", "Engineering Careers.ca", "Dice"):
        assert foreign not in pubs
    assert res.get("country") == "Poland"


def test_category_with_no_local_board_says_so(nv) -> None:
    res = nv._query_publishers({"country": "United Arab Emirates", "category": "Health"})
    pubs = res.get("publishers") or []
    assert all(_in_network(p, "United Arab Emirates") for p in pubs), pubs
    assert "Jama Career Center" not in pubs and "IntelyCare" not in pubs
    if not pubs:
        assert "United Arab Emirates" in (res.get("note") or "")
        # the best general boards for the market are offered instead
        assert res.get("best_local_boards")


def test_country_listing_in_local_relevance_order(nv) -> None:
    res = nv._query_publishers({"country": "India"})
    pubs = res["publishers"]
    assert pubs == channel_funding.rank_names(list(pubs), "India")
    best = [b["name"] for b in channel_funding.funding_order({}, "India")[:3]]
    # the boards the plan would fund lead the chat list too
    lead = [p for p in pubs[:10]]
    assert any(publisher_filter.names_match(b, p) for b in best for p in lead), (best, lead)


def test_formatter_shows_note_and_best_boards_when_none_local() -> None:
    out = nova._format_publisher_response(
        {
            "publishers": [],
            "count": 0,
            "category": "Health",
            "country": "United Arab Emirates",
            "note": "No Health boards are confirmed in the Joveo network for United Arab Emirates; the best general boards there are listed instead.",
            "best_local_boards": ["Board A", "Board B"],
        }
    )
    assert "(0 publishers)" not in out
    assert "No Health boards are confirmed" in out
    assert "- Board A" in out and "- Board B" in out


def test_client_exclusions_apply_to_the_best_local_boards(nv) -> None:
    """Review 2026-11-03: the fallback list ignored client exclusions (UAE
    with Jooble excluded still led with Jooble)."""
    res = nv._query_publishers(
        {"country": "United Arab Emirates", "category": "Health", "excluded_publishers": ["Jooble"]}
    )
    best = res.get("best_local_boards") or []
    assert best, res
    assert not any(publisher_filter.names_match("Jooble", b) for b in best), best
