"""Which local boards the local-relevance score funds for Poland, the UAE
and India (handoff 2026-10-23 section 4 item 8).

Poland: Pracuj.pl, OLX.pl, No Fluff Jobs and Just Join IT are all in the
Joveo network for Poland. Just Join IT (posting + XML feed) is funded for a
tech plan; Pracuj.pl, OLX.pl and No Fluff Jobs are bought by posting with no
XML feed, so the plan names them under "post directly" (never programmatic
budget, by design). The benchmark dataset ranks OLX's jobs section third in
Poland as "OLX Praca"; the repository row is "OLX.pl", so the market-share
signal missed it until the two names were grouped
(data/publisher_availability.json alias group, source https://www.olx.pl/praca).

UAE and India: pinned to what the score funds today (2026-11-03) so a
change is deliberate.
"""

from __future__ import annotations

import channel_funding as cf
import publisher_filter as pf

POLAND_TECH = {"locations": ["Warsaw, Poland"], "target_roles": ["Software Engineer"]}
POLAND_WAREHOUSE = {"locations": ["Warsaw, Poland"], "target_roles": ["Warehouse Associate"]}
POLISH = ("Pracuj.pl", "OLX.pl", "No Fluff Jobs", "Just Join IT")


def _names(boards):
    return [b["name"] for b in boards]


def _rows(boards):
    """Repository row names (the printed name may be the dataset's label,
    e.g. OLX.pl prints as "OLX Praca")."""
    return [b.get("row_name") or b["name"] for b in boards]


def test_the_four_polish_boards_are_in_network_for_poland():
    for name in POLISH:
        assert pf.board_supply_record(name, "Poland")["state"] == pf.SUPPLY_IN_NETWORK, name


def test_a_poland_tech_plan_funds_just_join_it_and_names_the_rest_to_post():
    funded = _names(cf.funding_order(POLAND_TECH, "Poland")[: cf.FUNDED_BOARD_COUNT])
    assert "Just Join IT" in funded, funded
    post = _rows(cf.post_directly_boards(POLAND_TECH, "Poland"))
    for name in ("Pracuj.pl", "OLX.pl", "No Fluff Jobs"):
        assert name in post, (name, post)


def test_a_poland_warehouse_plan_posts_on_pracuj_and_olx():
    post = _rows(cf.post_directly_boards(POLAND_WAREHOUSE, "Poland"))
    assert set(post[:2]) == {"Pracuj.pl", "OLX.pl"}, post


def test_olx_pl_carries_the_market_share_signal_of_olx_praca():
    assert pf.names_match("OLX Praca", "OLX.pl")
    cands = {c["row_name"]: c for c in cf._score_candidates(POLAND_WAREHOUSE, "Poland")}
    assert "market leader" in cands["OLX.pl"]["why"], cands["OLX.pl"]["why"]


def test_uae_and_india_funded_boards():
    uae = {"locations": ["Dubai, United Arab Emirates"], "target_roles": ["Sales Executive"]}
    india = {"locations": ["Bangalore, India"], "target_roles": ["Software Engineer"]}
    uae_funded = _names(cf.funding_order(uae, "United Arab Emirates")[:5])
    india_funded = _names(cf.funding_order(india, "India")[:5])
    assert uae_funded == ["Jooble", "Sercanto", "Allthetopbananas", "Talent.com", "Jobsora"]
    assert india_funded[:3] == ["Shine.com", "Foundit (formerly Monster India)", "TimesJobs"]
    # the UAE's own market leaders are posting products: named, never funded
    uae_post = _names(cf.post_directly_boards(uae, "United Arab Emirates"))
    for name in ("Bayt.com", "dubizzle", "NaukriGulf"):
        assert name in uae_post, (name, uae_post)
