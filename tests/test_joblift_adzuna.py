"""Joblift: insolvency (2026-04-07) and Adzuna asset purchase (2026-07-16).

Source: https://www.online-recruiting.net/adzuna-uebernimmt-joblift-die-totgesagte-marke-bekommt-ein-zweites-leben
(online-recruiting.net, 2026-07): Joblift GmbH filed for preliminary
insolvency on 7 Apr 2026; Adzuna bought the assets (brand, platform,
customer contacts) on 16 Jul 2026. joblift.de runs on as its own brand in
Germany; in the UK, US, Belgium, Italy, the Netherlands and France the
operation moves to the Adzuna platform.

Decision: joblift.de stays fundable in Germany while the site is live, but
every surface that names it says "now part of Adzuna, insolvency 2026",
the local-relevance score treats Joblift and Adzuna as one group (a plan
never funds both as independent boards), and outside Germany Joblift is a
folded board operated by Adzuna (never listed or funded on its own).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import channel_funding
import publisher_filter

ROOT = Path(__file__).resolve().parent.parent
SOURCE = (
    "https://www.online-recruiting.net/"
    "adzuna-uebernimmt-joblift-die-totgesagte-marke-bekommt-ein-zweites-leben"
)
NOTE = "now part of Adzuna, insolvency 2026"
GERMANY = {
    "locations": ["Berlin, Germany"],
    "target_roles": ["Maintenance Technician"],
    "industry": "automotive",
}


def _availability():
    return json.loads((ROOT / "data" / "publisher_availability.json").read_text())


def test_the_ownership_record_carries_events_owner_and_source_metadata_last():
    rows = [r for r in _availability().get("ownership") or [] if r["name"] == "Joblift"]
    assert len(rows) == 1
    row = rows[0]
    assert row["owner"] == "Adzuna"
    assert row["note"] == NOTE
    dates = {e["date"] for e in row["events"]}
    assert {"2026-04-07", "2026-07-16"} <= dates
    assert row["source"] == SOURCE
    assert list(row)[-2:] == ["source", "as_of"]
    folded = [f for f in _availability()["folded_boards"] if f["name"] == "Joblift"]
    assert folded and folded[0]["operated_by"] == "Adzuna"
    assert "Germany" not in folded[0]["countries"]
    assert folded[0]["source"] == SOURCE


def test_joblift_stays_available_in_germany_and_names_carry_the_note():
    assert publisher_filter.availability_for("Joblift", "Germany") is None
    assert not publisher_filter.is_folded("Joblift", "Germany")
    shown = publisher_filter.display_name("Joblift", "Germany")
    assert shown == f"Joblift ({NOTE})"
    # idempotent, and still the same board for every matcher
    assert publisher_filter.display_name(shown, "Germany") == shown
    assert publisher_filter.names_match(shown, "Joblift")
    assert publisher_filter.clean_names(["Joblift", "StepStone"], None, "Germany") == [
        f"Joblift ({NOTE})",
        "StepStone",
    ]


def test_outside_germany_joblift_is_an_adzuna_portal_not_a_board():
    for country in ("United Kingdom", "France", "Netherlands", "Belgium"):
        assert publisher_filter.is_folded("Joblift", country), country
        names = [b["row_name"] for b in channel_funding.ranked_boards(
            {"locations": [f"London, {country}"]}, country)]
        assert "Joblift" not in names, country


def test_germany_plan_funds_joblift_with_the_note_on_the_funded_name():
    split = channel_funding.regional_budget_split(
        GERMANY,
        {"channel_allocations": {"regional_boards": {"dollar_amount": 100000}}},
    )
    names = [b["name"] for b in split["countries"][0]["boards"]]
    assert f"Joblift ({NOTE})" in names, names


def test_joblift_and_adzuna_are_never_funded_as_two_independent_boards():
    real = channel_funding._performance_notes

    def _boost_adzuna(country):
        notes = list(real(country))
        if country == "Germany":
            notes.append(
                {"names": ["Adzuna"], "text": "test", "source": "", "as_of": ""}
            )
        return notes

    with mock.patch.object(channel_funding, "_performance_notes", _boost_adzuna):
        ranked = channel_funding.ranked_boards(GERMANY, "Germany")
    keys = [b["row_name"] for b in ranked]
    assert "Adzuna" in keys
    assert "Joblift" not in keys, "Joblift ranked beside its owner Adzuna"
