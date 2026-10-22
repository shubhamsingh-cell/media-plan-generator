"""Supply-team performance notes: a board reported performing well in a market
earns the performance-note score there (+35), but only if the supply data
already has it in network for that market."""

import channel_funding
import publisher_filter

_GERMANY_PLAN = {
    "locations": ["Berlin, Germany"],
    "target_roles": ["Maintenance Technician"],
    "industry": "automotive",
}


def _ranked(country="Germany", data=None):
    return channel_funding.ranked_boards(
        _GERMANY_PLAN if data is None else data, country
    )


def _by_name(ranked, name):
    return next((b for b in ranked if b["name"] == name), None)


def _note_for(name):
    return {
        "names": [name],
        "text": "performs well in Germany",
        "source": "Joveo supply team",
        "as_of": "2026-10-21",
    }


class TestPerformanceNoteScore:
    def test_germany_scores_studysmarter_with_the_note_and_funds_it(self):
        ranked = _ranked()
        board = _by_name(ranked, "StudySmarter")
        assert board is not None
        assert "performs well (Joveo supply team)" in board["why"]
        assert board["status"].endswith(
            "; performs well in Germany (Joveo supply team, 2026-10-21)"
        )
        assert ranked.index(board) < channel_funding.FUNDED_BOARD_COUNT

    def test_the_note_adds_exactly_its_weight_and_changes_nothing_else(
        self, monkeypatch
    ):
        with_note = {b["name"]: b for b in _ranked()}
        monkeypatch.setattr(channel_funding, "_performance_notes", lambda c: [])
        without = {b["name"]: b for b in _ranked()}
        assert all("performs well" not in b["status"] for b in without.values())
        assert (
            with_note["StudySmarter"]["score"] - without["StudySmarter"]["score"]
            == channel_funding.SCORE_PERFORMANCE_NOTE
        )
        for name, b in without.items():
            if name != "StudySmarter":
                assert with_note[name]["score"] == b["score"], name

    def test_studysmarter_is_funded_in_the_germany_split(self):
        alloc = {
            "channel_allocations": {
                "regional_boards": {"dollar_amount": 50000, "label": "Regional"}
            }
        }
        split = channel_funding.regional_budget_split(_GERMANY_PLAN, alloc)
        assert "StudySmarter" in channel_funding.funded_board_names(split)

    def test_a_note_for_a_board_not_in_network_does_not_fund_it(self, monkeypatch):
        # a board with a note but no in-network record, by name
        monkeypatch.setattr(
            channel_funding,
            "_performance_notes",
            lambda c: [_note_for("Nonexistent Board XYZ")],
        )
        assert _by_name(_ranked(), "Nonexistent Board XYZ") is None
        # a real row whose supply state is not in network
        for name in ("Jobflow", "Meinestadt.de"):
            assert (
                publisher_filter.board_supply_record(name, "Germany")["state"]
                != publisher_filter.SUPPLY_IN_NETWORK
            ), name
            monkeypatch.setattr(
                channel_funding, "_performance_notes", lambda c, n=name: [_note_for(n)]
            )
            ranked = _ranked()
            assert _by_name(ranked, name) is None
            assert not [
                b for b in ranked if publisher_filter.names_match(b["name"], name)
            ]

    def test_a_note_never_overrides_an_unavailable_board(self, monkeypatch):
        real = publisher_filter.board_supply_record

        def barred(name, country=None):
            rec = dict(real(name, country))
            if "studysmarter" in str(name).casefold().replace(" ", ""):
                rec["state"] = publisher_filter.SUPPLY_UNAVAILABLE
            return rec

        monkeypatch.setattr(publisher_filter, "board_supply_record", barred)
        assert _by_name(_ranked(), "StudySmarter") is None

    def test_a_note_for_another_country_does_not_apply(self):
        assert all(
            "performs well" not in b["status"]
            for b in _ranked("France", {"locations": ["Paris, France"]})
        )
