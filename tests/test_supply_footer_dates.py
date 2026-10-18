"""Supply freshness footers (international audit 2026-10-18, item #5).

The footers said "supply network 2026-10-17": that date is the newest single
board edit on top of the supply repository, not a refresh of anything, and
the deck footer dropped the repository's own date (2026-03-31).  A footer now
states the repository date and, separately, the latest confirmed exception, in
plain words, on the workbook and the deck.

Every expected date is read from the data files in the test, never typed.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import Any, List, Set

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402
from pptx import Presentation  # noqa: E402

import excel_v2  # noqa: E402
import plan_trust  # noqa: E402
import ppt_generator  # noqa: E402

DATA_DIR = PROJECT_ROOT / "data"
REPO = json.loads((DATA_DIR / "joveo_global_supply_repository.json").read_text())
GROUPS = json.loads((DATA_DIR / "joveo_network_groups.json").read_text())


def _plan() -> dict:
    return {
        "client_name": "Fixture Co",
        "company_name": "Fixture Co",
        "industry": "general_entry_level",
        "budget": "$100,000",
        "locations": ["Dallas, TX"],
        "roles": ["Agent"],
        "target_roles": ["Agent"],
        "campaign_duration": "3 months",
        "hire_volume": "50",
        "work_environment": "onsite",
        "_enriched": {},
        "_synthesized": {},
        "_budget_allocation": {},
    }


def _workbook(plan: dict):
    return openpyxl.load_workbook(io.BytesIO(excel_v2.generate_excel_v2(dict(plan))))


def _cells(ws) -> List[str]:
    return [
        str(v) for row in ws.iter_rows(values_only=True) for v in row if v is not None
    ]


def _deck_texts(plan: dict) -> List[str]:
    prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(plan))))
    return [
        shape.text_frame.text
        for slide in prs.slides
        for shape in slide.shapes
        if shape.has_text_frame and shape.text_frame.text.strip()
    ]


def _latest_exception_date() -> str:
    """Newest dated single-board edit on top of the repository (read from the
    three files that carry them)."""
    found: List[str] = []

    def walk(node: Any, keys: Set[str]) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if k in keys and isinstance(v, str):
                    found.append(v[:10])
                walk(v, keys)
        elif isinstance(node, list):
            for v in node:
                walk(v, keys)

    walk(GROUPS, {"as_of"})
    walk(json.loads((DATA_DIR / "publisher_availability.json").read_text()), {"as_of"})
    walk(REPO, {"network_as_of"})
    return max(found)


# ---------------------------------------------------------------------------
# #5: footers state the supply data's date and, separately, how far board
# availability has been updated -- in words a recruiter reads
# ---------------------------------------------------------------------------


def _footer(repo_date: str, latest: str) -> str:
    return f"Supply data dated {repo_date}; board availability updated through {latest}"


def test_footers_say_what_each_date_is():
    info = plan_trust.data_asof({})
    repo_date = REPO["last_updated"]
    latest = _latest_exception_date()
    assert info["headline"] == (
        f"Data as of: supply data dated {repo_date}; board availability updated "
        f"through {latest} | benchmarks "
        f"{plan_trust._row_date(info['families'], plan_trust.BENCHMARKS_LABEL, 'oldest')}"
    )
    assert info["footer_headline"] == _footer(repo_date, latest)
    for text in (info["headline"], info["footer_headline"]):
        assert "supply network" not in text.lower()
        assert "refresh" not in text.lower()
        assert "exception" not in text.lower()  # internal jargon


def test_the_two_dates_stay_separate_whichever_is_newer(tmp_path):
    # synthetic files: the data being tested must not decide which is newer
    for repo_date, board_date in (
        ("2026-03-31", "2026-10-17"),
        ("2026-11-02", "2026-10-17"),
        ("2026-05-05", "2026-05-05"),
    ):
        (tmp_path / "repo.json").write_text(json.dumps({"last_updated": repo_date}))
        (tmp_path / "groups.json").write_text(
            json.dumps({"groups": [{"as_of": board_date}]})
        )
        info = plan_trust.data_asof(
            {},
            data_dir=str(tmp_path),
            families=[
                (
                    plan_trust.SUPPLY_NETWORK_LABEL,
                    (("groups.json", "as_of"),),
                    "latest",
                ),
                (plan_trust.SUPPLY_REPO_LABEL, ("repo.json",)),
            ],
        )
        assert info["footer_headline"] == _footer(repo_date, board_date)
        assert f"supply data dated {repo_date}; board availability updated" in (
            info["headline"]
        )


def test_a_missing_date_says_so_instead_of_borrowing_the_other(tmp_path):
    (tmp_path / "repo.json").write_text(json.dumps({"last_updated": "2026-03-31"}))
    info = plan_trust.data_asof(
        {},
        data_dir=str(tmp_path),
        families=[
            (plan_trust.SUPPLY_NETWORK_LABEL, (("missing.json", "as_of"),), "latest"),
            (plan_trust.SUPPLY_REPO_LABEL, ("repo.json",)),
        ],
    )
    assert info["footer_headline"] == (
        "Supply data dated 2026-03-31; board availability update date not recorded"
    )


def test_board_availability_family_is_not_called_a_refresh():
    info = plan_trust.data_asof({})
    fam = next(
        f for f in info["families"] if f["label"] == plan_trust.SUPPLY_NETWORK_LABEL
    )
    assert fam["as_of"] == f"updated through {_latest_exception_date()}"
    repo = next(
        f for f in info["families"] if f["label"] == plan_trust.SUPPLY_REPO_LABEL
    )
    assert repo["as_of"] == REPO["last_updated"]
    assert "exception" not in fam["label"].lower()


def test_workbook_footer_states_both_dates_in_plain_words():
    wb = _workbook(_plan())
    text = "\n".join(_cells(wb["Executive Summary"]))
    assert f"supply data dated {REPO['last_updated']}" in text
    assert f"board availability updated through {_latest_exception_date()}" in text
    assert "supply network" not in text.lower()
    assert "exception" not in text.lower()
    sources = "\n".join(_cells(wb["Sources & Confidence"]))
    assert "does not mean the full listing was refreshed" in sources
    assert "Last Refreshed" not in sources


def test_deck_footer_names_both_dates_and_stays_clear_of_the_credit():
    prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(_plan())))
    slide = prs.slides[1]
    boxes = {
        sh.text_frame.text: sh
        for sh in slide.shapes
        if sh.has_text_frame and sh.text_frame.text
    }
    footer = _footer(REPO["last_updated"], _latest_exception_date())
    assert footer in boxes
    credit = next(t for t in boxes if t.startswith("Created by"))
    assert len(footer) <= ppt_generator._ASOF_FOOTER_MAX_CHARS
    # measured with real Poppins advances (the deck's own font), not a
    # character count: the line starts 0.1in inside its box and must end
    # before the credit's text begins
    sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "deck_qa"))
    import _textlayout as tl  # noqa: E402

    emu = 914400.0
    foot, cred = boxes[footer], boxes[credit]
    footer_ink_right = foot.left / emu + 0.1 + tl.adv_in(footer, 9, False)
    credit_ink_left = (cred.left + cred.width) / emu - 0.1 - tl.adv_in(credit, 9, False)
    assert footer_ink_right < credit_ink_left - 0.3, (footer_ink_right, credit_ink_left)
    # the two text boxes never share space either
    assert foot.left + foot.width <= cred.left
    # and the line stays inside the 16:9 slide
    assert (cred.left + cred.width) / emu <= prs.slide_width / emu
