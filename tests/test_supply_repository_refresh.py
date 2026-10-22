"""The 2026-10 supply repository refresh and the onboarding-pipeline source.

* the shipped data files are public: no e-mail-shaped string, no VONQ-only row,
  only the whitelisted pipeline fields and only the tracker's own vocabulary;
* scripts/build_supply_repository.py is deterministic and sanitizes (checked on
  synthetic workbooks: the real ones are private and never in git);
* a pipeline board that is not Live never reads as available or in network; a
  Live one with no contrary repository row reads in network;
* the deck / Nova publisher count has one source: the repository's
  "In Joveo Network" rows.
"""

import importlib.util
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import pytest

import publisher_filter as pf

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
REPO = json.loads((DATA / "joveo_global_supply_repository.json").read_text())
PIPE = json.loads((DATA / "publisher_onboarding_pipeline.json").read_text())

EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
PARTNER_KEYS = {
    "name",
    "regions",
    "type",
    "job_category",
    "stage",
    "status",
    "test_campaign_allocated",
}
STATUS_VOCAB = {
    "Not started",
    "In progress",
    "Waiting on them",
    "Blocked",
    "Done",
    "Dropped",
}
SENSITIVE = re.compile(r"commission|\bmsa\b|min(?:imum)?\s*bid|finance|owner", re.I)


def stages_before(stage: str) -> List[str]:
    names = [s["name"] for s in PIPE["stages"]]
    return names[: names.index(stage)]


def _rows() -> List[Dict[str, Any]]:
    return [p for blk in REPO["by_region"].values() for p in blk["publishers"]]


@pytest.fixture(autouse=True)
def _fresh():
    pf.reset_caches()
    yield
    pf.reset_caches()


# --------------------------------------------------------------------------
# the shipped files
# --------------------------------------------------------------------------
def test_repository_holds_no_email_and_no_vonq_only_row():
    text = (DATA / "joveo_global_supply_repository.json").read_text()
    assert not EMAIL.findall(text)
    # VONQ-only boards are absent from the data; the one place the words appear
    # is a network conflict's own export_status (owner decision 2026-10-20: the
    # newer export wins over a network-group confirmation).
    body = {k: v for k, v in REPO.items() if k != "network_conflicts"}
    assert "VONQ Only" not in json.dumps(body)
    assert all(
        set(c) == {"name", "country", "export_status", "as_of"}
        for c in REPO["network_conflicts"]
    )
    assert all(p["status"] != "VONQ Only" for p in _rows())
    assert "VONQ Only" not in REPO["summary"]["by_status"]


def test_repository_keeps_its_schema_and_adds_up():
    assert REPO["last_updated"] == "2026-10-20"
    assert set(REPO) == {
        "source",
        "last_updated",
        "total_publishers",
        "active_publishers",
        "summary",
        "by_region",
        "top_publishers_p0_p1",
        "network_conflicts",
    }
    assert set(REPO["summary"]) == {
        "by_priority",
        "by_status",
        "by_region",
        "by_job_type",
    }
    base = {
        "name",
        "parent",
        "priority",
        "countries",
        "job_type",
        "category",
        "job_categories",
        "pricing",
        "status",
        "easy_apply",
        "xml",
    }
    assert all(base <= set(p) for p in _rows())
    assert len(_rows()) == REPO["total_publishers"]
    assert sum(REPO["summary"]["by_status"].values()) == REPO["total_publishers"]
    for blk in REPO["by_region"].values():
        assert blk["total"] == len(blk["publishers"])
    top = REPO["top_publishers_p0_p1"]
    assert len(top) == sum(1 for p in _rows() if p["priority"] in ("P0", "P1"))
    inactive = sum(
        REPO["summary"]["by_status"].get(s, 0)
        for s in ("Deactivated", "Churned", "Paused / Pending Revival")
    )
    assert REPO["active_publishers"] == REPO["total_publishers"] - inactive
    assert not any("Collar Collar" in str(p["job_type"]) for p in _rows())


def test_curated_corrections_survive_a_refresh():
    names = {p["name"] for p in _rows()}
    assert "AAHA (American Animal Hospital Association)" in names
    assert not [n for n in names if "Veterinery" in n or "Assocoiation" in n]
    vets = {p["name"] for p in _rows() if p["category"] == "Veterinary"}
    assert "My Veterinary Job Board" in vets
    totaljobs = next(p for p in _rows() if p["name"] == "Totaljobs")
    assert "United Kingdom" in totaljobs["countries"]
    assert totaljobs["network_group"] == "The Stepstone Group"


def test_pipeline_ships_only_the_seven_fields_and_the_trackers_vocabulary():
    assert set(PIPE) == {"source", "as_of", "scope", "stages", "partners"}
    assert PIPE["scope"].startswith("partners at Contract signed or later")
    # the public file never names a prospect still being courted
    pre_contract = stages_before("Contract signed")
    assert pre_contract and not [
        p for p in PIPE["partners"] if p["stage"] in pre_contract
    ]
    assert {p["stage"] for p in PIPE["partners"]} <= {
        "Contract signed",
        "Dashboard access granted",
        "Test campaign allocated",
        "Live",
    }
    assert PIPE["as_of"] == "2026-10-20"
    stages = [s["name"] for s in PIPE["stages"]]
    assert stages[0] == "Sourced" and "Live" in stages and "Contract sent" in stages
    text = (DATA / "publisher_onboarding_pipeline.json").read_text()
    assert not EMAIL.findall(text)
    assert not SENSITIVE.findall(text)
    assert PIPE["partners"]
    for partner in PIPE["partners"]:
        assert set(partner) == PARTNER_KEYS, partner
        assert partner["stage"] in stages
        assert partner["status"] is None or partner["status"] in STATUS_VOCAB
        assert partner["test_campaign_allocated"] in (True, False, None)
        assert isinstance(partner["regions"], list)
        assert all(isinstance(r, str) and r.strip() for r in partner["regions"])


def test_stage_exit_criteria_carry_no_commercial_terms():
    for stage in PIPE["stages"]:
        crit = stage["exit_criteria"]
        assert crit is None or not SENSITIVE.search(crit)


# --------------------------------------------------------------------------
# status semantics
# --------------------------------------------------------------------------
def _use_pipeline(monkeypatch, partners: List[Dict[str, Any]]) -> None:
    """Replace the cached pipeline with synthetic partners (same loader rules)."""
    stages = [s["name"] for s in PIPE["stages"]]
    by_key: Dict[str, list] = {}
    for partner in partners:
        full = {
            "regions": [],
            "type": None,
            "job_category": None,
            "status": None,
            "test_campaign_allocated": None,
            **partner,
        }
        if full["stage"] == "Not pursued" or full["status"] == "Dropped":
            continue  # the loader drops these before indexing
        by_key.setdefault(pf.normalize(full["name"]), []).append(full)
    pf._pipeline()  # populate, then replace
    monkeypatch.setattr(
        pf,
        "_pipeline_cache",
        {"as_of": "2026-10-20", "stages": stages, "by_key": by_key},
    )


def test_a_board_in_onboarding_is_never_available_or_in_network(monkeypatch):
    _use_pipeline(monkeypatch, [{"name": "Zzyx Test Board", "stage": "Contract sent"}])
    record = pf.board_supply_record("Zzyx Test Board", "Germany")
    assert record["state"] == pf.SUPPLY_ONBOARDING
    assert record["status"] == "In onboarding (Contract sent) — not yet available"
    assert pf.status_label(record).startswith("In onboarding (Contract sent)")
    assert pf.relationship_unverified("Zzyx Test Board", "Germany") is True
    marked, flagged = pf.annotate(["Zzyx Test Board"], "Germany")
    assert flagged is True and marked == [f"Zzyx Test Board {pf.RELATIONSHIP_MARK}"]
    assert "not yet available" in pf.supply_status_phrase(record)
    assert "not yet available" in pf.unverified_note(record)
    # not counted as confirmed in a market's depth
    assert pf.supply_depth("Germany", ["Zzyx Test Board"])["confirmed"] == 0


def test_every_shipped_non_live_pipeline_board_is_unconfirmed():
    in_repo_network = {
        pf.normalize(p["name"]) for p in _rows() if p["status"] == "In Joveo Network"
    }
    checked = 0
    for partner in PIPE["partners"]:
        if partner["stage"] == "Live" or partner["status"] == "Dropped":
            continue
        if pf.normalize(partner["name"]) in in_repo_network:
            continue  # the repository itself lists it in network: that wins
        for country in partner["regions"] or [None]:
            if pf.availability_for(partner["name"], country):
                continue
            record = pf.board_supply_record(partner["name"], country)
            assert record["state"] != pf.SUPPLY_IN_NETWORK, (partner, country, record)
            checked += 1
    assert checked >= 1


def test_a_live_pipeline_board_with_no_repository_row_is_in_network(monkeypatch):
    _use_pipeline(monkeypatch, [{"name": "Zzyx Live Board", "stage": "Live"}])
    record = pf.board_supply_record("Zzyx Live Board", "Germany")
    assert record["state"] == pf.SUPPLY_IN_NETWORK
    assert record["as_of"] == "2026-10-20" and record["source"].startswith("onboarding")
    assert pf.relationship_unverified("Zzyx Live Board", "Germany") is False


def test_a_live_pipeline_board_never_overrides_a_contrary_repository_row(monkeypatch):
    _use_pipeline(monkeypatch, [{"name": "Zzyx Live Board", "stage": "Live"}])
    monkeypatch.setitem(
        pf._repo_index(),
        pf.normalize("Zzyx Live Board"),
        [(frozenset({"Germany"}), "Via Redwood")],
    )
    record = pf.board_supply_record("Zzyx Live Board", "Germany")
    assert (
        record["state"] == pf.SUPPLY_NOT_IN_NETWORK
        and record["status"] == "Via Redwood"
    )


def test_dropped_and_not_pursued_have_no_effect_and_regions_scope(monkeypatch):
    _use_pipeline(
        monkeypatch,
        [
            {"name": "Zzyx Dropped", "stage": "Contract sent", "status": "Dropped"},
            {"name": "Zzyx Parked", "stage": "Not pursued"},
            {"name": "Zzyx Scoped", "stage": "Responded", "regions": ["India"]},
        ],
    )
    assert (
        pf.board_supply_record("Zzyx Dropped", "Germany")["state"]
        == pf.SUPPLY_NO_RECORD
    )
    assert (
        pf.board_supply_record("Zzyx Parked", "Germany")["state"] == pf.SUPPLY_NO_RECORD
    )
    assert (
        pf.board_supply_record("Zzyx Scoped", "Germany")["state"] == pf.SUPPLY_NO_RECORD
    )
    assert (
        pf.board_supply_record("Zzyx Scoped", "India")["state"] == pf.SUPPLY_ONBOARDING
    )


def test_onboarding_state_does_not_displace_a_repository_network_row(monkeypatch):
    _use_pipeline(monkeypatch, [{"name": "Zzyx Both", "stage": "Contract sent"}])
    monkeypatch.setitem(
        pf._repo_index(),
        pf.normalize("Zzyx Both"),
        [(frozenset({"Germany"}), "In Joveo Network")],
    )
    assert (
        pf.board_supply_record("Zzyx Both", "Germany")["state"] == pf.SUPPLY_IN_NETWORK
    )


def test_status_key_says_what_the_partner_routes_are():
    key = pf.SUPPLY_STATUS_KEY
    assert (
        "Via Redwood / Handshake / Symplicity / 12twenty / Jobteaser = reachable "
        "through a partner route, not a direct Joveo network relationship"
    ) in key
    assert "VONQ" not in key
    assert "In onboarding" in key


# --------------------------------------------------------------------------
# Nova
# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def nova_engine():
    import nova

    return nova.Nova()


def test_nova_answers_is_x_onboarding_without_calling_it_available(nova_engine):
    name = next(
        p["name"]
        for p in PIPE["partners"]
        if p["stage"]
        in ("Contract signed", "Dashboard access granted", "Test campaign allocated")
        and pf.board_supply_record(p["name"], None)["state"] == pf.SUPPLY_ONBOARDING
    )
    res = nova_engine._query_publishers({"search_term": name.lower()})
    block = res["onboarding_pipeline"]
    row = next(r for r in block["partners"] if r["name"] == name)
    assert (
        row["availability"]
        == "Signed, going live soon \u2014 not yet available in plans"
    )
    assert "NOT available" in block["note"] and "do not count it" in block["note"]
    assert res.get("in_joveo_network") is not True


def test_nova_lists_new_partners_coming(nova_engine):
    res = nova_engine._query_publishers(
        {"search_term": "which new partners are coming"}
    )
    block = res["onboarding_pipeline"]
    assert block["count"] >= 3 and block["as_of"] == "2026-10-20"
    assert set(block["by_stage"]) <= {
        "Contract signed",
        "Dashboard access granted",
        "Test campaign allocated",
        "Live",
    }
    texts = {r["availability"] for r in block["partners"]}
    assert "Signed, going live soon \u2014 not yet available in plans" in texts
    for row in block["partners"]:
        if row["availability"] == pf.LIVE_IN_NETWORK_TEXT:  # Live AND not contradicted
            assert (
                pf.board_supply_record(row["name"], None)["state"]
                == pf.SUPPLY_IN_NETWORK
            )
    assert all(r["status"] != "Dropped" for r in block["partners"])


def test_nova_repository_only_hit_is_not_called_in_network(nova_engine):
    via = next(
        p for p in _rows() if p["status"] == "Via Redwood" and len(p["name"]) > 8
    )
    res = nova_engine._query_publishers({"search_term": via["name"].lower()})
    hits = [m for m in res["search_results"] if m["name"] == via["name"]]
    if hits and all(
        m.get("source") == "global_supply_repository" for m in res["search_results"]
    ):
        assert res["in_joveo_network"] is False


# --------------------------------------------------------------------------
# one source for the active-publisher count
# --------------------------------------------------------------------------
def test_active_publisher_count_has_one_source():
    in_network = REPO["summary"]["by_status"]["In Joveo Network"]
    assert in_network == sum(1 for p in _rows() if p["status"] == "In Joveo Network")
    pubs = json.loads((DATA / "joveo_publishers.json").read_text())
    assert pubs["total_active_publishers"] == in_network
    import ppt_generator

    assert ppt_generator._joveo_total_active_publishers({}) == in_network


# --------------------------------------------------------------------------
# the builder (synthetic workbooks: the real ones are private)
# --------------------------------------------------------------------------
openpyxl = pytest.importorskip("openpyxl")


def _builder():
    spec = importlib.util.spec_from_file_location(
        "build_supply_repository", ROOT / "scripts" / "build_supply_repository.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MASTER_HEADER = [
    "PUBLISHER", "PARENT COMPANY", "PRIORITY", "JOVEO NETWORK STATUS", "REGION",
    "COUNTRIES", "JOB TYPE", "PUBLISHER CATEGORY", "JOB CATEGORY", "POC EMAIL",
    "FINANCE POC", "PUBLISHER TYPE", "EASY APPLY CHECK", "REDIRECTION", "XML",
    "SP. REQS", "PUBLISHER SUMMARY",
]  # fmt: skip


def _master(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Master List"
    ws.append(MASTER_HEADER)
    for r in rows:
        ws.append(r + [None] * (len(MASTER_HEADER) - len(r)))
    wb.save(path)


def _pub(name, status, **kw):
    row = dict.fromkeys(MASTER_HEADER)
    row.update(
        {
            "PUBLISHER": name,
            "JOVEO NETWORK STATUS": status,
            "PRIORITY": kw.get("priority", "P2"),
            "REGION": kw.get("region", "AMER"),
            "COUNTRIES": kw.get("countries", "United States"),
            "JOB TYPE": kw.get("job_type", "White Collar Collar"),
            "PUBLISHER CATEGORY": kw.get("category", "Universal"),
            "POC EMAIL": "contact@example-publisher.com",
            "FINANCE POC": "A Finance Person",
            "SP. REQS": "min bid 5, commission 12%",
            "PUBLISHER SUMMARY": "internal note: MSA signed",
        }
    )
    return [row[h] for h in MASTER_HEADER]


def _tracker(path):
    wb = openpyxl.Workbook()
    wb.active.title = "Pipeline"
    pipe = wb["Pipeline"]
    pipe.append(
        ["Publisher", "Owner", "Source", "Primary Region(s)", "Secondary Region(s)", "Type",
         "Job Category", "Programmatic?", "Redirection", "XML", "Primary Contact",
         "Contact Email", "Current Stage", "Status", "Contact Medium", "First Contacted",
         "Test  Campaign Allocated"]
    )  # fmt: skip
    pipe.append(
        ["Alpha Jobs", "Someone", "Inbound", "US, UK", None, "CPC + CPA", "Blue collar",
         "Yes", "Yes", "Yes", "Jane Roe", "jane@alpha-jobs.test", "Contract sent",
         "Waiting on them", "Email", "2026-07-01", "Fakeclient Spring Pilot"]
    )  # fmt: skip
    pipe.append(
        ["Gamma Board", "Someone", "Inbound", "Atlantis", None, "Weird Type", "Other",
         "Yes", "Yes", "Yes", "Bob Roe", "bob@gamma.test", "Live", "Done", "Email",
         "2026-07-01", "No"]
    )  # fmt: skip
    pipe.append(["Not Started Co", "Someone"])  # no stage: left out
    part = wb.create_sheet("Partner Pipeline")
    part.append(
        ["Publisher", "Owner", None, "Primary Region(s)", "Type", "Job Category",
         "Programmatic?", "Redirection", "XML", "Primary Contact", "Contact Email",
         "Current Stage", "Status", "Contact Medium", "First Contacted",
         "Test  Campaign Allocated"]
    )  # fmt: skip
    # a shifted row: contact name / e-mail sit under Stage / Status
    part.append(
        ["Beta Media", "Someone", None, "US", None, None, "Yes", "Yes", "Yes", None, None,
         "Carl Contact", "carl@beta.test", "In redlines", "Blocked"]
    )  # fmt: skip
    # the same partner as the Pipeline sheet, further along
    part.append(
        ["alpha jobs", "Someone", None, "US", "CPC", "Blue collar", "Yes", "Yes", "Yes",
         "Jane Roe", "jane@alpha-jobs.test", "Contract signed", "Done", "Email",
         "2026-07-01", "Yes"]
    )  # fmt: skip
    lists = wb.create_sheet("Lists")
    lists.append(["Stage", "Status", "Source", "Type"])
    stages = [
        "Sourced",
        "Contract sent",
        "In redlines",
        "Contract signed",
        "Live",
        "Not pursued",
    ]
    statuses = [
        "Not started",
        "In progress",
        "Waiting on them",
        "Blocked",
        "Done",
        "Dropped",
    ]
    for i in range(6):
        lists.append([stages[i], statuses[i], None, "Job board" if i == 0 else None])
    chk = wb.create_sheet("Stages & Checklist")
    chk.append(["#", "Stage", "Exit criteria"])
    for i, st in enumerate(stages, 1):
        text = (
            "Terms and commission sent" if st == "Contract sent" else f"{st} is done."
        )
        chk.append([float(i), st, text])
    wb.save(path)


@pytest.fixture()
def built(tmp_path):
    mod = _builder()
    master, tracker = tmp_path / "m.xlsx", tmp_path / "t.xlsx"
    _master(
        master,
        [
            _pub("Plain Board", "In Joveo Network"),
            _pub("Kept Redwood Board", "Via Redwood", priority="P1"),
            _pub("Vonq Board", "VONQ Only"),
            _pub("Totaljobs", "Via Redwood", region="Europe", countries="Ireland"),
            _pub("CWJobs", "Deactivated", region="Europe", countries="United Kingdom"),
            _pub("My Veterinery Job Board", "In Joveo Network"),
            _pub("Blank Status Board", None),
        ],
    )
    _tracker(tracker)
    outs = []
    for i in (1, 2):
        repo_out, pipe_out = tmp_path / f"r{i}.json", tmp_path / f"p{i}.json"
        assert mod.main([str(master), str(tracker), "--repository-out", str(repo_out),
                         "--pipeline-out", str(pipe_out), "--no-sync-count"]) == 0  # fmt: skip
        priv_out = tmp_path / f"full{i}.json"
        assert mod.main([str(master), str(tracker), "--repository-out", str(repo_out),
                         "--pipeline-out", str(pipe_out), "--private-out", str(priv_out),
                         "--no-sync-count"]) == 0  # fmt: skip
        outs.append((repo_out, pipe_out, priv_out))
    return outs


def test_builder_is_deterministic(built):
    (r1, p1, f1), (r2, p2, f2) = built
    assert r1.read_bytes() == r2.read_bytes()
    assert p1.read_bytes() == p2.read_bytes()
    assert f1.read_bytes() == f2.read_bytes()


def test_builder_drops_vonq_only_and_private_columns(built):
    repo = json.loads(built[0][0].read_text())
    rows = {p["name"]: p for b in repo["by_region"].values() for p in b["publishers"]}
    assert "Vonq Board" not in rows and repo["total_publishers"] == 6
    text = built[0][0].read_text()
    assert not EMAIL.findall(text)
    assert "Finance" not in text and "commission" not in text and "MSA" not in text
    assert (
        rows["Plain Board"]["job_type"] == "White Collar"
    )  # "Collar Collar" normalised
    assert rows["Blank Status Board"]["status"] == "Status not recorded"
    assert repo["active_publishers"] == 6 - 1  # CWJobs is Deactivated


def test_builder_reapplies_curated_fixes_and_group_stamps(built):
    repo = json.loads(built[0][0].read_text())
    rows = {p["name"]: p for b in repo["by_region"].values() for p in b["publishers"]}
    vet = rows["My Veterinary Job Board"]
    assert vet["category"] == "Veterinary" and vet["name_previous"].startswith(
        "My Veterinery"
    )
    tj = rows["Totaljobs"]
    assert (
        tj["countries"] == "United Kingdom, Ireland"
        and tj["countries_previous"] == "Ireland"
    )
    assert tj["status"] == "In Joveo Network" and tj["status_previous"] == "Via Redwood"
    assert tj["network_group"] == "The Stepstone Group"
    # a deliberate Deactivated is not flipped or stamped
    cw = rows["CWJobs"]
    assert cw["status"] == "Deactivated" and "network_group" not in cw


def test_builder_pipeline_is_sanitized_validated_and_deduplicated(built):
    pipe = json.loads(built[0][1].read_text())
    text = built[0][1].read_text()
    assert not EMAIL.findall(text)
    for leaked in ("Jane", "Roe", "Carl", "Bob", "Fakeclient", "commission", "Someone"):
        assert leaked not in text, leaked
    by_name = {p["name"].lower(): p for p in pipe["partners"]}
    assert set(PARTNER_KEYS) == set(pipe["partners"][0])
    # alpha (merged across sheets) and gamma; beta is still In redlines
    assert len(pipe["partners"]) == 2 and "beta media" not in by_name
    assert pipe["scope"].startswith("partners at Contract signed or later")
    assert "Beta Media" not in text
    alpha = by_name["alpha jobs"]
    assert alpha["stage"] == "Contract signed"  # the further stage wins the duplicate
    assert alpha["regions"] == ["United States"] and alpha["type"] == "CPC"
    gamma = by_name["gamma board"]
    assert (
        gamma["regions"] == []
        and gamma["type"] is None
        and gamma["job_category"] is None
    )
    assert gamma["test_campaign_allocated"] is False
    assert "not started co" not in by_name  # no recognised stage: left out
    contract = next(s for s in pipe["stages"] if s["name"] == "Contract sent")
    assert contract["exit_criteria"] is None  # carried a commercial term: not shipped


def test_private_pipeline_keeps_every_stage_with_the_same_seven_keys(built):
    full = json.loads(built[0][2].read_text())
    pub = json.loads(built[0][1].read_text())
    by_name = {p["name"].lower(): p for p in full["partners"]}
    assert set(by_name) == {"alpha jobs", "beta media", "gamma board"}
    assert by_name["beta media"]["stage"] == "In redlines"
    assert by_name["beta media"]["status"] == "Blocked"
    assert all(set(p) == set(PARTNER_KEYS) for p in full["partners"])
    assert full["stages"] == pub["stages"]
    assert not EMAIL.findall(built[0][2].read_text())


def test_builder_region_tokens_become_canonical_countries():
    mod = _builder()
    known = {"germany": "Germany", "brazil": "Brazil", "india": "India"}
    assert mod._regions("DE / EU", known) == ["Germany", "Europe"]
    assert mod._regions("US, UK", known) == ["United States", "United Kingdom"]
    assert mod._regions("IN", known) == ["India"]
    assert mod._regions("BR", known) == ["Brazil"]
    assert mod._regions("Carl Contact", known) == []


def test_europe_only_partner_applies_to_europe_not_the_us(monkeypatch):
    _use_pipeline(
        monkeypatch,
        [{"name": "Zzyx Euro", "stage": "Contract signed", "regions": ["Europe"]}],
    )
    assert (
        pf.board_supply_record("Zzyx Euro", "Germany")["state"] == pf.SUPPLY_ONBOARDING
    )
    assert (
        pf.board_supply_record("Zzyx Euro", "United States")["state"]
        == pf.SUPPLY_NO_RECORD
    )
    assert pf.board_supply_record("Zzyx Euro", "India")["state"] == pf.SUPPLY_NO_RECORD
    assert [r["name"] for r in pf.onboarding_pipeline(country="Germany")] == [
        "Zzyx Euro"
    ]
    assert pf.onboarding_pipeline(country="United States") == []


def test_builder_refuses_to_write_an_email_shaped_string():
    mod = _builder()
    with pytest.raises(SystemExit):
        mod.assert_clean('{"x": "someone@example.org"}', "t")
    with pytest.raises(SystemExit):
        mod.assert_clean('{"x": "a commission rate"}', "t")
    mod.assert_clean('{"x": "Texas Veterans Commission"}', "t", words=False)
