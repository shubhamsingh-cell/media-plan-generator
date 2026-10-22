#!/usr/bin/env python3
"""Rebuild the supply repository snapshot and the onboarding-pipeline source.

    python3 scripts/build_supply_repository.py MASTER_LIST.xlsx ONBOARDING_TRACKER.xlsx

writes

* ``data/joveo_global_supply_repository.json`` -- every "Master List" row
  except status "VONQ Only" (VONQ-only boards are simply absent: the app reads
  them as "no record", never as available), in the schema the app has always
  read (see ``build_repository``);
* ``data/publisher_onboarding_pipeline.json`` -- the onboarding tracker,
  SANITIZED: partner name, regions, type, job category, stage, status and a
  yes/no test-campaign flag, and nothing else -- and only for partners at
  "Contract signed" or later (earlier-stage prospects are Joveo's commercial
  pipeline and the repository is public).  ``--private-out PATH`` writes the
  full sanitized pipeline (every stage) outside git;
* the ``total_active_publishers`` line of ``data/joveo_publishers.json`` (the
  one constant the deck and Nova read), kept equal to the repository's
  "In Joveo Network" count.

The data/ files are PUBLIC (this repository is public).  The source workbooks
carry contacts, e-mail addresses, owners, finance contacts, commission and
contract notes and client names; the builder copies only the whitelisted
fields below, validates the pipeline's stage/status against the tracker's own
vocabulary (anything else is dropped, which is how a contact name sitting in a
shifted column never ships), and refuses to write a file in which an
e-mail-shaped string or a sensitive word survived.

Deterministic: the same inputs give byte-identical output.  Needs openpyxl
(offline tool only; the server never imports this script).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("build_supply_repository")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
REPOSITORY_OUT = os.path.join(DATA_DIR, "joveo_global_supply_repository.json")
PIPELINE_OUT = os.path.join(DATA_DIR, "publisher_onboarding_pipeline.json")
PUBLISHERS_JSON = os.path.join(DATA_DIR, "joveo_publishers.json")

SOURCE_LABEL = "Joveo Global Supply Repository - Master List"
PIPELINE_SOURCE_LABEL = "Joveo publisher onboarding tracker (sanitized)"
EXPORT_DATE = "2026-10-20"  # the export the owner handed over
EXCLUDED_STATUS = "VONQ Only"
IN_NETWORK = "In Joveo Network"
# Rows the repository counts as not (or no longer) active partners.
INACTIVE_STATUSES = ("Deactivated", "Churned", "Paused / Pending Revival")
NO_STATUS = "Status not recorded"
MISSING = "nan"  # the repository's long-standing marker for an empty cell
UNSPECIFIED_REGION = "Unspecified"

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_SENSITIVE_RE = re.compile(
    r"commission|\bmsa\b|min(?:imum)?\s*bid|finance\s*poc|account\s*owner", re.I
)

# --------------------------------------------------------------------------
# Hand-curated corrections the workbook does not carry.  The 2026-03 export was
# corrected in git (commits c7a4e34 and 8bb4747); a fresh export re-introduces
# the typos, so the builder re-applies them.  Each guard only fires when the
# workbook still holds the value being corrected.
# --------------------------------------------------------------------------
NAME_FIXES = {
    "ACVIM (American College of Veterinery Internal Medicine)": (
        "ACVIM (American College of Veterinary Internal Medicine)"
    ),
    "AAHA (American Animal Hospital Assocoiation)": (
        "AAHA (American Animal Hospital Association)"
    ),
    "My Veterinery Job Board": "My Veterinary Job Board",
    "Veterinery Jobs Marketplace": "Veterinary Jobs Marketplace",
    "Stepstone Germa-ny (Pro - Media Buy) - Selected Customers Only": (
        "StepStone Germany (Pro - Media Buy) - Selected Customers Only"
    ),
}
RESTRICTIONS = {
    "StepStone Germany (Pro - Media Buy) - Selected Customers Only": (
        "Selected customers only (Pro - Media Buy)"
    ),
}
VETERINARY_BOARDS = frozenset(
    {
        "ACVIM (American College of Veterinary Internal Medicine)",
        "AAHA (American Animal Hospital Association)",
        "My Veterinary Job Board",
        "Veterinary Jobs Marketplace",
        "Animal Health Careers",
        "AnimalHealthJobs.com",
        "VeterinaryCareers.net",
    }
)
COUNTRY_FIXES = {  # board -> (value the workbook holds, corrected value)
    "Totaljobs": ("Ireland", "United Kingdom, Ireland"),
}
# Workbook statuses a confirmed board group's membership may override (the
# owner confirmed on 2026-10-17 that the group is in the network).  A
# deliberate "Deactivated" / "Churned" / "Paused" is never flipped.
STAMP_MAY_REPLACE = ("Via Redwood", "Via Handshake", "Via Symplicity", "via 12twenty")

# --------------------------------------------------------------------------
# Onboarding pipeline
# --------------------------------------------------------------------------
PARTNER_KEYS = (
    "name",
    "regions",
    "type",
    "job_category",
    "stage",
    "status",
    "test_campaign_allocated",
)
# Commercial-model words the tracker's "Type" column uses besides the Lists
# sheet's own Type vocabulary.
EXTRA_TYPES = {
    "cpc": "CPC",
    "cpa": "CPA",
    "cpm": "CPM",
    "cpc + cpa": "CPC + CPA",
    "cpc + tcpa": "CPC + tCPA",
    "posting": "Posting",
}
JOB_CATEGORIES = {
    "blue collar": "Blue collar",
    "white collar": "White collar",
    "both": "Both",
}
REGION_ALIASES = {
    "us": "United States",
    "usa": "United States",
    "uk": "United Kingdom",
    "de": "Germany",
    "in": "India",
    "br": "Brazil",
    "eu": "Europe",
    "netherland": "Netherlands",
    "uae": "United Arab Emirates",
}
_STAGE_NOT_PURSUED = "Not pursued"
# The public file names only partners whose contract is signed; earlier-stage
# prospects (Sourced ... In redlines) are Joveo's commercial pipeline and stay
# out of the public repository.
FIRST_PUBLIC_STAGE = "Contract signed"
PUBLIC_SCOPE = (
    "partners at Contract signed or later; earlier-stage prospects are kept out "
    "of this public file"
)
PRIVATE_SCOPE = "full sanitized tracker, all stages; private, never committed"


def _clean(value: Any) -> Optional[str]:
    """One workbook cell as stripped text; None for an empty cell, a float NaN
    or the repository's own "nan"/"None" markers."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():  # Excel stores the name "1840" as 1840.0
            value = int(value)
    text = re.sub(r"\s+", " ", str(value)).strip()
    return None if text.lower() in ("", "nan", "none") else text


def _load_openpyxl() -> Any:
    try:
        import openpyxl  # noqa: PLC0415 -- offline tool, optional dependency
    except ImportError as exc:  # pragma: no cover - environment issue
        raise SystemExit(f"openpyxl is required to read the workbooks: {exc}")
    return openpyxl


def _sheet_rows(path: str, sheet: str) -> List[Tuple[Any, ...]]:
    wb = _load_openpyxl().load_workbook(path, read_only=True, data_only=True)
    try:
        return [tuple(r) for r in wb[sheet].iter_rows(values_only=True)]
    finally:
        wb.close()


# --------------------------------------------------------------------------
# Repository
# --------------------------------------------------------------------------
_JOB_TYPE_FIXES = {
    "White Collar Collar": "White Collar",
    "Blue Collar Collar": "Blue Collar",
}


def read_master(path: str) -> List[Dict[str, Optional[str]]]:
    """Every named "Master List" row as ``{column header: cleaned text}``."""
    rows = _sheet_rows(path, "Master List")
    header = [str(c).strip() if c is not None else "" for c in rows[0]]
    out: List[Dict[str, Optional[str]]] = []
    for raw in rows[1:]:
        rec = {h: _clean(v) for h, v in zip(header, raw) if h}
        if rec.get("PUBLISHER"):
            out.append(rec)
    return out


def _record(row: Dict[str, Optional[str]]) -> Dict[str, Any]:
    def txt(key: str) -> str:
        return row.get(key) or MISSING

    job_type = txt("JOB TYPE")
    return {
        "name": row["PUBLISHER"],
        "parent": row.get("PARENT COMPANY") or "",
        "priority": txt("PRIORITY"),
        "countries": txt("COUNTRIES"),
        "job_type": _JOB_TYPE_FIXES.get(job_type, job_type),
        "category": txt("PUBLISHER CATEGORY"),
        "job_categories": txt("JOB CATEGORY"),
        "pricing": txt("PUBLISHER TYPE"),
        "status": row.get("JOVEO NETWORK STATUS") or NO_STATUS,
        "easy_apply": txt("EASY APPLY CHECK"),
        "xml": txt("XML"),
        "_region": row.get("REGION") or UNSPECIFIED_REGION,
    }


def _apply_curated_fixes(rec: Dict[str, Any]) -> None:
    fixed = NAME_FIXES.get(rec["name"])
    if fixed:
        rec["name_previous"] = rec["name"]
        rec["name"] = fixed
    if rec["name"] in RESTRICTIONS:
        rec["restriction"] = RESTRICTIONS[rec["name"]]
    if rec["name"] in VETERINARY_BOARDS:
        rec["category"] = "Veterinary"
        if rec["job_categories"] == "Healthcare":
            rec["job_categories"] = "Veterinary"
    fix = COUNTRY_FIXES.get(rec["name"])
    if fix and rec["countries"] == fix[0]:
        rec["countries_previous"] = rec["countries"]
        rec["countries"] = fix[1]


def _network_stamp(rec: Dict[str, Any], network_member: Any) -> None:
    """Stamp a board that belongs to a confirmed board group
    (data/joveo_network_groups.json) with the group, the date and the origin of
    the confirmation.  The status flips to "In Joveo Network" only from a
    partner-route status.  A row the export marks Deactivated/Churned/Paused is
    left untouched (no stamp).  The newer export wins over the group
    confirmation there: ``network_conflicts`` (see ``find_network_conflicts``)
    lists every such board and country, the builder prints them, and
    ``publisher_filter.board_supply_record`` reads them as not in network."""
    if rec["status"] in INACTIVE_STATUSES:
        return  # the export's own Deactivated/Churned/Paused is not contradicted
    countries = [c.strip() for c in rec["countries"].split(",") if c.strip()]
    if rec["countries"] == MISSING:
        countries = []
    hit = None
    for country in countries or [None]:
        hit = network_member(rec["name"], country)
        if hit:
            break
    if not hit:
        return
    rec["network_group"] = hit["group"]
    rec["network_as_of"] = hit["as_of"]
    rec["network_source"] = hit["status_source"]
    if rec["status"] in STAMP_MAY_REPLACE:
        rec["status_previous"] = rec["status"]
        rec["status"] = IN_NETWORK


NON_NETWORK_STATUSES = INACTIVE_STATUSES + (EXCLUDED_STATUS,)


def find_network_conflicts(
    rows: Iterable[Dict[str, Optional[str]]], network_member: Any
) -> List[Dict[str, str]]:
    """Boards a confirmed network group (data/joveo_network_groups.json, dated
    2026-10-17) names but the NEWER export gives an explicit non-network status
    (Deactivated, Churned, Paused, VONQ Only -- the last are left out of the
    data, but the workbook shows them).  Owner decision: the newer export wins.

    ``[{"name", "country", "export_status", "as_of"}]``, one per board and
    country, sorted.  A row that lists no country is scoped to the countries
    of the group member it matches ("" when the member lists none).  A variant
    of a member's name ("StepStone Germany (Pro - Media Buy) - ...") matches
    through the name before its first parenthesis."""
    found: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        status = row.get("JOVEO NETWORK STATUS") or ""
        if status not in NON_NETWORK_STATUSES:
            continue
        rec = _record(row)
        _apply_curated_fixes(rec)
        name = rec["name"]
        base = re.split(r"\s*\(", name, maxsplit=1)[0].strip() or name
        listed = (
            []
            if rec["countries"] == MISSING
            else [c.strip() for c in rec["countries"].split(",") if c.strip()]
        )
        scopes: List[str] = []
        for country in listed or [None]:
            hit = network_member(name, country) or (
                network_member(base, country) if base != name else None
            )
            if not hit:
                continue
            if listed:
                scopes.append(country)
            else:
                scopes.extend(hit["countries"] or [""])
        for country in scopes:
            found[(name, country)] = {
                "name": name,
                "country": country,
                "export_status": status,
                "as_of": EXPORT_DATE,
            }
    return [found[k] for k in sorted(found, key=lambda k: (k[0].lower(), k[1]))]


def _public_fields(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def build_repository(rows: Iterable[Dict[str, Optional[str]]]) -> Dict[str, Any]:
    """The repository snapshot: ``source``, ``last_updated``,
    ``total_publishers``, ``active_publishers``, ``summary`` (by_priority,
    by_status, by_region, by_job_type), ``by_region`` ({REGION: {total,
    p0_count, p1_count, publishers}}), ``top_publishers_p0_p1`` and
    ``network_conflicts`` (see ``find_network_conflicts``)."""
    sys.path.insert(0, ROOT)
    import publisher_filter  # noqa: PLC0415 -- the app's own matcher

    rows = list(rows)
    conflicts = find_network_conflicts(rows, publisher_filter.network_member)
    records: List[Dict[str, Any]] = []
    for row in rows:
        if (row.get("JOVEO NETWORK STATUS") or "") == EXCLUDED_STATUS:
            continue
        rec = _record(row)
        _apply_curated_fixes(rec)
        _network_stamp(rec, publisher_filter.network_member)
        records.append(rec)

    by_region: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        block = by_region.setdefault(
            rec["_region"],
            {"total": 0, "p0_count": 0, "p1_count": 0, "publishers": []},
        )
        block["total"] += 1
        block["p0_count"] += rec["priority"] == "P0"
        block["p1_count"] += rec["priority"] == "P1"
        block["publishers"].append(_public_fields(rec))

    top = [
        {
            **{k: rec[k] for k in ("name", "priority")},
            "region": rec["_region"],
            **{k: rec[k] for k in ("countries", "job_type", "pricing", "status")},
            **{
                k: rec[k]
                for k in (
                    "network_group",
                    "network_as_of",
                    "network_source",
                    "countries_previous",
                )
                if k in rec
            },
        }
        for rec in records
        if rec["priority"] in ("P0", "P1")
    ]

    def tally(key: str, skip: Tuple[str, ...] = (MISSING,)) -> Dict[str, int]:
        counts = Counter(r[key] if key != "_region" else r["_region"] for r in records)
        ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        return {k: v for k, v in ordered if k not in skip}

    statuses = Counter(r["status"] for r in records)
    inactive = sum(statuses[s] for s in INACTIVE_STATUSES)
    return {
        "source": SOURCE_LABEL,
        "last_updated": EXPORT_DATE,
        "total_publishers": len(records),
        "active_publishers": len(records) - inactive,
        "summary": {
            "by_priority": tally("priority"),
            "by_status": tally("status", skip=()),
            "by_region": tally("_region", skip=(UNSPECIFIED_REGION,)),
            "by_job_type": tally("job_type"),
        },
        "by_region": by_region,
        "top_publishers_p0_p1": top,
        "network_conflicts": conflicts,
    }


# --------------------------------------------------------------------------
# Onboarding pipeline
# --------------------------------------------------------------------------
def _norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def read_vocab(path: str) -> Dict[str, Any]:
    """The tracker's own vocabulary (Lists sheet) and ordered stage list with
    exit criteria (Stages & Checklist sheet)."""
    lists = _sheet_rows(path, "Lists")
    header = [_clean(c) or "" for c in lists[0]]
    columns: Dict[str, List[str]] = {h: [] for h in header if h}
    for raw in lists[1:]:
        for h, v in zip(header, raw):
            text = _clean(v)
            if h and text:
                columns[h].append(text)
    stages = []
    for raw in _sheet_rows(path, "Stages & Checklist")[1:]:
        cells = list(raw) + [None] * 3
        name, criteria = _clean(cells[1]), _clean(cells[2])
        if name:
            stages.append({"name": name, "exit_criteria": criteria})
    return {"lists": columns, "stages": stages}


def _safe_text(text: Optional[str]) -> Optional[str]:
    """Free text ships only when it carries no e-mail address and none of the
    sensitive commercial words."""
    if text and not _EMAIL_RE.search(text) and not _SENSITIVE_RE.search(text):
        return text
    return None


def _regions(cell: Any, known: Dict[str, str]) -> List[str]:
    """Country/region names in a free-text region cell.  A token ships only if
    it is a country the repository already lists or a known short code; a
    contact name sitting in a shifted column is not."""
    text = _clean(cell)
    if not text or _EMAIL_RE.search(text):
        return []
    out: List[str] = []
    for token in re.split(r"[,/;\n]+", text):
        key = token.strip().lower().rstrip(".")
        if not key:
            continue
        name = REGION_ALIASES.get(key) or known.get(key)
        if name and name not in out:
            out.append(name)
    return out


def _test_flag(cell: Any) -> Optional[bool]:
    """Was a test campaign allocated?  A date, "Yes" or any note means yes
    ("No" means no, an empty cell is unknown).  The note text itself is never
    kept: it can name a client."""
    text = _clean(cell)
    if text is None:
        return None
    return text.lower() not in ("no", "n", "false")


def read_pipeline(path: str, known_regions: Dict[str, str]) -> Dict[str, Any]:
    vocab = read_vocab(path)
    lists = vocab["lists"]
    stage_vocab = {s["name"].lower(): s["name"] for s in vocab["stages"]}
    stage_vocab.update({v.lower(): v for v in lists.get("Stage", [])})
    status_vocab = {v.lower(): v for v in lists.get("Status", [])}
    type_vocab = {v.lower(): v for v in lists.get("Type", [])}
    type_vocab.update(EXTRA_TYPES)
    order = {s["name"]: i for i, s in enumerate(vocab["stages"])}

    merged: Dict[str, Dict[str, Any]] = {}
    for sheet in ("Pipeline", "Partner Pipeline"):
        rows = _sheet_rows(path, sheet)
        header = [_clean(c) or "" for c in rows[0]]
        col = {h.lower(): i for i, h in enumerate(header) if h}

        def cell(raw: Tuple[Any, ...], label: str) -> Any:
            i = col.get(label.lower())
            return raw[i] if i is not None and i < len(raw) else None

        for raw in rows[1:]:
            name = _clean(raw[0]) if raw else None
            if not name or _EMAIL_RE.search(name):
                continue
            # Stage and status are found by exact match against the tracker's
            # vocabulary anywhere in the row: columns are shifted on many rows
            # (a contact name or address can sit under "Current Stage"), and a
            # vocabulary hit cannot leak anything.
            texts = [_clean(c) for c in raw[1:]]
            stage = next(
                (
                    stage_vocab[t.lower()]
                    for t in texts
                    if t and t.lower() in stage_vocab
                ),
                None,
            )
            status = next(
                (
                    status_vocab[t.lower()]
                    for t in texts
                    if t and t.lower() in status_vocab
                ),
                None,
            )
            if stage is None:
                continue  # no recognised stage: nothing to say about onboarding
            # Columns past the region are trusted only on a row whose stage sits
            # under the "Current Stage" header; on a shifted row they are unknown.
            aligned = (
                _clean(cell(raw, "Current Stage")) or ""
            ).lower() == stage.lower()
            type_text = _clean(cell(raw, "Type")) if aligned else None
            job_text = _clean(cell(raw, "Job Category")) if aligned else None
            partner = {
                "name": _safe_text(name),
                "regions": _regions(cell(raw, "Primary Region(s)"), known_regions),
                "type": type_vocab.get((type_text or "").lower()),
                "job_category": JOB_CATEGORIES.get((job_text or "").lower()),
                "stage": stage,
                "status": status,
                "test_campaign_allocated": (
                    _test_flag(cell(raw, "Test Campaign Allocated"))
                    if aligned
                    else None
                ),
            }
            if not partner["name"]:
                continue
            key = _norm_name(name)
            prior = merged.get(key)
            if prior is None or _rank(partner, order) > _rank(prior, order):
                if prior is not None:
                    for field in ("regions", "type", "job_category"):
                        partner[field] = partner[field] or prior[field]
                merged[key] = partner
            else:
                for field in ("regions", "type", "job_category"):
                    prior[field] = prior[field] or partner[field]
    partners = sorted(
        merged.values(), key=lambda p: (order.get(p["stage"], 99), p["name"].lower())
    )
    stages = [
        {"name": s["name"], "exit_criteria": _safe_text(s["exit_criteria"])}
        for s in vocab["stages"]
    ]
    return {
        "source": PIPELINE_SOURCE_LABEL,
        "as_of": EXPORT_DATE,
        "scope": PRIVATE_SCOPE,
        "stages": stages,
        "partners": [{k: p[k] for k in PARTNER_KEYS} for p in partners],
    }


def public_pipeline(full: Dict[str, Any]) -> Dict[str, Any]:
    """The shippable slice of the sanitized pipeline: partners at "Contract
    signed" or any later stage except "Not pursued".  The stage list is kept
    whole.  A tracker without a "Contract signed" stage ships no partners."""
    names = [s["name"] for s in full["stages"]]
    if FIRST_PUBLIC_STAGE not in names:
        allowed: set = set()
    else:
        start = names.index(FIRST_PUBLIC_STAGE)
        allowed = {n for n in names[start:] if n != _STAGE_NOT_PURSUED}
    return {
        **full,
        "scope": PUBLIC_SCOPE,
        "partners": [p for p in full["partners"] if p["stage"] in allowed],
    }


def _rank(partner: Dict[str, Any], order: Dict[str, int]) -> Tuple[int, int]:
    """Which duplicate to keep: the furthest real stage ("Not pursued" ranks
    lowest), then the one that carries a status."""
    stage = partner["stage"]
    real = -1 if stage == _STAGE_NOT_PURSUED else order.get(stage, -1)
    return (real, 1 if partner["status"] else 0)


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------
def dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=True) + "\n"


def assert_clean(text: str, label: str, words: bool = True) -> None:
    """Refuse to write a file holding an e-mail address, or (``words``) one of
    the sensitive commercial words.  The repository file skips the word check:
    real board names contain "Commission" (a state veterans commission)."""
    if _EMAIL_RE.search(text):
        raise SystemExit(
            f"{label}: an e-mail-shaped string survived; refusing to write"
        )
    hit = _SENSITIVE_RE.search(text) if words else None
    if hit:
        raise SystemExit(
            f"{label}: sensitive word {hit.group(0)!r} survived; refusing to write"
        )


def known_region_names(repo: Dict[str, Any]) -> Dict[str, str]:
    names: Dict[str, str] = {}
    for block in repo["by_region"].values():
        for pub in block["publishers"]:
            for token in str(pub["countries"]).split(","):
                token = token.strip()
                if token and token != MISSING:
                    names[token.lower()] = token
    names.setdefault("europe", "Europe")
    return names


def sync_active_count(count: int) -> bool:
    """Set ``total_active_publishers`` in data/joveo_publishers.json (a line
    edit: the file's other content is left byte-for-byte alone)."""
    with open(PUBLISHERS_JSON, encoding="utf-8") as fh:
        text = fh.read()
    new, n = re.subn(
        r'("total_active_publishers":\s*)\d+',
        lambda m: f"{m.group(1)}{count}",
        text,
        count=1,
    )
    if n != 1:
        raise SystemExit(
            "data/joveo_publishers.json: total_active_publishers not found"
        )
    if new == text:
        return False
    with open(PUBLISHERS_JSON, "w", encoding="utf-8") as fh:
        fh.write(new)
    return True


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("master_list", help="Joveo Global Supply Repository.xlsx")
    parser.add_argument("onboarding_tracker", help="Publisher-Onboarding-Tracker.xlsx")
    parser.add_argument("--repository-out", default=REPOSITORY_OUT)
    parser.add_argument("--pipeline-out", default=PIPELINE_OUT)
    parser.add_argument(
        "--private-out",
        default=None,
        help="also write the FULL sanitized pipeline (all stages) here; keep it "
        "outside git",
    )
    parser.add_argument(
        "--no-sync-count",
        action="store_true",
        help="leave data/joveo_publishers.json alone",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    repo = build_repository(read_master(args.master_list))
    full = read_pipeline(args.onboarding_tracker, known_region_names(repo))
    pipeline = public_pipeline(full)
    repo_text, pipe_text = dumps(repo), dumps(pipeline)
    assert_clean(repo_text, "repository", words=False)
    assert_clean(pipe_text, "pipeline")
    outputs = [(args.repository_out, repo_text), (args.pipeline_out, pipe_text)]
    if args.private_out:
        private_text = dumps(full)
        assert_clean(private_text, "private pipeline")
        outputs.append((args.private_out, private_text))
    for path, text in outputs:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
    for c in repo["network_conflicts"]:
        print(
            f"network conflict: {c['name']} ({c['country'] or 'any country'}): "
            f"{c['export_status']} in the {EXPORT_DATE} export overrides the "
            "network group confirmation"
        )
    in_network = repo["summary"]["by_status"].get(IN_NETWORK, 0)
    changed = False if args.no_sync_count else sync_active_count(in_network)
    logger.info(
        "repository: %d publishers (%d active), %d in network; pipeline: %d public partners (%d in the full tracker); "
        "joveo_publishers.json total_active_publishers %s",
        repo["total_publishers"],
        repo["active_publishers"],
        in_network,
        len(pipeline["partners"]),
        len(full["partners"]),
        "updated" if changed else "unchanged",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
