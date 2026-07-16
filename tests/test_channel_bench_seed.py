"""Seed <-> funnel-invariant fixture identity guard.

Background: 38ac22a fixed the cold-start data gap by shipping
``data/channel_benchmarks_seed.json`` (copied onto the gitignored live
filename at app.py import time by ``data_seeds.seed_runtime_data_files()``).
That seed is byte-identical, today, to
``tests/fixtures/funnel_invariant/channel_benchmarks_live.json`` -- the
fixture that ``tests/test_funnel_calibration.py``'s
``TestHeadlineInvariance`` pins its HARD INVARIANT headline numbers against
(e.g. Manpower total applications 18950, Atria 17780). The fixture started
as a frozen 2026-07-12 snapshot; 336480d re-baselined both the seed and the
fixture together to a 2026-07-16 web-researched refresh (job_boards figures
replacing the prior LLM-generated data), keeping this test's byte-identity
guard intact -- this test enforces the mechanism (seed <-> fixture
identity), not any one snapshot's vintage.

Nothing couples the two files. Without this guard, the seed could drift
from the approved calibration snapshot while every other test stays green
-- prod would then serve numbers the invariant test no longer protects,
which is exactly the recurrence class this repo's July-12 calibration
program exists to prevent.

If you need to refresh the seed to a newer live snapshot, you MUST either
(a) update the fixture too and re-baseline ``TestHeadlineInvariance`` with a
documented justification for the new expected numbers, or (b) consciously
remove this assertion with a comment explaining why the coupling no longer
applies. Do not let this test go red and "fix" it by just re-syncing the
bytes without touching the invariant.

Adapted from the superseded ``fix/cpc-coldstart-seed`` branch (commit
c7b0143, ``tests/test_channel_bench_seed.py::
test_seed_is_byte_identical_to_funnel_invariant_fixture``), retargeted from
that branch's ``data/channel_benchmarks_live.seed.json`` to the filename
38ac22a actually shipped: ``data/channel_benchmarks_seed.json``.

Runs under pytest, or standalone:
``python3 tests/test_channel_bench_seed.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SEED_PATH = PROJECT_ROOT / "data" / "channel_benchmarks_seed.json"
FIXTURE_PATH = (
    PROJECT_ROOT / "tests" / "fixtures" / "funnel_invariant" / "channel_benchmarks_live.json"
)


def test_seed_is_byte_identical_to_funnel_invariant_fixture():
    """data/channel_benchmarks_seed.json MUST stay byte-identical to
    tests/fixtures/funnel_invariant/channel_benchmarks_live.json. See this
    module's docstring for why the coupling exists and what to do if you
    intentionally need to refresh the seed.
    """
    assert SEED_PATH.exists(), f"seed file missing: {SEED_PATH}"
    assert FIXTURE_PATH.exists(), f"funnel invariant fixture missing: {FIXTURE_PATH}"
    seed_bytes = SEED_PATH.read_bytes()
    fixture_bytes = FIXTURE_PATH.read_bytes()
    assert seed_bytes == fixture_bytes, (
        "data/channel_benchmarks_seed.json has drifted from "
        "tests/fixtures/funnel_invariant/channel_benchmarks_live.json -- "
        "see this test's module docstring before resyncing."
    )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
