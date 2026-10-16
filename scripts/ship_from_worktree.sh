#!/usr/bin/env bash
#
# ship_from_worktree.sh -- safe-ship protocol for concurrent sessions.
#
# Purpose: multiple Claude sessions push to this repo's main branch
# concurrently, and Render auto-deploys from main on every push. This
# script is the one safe way to land a feature branch from a linked
# worktree: rebase onto origin/main, run the full test suite, wait out a
# stability window to make sure main hasn't moved again, then
# fast-forward-push -- retrying (never force-pushing) if another session
# wins the race, and giving up loudly if main stays too hot.
#
# GitHub CI gate: the local suite is not the only gate. "Nova AI Suite CI"
# runs on every push to main, and main sat red for two weeks (2026-10-01 ..
# 2026-10-15, a Linux-only argv limit) while every run of this script stayed
# green. So the script now (a) prints a loud WARNING at the start if the
# latest completed CI run on main is not green -- it never blocks, because the
# change being shipped may be the fix -- and (b) after the push and the deploy
# check, finds the CI run for the pushed commit and waits for it (`gh run
# watch`), failing with exit 6 if CI goes red. Needs an authenticated `gh`;
# without one it warns and carries on (the ship itself already succeeded).
#
# Ship lock: ships queue machine-wide. Several sessions running their full
# suites at once drove this Mac to load average 77-141 with ~277 pytest
# processes (2026-10-16), and a ship then failed its suite gate on two
# wall-clock timing tests that pass alone in 14 s. So the rebase -> suite ->
# stability window -> push section runs under an exclusive flock on
# $SHIP_LOCK_FILE (/tmp/mpg-ship.lock). A waiting ship logs "waiting for
# another ship (held by pid N since T)" every 30 s and gives up with exit 7
# after SHIP_LOCK_TIMEOUT_S. The lock is a kernel flock held by a tiny helper
# that exits when this script does, however it dies (even kill -9), so there
# is never a stale lock to clean up. It is released the moment the push
# lands -- deploy verification and the CI watch run outside it.
#
# Usage (from inside a linked worktree, on a feature branch, clean tree):
#   scripts/ship_from_worktree.sh
#   scripts/ship_from_worktree.sh --ci-check-only [<sha>]   # dry run of the
#       CI gate alone: pre-flight warning for main + wait for <sha>'s CI run
#       (default: current origin/main). No rebase, no tests, no push; runs
#       from any checkout.
#   scripts/ship_from_worktree.sh --lock-only [<hold_seconds>]   # dry run of
#       the ship lock alone: queue for it, hold it <hold_seconds> (default 0),
#       release. Exit 0 acquired | 7 timed out.
#
# Environment:
#   SHIP_LOCK_FILE        lock path (default /tmp/mpg-ship.lock -- a fixed
#       path on purpose: $TMPDIR differs between sessions, which would give
#       each its own lock and queue nobody).
#   SHIP_LOCK_TIMEOUT_S   give up waiting for the lock after this many seconds
#       (default 1800). SHIP_LOCK_WAIT_LOG_S: seconds between "waiting" log
#       lines (default 30).
#   SHIP_SKIP_CI_WATCH=1   skip the post-push CI watch (the skip is logged).
#   SHIP_CI_APPEAR_POLL_INTERVAL / SHIP_CI_APPEAR_POLL_MAX   seconds between
#       polls / total wait for the CI run to show up (defaults 10 / 180).
#   SHIP_CI_WATCH_INTERVAL / SHIP_CI_WATCH_MAX   `gh run watch` refresh
#       interval / give-up point in seconds (defaults 30 / 1200). CI takes
#       ~7-8 min, and it runs while the Render deploy is verified, so the
#       usual added wait is the remainder after the deploy check.
#
# Exit codes: 0 shipped | 1 preflight failed | 2 rebase conflict (resolve
# manually) | 3 test failure | 4 origin/main too hot after 3 attempts |
# 5 post-push ancestor check failed (should be impossible -- investigate) |
# 6 pushed, but GitHub CI for the pushed commit FAILED (the commit is already
# on main and deploying -- fix forward or revert; failing jobs are printed).
# CI that never appears, times out, or can't be queried is a loud WARNING,
# not a failure: only a run that finished red returns 6. | 7 gave up waiting
# for the machine-wide ship lock held by another ship (nothing was changed;
# re-run when the other ship finishes, or raise SHIP_LOCK_TIMEOUT_S).

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

MAX_ATTEMPTS=3
STABILITY_CHECKS=3
STABILITY_INTERVAL_SECONDS=30
DEPLOY_POLL_INTERVAL_SECONDS=30
DEPLOY_POLL_MAX_SECONDS=360
DEPLOY_READY_URL="https://media-plan-generator.onrender.com/api/deploy/ready"
CI_APPEAR_POLL_INTERVAL_SECONDS="${SHIP_CI_APPEAR_POLL_INTERVAL:-10}"
CI_APPEAR_POLL_MAX_SECONDS="${SHIP_CI_APPEAR_POLL_MAX:-180}"
CI_WATCH_INTERVAL_SECONDS="${SHIP_CI_WATCH_INTERVAL:-30}"
CI_WATCH_MAX_SECONDS="${SHIP_CI_WATCH_MAX:-1200}"
SHIP_LOCK_FILE="${SHIP_LOCK_FILE:-/tmp/mpg-ship.lock}"
SHIP_LOCK_TIMEOUT_S="${SHIP_LOCK_TIMEOUT_S:-1800}"
SHIP_LOCK_WAIT_LOG_S="${SHIP_LOCK_WAIT_LOG_S:-30}"

log() {
    printf '[ship] %s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1" >&2
}

# ── Machine-wide ship lock ───────────────────────────────────────────────
# macOS ships no flock(1) and bash 3.2 has no coproc, so a python3 helper
# (already a dependency of this script) takes fcntl.flock on SHIP_LOCK_FILE
# and holds it until this script's shell is gone: it polls its parent pid
# once a second, so SIGKILL of the script still frees the lock within ~1 s.
# The kernel drops a flock when its holder exits -- no stale-lock detection,
# no steal race. Holder info "pid|since|branch" is written into the lock file
# after acquiring, so waiters can say who they are queued behind. The helper
# reports on stdout (a status file we poll): WAIT|<holder>, ACQUIRED, TIMEOUT.
lock_helper_pid=""
lock_status_file=""

# Returns 0 once the lock is held -- or, with a loud WARNING, when the lock
# machinery itself is broken (a ship must never become impossible because
# /tmp is unwritable). Exits 7 on timeout.
ship_lock_acquire() {
    local branch="$1" seen=0 total line alive kind rest hpid hsince hbranch queued_since
    queued_since="$(date +%s)"
    lock_status_file="$(mktemp "${TMPDIR:-/tmp}/mpg-ship-lock-status.XXXXXX")"
    python3 -c '
import fcntl, os, sys, time
path, timeout, parent, wait_log, info = sys.argv[1:6]
timeout, parent, wait_log = int(timeout), int(parent), int(wait_log)


def say(line):
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
deadline = time.time() + timeout
last = None
while True:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        break
    except OSError:
        pass
    if os.getppid() != parent:
        sys.exit(0)
    now = time.time()
    if now >= deadline:
        say("TIMEOUT")
        sys.exit(0)
    if last is None or now - last >= wait_log:
        say("WAIT|" + os.pread(fd, 512, 0).decode("utf-8", "replace").strip())
        last = now
    time.sleep(1)
os.ftruncate(fd, 0)
os.pwrite(fd, info.encode("utf-8"), 0)
say("ACQUIRED")
while os.getppid() == parent:
    time.sleep(1)
' "$SHIP_LOCK_FILE" "$SHIP_LOCK_TIMEOUT_S" "$$" "$SHIP_LOCK_WAIT_LOG_S" \
        "$$|$(date -u '+%Y-%m-%dT%H:%M:%SZ')|$branch" >"$lock_status_file" 2>/dev/null &
    lock_helper_pid=$!
    while :; do
        # Sample liveness BEFORE reading: a helper that writes its last line
        # and exits between the two would otherwise look like it just died.
        alive=true
        kill -0 "$lock_helper_pid" 2>/dev/null || alive=false
        total="$(wc -l <"$lock_status_file" | tr -d ' ')"
        while [ "$seen" -lt "$total" ]; do
            seen=$((seen + 1))
            line="$(sed -n "${seen}p" "$lock_status_file")"
            kind="${line%%|*}"
            rest="${line#*|}"
            case "$kind" in
                ACQUIRED)
                    log "ship lock acquired ($SHIP_LOCK_FILE, queued $(($(date +%s) - queued_since))s)"
                    return 0
                    ;;
                TIMEOUT)
                    lock_helper_pid=""
                    echo "ERROR: gave up after ${SHIP_LOCK_TIMEOUT_S}s waiting for another ship to finish ($SHIP_LOCK_FILE). Nothing was changed -- re-run when it is done, or raise SHIP_LOCK_TIMEOUT_S." >&2
                    exit 7
                    ;;
                WAIT)
                    if [ "$rest" = "$line" ] || [ -z "$rest" ]; then
                        log "waiting for another ship (holder unknown, queued $(($(date +%s) - queued_since))s)"
                    else
                        IFS='|' read -r hpid hsince hbranch <<<"$rest"
                        log "waiting for another ship (held by pid ${hpid:-?} since ${hsince:-?}${hbranch:+, branch $hbranch}; queued $(($(date +%s) - queued_since))s)"
                    fi
                    ;;
            esac
        done
        if [ "$alive" = "false" ]; then
            lock_helper_pid=""
            log "WARNING: ship-lock helper exited without acquiring (is $SHIP_LOCK_FILE writable?) -- proceeding WITHOUT the machine-wide lock"
            return 0
        fi
        sleep 1
    done
}

# Idempotent; safe from the EXIT trap. Killing the helper closes its fd, which
# is what releases the flock.
ship_lock_release() {
    if [ -n "$lock_helper_pid" ]; then
        kill "$lock_helper_pid" 2>/dev/null || true
        wait "$lock_helper_pid" 2>/dev/null || true
        lock_helper_pid=""
        log "ship lock released"
    fi
    if [ -n "$lock_status_file" ]; then
        rm -f "$lock_status_file"
        lock_status_file=""
    fi
}

# Current remote main SHA; retries transient failures. Prints the SHA on
# success; returns 1 (prints nothing) after 3 failed tries.
remote_main_sha() {
    local sha i
    for i in 1 2 3; do
        if sha="$(git ls-remote origin refs/heads/main 2>/dev/null | cut -f1)" && [ -n "$sha" ]; then
            printf '%s\n' "$sha"
            return 0
        fi
        sleep 5
    done
    return 1
}

# Runs on every normal or `exit N` path: free the ship lock and the per-run
# slot dir. Deliberately NOT trapping INT/TERM/HUP: bash defers a trapped
# signal until its foreground child (the ~10 min pytest run) finishes, which
# would make a ship unkillable mid-suite. Death by signal skips this trap
# instead, and that is safe: the lock helper notices its parent is gone and
# frees the flock within a second.
cleanup() {
    ship_lock_release
    if [ -n "${NOVA_SLOT_DIR:-}" ]; then
        rm -rf "$NOVA_SLOT_DIR"
    fi
}
trap cleanup EXIT

# ── GitHub CI gate ───────────────────────────────────────────────────────
# Every function here degrades to a logged WARNING when gh is missing,
# unauthenticated, or slow -- the ship must never fail for a CI-query reason.
# Only ci_watch_for_sha returning 6 means "CI ran and went red".

# Warn (never block) if the newest COMPLETED push-triggered run on main is not
# green. An in-progress run newer than it is mentioned but does not hide it.
ci_preflight_warn() {
    local runs verdict state sha url conclusion pending
    if ! command -v gh >/dev/null 2>&1; then
        log "WARNING: gh is not installed -- cannot check main's GitHub CI status (not blocking)"
        return 0
    fi
    if ! runs="$(gh run list --branch main --event push --limit 10 --json status,conclusion,headSha,url 2>/dev/null)"; then
        log "WARNING: could not query GitHub CI status for main (gh failed or is not authenticated) -- not blocking"
        return 0
    fi
    verdict="$(printf '%s' "$runs" | python3 -c '
import json, sys
try:
    runs = json.load(sys.stdin)
    pending, latest = "", None
    for r in runs:
        if r.get("status") == "completed":
            latest = r
            break
        pending = pending or (r.get("url") or "")
    if latest is None:
        print("NONE||||" + pending)
    else:
        state = "OK" if latest.get("conclusion") == "success" else "RED"
        print("|".join([state, latest.get("headSha") or "", latest.get("url") or "",
                        latest.get("conclusion") or "", pending]))
except Exception:
    print("ERROR||||")
' 2>/dev/null)" || verdict="ERROR||||"
    IFS='|' read -r state sha url conclusion pending <<<"$verdict"
    case "$state" in
        OK)
            log "GitHub CI on main is green (latest completed run: ${sha:0:7} $url)"
            ;;
        RED)
            log "=================================================================="
            log "WARNING: GitHub CI on main is NOT GREEN -- latest completed run"
            log "  concluded '$conclusion' at ${sha:0:7}: $url"
            log "  Not blocking (this change may be the fix). The post-push CI watch"
            log "  will say whether main recovers; exit 6 if it does not."
            log "=================================================================="
            ;;
        NONE)
            log "WARNING: no completed GitHub CI run found on main -- cannot tell if main is green"
            ;;
        *)
            log "WARNING: could not read GitHub CI status for main -- not blocking"
            ;;
    esac
    if [ -n "${pending:-}" ]; then
        log "  (a newer CI run on main is still in progress: $pending)"
    fi
    return 0
}

# Print "<databaseId>|<url>" per push-triggered run of <sha> on main; return 1
# when the query itself failed and 2 when it succeeded but no run exists yet.
ci_runs_for_sha() {
    local sha="$1" out lines
    if ! out="$(gh run list --branch main --commit "$sha" --event push --json databaseId,url 2>/dev/null)"; then
        return 1
    fi
    lines="$(printf '%s' "$out" | python3 -c '
import json, sys
try:
    for r in json.load(sys.stdin):
        print("%s|%s" % (r.get("databaseId"), r.get("url") or ""))
except Exception:
    pass
' 2>/dev/null)" || lines=""
    if [ -z "$lines" ]; then
        return 2
    fi
    printf '%s\n' "$lines"
}

# Print the names of non-green jobs of run <id>, one "  - name (conclusion)"
# per line. Prints nothing when the jobs can't be read.
ci_failed_jobs() {
    gh run view "$1" --json jobs 2>/dev/null </dev/null | python3 -c '
import json, sys
try:
    for j in json.load(sys.stdin).get("jobs") or []:
        if j.get("conclusion") not in ("success", "skipped", "neutral", None, ""):
            print("  - %s (%s)" % (j.get("name"), j.get("conclusion")))
except Exception:
    pass
' 2>/dev/null || true
}

# Watch one run to completion, bounded by CI_WATCH_MAX_SECONDS. Returns
# 0 green | 1 red (failing jobs printed) | 2 state unknown (warned).
ci_watch_one() {
    local run_id="$1" run_url="$2" pid waited=0 watch_rc=0 view conclusion status jobs
    gh run watch "$run_id" --exit-status --interval "$CI_WATCH_INTERVAL_SECONDS" >&2 </dev/null &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge "$CI_WATCH_MAX_SECONDS" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            log "WARNING: CI run still unfinished after ${CI_WATCH_MAX_SECONDS}s -- stopped watching, CI NOT verified: $run_url"
            return 2
        fi
        sleep 1
        waited=$((waited + 1))
    done
    wait "$pid" || watch_rc=$?

    # `gh run watch` also exits non-zero on its own network errors, so the
    # run's recorded conclusion -- not the watch's exit code -- decides red.
    view="$(gh run view "$run_id" --json status,conclusion 2>/dev/null </dev/null | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    print("%s|%s" % (d.get("status") or "", d.get("conclusion") or ""))
except Exception:
    print("|")
' 2>/dev/null)" || view="|"
    IFS='|' read -r status conclusion <<<"$view"
    if [ "$status" = "completed" ]; then
        case "$conclusion" in
            success | skipped | neutral)
                log "CI green: $run_url"
                return 0
                ;;
            *)
                jobs="$(ci_failed_jobs "$run_id")"
                log "CI FAILED (conclusion: $conclusion): $run_url"
                if [ -n "$jobs" ]; then
                    printf '[ship] failing jobs:\n%s\n' "$jobs" >&2
                fi
                return 1
                ;;
        esac
    fi
    if [ "$watch_rc" -eq 0 ]; then
        log "CI green: $run_url (per gh run watch; run detail unreadable)"
        return 0
    fi
    log "WARNING: could not determine CI result (gh run watch exit $watch_rc, status '${status:-unknown}') -- CI NOT verified: $run_url"
    return 2
}

# Wait for <sha>'s CI run on main to appear (CI_APPEAR_POLL_MAX_SECONDS), then
# watch it. Returns 6 only if CI finished red; everything else returns 0.
ci_watch_for_sha() {
    local sha="$1" waited=0 failures=0 runs rc run_id run_url overall=0
    if [ "${SHIP_SKIP_CI_WATCH:-0}" = "1" ]; then
        log "CI watch SKIPPED (SHIP_SKIP_CI_WATCH=1): ${sha:0:7} is on main but its GitHub CI result was NOT checked"
        return 0
    fi
    if ! command -v gh >/dev/null 2>&1; then
        log "WARNING: gh is not installed -- GitHub CI for ${sha:0:7} NOT watched"
        return 0
    fi
    log "waiting for GitHub CI to pick up ${sha:0:7} (up to ${CI_APPEAR_POLL_MAX_SECONDS}s)"
    while :; do
        rc=0
        runs="$(ci_runs_for_sha "$sha")" || rc=$?
        if [ "$rc" -eq 0 ]; then
            break
        fi
        if [ "$rc" -eq 1 ]; then
            failures=$((failures + 1))
            if [ "$failures" -ge 3 ]; then
                log "WARNING: gh could not list CI runs 3 times (unauthenticated or offline?) -- GitHub CI for ${sha:0:7} NOT watched"
                return 0
            fi
        fi
        if [ "$waited" -ge "$CI_APPEAR_POLL_MAX_SECONDS" ]; then
            log "WARNING: no GitHub CI run for ${sha:0:7} appeared within ${CI_APPEAR_POLL_MAX_SECONDS}s -- CI NOT verified"
            return 0
        fi
        sleep "$CI_APPEAR_POLL_INTERVAL_SECONDS"
        waited=$((waited + CI_APPEAR_POLL_INTERVAL_SECONDS))
    done
    while IFS='|' read -r run_id run_url; do
        [ -n "$run_id" ] || continue
        log "GitHub CI run for ${sha:0:7}: $run_url"
        rc=0
        ci_watch_one "$run_id" "$run_url" || rc=$?
        if [ "$rc" -eq 1 ]; then
            overall=6
        fi
    done <<<"$runs"
    if [ "$overall" -eq 6 ]; then
        echo "ERROR: ${sha:0:7} is on main but its GitHub CI FAILED. It is already deploying -- fix forward or revert; main stays red until then." >&2
    fi
    return "$overall"
}

# ── Subcommands / argument check (before any ship-path side effect) ───────
case "${1:-}" in
    "") ;;
    --ci-check-only)
        # Dry run of the CI gate: no worktree/tree requirements, no rebase,
        # no tests, no push. Same code path the real ship uses.
        target_sha="${2:-}"
        if [ -z "$target_sha" ]; then
            target_sha="$(remote_main_sha)" || {
                echo "ERROR: --ci-check-only: could not read origin/main and no <sha> given." >&2
                exit 1
            }
        fi
        ci_preflight_warn
        ci_rc=0
        ci_watch_for_sha "$target_sha" || ci_rc=$?
        exit "$ci_rc"
        ;;
    --lock-only)
        # Dry run of the ship lock: queue, hold, release. Same code path the
        # real ship uses; nothing else is touched.
        ship_lock_acquire "lock-only"
        sleep "${2:-0}"
        ship_lock_release
        exit 0
        ;;
    *)
        echo "ERROR: unknown argument '$1'. Usage: scripts/ship_from_worktree.sh [--ci-check-only [<sha>] | --lock-only [<hold_seconds>]]" >&2
        exit 1
        ;;
esac

# ── Preflight ────────────────────────────────────────────────────────────
git_dir="$(git rev-parse --git-dir)"
case "$git_dir" in
    */worktrees/*) ;;
    *)
        echo "ERROR: not running inside a linked worktree (git-dir: $git_dir). Refusing to ship from the shared main checkout." >&2
        exit 1
        ;;
esac

if [ -n "$(git status --porcelain)" ]; then
    echo "ERROR: working tree is not clean. Commit or discard changes before shipping." >&2
    exit 1
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$current_branch" = "main" ]; then
    echo "ERROR: refusing to ship from main directly -- run this from a feature branch." >&2
    exit 1
fi

log "preflight OK: linked worktree, clean tree, branch=$current_branch"

# Surface a red main BEFORE the ~10 min suite run, not after (never blocks).
ci_preflight_warn

# ── Per-run test isolation ──────────────────────────────────────────────
# app._CrossProcessSlots keys its flock slot dir on $NOVA_SLOT_DIR, falling
# back to $TMPDIR/nova_gen_slots_{PORT|TEST_PORT|dev} -- and it's built once
# at module import, so these must be exported before pytest starts. A
# hardcoded TEST_PORT here would make every concurrent ship run share the
# same slot pool (a 2-slot pool) and starve each other's
# tests/test_generate_concurrency.py with 429s. Each run gets its own port
# and its own slot directory instead.
TEST_PORT="$(( 50000 + ($$ % 10000) ))"
NOVA_SLOT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nova_gen_slots_ship.XXXXXX")"
export TEST_PORT NOVA_SLOT_DIR

# Sweep stale slot dirs leaked by runs killed before their EXIT trap fired.
# Scoped ONLY to our own mktemp namespace (nova_gen_slots_ship.*) and ONLY
# to dirs older than 24h, excluding the one we just created. Never sweep
# port-keyed dirs (nova_gen_slots_59999, nova_gen_slots_dev, nova_gen_slots_<port>)
# -- those can belong to a live dev server or an in-flight old-version ship,
# and deleting files out from under an active flock silently breaks mutual
# exclusion for whoever holds it.
find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'nova_gen_slots_ship.*' -mmin +1440 ! -path "$NOVA_SLOT_DIR" -exec rm -rf {} + 2>/dev/null || true

# ── Ship loop ────────────────────────────────────────────────────────────
# Queue behind any other ship on this machine (exit 7 on timeout); released
# right after the push lands, or by the EXIT trap on any other way out (a
# signal death is covered by the helper's parent-pid watch).
ship_lock_acquire "$current_branch"

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
    # Reset the private slot dir BETWEEN attempts. The cross-process Voyage
    # rate windows (vector_search) persist reservations as files in
    # NOVA_SLOT_DIR; with one dir per RUN, attempt N's suite inherits attempt
    # N-1's reservations, and any rerank/embed test that expects an empty
    # window fails through no fault of the change being shipped (observed
    # 2026-07-21: attempts 1-2 green, attempt 3 failed 4 rerank unit tests
    # with "shared rate window full" because attempt 2's reservations were
    # still inside the 60s window). The dir is private to this run and no
    # suite is executing between attempts, so clearing it is safe.
    find "$NOVA_SLOT_DIR" -mindepth 1 -delete 2>/dev/null || true
    log "attempt $attempt/$MAX_ATTEMPTS: fetching origin"
    git fetch origin

    base="$(git rev-parse origin/main)"
    log "origin/main is at $base"

    if ! git rebase origin/main; then
        git rebase --abort || true
        echo "ERROR: rebase of $current_branch onto origin/main ($base) hit a conflict. Resolve manually (git rebase origin/main, fix conflicts, git rebase --continue), then re-run this script." >&2
        exit 2
    fi
    log "rebased $current_branch onto $base cleanly"

    log "running full test suite (TEST_PORT=$TEST_PORT NOVA_SLOT_DIR=$NOVA_SLOT_DIR pytest tests/ -q)"
    if ! python3 -m pytest tests/ -q; then
        echo "ERROR: test suite failed after rebasing onto $base. Fix the failure before shipping -- not retrying automatically." >&2
        exit 3
    fi
    log "test suite green"

    # Stability window: origin/main must hold still while we're about to push.
    stable=true
    check=1
    while [ "$check" -le "$STABILITY_CHECKS" ]; do
        if ! remote_sha="$(remote_main_sha)"; then
            log "remote unreachable during stability window -- restarting attempt"
            attempt=$((attempt + 1))
            continue 2
        fi
        if [ "$remote_sha" != "$base" ]; then
            log "stability check $check/$STABILITY_CHECKS: origin/main moved ($base -> $remote_sha)"
            stable=false
            break
        fi
        log "stability check $check/$STABILITY_CHECKS: origin/main still at $base"
        if [ "$check" -lt "$STABILITY_CHECKS" ]; then
            sleep "$STABILITY_INTERVAL_SECONDS"
        fi
        check=$((check + 1))
    done

    if [ "$stable" != "true" ]; then
        log "origin/main moved during the stability window -- starting next attempt"
        attempt=$((attempt + 1))
        continue
    fi

    log "final remote re-check before push"
    if ! final_sha="$(remote_main_sha)"; then
        log "remote unreachable during final pre-push check -- starting next attempt"
        attempt=$((attempt + 1))
        continue
    fi
    if [ "$final_sha" != "$base" ]; then
        log "origin/main moved just before push ($base -> $final_sha) -- starting next attempt"
        attempt=$((attempt + 1))
        continue
    fi

    local_head="$(git rev-parse HEAD)"
    log "pushing $local_head to origin main (no force)"
    if ! git push origin HEAD:main; then
        log "push rejected (origin/main moved under us) -- starting next attempt"
        attempt=$((attempt + 1))
        continue
    fi

    # Landed. Free the queue now: deploy verification and the CI watch below
    # take minutes and must not hold up the next ship's rebase and suite.
    ship_lock_release

    # The push's own exit code is the primary success signal -- from here
    # on, a succeeded push must never cause a non-zero exit.
    if pushed_sha="$(remote_main_sha)"; then
        if ! git fetch origin 2>/dev/null; then
            echo "PUSH SUCCEEDED ($local_head) but post-push remote verification was unreachable -- verify manually." >&2
            exit 0
        fi
        if git merge-base --is-ancestor HEAD origin/main; then
            log "SHIPPED: $pushed_sha is now origin/main"
        else
            echo "ERROR: push to $pushed_sha succeeded but HEAD ($local_head) is not an ancestor of origin/main. This should be impossible after a fast-forward push -- investigate manually." >&2
            exit 5
        fi
    else
        echo "PUSH SUCCEEDED ($local_head) but post-push remote verification was unreachable -- verify manually." >&2
        exit 0
    fi

    # ── Best-effort deploy verification (warn-only, never fails the ship) ──
    set +e
    elapsed=0
    deployed=false
    while [ "$elapsed" -lt "$DEPLOY_POLL_MAX_SECONDS" ]; do
        resp="$(curl -sS --max-time 10 "$DEPLOY_READY_URL" 2>/dev/null)"
        live_commit="$(printf '%s' "$resp" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("git_commit", ""))
except Exception:
    print("")
' 2>/dev/null)"
        if [ "$live_commit" = "$pushed_sha" ]; then
            deployed=true
            break
        fi
        sleep "$DEPLOY_POLL_INTERVAL_SECONDS"
        elapsed=$((elapsed + DEPLOY_POLL_INTERVAL_SECONDS))
    done
    set -e

    if [ "$deployed" = "true" ]; then
        log "deploy verified: Render is serving $pushed_sha"
    else
        log "WARNING: deploy not confirmed within ${DEPLOY_POLL_MAX_SECONDS}s -- Render may still be deploying, or this poll couldn't reach it. The ship itself succeeded; check $DEPLOY_READY_URL manually."
    fi

    # ── GitHub CI gate: our own pushed commit's run, not whatever main's tip is
    # by now (another session may already have pushed on top of us). ──
    ci_rc=0
    ci_watch_for_sha "$local_head" || ci_rc=$?
    if [ "$ci_rc" -ne 0 ]; then
        exit "$ci_rc"
    fi

    echo "$pushed_sha"
    exit 0
done

echo "ERROR: origin/main is too hot -- coordinate with the other sessions." >&2
exit 4
