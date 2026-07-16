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
# Usage (from inside a linked worktree, on a feature branch, clean tree):
#   scripts/ship_from_worktree.sh
#
# Exit codes: 0 shipped | 1 preflight failed | 2 rebase conflict (resolve
# manually) | 3 test failure | 4 origin/main too hot after 3 attempts |
# 5 post-push ancestor check failed (should be impossible -- investigate).

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

MAX_ATTEMPTS=3
STABILITY_CHECKS=3
STABILITY_INTERVAL_SECONDS=30
DEPLOY_POLL_INTERVAL_SECONDS=30
DEPLOY_POLL_MAX_SECONDS=360
DEPLOY_READY_URL="https://media-plan-generator.onrender.com/api/deploy/ready"

log() {
    printf '[ship] %s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1" >&2
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

# ── Ship loop ────────────────────────────────────────────────────────────
attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
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

    log "running full test suite (TEST_PORT=59999 pytest tests/ -q)"
    if ! TEST_PORT=59999 python3 -m pytest tests/ -q; then
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

    echo "$pushed_sha"
    exit 0
done

echo "ERROR: origin/main is too hot -- coordinate with the other sessions." >&2
exit 4
