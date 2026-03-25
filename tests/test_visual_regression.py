"""Visual regression tests using Playwright.

Captures screenshots of key pages on the live site and compares them
against stored baselines.  Uses the Playwright CLI (`npx playwright
screenshot`) so no Python bindings are required.

Usage:
    python tests/test_visual_regression.py              # capture + compare
    python tests/test_visual_regression.py --baseline   # save current as baseline
"""

import shutil
import subprocess
import sys
from pathlib import Path

SITE_URL = "https://media-plan-generator.onrender.com"
SCREENSHOT_DIR = Path(__file__).parent / "screenshots"
BASELINE_DIR = SCREENSHOT_DIR / "baseline"
CURRENT_DIR = SCREENSHOT_DIR / "current"

PAGES: list[tuple[str, str]] = [
    ("/", "homepage"),
    ("/platform", "platform"),
    ("/nova", "nova"),
    ("/media-plan", "media-plan"),
    ("/quick-plan", "quick-plan"),
    ("/competitive-intel", "competitive-intel"),
]


def capture_screenshots() -> list[str]:
    """Capture full-page screenshots of all pages using the Playwright CLI.

    Returns:
        List of page names that were successfully captured.
    """
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    captured: list[str] = []

    for path, name in PAGES:
        url = f"{SITE_URL}{path}"
        output = CURRENT_DIR / f"{name}.png"
        try:
            result = subprocess.run(
                ["npx", "playwright", "screenshot", "--full-page", url, str(output)],
                capture_output=True,
                timeout=30,
            )
            if output.exists():
                captured.append(name)
                print(f"  [OK] {name} ({output.stat().st_size:,} bytes)")
            else:
                stderr = result.stderr.decode("utf-8", errors="replace").strip()
                print(f"  [FAIL] {name}: no screenshot produced. {stderr}")
        except subprocess.TimeoutExpired:
            print(f"  [FAIL] {name}: timed out after 30s")
        except FileNotFoundError:
            print(
                f"  [FAIL] {name}: npx/playwright not found -- run `npm i playwright`"
            )
            break

    return captured


def save_baseline() -> None:
    """Copy current screenshots into the baseline directory."""
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for f in CURRENT_DIR.glob("*.png"):
        shutil.copy2(f, BASELINE_DIR / f.name)
        count += 1
    print(f"Saved {count} baseline screenshot(s) to {BASELINE_DIR}")


def compare_screenshots() -> dict[str, bool | None]:
    """Compare current screenshots against baselines.

    Uses a simple file-size heuristic: if the size difference is less
    than 10 % of the baseline, the page is considered unchanged.

    Returns:
        Dict mapping page name to True (pass), False (diff), or None
        (no baseline exists).
    """
    results: dict[str, bool | None] = {}

    for f in CURRENT_DIR.glob("*.png"):
        baseline = BASELINE_DIR / f.name
        if not baseline.exists():
            results[f.stem] = None  # No baseline yet
            continue

        current_size = f.stat().st_size
        baseline_size = baseline.stat().st_size
        diff_pct = abs(current_size - baseline_size) / max(baseline_size, 1) * 100
        results[f.stem] = diff_pct < 10  # Pass if < 10 % size difference

    return results


def main() -> None:
    """Entry point for the visual regression script."""
    is_baseline = "--baseline" in sys.argv

    print(f"Capturing screenshots from {SITE_URL} ...")
    captured = capture_screenshots()

    if not captured:
        print("No screenshots captured -- aborting.")
        sys.exit(1)

    if is_baseline:
        save_baseline()
    else:
        results = compare_screenshots()
        print("\nVisual regression results:")
        any_fail = False
        for name, passed in results.items():
            if passed is None:
                status = "NO BASELINE"
            elif passed:
                status = "PASS"
            else:
                status = "DIFF DETECTED"
                any_fail = True
            print(f"  {name}: {status}")

        if any_fail:
            print("\nSome pages have visual differences -- review manually.")
            sys.exit(1)
        elif not results:
            print("\nNo baselines found. Run with --baseline first.")
        else:
            print("\nAll pages match baseline.")


if __name__ == "__main__":
    main()
