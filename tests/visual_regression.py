"""Visual regression test suite for Nova AI Suite.

Uses Playwright (Python) for browser automation and Pillow for pixel-level
image comparison. Captures full-page screenshots of key pages on the live
site and compares them against stored baselines.

Usage:
    python tests/visual_regression.py                 # capture + compare
    python tests/visual_regression.py --baseline      # save current as baseline
    python tests/visual_regression.py --pages /hub /nova  # test specific pages only

Requirements:
    pip install playwright Pillow
    playwright install chromium
"""

from __future__ import annotations

import logging
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SITE_URL = "https://media-plan-generator.onrender.com"

SCREENSHOT_DIR = Path(__file__).resolve().parent / "screenshots"
BASELINE_DIR = Path(__file__).resolve().parent / "baselines"
CURRENT_DIR = SCREENSHOT_DIR / "current"

# Pixel diff threshold: percentage of pixels that may differ before FAIL
DIFF_THRESHOLD_PCT = 1.0  # 1% tolerance

# Navigation timeout per page (ms)
NAV_TIMEOUT_MS = 60_000

# ---------------------------------------------------------------------------
# Page definitions
# ---------------------------------------------------------------------------


@dataclass
class PageSpec:
    """Specification for a page to screenshot."""

    path: str
    name: str
    width: int = 1280
    height: int = 800
    full_page: bool = True
    wait_selector: Optional[str] = None  # Extra selector to wait for

    @property
    def filename(self) -> str:
        """Return the screenshot filename."""
        return f"{self.name}_{self.width}x{self.height}.png"


# Desktop pages (1280x800)
DESKTOP_PAGES: list[PageSpec] = [
    PageSpec(path="/", name="homepage"),
    PageSpec(path="/hub", name="hub"),
    PageSpec(path="/platform", name="platform"),
    PageSpec(path="/nova", name="nova"),
    PageSpec(path="/media-plan", name="media-plan"),
    PageSpec(path="/pricing", name="pricing"),
    PageSpec(path="/api-portal", name="api-portal"),
    PageSpec(path="/health-dashboard", name="health-dashboard"),
]

# Responsive pages
RESPONSIVE_PAGES: list[PageSpec] = [
    PageSpec(path="/", name="homepage_mobile", width=375, height=812),
    PageSpec(path="/hub", name="hub_mobile", width=375, height=812),
    PageSpec(path="/platform", name="platform_tablet", width=768, height=1024),
]

# Nova chat widget test
NOVA_CHAT_SPEC = PageSpec(
    path="/hub",
    name="nova_chat_widget",
    width=1280,
    height=800,
    full_page=False,
)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass
class PageResult:
    """Result of a single page test."""

    name: str
    captured: bool = False
    has_baseline: bool = False
    diff_pct: float = 0.0
    passed: Optional[bool] = None
    error: str = ""
    duration_s: float = 0.0


@dataclass
class SuiteResult:
    """Aggregate results from the full test suite."""

    results: list[PageResult] = field(default_factory=list)
    total_time_s: float = 0.0

    @property
    def passed_count(self) -> int:
        """Count of tests that passed."""
        return sum(1 for r in self.results if r.passed is True)

    @property
    def failed_count(self) -> int:
        """Count of tests that failed."""
        return sum(1 for r in self.results if r.passed is False)

    @property
    def no_baseline_count(self) -> int:
        """Count of tests with no baseline."""
        return sum(1 for r in self.results if not r.has_baseline and r.captured)

    @property
    def error_count(self) -> int:
        """Count of tests that errored."""
        return sum(1 for r in self.results if r.error)


# ---------------------------------------------------------------------------
# Image comparison (Pillow)
# ---------------------------------------------------------------------------


def compute_pixel_diff(baseline_path: Path, current_path: Path) -> float:
    """Compare two images pixel-by-pixel and return the diff percentage.

    Args:
        baseline_path: Path to the baseline screenshot.
        current_path: Path to the current screenshot.

    Returns:
        Percentage of pixels that differ (0.0 to 100.0).
    """
    try:
        from PIL import Image
    except ImportError:
        logger.error("Pillow not installed. Run: pip install Pillow")
        raise

    baseline_img = Image.open(baseline_path).convert("RGB")
    current_img = Image.open(current_path).convert("RGB")

    # Resize current to match baseline dimensions if they differ
    if baseline_img.size != current_img.size:
        logger.warning(
            f"Size mismatch: baseline={baseline_img.size}, "
            f"current={current_img.size}. Resizing current to match."
        )
        current_img = current_img.resize(baseline_img.size, Image.LANCZOS)

    baseline_pixels = baseline_img.load()
    current_pixels = current_img.load()
    width, height = baseline_img.size
    total_pixels = width * height

    if total_pixels == 0:
        return 0.0

    diff_count = 0
    for y in range(height):
        for x in range(width):
            bp = baseline_pixels[x, y]
            cp = current_pixels[x, y]
            # A pixel is "different" if any channel differs by more than 10
            if (
                abs(bp[0] - cp[0]) > 10
                or abs(bp[1] - cp[1]) > 10
                or abs(bp[2] - cp[2]) > 10
            ):
                diff_count += 1

    return (diff_count / total_pixels) * 100.0


def save_diff_image(baseline_path: Path, current_path: Path, diff_path: Path) -> None:
    """Generate a visual diff image highlighting changed pixels in red.

    Args:
        baseline_path: Path to the baseline screenshot.
        current_path: Path to the current screenshot.
        diff_path: Path to write the diff image.
    """
    try:
        from PIL import Image
    except ImportError:
        return

    baseline_img = Image.open(baseline_path).convert("RGB")
    current_img = Image.open(current_path).convert("RGB")

    if baseline_img.size != current_img.size:
        current_img = current_img.resize(baseline_img.size, Image.LANCZOS)

    width, height = baseline_img.size
    diff_img = Image.new("RGB", (width, height))
    diff_pixels = diff_img.load()
    baseline_pixels = baseline_img.load()
    current_pixels = current_img.load()

    for y in range(height):
        for x in range(width):
            bp = baseline_pixels[x, y]
            cp = current_pixels[x, y]
            if (
                abs(bp[0] - cp[0]) > 10
                or abs(bp[1] - cp[1]) > 10
                or abs(bp[2] - cp[2]) > 10
            ):
                diff_pixels[x, y] = (255, 0, 0)  # Red for diff
            else:
                # Dimmed version of original
                diff_pixels[x, y] = (bp[0] // 3, bp[1] // 3, bp[2] // 3)

    diff_img.save(diff_path)
    logger.info(f"Diff image saved: {diff_path}")


# ---------------------------------------------------------------------------
# Playwright browser helpers
# ---------------------------------------------------------------------------


def _ensure_playwright() -> None:
    """Verify Playwright is importable and chromium is installed."""
    try:
        import playwright  # noqa: F401
    except ImportError:
        logger.error(
            "Playwright not installed. Run:\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        )
        sys.exit(1)


def capture_page(
    page: "Page",  # type: ignore[name-defined]  # noqa: F821
    spec: PageSpec,
    output_dir: Path,
) -> PageResult:
    """Navigate to a page, wait for idle, and take a screenshot.

    Args:
        page: Playwright page object.
        spec: Page specification (path, viewport, etc.).
        output_dir: Directory to save the screenshot.

    Returns:
        PageResult with capture status.
    """
    result = PageResult(name=spec.name)
    start = time.monotonic()
    url = f"{SITE_URL}{spec.path}"

    try:
        page.set_viewport_size({"width": spec.width, "height": spec.height})
        page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)

        # Extra wait for any JS rendering
        page.wait_for_timeout(2000)

        if spec.wait_selector:
            try:
                page.wait_for_selector(spec.wait_selector, timeout=10_000)
            except Exception:
                logger.warning(
                    f"Selector '{spec.wait_selector}' not found on {spec.name}"
                )

        output_path = output_dir / spec.filename
        page.screenshot(path=str(output_path), full_page=spec.full_page)
        result.captured = True
        file_size = output_path.stat().st_size
        logger.info(
            f"Captured {spec.name} ({spec.width}x{spec.height}) "
            f"-- {file_size:,} bytes"
        )

    except Exception as exc:
        result.error = str(exc)
        logger.error(f"Failed to capture {spec.name}: {exc}", exc_info=True)

    result.duration_s = time.monotonic() - start
    return result


def capture_nova_chat(
    page: "Page",  # type: ignore[name-defined]  # noqa: F821
    output_dir: Path,
) -> PageResult:
    """Open the Nova chat widget on /hub and screenshot it.

    Args:
        page: Playwright page object.
        output_dir: Directory to save the screenshot.

    Returns:
        PageResult with capture status.
    """
    result = PageResult(name=NOVA_CHAT_SPEC.name)
    start = time.monotonic()
    url = f"{SITE_URL}{NOVA_CHAT_SPEC.path}"

    try:
        page.set_viewport_size(
            {"width": NOVA_CHAT_SPEC.width, "height": NOVA_CHAT_SPEC.height}
        )
        page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)
        page.wait_for_timeout(2000)

        # Try to find and click the Nova chat trigger button
        chat_opened = False
        chat_selectors = [
            "#nova-chat-trigger",
            ".nova-chat-trigger",
            "[data-nova-chat]",
            "#nova-widget-trigger",
            ".chat-trigger",
            'button:has-text("Nova")',
            'button:has-text("Chat")',
            'button:has-text("Ask Nova")',
        ]
        for selector in chat_selectors:
            try:
                el = page.query_selector(selector)
                if el and el.is_visible():
                    el.click()
                    page.wait_for_timeout(1500)
                    chat_opened = True
                    logger.info(f"Opened chat widget via: {selector}")
                    break
            except Exception:
                continue

        if not chat_opened:
            logger.warning(
                "Could not find Nova chat trigger button. " "Screenshotting page as-is."
            )

        output_path = output_dir / NOVA_CHAT_SPEC.filename
        page.screenshot(path=str(output_path), full_page=False)
        result.captured = True
        logger.info(
            f"Captured nova_chat_widget "
            f"(chat_opened={chat_opened}) "
            f"-- {output_path.stat().st_size:,} bytes"
        )

    except Exception as exc:
        result.error = str(exc)
        logger.error(f"Failed to capture nova chat: {exc}", exc_info=True)

    result.duration_s = time.monotonic() - start
    return result


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def compare_result(result: PageResult, spec: PageSpec) -> PageResult:
    """Compare a captured screenshot against its baseline.

    Args:
        result: The PageResult from capture.
        spec: The PageSpec used for capture.

    Returns:
        Updated PageResult with diff info.
    """
    if not result.captured:
        return result

    current_path = CURRENT_DIR / spec.filename
    baseline_path = BASELINE_DIR / spec.filename

    if not baseline_path.exists():
        result.has_baseline = False
        result.passed = None
        return result

    result.has_baseline = True
    try:
        result.diff_pct = compute_pixel_diff(baseline_path, current_path)
        result.passed = result.diff_pct <= DIFF_THRESHOLD_PCT

        if not result.passed:
            diff_dir = SCREENSHOT_DIR / "diffs"
            diff_dir.mkdir(parents=True, exist_ok=True)
            diff_path = diff_dir / f"diff_{spec.filename}"
            save_diff_image(baseline_path, current_path, diff_path)

    except Exception as exc:
        result.error = f"Comparison failed: {exc}"
        result.passed = False
        logger.error(f"Comparison failed for {spec.name}: {exc}", exc_info=True)

    return result


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


def run_suite(
    pages_filter: Optional[list[str]] = None,
    save_as_baseline: bool = False,
) -> SuiteResult:
    """Run the full visual regression test suite.

    Args:
        pages_filter: Optional list of page paths to test (e.g. ["/hub", "/nova"]).
        save_as_baseline: If True, save current screenshots as baselines.

    Returns:
        SuiteResult with all page results.
    """
    _ensure_playwright()
    from playwright.sync_api import sync_playwright

    suite = SuiteResult()
    suite_start = time.monotonic()

    # Prepare output directories
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)

    # Build page list
    all_specs: list[PageSpec] = []
    for spec in DESKTOP_PAGES + RESPONSIVE_PAGES:
        if pages_filter is None or spec.path in pages_filter:
            all_specs.append(spec)

    # Always include Nova chat if no filter or /hub is in filter
    include_chat = pages_filter is None or "/hub" in pages_filter

    logger.info(
        f"Testing {len(all_specs)} pages"
        f"{' + Nova chat widget' if include_chat else ''} "
        f"against {SITE_URL}"
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36 "
                "NovaVisualRegression/1.0"
            ),
        )
        page = context.new_page()

        # Capture all pages
        for spec in all_specs:
            result = capture_page(page, spec, CURRENT_DIR)
            if not save_as_baseline:
                result = compare_result(result, spec)
            suite.results.append(result)

        # Capture Nova chat widget
        if include_chat:
            chat_result = capture_nova_chat(page, CURRENT_DIR)
            if not save_as_baseline:
                chat_result = compare_result(chat_result, NOVA_CHAT_SPEC)
            suite.results.append(chat_result)

        browser.close()

    # Save baselines if requested
    if save_as_baseline:
        _save_baselines()

    suite.total_time_s = time.monotonic() - suite_start
    return suite


def _save_baselines() -> None:
    """Copy all current screenshots to the baselines directory."""
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for f in CURRENT_DIR.glob("*.png"):
        shutil.copy2(f, BASELINE_DIR / f.name)
        count += 1
    logger.info(f"Saved {count} baseline(s) to {BASELINE_DIR}")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_report(suite: SuiteResult) -> None:
    """Print a formatted test report to stdout.

    Args:
        suite: The SuiteResult to report on.
    """
    print("\n" + "=" * 72)
    print("  NOVA AI SUITE -- Visual Regression Test Report")
    print("=" * 72)
    print(f"  Site: {SITE_URL}")
    print(f"  Time: {suite.total_time_s:.1f}s")
    print(f"  Threshold: {DIFF_THRESHOLD_PCT}% pixel diff")
    print("-" * 72)

    for r in suite.results:
        if r.error:
            status = "ERROR"
            detail = r.error[:60]
        elif not r.captured:
            status = "SKIP"
            detail = "not captured"
        elif not r.has_baseline:
            status = "NEW"
            detail = "no baseline (run --baseline to create)"
        elif r.passed:
            status = "PASS"
            detail = f"{r.diff_pct:.2f}% diff"
        else:
            status = "FAIL"
            detail = f"{r.diff_pct:.2f}% diff (threshold: {DIFF_THRESHOLD_PCT}%)"

        icon = {
            "PASS": "[OK]  ",
            "FAIL": "[FAIL]",
            "NEW": "[NEW] ",
            "ERROR": "[ERR] ",
            "SKIP": "[SKIP]",
        }.get(status, "[??]  ")

        print(f"  {icon} {r.name:<35} {detail}  ({r.duration_s:.1f}s)")

    print("-" * 72)
    print(
        f"  Total: {len(suite.results)} | "
        f"Pass: {suite.passed_count} | "
        f"Fail: {suite.failed_count} | "
        f"New: {suite.no_baseline_count} | "
        f"Error: {suite.error_count}"
    )
    print("=" * 72 + "\n")

    if suite.failed_count > 0:
        diff_dir = SCREENSHOT_DIR / "diffs"
        print(f"  Diff images saved to: {diff_dir}")
        print("  Review diffs and run with --baseline to accept changes.\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the visual regression test suite."""
    args = sys.argv[1:]
    save_as_baseline = "--baseline" in args

    # Parse --pages filter
    pages_filter: Optional[list[str]] = None
    if "--pages" in args:
        idx = args.index("--pages")
        pages_filter = []
        for arg in args[idx + 1 :]:
            if arg.startswith("--"):
                break
            path = arg if arg.startswith("/") else f"/{arg}"
            pages_filter.append(path)
        if not pages_filter:
            logger.error("--pages requires at least one page path")
            sys.exit(1)

    suite = run_suite(pages_filter=pages_filter, save_as_baseline=save_as_baseline)
    print_report(suite)

    if save_as_baseline:
        print("  Baselines saved. Future runs will compare against these.\n")
        sys.exit(0)

    if suite.failed_count > 0 or suite.error_count > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
