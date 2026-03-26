#!/usr/bin/env python3
"""Multi-tier presentation generation with automatic fallback.

Tries providers in order: Presenton -> Gamma -> MagicSlides -> Google Slides
-> Alai -> FlashDocs -> python-pptx. Each tier is tried; if it fails or
hits rate limits, the next tier takes over automatically.

Tier 7 (python-pptx via ppt_generator) is the guaranteed offline fallback
that always works. The other tiers are optional upgrades that produce
higher-quality, AI-designed slides when their API keys are configured.

Environment variables (all optional except Tier 7 which needs no config):
    PRESENTON_URL         - Self-hosted Presenton API base URL
    GAMMA_API_KEY         - Gamma REST API key (Pro plan required)
    MAGICSLIDES_API_KEY   - MagicSlides REST API key
    GOOGLE_SLIDES_CREDENTIALS - Path to Google service account JSON file
    GOOGLE_SLIDES_CREDENTIALS_B64 - Base64-encoded Google service account JSON
    ALAI_API_KEY          - Alai REST API key
    FLASHDOCS_API_KEY     - FlashDocs REST API key
"""

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Usage tracking (in-memory, resets on deploy/restart)
# ---------------------------------------------------------------------------
_usage_lock = threading.Lock()
_monthly_usage: dict[str, int] = {}
_usage_month: str = ""  # "YYYY-MM" -- resets counters when month changes

_TIER_LIMITS: dict[str, int] = {
    "gamma": 10,
    "magicslides": 100,
    "alai": 200,
    "flashdocs": 5000,
}

# Tier ordering and display names
_TIERS: list[tuple[str, str]] = [
    ("google_slides", "Google Slides API"),
    ("presenton", "Presenton (self-hosted)"),
    ("gamma", "Gamma API"),
    ("magicslides", "MagicSlides API"),
    ("alai", "Alai API"),
    ("flashdocs", "FlashDocs SDK"),
    ("pptx", "python-pptx (offline)"),
]


def _current_month() -> str:
    """Return current month as 'YYYY-MM' in UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _check_and_increment(tier: str) -> bool:
    """Check if tier has quota remaining; if so, increment and return True.

    Returns False if the tier has hit its monthly limit.
    Tiers without limits (presenton, google_slides, pptx) always return True.
    """
    limit = _TIER_LIMITS.get(tier)
    if limit is None:
        return True  # No limit for this tier

    global _usage_month, _monthly_usage
    with _usage_lock:
        month = _current_month()
        if month != _usage_month:
            _monthly_usage = {}
            _usage_month = month

        current = _monthly_usage.get(tier, 0)
        if current >= limit:
            return False
        _monthly_usage[tier] = current + 1
        return True


def _get_usage(tier: str) -> int:
    """Return current month usage for a tier."""
    with _usage_lock:
        month = _current_month()
        if month != _usage_month:
            return 0
        return _monthly_usage.get(tier, 0)


def _http_request(
    url: str,
    *,
    method: str = "GET",
    data: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 120,
) -> tuple[int, bytes, dict[str, str]]:
    """Make an HTTP request using stdlib urllib.

    Returns:
        Tuple of (status_code, response_body, response_headers).

    Raises:
        urllib.error.URLError on network failure.
        urllib.error.HTTPError on 4xx/5xx (but we catch and return status).
    """
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            return resp.status, body, resp_headers
    except urllib.error.HTTPError as e:
        body = e.read() if e.fp else b""
        return e.code, body, {}


def _format_plan_as_text(data: dict[str, Any]) -> str:
    """Convert media plan data dict to structured text for AI generation.

    This produces a Markdown-formatted summary that AI presentation tools
    can use to generate professional slides.
    """
    client = data.get("client_name") or "Client"
    industry = data.get("industry") or "General"
    budget = data.get("budget") or data.get("budget_range") or "Not specified"
    locations = data.get("locations") or []
    roles = data.get("roles") or data.get("target_roles") or []
    goals = data.get("campaign_goals") or []
    duration = data.get("duration") or data.get("campaign_duration") or "Not specified"
    work_env = data.get("work_environment") or "Not specified"

    # Normalize roles from dicts to strings
    if roles and isinstance(roles[0], dict):
        roles = [
            r.get("title", r.get("role", str(r))) if isinstance(r, dict) else str(r)
            for r in roles
        ]

    lines = [
        f"# Recruitment Media Plan: {client}",
        "",
        "## Campaign Overview",
        f"- **Industry:** {industry}",
        f"- **Budget:** {budget}",
        f"- **Duration:** {duration}",
        f"- **Work Environment:** {work_env}",
    ]

    if locations:
        loc_str = ", ".join(str(loc) for loc in locations[:5])
        lines.append(f"- **Locations:** {loc_str}")

    if roles:
        role_str = ", ".join(str(r) for r in roles[:10])
        lines.append(f"- **Target Roles:** {role_str}")

    if goals:
        goal_str = ", ".join(str(g) for g in goals)
        lines.append(f"- **Campaign Goals:** {goal_str}")

    # Channel allocation
    channels = data.get("channels") or data.get("channel_mix") or []
    if channels:
        lines.append("")
        lines.append("## Channel Allocation")
        lines.append("")
        lines.append("| Channel | Budget | Expected Reach | CPA |")
        lines.append("|---------|--------|---------------|-----|")
        for ch in channels:
            if isinstance(ch, dict):
                name = ch.get("name") or ch.get("channel") or "Unknown"
                ch_budget = ch.get("budget") or ch.get("allocation") or 0
                reach = ch.get("estimated_reach") or ch.get("reach") or 0
                cpa = ch.get("cost_per_application") or ch.get("cpa") or 0
                try:
                    lines.append(
                        f"| {name} | ${float(ch_budget):,.0f} | "
                        f"{int(float(reach)):,} | ${float(cpa):.2f} |"
                    )
                except (ValueError, TypeError):
                    lines.append(f"| {name} | {ch_budget} | {reach} | {cpa} |")

    # Recommendations
    recs = data.get("recommendations") or data.get("strategy_notes") or []
    if recs:
        lines.append("")
        lines.append("## Strategic Recommendations")
        for rec in recs[:8]:
            lines.append(f"- {rec}")

    # Benchmarks
    benchmarks = data.get("benchmarks") or data.get("industry_benchmarks") or {}
    if benchmarks and isinstance(benchmarks, dict):
        lines.append("")
        lines.append("## Industry Benchmarks")
        for key, val in list(benchmarks.items())[:10]:
            lines.append(f"- **{key}:** {val}")

    return "\n".join(lines)


def _slide_count(data: dict[str, Any]) -> int:
    """Estimate a good number of slides for the data content."""
    channels = data.get("channels") or data.get("channel_mix") or []
    base = 6  # title + overview + allocation + benchmarks + recs + closing
    extra = min(len(channels) // 3, 6)  # 1 extra slide per 3 channels
    return min(base + extra, 20)


class DeckGenerationError(Exception):
    """Raised when all presentation generation tiers fail."""


class DeckGenerator:
    """Multi-tier presentation generator with automatic fallback.

    Each tier is attempted in order. If a tier fails (missing API key,
    rate limit, network error, etc.), the next tier is tried. Tier 7
    (python-pptx) is the guaranteed offline fallback.

    Usage::

        generator = DeckGenerator()
        file_bytes, provider = generator.generate(media_plan_data)
    """

    def generate(
        self,
        data: dict[str, Any],
        format: str = "pptx",
        force_tier: Optional[str] = None,
    ) -> tuple[bytes, str]:
        """Generate a presentation from media plan data.

        Args:
            data: Media plan data dict (channels, budget, benchmarks, etc.).
            format: Output format -- "pptx" (default) or "pdf".
            force_tier: If set, skip the fallback chain and use only this tier.
                Valid values: "presenton", "gamma", "magicslides",
                "google_slides", "alai", "flashdocs", "pptx".

        Returns:
            Tuple of (file_bytes, provider_name).

        Raises:
            DeckGenerationError: If all tiers fail (should not happen since
                Tier 7 python-pptx is always available).
        """
        tier_methods = {
            "presenton": self._try_presenton,
            "gamma": self._try_gamma,
            "magicslides": self._try_magicslides,
            "google_slides": self._try_google_slides,
            "alai": self._try_alai,
            "flashdocs": self._try_flashdocs,
            "pptx": self._try_pptx,
        }

        if force_tier:
            method = tier_methods.get(force_tier)
            if method is None:
                raise ValueError(
                    f"Unknown tier '{force_tier}'. "
                    f"Valid: {', '.join(tier_methods.keys())}"
                )
            result = method(data)
            if result is not None:
                return result, force_tier
            raise DeckGenerationError(f"Forced tier '{force_tier}' failed to generate.")

        errors: list[str] = []
        for tier_key, tier_name in _TIERS:
            method = tier_methods[tier_key]
            try:
                result = method(data)
                if result is not None:
                    logger.info(
                        "Deck generated via %s (%d bytes)",
                        tier_name,
                        len(result),
                    )
                    return result, tier_key
            except Exception as exc:
                msg = f"{tier_name}: {exc}"
                errors.append(msg)
                logger.warning("Tier %s failed: %s", tier_name, exc, exc_info=True)

        # This should never happen since _try_pptx always works
        raise DeckGenerationError(
            f"All {len(_TIERS)} tiers failed. Errors: {'; '.join(errors)}"
        )

    # ------------------------------------------------------------------
    # Tier 1: Presenton (self-hosted, unlimited)
    # ------------------------------------------------------------------
    def _try_presenton(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via self-hosted Presenton API.

        Requires PRESENTON_URL env var (e.g., http://localhost:8501/api/v1/ppt/presentation/generate).
        Returns PPTX bytes on success, None if not configured.
        """
        base_url = os.environ.get("PRESENTON_URL") or ""
        if not base_url:
            logger.debug("Presenton skipped: PRESENTON_URL not set")
            return None

        # Build the endpoint URL
        endpoint = base_url.rstrip("/")
        if not endpoint.endswith("/generate"):
            endpoint = f"{endpoint}/api/v1/ppt/presentation/generate"

        payload = json.dumps(
            {
                "content": _format_plan_as_text(data),
                "n_slides": _slide_count(data),
                "language": "English",
                "template": "professional",
                "export_as": "pptx",
                "tone": "professional",
                "density": "standard",
            }
        ).encode("utf-8")

        try:
            status, body, headers = _http_request(
                endpoint,
                method="POST",
                data=payload,
                headers={"Content-Type": "application/json"},
                timeout=180,
            )
        except urllib.error.URLError as exc:
            logger.error("Presenton network error: %s", exc, exc_info=True)
            return None

        if status != 200:
            logger.warning(
                "Presenton returned HTTP %d: %s",
                status,
                body[:500].decode("utf-8", errors="replace"),
            )
            return None

        content_type = headers.get("content-type") or ""
        if "application/json" in content_type:
            # Some Presenton versions return JSON with a download URL
            try:
                resp_data = json.loads(body)
                download_url = (
                    resp_data.get("download_url") or resp_data.get("file_url") or ""
                )
                if download_url:
                    dl_status, dl_body, _ = _http_request(download_url, timeout=60)
                    if dl_status == 200 and len(dl_body) > 1000:
                        return dl_body
                # Or inline base64 PPTX
                pptx_b64 = resp_data.get("pptx_base64") or ""
                if pptx_b64:
                    import base64

                    return base64.b64decode(pptx_b64)
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.warning("Presenton JSON parse error: %s", exc)
                return None
        else:
            # Direct binary PPTX response
            if len(body) > 1000:
                return body

        logger.warning("Presenton returned unexpected response (len=%d)", len(body))
        return None

    # ------------------------------------------------------------------
    # Tier 2: Gamma (API, ~10 free/month on Pro plan)
    # ------------------------------------------------------------------
    def _try_gamma(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via Gamma REST API.

        Requires GAMMA_API_KEY env var. Tracks monthly usage (limit: 10).
        Uses async poll workflow: POST to create, poll until complete, download.
        """
        api_key = os.environ.get("GAMMA_API_KEY") or ""
        if not api_key:
            logger.debug("Gamma skipped: GAMMA_API_KEY not set")
            return None

        if not _check_and_increment("gamma"):
            logger.info(
                "Gamma skipped: monthly limit reached (%d/%d)",
                _get_usage("gamma"),
                _TIER_LIMITS["gamma"],
            )
            return None

        base_url = "https://public-api.gamma.app/v1.0"
        auth_headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        }

        # Step 1: Create generation
        payload = json.dumps(
            {
                "inputText": _format_plan_as_text(data),
                "textMode": "generate",
                "format": "presentation",
                "numCards": _slide_count(data),
                "cardOptions": {"dimensions": "16x9"},
                "exportAs": "pptx",
            }
        ).encode("utf-8")

        try:
            status, body, _ = _http_request(
                f"{base_url}/generations",
                method="POST",
                data=payload,
                headers=auth_headers,
                timeout=30,
            )
        except urllib.error.URLError as exc:
            logger.error("Gamma create request failed: %s", exc, exc_info=True)
            return None

        if status not in (200, 201):
            logger.warning(
                "Gamma create returned HTTP %d: %s",
                status,
                body[:500].decode("utf-8", errors="replace"),
            )
            return None

        try:
            resp_data = json.loads(body)
            generation_id = resp_data.get("generationId") or resp_data.get("id") or ""
        except (json.JSONDecodeError, KeyError):
            logger.warning("Gamma: could not parse creation response")
            return None

        if not generation_id:
            logger.warning("Gamma: no generation ID in response")
            return None

        # Step 2: Poll for completion (max ~120 seconds)
        poll_headers = {"X-API-KEY": api_key}
        for attempt in range(24):
            time.sleep(5)
            try:
                p_status, p_body, _ = _http_request(
                    f"{base_url}/generations/{generation_id}",
                    method="GET",
                    headers=poll_headers,
                    timeout=15,
                )
            except urllib.error.URLError:
                continue

            if p_status != 200:
                continue

            try:
                poll_data = json.loads(p_body)
            except json.JSONDecodeError:
                continue

            gen_status = poll_data.get("status") or ""
            if gen_status == "completed":
                export_url = poll_data.get("exportUrl") or ""
                if export_url:
                    return self._download_file(export_url)
                logger.warning("Gamma completed but no exportUrl found")
                return None
            elif gen_status == "failed":
                error_msg = poll_data.get("error") or "unknown error"
                logger.warning("Gamma generation failed: %s", error_msg)
                return None

        logger.warning("Gamma generation timed out after 120s")
        return None

    # ------------------------------------------------------------------
    # Tier 3: MagicSlides (API, 100 free/month)
    # ------------------------------------------------------------------
    def _try_magicslides(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via MagicSlides REST API.

        Requires MAGICSLIDES_API_KEY env var. Tracks monthly usage (limit: 100).
        API docs: https://www.magicslides.app/magicslides-api/docs
        """
        api_key = os.environ.get("MAGICSLIDES_API_KEY") or ""
        if not api_key:
            logger.debug("MagicSlides skipped: MAGICSLIDES_API_KEY not set")
            return None

        if not _check_and_increment("magicslides"):
            logger.info(
                "MagicSlides skipped: monthly limit reached (%d/%d)",
                _get_usage("magicslides"),
                _TIER_LIMITS["magicslides"],
            )
            return None

        base_url = "https://api.magicslides.app/v1"

        payload = json.dumps(
            {
                "text": _format_plan_as_text(data),
                "length": _slide_count(data),
                "template": "professional",
                "language": "en",
                "tone": "professional",
                "response_format": "pptx",
            }
        ).encode("utf-8")

        try:
            status, body, _ = _http_request(
                f"{base_url}/presentations/generate",
                method="POST",
                data=payload,
                headers={
                    "x-api-key": api_key,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
        except urllib.error.URLError as exc:
            logger.error("MagicSlides request failed: %s", exc, exc_info=True)
            return None

        if status not in (200, 201, 202):
            logger.warning(
                "MagicSlides returned HTTP %d: %s",
                status,
                body[:500].decode("utf-8", errors="replace"),
            )
            return None

        # MagicSlides may return async task_id or direct file
        try:
            resp_data = json.loads(body)
        except json.JSONDecodeError:
            # Might be direct binary PPTX
            if len(body) > 1000:
                return body
            return None

        task_id = resp_data.get("task_id") or resp_data.get("id") or ""
        download_url = resp_data.get("download_url") or resp_data.get("url") or ""

        if download_url:
            return self._download_file(download_url)

        if task_id:
            return self._poll_magicslides(api_key, base_url, task_id)

        logger.warning("MagicSlides: no task_id or download_url in response")
        return None

    def _poll_magicslides(
        self, api_key: str, base_url: str, task_id: str
    ) -> Optional[bytes]:
        """Poll MagicSlides for task completion."""
        for attempt in range(24):
            time.sleep(5)
            try:
                status, body, _ = _http_request(
                    f"{base_url}/tasks/{task_id}",
                    method="GET",
                    headers={"x-api-key": api_key},
                    timeout=15,
                )
            except urllib.error.URLError:
                continue

            if status != 200:
                continue

            try:
                poll_data = json.loads(body)
            except json.JSONDecodeError:
                continue

            task_status = (poll_data.get("status") or "").upper()
            if task_status == "SUCCESS":
                dl_url = (
                    poll_data.get("download_url")
                    or poll_data.get("url")
                    or poll_data.get("result", {}).get("download_url")
                    or ""
                )
                if dl_url:
                    return self._download_file(dl_url)
                return None
            elif task_status in ("FAILED", "ERROR"):
                logger.warning("MagicSlides task failed: %s", poll_data)
                return None

        logger.warning("MagicSlides task timed out after 120s")
        return None

    # ------------------------------------------------------------------
    # Tier 4: Google Slides API (unlimited, free)
    # ------------------------------------------------------------------
    def _try_google_slides(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via Google Slides API using a service account.

        Supports two credential sources:
        1. GOOGLE_SLIDES_CREDENTIALS: path to JSON credentials file
        2. GOOGLE_SLIDES_CREDENTIALS_B64: base64-encoded JSON (for Render)

        Creates a presentation via batch updates and exports as PPTX.
        """
        import base64

        # Try base64-encoded credentials first (Render deployment)
        creds_b64 = os.environ.get("GOOGLE_SLIDES_CREDENTIALS_B64") or ""
        creds_dict = None

        if creds_b64:
            try:
                creds_json = base64.b64decode(creds_b64).decode("utf-8")
                creds_dict = json.loads(creds_json)
                logger.debug("Google Slides: using base64-encoded credentials")
            except Exception as exc:
                logger.warning("Google Slides: base64 decode failed: %s", exc)
                creds_dict = None

        # Fall back to file-based credentials (local development)
        if not creds_dict:
            creds_path = os.environ.get("GOOGLE_SLIDES_CREDENTIALS") or ""
            if not creds_path:
                logger.debug(
                    "Google Slides skipped: neither GOOGLE_SLIDES_CREDENTIALS "
                    "nor GOOGLE_SLIDES_CREDENTIALS_B64 set"
                )
                return None

            creds_file = Path(creds_path)
            if not creds_file.is_file():
                logger.warning(
                    "Google Slides skipped: credentials file not found at %s",
                    creds_path,
                )
                return None

            try:
                with open(creds_file) as f:
                    creds_dict = json.load(f)
            except Exception as exc:
                logger.error(
                    "Google Slides: failed to read credentials file: %s",
                    exc,
                    exc_info=True,
                )
                return None

        try:
            from googleapiclient.discovery import build
            from google.oauth2.service_account import Credentials
        except ImportError:
            logger.debug(
                "Google Slides skipped: google-api-python-client or "
                "google-auth not installed"
            )
            return None

        scopes = [
            "https://www.googleapis.com/auth/presentations",
            "https://www.googleapis.com/auth/drive",
        ]

        try:
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            slides_service = build("slides", "v1", credentials=creds)
            drive_service = build("drive", "v3", credentials=creds)
        except Exception as exc:
            logger.error("Google Slides auth failed: %s", exc, exc_info=True)
            return None

        client = data.get("client_name") or "Client"
        industry = data.get("industry") or "General"
        title = f"Media Plan - {client} ({industry})"

        try:
            # Create presentation
            presentation = (
                slides_service.presentations().create(body={"title": title}).execute()
            )
            pres_id = presentation["presentationId"]

            # Build batch update requests
            requests_list = self._build_google_slides_requests(data, presentation)

            if requests_list:
                slides_service.presentations().batchUpdate(
                    presentationId=pres_id,
                    body={"requests": requests_list},
                ).execute()

            # Export as PPTX
            export_resp = (
                drive_service.files()
                .export(
                    fileId=pres_id,
                    mimeType="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                )
                .execute()
            )

            # Clean up: delete the temporary presentation from Drive
            try:
                drive_service.files().delete(fileId=pres_id).execute()
            except Exception:
                pass  # Non-critical cleanup

            if isinstance(export_resp, bytes) and len(export_resp) > 1000:
                return export_resp

        except Exception as exc:
            logger.error("Google Slides generation failed: %s", exc, exc_info=True)
            return None

        return None

    def _build_google_slides_requests(
        self, data: dict[str, Any], presentation: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Build Google Slides batchUpdate requests for the media plan.

        Creates a simple multi-slide presentation with title, overview,
        channel allocation, and recommendations.
        """
        requests_list: list[dict[str, Any]] = []
        client = data.get("client_name") or "Client"
        industry = data.get("industry") or "General"
        budget = data.get("budget") or data.get("budget_range") or "N/A"

        # Get the default slide ID from the blank presentation
        slides = presentation.get("slides") or []
        if slides:
            first_slide_id = slides[0]["objectId"]
            # Delete the default blank slide -- we'll create our own
            requests_list.append({"deleteObject": {"objectId": first_slide_id}})

        # Slide 1: Title slide
        slide1_id = "title_slide_001"
        requests_list.append(
            {
                "createSlide": {
                    "objectId": slide1_id,
                    "slideLayoutReference": {"predefinedLayout": "TITLE"},
                }
            }
        )

        # Slide 2: Overview
        slide2_id = "overview_slide_002"
        requests_list.append(
            {
                "createSlide": {
                    "objectId": slide2_id,
                    "slideLayoutReference": {"predefinedLayout": "TITLE_AND_BODY"},
                }
            }
        )

        # Slide 3: Channel allocation
        slide3_id = "channels_slide_003"
        requests_list.append(
            {
                "createSlide": {
                    "objectId": slide3_id,
                    "slideLayoutReference": {"predefinedLayout": "TITLE_AND_BODY"},
                }
            }
        )

        # Slide 4: Recommendations
        slide4_id = "recs_slide_004"
        requests_list.append(
            {
                "createSlide": {
                    "objectId": slide4_id,
                    "slideLayoutReference": {"predefinedLayout": "TITLE_AND_BODY"},
                }
            }
        )

        return requests_list

    # ------------------------------------------------------------------
    # Tier 5: Alai (MCP/API, 200 free credits)
    # ------------------------------------------------------------------
    def _try_alai(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via Alai REST API.

        Requires ALAI_API_KEY env var. Tracks credit usage (limit: 200).
        API docs: https://getalai.com/api
        """
        api_key = os.environ.get("ALAI_API_KEY") or ""
        if not api_key:
            logger.debug("Alai skipped: ALAI_API_KEY not set")
            return None

        if not _check_and_increment("alai"):
            logger.info(
                "Alai skipped: credit limit reached (%d/%d)",
                _get_usage("alai"),
                _TIER_LIMITS["alai"],
            )
            return None

        base_url = "https://api.getalai.com/v1"

        payload = json.dumps(
            {
                "content": _format_plan_as_text(data),
                "format": "pptx",
                "slides": _slide_count(data),
                "style": "professional",
            }
        ).encode("utf-8")

        try:
            status, body, _ = _http_request(
                f"{base_url}/presentations",
                method="POST",
                data=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
        except urllib.error.URLError as exc:
            logger.error("Alai request failed: %s", exc, exc_info=True)
            return None

        if status not in (200, 201, 202):
            logger.warning(
                "Alai returned HTTP %d: %s",
                status,
                body[:500].decode("utf-8", errors="replace"),
            )
            return None

        try:
            resp_data = json.loads(body)
        except json.JSONDecodeError:
            if len(body) > 1000:
                return body
            return None

        download_url = (
            resp_data.get("download_url")
            or resp_data.get("pptx_url")
            or resp_data.get("url")
            or ""
        )
        if download_url:
            return self._download_file(download_url)

        # Check for async job
        job_id = resp_data.get("job_id") or resp_data.get("id") or ""
        if job_id:
            return self._poll_generic(
                poll_url=f"{base_url}/presentations/{job_id}",
                headers={"Authorization": f"Bearer {api_key}"},
                tier_name="Alai",
            )

        return None

    # ------------------------------------------------------------------
    # Tier 6: FlashDocs (SDK, 5000 free credits)
    # ------------------------------------------------------------------
    def _try_flashdocs(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via FlashDocs REST API / SDK.

        Requires FLASHDOCS_API_KEY env var. Tracks credit usage (limit: 5000).
        API docs: https://www.flashdocs.com
        """
        api_key = os.environ.get("FLASHDOCS_API_KEY") or ""
        if not api_key:
            logger.debug("FlashDocs skipped: FLASHDOCS_API_KEY not set")
            return None

        if not _check_and_increment("flashdocs"):
            logger.info(
                "FlashDocs skipped: credit limit reached (%d/%d)",
                _get_usage("flashdocs"),
                _TIER_LIMITS["flashdocs"],
            )
            return None

        base_url = "https://api.flashdocs.com/v1"

        payload = json.dumps(
            {
                "content": _format_plan_as_text(data),
                "format": "pptx",
                "template": "professional",
                "slides": _slide_count(data),
            }
        ).encode("utf-8")

        try:
            status, body, _ = _http_request(
                f"{base_url}/presentations",
                method="POST",
                data=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
        except urllib.error.URLError as exc:
            logger.error("FlashDocs request failed: %s", exc, exc_info=True)
            return None

        if status not in (200, 201, 202):
            logger.warning(
                "FlashDocs returned HTTP %d: %s",
                status,
                body[:500].decode("utf-8", errors="replace"),
            )
            return None

        try:
            resp_data = json.loads(body)
        except json.JSONDecodeError:
            if len(body) > 1000:
                return body
            return None

        download_url = (
            resp_data.get("download_url")
            or resp_data.get("pptx_url")
            or resp_data.get("url")
            or ""
        )
        if download_url:
            return self._download_file(download_url)

        job_id = resp_data.get("job_id") or resp_data.get("id") or ""
        if job_id:
            return self._poll_generic(
                poll_url=f"{base_url}/presentations/{job_id}",
                headers={"Authorization": f"Bearer {api_key}"},
                tier_name="FlashDocs",
            )

        return None

    # ------------------------------------------------------------------
    # Tier 7: python-pptx (offline, always works)
    # ------------------------------------------------------------------
    def _try_pptx(self, data: dict[str, Any]) -> Optional[bytes]:
        """Generate via the existing ppt_generator.py module.

        This is the guaranteed offline fallback -- it always works since
        ppt_generator.py is a 6,631-line local module with no external
        API dependencies.
        """
        try:
            from ppt_generator import generate_pptx
        except ImportError as exc:
            logger.error(
                "python-pptx fallback failed: could not import ppt_generator: %s",
                exc,
                exc_info=True,
            )
            return None

        try:
            result = generate_pptx(data)
            if isinstance(result, bytes) and len(result) > 0:
                return result
            logger.warning("ppt_generator returned empty/non-bytes result")
            return None
        except Exception as exc:
            logger.error("python-pptx generation failed: %s", exc, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _download_file(self, url: str, timeout: int = 60) -> Optional[bytes]:
        """Download a file from a URL, returning bytes or None."""
        try:
            status, body, _ = _http_request(url, timeout=timeout)
            if status == 200 and len(body) > 1000:
                return body
            logger.warning(
                "Download from %s returned HTTP %d (%d bytes)",
                url,
                status,
                len(body),
            )
        except urllib.error.URLError as exc:
            logger.error("File download failed (%s): %s", url, exc, exc_info=True)
        return None

    def _poll_generic(
        self,
        poll_url: str,
        headers: dict[str, str],
        tier_name: str,
        max_attempts: int = 24,
        interval: int = 5,
    ) -> Optional[bytes]:
        """Generic async poll for presentation generation APIs.

        Polls the given URL until status is completed/success or failed.
        Returns downloaded PPTX bytes or None.
        """
        for attempt in range(max_attempts):
            time.sleep(interval)
            try:
                status, body, _ = _http_request(
                    poll_url, method="GET", headers=headers, timeout=15
                )
            except urllib.error.URLError:
                continue

            if status != 200:
                continue

            try:
                poll_data = json.loads(body)
            except json.JSONDecodeError:
                continue

            gen_status = (poll_data.get("status") or "").lower()
            if gen_status in ("completed", "success", "done", "ready"):
                dl_url = (
                    poll_data.get("download_url")
                    or poll_data.get("pptx_url")
                    or poll_data.get("exportUrl")
                    or poll_data.get("url")
                    or poll_data.get("result", {}).get("download_url")
                    or ""
                )
                if dl_url:
                    return self._download_file(dl_url)
                return None
            elif gen_status in ("failed", "error"):
                error_msg = (
                    poll_data.get("error") or poll_data.get("message") or "unknown"
                )
                logger.warning("%s generation failed: %s", tier_name, error_msg)
                return None

        logger.warning("%s timed out after %ds", tier_name, max_attempts * interval)
        return None

    # ------------------------------------------------------------------
    # Status & health
    # ------------------------------------------------------------------
    def get_status(self) -> dict[str, Any]:
        """Return availability and usage stats for each tier.

        Returns a dict with tier info suitable for a /api/deck/status endpoint.
        """
        tiers_status: list[dict[str, Any]] = []

        for tier_key, tier_name in _TIERS:
            tier_info: dict[str, Any] = {
                "tier": tier_key,
                "name": tier_name,
                "configured": False,
                "available": False,
                "usage": 0,
                "limit": _TIER_LIMITS.get(tier_key),
            }

            if tier_key == "presenton":
                url = os.environ.get("PRESENTON_URL") or ""
                tier_info["configured"] = bool(url)
                tier_info["available"] = bool(url)
            elif tier_key == "gamma":
                key = os.environ.get("GAMMA_API_KEY") or ""
                tier_info["configured"] = bool(key)
                usage = _get_usage("gamma")
                tier_info["usage"] = usage
                tier_info["available"] = bool(key) and usage < _TIER_LIMITS["gamma"]
            elif tier_key == "magicslides":
                key = os.environ.get("MAGICSLIDES_API_KEY") or ""
                tier_info["configured"] = bool(key)
                usage = _get_usage("magicslides")
                tier_info["usage"] = usage
                tier_info["available"] = (
                    bool(key) and usage < _TIER_LIMITS["magicslides"]
                )
            elif tier_key == "google_slides":
                # Check both file-based and base64-encoded credentials
                creds_path = os.environ.get("GOOGLE_SLIDES_CREDENTIALS") or ""
                creds_b64 = os.environ.get("GOOGLE_SLIDES_CREDENTIALS_B64") or ""

                file_exists = Path(creds_path).is_file() if creds_path else False
                has_b64 = bool(creds_b64)

                tier_info["configured"] = file_exists or has_b64
                tier_info["available"] = file_exists or has_b64
            elif tier_key == "alai":
                key = os.environ.get("ALAI_API_KEY") or ""
                tier_info["configured"] = bool(key)
                usage = _get_usage("alai")
                tier_info["usage"] = usage
                tier_info["available"] = bool(key) and usage < _TIER_LIMITS["alai"]
            elif tier_key == "flashdocs":
                key = os.environ.get("FLASHDOCS_API_KEY") or ""
                tier_info["configured"] = bool(key)
                usage = _get_usage("flashdocs")
                tier_info["usage"] = usage
                tier_info["available"] = bool(key) and usage < _TIER_LIMITS["flashdocs"]
            elif tier_key == "pptx":
                try:
                    from ppt_generator import generate_pptx  # noqa: F811

                    tier_info["configured"] = True
                    tier_info["available"] = True
                except ImportError:
                    tier_info["configured"] = False
                    tier_info["available"] = False

            tiers_status.append(tier_info)

        return {
            "month": _current_month(),
            "tiers": tiers_status,
            "total_tiers": len(_TIERS),
            "available_tiers": sum(1 for t in tiers_status if t["available"]),
        }
