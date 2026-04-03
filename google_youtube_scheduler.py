"""YouTube Analytics + Cloud Scheduler integration for recruitment video intelligence.

Auth: GOOGLE_MAPS_API_KEY for YouTube Data API v3, service account via
GOOGLE_SLIDES_CREDENTIALS_B64 for Cloud Scheduler (bearer token).
Env: GCP_PROJECT_ID (default: gen-lang-client-0603536849), GCP_LOCATION (default: us-central1).
"""

from __future__ import annotations

import json, logging, math, os, ssl, threading
import urllib.error, urllib.parse, urllib.request
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_YT_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
_YT_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
_YT_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
_SCHEDULER_BASE = "https://cloudscheduler.googleapis.com/v1"
_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID") or "gen-lang-client-0603536849"
_GCP_LOCATION = os.environ.get("GCP_LOCATION") or "us-central1"
_lock = threading.Lock()
_ssl_ctx = ssl.create_default_context()
_TIMEOUT = 15


def _get_api_key() -> Optional[str]:
    """Return the Google API key from environment, or None."""
    return os.environ.get("GOOGLE_MAPS_API_KEY") or None


def _get_service_account_token() -> Optional[str]:
    """Obtain an OAuth2 bearer token via the shared service account."""
    try:
        from sheets_export import _get_access_token

        return _get_access_token()
    except ImportError:
        logger.warning("sheets_export not available for service account auth")
        return None
    except Exception as exc:
        logger.error("Service account token failed: %s", exc, exc_info=True)
        return None


def _yt_request(url: str, params: Dict[str, str]) -> Optional[dict]:
    """Authenticated GET to YouTube Data API v3. Returns parsed JSON or None."""
    api_key = _get_api_key()
    if not api_key:
        logger.error("YouTube: GOOGLE_MAPS_API_KEY not configured")
        return None
    params["key"] = api_key
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url)
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if "error" in data:
            logger.error(
                "YouTube API error: %s",
                (data.get("error") or {}).get("message") or "unknown",
            )
            return None
        return data
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("YouTube HTTP %d: %s", exc.code, body, exc_info=True)
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("YouTube request failed: %s", exc, exc_info=True)
        return None


def _scheduler_request(
    method: str, path: str, body: Optional[dict] = None
) -> Optional[dict]:
    """Authenticated request to Cloud Scheduler REST API. Returns parsed JSON or None."""
    token = _get_service_account_token()
    if not token:
        logger.error("Cloud Scheduler: no service account configured")
        return None
    url = f"{_SCHEDULER_BASE}/{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data_bytes = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("Scheduler HTTP %d: %s", exc.code, err_body, exc_info=True)
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Scheduler request failed: %s", exc, exc_info=True)
        return None


# -- YouTube public API ------------------------------------------------------


def search_recruitment_videos(query: str, max_results: int = 10) -> list[dict]:
    """Search YouTube for recruitment/employer branding videos.

    Returns list of dicts: {title, channel, views, likes, published, url, thumbnail}.
    """
    if not query or not query.strip():
        return []
    max_results = max(1, min(50, max_results))
    data = _yt_request(
        _YT_SEARCH_URL,
        {
            "part": "snippet",
            "q": f"{query.strip()} recruitment hiring employer branding",
            "type": "video",
            "maxResults": str(max_results),
            "order": "relevance",
            "relevanceLanguage": "en",
        },
    )
    if not data:
        return []
    items = data.get("items") or []
    if not items:
        return []
    video_ids = [
        i.get("id", {}).get("videoId") or ""
        for i in items
        if i.get("id", {}).get("videoId")
    ]
    stats_map = _bulk_video_stats(video_ids) if video_ids else {}
    results: list[dict] = []
    for item in items:
        vid_id = (item.get("id") or {}).get("videoId") or ""
        snippet = item.get("snippet") or {}
        stats = stats_map.get(vid_id) or {}
        results.append(
            {
                "title": snippet.get("title") or "",
                "channel": snippet.get("channelTitle") or "",
                "views": int(stats.get("viewCount") or 0),
                "likes": int(stats.get("likeCount") or 0),
                "published": snippet.get("publishedAt") or "",
                "url": f"https://www.youtube.com/watch?v={vid_id}" if vid_id else "",
                "thumbnail": (snippet.get("thumbnails") or {})
                .get("high", {})
                .get("url")
                or "",
            }
        )
    return results


def _bulk_video_stats(video_ids: list[str]) -> Dict[str, dict]:
    """Fetch statistics for multiple video IDs in one API call."""
    if not video_ids:
        return {}
    data = _yt_request(
        _YT_VIDEOS_URL, {"part": "statistics", "id": ",".join(video_ids[:50])}
    )
    if not data:
        return {}
    return {
        (item.get("id") or ""): (item.get("statistics") or {})
        for item in data.get("items") or []
    }


def get_video_stats(video_id: str) -> dict:
    """Get view_count, like_count, comment_count, duration for a video."""
    if not video_id or not video_id.strip():
        return {"error": "video_id is required"}
    data = _yt_request(
        _YT_VIDEOS_URL, {"part": "statistics,contentDetails", "id": video_id.strip()}
    )
    if not data:
        return {"error": "video stats request failed"}
    items = data.get("items") or []
    if not items:
        return {"error": "video not found", "video_id": video_id}
    stats = items[0].get("statistics") or {}
    details = items[0].get("contentDetails") or {}
    return {
        "view_count": int(stats.get("viewCount") or 0),
        "like_count": int(stats.get("likeCount") or 0),
        "comment_count": int(stats.get("commentCount") or 0),
        "duration": details.get("duration") or "",
    }


def get_channel_stats(channel_id: str) -> dict:
    """Get subscriber_count, video_count, total_views, title, description for a channel."""
    if not channel_id or not channel_id.strip():
        return {"error": "channel_id is required"}
    data = _yt_request(
        _YT_CHANNELS_URL, {"part": "statistics,snippet", "id": channel_id.strip()}
    )
    if not data:
        return {"error": "channel stats request failed"}
    items = data.get("items") or []
    if not items:
        return {"error": "channel not found", "channel_id": channel_id}
    stats = items[0].get("statistics") or {}
    snippet = items[0].get("snippet") or {}
    return {
        "subscriber_count": int(stats.get("subscriberCount") or 0),
        "video_count": int(stats.get("videoCount") or 0),
        "total_views": int(stats.get("viewCount") or 0),
        "title": snippet.get("title") or "",
        "description": snippet.get("description") or "",
    }


def analyze_employer_brand(company_name: str) -> dict:
    """Analyze a company's employer brand presence on YouTube.

    Returns {video_count, total_views, avg_views, top_videos, brand_presence_score (1-10)}.
    """
    if not company_name or not company_name.strip():
        return {"error": "company_name is required"}
    with _lock:
        videos = search_recruitment_videos(company_name.strip(), max_results=25)
    if not videos:
        return {
            "company": company_name,
            "video_count": 0,
            "total_views": 0,
            "avg_views": 0,
            "top_videos": [],
            "brand_presence_score": 1,
        }
    total_views = sum(v.get("views") or 0 for v in videos)
    avg_views = total_views // len(videos) if videos else 0
    top_videos = sorted(videos, key=lambda v: v.get("views") or 0, reverse=True)[:5]
    # Score: volume (0-3) + reach (0-4) + engagement (0-3) = 1-10
    vol = min(3.0, math.log10(max(len(videos), 1) + 1))
    reach = min(4.0, math.log10(max(total_views, 1) + 1) / 1.5)
    eng = min(3.0, math.log10(max(avg_views, 1) + 1) / 1.2)
    score = max(1, min(10, round(vol + reach + eng)))
    return {
        "company": company_name,
        "video_count": len(videos),
        "total_views": total_views,
        "avg_views": avg_views,
        "top_videos": top_videos,
        "brand_presence_score": score,
    }


# -- Cloud Scheduler public API -----------------------------------------------


def create_data_refresh_schedule(
    job_name: str, schedule_cron: str, target_url: str
) -> dict:
    """Create a Cloud Scheduler job for periodic data refresh.

    Args:
        job_name: Job identifier (e.g. "refresh-bls-data").
        schedule_cron: Cron expression (e.g. "0 6 * * 1" for Mon 6am).
        target_url: HTTP endpoint to POST on each trigger.
    """
    if not job_name or not schedule_cron or not target_url:
        return {"error": "job_name, schedule_cron, and target_url are required"}
    parent = f"projects/{_GCP_PROJECT}/locations/{_GCP_LOCATION}"
    body = {
        "name": f"{parent}/jobs/{job_name}",
        "schedule": schedule_cron,
        "timeZone": "America/New_York",
        "httpTarget": {"uri": target_url, "httpMethod": "POST"},
    }
    result = _scheduler_request("POST", f"{parent}/jobs", body)
    if not result:
        return {"error": "failed to create scheduler job"}
    return {
        "name": result.get("name") or "",
        "schedule": result.get("schedule") or "",
        "state": result.get("state") or "",
        "target_url": (result.get("httpTarget") or {}).get("uri") or "",
    }


def list_scheduled_jobs() -> list[dict]:
    """List all Cloud Scheduler jobs. Returns [{name, schedule, state, last_run}]."""
    parent = f"projects/{_GCP_PROJECT}/locations/{_GCP_LOCATION}"
    data = _scheduler_request("GET", f"{parent}/jobs")
    if not data:
        return []
    return [
        {
            "name": (j.get("name") or "").rsplit("/", 1)[-1],
            "schedule": j.get("schedule") or "",
            "state": j.get("state") or "",
            "last_run": (j.get("status") or {}).get("lastAttemptTime") or "",
        }
        for j in data.get("jobs") or []
    ]


# -- Health check -------------------------------------------------------------


def get_status() -> dict:
    """Health check for YouTube + Cloud Scheduler integration."""
    api_key = _get_api_key()
    sa_available = False
    try:
        from sheets_export import _load_credentials

        sa_available = _load_credentials() is not None
    except ImportError:
        pass
    return {
        "youtube": {
            "configured": bool(api_key),
            "auth_mode": "api_key" if api_key else "none",
            "endpoints": {
                "search_recruitment_videos": bool(api_key),
                "get_video_stats": bool(api_key),
                "get_channel_stats": bool(api_key),
                "analyze_employer_brand": bool(api_key),
            },
        },
        "cloud_scheduler": {
            "configured": sa_available,
            "auth_mode": "service_account" if sa_available else "none",
            "project": _GCP_PROJECT,
            "location": _GCP_LOCATION,
            "endpoints": {
                "create_data_refresh_schedule": sa_available,
                "list_scheduled_jobs": sa_available,
            },
        },
    }
