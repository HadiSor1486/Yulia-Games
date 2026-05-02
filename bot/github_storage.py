# ═══════════════════════════════════════════════════════════════════════════
#  github_storage.py  —  Persistent JSON storage backed by GitHub
#
#  Reads/writes JSON files to a GitHub repo so data survives Render restarts.
#  Falls back to local disk if GitHub is unreachable.
#
#  ENV VARS REQUIRED:
#    GITHUB_TOKEN      —  fine-grained personal access token with Contents:RW
#    GITHUB_USER       —  your GitHub username  (default: HadiSor1486)
#    GITHUB_REPO       —  the repo name          (default: Yulia-Games)
#    GITHUB_JSON_PATH  —  subfolder in repo where JSON files live
#                          e.g. "bot" if files are at bot/accounts.json
#                          (default: empty → root of repo)
# ═══════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from contextlib import suppress
from typing import Any

import httpx
from loguru import logger

# ── Configuration ────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_USER  = os.getenv("GITHUB_USER",  "HadiSor1486")
GITHUB_REPO  = os.getenv("GITHUB_REPO",  "Yulia-Games")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
GITHUB_JSON_PATH = os.getenv("GITHUB_JSON_PATH", "").strip("/")

API_BASE = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}"
FILES_TO_SYNC = ("accounts.json", "banned.json", "members.json")

# In-memory write debounce — prevents GitHub API spam
_write_timers: dict[str, Any] = {}  # filename → asyncio.TimerHandle
_debounce_delay = 3.0  # seconds
_http: httpx.AsyncClient | None = None

# Track the last known GitHub blob SHA for each file so we can update
# without fetching first every time.
_file_shas: dict[str, str | None] = {
    "accounts.json": None,
    "banned.json":   None,
    "members.json":  None,
}


def _github_path(filename: str) -> str:
    """Return the path inside the repo for *filename*."""
    if GITHUB_JSON_PATH:
        return f"{GITHUB_JSON_PATH}/{filename}"
    return filename


def _http_client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(
            timeout=httpx.Timeout(15),
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
    return _http


async def _github_api(method: str, path: str, **kwargs) -> dict | None:
    """Call the GitHub REST API and return parsed JSON, or None on failure."""
    if not GITHUB_TOKEN:
        return None
    client = _http_client()
    url = f"{API_BASE}{path}"
    try:
        resp = await client.request(method, url, **kwargs)
        if resp.status_code in (200, 201):
            return resp.json()
        # Log non-2xx for debugging
        logger.warning(f"[github] {method} {path} → {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"[github] {method} {path} → {e}")
    return None


# ── Public API ───────────────────────────────────────────────────────────────

async def fetch_from_github(filename: str) -> dict | list | None:
    """
    Download *filename* from GitHub and return the parsed JSON contents.
    Returns None if the file doesn't exist on GitHub or the call fails.
    Also caches the blob SHA so future pushes are cheap.
    """
    if filename not in FILES_TO_SYNC:
        return None

    path = _github_path(filename)
    data = await _github_api("GET", f"/contents/{path}?ref={GITHUB_BRANCH}")
    if data is None:
        return None

    # Cache SHA for later updates
    _file_shas[filename] = data.get("sha")

    content = data.get("content", "")
    try:
        decoded = base64.b64decode(content.replace("\n", "")).decode("utf-8")
        return json.loads(decoded)
    except Exception as e:
        logger.error(f"[github] decode {filename}: {e}")
        return None


async def push_to_github(filename: str, data: dict | list) -> bool:
    """
    Overwrite *filename* on GitHub with *data* (JSON-serialised).
    Uses the cached SHA if available; falls back to fetching first.
    Returns True on success.
    """
    if filename not in FILES_TO_SYNC:
        return False

    sha = _file_shas.get(filename)
    path = _github_path(filename)

    # If we don't have a SHA yet, try to fetch it first
    if not sha:
        info = await _github_api("GET", f"/contents/{path}?ref={GITHUB_BRANCH}")
        if info:
            sha = info.get("sha")
            _file_shas[filename] = sha
        # If file doesn't exist yet, sha stays None — GitHub will create it

    payload = {
        "message": f"bot: update {filename} @ {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
        "content": base64.b64encode(json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")).decode(),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    result = await _github_api("PUT", f"/contents/{path}", json=payload)
    if result and result.get("content", {}).get("sha"):
        _file_shas[filename] = result["content"]["sha"]
        logger.debug(f"[github] pushed {filename}")
        return True
    return False


def schedule_push(filename: str, data: dict | list):
    """
    Debounced push — call this instead of push_to_github directly.
    Waits *DEBOUNCE_DELAY* seconds of quiet time before syncing to GitHub.
    Safe to call from synchronous code (uses asyncio.get_event_loop).
    """
    if not GITHUB_TOKEN:
        return

    # Cancel any pending timer for this file
    old_timer = _write_timers.pop(filename, None)
    if old_timer:
        with suppress(Exception):
            old_timer.cancel()

    loop = asyncio.get_event_loop()

    async def _do_push():
        await asyncio.sleep(_debounce_delay)
        _write_timers.pop(filename, None)
        success = await push_to_github(filename, data)
        if success:
            logger.info(f"[github] synced {filename}")
        else:
            logger.warning(f"[github] failed to sync {filename}")

    _write_timers[filename] = loop.create_task(_do_push())


async def pull_all() -> dict[str, dict | list]:
    """
    Fetch all three JSON files from GitHub.
    Returns a dict mapping filename → parsed data (or {} if missing/failed).
    Call this once at bot startup.
    """
    results: dict[str, dict | list] = {}
    for fn in FILES_TO_SYNC:
        data = await fetch_from_github(fn)
        if data is not None:
            results[fn] = data
            logger.info(f"[github] loaded {fn} from GitHub ({len(str(data))} chars)")
        else:
            logger.info(f"[github] {fn} not on GitHub (will use local/create new)")
    return results


async def close():
    """Flush any pending pushes and close the HTTP client."""
    # Cancel & await pending debounced tasks
    for fn in list(_write_timers):
        task = _write_timers.pop(fn, None)
        if task:
            with suppress(Exception):
                task.cancel()
            with suppress(Exception):
                await task
    global _http
    if _http and not _http.is_closed:
        await _http.aclose()

