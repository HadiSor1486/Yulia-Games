# ═══════════════════════════════════════════════════════════════════════════
#  github_storage.py  —  Bulletproof Persistent JSON Storage via GitHub
#
#  FEATURES:
#    • Auto-retry on 409 conflicts (fetches latest SHA, merges, retries)
#    • Per-file async locks — no concurrent pushes of the same file
#    • Queue-based push scheduler — reliable, cancellable, deduplicated
#    • Exponential backoff with jitter for transient failures
#    • Remote merge strategy — fetches remote, merges local changes, pushes
#    • Graceful degradation — falls back to local disk if GitHub is down
#
#  ENV VARS:
#    GITHUB_TOKEN      — fine-grained PAT with Contents:RW
#    GITHUB_USER       — default: HadiSor1486
#    GITHUB_REPO       — default: Yulia-Games
#    GITHUB_BRANCH     — default: main
#    GITHUB_JSON_PATH  — subfolder, e.g. "bot" → bot/accounts.json
# ═══════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import asyncio
import base64
import json
import os
import random
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
FILES_TO_SYNC = ("accounts.json", "banned.json")

# Retry settings
_MAX_RETRIES = 5
_BASE_DELAY = 1.0        # seconds
_MAX_DELAY = 30.0        # cap exponential backoff
_JITTER = 0.3            # ±30% jitter
_DEBOUNCE_DELAY = 2.0    # seconds of quiet time before push
_LOCK_TIMEOUT = 30.0     # max seconds to wait for a file lock

_http: httpx.AsyncClient | None = None

# Per-file async locks to prevent concurrent pushes
_file_locks: dict[str, asyncio.Lock] = {fn: asyncio.Lock() for fn in FILES_TO_SYNC}

# Pending push queue: filename → (data, timestamp)
_push_queue: dict[str, tuple[Any, float]] = {}

# Track whether a queue processor is running
_queue_processor_task: asyncio.Task | None = None

# ── SHA cache: filename → sha string ──────────────────────────────────────
_file_shas: dict[str, str | None] = {
    "accounts.json": None,
    "banned.json":   None,
}


def _github_path(filename: str) -> str:
    if GITHUB_JSON_PATH:
        return f"{GITHUB_JSON_PATH}/{filename}"
    return filename


def _http_client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=10.0),
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            follow_redirects=True,
        )
    return _http


def _jittered_delay(attempt: int) -> float:
    """Exponential backoff with jitter."""
    delay = min(_BASE_DELAY * (2 ** attempt), _MAX_DELAY)
    jitter = delay * _JITTER * (2 * random.random() - 1)
    return max(0.1, delay + jitter)


async def _github_api(method: str, path: str, **kwargs) -> dict | None:
    """Call GitHub REST API with retry on transient errors."""
    if not GITHUB_TOKEN:
        return None
    client = _http_client()
    url = f"{API_BASE}{path}"
    for attempt in range(_MAX_RETRIES):
        try:
            resp = await client.request(method, url, **kwargs)
            if resp.status_code in (200, 201):
                return resp.json()
            # 409 = conflict (stale SHA) — let caller handle with retry
            if resp.status_code == 409:
                logger.warning(f"[github] {method} {path} → 409 conflict")
                return {"__error_409": True, "raw": resp.text[:300]}
            # 404 = file doesn't exist yet
            if resp.status_code == 404:
                logger.debug(f"[github] {method} {path} → 404")
                return None
            # Rate limit
            if resp.status_code == 403 and "rate limit" in resp.text.lower():
                reset_at = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(reset_at - int(time.time()), 10)
                logger.warning(f"[github] rate limited, waiting {wait}s…")
                await asyncio.sleep(wait)
                continue
            logger.warning(f"[github] {method} {path} → {resp.status_code}: {resp.text[:200]}")
            return None
        except httpx.TimeoutException:
            logger.warning(f"[github] {method} {path} → timeout (attempt {attempt + 1})")
        except httpx.NetworkError as e:
            logger.warning(f"[github] {method} {path} → network error: {e}")
        except Exception as e:
            logger.warning(f"[github] {method} {path} → {type(e).__name__}: {e}")
        if attempt < _MAX_RETRIES - 1:
            await asyncio.sleep(_jittered_delay(attempt))
    return None


async def _fetch_sha(filename: str) -> str | None:
    """Fetch the latest blob SHA for a file from GitHub. Returns None on error."""
    path = _github_path(filename)
    data = await _github_api("GET", f"/contents/{path}?ref={GITHUB_BRANCH}")
    if data and isinstance(data, dict) and "sha" in data:
        _file_shas[filename] = data["sha"]
        return data["sha"]
    return None


async def fetch_from_github(filename: str) -> dict | list | None:
    """Download and parse a JSON file from GitHub. Cache its SHA for pushes."""
    if filename not in FILES_TO_SYNC:
        return None
    path = _github_path(filename)
    data = await _github_api("GET", f"/contents/{path}?ref={GITHUB_BRANCH}")
    if data is None:
        return None
    if isinstance(data, dict) and data.get("__error_409"):
        return None
    _file_shas[filename] = data.get("sha")
    content = data.get("content", "")
    try:
        decoded = base64.b64decode(content.replace("\n", "")).decode("utf-8")
        return json.loads(decoded)
    except Exception as e:
        logger.error(f"[github] decode {filename}: {e}")
        return None


async def _do_push_with_retry(filename: str, data: dict | list) -> bool:
    """
    Push a file to GitHub with full conflict resolution.
    1. Acquire per-file lock
    2. Fetch latest SHA
    3. Attempt push
    4. On 409: fetch remote, merge, retry
    """
    if not GITHUB_TOKEN:
        return False

    lock = _file_locks[filename]
    acquired = False
    try:
        acquired = await asyncio.wait_for(lock.acquire(), timeout=_LOCK_TIMEOUT)
    except asyncio.TimeoutError:
        logger.error(f"[github] could not acquire lock for {filename} — skipping push")
        return False

    try:
        for attempt in range(_MAX_RETRIES):
            # Always refresh SHA before pushing
            sha = await _fetch_sha(filename)

            path = _github_path(filename)
            payload = {
                "message": f"bot: update {filename} @ {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
                "content": base64.b64encode(
                    json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
                ).decode(),
                "branch": GITHUB_BRANCH,
            }
            if sha:
                payload["sha"] = sha

            result = await _github_api("PUT", f"/contents/{path}", json=payload)

            # Success
            if result and isinstance(result, dict) and result.get("content", {}).get("sha"):
                _file_shas[filename] = result["content"]["sha"]
                logger.info(f"[github] synced {filename}")
                return True

            # 409 conflict — merge and retry
            if result and isinstance(result, dict) and result.get("__error_409"):
                logger.warning(f"[github] 409 on {filename}, attempt {attempt + 1}/{_MAX_RETRIES} — merging…")
                remote_data = await fetch_from_github(filename)
                if remote_data is not None and isinstance(remote_data, dict) and isinstance(data, dict):
                    merged = _deep_merge(remote_data, data)
                    data = merged
                    logger.info(f"[github] merged remote + local for {filename}")
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(_jittered_delay(attempt))
                continue

            # Other failure — retry with backoff
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(_jittered_delay(attempt))

        logger.error(f"[github] exhausted retries for {filename}")
        return False

    finally:
        if acquired:
            lock.release()


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. For dict values, recurse."""
    merged = dict(base)
    for key, val in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(val, dict):
            merged[key] = _deep_merge(merged[key], val)
        else:
            merged[key] = val
    return merged


async def _queue_processor():
    """Background task that processes the push queue with debouncing."""
    global _queue_processor_task
    try:
        while True:
            await asyncio.sleep(0.5)
            if not _push_queue:
                continue

            now = time.time()
            ready = []
            pending = {}
            for fn, (data, ts) in _push_queue.items():
                if now - ts >= _DEBOUNCE_DELAY:
                    ready.append((fn, data))
                else:
                    pending[fn] = (data, ts)

            if not ready:
                continue

            _push_queue.clear()
            _push_queue.update(pending)

            for fn, data in ready:
                try:
                    await _do_push_with_retry(fn, data)
                except Exception as e:
                    logger.exception(f"[github] queue push failed for {fn}: {e}")
                await asyncio.sleep(0.3)

    except asyncio.CancelledError:
        for fn, (data, _) in list(_push_queue.items()):
            try:
                await _do_push_with_retry(fn, data)
            except Exception:
                pass
        _push_queue.clear()
        raise
    finally:
        _queue_processor_task = None


def schedule_push(filename: str, data: dict | list):
    """
    Queue a file for GitHub sync. Debounced — multiple rapid calls
    for the same file are collapsed into a single push.
    """
    global _queue_processor_task
    if not GITHUB_TOKEN or filename not in FILES_TO_SYNC:
        return

    _push_queue[filename] = (data, time.time())

    if _queue_processor_task is None or _queue_processor_task.done():
        try:
            loop = asyncio.get_running_loop()
            _queue_processor_task = loop.create_task(_queue_processor())
        except RuntimeError:
            pass


async def pull_all() -> dict[str, dict | list]:
    """Fetch all JSON files from GitHub at startup."""
    results: dict[str, dict | list] = {}
    for fn in FILES_TO_SYNC:
        data = await fetch_from_github(fn)
        if data is not None:
            results[fn] = data
            logger.info(f"[github] loaded {fn} from GitHub ({len(json.dumps(data))} chars)")
        else:
            logger.info(f"[github] {fn} not on GitHub (will use local/create new)")
    return results


async def close():
    """Flush pending pushes and close HTTP client."""
    global _queue_processor_task
    if _queue_processor_task and not _queue_processor_task.done():
        with suppress(Exception):
            _queue_processor_task.cancel()
        with suppress(Exception):
            await _queue_processor_task

    _push_queue.clear()

    global _http
    if _http and not _http.is_closed:
        await _http.aclose()
