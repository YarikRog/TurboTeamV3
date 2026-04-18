import aiohttp
import logging
import asyncio
import json
import random
import pytz

from datetime import datetime, timedelta
from typing import Optional, List, Union, Dict, Any

from config import GOOGLE_SCRIPT_URL, MAX_RETRIES, RETRY_DELAY
from cache import get_data, set_flag, KeyManager, acquire_lock, delete_data
from supabase_db import (
    get_user_by_telegram_id,
    add_activity as supabase_add_activity,
    get_user_activities,
)

logger = logging.getLogger(__name__)

# ==============================================================================
# TIMEZONE (single source of truth)
# ==============================================================================
KYIV_TZ = pytz.timezone("Europe/Kyiv")


def get_kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def get_seconds_until_kyiv_midnight() -> int:
    """
    Returns TTL in seconds until next midnight in Kyiv timezone.
    Minimum value is 1 second.
    """
    now = get_kyiv_now()
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return max(1, int((next_midnight - now).total_seconds()))


def _parse_activity_created_at(value: Any) -> Optional[datetime]:
    """
    Safely parses Supabase created_at value into timezone-aware datetime.
    Supports ISO strings with trailing Z.
    """
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)

    return dt.astimezone(KYIV_TZ)


async def _get_supabase_user_row(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Resolves Telegram user id -> Supabase users row.
    """
    try:
        return await get_user_by_telegram_id(user_id)
    except Exception as e:
        logger.error(f"[DB] failed to load Supabase user: user_id={user_id}, error={e}")
        return None


async def _has_activity_today(
    user_id: int,
    action_name: str,
    video_id: str = "",
) -> bool:
    """
    Checks whether this activity already exists today in Kyiv timezone.
    """
    user_row = await _get_supabase_user_row(user_id)
    if not user_row:
        return False

    supabase_user_id = user_row.get("id")
    if not supabase_user_id:
        logger.warning(f"[DB] Supabase user row has no id: telegram_user_id={user_id}")
        return False

    try:
        activities = await get_user_activities(str(supabase_user_id), limit=200)
    except Exception as e:
        logger.error(f"[DB] failed to read activities: user_id={user_id}, error={e}")
        return False

    today = get_kyiv_now().date()
    normalized_video_id = str(video_id or "").strip()

    for activity in activities:
        if str(activity.get("action_name", "")) != str(action_name):
            continue

        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at or created_at.date() != today:
            continue

        existing_video_id = str(activity.get("video_id") or "").strip()

        # If video_id is provided, compare it too.
        # If not provided, action_name + today's date is enough (same behavior as current lock key).
        if normalized_video_id:
            if existing_video_id == normalized_video_id:
                return True
            continue

        return True

    return False


# ==============================================================================
# HTTP LAYER (ULTRA OPTIMIZED SINGLETON)
# ==============================================================================
API_SEMAPHORE = asyncio.Semaphore(20)

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


async def get_session() -> aiohttp.ClientSession:
    """
    High-load safe singleton session.
    - avoids race condition
    - reuses TCP pool
    - minimal lock contention
    """
    global _session

    if _session and not _session.closed:
        return _session

    async with _session_lock:
        if _session and not _session.closed:
            return _session

        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        timeout = aiohttp.ClientTimeout(
            total=25,
            connect=5,
            sock_connect=5,
            sock_read=20,
        )

        _session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )

        logger.info("[DB] HTTP session initialized (PRODUCTION MODE)")

    return _session


async def close_db_session() -> None:
    global _session

    async with _session_lock:
        if _session and not _session.closed:
            await _session.close()
            _session = None
            logger.info("[DB] HTTP session closed")


# ==============================================================================
# INTERNAL CORE REQUEST (ALL TRAFFIC GOES HERE)
# ==============================================================================
async def _request(payload: dict, method: str = "POST") -> Any:
    """
    Single execution point for all GAS traffic.
    """
    session = await get_session()

    async with API_SEMAPHORE:
        try:
            if method == "GET":
                async with session.get(
                    GOOGLE_SCRIPT_URL,
                    params=payload,
                    allow_redirects=True,
                ) as resp:
                    return await _handle_response(resp)

            async with session.post(
                GOOGLE_SCRIPT_URL,
                json=payload,
                allow_redirects=True,
            ) as resp:
                return await _handle_response(resp)
        except Exception as e:
            logger.error(f"[DB] critical request error: {e}")
            return {"success": False}


async def _handle_response(resp: aiohttp.ClientResponse) -> Any:
    """
    Safe JSON parsing + fallback protection.
    """
    try:
        text = await resp.text()

        if resp.status != 200:
            return {"success": False, "status": resp.status}

        try:
            return json.loads(text)
        except Exception:
            logger.error(f"[DB] invalid JSON: {text[:200]}")
            return {"success": False}

    except Exception as e:
        logger.error(f"[DB] response error: {e}")
        return {"success": False}


# ==============================================================================
# PUBLIC API (FAST PATH FIRST)
# ==============================================================================

async def check_activity_limit(user_id: int, nickname: str, action_name: str) -> bool:
    """
    FAST PATH:
    - Redis cache first
    - no DB call if locked

    Returns True if activity can be added, False if already exists today.
    """

    key = KeyManager.get_action_lock_key(
        user_id, f"{action_name}:{get_kyiv_now().date()}"
    )

    cached = await get_data(key)
    if cached is not None:
        return False

    already_exists = await _has_activity_today(user_id, action_name)
    return not already_exists


async def add_activity(
    user_id: int,
    nickname: str,
    action_name: str,
    hp_change: int,
    video_id: str = ""
) -> Union[bool, str]:

    return await update_user_activity(
        user_id, nickname, action_name, hp_change, video_id, False
    )


async def get_user_stats(user_id: int) -> Optional[Dict]:
    res = await _request({
        "action": "get_user",
        "user_id": str(user_id),
    }, method="GET")

    return res if isinstance(res, dict) else None


# ==============================================================================
# CORE WRITE (ZERO LOSS + IDEMPOTENCY LOCK)
# ==============================================================================
async def update_user_activity(
    user_id: int,
    nickname: str,
    action_name: str,
    hp_change: int,
    video_id: str = "",
    is_check: bool = False,
    skip_lock: bool = False,
) -> Union[bool, str]:

    today = get_kyiv_now().strftime("%Y-%m-%d")
    lock_key = KeyManager.get_action_lock_key(user_id, f"{action_name}:{today}")

    if not skip_lock:
        lock = await acquire_lock(lock_key, ex=get_seconds_until_kyiv_midnight())
        if not lock:
            return False

    delay = RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        try:
            if is_check:
                already_exists = await _has_activity_today(user_id, action_name, video_id)
                return not already_exists

            already_exists = await _has_activity_today(user_id, action_name, video_id)
            if already_exists:
                return "already_done"

            user_row = await _get_supabase_user_row(user_id)
            if not user_row:
                logger.warning(f"[DB] user not found in Supabase: telegram_user_id={user_id}")
                await asyncio.sleep(delay + random.uniform(0, 0.3))
                delay *= 1.6
                continue

            supabase_user_id = user_row.get("id")
            if not supabase_user_id:
                logger.warning(f"[DB] Supabase user row has no id: telegram_user_id={user_id}")
                await asyncio.sleep(delay + random.uniform(0, 0.3))
                delay *= 1.6
                continue

            await supabase_add_activity(
                user_id=str(supabase_user_id),
                action_name=str(action_name),
                hp_change=int(hp_change),
                video_status="✅",
                video_id=str(video_id) if video_id else None,
            )

            return True

        except Exception as e:
            logger.error(f"[DB] retry error: {e}")
            await asyncio.sleep(delay + random.uniform(0, 0.3))
            delay *= 1.6

    if not skip_lock:
        await delete_data(lock_key)
    return False


# ==============================================================================
# USER SYSTEM
# ==============================================================================
async def check_user_exists(user_id: int) -> bool:
    cache_key = KeyManager.get_reg_key(user_id)

    cached = await get_data(cache_key)
    if cached is not None:
        return True

    res = await _request({
        "action": "check_user",
        "user_id": str(user_id),
    }, method="GET")

    exists = bool(res.get("exists", False))

    if exists:
        await set_flag(cache_key, ex=3600)

    return exists


async def register_user_from_quiz(user_id: int, nickname: str, quiz_data: dict) -> bool:
    """
    Registration without duplicate pre-check.
    Existence is already checked in orchestrator before this call.
    """
    res = await _request({
        "action": "register_user",
        "date": get_kyiv_now().strftime("%d.%m.%Y"),
        "nickname": str(nickname),
        "user_id": str(user_id),
        "gender": quiz_data.get("gender", "N/A"),
        "level": quiz_data.get("level", "N/A"),
        "goal": str(quiz_data.get("goal", "N/A"))[:200],
    })

    if isinstance(res, dict) and res.get("success"):
        await set_flag(KeyManager.get_reg_key(user_id), ex=86400)
        return True

    logger.warning(f"[DB] registration failed: user_id={user_id}, response={res}")
    return False


# ==============================================================================
# ANALYTICS
# ==============================================================================
async def get_weekly_top_users():
    return await _request({"action": "get_weekly_leader"})


async def reset_weekly_stats() -> bool:
    res = await _request({"action": "reset_weekly_stats"})
    return bool(res.get("success"))


async def penalty_user(user_id: int, points: int) -> bool:
    res = await _request({
        "action": "penalty",
        "user_id": str(user_id),
        "points": int(points),
        "date": get_kyiv_now().strftime("%d.%m.%Y"),
    })
    return bool(res.get("success"))


async def get_inactive_users() -> List[str]:
    res = await _request({"action": "get_inactive"}, method="GET")
    return res.get("success", []) if isinstance(res, dict) else []


async def add_referral_bonus(referrer_id: int, new_user_id: int, new_user_name: str) -> bool:
    res = await _request({
        "action": "add_referral",
        "referrer_id": str(referrer_id),
        "new_user_id": str(new_user_id),
        "new_user_name": new_user_name,
        "date": get_kyiv_now().strftime("%d.%m.%Y"),
    })

    return bool(res.get("success"))