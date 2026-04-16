import aiohttp
import logging
import asyncio
import json
import random
import pytz

from datetime import datetime
from typing import Optional, List, Union, Dict, Any

from config import GOOGLE_SCRIPT_URL, MAX_RETRIES, RETRY_DELAY
from cache import get_data, set_flag, KeyManager, acquire_lock, delete_data

logger = logging.getLogger(__name__)

# ==============================================================================
# TIMEZONE (single source of truth)
# ==============================================================================
KYIV_TZ = pytz.timezone("Europe/Kyiv")


def get_kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


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
    - no GAS call if locked
    """

    key = KeyManager.get_action_lock_key(
        user_id, f"{action_name}:{get_kyiv_now().date()}"
    )

    cached = await get_data(key)
    if cached is not None:
        return False

    res = await _request({
        "action": "update_hp",
        "user_id": str(user_id),
        "nickname": nickname,
        "action_name": action_name,
        "hp_change": 0,
        "is_check": "true",
    })

    return bool(res.get("success", False))


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
        lock = await acquire_lock(lock_key, ex=86400)
        if not lock:
            return False

    payload = {
        "action": "update_hp",
        "date": get_kyiv_now().strftime("%d.%m.%Y"),
        "nickname": str(nickname),
        "user_id": str(user_id),
        "action_name": str(action_name),
        "hp_change": int(hp_change),
        "video_id": str(video_id),
        "is_check": "true" if is_check else "false",
    }

    delay = RETRY_DELAY

    for attempt in range(MAX_RETRIES):
        try:
            res = await _request(payload)

            if isinstance(res, dict):
                if res.get("error") == "already_done" or res.get("msg") == "Already done":
                    return "already_done"

                if res.get("success"):
                    return True

            await asyncio.sleep(delay + random.uniform(0, 0.3))
            delay *= 1.6

        except Exception as e:
            logger.error(f"[DB] retry error: {e}")
            await asyncio.sleep(delay)
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
    Safe registration flow:
    1. Check local/cache + GAS existence first.
    2. Do not call register endpoint if user already exists.
    3. Set registration cache only after successful registration.
    """
    already_exists = await check_user_exists(user_id)
    if already_exists:
        logger.info(f"[DB] user already exists: user_id={user_id}")
        return False

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