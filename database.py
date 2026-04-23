import aiohttp
import logging
import asyncio
import json
import random
import pytz
from html import escape

from datetime import datetime, timedelta
from typing import Optional, List, Union, Dict, Any

from config import GOOGLE_SCRIPT_URL, MAX_RETRIES, RETRY_DELAY
from cache import get_data, set_flag, KeyManager, acquire_lock, delete_data
from supabase_db import (
    get_user_by_telegram_id,
    create_user,
    add_activity as supabase_add_activity,
    get_user_activities,
    get_all_users,
    get_user_activities_in_period,
    get_referrals_count,
    get_weekly_rating,
    add_referral as supabase_add_referral,
)

logger = logging.getLogger(__name__)

# ==============================================================================
# TIMEZONE (single source of truth)
# ==============================================================================
KYIV_TZ = pytz.timezone("Europe/Kyiv")
INACTIVE_DAYS_THRESHOLD = 3


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


def _get_current_week_period() -> tuple[str, str]:
    """
    Returns current week boundaries in ISO format.
    TurboTeam week starts on Sunday at 20:00 Kyiv time.
    """
    now = get_kyiv_now()
    current_sunday_20 = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=20,
        minute=0,
        second=0,
        microsecond=0,
    )
    if now < current_sunday_20:
        week_start = current_sunday_20 - timedelta(days=7)
    else:
        week_start = current_sunday_20

    week_end = week_start + timedelta(days=7)

    return week_start.isoformat(), week_end.isoformat()


async def _get_supabase_user_row(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Resolves Telegram user id -> Supabase users row.
    """
    try:
        return await get_user_by_telegram_id(user_id)
    except Exception as e:
        logger.error(f"[DB] failed to load Supabase user: user_id={user_id}, error={e}")
        return None


def _calculate_training_streak(activities: List[Dict[str, Any]]) -> int:
    """
    Counts consecutive training days (Gym/Street) backward from today in Kyiv timezone.
    Multiple trainings on the same day count as one streak day.
    """
    training_actions = {"Gym", "Street"}
    training_dates = set()

    for activity in activities:
        action_name = str(activity.get("action_name", ""))
        if action_name not in training_actions:
            continue

        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue

        training_dates.add(created_at.date())

    streak = 0
    cursor = get_kyiv_now().date()

    while cursor in training_dates:
        streak += 1
        cursor -= timedelta(days=1)

    return streak


async def _has_activity_today(
    user_id: int,
    action_name: str,
    video_id: str = "",
) -> bool:
    """
    Checks whether this activity already exists today in Kyiv timezone.
    Ignores rollback rows.
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
        current_action_name = str(activity.get("action_name", "")).strip()

        if current_action_name.endswith("Rollback"):
            continue

        if current_action_name != str(action_name):
            continue

        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at or created_at.date() != today:
            continue

        existing_video_id = str(activity.get("video_id") or "").strip()

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
    Legacy GAS request layer. Kept temporarily for compatibility.
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
    user_row = await _get_supabase_user_row(user_id)
    if not user_row:
        return None

    supabase_user_id = user_row.get("id")
    if not supabase_user_id:
        logger.warning(f"[DB] Supabase user row has no id: telegram_user_id={user_id}")
        return None

    try:
        activities = await get_user_activities(str(supabase_user_id), limit=1000)
    except Exception as e:
        logger.error(f"[DB] failed to get user activities for stats: user_id={user_id}, error={e}")
        return None

    hp_total = 0
    for activity in activities:
        try:
            hp_total += int(activity.get("hp_change", 0) or 0)
        except Exception:
            continue

    stats = {
        "user_id": str(supabase_user_id),
        "telegram_user_id": user_row.get("telegram_user_id"),
        "nickname": user_row.get("nickname", ""),
        "hp": hp_total,
        "hp_total": hp_total,
        "activities_count": len(activities),
        "streak": _calculate_training_streak(activities),
    }

    return stats


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

    user_row = await _get_supabase_user_row(user_id)
    exists = user_row is not None

    if exists:
        await set_flag(cache_key, ex=3600)

    return exists


async def register_user_from_quiz(user_id: int, nickname: str, quiz_data: dict) -> bool:
    """
    Registration without duplicate pre-check.
    Existence is already checked in orchestrator before this call.
    """
    try:
        existing_user = await _get_supabase_user_row(user_id)
        if existing_user:
            await set_flag(KeyManager.get_reg_key(user_id), ex=86400)
            return True

        await create_user(
            telegram_user_id=user_id,
            nickname=str(nickname),
            gender=quiz_data.get("gender", "N/A"),
            level=quiz_data.get("level", "N/A"),
            goal=str(quiz_data.get("goal", "N/A"))[:200],
            weekly_plan=str(quiz_data.get("weekly_plan", "N/A"))[:100],
            training_place=str(quiz_data.get("training_place", "N/A"))[:100],
        )

        await set_flag(KeyManager.get_reg_key(user_id), ex=86400)
        return True

    except Exception as e:
        logger.warning(f"[DB] registration failed: user_id={user_id}, error={e}")
        return False


# ==============================================================================
# ANALYTICS
# ==============================================================================
async def get_weekly_top_users():
    try:
        period_start, period_end = _get_current_week_period()
        ranking_rows = await get_weekly_rating(period_start, period_end)
        return ranking_rows[:10]

    except Exception as e:
        logger.error(f"[DB] failed to build weekly top users: {e}", exc_info=True)
        return []


async def reset_weekly_stats() -> bool:
    """
    Supabase weekly rating is calculated by date range,
    so no explicit reset is required anymore.
    """
    return True


async def penalty_user(user_id: int, points: int) -> bool:
    result = await update_user_activity(
        user_id=user_id,
        nickname="system",
        action_name="Penalty",
        hp_change=-abs(int(points)),
        video_id="manual_penalty",
        is_check=False,
        skip_lock=False,
    )
    return result is True


async def get_inactive_users() -> List[str]:
    try:
        users = await get_all_users()
        today = get_kyiv_now().date()
        inactive_users = []

        for user in users:
            telegram_user_id = user.get("telegram_user_id")
            user_uuid = user.get("id")
            nickname = str(user.get("nickname") or telegram_user_id or "Учасник").strip()

            if not telegram_user_id or not user_uuid:
                continue

            activities = await get_user_activities(str(user_uuid), limit=50)

            last_activity_date = None
            for activity in activities:
                action_name = str(activity.get("action_name", "")).strip()
                if action_name.endswith("Rollback"):
                    continue

                created_at = _parse_activity_created_at(activity.get("created_at"))
                if not created_at:
                    continue

                activity_date = created_at.date()
                if last_activity_date is None or activity_date > last_activity_date:
                    last_activity_date = activity_date

            if last_activity_date is None:
                display_name = escape(nickname)
                inactive_users.append(
                    f'<a href="tg://user?id={telegram_user_id}">{display_name}</a>'
                )
                continue

            silent_days = (today - last_activity_date).days

            if silent_days >= INACTIVE_DAYS_THRESHOLD:
                display_name = escape(nickname)
                inactive_users.append(
                    f'<a href="tg://user?id={telegram_user_id}">{display_name}</a>'
                )

        return inactive_users

    except Exception as e:
        logger.error(f"[DB] failed to get inactive users: {e}", exc_info=True)
        return []


async def add_referral_bonus(referrer_id: int, new_user_id: int, new_user_name: str) -> bool:
    try:
        referrer_row = await _get_supabase_user_row(referrer_id)
        new_user_row = await _get_supabase_user_row(new_user_id)

        if not referrer_row or not new_user_row:
            logger.warning(
                f"[DB] referral write failed: referrer={referrer_id} new_user={new_user_id}"
            )
            return False

        referrer_user_uuid = referrer_row.get("id")
        new_user_uuid = new_user_row.get("id")

        if not referrer_user_uuid or not new_user_uuid:
            logger.warning(
                f"[DB] referral UUID missing: referrer={referrer_id} new_user={new_user_id}"
            )
            return False

        await supabase_add_referral(
            referrer_user_id=str(referrer_user_uuid),
            new_user_id=str(new_user_uuid),
        )
        return True

    except Exception as e:
        logger.error(f"[DB] failed to add referral bonus: {e}", exc_info=True)
        return False