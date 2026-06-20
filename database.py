import aiohttp
import logging
import asyncio
import json
import random
import pytz
from html import escape

from datetime import datetime, timedelta, date
from typing import Optional, List, Union, Dict, Any

from config import GOOGLE_SCRIPT_URL, MAX_RETRIES, RETRY_DELAY
from cache import get_data, set_flag, set_data, KeyManager, acquire_lock, delete_data
from supabase_db import (
    get_user_by_telegram_id,
    create_user,
    add_activity as supabase_add_activity,
    get_user_activities,
    get_all_users,
    get_user_activities_in_period,
    get_user_activities_by_actions_in_period,
    get_referrals_count,
    get_weekly_rating,
    add_referral as supabase_add_referral,
    get_last_user_activity_by_actions,
    get_user_achievements,
)

logger = logging.getLogger(__name__)

KYIV_TZ = pytz.timezone("Europe/Kyiv")
INACTIVE_DAYS_THRESHOLD = 3
LAST_WARNING_DAYS_THRESHOLD = 7
AUTO_REMOVE_DAYS_THRESHOLD = 8

AUTO_REMOVE_REDIS_PREFIX = "turbo:auto_removed"

REAL_ACTIVITY_ACTIONS = {
    "Gym",
    "Street",
    "Rest",
    "Skipped",
    "Welcome Bonus",
    "Returned",
    "Weekly Challenge",
}

TRAINING_ACTIVITY_ACTIONS = {"Gym", "Street"}
TRAINING_ROLLBACK_ACTIONS = {"Gym Rollback", "Street Rollback"}

INACTIVITY_ACTIVITY_LIMIT = 1000
WEEKLY_STREAK_MAX = 14
USER_CACHE_TTL = 86400


def get_kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def get_seconds_until_kyiv_midnight() -> int:
    now = get_kyiv_now()
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return max(1, int((next_midnight - now).total_seconds()))


def _get_auto_removed_key(user_id: int) -> str:
    return f"{AUTO_REMOVE_REDIS_PREFIX}:{user_id}"


async def _is_auto_removed_user(telegram_user_id: int) -> bool:
    removed_key = _get_auto_removed_key(int(telegram_user_id))
    already_removed = await get_data(removed_key)
    return already_removed is not None


async def get_currently_banned_telegram_ids() -> set:
    """All telegram_user_ids currently tracked as auto-removed (banned) in Redis."""
    from cache import redis_client

    if redis_client is None:
        return set()

    banned_ids: set = set()
    cursor = 0

    while True:
        cursor, keys = await redis_client.scan(
            cursor=cursor,
            match=f"{AUTO_REMOVE_REDIS_PREFIX}:*",
            count=100,
        )

        for key in keys:
            try:
                banned_ids.add(int(str(key).rsplit(":", 1)[-1]))
            except ValueError:
                continue

        if cursor == 0:
            break

    return banned_ids


def _parse_activity_created_at(value: Any) -> Optional[datetime]:
    """
    Safely parses Supabase/Postgres created_at into Kyiv-aware datetime.

    Supports:
    - datetime objects
    - 2026-05-14T09:23:14.6173+00:00
    - 2026-05-13T16:50:24.31573+00:00
    - 2026-05-12T15:27:17.827410+00:00
    - 2026-05-12T15:27:17Z
    - 2026-05-12 15:27:17.82741+00
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        normalized = raw.replace(" ", "T").replace("Z", "+00:00")

        if normalized.endswith("+00"):
            normalized = normalized[:-3] + "+00:00"

        try:
            if "." in normalized:
                date_part, tail = normalized.split(".", 1)

                tz_part = ""
                fraction_part = tail

                plus_index = tail.find("+")
                minus_index = tail.find("-", 1)

                if plus_index != -1:
                    fraction_part = tail[:plus_index]
                    tz_part = tail[plus_index:]
                elif minus_index != -1:
                    fraction_part = tail[:minus_index]
                    tz_part = tail[minus_index:]

                digits = "".join(ch for ch in fraction_part if ch.isdigit())
                digits = digits[:6].ljust(6, "0")

                normalized = f"{date_part}.{digits}{tz_part}"

            dt = datetime.fromisoformat(normalized)

        except Exception as e:
            logger.error(
                "[DATE_PARSE_ERROR] Failed to parse created_at=%r normalized=%r error=%s",
                value,
                normalized,
                e,
                exc_info=True,
            )
            return None
    else:
        logger.error(
            "[DATE_PARSE_ERROR] Unsupported created_at type=%s value=%r",
            type(value).__name__,
            value,
        )
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)

    return dt.astimezone(KYIV_TZ)


def _is_real_activity(action_name: str) -> bool:
    return str(action_name or "").strip() in REAL_ACTIVITY_ACTIONS


def _is_training_activity(action_name: str) -> bool:
    return str(action_name or "").strip() in TRAINING_ACTIVITY_ACTIONS


def _is_training_rollback_activity(action_name: str) -> bool:
    return str(action_name or "").strip() in TRAINING_ROLLBACK_ACTIONS


def _get_current_week_period() -> tuple[str, str]:
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


def _get_current_week_period_dt() -> tuple[datetime, datetime]:
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

    return week_start, week_end


def _get_last_finished_week_period() -> tuple[str, str]:
    now = get_kyiv_now()
    current_sunday_20 = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=20,
        minute=0,
        second=0,
        microsecond=0,
    )

    if now >= current_sunday_20:
        week_end = current_sunday_20
    else:
        week_end = current_sunday_20 - timedelta(days=7)

    week_start = week_end - timedelta(days=7)

    return week_start.isoformat(), week_end.isoformat()


async def _get_supabase_user_row(user_id: int) -> Optional[Dict[str, Any]]:
    try:
        user_row = await get_user_by_telegram_id(user_id)
        if user_row:
            await _cache_supabase_user_row(user_id, user_row)
        return user_row
    except Exception as e:
        logger.error(f"[DB] failed to load Supabase user: user_id={user_id}, error={e}")
        return None


async def _cache_supabase_user_row(user_id: int, user_row: Dict[str, Any]) -> None:
    supabase_user_id = str(user_row.get("id") or "").strip()
    if not supabase_user_id:
        return

    await set_flag(KeyManager.get_reg_key(user_id), ex=USER_CACHE_TTL)
    await set_data(KeyManager.get_user_uuid_key(user_id), supabase_user_id, ex=USER_CACHE_TTL)


async def get_cached_supabase_user_id(user_id: int) -> Optional[str]:
    cached_uuid = await get_data(KeyManager.get_user_uuid_key(user_id))
    if isinstance(cached_uuid, str) and cached_uuid.strip():
        return cached_uuid.strip()

    user_row = await _get_supabase_user_row(user_id)
    if not user_row:
        return None

    supabase_user_id = str(user_row.get("id") or "").strip()
    return supabase_user_id or None


def get_kyiv_day_bounds_utc_strings(target_date: Optional[date] = None) -> tuple[str, str]:
    now = get_kyiv_now()
    day = target_date or now.date()

    day_start_kyiv = KYIV_TZ.localize(
        datetime.combine(day, datetime.min.time())
    )
    day_end_kyiv = day_start_kyiv + timedelta(days=1)

    return (
        day_start_kyiv.astimezone(pytz.UTC).isoformat(),
        day_end_kyiv.astimezone(pytz.UTC).isoformat(),
    )


def get_kyiv_month_bounds_utc_strings(year: int, month: int) -> tuple[str, str]:
    month_start_kyiv = KYIV_TZ.localize(datetime(year, month, 1))

    if month == 12:
        next_month_start_kyiv = KYIV_TZ.localize(datetime(year + 1, 1, 1))
    else:
        next_month_start_kyiv = KYIV_TZ.localize(datetime(year, month + 1, 1))

    return (
        month_start_kyiv.astimezone(pytz.UTC).isoformat(),
        next_month_start_kyiv.astimezone(pytz.UTC).isoformat(),
    )


async def get_user_calendar_month(user_id: str, year: int, month: int) -> Dict[str, List[Dict[str, Any]]]:
    """All activities per Kyiv calendar day: {"YYYY-MM-DD": [{"action_name", "hp_change"}, ...]}."""
    period_from, period_to = get_kyiv_month_bounds_utc_strings(year, month)
    activities = await get_user_activities_in_period(user_id, period_from, period_to, limit=1000)

    days: Dict[str, List[Dict[str, Any]]] = {}

    for activity in activities:
        action_name = str(activity.get("action_name", "")).strip()
        created_at = _parse_activity_created_at(activity.get("created_at"))

        if not action_name or not created_at:
            continue

        day_key = created_at.astimezone(KYIV_TZ).strftime("%Y-%m-%d")
        days.setdefault(day_key, []).append({
            "action_name": action_name,
            "hp_change": activity.get("hp_change", 0),
        })

    return days


def _calculate_training_streak(activities: List[Dict[str, Any]]) -> int:
    """
    Weekly TurboTeam streak.

    Counts only valid Gym/Street trainings inside current Turbo-week:
    Sunday 20:00 Kyiv -> next Sunday 20:00 Kyiv.

    Rollback logic:
    - Gym Rollback cancels one Gym.
    - Street Rollback cancels one Street.
    - Returned / Welcome Bonus / Rest / Skipped do not count as training streak.
    - Max visible streak is 14.
    """
    week_start, week_end = _get_current_week_period_dt()

    training_count = 0
    rollback_count = 0

    for activity in activities:
        action_name = str(activity.get("action_name", "")).strip()
        created_at = _parse_activity_created_at(activity.get("created_at"))

        if not created_at:
            continue

        if created_at < week_start or created_at >= week_end:
            continue

        if _is_training_activity(action_name):
            training_count += 1
        elif _is_training_rollback_activity(action_name):
            rollback_count += 1

    valid_training_count = max(0, training_count - rollback_count)
    return min(valid_training_count, WEEKLY_STREAK_MAX)


def _get_last_real_activity_date(activities: List[Dict[str, Any]]) -> Optional[datetime.date]:
    last_activity_date = None

    for activity in activities:
        action_name = str(activity.get("action_name", "")).strip()

        if not _is_real_activity(action_name):
            continue

        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue

        activity_date = created_at.date()
        if last_activity_date is None or activity_date > last_activity_date:
            last_activity_date = activity_date

    return last_activity_date


def _get_last_activity_date_from_row(activity: Optional[Dict[str, Any]]) -> Optional[datetime.date]:
    if not activity:
        return None

    created_at = _parse_activity_created_at(activity.get("created_at"))
    if not created_at:
        return None

    return created_at.date()


def _should_skip_due_to_bad_activity_date(
    context: str,
    telegram_user_id: Any,
    user_uuid: Any,
    nickname: str,
    last_activity_row: Optional[Dict[str, Any]],
    last_activity_date: Optional[datetime.date],
) -> bool:
    """
    Safety guard.

    If Supabase returned an activity row but date parsing failed, we must NOT treat
    the user as inactive. Otherwise a valid active user can be warned/removed.
    """
    if last_activity_row is not None and last_activity_date is None:
        logger.error(
            "[%s] SAFETY_SKIP_BAD_DATE user_id=%s uuid=%s nickname=%s "
            "last_action=%s last_created_at=%s",
            context,
            telegram_user_id,
            user_uuid,
            nickname,
            last_activity_row.get("action_name"),
            last_activity_row.get("created_at"),
        )
        return True

    return False


async def _get_last_real_activity_row(user_uuid: str) -> Optional[Dict[str, Any]]:
    try:
        return await get_last_user_activity_by_actions(
            user_id=str(user_uuid),
            actions=list(REAL_ACTIVITY_ACTIONS),
        )
    except Exception as e:
        logger.error(
            "[DB] failed to get last real activity row: user_uuid=%s error=%s",
            user_uuid,
            e,
            exc_info=True,
        )
        return None


async def _get_last_real_activity_date_direct(user_uuid: str) -> Optional[datetime.date]:
    activity = await _get_last_real_activity_row(user_uuid)
    return _get_last_activity_date_from_row(activity)


async def _has_activity_today(
    user_id: int,
    action_name: str,
    video_id: str = "",
    supabase_user_id: Optional[str] = None,
) -> bool:
    supabase_user_id = supabase_user_id or await get_cached_supabase_user_id(user_id)
    if not supabase_user_id:
        logger.warning(f"[DB] Supabase user not found: telegram_user_id={user_id}")
        return False

    try:
        day_start_utc, day_end_utc = get_kyiv_day_bounds_utc_strings()
        activities = await get_user_activities_by_actions_in_period(
            user_id=str(supabase_user_id),
            actions=[action_name, f"{action_name} Rollback"],
            created_at_from=day_start_utc,
            created_at_to=day_end_utc,
            limit=1000,
        )
    except Exception as e:
        logger.error(f"[DB] failed to read activities: user_id={user_id}, error={e}")
        return False

    normalized_action = str(action_name).strip()
    normalized_video_id = str(video_id or "").strip()

    action_count = 0
    rollback_count = 0

    for activity in activities:
        current_action_name = str(activity.get("action_name", "")).strip()
        existing_video_id = str(activity.get("video_id") or "").strip()

        if current_action_name == normalized_action:
            if normalized_video_id:
                if existing_video_id == normalized_video_id:
                    action_count += 1
            else:
                action_count += 1

        elif current_action_name == f"{normalized_action} Rollback":
            rollback_count += 1

    return action_count > rollback_count


API_SEMAPHORE = asyncio.Semaphore(20)

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


async def get_session() -> aiohttp.ClientSession:
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


async def _request(payload: dict, method: str = "POST") -> Any:
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
    real_activities_count = 0

    for activity in activities:
        action_name = str(activity.get("action_name", "")).strip()

        if _is_real_activity(action_name):
            real_activities_count += 1

        try:
            hp_total += int(activity.get("hp_change", 0) or 0)
        except Exception:
            continue

    streak = _calculate_training_streak(activities)

    stats = {
        "user_id": str(supabase_user_id),
        "telegram_user_id": user_row.get("telegram_user_id"),
        "nickname": user_row.get("nickname", ""),
        "hp": hp_total,
        "hp_total": hp_total,
        "activities_count": real_activities_count,
        "streak": streak,
        "weekly_streak": streak,
        "weekly_streak_max": WEEKLY_STREAK_MAX,
    }

    return stats


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
            supabase_user_id = await get_cached_supabase_user_id(user_id)

            if is_check:
                already_exists = await _has_activity_today(
                    user_id,
                    action_name,
                    video_id,
                    supabase_user_id=supabase_user_id,
                )
                return not already_exists

            already_exists = await _has_activity_today(
                user_id,
                action_name,
                video_id,
                supabase_user_id=supabase_user_id,
            )
            if already_exists:
                return "already_done"

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


async def check_user_exists(user_id: int) -> bool:
    cache_key = KeyManager.get_reg_key(user_id)

    cached = await get_data(cache_key)
    if cached is not None:
        return True

    user_uuid = await get_cached_supabase_user_id(user_id)
    exists = user_uuid is not None

    if exists:
        await set_flag(cache_key, ex=3600)

    return exists


async def register_user_from_quiz(user_id: int, nickname: str, quiz_data: dict) -> bool:
    try:
        existing_user = await _get_supabase_user_row(user_id)
        if existing_user:
            await _cache_supabase_user_row(user_id, existing_user)
            return True

        created_user = await create_user(
            telegram_user_id=user_id,
            nickname=str(nickname),
            gender=quiz_data.get("gender", "N/A"),
            level=quiz_data.get("level", "N/A"),
            goal=str(quiz_data.get("goal", "N/A"))[:200],
            weekly_plan=str(quiz_data.get("weekly_plan", "N/A"))[:100],
            training_place=str(quiz_data.get("training_place", "N/A"))[:100],
        )

        await _cache_supabase_user_row(user_id, created_user)
        return True

    except Exception as e:
        logger.warning(f"[DB] registration failed: user_id={user_id}, error={e}")
        return False


async def get_weekly_top_users(finished_week: bool = False):
    try:
        if finished_week:
            period_start, period_end = _get_last_finished_week_period()
        else:
            period_start, period_end = _get_current_week_period()

        logger.info(
            "[DB] weekly top period: start=%s end=%s finished_week=%s",
            period_start,
            period_end,
            finished_week,
        )

        ranking_rows = await get_weekly_rating(period_start, period_end)
        return ranking_rows[:10]

    except Exception as e:
        logger.error(f"[DB] failed to build weekly top users: {e}", exc_info=True)
        return []


# get_weekly_rating is a generic RPC over any [start, end) window — reusing it with
# a window wide enough to cover the project's whole lifetime gives an all-time rating
# without any new SQL.
ALL_TIME_RATING_START = "2000-01-01T00:00:00+00:00"
ALL_TIME_RATING_END = "2100-01-01T00:00:00+00:00"


async def get_all_time_rating() -> List[Dict[str, Any]]:
    """All users ranked by total HP since the start of the project."""
    try:
        ranking_rows = await get_weekly_rating(ALL_TIME_RATING_START, ALL_TIME_RATING_END)
        return ranking_rows
    except Exception as e:
        logger.error(f"[DB] failed to build all-time rating: {e}", exc_info=True)
        return []


# ==============================================================================
# ACTIVITY FEED ("Останнє в стрічці")
# ==============================================================================

REFERRAL_BONUS_ACTION_PREFIX = "Referral Bonus"

FEED_EVENT_META: dict[str, tuple[str, str]] = {
    "Gym": ("🏋️", "Тренування в залі"),
    "Street": ("🦾", "Тренування на вулиці"),
    "Rest": ("🧘", "Відпочинок"),
    "Skipped": ("🚫", "Пропуск дня"),
    "Welcome Bonus": ("🎁", "Вітальний бонус"),
    "Returned": ("🏎️", "Повернення в групу"),
    "Weekly Challenge": ("🔥", "Тижневий челендж"),
    "Referral Welcome Bonus": ("🎉", "Бонус за приєднання"),
}


def _format_days_uk_short(days: int) -> str:
    if days % 10 == 1 and days % 100 != 11:
        return "день"
    if 2 <= days % 10 <= 4 and not (12 <= days % 100 <= 14):
        return "дні"
    return "днів"


def _feed_relative_label(created_at: datetime) -> str:
    """Honest human-readable gap label — never invents recency that isn't there."""
    seconds = max(0.0, (get_kyiv_now() - created_at).total_seconds())

    if seconds < 3600:
        minutes = max(1, int(seconds // 60))
        return f"{minutes} хв тому"

    if seconds < 86400:
        hours = int(seconds // 3600)
        return "годину тому" if hours == 1 else f"{hours} год тому"

    days = int(seconds // 86400)
    if days == 1:
        return "учора"
    if days < 30:
        return f"{days} {_format_days_uk_short(days)} тому"
    if days < 365:
        months = max(1, days // 30)
        return f"{months} міс тому"

    return created_at.strftime("%d.%m.%Y")


async def get_user_feed(user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Recent activity feed for the profile webapp: real trainings/rest/skips/referrals
    merged with achievement unlocks, sorted newest-first.
    """
    from services import TRAINING_ACHIEVEMENT_ICONS  # deferred: services.py imports database.py

    activities = await get_user_activities(user_id, limit=200)
    achievements = await get_user_achievements(user_id, limit=50)

    entries: List[Dict[str, Any]] = []

    for activity in activities:
        action_name = str(activity.get("action_name") or "").strip()
        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue

        if action_name in FEED_EVENT_META:
            icon, title = FEED_EVENT_META[action_name]
        elif action_name.startswith(REFERRAL_BONUS_ACTION_PREFIX):
            icon, title = "🚀", "Запросив друга"
        else:
            continue

        try:
            hp_change = int(activity.get("hp_change") or 0)
        except Exception:
            hp_change = 0

        hp_label = f"{hp_change:+d} HP" if hp_change else ""
        subtitle = " · ".join(filter(None, [hp_label, _feed_relative_label(created_at)]))

        entries.append({
            "type": "activity",
            "icon": icon,
            "title": title,
            "subtitle": subtitle,
            "created_at": created_at.isoformat(),
        })

    for achievement in achievements:
        created_at = _parse_activity_created_at(achievement.get("created_at"))
        if not created_at:
            continue

        achievement_code = str(achievement.get("achievement_code") or "").strip()
        achievement_title = str(achievement.get("achievement_title") or "Досягнення").strip()
        icon = TRAINING_ACHIEVEMENT_ICONS.get(achievement_code, "🏅")

        entries.append({
            "type": "achievement",
            "icon": icon,
            "title": f"Розблоковано «{achievement_title}»",
            "subtitle": _feed_relative_label(created_at),
            "created_at": created_at.isoformat(),
        })

    entries.sort(key=lambda item: item["created_at"], reverse=True)
    return entries[:limit]


async def reset_weekly_stats() -> bool:
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

            if await _is_auto_removed_user(int(telegram_user_id)):
                logger.info(
                    "[INACTIVE] skipped auto-removed user_id=%s nickname=%s",
                    telegram_user_id,
                    nickname,
                )
                continue

            last_activity_row = await _get_last_real_activity_row(str(user_uuid))
            last_activity_date = _get_last_activity_date_from_row(last_activity_row)

            if _should_skip_due_to_bad_activity_date(
                "INACTIVE",
                telegram_user_id,
                user_uuid,
                nickname,
                last_activity_row,
                last_activity_date,
            ):
                continue

            if last_activity_date is None:
                silent_days = INACTIVE_DAYS_THRESHOLD
            else:
                silent_days = (today - last_activity_date).days

            if silent_days != INACTIVE_DAYS_THRESHOLD:
                continue

            logger.info(
                "[INACTIVE] user_id=%s uuid=%s nickname=%s last_action=%s last_created_at=%s last_activity_date=%s today=%s silent_days=%s",
                telegram_user_id,
                user_uuid,
                nickname,
                last_activity_row.get("action_name") if last_activity_row else None,
                last_activity_row.get("created_at") if last_activity_row else None,
                last_activity_date,
                today,
                silent_days,
            )

            display_name = escape(nickname)
            inactive_users.append(
                f'<a href="tg://user?id={telegram_user_id}">{display_name}</a>'
            )

        return inactive_users

    except Exception as e:
        logger.error(f"[DB] failed to get inactive users: {e}", exc_info=True)
        return []


async def get_users_for_last_warning() -> List[Dict[str, Any]]:
    try:
        users = await get_all_users()
        today = get_kyiv_now().date()
        warning_users: List[Dict[str, Any]] = []

        for user in users:
            telegram_user_id = user.get("telegram_user_id")
            user_uuid = user.get("id")
            nickname = str(user.get("nickname") or telegram_user_id or "Учасник").strip()

            if not telegram_user_id or not user_uuid:
                continue

            if await _is_auto_removed_user(int(telegram_user_id)):
                logger.info(
                    "[LAST_WARNING] skipped auto-removed user_id=%s nickname=%s",
                    telegram_user_id,
                    nickname,
                )
                continue

            last_activity_row = await _get_last_real_activity_row(str(user_uuid))
            last_activity_date = _get_last_activity_date_from_row(last_activity_row)

            if _should_skip_due_to_bad_activity_date(
                "LAST_WARNING",
                telegram_user_id,
                user_uuid,
                nickname,
                last_activity_row,
                last_activity_date,
            ):
                continue

            if last_activity_date is None:
                silent_days = LAST_WARNING_DAYS_THRESHOLD
            else:
                silent_days = (today - last_activity_date).days

            if silent_days != LAST_WARNING_DAYS_THRESHOLD:
                continue

            logger.info(
                "[LAST_WARNING_CANDIDATE] user_id=%s uuid=%s nickname=%s last_action=%s last_created_at=%s last_activity_date=%s today=%s silent_days=%s",
                telegram_user_id,
                user_uuid,
                nickname,
                last_activity_row.get("action_name") if last_activity_row else None,
                last_activity_row.get("created_at") if last_activity_row else None,
                last_activity_date,
                today,
                silent_days,
            )

            display_name = escape(nickname)
            warning_users.append(
                {
                    "telegram_user_id": int(telegram_user_id),
                    "user_uuid": str(user_uuid),
                    "nickname": nickname,
                    "mention_html": f'<a href="tg://user?id={telegram_user_id}">{display_name}</a>',
                    "silent_days": int(silent_days),
                }
            )

        return warning_users

    except Exception as e:
        logger.error(f"[DB] failed to get users for last warning: {e}", exc_info=True)
        return []


async def get_users_for_auto_removal() -> List[Dict[str, Any]]:
    try:
        users = await get_all_users()
        today = get_kyiv_now().date()
        removable_users: List[Dict[str, Any]] = []

        for user in users:
            telegram_user_id = user.get("telegram_user_id")
            user_uuid = user.get("id")
            nickname = str(user.get("nickname") or telegram_user_id or "Учасник").strip()

            if not telegram_user_id or not user_uuid:
                continue

            if await _is_auto_removed_user(int(telegram_user_id)):
                logger.info(
                    "[AUTO_REMOVE] skipped already auto-removed user_id=%s nickname=%s",
                    telegram_user_id,
                    nickname,
                )
                continue

            last_activity_row = await _get_last_real_activity_row(str(user_uuid))
            last_activity_date = _get_last_activity_date_from_row(last_activity_row)

            if _should_skip_due_to_bad_activity_date(
                "AUTO_REMOVE",
                telegram_user_id,
                user_uuid,
                nickname,
                last_activity_row,
                last_activity_date,
            ):
                continue

            if last_activity_date is None:
                silent_days = AUTO_REMOVE_DAYS_THRESHOLD
            else:
                silent_days = (today - last_activity_date).days

            if silent_days < AUTO_REMOVE_DAYS_THRESHOLD:
                continue

            logger.info(
                "[AUTO_REMOVE_CANDIDATE] user_id=%s uuid=%s nickname=%s last_action=%s last_created_at=%s last_activity_date=%s today=%s silent_days=%s",
                telegram_user_id,
                user_uuid,
                nickname,
                last_activity_row.get("action_name") if last_activity_row else None,
                last_activity_row.get("created_at") if last_activity_row else None,
                last_activity_date,
                today,
                silent_days,
            )

            display_name = escape(nickname)
            removable_users.append(
                {
                    "telegram_user_id": int(telegram_user_id),
                    "user_uuid": str(user_uuid),
                    "nickname": nickname,
                    "mention_html": f'<a href="tg://user?id={telegram_user_id}">{display_name}</a>',
                    "silent_days": int(silent_days),
                }
            )

        return removable_users

    except Exception as e:
        logger.error(f"[DB] failed to get users for auto removal: {e}", exc_info=True)
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
