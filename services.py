import logging
import random
import asyncio
import functools
from typing import Any, Callable, Optional
from datetime import datetime, timedelta
from html import escape
import pytz

from aiogram import types
from aiogram.types import Message

from config import RANDOM_HP_RANGE, HP_GYM, HP_STREET, HP_REST, HP_SKIP, REPORTS_GROUP_ID
from cache import KeyManager, acquire_lock, get_data, redis_client, set_data
from database import (
    get_kyiv_now,
    add_activity,
    check_activity_limit,
    update_user_activity,
    get_cached_supabase_user_id,
    get_kyiv_day_bounds_utc_strings,
    get_seconds_until_kyiv_midnight,
    get_user_streak_multiplier,
)
from phrases import get_phrase
from config import GROUP_LINK
from reports import build_report_keyboard
from supabase_db import (
    get_user_by_telegram_id,
    get_user_activities,
    get_user_activities_by_actions_in_period,
    has_user_achievement,
    add_user_achievement,
)

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

REPORT_META_TTL = 172800  # 48 hours

# ==============================================================================
# STREAK PARAMETERS
# ==============================================================================

WEEKLY_STREAK_MAX = 14

STREAK_BONUS_MILESTONES: dict[int, int] = {
    3: 50,
    7: 75,
    14: 125,
}

STREAK_BONUS_REDIS_PREFIX = "turbo:streak_bonus"

# ==============================================================================
# TRAINING STATUS LEVELS
# ==============================================================================

TRAINING_STATUS_LEVELS = [
    (1, "Новачок"),
    (5, "Вкатався"),
    (10, "Боєць"),
    (25, "Стабільний"),
    (50, "Мотор"),
    (100, "Турбо"),
    (200, "Машина"),
    (350, "Термінатор"),
    (500, "Монстр"),
    (1000, "Легенда TurboTeam"),
]

# ==============================================================================
# ACHIEVEMENTS (TRAINING ONLY, V2 — 15 thresholds)
# ==============================================================================

TRAINING_ACHIEVEMENTS = [
    (1, "training_1", "Перший крок"),
    (3, "training_3", "Втягнувся"),
    (5, "training_5", "Розігрів"),
    (10, "training_10", "Перша десятка"),
    (15, "training_15", "Стабільність"),
    (25, "training_25", "У ритмі"),
    (50, "training_50", "Півсотні"),
    (75, "training_75", "На повну"),
    (100, "training_100", "Сотка"),
    (150, "training_150", "Залізна дисципліна"),
    (200, "training_200", "Машина"),
    (300, "training_300", "Турбо-режим"),
    (500, "training_500", "Монстр"),
    (750, "training_750", "Невгамовний"),
    (1000, "training_1000", "Легенда TurboTeam"),
]

TRAINING_ACHIEVEMENT_ICONS: dict[str, str] = {
    "training_1": "🐣",
    "training_3": "🔥",
    "training_5": "💪",
    "training_10": "🎯",
    "training_15": "🧱",
    "training_25": "🌀",
    "training_50": "⚡",
    "training_75": "🚀",
    "training_100": "💯",
    "training_150": "🛡️",
    "training_200": "🤖",
    "training_300": "🏎️",
    "training_500": "👹",
    "training_750": "🦾",
    "training_1000": "👑",
}


# ==============================================================================
# QUIZ VALIDATION
# ==============================================================================

def validate_quiz(data: dict) -> bool:
    """
    Validates quiz data from WebApp.
    Accepts any non-empty strings for all 5 quiz fields.
    """
    try:
        logger.debug(f"[VALIDATE] Quiz data: {data}")

        gender = data.get("gender")
        if not isinstance(gender, str) or len(gender.strip()) == 0:
            logger.warning(f"[VALIDATE] Invalid gender: {gender!r}")
            return False

        level = data.get("level")
        if not isinstance(level, str) or len(level.strip()) == 0:
            logger.warning(f"[VALIDATE] Invalid level: {level!r}")
            return False

        goal = data.get("goal")
        if not isinstance(goal, str) or not (0 < len(goal.strip()) < 200):
            logger.warning(f"[VALIDATE] Invalid goal: {goal!r}")
            return False

        weekly_plan = data.get("weekly_plan")
        if not isinstance(weekly_plan, str) or len(weekly_plan.strip()) == 0:
            logger.warning(f"[VALIDATE] Invalid weekly_plan: {weekly_plan!r}")
            return False

        training_place = data.get("training_place")
        if not isinstance(training_place, str) or len(training_place.strip()) == 0:
            logger.warning(f"[VALIDATE] Invalid training_place: {training_place!r}")
            return False

        return True

    except Exception as e:
        logger.error(f"[VALIDATE] Critical validation error: {e}", exc_info=True)
        return False


# ==============================================================================
# DECORATORS
# ==============================================================================

def handle_exceptions(default_return: Any = None):
    """
    Catches exceptions, logs traceback, returns default_return.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"[SERVICE] Error in {func.__name__}: {e}",
                    exc_info=True,
                )
                return default_return

        return wrapper

    return decorator


# ==============================================================================
# UTILITIES
# ==============================================================================

def safe_create_task(coro, name: str = "task") -> asyncio.Task:
    """
    Creates asyncio.Task with automatic exception logging.
    """
    task = asyncio.create_task(coro, name=name)

    @functools.wraps(coro.__class__.__call__)
    def _callback(t: asyncio.Task):
        try:
            exc = t.exception()
            if exc:
                logger.error(
                    f"[TASK] Task {name!r} failed with error: {exc}",
                    exc_info=exc,
                )
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

    task.add_done_callback(_callback)
    return task


async def auto_delete(message: Any, delay: int = 5) -> None:
    """
    Deletes message after delay seconds.
    """
    await asyncio.sleep(delay)

    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"[AUTO_DELETE] Failed to delete message: {e}")


def _get_current_turbo_week_period() -> tuple[datetime, datetime]:
    """
    Current TurboTeam week.
    Same logic as rating:
    Sunday 20:00 Kyiv -> next Sunday 20:00 Kyiv.
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
    return week_start, week_end


def _get_streak_bonus_key(user_id: int, week_start: datetime, streak: int) -> str:
    week_key = week_start.strftime("%Y-%m-%d_%H-%M")
    return f"{STREAK_BONUS_REDIS_PREFIX}:{user_id}:{week_key}:{streak}"


def _get_seconds_until_datetime(target_dt: datetime) -> int:
    now = get_kyiv_now()

    if target_dt.tzinfo is None:
        target_dt = KYIV_TZ.localize(target_dt)
    else:
        target_dt = target_dt.astimezone(KYIV_TZ)

    return max(1, int((target_dt - now).total_seconds()))


def _format_days_uk(days: int) -> str:
    if days % 10 == 1 and days % 100 != 11:
        return "день"

    if 2 <= days % 10 <= 4 and not (12 <= days % 100 <= 14):
        return "дні"

    return "днів"


def _build_progress_bar(current: int, target: int, width: int = 10) -> str:
    """
    Builds text progress bar.

    Example:
    current=1 target=5 width=10 -> ██░░░░░░░░
    """
    try:
        current = max(0, int(current or 0))
        target = max(1, int(target or 1))
        width = max(5, int(width or 10))

        ratio = min(current, target) / target
        filled = int(ratio * width)

        if current > 0 and filled == 0:
            filled = 1

        filled = min(width, max(0, filled))
        empty = width - filled

        return "█" * filled + "░" * empty
    except Exception:
        try:
            return "░" * max(5, int(width or 10))
        except Exception:
            return "░░░░░░░░░░"


def _get_next_training_rank_progress(training_count: int) -> dict[str, Any]:
    """
    Returns progress data to next rank.

    Rank bar is calculated from 0 to next threshold.
    Example:
    1/5 -> progress_current=1, progress_target=5.
    """
    training_count = max(0, int(training_count or 0))

    current_threshold = 0
    current_title = "Без статусу"

    for threshold, title in TRAINING_STATUS_LEVELS:
        if training_count >= threshold:
            current_threshold = threshold
            current_title = title
            continue

        return {
            "current_title": current_title,
            "current_threshold": current_threshold,
            "next_title": title,
            "next_threshold": threshold,
            "progress_current": training_count,
            "progress_target": threshold,
            "left": max(0, threshold - training_count),
            "is_max": False,
        }

    return {
        "current_title": current_title,
        "current_threshold": current_threshold,
        "next_title": None,
        "next_threshold": None,
        "progress_current": training_count,
        "progress_target": training_count,
        "left": 0,
        "is_max": True,
    }


def _get_next_streak_bonus_progress(streak_days: int) -> dict[str, Any]:
    """
    Returns next streak bonus milestone data.
    """
    streak_days = max(0, int(streak_days or 0))

    for milestone in sorted(STREAK_BONUS_MILESTONES):
        if streak_days < milestone:
            return {
                "next_milestone": milestone,
                "bonus": int(STREAK_BONUS_MILESTONES[milestone]),
                "left": milestone - streak_days,
                "is_max": False,
            }

    return {
        "next_milestone": WEEKLY_STREAK_MAX,
        "bonus": int(STREAK_BONUS_MILESTONES.get(WEEKLY_STREAK_MAX, 0)),
        "left": 0,
        "is_max": True,
    }


def _get_next_training_achievement_progress(training_count: int) -> dict[str, Any]:
    """
    Returns next training achievement data.
    """
    training_count = max(0, int(training_count or 0))

    for threshold, achievement_code, achievement_title in TRAINING_ACHIEVEMENTS:
        if training_count < threshold:
            return {
                "threshold": threshold,
                "achievement_code": achievement_code,
                "achievement_title": achievement_title,
                "left": threshold - training_count,
                "is_max": False,
            }

    last_threshold, last_code, last_title = TRAINING_ACHIEVEMENTS[-1]
    return {
        "threshold": last_threshold,
        "achievement_code": last_code,
        "achievement_title": last_title,
        "left": 0,
        "is_max": True,
    }


def _build_training_progress_report_block(
    training_count: int,
    current_status_title: str,
    streak_days: int,
) -> str:
    """
    Builds beautiful progress block for group training report.
    """
    rank_progress = _get_next_training_rank_progress(training_count)
    streak_progress = _get_next_streak_bonus_progress(streak_days)
    achievement_progress = _get_next_training_achievement_progress(training_count)

    training_count = max(0, int(training_count or 0))
    streak_days = max(0, min(int(streak_days or 0), WEEKLY_STREAK_MAX))
    streak_bar = _build_progress_bar(streak_days, WEEKLY_STREAK_MAX, width=14)

    lines = [
        "",
        f"🎖️ Рівень: <b>{escape(str(current_status_title))}</b>",
        f"📊 Тренувань: <b>{training_count}</b>",
        "",
    ]

    if rank_progress["is_max"]:
        lines.extend(
            [
                "📈 <b>Ранг прокачано на максимум</b>",
                "██████████ MAX",
                "Ти вже в легендарній зоні 🏎️🔥",
                "",
            ]
        )
    else:
        rank_bar = _build_progress_bar(
            rank_progress["progress_current"],
            rank_progress["progress_target"],
            width=10,
        )
        lines.extend(
            [
                f"📈 <b>До рангу “{escape(str(rank_progress['next_title']))}”</b>",
                f"{rank_bar} {training_count}/{rank_progress['next_threshold']}",
                f"Ще <b>{rank_progress['left']}</b> тренувань 🔥",
                "",
            ]
        )

    lines.extend(
        [
            "🔥 <b>Streak</b>",
            f"{streak_bar} {streak_days}/{WEEKLY_STREAK_MAX}",
        ]
    )

    if streak_progress["is_max"]:
        lines.append("Максимальна streak-нагорода цього тижня взята 🏆")
    else:
        lines.append(
            f"До бонусу <b>+{streak_progress['bonus']} HP</b>: "
            f"ще <b>{streak_progress['left']}</b>"
        )

    if achievement_progress["is_max"]:
        lines.extend(
            [
                "",
                "🎯 Наступна ціль: легендарний режим тримається 😎",
            ]
        )
    else:
        lines.extend(
            [
                "",
                f"🎯 Наступна ціль: <b>{achievement_progress['threshold']}</b> тренувань",
                f"До “{escape(str(achievement_progress['achievement_title']))}” ще "
                f"<b>{achievement_progress['left']}</b>",
            ]
        )

    return "\n".join(lines)


# ==============================================================================
# ACTIVITY SERVICE
# ==============================================================================

class ActivityService:
    """
    User activity service.
    """

    ACTION_HP_MAPPING: dict[str, int] = {
        "Rest": int(HP_REST),
        "Skipped": int(HP_SKIP),
        "Відпочинок": int(HP_REST),
        "Забив болт": int(HP_SKIP),
    }

    @staticmethod
    async def get_training_count(user_id: int) -> int:
        """
        Counts user's real training activities: Gym + Street.
        Ignores rollback rows.
        Cached in Redis for 5 minutes to reduce Supabase load.
        """
        cache_key = f"training_count:{user_id}"

        if redis_client is not None:
            try:
                cached = await redis_client.get(cache_key)
                if cached is not None:
                    return int(cached)
            except Exception as e:
                logger.warning("[get_training_count] Redis get error: %s", e)

        user_row = await get_user_by_telegram_id(user_id)
        if not user_row:
            return 0

        user_uuid = user_row.get("id")
        if not user_uuid:
            return 0

        activities = await get_user_activities(str(user_uuid), limit=5000)

        training_count = 0
        rollback_count = 0

        for activity in activities:
            action_name = str(activity.get("action_name", "")).strip()

            if action_name in {"Gym", "Street"}:
                training_count += 1
            elif action_name in {"Gym Rollback", "Street Rollback"}:
                rollback_count += 1

        result = max(0, training_count - rollback_count)

        if redis_client is not None:
            try:
                await redis_client.set(cache_key, result, ex=300)
            except Exception as e:
                logger.warning("[get_training_count] Redis set error: %s", e)

        return result

    @staticmethod
    async def invalidate_training_count_cache(user_id: int) -> None:
        """
        Clears cached training_count after a new training or rollback.
        """
        if redis_client is None:
            return

        try:
            await redis_client.delete(f"training_count:{user_id}")
        except Exception as e:
            logger.warning("[invalidate_training_count_cache] %s", e)

    @staticmethod
    async def refresh_streak_after_training(
        user_id: int,
        fallback_streak: int = 0,
        attempts: int = 3,
        delay_seconds: float = 0.4,
    ) -> int:
        """
        Reads fresh weekly streak after a training write.

        Supabase can occasionally return stale activity data immediately after insert.
        This helper retries shortly before building the public report.
        """
        best_streak = max(0, int(fallback_streak or 0))

        for attempt in range(1, attempts + 1):
            try:
                from database import get_user_stats

                stats = await get_user_stats(user_id)
                if stats:
                    fresh_streak = int(stats.get("streak", 0) or 0)
                    fresh_streak = min(max(0, fresh_streak), WEEKLY_STREAK_MAX)

                    if fresh_streak > best_streak:
                        best_streak = fresh_streak

                    if fresh_streak > 0:
                        logger.info(
                            "[STREAK] Fresh streak loaded: uid=%s streak=%s attempt=%s",
                            user_id,
                            fresh_streak,
                            attempt,
                        )
                        return fresh_streak

                logger.info(
                    "[STREAK] Fresh streak not ready: uid=%s attempt=%s fallback=%s",
                    user_id,
                    attempt,
                    best_streak,
                )

            except Exception as e:
                logger.warning(
                    "[STREAK] Failed to refresh streak: uid=%s attempt=%s error=%s",
                    user_id,
                    attempt,
                    e,
                )

            if attempt < attempts:
                await asyncio.sleep(delay_seconds)

        return best_streak

    @staticmethod
    def get_current_training_status(training_count: int) -> str:
        """
        Returns current status title for any training count.
        """
        status = "Без статусу"

        for threshold, title in TRAINING_STATUS_LEVELS:
            if training_count >= threshold:
                status = title
            else:
                break

        return status

    @staticmethod
    def get_new_training_status_by_exact_count(training_count: int) -> Optional[str]:
        """
        Returns new status title only when training_count exactly matches a threshold.
        """
        for threshold, title in TRAINING_STATUS_LEVELS:
            if training_count == threshold:
                return title

        return None

    @staticmethod
    async def maybe_grant_training_achievement(user_id: int) -> Optional[str]:
        """
        Grants a training achievement if the user has reached a new training milestone.
        Returns achievement title if granted, otherwise None.
        Uses cached training_count to avoid duplicate Supabase queries.
        """
        user_row = await get_user_by_telegram_id(user_id)
        if not user_row:
            return None

        user_uuid = user_row.get("id")
        if not user_uuid:
            return None

        training_count = await ActivityService.get_training_count(user_id)

        granted_title: Optional[str] = None

        for threshold, achievement_code, achievement_title in TRAINING_ACHIEVEMENTS:
            if training_count < threshold:
                continue

            already_has = await has_user_achievement(str(user_uuid), achievement_code)
            if already_has:
                continue

            await add_user_achievement(
                user_id=str(user_uuid),
                achievement_code=achievement_code,
                achievement_title=achievement_title,
            )
            granted_title = achievement_title

        return granted_title

    @staticmethod
    async def _has_non_rollback_activity_today_in_db(user_id: int, action_name: str) -> bool:
        user_uuid = await get_cached_supabase_user_id(user_id)
        if not user_uuid:
            return False

        day_start_utc, day_end_utc = get_kyiv_day_bounds_utc_strings()
        activities = await get_user_activities_by_actions_in_period(
            user_id=str(user_uuid),
            actions=[action_name, f"{action_name} Rollback"],
            created_at_from=day_start_utc,
            created_at_to=day_end_utc,
            limit=200,
        )

        action_count = 0
        rollback_count = 0

        for activity in activities:
            current_action_name = str(activity.get("action_name", "")).strip()

            if current_action_name == str(action_name):
                action_count += 1
            elif current_action_name == f"{action_name} Rollback":
                rollback_count += 1

        return action_count > rollback_count

    @staticmethod
    @handle_exceptions(default_return=False)
    async def can_user_log_activity(user_id: int, action_type: str) -> bool:
        """
        Checks whether user can log activity today.
        """
        today = get_kyiv_now().strftime("%Y-%m-%d")
        lock_key = KeyManager.get_action_lock_key(user_id, f"{action_type}:{today}")

        if (await get_data(lock_key)) is not None:
            logger.info(
                f"[SERVICE] Cache hit: uid={user_id} already did {action_type} today"
            )
            return False

        result = await check_activity_limit(user_id, "system", action_type)
        return bool(result)

    @staticmethod
    @handle_exceptions(default_return=False)
    async def check_today_report(user_id: int, ignore_actions: Optional[list[str]] = None) -> bool:
        """
        Returns True if user already has a daily activity today.
        Ignores rollback rows.
        """
        ignore_set = {
            str(item).strip().lower()
            for item in (ignore_actions or [])
            if str(item).strip()
        }
        today = get_kyiv_now().strftime("%Y-%m-%d")

        daily_actions = [
            "Gym",
            "Street",
            "Rest",
            "Skipped",
        ]
        cache_actions = [
            action_name
            for action_name in daily_actions
            if action_name.strip().lower() not in ignore_set
        ]
        rollback_actions = [f"{action_name} Rollback" for action_name in cache_actions]

        if cache_actions:
            if redis_client is not None:
                try:
                    pipe = redis_client.pipeline()
                    for action_name in cache_actions:
                        pipe.get(KeyManager.get_action_lock_key(user_id, f"{action_name}:{today}"))
                    cached_locks = await pipe.execute()

                    for action_name, cached_lock in zip(cache_actions, cached_locks):
                        if cached_lock is not None:
                            logger.debug(
                                "[check_today_report] Redis-hit: uid=%s action=%s date=%s",
                                user_id,
                                action_name,
                                today,
                            )
                            return True
                except Exception as e:
                    logger.error(f"[check_today_report] Redis pipeline error: {e}")
            else:
                for action_name in cache_actions:
                    lock_key = KeyManager.get_action_lock_key(user_id, f"{action_name}:{today}")
                    if (await get_data(lock_key)) is not None:
                        logger.debug(
                            "[check_today_report] Redis-hit: uid=%s action=%s date=%s",
                            user_id,
                            action_name,
                            today,
                        )
                        return True

        if not cache_actions:
            return False

        user_uuid = await get_cached_supabase_user_id(user_id)
        if not user_uuid:
            return False

        day_start_utc, day_end_utc = get_kyiv_day_bounds_utc_strings()
        activities = await get_user_activities_by_actions_in_period(
            user_id=str(user_uuid),
            actions=cache_actions + rollback_actions,
            created_at_from=day_start_utc,
            created_at_to=day_end_utc,
            limit=1000,
        )

        counts = {action_name: 0 for action_name in cache_actions}
        rollback_counts = {action_name: 0 for action_name in cache_actions}

        for activity in activities:
            current_action_name = str(activity.get("action_name", "")).strip()
            if current_action_name in counts:
                counts[current_action_name] += 1
                continue

            for action_name in cache_actions:
                if current_action_name == f"{action_name} Rollback":
                    rollback_counts[action_name] += 1
                    break

        for action_name in cache_actions:
            if counts[action_name] > rollback_counts[action_name]:
                logger.debug(
                    "[check_today_report] DB-hit: uid=%s action=%s date=%s",
                    user_id,
                    action_name,
                    today,
                )
                return True

        return False

    @staticmethod
    @handle_exceptions(default_return=(0, 0))
    async def check_and_grant_streak_bonus(user_id: int, nickname: str) -> tuple[int, int]:
        """
        Checks weekly streak and grants bonus once per Turbo-week for 3/7/14 milestones.
        Returns (bonus, streak).
        """
        from database import get_user_stats

        stats = await get_user_stats(user_id)
        if not stats:
            return 0, 0

        streak = int(stats.get("streak", 0) or 0)
        streak = min(streak, WEEKLY_STREAK_MAX)

        bonus = int(STREAK_BONUS_MILESTONES.get(streak, 0) or 0)
        if bonus <= 0:
            return 0, streak

        week_start, week_end = _get_current_turbo_week_period()
        bonus_key = _get_streak_bonus_key(user_id, week_start, streak)

        already_granted = await get_data(bonus_key)
        if already_granted is not None:
            logger.info(
                "[STREAK] Bonus already granted this Turbo-week: uid=%s streak=%s",
                user_id,
                streak,
            )
            return 0, streak

        ex_seconds = _get_seconds_until_datetime(week_end)

        await set_data(
            bonus_key,
            {
                "user_id": user_id,
                "nickname": str(nickname),
                "streak": streak,
                "streak_max": WEEKLY_STREAK_MAX,
                "bonus": bonus,
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
            },
            ex=ex_seconds,
        )

        action_label = f"🔥 Streak Bonus ({streak}/{WEEKLY_STREAK_MAX})"
        await add_activity(user_id, nickname, action_label, bonus)

        logger.info(
            "[STREAK] Bonus +%s HP granted to %s for %s/%s. key=%s ex=%s",
            bonus,
            nickname,
            streak,
            WEEKLY_STREAK_MAX,
            bonus_key,
            ex_seconds,
        )

        return bonus, streak

    @staticmethod
    @handle_exceptions(default_return=(False, 0, 0))
    async def grant_hp(
        user_id: int,
        nickname: str,
        action_type: str,
        hp: int,
        video_id: str = "",
    ) -> tuple[bool, int, int]:
        """
        Grants HP to user with atomic Redis lock.
        Returns (success, streak_bonus, streak_days).
        Daily lock expires at Kyiv midnight, not after 24 hours.
        """
        today = get_kyiv_now().strftime("%Y-%m-%d")
        lock_key = KeyManager.get_action_lock_key(user_id, f"{action_type}:{today}")

        lock_acquired = await acquire_lock(
            lock_key,
            ex=ActivityService.get_seconds_until_kyiv_midnight(),
        )
        if not lock_acquired:
            logger.info(
                f"[SERVICE] Lock busy: uid={user_id} action={action_type} duplicate rejected"
            )
            return False, 0, 0

        result = await update_user_activity(
            user_id,
            nickname,
            action_type,
            hp,
            video_id,
            False,
            skip_lock=True,
        )

        if result == "already_done" or result is False:
            from cache import delete_data
            await delete_data(lock_key)
            logger.warning(
                f"[SERVICE] GAS rejected write uid={user_id}, lock removed"
            )
            return False, 0, 0

        streak_bonus = 0
        streak_days = 0

        if action_type in ["Gym", "Street"]:
            streak_bonus, streak_days = await ActivityService.check_and_grant_streak_bonus(
                user_id,
                nickname,
            )

        logger.info(f"[SERVICE] HP GRANTED: uid={user_id} +{hp} HP for {action_type}")
        return True, streak_bonus, streak_days

    @staticmethod
    def calculate_training_hp(action_type: str = "Gym") -> int:
        """
        Calculates HP for training: base + random bonus.
        """
        try:
            base = int(HP_GYM) if action_type == "Gym" else int(HP_STREET)
            bonus = random.randint(int(RANDOM_HP_RANGE[0]), int(RANDOM_HP_RANGE[1]))
            total = base + bonus
            logger.debug(
                f"[SERVICE] calculate_training_hp: action={action_type} "
                f"base={base} bonus={bonus} total={total}"
            )
            return total
        except Exception as e:
            logger.error(f"[SERVICE] calculate_training_hp error: {e}", exc_info=True)
            return int(HP_GYM)

    @staticmethod
    def get_action_hp(action_type: str) -> int:
        """
        Returns fixed HP for rest/skip actions.
        """
        for key, value in ActivityService.ACTION_HP_MAPPING.items():
            if key in action_type:
                return int(value)

        logger.warning(f"[SERVICE] Unknown action type: {action_type!r}, returning 0")
        return 0

    @staticmethod
    def get_kyiv_date_string() -> str:
        """
        Date in DD.MM.YYYY format for Google Sheets.
        """
        return get_kyiv_now().strftime("%d.%m.%Y")

    @staticmethod
    def get_seconds_until_kyiv_midnight() -> int:
        now = get_kyiv_now()
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        return max(1, int((next_midnight - now).total_seconds()))

    @staticmethod
    @handle_exceptions(default_return=False)
    async def process_training_full_cycle(message: Message, action_type: str) -> bool:
        """
        Full training orchestration:
        1. calculate HP with streak multiplier
        2. write activity
        3. publish report to group with complaint button
        4. save mapping for manual/admin rollback
        """
        user = message.from_user
        nickname = user.full_name
        base_hp = ActivityService.calculate_training_hp(action_type)
        video_id = message.video_note.file_id if message.video_note else ""

        streak_info = await get_user_streak_multiplier(user.id)
        multiplier = streak_info.get("multiplier", 1.0)
        hp = int(base_hp * multiplier)

        logger.info(
            f"[SERVICE] Streak multiplier applied: uid={user.id} "
            f"base_hp={base_hp} multiplier={multiplier}x final_hp={hp}"
        )

        granted, streak_bonus, streak_days = await ActivityService.grant_hp(
            user.id,
            nickname,
            action_type,
            hp,
            video_id=video_id,
        )
        if not granted:
            return False

        if action_type in ["Gym", "Street"]:
            await ActivityService.invalidate_training_count_cache(user.id)
            streak_days = await ActivityService.refresh_streak_after_training(
                user_id=user.id,
                fallback_streak=streak_days,
                attempts=3,
                delay_seconds=0.4,
            )

        back_to_group_kb = types.InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    types.InlineKeyboardButton(
                        text="😎 Повертайся в банду",
                        url=GROUP_LINK,
                    )
                ]
            ]
        )

        multiplier_text = f" 🔥 x{multiplier:g}" if multiplier > 1.0 else ""
        await message.answer(
            f"✅ {action_type} зафіксовано. +{hp} HP{multiplier_text}",
            reply_markup=back_to_group_kb,
        )

        if streak_bonus > 0:
            await message.answer(
                f"🔥 <b>STREAK BONUS!</b>\n"
                f"Серія: <b>{streak_days}/{WEEKLY_STREAK_MAX}</b>\n"
                f"+<b>{streak_bonus}</b> HP",
                parse_mode="HTML",
            )

            mention_text = (
                f"@{escape(str(user.username))}"
                if user.username
                else escape(str(user.full_name or user.first_name or "Учасник"))
            )

            await message.bot.send_message(
                REPORTS_GROUP_ID,
                (
                    f"🔥 <b>STREAK BONUS!</b>\n\n"
                    f"{mention_text} тримає серію: <b>{streak_days}/{WEEKLY_STREAK_MAX}</b>\n"
                    f"+<b>{streak_bonus}</b> HP за дисципліну.\n\n"
                    f"Оце вже не випадковість — це система 🏎️🔥"
                ),
                parse_mode="HTML",
            )

        achievement_title = await ActivityService.maybe_grant_training_achievement(user.id)
        if achievement_title:
            achievement_title_html = escape(str(achievement_title))
            await message.answer(
                f"🏅 <b>НОВЕ ДОСЯГНЕННЯ!</b>\n\n"
                f"<b>{achievement_title_html}</b>\n"
                f"Ти відкрив нову віху в TurboTeam 🔥",
                parse_mode="HTML",
            )

        training_count = 0
        current_status_title = "Без статусу"
        new_status_title = None

        if action_type in ["Gym", "Street"]:
            training_count = await ActivityService.get_training_count(user.id)
            current_status_title = ActivityService.get_current_training_status(training_count)
            new_status_title = ActivityService.get_new_training_status_by_exact_count(training_count)

        report_kb = build_report_keyboard(
            target_uid=user.id,
            action_type=action_type,
        )

        group_video_msg = None
        group_text_msg = None

        try:
            group_video_msg = await message.copy_to(REPORTS_GROUP_ID)
        except Exception as e:
            logger.warning("[SERVICE] Failed to copy video to group: %s", e)

        safe_display_name = user.username or user.first_name or user.full_name or "Учасник"
        report_nickname = f"@{safe_display_name}" if user.username else safe_display_name
        report_nickname_html = escape(str(report_nickname))

        hp_line = f"+{hp} HP"
        if multiplier > 1.0:
            hp_line += f" з множником х{multiplier:g} 😎"

        report_text = (
            f"{get_phrase('report', nickname=report_nickname_html)}\n"
            f"{hp_line}"
        )

        if action_type in ["Gym", "Street"]:
            report_text += _build_training_progress_report_block(
                training_count=training_count,
                current_status_title=current_status_title,
                streak_days=streak_days,
            )

        group_text_msg = await message.bot.send_message(
            REPORTS_GROUP_ID,
            report_text,
            reply_markup=report_kb,
            parse_mode="HTML",
        )

        if new_status_title:
            mention_text = (
                f"@{escape(str(user.username))}"
                if user.username
                else escape(str(user.full_name or user.first_name or 'Учасник'))
            )

            await message.bot.send_message(
                REPORTS_GROUP_ID,
                (
                    f"🎖️ <b>НОВИЙ РІВЕНЬ У TURBOTEAM!</b>\n\n"
                    f"{mention_text} переходить на рівень:\n"
                    f"🔥 <b>{escape(str(new_status_title))}</b>\n\n"
                    f"Тренувань виконано: <b>{training_count}</b>"
                ),
                parse_mode="HTML",
            )

        today = get_kyiv_now().strftime("%Y-%m-%d")
        rollback_key = KeyManager.get_training_rollback_key(
            user.id,
            today,
            action_type,
            video_id or "no_video_id",
        )

        report_meta = {
            "target_uid": user.id,
            "nickname": nickname,
            "action_type": action_type,
            "hp": hp,
            "video_id": video_id,
            "date_str": today,
            "group_chat_id": REPORTS_GROUP_ID,
            "rollback_key": rollback_key,
            "video_group_message_id": group_video_msg.message_id if group_video_msg else None,
            "text_group_message_id": group_text_msg.message_id if group_text_msg else None,
        }

        reaction_window_ttl = get_seconds_until_kyiv_midnight()

        if group_video_msg:
            await set_data(
                KeyManager.get_report_meta_key(group_video_msg.message_id),
                report_meta,
                ex=REPORT_META_TTL,
            )
            await set_data(
                KeyManager.get_reaction_window_key(group_video_msg.message_id),
                True,
                ex=reaction_window_ttl,
            )

        if group_text_msg:
            await set_data(
                KeyManager.get_report_meta_key(group_text_msg.message_id),
                report_meta,
                ex=REPORT_META_TTL,
            )
            await set_data(
                KeyManager.get_reaction_window_key(group_text_msg.message_id),
                True,
                ex=reaction_window_ttl,
            )

        await set_data(rollback_key, report_meta, ex=REPORT_META_TTL)

        return True
