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
from cache import KeyManager, acquire_lock, get_data
from database import get_kyiv_now, add_activity, check_activity_limit, update_user_activity
from phrases import get_phrase
from config import GROUP_LINK
from reports import build_report_keyboard
from supabase_db import (
    get_user_by_telegram_id,
    get_user_activities,
    has_user_achievement,
    add_user_achievement,
)

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

# ==============================================================================
# STREAK PARAMETERS
# ==============================================================================
STREAK_BONUS_3_DAYS = 50
STREAK_BONUS_5_DAYS = 100
STREAK_BONUS_7_DAYS = 200

# ==============================================================================
# ACHIEVEMENTS (TRAINING ONLY, V1)
# ==============================================================================
TRAINING_ACHIEVEMENTS = [
    (1, "training_1", "Перший крок"),
    (5, "training_5", "Розігрів"),
    (10, "training_10", "Перша десятка"),
    (25, "training_25", "У ритмі"),
    (50, "training_50", "Півсотні"),
    (100, "training_100", "Сотка"),
    (200, "training_200", "Машина"),
    (500, "training_500", "Монстр"),
    (1000, "training_1000", "Легенда TurboTeam"),
]


# ==============================================================================
# QUIZ VALIDATION
# ==============================================================================

def validate_quiz(data: dict) -> bool:
    """
    Validates quiz data from WebApp.
    Accepts any non-empty strings.
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
        if not isinstance(goal, str) or not (0 < len(goal) < 200):
            logger.warning(f"[VALIDATE] Invalid goal: {goal!r}")
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
                    exc_info=True
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
    async def maybe_grant_training_achievement(user_id: int) -> Optional[str]:
        """
        Grants a training achievement if the user has reached a new training milestone.
        Returns achievement title if granted, otherwise None.
        """
        user_row = await get_user_by_telegram_id(user_id)
        if not user_row:
            return None

        user_uuid = user_row.get("id")
        if not user_uuid:
            return None

        activities = await get_user_activities(str(user_uuid), limit=1000)

        training_count = 0
        for activity in activities:
            action_name = str(activity.get("action_name", ""))
            if action_name in {"Gym", "Street"}:
                training_count += 1

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

        for action_name in daily_actions:
            if action_name.strip().lower() in ignore_set:
                continue

            lock_key = KeyManager.get_action_lock_key(user_id, f"{action_name}:{today}")
            if (await get_data(lock_key)) is not None:
                logger.debug(
                    "[check_today_report] Redis-hit: uid=%s action=%s date=%s",
                    user_id,
                    action_name,
                    today,
                )
                return True

        for action_name in daily_actions:
            if action_name.strip().lower() in ignore_set:
                continue

            can_log = await ActivityService.can_user_log_activity(user_id, action_name)
            if not can_log:
                logger.debug(
                    "[check_today_report] GAS-hit: uid=%s action=%s date=%s",
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
        Checks streak and grants bonus.
        Returns (bonus, streak).
        """
        from database import get_user_stats

        stats = await get_user_stats(user_id)
        if not stats:
            return 0, 0

        streak = int(stats.get("streak", 0))

        bonus = 0
        if streak == 3:
            bonus = STREAK_BONUS_3_DAYS
        elif streak == 5:
            bonus = STREAK_BONUS_5_DAYS
        elif streak >= 7 and streak % 7 == 0:
            bonus = STREAK_BONUS_7_DAYS

        if bonus > 0:
            action_label = f"🔥 Streak Bonus ({streak} days)"
            await add_activity(user_id, nickname, action_label, bonus)
            logger.info(f"[STREAK] Bonus +{bonus} HP granted to {nickname} for {streak} days")

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
            streak_bonus, streak_days = await ActivityService.check_and_grant_streak_bonus(user_id, nickname)

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
            microsecond=0
        )
        return max(1, int((next_midnight - now).total_seconds()))

    @staticmethod
    @handle_exceptions(default_return=False)
    async def process_training_full_cycle(message: Message, action_type: str) -> bool:
        """
        Full training orchestration:
        1. calculate HP
        2. write activity
        3. publish report to group with complaint button
        """
        user = message.from_user
        nickname = user.full_name
        hp = ActivityService.calculate_training_hp(action_type)
        video_id = message.video_note.file_id if message.video_note else ""

        granted, streak_bonus, streak_days = await ActivityService.grant_hp(
            user.id,
            nickname,
            action_type,
            hp,
            video_id=video_id,
        )
        if not granted:
            return False

        back_to_group_kb = types.InlineKeyboardMarkup(
            inline_keyboard=[[
                types.InlineKeyboardButton(text="😎 Повертайся в банду", url=GROUP_LINK)
            ]]
        )
        await message.answer(
            f"✅ {action_type} зафіксовано. +{hp} HP",
            reply_markup=back_to_group_kb,
        )

        if streak_bonus > 0:
            await message.answer(
                f"🔥 <b>STREAK BONUS!</b>\n"
                f"Серія: {streak_days} дні\n"
                f"+{streak_bonus} HP",
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

        report_kb = build_report_keyboard(
            target_uid=user.id,
            action_type=action_type,
        )

        try:
            await message.copy_to(
                REPORTS_GROUP_ID,
                reply_markup=report_kb,
            )
        except Exception as e:
            logger.warning("[SERVICE] Failed to copy video to group: %s", e)

        await message.bot.send_message(
            REPORTS_GROUP_ID,
            f"{get_phrase('report', nickname=f'@{user.username or user.first_name}')}\n+{hp} HP",
        )
        return True
