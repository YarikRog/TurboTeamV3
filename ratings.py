import logging
import random
from html import escape
from typing import Optional, Dict, Any
from datetime import timedelta, datetime

import pytz
from aiogram.types import Message, User

from cache import get_data, set_data, set_flag, KeyManager
from services import safe_create_task, auto_delete
from supabase_db import get_weekly_rating, get_user_by_telegram_id, get_user_activities

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")


def _get_current_week_period() -> tuple[str, str]:
    """
    Returns custom week boundaries in ISO format.
    Week starts on Sunday at 20:00 Kyiv time.
    Period: Sunday 20:00 -> next Sunday 20:00
    """
    now = datetime.now(KYIV_TZ)

    # weekday(): Monday=0 ... Sunday=6
    days_since_sunday = (now.weekday() + 1) % 7
    current_sunday = (now - timedelta(days=days_since_sunday)).replace(
        hour=20,
        minute=0,
        second=0,
        microsecond=0,
    )

    if now < current_sunday:
        week_start = current_sunday - timedelta(days=7)
    else:
        week_start = current_sunday

    week_end = week_start + timedelta(days=7)

    return week_start.isoformat(), week_end.isoformat()


def _parse_activity_created_at(value):
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


def _calculate_training_streak(activities: list[dict]) -> int:
    training_actions = {"Gym", "Street"}
    training_dates = set()

    for activity in activities:
        action_name = str(activity.get("action_name", "")).strip()
        if action_name not in training_actions:
            continue

        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue

        training_dates.add(created_at.date())

    streak = 0
    cursor = datetime.now(KYIV_TZ).date()

    while cursor in training_dates:
        streak += 1
        cursor -= timedelta(days=1)

    return streak


def _format_days_uk(days: int) -> str:
    if days % 10 == 1 and days % 100 != 11:
        return "день"

    if 2 <= days % 10 <= 4 and not (12 <= days % 100 <= 14):
        return "дні"

    return "днів"


async def _get_user_streak_by_telegram_id(telegram_user_id: int) -> int:
    try:
        user_row = await get_user_by_telegram_id(int(telegram_user_id))
        if not user_row:
            return 0

        user_uuid = user_row.get("id")
        if not user_uuid:
            return 0

        activities = await get_user_activities(str(user_uuid), limit=500)
        return _calculate_training_streak(activities)

    except Exception as e:
        logger.error(
            "[RATINGS] Failed to calculate streak for telegram_user_id=%s: %s",
            telegram_user_id,
            e,
            exc_info=True,
        )
        return 0


async def _get_cached_rating_rows(cache_key: str) -> Optional[list[dict]]:
    try:
        cached = await get_data(cache_key)
        if isinstance(cached, dict) and isinstance(cached.get("rows"), list):
            return cached["rows"]
    except Exception as e:
        logger.error(f"[RATINGS] Redis read error: {e}")

    return None


async def _load_rating_rows() -> list[dict]:
    period_start, period_end = _get_current_week_period()
    ranking_rows = await get_weekly_rating(period_start, period_end)

    normalized_rows = []
    for row in ranking_rows:
        normalized_rows.append({
            "telegram_user_id": row.get("telegram_user_id"),
            "nick": row.get("nick") or f"ID:{row.get('telegram_user_id', 'unknown')}",
            "hp": int(row.get("hp", 0) or 0),
            "referrals_count": int(row.get("referrals_count", 0) or 0),
            "rank": int(row.get("rank", 0) or 0),
        })

    return normalized_rows


async def _add_streaks_for_top_players(top_list: list[dict]) -> list[dict]:
    result = []

    for player in top_list:
        player_with_streak = dict(player)
        telegram_user_id = player_with_streak.get("telegram_user_id")
        streak = 0

        if telegram_user_id:
            streak = await _get_user_streak_by_telegram_id(int(telegram_user_id))

        player_with_streak["streak"] = int(streak)
        result.append(player_with_streak)

    return result


# ==============================================================================
# ОТРИМАННЯ ДАНИХ РЕЙТИНГУ (ULTRA SAFE)
# ==============================================================================

async def get_rating_data(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Отримує рейтингові дані з багаторівневим захистом.
    Redis cache (60-120s) -> Supabase weekly rating RPC.

    Important:
    cache stores only global rating rows.
    User-specific fields are calculated per current user and are not stored
    in the shared global cache.
    """
    cache_key = KeyManager.get_rating_cache_key()

    try:
        normalized_rows = await _get_cached_rating_rows(cache_key)

        if normalized_rows is None:
            normalized_rows = await _load_rating_rows()
            ttl = random.randint(60, 120)
            await set_data(cache_key, {"rows": normalized_rows}, ex=ttl)

        top_list = await _add_streaks_for_top_players(normalized_rows[:3])

        user_rank = "?"
        user_hp = 0

        for player in normalized_rows:
            if int(player.get("telegram_user_id", 0) or 0) == int(user_id):
                user_rank = int(player.get("rank", 0) or 0) or "?"
                user_hp = int(player.get("hp", 0) or 0)
                break

        user_streak = await _get_user_streak_by_telegram_id(int(user_id))

        return {
            "top": top_list,
            "user_rank": user_rank,
            "user_hp": user_hp,
            "user_streak": user_streak,
        }

    except Exception as e:
        logger.error(f"[RATINGS] Supabase weekly rating RPC failed for uid={user_id}: {e}", exc_info=True)

    return None


# ==============================================================================
# ВІДОБРАЖЕННЯ РЕЙТИНГУ (UX OPTIMIZED)
# ==============================================================================

async def show_rating_for_user(message: Message, actor: User) -> Optional[Message]:
    """
    Формує та виводить рейтинг з рефералами й streak під кожним гравцем.
    HTML-safe version.
    """
    uid = actor.id

    try:
        await message.delete()
    except Exception:
        pass

    limit_key = KeyManager.get_rating_limit_key(uid)
    if (await get_data(limit_key)) is not None:
        msg = await message.answer("⏳ Бро, рейтинг оновлюється раз на 5 хв. Зачекай!")
        safe_create_task(auto_delete(msg, 5))
        return None

    await set_flag(limit_key, ex=300)

    data = await get_rating_data(uid)
    if not data or "top" not in data:
        err = await message.answer("📉 Рейтинг зараз недоступний. Спробуй пізніше!")
        safe_create_task(auto_delete(err, 5))
        return None

    try:
        top_list = data.get("top", [])
        user_rank = escape(str(data.get("user_rank", "?")))
        user_hp = int(data.get("user_hp", 0) or 0)
        user_streak = int(data.get("user_streak", 0) or 0)

        text = "🏆 <b>РЕЙТИНГ ТИЖНЯ</b>\n\n"

        for i, player in enumerate(top_list):
            if i == 0:
                icon = "🥇"
            elif i == 1:
                icon = "🥈"
            elif i == 2:
                icon = "🥉"
            else:
                icon = f"{i + 1}."

            nick = escape(str(player.get("nick", "Unknown")))
            hp = int(player.get("hp", 0) or 0)
            refs = int(player.get("referrals_count", 0) or 0)
            streak = int(player.get("streak", 0) or 0)

            text += f"{icon} {nick} — <b>{hp}</b> HP\n"
            text += f"   Рефералів: <b>{refs}</b>\n"
            text += f"   Streak: <b>{streak}</b> {_format_days_uk(streak)} 🔥\n\n"

        text += (
            "----------------\n"
            f"Твоє місце: <b>{user_rank}</b> | Твої HP: <b>{user_hp}</b>"
            f"\nТвій Streak: <b>{user_streak}</b> {_format_days_uk(user_streak)} 🔥"
        )

    except Exception as e:
        logger.error(f"[RATINGS] Text formatting error: {e}", exc_info=True)
        return await message.answer("⚠️ Помилка формування рейтингу.")

    try:
        sent_msg = await message.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent_msg, 30))
        return sent_msg

    except Exception as e:
        logger.error(f"[RATINGS] Send error: {e}", exc_info=True)
        return None


async def show_rating(message: Message) -> Optional[Message]:
    return await show_rating_for_user(message, message.from_user)
