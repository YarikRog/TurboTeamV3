import logging
import random
from html import escape
from typing import Optional, Dict, Any
from datetime import timedelta, datetime

import pytz
from aiogram.types import Message, User

from cache import get_data, set_data, set_flag, KeyManager
from services import safe_create_task, auto_delete
from supabase_db import get_weekly_rating

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


# ==============================================================================
# ОТРИМАННЯ ДАНИХ РЕЙТИНГУ (ULTRA SAFE)
# ==============================================================================

async def get_rating_data(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Отримує рейтингові дані з багаторівневим захистом.
    Redis cache (60-120s) -> Supabase weekly rating RPC.
    """
    cache_key = KeyManager.get_rating_cache_key()

    try:
        cached = await get_data(cache_key)
        if cached:
            return cached
    except Exception as e:
        logger.error(f"[RATINGS] Redis read error: {e}")

    try:
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

        top_list = normalized_rows[:3]

        user_rank = "?"
        user_hp = 0

        for player in normalized_rows:
            if int(player.get("telegram_user_id", 0) or 0) == int(user_id):
                user_rank = int(player.get("rank", 0) or 0) or "?"
                user_hp = int(player.get("hp", 0) or 0)
                break

        result = {
            "top": top_list,
            "user_rank": user_rank,
            "user_hp": user_hp,
        }

        ttl = random.randint(60, 120)
        await set_data(cache_key, result, ex=ttl)
        return result

    except Exception as e:
        logger.error(f"[RATINGS] Supabase weekly rating RPC failed for uid={user_id}: {e}", exc_info=True)

    return None


# ==============================================================================
# ВІДОБРАЖЕННЯ РЕЙТИНГУ (UX OPTIMIZED)
# ==============================================================================

async def show_rating_for_user(message: Message, actor: User) -> Optional[Message]:
    """
    Формує та виводить рейтинг з рефералами під кожним гравцем і самознищенням.
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

            text += f"{icon} {nick} — <b>{hp}</b> HP\n"
            text += f"   Рефералів: <b>{refs}</b>\n\n"

        text += (
            "----------------\n"
            f"Твоє місце: <b>{user_rank}</b> | Твої HP: <b>{user_hp}</b>"
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