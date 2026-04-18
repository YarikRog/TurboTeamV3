import logging
import random
from typing import Optional, Dict, Any, List
from datetime import timedelta

import pytz
from aiogram.types import Message, User

from config import ADMIN_IDS
from cache import get_data, set_data, set_flag, KeyManager
from services import safe_create_task, auto_delete
from supabase_db import (
    get_all_users,
    get_user_activities_in_period,
    get_referrals_count,
)

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")


def _get_current_week_period() -> tuple[str, str]:
    """
    Returns current week boundaries in ISO format.
    Week starts on Monday, timezone = Kyiv.
    """
    now = pytz.UTC.localize(__import__("datetime").datetime.utcnow()).astimezone(KYIV_TZ)
    week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    week_end = week_start + timedelta(days=7)

    return week_start.isoformat(), week_end.isoformat()


async def _get_user_week_hp(user_uuid: str, period_start: str, period_end: str) -> int:
    activities = await get_user_activities_in_period(
        user_uuid,
        created_at_from=period_start,
        created_at_to=period_end,
        limit=1000,
    )

    total = 0
    for activity in activities:
        try:
            total += int(activity.get("hp_change", 0) or 0)
        except Exception:
            continue

    return total


# ==============================================================================
# ОТРИМАННЯ ДАНИХ РЕЙТИНГУ (ULTRA SAFE)
# ==============================================================================

async def get_rating_data(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Отримує рейтингові дані з багаторівневим захистом.
    Redis cache (60-120s) -> Supabase weekly rating.
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
        users = await get_all_users()
        ranking_rows = []

        for user in users:
            user_uuid = user.get("id")
            if not user_uuid:
                continue

            hp = await _get_user_week_hp(str(user_uuid), period_start, period_end)
            referrals_count = await get_referrals_count(str(user_uuid))

            nickname = user.get("nickname") or f"ID:{user.get('telegram_user_id', 'unknown')}"

            ranking_rows.append({
                "telegram_user_id": user.get("telegram_user_id"),
                "nick": nickname,
                "hp": hp,
                "referrals_count": referrals_count,
            })

        ranking_rows.sort(key=lambda x: (-int(x.get("hp", 0)), str(x.get("nick", ""))))

        top_list = ranking_rows[:10]

        user_rank = "?"
        user_hp = 0

        for idx, player in enumerate(ranking_rows, start=1):
            if int(player.get("telegram_user_id", 0) or 0) == int(user_id):
                user_rank = idx
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
        logger.error(f"[RATINGS] Supabase weekly rating build failed for uid={user_id}: {e}", exc_info=True)

    return None


# ==============================================================================
# ВІДОБРАЖЕННЯ РЕЙТИНГУ (UX OPTIMIZED)
# ==============================================================================

async def show_rating_for_user(message: Message, actor: User) -> Optional[Message]:
    """
    Формує та виводить рейтинг з рефералами під кожним гравцем і самознищенням.
    """
    uid = actor.id

    limit_key = KeyManager.get_rating_limit_key(uid)
    if (await get_data(limit_key)) is not None:
        msg = await message.answer("⏳ Бро, рейтинг оновлюється раз на 5 хв. Зачекай!")
        safe_create_task(auto_delete(msg, 5))
        try:
            await message.delete()
        except Exception:
            pass
        return None

    await set_flag(limit_key, ex=300)

    data = await get_rating_data(uid)
    if not data or "top" not in data:
        err = await message.answer("📉 Рейтинг зараз недоступний. Спробуй пізніше!")
        safe_create_task(auto_delete(err, 5))
        return None

    try:
        top_list = data.get("top", [])
        user_rank = data.get("user_rank", "?")
        user_hp = data.get("user_hp", 0)

        text = "🏆 *РЕЙТИНГ ТИЖНЯ*\n\n"

        for i, player in enumerate(top_list):
            if i == 0:
                icon = "🥇"
            elif i == 1:
                icon = "🥈"
            elif i == 2:
                icon = "🥉"
            else:
                icon = f"{i + 1}."

            nick = player.get("nick", "Unknown")
            hp = player.get("hp", 0)
            refs = player.get("referrals_count", 0)

            text += f"{icon} {nick} — {hp} HP\n"
            text += f"   Рефералів: {refs}\n\n"

        text += (
            "----------------\n"
            f"Твоє місце: *{user_rank}* | Твої HP: *{user_hp}*"
        )

    except Exception as e:
        logger.error(f"[RATINGS] Text formatting error: {e}", exc_info=True)
        return await message.answer("⚠️ Помилка формування рейтингу.")

    try:
        sent_msg = await message.answer(text, parse_mode="Markdown")

        safe_create_task(auto_delete(sent_msg, 30))

        if uid not in ADMIN_IDS:
            try:
                await message.delete()
            except Exception:
                pass

        return sent_msg

    except Exception as e:
        logger.error(f"[RATINGS] Send error: {e}")
        return None


async def show_rating(message: Message) -> Optional[Message]:
    return await show_rating_for_user(message, message.from_user)
