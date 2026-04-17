import logging
import random
from typing import Optional, Dict, Any

from aiogram.types import Message, User

from config import ADMIN_IDS
from database import _request
from cache import get_data, set_data, set_flag, KeyManager
from services import safe_create_task, auto_delete

logger = logging.getLogger(__name__)

# ==============================================================================
# ОТРИМАННЯ ДАНИХ РЕЙТИНГУ (ULTRA SAFE)
# ==============================================================================

async def get_rating_data(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Отримує рейтингові дані з багаторівневим захистом.
    Redis cache (60-120s) -> GAS via database._request.
    """
    cache_key = KeyManager.get_rating_cache_key()

    try:
        cached = await get_data(cache_key)
        if cached:
            return cached
    except Exception as e:
        logger.error(f"[RATINGS] Redis read error: {e}")

    payload = {
        "action": "get_rating",
        "user_id": str(user_id)
    }

    try:
        result = await _request(payload, method="POST")
        logger.info(f"[RATINGS] GAS raw response for uid={user_id}: {result}")

        if isinstance(result, dict) and "top" in result:
            ttl = random.randint(60, 120)
            await set_data(cache_key, result, ex=ttl)
            return result

        logger.warning(f"[RATINGS] Invalid GAS response for uid={user_id}: {result}")
    except Exception as e:
        logger.error(f"[RATINGS] Request failed for uid={user_id}: {e}")

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

        if uid not in ADMIN_IDS:
            safe_create_task(auto_delete(sent_msg, 30))
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