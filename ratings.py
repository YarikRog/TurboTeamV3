import logging
import random
import asyncio
from urllib.parse import quote
from typing import Optional, Dict, Any

from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, User

from config import ADMIN_IDS
# Використовуємо уніфікований метод з database.py для стабільності
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

    # 1. Швидкий шлях: Redis Cache
    try:
        cached = await get_data(cache_key)
        if cached:
            return cached
    except Exception as e:
        logger.error(f"[RATINGS] Redis read error: {e}")

    # 2. Основний шлях: Запит до Google Script через базу
    # Використовуємо POST, як ми налаштували в database.py
    payload = {
        "action": "get_rating", 
        "user_id": str(user_id)
    }
    
    try:
        # _request вже має семафори, ретраї та обробку статусів
        result = await _request(payload, method="POST")

        if isinstance(result, dict) and "top" in result:
            # Jitter TTL (захист від Thundering Herd)
            ttl = random.randint(60, 120)
            await set_data(cache_key, result, ex=ttl)
            return result
        
        logger.warning(f"[RATINGS] Invalid GAS response for uid={user_id}")
    except Exception as e:
        logger.error(f"[RATINGS] Request failed for uid={user_id}: {e}")

    return None


# ==============================================================================
# ВІДОБРАЖЕННЯ РЕЙТИНГУ (UX OPTIMIZED)
# ==============================================================================

async def show_rating_for_user(message: Message, actor: User) -> Optional[Message]:
    """
    Формує та виводить рейтинг з реферальною системою та самознищенням.
    """
    uid = actor.id

    # 1. Персональний Rate Limit (захист від спаму кнопкою)
    limit_key = KeyManager.get_rating_limit_key(uid)
    if (await get_data(limit_key)) is not None:
        msg = await message.answer("⏳ Бро, рейтинг оновлюється раз на 10 сек. Зачекай!")
        safe_create_task(auto_delete(msg, 5))
        try:
            await message.delete()
        except:
            pass
        return None

    await set_flag(limit_key, ex=10)

    # 2. Отримання даних
    data = await get_rating_data(uid)
    if not data or "top" not in data:
        err = await message.answer("📉 Рейтинг зараз недоступний. Спробуй пізніше!")
        safe_create_task(auto_delete(err, 5))
        return None

    # 3. Формування тексту (Безпечна ітерація)
    try:
        top_list = data.get("top", [])
        user_rank = data.get("user_rank", "?")
        user_hp = data.get("user_hp", 0)

        text = "🏆 *РЕЙТИНГ ТИЖНЯ*\n\n"
        
        # Динамічні медалі (захист від IndexError)
        for i, player in enumerate(top_list):
            if i == 0:
                icon = "🥇"
            elif i == 1:
                icon = "🥈"
            elif i == 2:
                icon = "🥉"
            else:
                icon = f"{i + 1}."
            
            text += f"{icon} {player['nick']} — {player['hp']} HP\n"
            
        text += f"\n----------------\nТвоє місце: *{user_rank}* | Твої HP: *{user_hp}*"

    except Exception as e:
        logger.error(f"[RATINGS] Text formatting error: {e}", exc_info=True)
        return await message.answer("⚠️ Помилка формування рейтингу.")

    # 4. Реферальна кнопка (Динамічна)
    kb = None
    try:
        # Отримуємо актуальний username бота прямо з API
        bot_info = await message.bot.get_me()
        bot_username = bot_info.username or "TurboTeamBot"
        
        ref_link = f"https://t.me/{bot_username}?start={uid}"
        invite_text = (
            f"Бро, залітай у TurboTeam! 🏎️💨\n"
            f"Рубаємо HP та змагаємося за рейтинги.\n"
            f"Отримай бонус за посиланням: {ref_link}"
        )
        share_url = f"https://t.me/share/url?text={quote(invite_text)}"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="ЗАПРОСИТИ МОНСТРА 🔗", url=share_url)
        ]])
    except Exception as e:
        logger.warning(f"[RATINGS] KB creation failed: {e}")

    # 5. Відправка та очищення
    try:
        sent_msg = await message.answer(text, reply_markup=kb, parse_mode="Markdown")

        # Самознищення повідомлення для чистоти чату (крім адмінів)
        if uid not in ADMIN_IDS:
            safe_create_task(auto_delete(sent_msg, 30))
            try:
                if message.from_user and message.from_user.id == uid:
                    await message.delete()
            except:
                pass
        
        return sent_msg

    except Exception as e:
        logger.error(f"[RATINGS] Send error: {e}")
        return None


async def show_rating(message: Message) -> Optional[Message]:
    return await show_rating_for_user(message, message.from_user)
