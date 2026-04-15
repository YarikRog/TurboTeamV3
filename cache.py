import redis.asyncio as redis
import json
import logging
import os
from typing import Any, Optional

from config import REDIS_URL

logger = logging.getLogger(__name__)

# ==============================================================================
# ЄДИНИЙ REDIS КЛІЄНТ
# Один клієнт для всього проєкту. Імпортується скрізь звідси.
# main.py використовує його ж для RedisStorage — нема двох підключень.
# ==============================================================================
try:
    _url = os.getenv("REDIS_URL") or REDIS_URL
    redis_client: Optional[redis.Redis] = redis.from_url(
        _url,
        decode_responses=True,
        # Connection pool: max 20 з'єднань
        max_connections=20,
    )
    logger.info("--- [CACHE] Redis підключено (єдиний клієнт) ---")
except Exception as e:
    logger.critical(f"--- [CACHE CRITICAL] Помилка підключення до Redis: {e} ---")
    redis_client = None


# ==============================================================================
# KEY MANAGER — ЄДИНЕ ДЖЕРЕЛО ІСТИНИ ДЛЯ КЛЮЧІВ
# Правило: ЖОДНОГО сирого f-string для Redis-ключів за межами цього класу.
# Всі ключі мають PREFIX, щоб не конфліктувати з RedisStorage aiogram.
# ==============================================================================
class KeyManager:
    PREFIX = "turbo"

    # --- Реєстрація ---
    @staticmethod
    def get_reg_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:is_reg:{uid}"

    # --- Реферали ---
    @staticmethod
    def get_ref_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:pending_ref:{uid}"

    @staticmethod
    def get_ref_cooldown_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:ref_cooldown:{uid}"

    @staticmethod
    def get_ref_processed_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:ref_processed:{uid}"

    # --- Стан (очікування відео) ---
    @staticmethod
    def get_state_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:state:{uid}"

    @staticmethod
    def get_session_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:session:{uid}"

    # --- Скарги ---
    @staticmethod
    def get_report_key(target_uid: int, v_msg_id: int) -> str:
        return f"{KeyManager.PREFIX}:reports:{target_uid}:{v_msg_id}"

    # --- Ліміти команд (спам-захист) ---
    @staticmethod
    def get_limit_key(user_id: int, action_type: str, date_str: str) -> str:
        return f"{KeyManager.PREFIX}:limit:{user_id}:{action_type}:{date_str}"

    # --- Distributed lock для активностей (анти-дублікат) ---
    @staticmethod
    def get_action_lock_key(uid: int, action_and_date: str) -> str:
        """
        Атомарний lock через SET NX.
        action_and_date = "Gym:2025-01-15"
        """
        return f"{KeyManager.PREFIX}:lock:{uid}:{action_and_date}"

    # --- Рейтинг ---
    @staticmethod
    def get_rating_limit_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:rating_limit:{uid}"

    @staticmethod
    def get_rating_cache_key() -> str:
        return f"{KeyManager.PREFIX}:global_rating"

    # --- Бот ---
    @staticmethod
    def get_bot_username_key() -> str:
        return f"{KeyManager.PREFIX}:bot_username"

    @staticmethod
    def get_event_idempotency_key(event_key: str) -> str:
        return f"{KeyManager.PREFIX}:event:{event_key}"


# ==============================================================================
# СТАНДАРТНІ ОПЕРАЦІЇ
# СТАНДАРТ ПРОЄКТУ:
#   - Прапорці зберігати як value="1"
#   - Перевіряти через: (await get_data(key)) is not None
# ==============================================================================

async def set_data(key: str, value: Any, ex: Optional[int] = None) -> bool:
    """
    Зберігає дані в Redis.
    Повертає True при успіху, False при помилці.
    """
    if redis_client is None:
        return False
    try:
        if isinstance(value, (dict, list)):
            val_to_save = json.dumps(value, ensure_ascii=False)
        else:
            val_to_save = str(value)

        await redis_client.set(key, val_to_save, ex=ex)
        return True
    except Exception as e:
        logger.error(f"[CACHE] Помилка запису key={key}: {e}")
        return False


async def get_data(key: str) -> Any:
    """
    Читає дані з Redis.
    Повертає None якщо ключ не існує або при помилці.
    """
    if redis_client is None:
        return None
    try:
        data = await redis_client.get(key)
        if data is None:
            return None

        # Намагаємось десеріалізувати JSON (dict/list)
        try:
            return json.loads(data)
        except (json.JSONDecodeError, ValueError):
            pass

        # Повертаємо як рядок
        return data

    except Exception as e:
        logger.error(f"[CACHE] Помилка читання key={key}: {e}")
        return None


async def delete_data(key: str) -> bool:
    """Видаляє ключ. Повертає True при успіху."""
    if redis_client is None:
        return False
    try:
        await redis_client.delete(key)
        return True
    except Exception as e:
        logger.error(f"[CACHE] Помилка видалення key={key}: {e}")
        return False


async def set_flag(key: str, ex: Optional[int] = None) -> bool:
    """
    Зручний хелпер для збереження прапорця зі значенням "1".
    Використовуй замість set_data(key, "1", ex=...) для читабельності.
    """
    return await set_data(key, "1", ex=ex)


async def acquire_lock(key: str, ex: int = 86400) -> bool:
    """
    Атомарна операція SET NX — отримати lock.
    Повертає True якщо lock отримано (ключа не було).
    Повертає False якщо lock вже зайнятий (ключ існував).
    Це єдиний правильний спосіб запобігти race condition.
    """
    if redis_client is None:
        return False
    try:
        result = await redis_client.set(key, "1", nx=True, ex=ex)
        return result is True
    except Exception as e:
        logger.error(f"[CACHE] Помилка acquire_lock key={key}: {e}")
        return False
