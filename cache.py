import redis.asyncio as redis
import json
import logging
import os
from typing import Any, Optional

from config import REDIS_URL

logger = logging.getLogger(__name__)

# ==============================================================================
# SINGLE REDIS CLIENT
# ==============================================================================
try:
    _url = os.getenv("REDIS_URL") or REDIS_URL
    redis_client: Optional[redis.Redis] = redis.from_url(
        _url,
        decode_responses=True,
        max_connections=20,
    )
    logger.info("--- [CACHE] Redis connected (single client) ---")
except Exception as e:
    logger.critical(f"--- [CACHE CRITICAL] Redis connection error: {e} ---")
    redis_client = None


# ==============================================================================
# KEY MANAGER
# ==============================================================================
class KeyManager:
    PREFIX = "turbo"

    # --- Registration ---
    @staticmethod
    def get_reg_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:is_reg:{uid}"

    # --- Referrals ---
    @staticmethod
    def get_ref_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:pending_ref:{uid}"

    @staticmethod
    def get_ref_cooldown_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:ref_cooldown:{uid}"

    @staticmethod
    def get_ref_processed_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:ref_processed:{uid}"

    # --- State ---
    @staticmethod
    def get_state_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:state:{uid}"

    @staticmethod
    def get_session_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:session:{uid}"

    # --- Reports / complaints ---
    @staticmethod
    def get_report_key(target_uid: int, v_msg_id: int) -> str:
        return f"{KeyManager.PREFIX}:reports:{target_uid}:{v_msg_id}"

    @staticmethod
    def get_report_vote_key(target_uid: int, v_msg_id: int, voter_uid: int) -> str:
        return f"{KeyManager.PREFIX}:report_vote:{target_uid}:{v_msg_id}:{voter_uid}"

    @staticmethod
    def get_report_penalty_key(target_uid: int, v_msg_id: int) -> str:
        return f"{KeyManager.PREFIX}:report_penalty:{target_uid}:{v_msg_id}"

    # --- Limits / anti-spam ---
    @staticmethod
    def get_limit_key(user_id: int, action_type: str, date_str: str) -> str:
        return f"{KeyManager.PREFIX}:limit:{user_id}:{action_type}:{date_str}"

    # --- Distributed lock for activities ---
    @staticmethod
    def get_action_lock_key(uid: int, action_and_date: str) -> str:
        return f"{KeyManager.PREFIX}:lock:{uid}:{action_and_date}"

    # --- Rating ---
    @staticmethod
    def get_rating_limit_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:rating_limit:{uid}"

    @staticmethod
    def get_rating_cache_key() -> str:
        return f"{KeyManager.PREFIX}:global_rating"

    # --- Profile ---
    @staticmethod
    def get_profile_limit_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:profile_limit:{uid}"

    @staticmethod
    def get_profile_warn_key(uid: int) -> str:
        return f"{KeyManager.PREFIX}:profile_warn:{uid}"

    # --- Bot ---
    @staticmethod
    def get_bot_username_key() -> str:
        return f"{KeyManager.PREFIX}:bot_username"

    @staticmethod
    def get_event_idempotency_key(event_key: str) -> str:
        return f"{KeyManager.PREFIX}:event:{event_key}"

    @staticmethod
    def get_start_dedupe_key(uid: int, payload: str) -> str:
        return f"{KeyManager.PREFIX}:start:{uid}:{payload}"

    @staticmethod
    def get_training_repeat_key(uid: int, date_str: str) -> str:
        return f"{KeyManager.PREFIX}:train_repeat:{uid}:{date_str}"


# ==============================================================================
# STANDARD OPERATIONS
# ==============================================================================

async def set_data(key: str, value: Any, ex: Optional[int] = None) -> bool:
    """
    Saves data to Redis.
    Returns True on success, False on error.
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
        logger.error(f"[CACHE] Write error key={key}: {e}")
        return False


async def get_data(key: str) -> Any:
    """
    Reads data from Redis.
    Returns None if key does not exist or on error.
    """
    if redis_client is None:
        return None
    try:
        data = await redis_client.get(key)
        if data is None:
            return None

        try:
            return json.loads(data)
        except (json.JSONDecodeError, ValueError):
            pass

        return data

    except Exception as e:
        logger.error(f"[CACHE] Read error key={key}: {e}")
        return None


async def delete_data(key: str) -> bool:
    """Deletes key. Returns True on success."""
    if redis_client is None:
        return False
    try:
        await redis_client.delete(key)
        return True
    except Exception as e:
        logger.error(f"[CACHE] Delete error key={key}: {e}")
        return False


async def set_flag(key: str, ex: Optional[int] = None) -> bool:
    """
    Helper for storing flag value "1".
    """
    return await set_data(key, "1", ex=ex)


async def acquire_lock(key: str, ex: int = 86400) -> bool:
    """
    Atomic SET NX lock.
    Returns True if lock acquired, False if already exists or on error.
    """
    if redis_client is None:
        return False
    try:
        result = await redis_client.set(key, "1", nx=True, ex=ex)
        return result is True
    except Exception as e:
        logger.error(f"[CACHE] acquire_lock error key={key}: {e}")
        return False
