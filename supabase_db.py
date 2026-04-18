import os
import logging
from typing import Optional, Dict, Any, List

from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

_supabase: Optional[Client] = None


def get_supabase() -> Client:
    global _supabase

    if _supabase is not None:
        return _supabase

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set")

    _supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    logger.info("[SUPABASE] Client initialized")
    return _supabase


# ==============================================================================
# USERS
# ==============================================================================

async def get_user_by_telegram_id(telegram_user_id: int) -> Optional[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("users")
        .select("*")
        .eq("telegram_user_id", telegram_user_id)
        .limit(1)
        .execute()
    )

    if response.data:
        return response.data[0]
    return None


async def get_all_users() -> List[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("users")
        .select("*")
        .execute()
    )

    return response.data or []


async def create_user(
    telegram_user_id: int,
    nickname: str,
    gender: Optional[str] = None,
    level: Optional[str] = None,
    goal: Optional[str] = None,
) -> Dict[str, Any]:
    sb = get_supabase()

    payload = {
        "telegram_user_id": telegram_user_id,
        "nickname": nickname,
        "gender": gender,
        "level": level,
        "goal": goal,
    }

    response = sb.table("users").insert(payload).execute()

    if not response.data:
        raise RuntimeError("Failed to create user in Supabase")

    return response.data[0]


# ==============================================================================
# ACTIVITIES
# ==============================================================================

async def add_activity(
    user_id: str,
    action_name: str,
    hp_change: int,
    video_status: Optional[str] = None,
    video_id: Optional[str] = None,
) -> Dict[str, Any]:
    sb = get_supabase()

    payload = {
        "user_id": user_id,
        "action_name": action_name,
        "hp_change": hp_change,
        "video_status": video_status,
        "video_id": video_id,
    }

    response = sb.table("activities").insert(payload).execute()

    if not response.data:
        raise RuntimeError("Failed to add activity in Supabase")

    return response.data[0]


async def get_user_activities(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("activities")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )

    return response.data or []


async def get_user_activities_in_period(
    user_id: str,
    created_at_from: str,
    created_at_to: str,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("activities")
        .select("*")
        .eq("user_id", user_id)
        .gte("created_at", created_at_from)
        .lt("created_at", created_at_to)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )

    return response.data or []


async def get_all_activities_in_period(
    created_at_from: str,
    created_at_to: str,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("activities")
        .select("*")
        .gte("created_at", created_at_from)
        .lt("created_at", created_at_to)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )

    return response.data or []


async def get_user_activities_count(user_id: str) -> int:
    sb = get_supabase()

    response = (
        sb.table("activities")
        .select("id", count="exact")
        .eq("user_id", user_id)
        .execute()
    )

    return response.count or 0


# ==============================================================================
# REFERRALS
# ==============================================================================

async def add_referral(
    referrer_user_id: str,
    new_user_id: str,
    points: int = 150,
) -> Dict[str, Any]:
    sb = get_supabase()

    payload = {
        "referrer_user_id": referrer_user_id,
        "new_user_id": new_user_id,
        "points": points,
    }

    response = sb.table("referrals").insert(payload).execute()

    if not response.data:
        raise RuntimeError("Failed to add referral in Supabase")

    return response.data[0]


async def get_referrals_count(referrer_user_id: str) -> int:
    sb = get_supabase()

    response = (
        sb.table("referrals")
        .select("id", count="exact")
        .eq("referrer_user_id", referrer_user_id)
        .execute()
    )

    return response.count or 0


# ==============================================================================
# ACHIEVEMENTS
# ==============================================================================

async def add_user_achievement(
    user_id: str,
    achievement_code: str,
    achievement_title: str,
) -> Dict[str, Any]:
    sb = get_supabase()

    payload = {
        "user_id": user_id,
        "achievement_code": achievement_code,
        "achievement_title": achievement_title,
    }

    response = sb.table("user_achievements").insert(payload).execute()

    if not response.data:
        raise RuntimeError("Failed to add user achievement in Supabase")

    return response.data[0]


async def get_user_achievements(user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("user_achievements")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )

    return response.data or []


async def has_user_achievement(user_id: str, achievement_code: str) -> bool:
    sb = get_supabase()

    response = (
        sb.table("user_achievements")
        .select("id")
        .eq("user_id", user_id)
        .eq("achievement_code", achievement_code)
        .limit(1)
        .execute()
    )

    return bool(response.data)


async def get_user_achievements_count(user_id: str) -> int:
    sb = get_supabase()

    response = (
        sb.table("user_achievements")
        .select("id", count="exact")
        .eq("user_id", user_id)
        .execute()
    )

    return response.count or 0


async def get_last_user_achievement(user_id: str) -> Optional[Dict[str, Any]]:
    sb = get_supabase()

    response = (
        sb.table("user_achievements")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

    if response.data:
        return response.data[0]
    return None
