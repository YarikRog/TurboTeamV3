import asyncio
import os
import logging
from typing import Optional, Dict, Any, List, Callable, TypeVar

from supabase import create_client, Client

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

_supabase: Optional[Client] = None
T = TypeVar("T")


def get_supabase() -> Client:
    global _supabase

    if _supabase is not None:
        return _supabase

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set")

    _supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    logger.info("[SUPABASE] Client initialized")
    return _supabase


async def _run_sync(fn: Callable[[], T]) -> T:
    return await asyncio.to_thread(fn)


# ==============================================================================
# USERS
# ==============================================================================

async def get_user_by_telegram_id(telegram_user_id: int) -> Optional[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("users")
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .limit(1)
            .execute()
        )

    response = await _run_sync(_query)

    if response.data:
        return response.data[0]
    return None


async def get_user_by_nickname(nickname: str) -> Optional[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("users")
            .select("*")
            .eq("nickname", nickname)
            .limit(1)
            .execute()
        )

    response = await _run_sync(_query)

    if response.data:
        return response.data[0]
    return None


async def get_all_users() -> List[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("users")
            .select("*")
            .execute()
        )

    response = await _run_sync(_query)
    return response.data or []


async def create_user(
    telegram_user_id: int,
    nickname: str,
    gender: Optional[str] = None,
    level: Optional[str] = None,
    goal: Optional[str] = None,
    weekly_plan: Optional[str] = None,
    training_place: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {
        "telegram_user_id": telegram_user_id,
        "nickname": nickname,
        "gender": gender,
        "level": level,
        "goal": goal,
        "weekly_plan": weekly_plan,
        "training_place": training_place,
    }

    def _query():
        sb = get_supabase()
        return sb.table("users").insert(payload).execute()

    response = await _run_sync(_query)

    if not response.data:
        raise RuntimeError("Failed to create user in Supabase")

    return response.data[0]


async def delete_user_by_id(user_id: str) -> bool:
    def _query():
        sb = get_supabase()
        return sb.table("users").delete().eq("id", user_id).execute()

    await _run_sync(_query)
    return True


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
    payload = {
        "user_id": user_id,
        "action_name": action_name,
        "hp_change": hp_change,
        "video_status": video_status,
        "video_id": video_id,
    }

    def _query():
        sb = get_supabase()
        return sb.table("activities").insert(payload).execute()

    response = await _run_sync(_query)

    if not response.data:
        raise RuntimeError("Failed to add activity in Supabase")

    return response.data[0]


async def get_user_activities(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("activities")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

    response = await _run_sync(_query)
    return response.data or []


async def get_user_activities_in_period(
    user_id: str,
    created_at_from: str,
    created_at_to: str,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("activities")
            .select("*")
            .eq("user_id", user_id)
            .gte("created_at", created_at_from)
            .lt("created_at", created_at_to)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

    response = await _run_sync(_query)
    return response.data or []


async def get_all_activities_in_period(
    created_at_from: str,
    created_at_to: str,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("activities")
            .select("*")
            .gte("created_at", created_at_from)
            .lt("created_at", created_at_to)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

    response = await _run_sync(_query)
    return response.data or []


async def get_user_activities_count(user_id: str) -> int:
    def _query():
        sb = get_supabase()
        return (
            sb.table("activities")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .execute()
        )

    response = await _run_sync(_query)
    return response.count or 0


# ==============================================================================
# REFERRALS
# ==============================================================================

async def add_referral(
    referrer_user_id: str,
    new_user_id: str,
    points: int = 150,
) -> Dict[str, Any]:
    payload = {
        "referrer_user_id": referrer_user_id,
        "new_user_id": new_user_id,
        "points": points,
    }

    def _query():
        sb = get_supabase()
        return sb.table("referrals").insert(payload).execute()

    response = await _run_sync(_query)

    if not response.data:
        raise RuntimeError("Failed to add referral in Supabase")

    return response.data[0]


async def get_referrals_count(referrer_user_id: str) -> int:
    def _query():
        sb = get_supabase()
        return (
            sb.table("referrals")
            .select("id", count="exact")
            .eq("referrer_user_id", referrer_user_id)
            .execute()
        )

    response = await _run_sync(_query)
    return response.count or 0


# ==============================================================================
# ACHIEVEMENTS
# ==============================================================================

async def add_user_achievement(
    user_id: str,
    achievement_code: str,
    achievement_title: str,
) -> Dict[str, Any]:
    payload = {
        "user_id": user_id,
        "achievement_code": achievement_code,
        "achievement_title": achievement_title,
    }

    def _query():
        sb = get_supabase()
        return sb.table("user_achievements").insert(payload).execute()

    response = await _run_sync(_query)

    if not response.data:
        raise RuntimeError("Failed to add user achievement in Supabase")

    return response.data[0]


async def get_user_achievements(user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("user_achievements")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

    response = await _run_sync(_query)
    return response.data or []


async def has_user_achievement(user_id: str, achievement_code: str) -> bool:
    def _query():
        sb = get_supabase()
        return (
            sb.table("user_achievements")
            .select("id")
            .eq("user_id", user_id)
            .eq("achievement_code", achievement_code)
            .limit(1)
            .execute()
        )

    response = await _run_sync(_query)
    return bool(response.data)


async def get_user_achievements_count(user_id: str) -> int:
    def _query():
        sb = get_supabase()
        return (
            sb.table("user_achievements")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .execute()
        )

    response = await _run_sync(_query)
    return response.count or 0


async def get_last_user_achievement(user_id: str) -> Optional[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return (
            sb.table("user_achievements")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

    response = await _run_sync(_query)

    if response.data:
        return response.data[0]
    return None


# ==============================================================================
# RPC
# ==============================================================================

async def get_weekly_rating(period_start: str, period_end: str) -> List[Dict[str, Any]]:
    def _query():
        sb = get_supabase()
        return sb.rpc(
            "get_weekly_rating",
            {
                "p_period_start": period_start,
                "p_period_end": period_end,
            },
        ).execute()

    response = await _run_sync(_query)
    return response.data or []