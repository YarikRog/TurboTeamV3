import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl

from aiohttp import web

from cache import KeyManager, get_data
from config import BOT_TOKEN, GROUP_LINK, WEBAPP_CORS_ORIGIN
from database import (
    get_all_time_rating,
    get_kyiv_now,
    get_user_calendar_month,
    get_user_feed,
    get_user_stats,
)
from handlers import get_next_training_goal, get_training_status, word_trainings
from services import TRAINING_ACHIEVEMENTS, TRAINING_ACHIEVEMENT_ICONS
from supabase_db import (
    get_last_user_achievement,
    get_referrals_count,
    get_user_achievements,
    get_user_activities,
    get_user_by_telegram_id,
)

logger = logging.getLogger(__name__)

INIT_DATA_MAX_AGE_SECONDS = 86400


def verify_init_data(init_data: str, bot_token: str) -> Optional[Dict[str, Any]]:
    """Validates Telegram WebApp initData per the official HMAC-SHA256 scheme."""
    if not init_data:
        return None

    try:
        parsed = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        return None

    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None

    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        return None

    try:
        auth_date = int(parsed.get("auth_date", "0"))
    except ValueError:
        return None

    if time.time() - auth_date > INIT_DATA_MAX_AGE_SECONDS:
        return None

    try:
        user = json.loads(parsed.get("user", ""))
    except json.JSONDecodeError:
        return None

    telegram_user_id = user.get("id")
    if not isinstance(telegram_user_id, int):
        return None

    return {"telegram_user_id": telegram_user_id, "user": user}


def _extract_init_data(request: web.Request) -> str:
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("tma "):
        return auth_header[len("tma "):]
    return request.query.get("initData", "")


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        response = web.Response()
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc

    response.headers["Access-Control-Allow-Origin"] = WEBAPP_CORS_ORIGIN
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


async def handle_profile(request: web.Request) -> web.Response:
    auth = verify_init_data(_extract_init_data(request), BOT_TOKEN)
    if not auth:
        return web.json_response({"error": "unauthorized"}, status=401)

    telegram_user_id = auth["telegram_user_id"]

    user_row = await get_user_by_telegram_id(telegram_user_id)
    stats = await get_user_stats(telegram_user_id)

    if not user_row or not stats:
        return web.json_response({"error": "profile not found"}, status=404)

    user_uuid = str(user_row.get("id") or "")
    if not user_uuid:
        return web.json_response({"error": "profile not found"}, status=404)

    now_kyiv = get_kyiv_now()
    try:
        year = int(request.query.get("year", now_kyiv.year))
        month = int(request.query.get("month", now_kyiv.month))
    except ValueError:
        return web.json_response({"error": "invalid year/month"}, status=400)

    if not (1 <= month <= 12):
        return web.json_response({"error": "invalid month"}, status=400)

    activities = await get_user_activities(user_uuid, limit=1000)
    referrals_count = await get_referrals_count(user_uuid)
    unlocked_achievements = await get_user_achievements(user_uuid, limit=200)
    last_achievement = await get_last_user_achievement(user_uuid)
    calendar_days = await get_user_calendar_month(user_uuid, year, month)
    bot_username = await get_data(KeyManager.get_bot_username_key())

    gym_count = sum(1 for a in activities if a.get("action_name") == "Gym")
    street_count = sum(1 for a in activities if a.get("action_name") == "Street")
    rest_count = sum(1 for a in activities if a.get("action_name") == "Rest")
    skip_count = sum(1 for a in activities if a.get("action_name") == "Skipped")
    training_count = gym_count + street_count
    activities_count = gym_count + street_count + rest_count + skip_count

    status_title = get_training_status(training_count)
    next_goal, next_goal_progress = get_next_training_goal(training_count)
    next_goal_text = (
        "MAX"
        if next_goal is None
        else f"{next_goal} {word_trainings(next_goal)} ({next_goal_progress})"
    )

    last_achievement_title = "Поки немає"
    if last_achievement:
        last_achievement_title = str(last_achievement.get("achievement_title") or "Поки немає")

    unlocked_codes = {
        str(item.get("achievement_code") or "").strip()
        for item in unlocked_achievements
        if str(item.get("achievement_code") or "").strip()
    }
    unlocked_at_by_code = {
        str(item.get("achievement_code") or "").strip(): item.get("created_at")
        for item in unlocked_achievements
        if str(item.get("achievement_code") or "").strip()
    }

    achievements_payload = []
    for threshold, code, title in TRAINING_ACHIEVEMENTS:
        unlocked = code in unlocked_codes or training_count >= threshold
        achievements_payload.append(
            {
                "code": code,
                "title": title,
                "icon": TRAINING_ACHIEVEMENT_ICONS.get(code, "🏅"),
                "threshold": threshold,
                "unlocked": unlocked,
                "unlocked_at": unlocked_at_by_code.get(code),
                "remaining": 0 if unlocked else max(0, threshold - training_count),
            }
        )

    nickname = user_row.get("nickname") or auth["user"].get("first_name") or ""
    referral_link = (
        f"https://t.me/{bot_username}?start={telegram_user_id}" if bot_username else None
    )

    return web.json_response(
        {
            "nickname": nickname,
            "status": status_title,
            "hp_total": int(stats.get("hp_total", 0) or 0),
            "streak": int(stats.get("streak", 0) or 0),
            "streak_max": int(stats.get("weekly_streak_max", 0) or 0),
            "gym_count": gym_count,
            "street_count": street_count,
            "rest_count": rest_count,
            "skip_count": skip_count,
            "activities_count": activities_count,
            "referrals_count": referrals_count,
            "achievements_count": len(unlocked_codes),
            "last_achievement_title": last_achievement_title,
            "next_goal_text": next_goal_text,
            "training_count": training_count,
            "group_link": GROUP_LINK,
            "referral_link": referral_link,
            "achievements": achievements_payload,
            "calendar": {
                "year": year,
                "month": month,
                "days": calendar_days,
            },
        }
    )


async def handle_rating(request: web.Request) -> web.Response:
    auth = verify_init_data(_extract_init_data(request), BOT_TOKEN)
    if not auth:
        return web.json_response({"error": "unauthorized"}, status=401)

    telegram_user_id = auth["telegram_user_id"]
    rows = await get_all_time_rating()

    top = [
        {
            "telegram_user_id": row.get("telegram_user_id"),
            "nick": row.get("nick") or f"ID:{row.get('telegram_user_id', 'unknown')}",
            "hp": int(row.get("hp", 0) or 0),
            "referrals_count": int(row.get("referrals_count", 0) or 0),
            "rank": int(row.get("rank", 0) or 0),
        }
        for row in rows[:5]
    ]

    me = None
    for row in rows:
        if int(row.get("telegram_user_id") or 0) == int(telegram_user_id):
            me = {
                "telegram_user_id": row.get("telegram_user_id"),
                "nick": row.get("nick") or f"ID:{telegram_user_id}",
                "hp": int(row.get("hp", 0) or 0),
                "referrals_count": int(row.get("referrals_count", 0) or 0),
                "rank": int(row.get("rank", 0) or 0),
            }
            break

    return web.json_response(
        {
            "top": top,
            "me": me,
            "total_users": len(rows),
        }
    )


async def handle_feed(request: web.Request) -> web.Response:
    auth = verify_init_data(_extract_init_data(request), BOT_TOKEN)
    if not auth:
        return web.json_response({"error": "unauthorized"}, status=401)

    user_row = await get_user_by_telegram_id(auth["telegram_user_id"])
    if not user_row:
        return web.json_response({"error": "profile not found"}, status=404)

    user_uuid = str(user_row.get("id") or "")
    if not user_uuid:
        return web.json_response({"error": "profile not found"}, status=404)

    items = await get_user_feed(user_uuid, limit=20)
    return web.json_response({"items": items})


def create_webapp_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/api/profile", handle_profile)
    app.router.add_get("/api/rating", handle_rating)
    app.router.add_get("/api/feed", handle_feed)
    app.router.add_route("OPTIONS", "/api/profile", lambda request: web.Response())
    app.router.add_route("OPTIONS", "/api/rating", lambda request: web.Response())
    app.router.add_route("OPTIONS", "/api/feed", lambda request: web.Response())
    return app


async def run_webapp_server(port: int) -> web.AppRunner:
    app = create_webapp_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"🌐 [WEBAPP] Profile API listening on 0.0.0.0:{port}")
    return runner
