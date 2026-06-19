import logging
from datetime import datetime, timedelta
from html import escape
from typing import Any, Optional

import pytz

from database import get_kyiv_now, update_user_activity
from supabase_db import (
    get_user_by_telegram_id,
    get_user_activities,
)

logger = logging.getLogger(__name__)

KYIV_TZ = pytz.timezone("Europe/Kyiv")

# ==============================================================================
# WEEKLY CHALLENGE CONFIG
# ==============================================================================

CHALLENGE_ACTION = "Weekly Challenge"
CHALLENGE_HP = 50
CHALLENGE_WEEKLY_TARGET = 5
CHALLENGE_MAX_PER_DAY = 1

# Міняєш цей текст щотижня — і все.
CHALLENGE_TITLE = "🔥 ЧЕЛЕНДЖ ТИЖНЯ"
CHALLENGE_NAME = "Прес"
CHALLENGE_DESCRIPTION = (
    "Завдання: зроби будь-яку вправу на прес <b>5 разів за тиждень</b>.\n\n"
    "Це можуть бути скручування, підйоми ніг, планка, велосипед або будь-яка інша вправа на м'язи живота.\n\n"
    "Кількість повторень не важлива — головне виконати вправу та скинути відео.\n\n"
    "✅ 1 виконання = <b>+50 HP</b>\n"
    "📅 Ліміт: <b>1 раз на день</b>\n"
    "🏆 Повний челендж: <b>5/5 = 250 HP</b>\n\n"
    "Прокачай свій прес, дисципліну та витривалість.\n\n"
    "Щоб здати челендж — натисни кнопку <b>✅ Здати челендж</b> "
    "і скинь свіже відео/кружечок у бот."
)

# ==============================================================================
# DATE HELPERS
# ==============================================================================

def parse_activity_created_at(value: Any) -> Optional[datetime]:
    """
    Safe parser for Supabase/Postgres created_at.
    Returns Kyiv-aware datetime or None.

    Supports:
    - datetime objects
    - 2026-05-26T17:20:29.69094+00:00
    - 2026-05-26T17:20:29.690940+00:00
    - 2026-05-26T17:20:29Z
    - 2026-05-26 17:20:29.69094+00
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        normalized = raw.replace(" ", "T").replace("Z", "+00:00")

        if normalized.endswith("+00"):
            normalized = normalized[:-3] + "+00:00"

        try:
            if "." in normalized:
                date_part, tail = normalized.split(".", 1)

                tz_part = ""
                fraction_part = tail

                plus_index = tail.find("+")
                minus_index = tail.find("-", 1)

                if plus_index != -1:
                    fraction_part = tail[:plus_index]
                    tz_part = tail[plus_index:]
                elif minus_index != -1:
                    fraction_part = tail[:minus_index]
                    tz_part = tail[minus_index:]

                digits = "".join(ch for ch in fraction_part if ch.isdigit())
                digits = digits[:6].ljust(6, "0")

                normalized = f"{date_part}.{digits}{tz_part}"

            dt = datetime.fromisoformat(normalized)

        except Exception as e:
            logger.error(
                "[CHALLENGE] Failed to parse created_at=%r normalized=%r error=%s",
                value,
                normalized,
                e,
                exc_info=True,
            )
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)

    return dt.astimezone(KYIV_TZ)


def get_current_turbo_week_period() -> tuple[datetime, datetime]:
    """
    TurboTeam week:
    Sunday 20:00 Kyiv -> next Sunday 20:00 Kyiv.
    Same logic as rating/streak.
    """
    now = get_kyiv_now()
    current_sunday_20 = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=20,
        minute=0,
        second=0,
        microsecond=0,
    )

    if now < current_sunday_20:
        week_start = current_sunday_20 - timedelta(days=7)
    else:
        week_start = current_sunday_20

    week_end = week_start + timedelta(days=7)
    return week_start, week_end


# ==============================================================================
# TEXT / UI HELPERS
# ==============================================================================

def build_progress_bar(current: int, target: int, width: int = 10) -> str:
    current = max(0, int(current or 0))
    target = max(1, int(target or 1))
    width = max(5, int(width or 10))

    ratio = min(current, target) / target
    filled = int(ratio * width)

    if current > 0 and filled == 0:
        filled = 1

    filled = min(width, max(0, filled))
    empty = width - filled

    return "█" * filled + "░" * empty


def build_challenge_text(
    weekly_count: int = 0,
    done_today: bool = False,
) -> str:
    weekly_count = max(0, min(int(weekly_count or 0), CHALLENGE_WEEKLY_TARGET))
    left = max(0, CHALLENGE_WEEKLY_TARGET - weekly_count)
    total_hp = weekly_count * CHALLENGE_HP
    bar = build_progress_bar(weekly_count, CHALLENGE_WEEKLY_TARGET, width=10)

    today_status = "✅ Сьогодні вже здано" if done_today else "⏳ Сьогодні ще можна здати"

    return (
        f"{CHALLENGE_TITLE}\n\n"
        f"<b>{escape(CHALLENGE_NAME)}</b>\n\n"
        f"{CHALLENGE_DESCRIPTION}\n\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"📊 <b>Твій прогрес цього тижня</b>\n"
        f"{bar} <b>{weekly_count}/{CHALLENGE_WEEKLY_TARGET}</b>\n"
        f"⚡ Уже зароблено: <b>{total_hp} HP</b>\n"
        f"🎯 До повного челенджу: <b>{left}</b>\n"
        f"{today_status}"
    )


def build_challenge_group_report_text(
    nickname: str,
    hp: int,
    weekly_count: int,
) -> str:
    weekly_count = max(0, min(int(weekly_count or 0), CHALLENGE_WEEKLY_TARGET))
    left = max(0, CHALLENGE_WEEKLY_TARGET - weekly_count)
    bar = build_progress_bar(weekly_count, CHALLENGE_WEEKLY_TARGET, width=10)

    nickname_html = escape(str(nickname))

    if left > 0:
        finish_line = f"До повного челенджу: <b>{left}</b>"
    else:
        finish_line = "🏆 Повний челендж тижня закрито!"

    return (
        f"🔥 <b>ЧЕЛЕНДЖ ЗАРАХОВАНО!</b>\n\n"
        f"{nickname_html} виконав завдання дня.\n"
        f"+<b>{int(hp)}</b> HP\n\n"
        f"📊 <b>Прогрес тижня</b>\n"
        f"{bar} <b>{weekly_count}/{CHALLENGE_WEEKLY_TARGET}</b>\n"
        f"{finish_line}"
    )


# ==============================================================================
# CORE LOGIC
# ==============================================================================

async def get_challenge_stats(user_id: int) -> dict[str, Any]:
    """
    Returns:
    - weekly_count: how many Weekly Challenge actions this Turbo-week
    - done_today: whether user already did challenge today
    - can_submit: True if user can submit now
    """
    user_row = await get_user_by_telegram_id(user_id)
    if not user_row:
        return {
            "registered": False,
            "weekly_count": 0,
            "done_today": False,
            "can_submit": False,
        }

    user_uuid = user_row.get("id")
    if not user_uuid:
        return {
            "registered": False,
            "weekly_count": 0,
            "done_today": False,
            "can_submit": False,
        }

    activities = await get_user_activities(str(user_uuid), limit=1000)

    today = get_kyiv_now().date()
    week_start, week_end = get_current_turbo_week_period()

    weekly_count = 0
    done_today = False

    for activity in activities:
        action_name = str(activity.get("action_name") or "").strip()
        if action_name != CHALLENGE_ACTION:
            continue

        created_at = parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue

        if week_start <= created_at < week_end:
            weekly_count += 1

        if created_at.date() == today:
            done_today = True

    weekly_count = min(weekly_count, CHALLENGE_WEEKLY_TARGET)

    return {
        "registered": True,
        "weekly_count": weekly_count,
        "done_today": done_today,
        "can_submit": not done_today,
    }


async def can_submit_challenge_today(user_id: int) -> bool:
    stats = await get_challenge_stats(user_id)
    return bool(stats.get("registered") and stats.get("can_submit"))


async def submit_weekly_challenge(
    user_id: int,
    nickname: str,
    video_id: str = "",
) -> dict[str, Any]:
    """
    Writes Weekly Challenge +50 HP.
    Does NOT affect Gym/Street/streak.

    Returns dict:
    {
      ok: bool,
      reason: str,
      hp: int,
      weekly_count: int,
      done_today: bool,
    }
    """
    stats_before = await get_challenge_stats(user_id)

    if not stats_before.get("registered"):
        return {
            "ok": False,
            "reason": "not_registered",
            "hp": 0,
            "weekly_count": 0,
            "done_today": False,
        }

    if stats_before.get("done_today"):
        return {
            "ok": False,
            "reason": "already_done_today",
            "hp": 0,
            "weekly_count": int(stats_before.get("weekly_count", 0) or 0),
            "done_today": True,
        }

    result = await update_user_activity(
        user_id=user_id,
        nickname=nickname,
        action_name=CHALLENGE_ACTION,
        hp_change=CHALLENGE_HP,
        video_id=f"challenge:{video_id}" if video_id else "challenge:no_video",
        is_check=False,
        skip_lock=False,
    )

    if result is not True:
        return {
            "ok": False,
            "reason": str(result),
            "hp": 0,
            "weekly_count": int(stats_before.get("weekly_count", 0) or 0),
            "done_today": bool(stats_before.get("done_today")),
        }

    stats_after = await get_challenge_stats(user_id)

    return {
        "ok": True,
        "reason": "submitted",
        "hp": CHALLENGE_HP,
        "weekly_count": int(stats_after.get("weekly_count", 0) or 0),
        "done_today": True,
    }