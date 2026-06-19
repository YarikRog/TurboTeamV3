import asyncio
import logging
import time
from html import escape
from datetime import datetime, timedelta

import pytz
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from architecture.events import REST_SELECTED, SKIP_SELECTED, VIDEO_UPLOADED
from architecture.events import EventEnvelope
from architecture.orchestrator import flow_event_bus
from config import ADMIN_IDS, REPORTS_GROUP_ID, GROUP_LINK
from cache import get_data, set_flag, delete_data, KeyManager
from database import get_user_stats, check_user_exists, update_user_activity
from referral import send_invite_prompt
from ratings import show_rating_for_user
from reports import rollback_training_report
from services import safe_create_task, auto_delete, validate_quiz
from supabase_db import (
    get_user_by_telegram_id,
    get_user_activities,
    get_referrals_count,
    get_user_achievements,
    get_all_users,
    get_all_activities,
    get_all_activities_in_period,
    get_referrals_in_period,
)

router = Router()
logger = logging.getLogger(__name__)

KYIV_TZ = pytz.timezone("Europe/Kyiv")

ADMIN_HELP_TTL = 120

REAL_ACTIVITY_ACTIONS = {
    "Gym",
    "Street",
    "Rest",
    "Skipped",
    "Welcome Bonus",
    "Returned",
    "Weekly Challenge",
}

TRAINING_ACTIONS = {"Gym", "Street"}
WEEKLY_STREAK_MAX = 14

TRAINING_STATUS_LEVELS = [
    (1, "Новачок"),
    (5, "Вкатався"),
    (10, "Боєць"),
    (25, "Стабільний"),
    (50, "Мотор"),
    (100, "Турбо"),
    (200, "Машина"),
    (350, "Термінатор"),
    (500, "Монстр"),
    (1000, "Легенда TurboTeam"),
]

TRAINING_GOALS = [1, 3, 5, 10, 15, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000]

TRAINING_ACHIEVEMENTS = [
    (1, "training_1", "Перший крок"),
    (3, "training_3", "Втягнувся"),
    (5, "training_5", "Розігрів"),
    (10, "training_10", "Перша десятка"),
    (15, "training_15", "Стабільність"),
    (25, "training_25", "У ритмі"),
    (50, "training_50", "Півсотні"),
    (75, "training_75", "На повну"),
    (100, "training_100", "Сотка"),
    (150, "training_150", "Залізна дисципліна"),
    (200, "training_200", "Машина"),
    (300, "training_300", "Турбо-режим"),
    (500, "training_500", "Монстр"),
    (750, "training_750", "Невгамовний"),
    (1000, "training_1000", "Легенда TurboTeam"),
]


def _build_progress_bar(current: int, target: int, width: int = 10) -> str:
    current = max(0, int(current or 0))
    target = max(1, int(target or 1))
    width = max(5, int(width or 10))

    filled = round((min(current, target) / target) * width)
    filled = min(width, max(0, filled))
    empty = width - filled

    return "█" * filled + "░" * empty


def _count_training_from_activities(activities: list[dict]) -> int:
    training_count = 0
    rollback_count = 0

    for activity in activities:
        action_name = str(activity.get("action_name") or "").strip()

        if action_name in {"Gym", "Street"}:
            training_count += 1
        elif action_name in {"Gym Rollback", "Street Rollback"}:
            rollback_count += 1

    return max(0, training_count - rollback_count)


def _build_achievements_text(
    nickname: str,
    training_count: int,
    unlocked_codes: set[str],
    hp_total: int,
    streak: int,
    referrals_count: int,
) -> str:
    total = len(TRAINING_ACHIEVEMENTS)
    unlocked_count = sum(
        1
        for threshold, code, title in TRAINING_ACHIEVEMENTS
        if code in unlocked_codes or training_count >= threshold
    )

    percent = round((unlocked_count / total) * 100) if total > 0 else 0
    bar = _build_progress_bar(unlocked_count, total, width=10)

    lines = [
        "🏅 <b>ДОСЯГНЕННЯ TURBOTEAM</b>",
        "",
        f"👤 Гравець: <b>{escape(str(nickname))}</b>",
        f"⚡ HP: <b>{int(hp_total)}</b>",
        f"🔥 Streak: <b>{int(streak)}/{WEEKLY_STREAK_MAX}</b>",
        f"🚀 Реферали: <b>{int(referrals_count)}</b>",
        "",
        f"📦 Відкрито: <b>{unlocked_count}/{total}</b> — <b>{percent}%</b>",
        f"{bar}",
        "",
        "🏋️ <b>Тренувальні бейджі</b>",
    ]

    for threshold, code, title in TRAINING_ACHIEVEMENTS:
        if code in unlocked_codes or training_count >= threshold:
            lines.append(f"✅ <b>{escape(title)}</b> — {threshold} {word_trainings(threshold)}")
        else:
            left = max(0, threshold - training_count)
            lines.append(f"🔒 {escape(title)} — ще <b>{left}</b>")

    next_locked = None
    for threshold, code, title in TRAINING_ACHIEVEMENTS:
        if code not in unlocked_codes and training_count < threshold:
            next_locked = (threshold, title, threshold - training_count)
            break

    lines.append("")

    if next_locked:
        threshold, title, left = next_locked
        lines.append("🎯 <b>Найближча ціль</b>")
        lines.append(f"До “{escape(title)}” — ще <b>{left}</b> {word_trainings(left)}")
    else:
        lines.append("🏆 Усі базові тренувальні досягнення відкрито. Це вже режим легенди.")

    return "\n".join(lines)


AUTO_REMOVE_REDIS_PREFIX = "turbo:auto_removed"
LAST_WARNING_REDIS_PREFIX = "turbo:last_warning"


def get_training_status(training_count: int) -> str:
    status = "Без статусу"

    for threshold, title in TRAINING_STATUS_LEVELS:
        if training_count >= threshold:
            status = title
        else:
            break

    return status


def get_next_training_goal(training_count: int) -> tuple[int | None, str]:
    for goal in TRAINING_GOALS:
        if training_count < goal:
            return goal, f"{training_count}/{goal}"

    return None, "MAX"


def _get_current_week_period() -> tuple[datetime, datetime]:
    now = datetime.now(KYIV_TZ)
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


def _get_last_finished_week_period() -> tuple[datetime, datetime]:
    now = datetime.now(KYIV_TZ)
    current_sunday_20 = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=20,
        minute=0,
        second=0,
        microsecond=0,
    )

    if now >= current_sunday_20:
        week_end = current_sunday_20
    else:
        week_end = current_sunday_20 - timedelta(days=7)

    week_start = week_end - timedelta(days=7)
    return week_start, week_end


def _get_previous_finished_week_period() -> tuple[datetime, datetime]:
    last_week_start, _ = _get_last_finished_week_period()
    previous_week_end = last_week_start
    previous_week_start = previous_week_end - timedelta(days=7)

    return previous_week_start, previous_week_end


def _format_period(dt: datetime) -> str:
    return dt.strftime("%d.%m %H:%M")


def _calc_percent(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100, 1)


def _count_values(users: list[dict], field_name: str, allowed_values: list[str]) -> dict[str, int]:
    result = {value: 0 for value in allowed_values}

    for user in users:
        raw_value = str(user.get(field_name) or "").strip()
        if raw_value in result:
            result[raw_value] += 1

    return result


def _count_filled(users: list[dict], field_name: str) -> int:
    total = 0
    for user in users:
        raw_value = str(user.get(field_name) or "").strip()
        if raw_value:
            total += 1
    return total


def _word_users(count: int) -> str:
    return "юзер" if count == 1 else "юзери" if 2 <= count <= 4 else "юзерів"


def _word_actions(count: int) -> str:
    return "дія" if count == 1 else "дії" if 2 <= count <= 4 else "дій"


def _word_days(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "день"

    if 2 <= count % 10 <= 4 and not (12 <= count % 100 <= 14):
        return "дні"

    return "днів"


def word_trainings(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "тренування"

    if 2 <= count % 10 <= 4 and not (12 <= count % 100 <= 14):
        return "тренування"

    return "тренувань"


def _format_delta(current_value, previous_value, suffix: str = "") -> str:
    try:
        diff = current_value - previous_value
    except Exception:
        return "—"

    if isinstance(diff, float):
        diff_text = f"{diff:+.1f}"
    else:
        diff_text = f"{int(diff):+d}"

    if suffix:
        return f"{diff_text}{suffix}"

    return diff_text


def _get_auto_removed_key(user_id: int) -> str:
    return f"{AUTO_REMOVE_REDIS_PREFIX}:{user_id}"


def _get_last_warning_key(user_id: int) -> str:
    return f"{LAST_WARNING_REDIS_PREFIX}:{user_id}"


async def _is_user_in_group_for_stats(bot, user_id: int) -> bool:
    """
    Returns True if Telegram says the user is currently in REPORTS_GROUP_ID.
    Counts active members.
    Skips left / kicked / inaccessible users.
    """
    try:
        member = await bot.get_chat_member(REPORTS_GROUP_ID, user_id)

        status_raw = getattr(member, "status", "")
        status_value = getattr(status_raw, "value", None)

        if status_value:
            status = str(status_value).lower().strip()
        else:
            status = str(status_raw).lower().strip()

        if status in {"left", "kicked"}:
            return False

        if status.endswith(".left") or status.endswith(".kicked"):
            return False

        if status in {
            "member",
            "administrator",
            "creator",
            "owner",
            "chatmemberstatus.member",
            "chatmemberstatus.administrator",
            "chatmemberstatus.creator",
            "chatmemberstatus.owner",
        }:
            return True

        logger.info(
            "[STATS] unknown chat member status: user_id=%s status=%s raw=%r",
            user_id,
            status,
            status_raw,
        )
        return False

    except Exception as e:
        logger.info(
            "[STATS] user is not available in group: user_id=%s error=%s",
            user_id,
            e,
        )
        return False


async def _filter_users_currently_in_group(bot, users: list[dict]) -> list[dict]:
    """
    Filters Supabase users by real Telegram group membership.
    """
    result = []

    for user in users:
        telegram_user_id = user.get("telegram_user_id")

        if not telegram_user_id:
            continue

        try:
            user_id = int(telegram_user_id)
        except Exception:
            continue

        if await _is_user_in_group_for_stats(bot, user_id):
            result.append(user)

    logger.info(
        "[STATS] group member filter: supabase_users=%s active_group_users=%s",
        len(users),
        len(result),
    )

    return result
    logger.info(
        "[STATS] group member filter: supabase_users=%s active_group_users=%s",
        len(users),
        len(result),
    )

    return result


def _build_promo_stats_text(data: dict) -> str:
    champion = escape(str(data["champion_nickname"]))
    turbo_index = int(data["turbo_index"])
    turbo_level = escape(str(data["turbo_level"]))

    return (
        f"🔥 <b>TURBOTEAM WEEKLY IMPACT</b>\n\n"
        f"🔥 <b>Turbo Index: {turbo_index}/100</b>\n"
        f"Рівень залучення: <b>{turbo_level}</b>\n\n"
        f"📅 Період:\n"
        f"{_format_period(data['week_start'])} — {_format_period(data['week_end'])}\n\n"
        f"👥 Учасників у групі: <b>{data['total_users']}</b>\n"
        f"⚡ Активних: <b>{data['active_users']}</b> із <b>{data['total_users']}</b> — <b>{data['active_percent']}%</b>\n"
        f"🏋️ Підтверджених тренувань: <b>{data['training_count']}</b>\n"
        f"📹 Відео-звітів: <b>{data['video_reports']}</b>\n"
        f"🔁 Реферальних переходів: <b>{data['referrals_count']}</b>\n"
        f"🏆 HP видано: <b>{data['hp_total']}</b>\n"
        f"🔥 Середня активність: <b>{data['avg_activity']}</b> дії на активного учасника\n"
        f"🥇 Чемпіон тижня: <b>@{champion}</b> — <b>{data['champion_hp']} HP</b>\n\n"
        f"TurboTeam перетворює чат на гру: люди тренуються, звітують, змагаються і повертаються."
    )


def _build_promo_compare_text(current: dict, previous: dict) -> str:
    current_text = _build_promo_stats_text(current)
    previous_champion = escape(str(previous["champion_nickname"]))

    compare_text = (
        f"\n\n━━━━━━━━━━━━━━\n\n"
        f"📊 <b>ПОРІВНЯННЯ З МИНУЛИМ ТИЖНЕМ</b>\n\n"
        f"📅 Минулий період:\n"
        f"{_format_period(previous['week_start'])} — {_format_period(previous['week_end'])}\n\n"
        f"🔥 Turbo Index: <b>{previous['turbo_index']}</b> → <b>{current['turbo_index']}</b> "
        f"({_format_delta(current['turbo_index'], previous['turbo_index'])})\n"
        f"⚡ Активних: <b>{previous['active_users']}</b> → <b>{current['active_users']}</b> "
        f"({_format_delta(current['active_users'], previous['active_users'])})\n"
        f"🏋️ Тренувань: <b>{previous['training_count']}</b> → <b>{current['training_count']}</b> "
        f"({_format_delta(current['training_count'], previous['training_count'])})\n"
        f"🔁 Рефералів: <b>{previous['referrals_count']}</b> → <b>{current['referrals_count']}</b> "
        f"({_format_delta(current['referrals_count'], previous['referrals_count'])})\n"
        f"🏆 HP: <b>{previous['hp_total']}</b> → <b>{current['hp_total']}</b> "
        f"({_format_delta(current['hp_total'], previous['hp_total'])})\n"
        f"🔥 Середня активність: <b>{previous['avg_activity']}</b> → <b>{current['avg_activity']}</b> "
        f"({_format_delta(float(current['avg_activity']), float(previous['avg_activity']))})\n\n"
        f"🥇 Чемпіон минулого тижня: <b>@{previous_champion}</b> — <b>{previous['champion_hp']} HP</b>"
    )

    if int(current["turbo_index"]) > int(previous["turbo_index"]):
        conclusion = "Висновок: тиждень сильніший за попередній. Є ріст, банда прокидається 🔥"
    elif int(current["turbo_index"]) < int(previous["turbo_index"]):
        conclusion = "Висновок: тиждень просів. Треба підсилювати мотивацію й повертати людей у гру 👀"
    else:
        conclusion = "Висновок: рівень тримається стабільно. Тепер задача — зробити наступний тиждень сильнішим 🏎️"

    return current_text + compare_text + "\n\n" + conclusion


def _build_promo_compare_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 Минулий тиждень",
                    callback_data="promo_compare_previous",
                )
            ]
        ]
    )


def _format_stat_block(
    title: str,
    stats: dict[str, int],
    total: int,
    ordered_items: list[tuple[str, str]],
) -> str:
    lines = [title]

    for value_key, display_label in ordered_items:
        count = int(stats.get(value_key, 0) or 0)
        percent = _calc_percent(count, total)
        lines.append(f"{display_label} — <b>{count}</b> {_word_users(count)} — <b>{percent}%</b>")

    return "\n".join(lines)


def _build_admin_help_text() -> str:
    return (
        "🛠️ <b>АДМІН-КОМАНДИ TURBOTEAM</b>\n\n"
        "📋 <b>ОСНОВНІ</b>\n"
        "/adminhelp — список усіх адмін-команд\n"
        "/panel — відкрити Turbo-панель\n"
        "/menu — вивести Turbo-меню в групі\n"
        "/rating — показати рейтинг\n"
        "/reject — скасувати тренування через reply\n"
        "/quizstats — статистика квізу\n"
        "/activitystats — статистика активностей\n"
        "/promostats — короткий рекламний звіт\n"
        "/impactstats — повний impact-звіт\n\n"
        "🧪 <b>ТЕСТИ</b>\n"
        "/testaward — тестова FIFA-картка\n"
        "/testref — тест реферального повідомлення\n"
        "/loadtest 50 — безпечний тест паралельного навантаження\n\n"
        "🧹 <b>АДМІН-ДІЇ</b>\n"
        "/wipeuser 123456789 — видалити юзера за Telegram ID\n"
        "/wipeuser @username — видалити юзера за ніком\n\n"
        "📘 <b>ІНШЕ</b>\n"
        "/rules — текст правил"
    )


def _is_real_activity(activity: dict) -> bool:
    action_name = str(activity.get("action_name") or "").strip()
    return action_name in REAL_ACTIVITY_ACTIONS


def _is_training_activity(activity: dict) -> bool:
    action_name = str(activity.get("action_name") or "").strip()
    return action_name in TRAINING_ACTIONS


def _build_activity_counter(activities: list[dict]) -> dict[str, int]:
    result = {
        "Gym": 0,
        "Street": 0,
        "Rest": 0,
        "Skipped": 0,
        "Welcome Bonus": 0,
        "Returned": 0,
        "Weekly Challenge": 0,
    }

    for activity in activities:
        action_name = str(activity.get("action_name") or "").strip()
        if action_name in result:
            result[action_name] += 1

    return result


def _sum_hp(activities: list[dict]) -> int:
    total = 0

    for activity in activities:
        if not _is_real_activity(activity):
            continue

        try:
            total += int(activity.get("hp_change") or 0)
        except Exception:
            continue

    return total


def _count_active_users(activities: list[dict]) -> int:
    user_ids = set()

    for activity in activities:
        if not _is_real_activity(activity):
            continue

        user_id = str(activity.get("user_id") or "").strip()
        if user_id:
            user_ids.add(user_id)

    return len(user_ids)


def _calculate_turbo_index(
    total_users: int,
    active_users: int,
    training_count: int,
    video_reports: int,
    referrals_count: int,
    avg_activity: float,
) -> tuple[int, str]:
    active_score = _calc_percent(active_users, total_users)

    training_target = max(active_users * 3, 1)
    training_score = min(100.0, round((training_count / training_target) * 100, 1))

    if training_count > 0:
        video_score = min(100.0, round((video_reports / training_count) * 100, 1))
    else:
        video_score = 0.0

    referral_target = max(round(total_users * 0.2), 1)
    referral_score = min(100.0, round((referrals_count / referral_target) * 100, 1))

    avg_score = min(100.0, round((avg_activity / 5) * 100, 1))

    turbo_index = round(
        active_score * 0.40
        + training_score * 0.30
        + video_score * 0.15
        + referral_score * 0.10
        + avg_score * 0.05
    )

    if turbo_index >= 80:
        level = "ДУЖЕ ВИСОКИЙ 🔥"
    elif turbo_index >= 60:
        level = "ВИСОКИЙ 🔥"
    elif turbo_index >= 40:
        level = "СЕРЕДНІЙ ⚡"
    elif turbo_index > 0:
        level = "НИЗЬКИЙ 💤"
    else:
        level = "СТАРТ ТИЖНЯ 🏁"

    return turbo_index, level


async def _build_weekly_impact_data(
    bot,
    finished_week: bool = True,
    previous_week: bool = False,
) -> dict:
    all_users = await get_all_users()
    users = await _filter_users_currently_in_group(bot, all_users)
    total_users = len(users)

    current_group_user_ids = {
        str(user.get("id"))
        for user in users
        if user.get("id")
    }

    if previous_week:
        week_start, week_end = _get_previous_finished_week_period()
    elif finished_week:
        week_start, week_end = _get_last_finished_week_period()
    else:
        week_start, week_end = _get_current_week_period()
    activities_raw = await get_all_activities_in_period(
        created_at_from=week_start.isoformat(),
        created_at_to=week_end.isoformat(),
        limit=5000,
    )

    referrals_raw = await get_referrals_in_period(
        created_at_from=week_start.isoformat(),
        created_at_to=week_end.isoformat(),
        limit=5000,
    )

    real_activities = [
        activity for activity in activities_raw
        if _is_real_activity(activity)
        and str(activity.get("user_id") or "").strip() in current_group_user_ids
    ]

    training_activities = [
        activity for activity in real_activities
        if _is_training_activity(activity)
    ]

    counts = _build_activity_counter(real_activities)

    active_users = _count_active_users(real_activities)
    real_total = len(real_activities)
    training_count = len(training_activities)
    video_reports = training_count
    referrals_count = len(referrals_raw)
    hp_total = _sum_hp(real_activities)

    avg_activity = 0.0
    if active_users > 0:
        avg_activity = round(real_total / active_users, 1)

    active_percent = _calc_percent(active_users, total_users)

    user_map = {
        str(user.get("id")): user
        for user in users
        if user.get("id")
    }

    hp_by_user: dict[str, int] = {}
    for activity in real_activities:
        user_uuid = str(activity.get("user_id") or "").strip()
        if not user_uuid:
            continue

        try:
            hp_change = int(activity.get("hp_change") or 0)
        except Exception:
            hp_change = 0

        hp_by_user[user_uuid] = hp_by_user.get(user_uuid, 0) + hp_change

    champion_nickname = "Немає"
    champion_hp = 0

    if hp_by_user:
        champion_uuid, champion_hp = max(hp_by_user.items(), key=lambda item: item[1])
        champion_row = user_map.get(champion_uuid, {})
        champion_nickname = (
            champion_row.get("nickname")
            or champion_row.get("telegram_user_id")
            or "Невідомий"
        )

    turbo_index, turbo_level = _calculate_turbo_index(
        total_users=total_users,
        active_users=active_users,
        training_count=training_count,
        video_reports=video_reports,
        referrals_count=referrals_count,
        avg_activity=avg_activity,
    )

    return {
        "week_start": week_start,
        "week_end": week_end,
        "total_users": total_users,
        "active_users": active_users,
        "active_percent": active_percent,
        "real_total": real_total,
        "training_count": training_count,
        "video_reports": video_reports,
        "referrals_count": referrals_count,
        "hp_total": hp_total,
        "avg_activity": avg_activity,
        "champion_nickname": str(champion_nickname),
        "champion_hp": int(champion_hp),
        "turbo_index": turbo_index,
        "turbo_level": turbo_level,
        "counts": counts,
    }


async def _run_single_load_job(job_id: int) -> dict:
    started = time.perf_counter()

    quiz_data = {
        "gender": "Чоловік",
        "level": "Новачок",
        "goal": "Схуднення",
        "weekly_plan": "3-4 рази",
        "training_place": "І там, і там",
    }

    test_uid = 900000000 + job_id

    try:
        quiz_ok = validate_quiz(quiz_data)
        if not quiz_ok:
            return {
                "ok": False,
                "job_id": job_id,
                "duration_ms": round((time.perf_counter() - started) * 1000, 1),
                "error": "quiz validation failed",
            }

        redis_key = KeyManager.get_profile_warn_key(test_uid)
        await set_flag(redis_key, ex=60)
        redis_value = await get_data(redis_key)

        exists = await check_user_exists(test_uid)

        dummy_users = [
            {"level": "Новачок", "goal": "Схуднення", "weekly_plan": "1-2 рази"},
            {"level": "Середній", "goal": "Набір маси", "weekly_plan": "3-4 рази"},
            {"level": "Профі", "goal": "Витривалість", "weekly_plan": "5+ разів"},
        ]

        level_stats = _count_values(dummy_users, "level", ["Новачок", "Середній", "Профі"])
        total_level = _count_filled(dummy_users, "level")
        _ = _format_stat_block(
            "TEST",
            level_stats,
            total_level,
            [("Новачок", "Новачок"), ("Середній", "Середній"), ("Профі", "Профі")],
        )

        await asyncio.sleep(0)

        return {
            "ok": redis_value is not None and exists is False,
            "job_id": job_id,
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
        }

    except Exception as e:
        logger.error(f"[LOADTEST2] job failed: job_id={job_id}, error={e}", exc_info=True)
        return {
            "ok": False,
            "job_id": job_id,
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": str(e),
        }


async def _run_loadtest_batch(total_jobs: int) -> dict:
    started = time.perf_counter()

    tasks = [
        asyncio.create_task(_run_single_load_job(i + 1))
        for i in range(total_jobs)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=False)

    success_count = sum(1 for item in results if item.get("ok"))
    fail_count = total_jobs - success_count
    max_ms = max((item.get("duration_ms", 0) for item in results), default=0)
    min_ms = min((item.get("duration_ms", 0) for item in results), default=0)
    avg_ms = round(
        sum(item.get("duration_ms", 0) for item in results) / total_jobs,
        1,
    ) if total_jobs > 0 else 0.0

    return {
        "total_jobs": total_jobs,
        "success_count": success_count,
        "fail_count": fail_count,
        "total_duration_s": round(time.perf_counter() - started, 2),
        "min_ms": min_ms,
        "max_ms": max_ms,
        "avg_ms": avg_ms,
    }


@router.message(F.new_chat_members)
async def handle_new_chat_members(message: Message):
    if message.chat.id != REPORTS_GROUP_ID:
        return

    # Delete Telegram system "user joined the group" message.
    # NOTE: this must live in the same handler as the return/unban logic below.
    # If join-message deletion is registered as a separate dispatcher-level
    # handler, it consumes the new_chat_members event first and stops
    # propagation, so the Returned-activity / unban-cleanup logic never runs.
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"[JOIN] Failed to delete join message: {e}")

    for member in message.new_chat_members:
        if member.is_bot:
            continue

        user_id = int(member.id)
        removed_key = _get_auto_removed_key(user_id)
        removed_payload = await get_data(removed_key)

        name_raw = f"@{member.username}" if member.username else (member.full_name or "Учасник")
        name_html = escape(str(name_raw))

        # Only registered users have a Supabase row. Writing "Returned" for a
        # never-registered joiner would just spin update_user_activity through
        # its full retry/backoff cycle (~10s) and write nothing.
        # check_user_exists caches the uuid, so the write below hits Redis, not Supabase again.
        is_registered = await check_user_exists(user_id)

        if is_registered:
            try:
                await update_user_activity(
                    user_id=user_id,
                    nickname=str(name_raw),
                    action_name="Returned",
                    hp_change=0,
                    video_id=f"returned:{user_id}:{message.message_id}",
                    is_check=False,
                    skip_lock=True,
                )
            except Exception as e:
                logger.error(
                    "[HANDLERS] failed to write Returned activity user_id=%s error=%s",
                    user_id,
                    e,
                    exc_info=True,
                )
        else:
            logger.info(
                "[HANDLERS] new member not registered, skipping Returned write user_id=%s",
                user_id,
            )

        if removed_payload is None:
            continue

        await delete_data(removed_key)
        await delete_data(_get_last_warning_key(user_id))

        try:
            await message.answer(
                f"🏎️ <b>{name_html}</b> повернувся в TurboTeam.\n"
                f"Доступ відновлено. Тепер головне — не випадати з гри 🔥",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(
                f"[HANDLERS] failed to send return message user_id={user_id}: {e}",
                exc_info=True,
            )


@router.message(F.text == "🏆 Рейтинг ТОП")
async def handle_show_rating_message(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(Command("rating"))
async def handle_show_rating_command(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(Command("adminhelp"))
async def handle_admin_help(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        try:
            await message.delete()
        except Exception:
            pass

        sent = await message.answer(_build_admin_help_text(), parse_mode="HTML")
        safe_create_task(auto_delete(sent, ADMIN_HELP_TTL))
    except Exception as e:
        logger.error(f"[HANDLERS] handle_admin_help error: {e}", exc_info=True)
        sent = await message.answer("⚠️ Не вдалося відкрити список адмін-команд.")
        safe_create_task(auto_delete(sent, 10))


@router.message(F.text == "🏎️ ПОВЕРНУТИСЯ В ГРУПУ")
async def handle_return_to_group(message: Message):
    try:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="ВХІД У ГРУПУ 🏎️",
                        url=GROUP_LINK,
                    )
                ]
            ]
        )

        sent = await message.answer(
            "Тисни кнопку нижче, щоб повернутися в групу 👇",
            reply_markup=kb,
        )
        safe_create_task(auto_delete(sent, 120))

        try:
            await message.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_return_to_group error: {e}", exc_info=True)


@router.message(F.text == "🏅 Досягнення")
async def handle_my_achievements(message: Message):
    telegram_user_id = message.from_user.id

    try:
        try:
            await message.delete()
        except Exception:
            pass

        stats = await get_user_stats(telegram_user_id)
        user_row = await get_user_by_telegram_id(telegram_user_id)

        if not stats or not user_row:
            sent_msg = await message.answer(
                "⚠️ Досягнення не знайдено. Спочатку зареєструйся в TurboTeam."
            )
            safe_create_task(auto_delete(sent_msg, 5))
            return

        user_uuid = user_row.get("id")
        if not user_uuid:
            sent_msg = await message.answer("⚠️ Не вдалося завантажити досягнення.")
            safe_create_task(auto_delete(sent_msg, 5))
            return

        activities = await get_user_activities(str(user_uuid), limit=5000)
        achievements = await get_user_achievements(str(user_uuid), limit=200)
        referrals_count = await get_referrals_count(str(user_uuid))

        unlocked_codes = {
            str(item.get("achievement_code") or "").strip()
            for item in achievements
            if str(item.get("achievement_code") or "").strip()
        }

        training_count = _count_training_from_activities(activities)
        nickname = user_row.get("nickname") or message.from_user.first_name or "Учасник"
        hp_total = int(stats.get("hp_total", 0) or 0)
        streak = int(stats.get("streak", 0) or 0)

        text = _build_achievements_text(
            nickname=str(nickname),
            training_count=training_count,
            unlocked_codes=unlocked_codes,
            hp_total=hp_total,
            streak=streak,
            referrals_count=referrals_count,
        )

        sent_msg = await message.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent_msg, 180))

    except Exception as e:
        logger.error(f"[HANDLERS] handle_my_achievements error: {e}", exc_info=True)
        sent_msg = await message.answer("⚠️ Не вдалося завантажити досягнення. Спробуй ще раз.")
        safe_create_task(auto_delete(sent_msg, 10))


@router.message(F.text == "🚀 Запросити друга 🔥")
async def handle_invite_friend_message(message: Message):
    await send_invite_prompt(message, message.from_user, delete_origin=True)


@router.callback_query(F.data == "invite_friend")
async def handle_invite_friend(callback: CallbackQuery):
    await send_invite_prompt(callback.message, callback.from_user)
    await callback.answer()


@router.callback_query(F.data == "community_rules")
async def handle_community_rules(callback: CallbackQuery):
    rules_text = (
        "📘 Правила TurboTeam\n\n"
        "• Тільки свіжі кружечки\n"
        "• Без фейків\n"
        "• Без спаму\n"
        "• Без токсичності\n"
        "• За порушення знімаємо HP"
    )
    await callback.answer(rules_text, show_alert=True)

@router.callback_query(F.data == "turbo_rules")
async def handle_turbo_rules(callback: CallbackQuery):
    text = (
        "📘 *Правила користування TurboTeam*\n\n"
        "1. Щоб отримати HP за тренування, кидай тільки свіжий кружечок після вибору Gym або Street.\n\n"
        "2. Переслані, старі або фейкові відео не зараховуються.\n\n"
        "3. За день можна зробити Gym і Street окремо, але один і той самий тип тренування двічі не рахується.\n\n"
        "4. Якщо на сьогодні вже є активність, відпочинок або пропуск вдруге не записуються.\n\n"
        "5. Не спам кнопками, не кидай фейки й не намагайся накрутити HP.\n\n"
        "6. Якщо учасники поскаржаться на фейковий звіт, можуть зняти HP і доведеться перездати тренування.\n\n"
        "7. Спілкуйся нормально: без образ, токсичності, сварок і політики.\n\n"
        "8. TurboTeam — це про дисципліну, чесність і рух уперед. Тренуйся чесно й кайфуй від прогресу 💪"
    )

    try:
        await callback.message.answer(text, parse_mode="Markdown")
        await callback.answer()
    except Exception as e:
        logger.error(f"[HANDLERS] handle_turbo_rules error: {e}", exc_info=True)
        await callback.answer("⚠️ Не вдалося відкрити правила. Спробуй ще раз.", show_alert=True)


@router.callback_query(F.data.in_(["action_rest", "action_skip"]))
async def handle_static_actions(callback: CallbackQuery):
    event_name = REST_SELECTED if callback.data == "action_rest" else SKIP_SELECTED

    try:
        await flow_event_bus.publish(
            EventEnvelope(
                name=event_name,
                user_id=callback.from_user.id,
                payload={
                    "source": callback,
                    "user": callback.from_user,
                },
                idempotency_key=f"{event_name}:{callback.from_user.id}:{callback.id}",
            )
        )
    except Exception as e:
        logger.error(f"[HANDLERS] handle_static_actions error: {e}", exc_info=True)
        await callback.message.answer("⚠️ Сталася помилка. Спробуй ще раз.")


@router.message(F.video_note)
async def gateway_video_note(m: Message):
    if m.chat.type != "private":
        return

    await flow_event_bus.publish(
        EventEnvelope(
            name=VIDEO_UPLOADED,
            user_id=m.from_user.id,
            payload={"message": m},
            idempotency_key=f"video:{m.from_user.id}:{m.message_id}",
        )
    )


@router.message(Command("reject"))
async def handle_reject_training(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    if m.chat.id != REPORTS_GROUP_ID:
        return

    if not m.reply_to_message:
        sent = await m.answer("⚠️ Використання: reply на кружок або текст репорту командою /reject")
        safe_create_task(auto_delete(sent, 5))
        try:
            await m.delete()
        except Exception:
            pass
        return

    ok = await rollback_training_report(
        bot=m.bot,
        group_message_id=m.reply_to_message.message_id,
        moderator_name=m.from_user.full_name,
        reason="manual_reject",
    )

    try:
        await m.delete()
    except Exception:
        pass

    if not ok:
        sent = await m.answer("⚠️ Не вдалося скасувати саме це тренування. Reply має бути на кружок або текст репорту.")
        safe_create_task(auto_delete(sent, 5))
        return


@router.message(Command("quizstats"))
async def handle_quiz_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    try:
        users = await get_all_users()
        total_users = len(users)

        if total_users == 0:
            sent = await m.answer("📉 У базі поки немає юзерів.")
            safe_create_task(auto_delete(sent, 10))
            return

        level_values = ["Новачок", "Середній", "Профі"]
        goal_values = ["Схуднення", "Набір маси", "Витривалість"]

        weekly_plan_items = [
            ("1-2 рази", "1–2 рази"),
            ("3-4 рази", "3–4 рази"),
            ("5+ разів", "5+ разів"),
        ]

        training_place_items = [
            ("У залі", "У залі"),
            ("На вулиці / турніках", "На вулиці / турніках"),
            ("І там, і там", "І там, і там"),
        ]

        weekly_plan_values = [item[0] for item in weekly_plan_items]
        training_place_values = [item[0] for item in training_place_items]

        level_stats = _count_values(users, "level", level_values)
        goal_stats = _count_values(users, "goal", goal_values)
        weekly_plan_stats = _count_values(users, "weekly_plan", weekly_plan_values)
        training_place_stats = _count_values(users, "training_place", training_place_values)

        total_level = _count_filled(users, "level")
        total_goal = _count_filled(users, "goal")
        total_weekly_plan = _count_filled(users, "weekly_plan")
        total_training_place = _count_filled(users, "training_place")

        text = (
            f"📊 <b>СТАТИСТИКА КВІЗУ</b>\n\n"
            f"👥 Усього юзерів у базі: <b>{total_users}</b>\n\n"
            f"{_format_stat_block('🎖️ <b>РІВЕНЬ</b>', level_stats, total_level, [(x, x) for x in level_values])}\n\n"
            f"{_format_stat_block('🎯 <b>ЦІЛЬ</b>', goal_stats, total_goal, [(x, x) for x in goal_values])}\n\n"
            f"{_format_stat_block('📅 <b>ПЛАН НА ТИЖДЕНЬ</b>', weekly_plan_stats, total_weekly_plan, weekly_plan_items)}\n\n"
            f"{_format_stat_block('🏋️ <b>ДЕ ТРЕНУЮТЬСЯ</b>', training_place_stats, total_training_place, training_place_items)}"
        )

        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))

        try:
            await m.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_quiz_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати статистику квізу.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("activitystats"))
async def handle_activity_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    try:
        users = await get_all_users()
        total_users = len(users)

        week_start, week_end = _get_current_week_period()

        all_activities_raw = await get_all_activities(limit=10000)
        week_activities_raw = await get_all_activities_in_period(
            created_at_from=week_start.isoformat(),
            created_at_to=week_end.isoformat(),
            limit=5000,
        )

        all_real_activities = [
            activity for activity in all_activities_raw
            if _is_real_activity(activity)
        ]
        week_real_activities = [
            activity for activity in week_activities_raw
            if _is_real_activity(activity)
        ]

        all_total = len(all_real_activities)
        week_total = len(week_real_activities)

        week_counts = _build_activity_counter(week_real_activities)

        active_users_week = _count_active_users(week_real_activities)
        week_hp = _sum_hp(week_real_activities)

        avg_activity = 0.0
        if active_users_week > 0:
            avg_activity = round(week_total / active_users_week, 1)

        text = (
            f"📊 <b>СТАТИСТИКА АКТИВНОСТІ</b>\n\n"
            f"📅 <b>TurboTeam-тиждень:</b>\n"
            f"{_format_period(week_start)} — {_format_period(week_end)}\n\n"
            f"👥 Усього юзерів у базі: <b>{total_users}</b>\n"
            f"🔥 Активних юзерів за тиждень: <b>{active_users_week}</b>\n\n"
            f"📦 Активностей за весь час: <b>{all_total}</b> {_word_actions(all_total)}\n"
            f"📅 Активностей за тиждень: <b>{week_total}</b> {_word_actions(week_total)}\n\n"
            f"🏋️ Gym: <b>{week_counts['Gym']}</b>\n"
            f"🦾 Street: <b>{week_counts['Street']}</b>\n"
            f"🧘 Rest: <b>{week_counts['Rest']}</b>\n"
            f"🚫 Skip: <b>{week_counts['Skipped']}</b>\n"
            f"🎁 Welcome Bonus: <b>{week_counts['Welcome Bonus']}</b>\n"
            f"🏎️ Returned: <b>{week_counts['Returned']}</b>\n\n"
            f"⚡ HP за тиждень: <b>{week_hp}</b>\n"
            f"📈 Середня активність: <b>{avg_activity}</b> дії на активного юзера"
        )

        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))

        try:
            await m.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_activity_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати статистику активностей.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("promostats"))
async def handle_promo_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    try:
        data = await _build_weekly_impact_data(m.bot, finished_week=True)
        text = _build_promo_stats_text(data)

        sent = await m.answer(
            text,
            parse_mode="HTML",
            reply_markup=_build_promo_compare_keyboard(),
        )
        safe_create_task(auto_delete(sent, 900))

        try:
            await m.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_promo_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати promo-статистику.")
        safe_create_task(auto_delete(sent, 10))


@router.callback_query(F.data == "promo_compare_previous")
async def handle_promo_compare_previous(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer()
        return

    try:
        current_data = await _build_weekly_impact_data(callback.bot, finished_week=True)
        previous_data = await _build_weekly_impact_data(
            callback.bot,
            finished_week=True,
            previous_week=True,
        )

        text = _build_promo_compare_text(current_data, previous_data)

        await callback.message.edit_text(
            text,
            parse_mode="HTML",
        )
        await callback.answer("Порівняння додано ✅")

    except Exception as e:
        logger.error(f"[HANDLERS] handle_promo_compare_previous error: {e}", exc_info=True)
        await callback.answer("⚠️ Не вдалося зібрати порівняння.", show_alert=True)


@router.message(Command("impactstats"))
async def handle_impact_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    try:
        data = await _build_weekly_impact_data(m.bot, finished_week=True)

        champion = escape(str(data["champion_nickname"]))
        turbo_index = int(data["turbo_index"])
        turbo_level = escape(str(data["turbo_level"]))
        counts = data["counts"]

        text = (
            f"🔥 <b>TURBOTEAM WEEKLY IMPACT</b>\n\n"
            f"<b>Turbo Index: {turbo_index}/100</b>\n"
            f"Рівень залучення: <b>{turbo_level}</b>\n\n"
            f"За минулий тиждень бот не просто рахував активність — він змушував учасників "
            f"повертатися в гру, тренуватися, звітувати й змагатися.\n\n"
            f"📅 <b>Період:</b>\n"
            f"{_format_period(data['week_start'])} — {_format_period(data['week_end'])}\n\n"
            f"👥 Учасників у групі: <b>{data['total_users']}</b>\n"
            f"⚡ Активних за тиждень: <b>{data['active_users']}</b> із <b>{data['total_users']}</b> — <b>{data['active_percent']}%</b>\n"
            f"🏋️ Підтверджених тренувань: <b>{data['training_count']}</b>\n"
            f"📹 Відео-звітів: <b>{data['video_reports']}</b>\n"
            f"🔁 Реферальних переходів: <b>{data['referrals_count']}</b>\n"
            f"🏆 HP видано: <b>{data['hp_total']}</b>\n"
            f"🔥 Середня активність: <b>{data['avg_activity']}</b> дії на активного учасника\n"
            f"🥇 Чемпіон тижня: <b>@{champion}</b> — <b>{data['champion_hp']} HP</b>\n\n"
            f"📊 <b>Розбивка дій:</b>\n"
            f"🏋️ Gym: <b>{counts['Gym']}</b>\n"
            f"🦾 Street: <b>{counts['Street']}</b>\n"
            f"🧘 Rest: <b>{counts['Rest']}</b>\n"
            f"🚫 Skip: <b>{counts['Skipped']}</b>\n"
            f"🎁 Welcome Bonus: <b>{counts['Welcome Bonus']}</b>\n"
            f"🏎️ Returned: <b>{counts['Returned']}</b>\n\n"
            f"<b>Що це означає:</b>\n"
            f"• {data['active_percent']}% учасників не просто зайшли в чат, а виконали дію.\n"
            f"• {data['training_count']} тренувань підтверджені через бота.\n"
            f"• Рейтинг і HP створили змагання всередині комʼюніті.\n"
            f"• Реферали показують органічний ріст без додаткової реклами.\n\n"
            f"<b>Висновок:</b>\n"
            f"TurboTeam перетворює Telegram-групу з пасивного чату на фітнес-гру, "
            f"де люди тренуються, звітують, повертаються і тягнуть друзів."
        )

        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 300))

        try:
            await m.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_impact_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати impact-статистику.")
        safe_create_task(auto_delete(sent, 10))
