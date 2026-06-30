import logging
import functools
from datetime import datetime, timedelta
from html import escape

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import REPORTS_GROUP_ID, GROUP_LINK, PROFILE_WEB_APP_SHORT_NAME
from phrases import get_phrase
from awards import sunday_final_logic
from database import (
    get_users_for_last_warning,
    get_users_for_auto_removal,
    get_users_with_zero_weekly_streak,
    get_kyiv_now,
    get_weekly_top_users,
    get_last_finished_week_period,
    get_current_week_period,
    get_consecutive_day_streak_status,
    get_last_finished_month_period,
)
from supabase_db import (
    get_all_users,
    get_user_activities,
    get_user_activities_in_period,
    get_weekly_rating,
)
from cache import get_data, set_data, delete_data

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

AUTO_REMOVE_BAN_DAYS = 7
AUTO_REMOVE_REDIS_PREFIX = "turbo:auto_removed"
LAST_WARNING_REDIS_PREFIX = "turbo:last_warning"
WEEKLY_STREAK_REMINDER_REDIS_PREFIX = "turbo:weekly_streak_reminder"

WEEKLY_MILESTONE_REDIS_PREFIX = "turbo:weekly_milestone"

WEEKLY_MILESTONE_THRESHOLDS = {
    6: (7, 50),
    9: (10, 100),
    13: (14, 125),
}

RANK_OVERTAKE_REDIS_PREFIX = "turbo:rank_overtake"
RANK_OVERTAKE_SNAPSHOT_REDIS_PREFIX = "turbo:rank_snapshot"

INACTIVE_DAYS_THRESHOLD = 3
LAST_WARNING_DAYS_THRESHOLD = 7
AUTO_REMOVE_DAYS_THRESHOLD = 8

SECOND_DAY_REMINDER_DAYS = 2
SECOND_DAY_REMINDER_LINK = "https://t.me/turboteampro/3746"
SECOND_DAY_REMINDER_REDIS_PREFIX = "turbo:second_day_reminder"

BAD_DATE = "BAD_DATE"

REAL_ACTIVITY_ACTIONS = {
    "Gym",
    "Street",
    "Rest",
    "Skipped",
    "Welcome Bonus",
    "Returned",
    "Weekly Challenge",
    "Reaction Bonus",
}


def safe_job(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"[SCHEDULER] Error in job {func.__name__}: {e}",
                exc_info=True,
            )

    return wrapper


def _get_auto_removed_key(user_id: int) -> str:
    return f"{AUTO_REMOVE_REDIS_PREFIX}:{user_id}"


def _get_last_warning_key(user_id: int) -> str:
    return f"{LAST_WARNING_REDIS_PREFIX}:{user_id}"


def _get_weekly_streak_reminder_key(user_id: int, date_str: str) -> str:
    return f"{WEEKLY_STREAK_REMINDER_REDIS_PREFIX}:{user_id}:{date_str}"


def _get_second_day_reminder_key(user_id: int, date_str: str) -> str:
    return f"{SECOND_DAY_REMINDER_REDIS_PREFIX}:{user_id}:{date_str}"


def _get_weekly_milestone_key(user_id: int, date_str: str) -> str:
    return f"{WEEKLY_MILESTONE_REDIS_PREFIX}:{user_id}:{date_str}"


def _get_rank_overtake_reminder_key(user_id: int, date_str: str) -> str:
    return f"{RANK_OVERTAKE_REDIS_PREFIX}:{user_id}:{date_str}"


def _get_rank_snapshot_key() -> str:
    return RANK_OVERTAKE_SNAPSHOT_REDIS_PREFIX


def _normalize_iso_datetime_string(raw: str) -> str:
    """
    Normalizes Supabase/Postgres created_at for datetime.fromisoformat.

    Fixes:
    - 2026-05-19T15:31:09.72724+00:00
    - 2026-05-14T09:23:14.6173+00:00
    - 2026-04-29T16:53:18.4106+00:00
    """
    normalized = raw.strip().replace("Z", "+00:00").replace(" ", "T")

    if normalized.endswith("+00"):
        normalized = normalized[:-3] + "+00:00"

    if normalized.endswith("-00"):
        normalized = normalized[:-3] + "-00:00"

    t_pos = normalized.find("T")
    plus_pos = normalized.rfind("+")
    minus_pos = normalized.rfind("-")

    timezone_pos = -1

    if plus_pos > t_pos:
        timezone_pos = plus_pos

    if minus_pos > t_pos:
        timezone_pos = max(timezone_pos, minus_pos)

    if timezone_pos != -1:
        datetime_part = normalized[:timezone_pos]
        timezone_part = normalized[timezone_pos:]
    else:
        datetime_part = normalized
        timezone_part = ""

    if "." in datetime_part:
        base_part, fraction_part = datetime_part.split(".", 1)
        fraction_digits = "".join(char for char in fraction_part if char.isdigit())

        if fraction_digits:
            fraction_digits = fraction_digits[:6].ljust(6, "0")
            datetime_part = f"{base_part}.{fraction_digits}"
        else:
            datetime_part = base_part

    return f"{datetime_part}{timezone_part}"


def _parse_activity_created_at(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        normalized = _normalize_iso_datetime_string(raw)

        try:
            dt = datetime.fromisoformat(normalized)
        except Exception as e:
            logger.error(
                "[TASKS_DATE_PARSE_ERROR] Failed to parse created_at=%r normalized=%r error=%s",
                value,
                normalized,
                e,
                exc_info=True,
            )
            return None
    else:
        logger.error(
            "[TASKS_DATE_PARSE_ERROR] Unsupported created_at type=%s value=%r",
            type(value).__name__,
            value,
        )
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)

    return dt.astimezone(KYIV_TZ)


def _is_real_activity(activity: dict) -> bool:
    action_name = str(activity.get("action_name") or "").strip()
    return action_name in REAL_ACTIVITY_ACTIONS


def _get_last_real_activity_date(activities: list[dict]):
    last_activity_date = None
    had_real_activity_with_bad_date = False

    for activity in activities:
        if not _is_real_activity(activity):
            continue

        created_at_raw = activity.get("created_at")
        created_at = _parse_activity_created_at(created_at_raw)

        if not created_at:
            had_real_activity_with_bad_date = True
            logger.error(
                "[TASKS] SAFETY_BAD_ACTIVITY_DATE action=%s created_at=%r",
                activity.get("action_name"),
                created_at_raw,
            )
            continue

        activity_date = created_at.date()
        if last_activity_date is None or activity_date > last_activity_date:
            last_activity_date = activity_date

    if had_real_activity_with_bad_date:
        return BAD_DATE

    return last_activity_date


async def _is_user_in_group(bot, user_id: int) -> bool:
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

        return True

    except Exception as e:
        logger.info(
            "[TASKS] User is not available in group or get_chat_member failed: user_id=%s error=%s",
            user_id,
            e,
        )
        return False


async def build_top3_text() -> str:
    try:
        top_list = await get_weekly_top_users(finished_week=False)
        if not top_list:
            return ""

        lines = ["", "🏆 <b>ТОП-3 ЗАРАЗ:</b>", ""]

        for i, player in enumerate(top_list[:3]):
            icon = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"

            nick = (
                player.get("nick")
                or player.get("nickname")
                or f"ID:{player.get('telegram_user_id', 'unknown')}"
            )
            hp = int(player.get("hp", 0) or 0)

            lines.append(f"{icon} {escape(str(nick))} — <b>{hp}</b> HP")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"[TASKS] Failed to build top-3 text: {e}", exc_info=True)
        return ""


async def build_training_action_keyboard(bot) -> InlineKeyboardMarkup:
    me = await bot.get_me()

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏋️ Gym", url=f"https://t.me/{me.username}?start=gym"),
                InlineKeyboardButton(text="🦾 Street", url=f"https://t.me/{me.username}?start=street"),
            ],
            [
                InlineKeyboardButton(
                    text="🔥 Челендж тижня",
                    url=f"https://t.me/{me.username}?start=challenge",
                ),
            ],
            [
                InlineKeyboardButton(text="🧘 Rest", callback_data="action_rest"),
                InlineKeyboardButton(text="🚫 Skip", callback_data="action_skip"),
            ],
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{me.username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ],
        ]
    )


def build_return_group_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="ВХІД У ГРУПУ 🏎️", url=GROUP_LINK),
            ]
        ]
    )


def build_second_day_reminder_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🏎️ Повернутися в групу",
                    url=SECOND_DAY_REMINDER_LINK,
                ),
            ]
        ]
    )


def build_motivation_text(phrase_key: str, top3: str) -> str:
    phrase = escape(str(get_phrase(phrase_key)))
    return phrase + top3


def build_html_phrase_text(phrase_key: str, top3: str) -> str:
    phrase = str(get_phrase(phrase_key))
    return phrase + top3


@safe_job
async def send_morning_motivation(bot) -> None:
    top3 = await build_top3_text()
    text = build_motivation_text("morning", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Morning motivation sent")


@safe_job
async def send_midday_motivation(bot) -> None:
    top3 = await build_top3_text()
    text = build_motivation_text("midday", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Midday motivation sent")


@safe_job
async def send_day_motivation(bot) -> None:
    top3 = await build_top3_text()
    text = build_html_phrase_text("turbo_fact", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] TurboFact sent")


@safe_job
async def send_peak_motivation(bot) -> None:
    top3 = await build_top3_text()
    text = build_motivation_text("peak", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Peak motivation sent")


@safe_job
async def send_evening_motivation(bot) -> None:
    top3 = await build_top3_text()
    text = build_motivation_text("evening", top3)

    additional_text = "\n\n<b>А поки що можеш перейти по кнопці в Мій профіль і подивитися свої результати 👇</b>"
    text += additional_text

    me = await bot.get_me()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{me.username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ]
        ]
    )

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Evening motivation sent")


@safe_job
async def send_second_day_private_reminder(bot) -> None:
    try:
        users = await get_all_users()
        if not users:
            logger.info("[TASKS] Second-day private reminder: no users")
            return

        today = get_kyiv_now().date()
        today_str = today.strftime("%Y-%m-%d")
        sent_count = 0
        skipped_count = 0
        skipped_not_in_group = 0
        skipped_removed = 0
        safety_skipped_bad_date = 0

        for user in users:
            telegram_user_id = user.get("telegram_user_id")
            user_uuid = user.get("id")

            if not telegram_user_id or not user_uuid:
                skipped_count += 1
                continue

            user_id = int(telegram_user_id)

            removed_key = _get_auto_removed_key(user_id)
            already_removed = await get_data(removed_key)
            if already_removed is not None:
                skipped_removed += 1
                skipped_count += 1
                continue

            in_group = await _is_user_in_group(bot, user_id)
            if not in_group:
                skipped_not_in_group += 1
                skipped_count += 1
                continue

            reminder_key = _get_second_day_reminder_key(user_id, today_str)
            already_sent = await get_data(reminder_key)
            if already_sent is not None:
                skipped_count += 1
                continue

            try:
                activities = await get_user_activities(str(user_uuid), limit=1000)
            except Exception as e:
                logger.error(
                    f"[TASKS] Failed to get activities for second-day reminder user_id={user_id}: {e}",
                    exc_info=True,
                )
                skipped_count += 1
                continue

            last_activity_date = _get_last_real_activity_date(activities)

            if last_activity_date == BAD_DATE:
                safety_skipped_bad_date += 1
                skipped_count += 1
                logger.error(
                    "[TASKS] Second-day reminder SAFETY_SKIP_BAD_DATE user_id=%s",
                    user_id,
                )
                continue

            if last_activity_date is None:
                silent_days = SECOND_DAY_REMINDER_DAYS
            else:
                silent_days = (today - last_activity_date).days

            if silent_days != SECOND_DAY_REMINDER_DAYS:
                skipped_count += 1
                continue

            text = (
                "Бро, ти вже 2 дні без активності 👀\n\n"
                "Ще не критично, але ти починаєш випадати з гри.\n"
                "Зроби сьогодні хоча б мінімалку — Gym, Street або Rest, "
                "щоб не зливати ритм 🔥"
            )

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=text,
                    reply_markup=build_second_day_reminder_keyboard(),
                )
                await set_data(reminder_key, "1", ex=172800)
                sent_count += 1
            except Exception as e:
                logger.debug(
                    f"[TASKS] Failed to send second-day private reminder user_id={user_id}: {e}"
                )
                skipped_count += 1

        logger.info(
            "[TASKS] Second-day private reminder finished. Sent: %s, skipped: %s, skipped_removed=%s, skipped_not_in_group=%s, safety_skipped_bad_date=%s",
            sent_count,
            skipped_count,
            skipped_removed,
            skipped_not_in_group,
            safety_skipped_bad_date,
        )

    except Exception as e:
        logger.error(f"[TASKS] Second-day private reminder failed: {e}", exc_info=True)


@safe_job
async def inactive_reminder(bot) -> None:
    try:
        users = await get_all_users()
        if not users:
            logger.info("[TASKS] Inactive reminder: no users")
            return

        today = get_kyiv_now().date()
        inactive_mentions = []
        skipped_not_in_group = 0
        skipped_removed = 0
        skipped_active = 0
        safety_skipped_bad_date = 0

        for user in users:
            telegram_user_id = user.get("telegram_user_id")
            user_uuid = user.get("id")
            nickname = str(user.get("nickname") or telegram_user_id or "Учасник").strip()

            if not telegram_user_id or not user_uuid:
                continue

            user_id = int(telegram_user_id)

            removed_key = _get_auto_removed_key(user_id)
            already_removed = await get_data(removed_key)
            if already_removed is not None:
                skipped_removed += 1
                continue

            in_group = await _is_user_in_group(bot, user_id)
            if not in_group:
                skipped_not_in_group += 1
                continue

            try:
                activities = await get_user_activities(str(user_uuid), limit=1000)
            except Exception as e:
                logger.error(
                    "[TASKS] Failed to get activities for inactive reminder user_id=%s error=%s",
                    user_id,
                    e,
                    exc_info=True,
                )
                continue

            last_activity_date = _get_last_real_activity_date(activities)

            if last_activity_date == BAD_DATE:
                safety_skipped_bad_date += 1
                skipped_active += 1
                logger.error(
                    "[TASKS] Inactive reminder SAFETY_SKIP_BAD_DATE user_id=%s nickname=%s",
                    user_id,
                    nickname,
                )
                continue

            if last_activity_date is None:
                silent_days = INACTIVE_DAYS_THRESHOLD
            else:
                silent_days = (today - last_activity_date).days

            if silent_days != INACTIVE_DAYS_THRESHOLD:
                skipped_active += 1
                continue

            logger.info(
                "[TASKS] Inactive reminder candidate user_id=%s nickname=%s last_activity_date=%s today=%s silent_days=%s",
                user_id,
                nickname,
                last_activity_date,
                today,
                silent_days,
            )

            display_name = escape(nickname)
            inactive_mentions.append(
                f'<a href="tg://user?id={user_id}">{display_name}</a>'
            )

        if not inactive_mentions:
            logger.info(
                "[TASKS] Inactive reminder: no valid users. skipped_removed=%s skipped_not_in_group=%s skipped_active=%s safety_skipped_bad_date=%s",
                skipped_removed,
                skipped_not_in_group,
                skipped_active,
                safety_skipped_bad_date,
            )
            return

        mentions = " ".join(inactive_mentions)
        text = (
            f"🚨 <b>РОЗДУПЛЯТОР ТУРБОТІМ</b> 🚨\n\n"
            f"{mentions}\n\n"
            f"Бро, ти де зник? Вже 3 дні тиші! "
            f"Повертайся в стрій, HP самі себе не зароблять! 🔥"
        )
        keyboard = await build_training_action_keyboard(bot)

        await bot.send_message(
            chat_id=REPORTS_GROUP_ID,
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )

        logger.info(
            "[TASKS] Inactive reminder triggered for %s users. skipped_removed=%s skipped_not_in_group=%s skipped_active=%s safety_skipped_bad_date=%s",
            len(inactive_mentions),
            skipped_removed,
            skipped_not_in_group,
            skipped_active,
            safety_skipped_bad_date,
        )

    except Exception as e:
        logger.error(f"[TASKS] Inactive reminder failed: {e}", exc_info=True)


@safe_job
async def send_last_day_warning(bot) -> None:
    warning_users = await get_users_for_last_warning()
    if not warning_users:
        logger.info("[TASKS] Last-day warning: no users")
        return

    warned_count = 0
    skipped_not_in_group = 0
    skipped_removed = 0
    skipped_already_warned = 0
    skipped_overdue = 0

    for user in warning_users:
        user_id = int(user["telegram_user_id"])
        silent_days = int(user.get("silent_days") or 0)

        if silent_days != LAST_WARNING_DAYS_THRESHOLD:
            skipped_overdue += 1
            logger.info(
                "[TASKS] Last-day warning skipped: user_id=%s silent_days=%s expected=%s",
                user_id,
                silent_days,
                LAST_WARNING_DAYS_THRESHOLD,
            )
            continue

        removed_key = _get_auto_removed_key(user_id)
        already_removed = await get_data(removed_key)
        if already_removed is not None:
            skipped_removed += 1
            continue

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        warned_key = _get_last_warning_key(user_id)
        already_warned = await get_data(warned_key)
        if already_warned is not None:
            skipped_already_warned += 1
            continue

        mention_html = str(user.get("mention_html") or escape(str(user.get("nickname") or user_id)))

        try:
            await bot.send_message(
                chat_id=REPORTS_GROUP_ID,
                text=(
                    f"⚠️ {mention_html}, це останнє попередження без активності.\n"
                    f"У тебе вже <b>{silent_days} днів тиші</b>.\n"
                    f"Якщо сьогодні не буде жодної дії, бот автоматично вилучить тебе з TurboTeam."
                ),
                parse_mode="HTML",
            )
            await set_data(warned_key, "1", ex=172800)
            warned_count += 1
        except Exception as e:
            logger.error(
                f"[TASKS] Failed to send last-day warning user_id={user_id}: {e}",
                exc_info=True,
            )

    logger.info(
        "[TASKS] Last-day warning finished. Warned: %s, skipped_removed=%s, skipped_not_in_group=%s, skipped_already_warned=%s, skipped_overdue=%s",
        warned_count,
        skipped_removed,
        skipped_not_in_group,
        skipped_already_warned,
        skipped_overdue,
    )


@safe_job
async def send_weekly_streak_reminder(bot) -> None:
    """Sunday 17:00 Kyiv: reminds users with 0 trainings this week, 3 hours before the week resets."""
    zero_streak_users = await get_users_with_zero_weekly_streak()
    if not zero_streak_users:
        logger.info("[TASKS] Weekly streak reminder: no users with zero streak")
        return

    today_str = get_kyiv_now().strftime("%Y-%m-%d")

    reminded_count = 0
    skipped_not_in_group = 0
    skipped_already_reminded = 0

    for user in zero_streak_users:
        user_id = int(user["telegram_user_id"])

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        reminder_key = _get_weekly_streak_reminder_key(user_id, today_str)
        already_reminded = await get_data(reminder_key)
        if already_reminded is not None:
            skipped_already_reminded += 1
            continue

        nickname = str(user.get("nickname") or user_id)

        me = await bot.get_me()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="👤 Мій профіль",
                        url=f"https://t.me/{me.username}/{PROFILE_WEB_APP_SHORT_NAME}",
                    ),
                ]
            ]
        )

        try:
            await bot.send_message(
                chat_id=user_id,
                text=(
                    f"🔥 {escape(nickname)}, тиждень майже закінчився, а тренувань ще не було!\n"
                    f"Зроби хоча б одне тренування сьогодні — не лінуйся, ще встигнеш 💪"
                ),
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            await set_data(reminder_key, "1", ex=86400)
            reminded_count += 1
        except Exception as e:
            logger.error(
                f"[TASKS] Failed to send weekly streak reminder user_id={user_id}: {e}",
                exc_info=True,
            )

    logger.info(
        "[TASKS] Weekly streak reminder finished. Reminded: %s, skipped_not_in_group=%s, skipped_already_reminded=%s",
        reminded_count,
        skipped_not_in_group,
        skipped_already_reminded,
    )


@safe_job
async def auto_remove_inactive_users(bot) -> None:
    removable_users = await get_users_for_auto_removal()
    if not removable_users:
        logger.info("[TASKS] Auto-removal: no users to remove")
        return

    now = get_kyiv_now()
    ban_until = now + timedelta(days=AUTO_REMOVE_BAN_DAYS)
    removed_count = 0
    skipped_not_in_group = 0
    skipped_existing_key = 0
    safety_skipped_not_due = 0

    for user in removable_users:
        user_id = int(user["telegram_user_id"])
        user_key = _get_auto_removed_key(user_id)
        silent_days = int(user.get("silent_days") or 0)

        if silent_days < AUTO_REMOVE_DAYS_THRESHOLD:
            safety_skipped_not_due += 1
            logger.warning(
                "[TASKS] Auto-removal SAFETY_SKIP_NOT_DUE: user_id=%s silent_days=%s threshold=%s",
                user_id,
                silent_days,
                AUTO_REMOVE_DAYS_THRESHOLD,
            )
            continue

        existing = await get_data(user_key)
        if existing is not None:
            skipped_existing_key += 1
            continue

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        try:
            await bot.ban_chat_member(
                chat_id=REPORTS_GROUP_ID,
                user_id=user_id,
                until_date=ban_until,
            )

            payload = {
                "telegram_user_id": user_id,
                "nickname": str(user.get("nickname") or ""),
                "silent_days": silent_days,
                "unban_at": ban_until.isoformat(),
                "status": "banned",
                "created_at": now.isoformat(),
                "unban_notified": False,
            }
            await set_data(user_key, payload)

            await delete_data(_get_last_warning_key(user_id))

            mention_html = str(user.get("mention_html") or escape(str(user.get("nickname") or user_id)))

            await bot.send_message(
                chat_id=REPORTS_GROUP_ID,
                text=(
                    f"🚪 {mention_html} вилучений із TurboTeam.\n"
                    f"Причина: <b>{silent_days} днів</b> без жодної активності.\n"
                    f"Бан: <b>{AUTO_REMOVE_BAN_DAYS} днів</b>.\n"
                    f"Після завершення блокування бот напише в приват, що можна повернутися."
                ),
                parse_mode="HTML",
            )

            removed_count += 1

        except Exception as e:
            logger.error(
                f"[TASKS] Failed to auto-remove user_id={user_id}: {e}",
                exc_info=True,
            )

    logger.info(
        "[TASKS] Auto-removal finished. Removed: %s, skipped_existing_key=%s, skipped_not_in_group=%s, safety_skipped_not_due=%s",
        removed_count,
        skipped_existing_key,
        skipped_not_in_group,
        safety_skipped_not_due,
    )


@safe_job
async def auto_unban_inactive_users(bot) -> None:
    logger.info("[TASKS] Auto-unban scan started")

    from cache import redis_client

    if redis_client is None:
        logger.warning("[TASKS] Auto-unban skipped: redis unavailable")
        return

    cursor = 0
    unbanned_count = 0
    notified_count = 0
    skipped_not_due = 0
    deleted_bad_keys = 0
    now = get_kyiv_now()

    while True:
        cursor, keys = await redis_client.scan(
            cursor=cursor,
            match=f"{AUTO_REMOVE_REDIS_PREFIX}:*",
            count=100,
        )

        for key in keys:
            payload = await get_data(key)
            if not isinstance(payload, dict):
                await delete_data(key)
                deleted_bad_keys += 1
                continue

            user_id = int(payload.get("telegram_user_id") or 0)
            unban_at_raw = str(payload.get("unban_at") or "").strip()

            if not user_id or not unban_at_raw:
                await delete_data(key)
                deleted_bad_keys += 1
                continue

            try:
                unban_at = datetime.fromisoformat(unban_at_raw)
                if unban_at.tzinfo is None:
                    unban_at = KYIV_TZ.localize(unban_at)
                else:
                    unban_at = unban_at.astimezone(KYIV_TZ)
            except Exception:
                await delete_data(key)
                deleted_bad_keys += 1
                continue

            if now < unban_at:
                skipped_not_due += 1
                continue

            try:
                await bot.unban_chat_member(
                    chat_id=REPORTS_GROUP_ID,
                    user_id=user_id,
                    only_if_banned=True,
                )
                unbanned_count += 1
            except Exception as e:
                logger.error(f"[TASKS] Failed to unban user_id={user_id}: {e}", exc_info=True)
                continue

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        "🏎️ Доступ до TurboTeam знову відкритий.\n\n"
                        "Якщо хочеш повернутись у стрій — залітай назад і не випадай із гри."
                    ),
                    reply_markup=build_return_group_keyboard(),
                )
                notified_count += 1
            except Exception as e:
                logger.debug(f"[TASKS] Failed to notify unbanned user_id={user_id}: {e}")

            await delete_data(key)
            await delete_data(_get_last_warning_key(user_id))

        if cursor == 0:
            break

    logger.info(
        "[TASKS] Auto-unban finished. Unbanned: %s, notified: %s, skipped_not_due=%s, deleted_bad_keys=%s",
        unbanned_count,
        notified_count,
        skipped_not_due,
        deleted_bad_keys,
    )


def build_weekly_personal_report_keyboard(bot_username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{bot_username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ]
        ]
    )


@safe_job
async def send_weekly_personal_reports(bot) -> None:
    """Sunday 20:00 Kyiv, right after the champion announcement: DMs everyone else their weekly recap."""
    top_users = await get_weekly_top_users(finished_week=True)
    champion_id = None
    if top_users and int(top_users[0].get("hp") or 0) > 0:
        champion_id = top_users[0].get("telegram_user_id")

    users = await get_all_users()
    if not users:
        logger.info("[TASKS] Weekly personal report: no users")
        return

    period_start, period_end = get_last_finished_week_period()
    me = await bot.get_me()
    keyboard = build_weekly_personal_report_keyboard(me.username)

    sent_count = 0
    skipped_champion = 0
    skipped_not_in_group = 0
    skipped_no_activity = 0
    skipped_errors = 0

    for user in users:
        telegram_user_id = user.get("telegram_user_id")
        user_uuid = user.get("id")
        if not telegram_user_id or not user_uuid:
            continue

        user_id = int(telegram_user_id)

        if champion_id and user_id == int(champion_id):
            skipped_champion += 1
            continue

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        try:
            activities = await get_user_activities_in_period(str(user_uuid), period_start, period_end)
        except Exception as e:
            logger.error(
                f"[TASKS] Failed to get weekly activities for user_id={user_id}: {e}",
                exc_info=True,
            )
            skipped_errors += 1
            continue

        if not activities:
            skipped_no_activity += 1
            continue

        hp = sum(int(a.get("hp_change") or 0) for a in activities)
        trainings = sum(1 for a in activities if a.get("action_name") in ("Gym", "Street"))
        rests = sum(1 for a in activities if a.get("action_name") == "Rest")
        challenges = sum(1 for a in activities if a.get("action_name") == "Weekly Challenge")

        text = (
            "📊 <b>Твій тижневий звіт</b>\n\n"
            f"💪 HP за тиждень: <b>{hp}</b>\n"
            f"🏋️ Тренувань: <b>{trainings}</b>\n"
            f"🧘 Рестів: <b>{rests}</b>\n"
            f"🔥 Челенджів: <b>{challenges}</b>\n\n"
            "Новий тиждень почався — покажи на що здатен 🏎️💨"
        )

        try:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            sent_count += 1
        except Exception as e:
            logger.debug(f"[TASKS] Failed to send weekly personal report user_id={user_id}: {e}")
            skipped_errors += 1

    logger.info(
        "[TASKS] Weekly personal report finished. Sent: %s, skipped_champion=%s, skipped_not_in_group=%s, skipped_no_activity=%s, skipped_errors=%s",
        sent_count,
        skipped_champion,
        skipped_not_in_group,
        skipped_no_activity,
        skipped_errors,
    )


def build_monthly_personal_report_keyboard(bot_username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{bot_username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ]
        ]
    )


@safe_job
async def send_monthly_personal_reports(bot) -> None:
    """1st of each month at 07:00 Kyiv: sends everyone their monthly recap."""
    users = await get_all_users()
    if not users:
        logger.info("[TASKS] Monthly personal report: no users")
        return

    period_start, period_end = get_last_finished_month_period()
    me = await bot.get_me()
    keyboard = build_monthly_personal_report_keyboard(me.username)

    # Get month name in Ukrainian
    now = get_kyiv_now()
    if now.month == 1:
        prev_month = 12
        prev_year = now.year - 1
    else:
        prev_month = now.month - 1
        prev_year = now.year

    month_names = {
        1: "січня", 2: "лютого", 3: "березня", 4: "квітня",
        5: "травня", 6: "червня", 7: "липня", 8: "серпня",
        9: "вересня", 10: "жовтня", 11: "листопада", 12: "грудня"
    }
    month_name = month_names.get(prev_month, "місяця")

    sent_count = 0
    skipped_not_in_group = 0
    skipped_no_activity = 0
    skipped_errors = 0

    for user in users:
        telegram_user_id = user.get("telegram_user_id")
        user_uuid = user.get("id")
        if not telegram_user_id or not user_uuid:
            continue

        user_id = int(telegram_user_id)

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        try:
            activities = await get_user_activities_in_period(str(user_uuid), period_start, period_end)
        except Exception as e:
            logger.error(
                f"[TASKS] Failed to get monthly activities for user_id={user_id}: {e}",
                exc_info=True,
            )
            skipped_errors += 1
            continue

        if not activities:
            skipped_no_activity += 1
            continue

        # Calculate statistics
        gym_trainings = sum(1 for a in activities if a.get("action_name") == "Gym")
        street_trainings = sum(1 for a in activities if a.get("action_name") == "Street")
        total_trainings = gym_trainings + street_trainings
        rest_days = sum(1 for a in activities if a.get("action_name") == "Rest")

        # Calculate days with no activity (entire month days without any action)
        period_start_dt = datetime.fromisoformat(period_start).astimezone(KYIV_TZ)
        period_end_dt = datetime.fromisoformat(period_end).astimezone(KYIV_TZ)

        # Get set of days with any activity
        active_days = set()
        for activity in activities:
            created_at_str = activity.get("created_at")
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str)
                    if created_at.tzinfo is None:
                        created_at = pytz.UTC.localize(created_at)
                    day_key = created_at.astimezone(KYIV_TZ).strftime("%Y-%m-%d")
                    active_days.add(day_key)
                except Exception:
                    pass

        # Count all days in the month
        current_day = period_start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        total_month_days = 0
        while current_day < period_end_dt:
            total_month_days += 1
            current_day += timedelta(days=1)

        # Days with no activity
        inactive_days = total_month_days - len(active_days)

        text = (
            f"Привіт! 👋 За минулий <b>{month_name}</b> у тебе було <b>{total_trainings}</b> тренувань.\n\n"
            f"📊 <b>Статистика</b>:\n"
            f"• <b>Всього тренувань</b>: {total_trainings}\n"
            f"• 🏋️ <b>Gym</b>: {gym_trainings}\n"
            f"• 🏃 <b>Street</b>: {street_trainings}\n"
            f"• 😴 <b>Дні без активності</b>: {inactive_days}\n\n"
            f"Ти можеш ознайомитися з календарем своїх тренувань по цій кнопці 👇\n\n"
            f"Новий місяць — нові можливості! 🚀 Давай роби вітер і досягай своїх цілей! 💪"
        )

        try:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            sent_count += 1
        except Exception as e:
            logger.debug(f"[TASKS] Failed to send monthly personal report user_id={user_id}: {e}")
            skipped_errors += 1

    logger.info(
        "[TASKS] Monthly personal report finished. Sent: %s, skipped_not_in_group=%s, skipped_no_activity=%s, skipped_errors=%s",
        sent_count,
        skipped_not_in_group,
        skipped_no_activity,
        skipped_errors,
    )


def build_weekly_milestone_keyboard(bot_username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{bot_username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ]
        ]
    )


@safe_job
async def send_weekly_milestone_reminder(bot) -> None:
    """Daily 19:00 Kyiv: reminds users with 6/9/13 trainings to go for the next milestone (7/10/14)."""
    users = await get_all_users()
    if not users:
        logger.info("[TASKS] Weekly milestone reminder: no users")
        return

    today_str = get_kyiv_now().strftime("%Y-%m-%d")
    me = await bot.get_me()
    keyboard = build_weekly_milestone_keyboard(me.username)

    sent_count = 0
    skipped_not_in_group = 0
    skipped_already_reminded = 0
    skipped_not_at_threshold = 0
    skipped_errors = 0

    for user in users:
        telegram_user_id = user.get("telegram_user_id")
        user_uuid = user.get("id")
        if not telegram_user_id or not user_uuid:
            continue

        user_id = int(telegram_user_id)

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        reminder_key = _get_weekly_milestone_key(user_id, today_str)
        already_reminded = await get_data(reminder_key)
        if already_reminded is not None:
            skipped_already_reminded += 1
            continue

        try:
            activities = await get_user_activities(str(user_uuid), limit=500)
        except Exception as e:
            logger.error(
                f"[TASKS] Failed to get activities for weekly milestone user_id={user_id}: {e}",
                exc_info=True,
            )
            skipped_errors += 1
            continue

        from database import _calculate_training_streak
        weekly_count = _calculate_training_streak(activities)

        if weekly_count not in WEEKLY_MILESTONE_THRESHOLDS:
            skipped_not_at_threshold += 1
            continue

        next_milestone, bonus = WEEKLY_MILESTONE_THRESHOLDS[weekly_count]

        text = (
            f"💪 У тебе вже <b>{weekly_count}</b> тренувань за цей тиждень!\n\n"
            f"Зроби ще одне і отримай <b>+{bonus} HP</b> на {next_milestone}-му тренуванні 🔥"
        )

        try:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await set_data(reminder_key, "1", ex=86400)
            sent_count += 1
        except Exception as e:
            logger.debug(f"[TASKS] Failed to send weekly milestone reminder user_id={user_id}: {e}")
            skipped_errors += 1

    logger.info(
        "[TASKS] Weekly milestone reminder finished. Sent: %s, skipped_not_in_group=%s, "
        "skipped_already_reminded=%s, skipped_not_at_threshold=%s, skipped_errors=%s",
        sent_count,
        skipped_not_in_group,
        skipped_already_reminded,
        skipped_not_at_threshold,
        skipped_errors,
    )


def build_rank_overtake_keyboard(bot_username: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{bot_username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ]
        ]
    )


@safe_job
async def send_rank_overtake_reminder(bot) -> None:
    """Wed/Sat: compares current weekly rank to the last saved snapshot and DMs anyone who got overtaken."""
    period_start, period_end = get_current_week_period()
    current_rows = await get_weekly_rating(period_start, period_end)

    if not current_rows:
        logger.info("[TASKS] Rank overtake reminder: no rating rows")
        return

    snapshot_key = _get_rank_snapshot_key()
    saved_snapshot = await get_data(snapshot_key)
    if not isinstance(saved_snapshot, dict):
        saved_snapshot = {}

    # A snapshot from a previous TurboTeam week is meaningless here — ranks reset every
    # week, so comparing against it would falsely flag "overtakes" right after the reset.
    if saved_snapshot.get("week_start") == period_start:
        previous_ranks = saved_snapshot.get("ranks") or {}
    else:
        previous_ranks = {}

    current_ranks = {}
    for row in current_rows:
        telegram_user_id = row.get("telegram_user_id")
        if telegram_user_id:
            current_ranks[str(telegram_user_id)] = int(row.get("rank") or 0)

    new_snapshot = {"week_start": period_start, "ranks": current_ranks}

    if not previous_ranks:
        await set_data(snapshot_key, new_snapshot, ex=7 * 24 * 60 * 60)
        logger.info("[TASKS] Rank overtake reminder: no comparable previous snapshot, saved baseline")
        return

    today_str = get_kyiv_now().strftime("%Y-%m-%d")
    me = await bot.get_me()
    keyboard = build_rank_overtake_keyboard(me.username)

    sent_count = 0
    skipped_not_in_group = 0
    skipped_already_reminded = 0
    skipped_errors = 0

    for row in current_rows:
        telegram_user_id = row.get("telegram_user_id")
        hp = int(row.get("hp") or 0)
        if not telegram_user_id or hp <= 0:
            continue

        user_id = int(telegram_user_id)
        current_rank = int(row.get("rank") or 0)
        previous_rank = previous_ranks.get(str(user_id))

        if previous_rank is None or current_rank <= previous_rank:
            continue

        in_group = await _is_user_in_group(bot, user_id)
        if not in_group:
            skipped_not_in_group += 1
            continue

        reminder_key = _get_rank_overtake_reminder_key(user_id, today_str)
        already_reminded = await get_data(reminder_key)
        if already_reminded is not None:
            skipped_already_reminded += 1
            continue

        text = (
            "📉 Тебе обігнали в рейтингу!\n\n"
            "Подивись актуальний рейтинг командою /rating або у вебапі 👇"
        )

        try:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await set_data(reminder_key, "1", ex=86400)
            sent_count += 1
        except Exception as e:
            logger.debug(f"[TASKS] Failed to send rank overtake reminder user_id={user_id}: {e}")
            skipped_errors += 1

    await set_data(snapshot_key, new_snapshot, ex=7 * 24 * 60 * 60)

    logger.info(
        "[TASKS] Rank overtake reminder finished. Sent: %s, skipped_not_in_group=%s, "
        "skipped_already_reminded=%s, skipped_errors=%s",
        sent_count,
        skipped_not_in_group,
        skipped_already_reminded,
        skipped_errors,
    )


@safe_job
async def run_sunday_final(bot) -> None:
    logger.info("[TASKS] Sunday Final started...")
    await sunday_final_logic(bot)
    await send_weekly_personal_reports(bot)
    logger.info("[TASKS] Sunday Final finished.")


def setup_scheduler(bot) -> AsyncIOScheduler:
    kyiv_tz = pytz.timezone("Europe/Kyiv")
    scheduler = AsyncIOScheduler(timezone=kyiv_tz)

    scheduler.add_job(send_morning_motivation, "cron", hour=8, minute=0, args=[bot])
    scheduler.add_job(auto_unban_inactive_users, "cron", hour=9, minute=0, args=[bot])
    scheduler.add_job(inactive_reminder, "cron", hour=11, minute=0, args=[bot])
    scheduler.add_job(send_midday_motivation, "cron", hour=12, minute=0, args=[bot])
    scheduler.add_job(auto_remove_inactive_users, "cron", hour=12, minute=5, args=[bot])
    scheduler.add_job(send_day_motivation, "cron", hour=15, minute=0, args=[bot])
    scheduler.add_job(send_peak_motivation, "cron", hour=18, minute=30, args=[bot])
    scheduler.add_job(send_last_day_warning, "cron", hour=19, minute=0, args=[bot])
    scheduler.add_job(send_second_day_private_reminder, "cron", hour=19, minute=30, args=[bot])
    scheduler.add_job(send_evening_motivation, "cron", hour=21, minute=0, args=[bot])
    scheduler.add_job(send_weekly_streak_reminder, "cron", day_of_week="sun", hour=17, minute=0, args=[bot])
    scheduler.add_job(run_sunday_final, "cron", day_of_week="sun", hour=20, minute=0, args=[bot])
    scheduler.add_job(send_weekly_milestone_reminder, "cron", hour=19, minute=0, args=[bot])
    scheduler.add_job(send_rank_overtake_reminder, "cron", day_of_week="wed,sat", hour=13, minute=0, args=[bot])
    scheduler.add_job(send_monthly_personal_reports, "cron", day=1, hour=7, minute=0, args=[bot])

    scheduler.add_job(
        auto_unban_inactive_users,
        "date",
        run_date=datetime.now(kyiv_tz) + timedelta(seconds=20),
        args=[bot],
    )

    scheduler.start()

    now_str = datetime.now(kyiv_tz).strftime("%H:%M:%S")
    logger.info(f"[TASKS] Scheduler started. Kyiv time: {now_str}")

    return scheduler