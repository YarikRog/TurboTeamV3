import logging
import functools
from datetime import datetime, timedelta
from html import escape

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import REPORTS_GROUP_ID, GROUP_LINK
from phrases import get_phrase
from awards import sunday_final_logic
from database import (
    get_users_for_last_warning,
    get_users_for_auto_removal,
    get_kyiv_now,
    get_weekly_top_users,
)
from supabase_db import (
    get_all_users,
    get_user_activities,
)
from cache import get_data, set_data, delete_data

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

AUTO_REMOVE_BAN_DAYS = 7
AUTO_REMOVE_REDIS_PREFIX = "turbo:auto_removed"
LAST_WARNING_REDIS_PREFIX = "turbo:last_warning"

INACTIVE_DAYS_THRESHOLD = 3
SECOND_DAY_REMINDER_DAYS = 2
SECOND_DAY_REMINDER_LINK = "https://t.me/turboteampro/3746"
SECOND_DAY_REMINDER_REDIS_PREFIX = "turbo:second_day_reminder"

REAL_ACTIVITY_ACTIONS = {
    "Gym",
    "Street",
    "Rest",
    "Skipped",
    "Welcome Bonus",
    "Returned",
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


def _get_second_day_reminder_key(user_id: int, date_str: str) -> str:
    return f"{SECOND_DAY_REMINDER_REDIS_PREFIX}:{user_id}:{date_str}"


def _parse_activity_created_at(value):
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)

    return dt.astimezone(KYIV_TZ)


def _is_real_activity(activity: dict) -> bool:
    action_name = str(activity.get("action_name") or "").strip()
    return action_name in REAL_ACTIVITY_ACTIONS


def _get_last_real_activity_date(activities: list[dict]):
    last_activity_date = None

    for activity in activities:
        if not _is_real_activity(activity):
            continue

        created_at = _parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue

        activity_date = created_at.date()
        if last_activity_date is None or activity_date > last_activity_date:
            last_activity_date = activity_date

    return last_activity_date


async def _is_user_in_group(bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(REPORTS_GROUP_ID, user_id)
        status = str(member.status)

        if status in {"left", "kicked"}:
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
                InlineKeyboardButton(text="🧘 Rest", callback_data="action_rest"),
                InlineKeyboardButton(text="🚫 Skip", callback_data="action_skip"),
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

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
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
            "[TASKS] Second-day private reminder finished. Sent: %s, skipped: %s, skipped_removed=%s, skipped_not_in_group=%s",
            sent_count,
            skipped_count,
            skipped_removed,
            skipped_not_in_group,
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
                "[TASKS] Inactive reminder: no valid users. skipped_removed=%s skipped_not_in_group=%s skipped_active=%s",
                skipped_removed,
                skipped_not_in_group,
                skipped_active,
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
            "[TASKS] Inactive reminder triggered for %s users. skipped_removed=%s skipped_not_in_group=%s skipped_active=%s",
            len(inactive_mentions),
            skipped_removed,
            skipped_not_in_group,
            skipped_active,
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

    for user in warning_users:
        user_id = int(user["telegram_user_id"])

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
            continue

        mention_html = str(user.get("mention_html") or escape(str(user.get("nickname") or user_id)))
        silent_days = int(user.get("silent_days") or 7)

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
            await set_data(_get_last_warning_key(user_id), "1", ex=172800)
            warned_count += 1
        except Exception as e:
            logger.error(
                f"[TASKS] Failed to send last-day warning user_id={user_id}: {e}",
                exc_info=True,
            )

    logger.info(
        "[TASKS] Last-day warning finished. Warned: %s, skipped_removed=%s, skipped_not_in_group=%s",
        warned_count,
        skipped_removed,
        skipped_not_in_group,
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

    for user in removable_users:
        user_id = int(user["telegram_user_id"])
        user_key = _get_auto_removed_key(user_id)

        existing = await get_data(user_key)
        if existing is not None:
            skipped_existing_key += 1
            continue

        silent_days = int(user.get("silent_days") or 0)

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
        "[TASKS] Auto-removal finished. Removed: %s, skipped_existing_key=%s, skipped_not_in_group=%s",
        removed_count,
        skipped_existing_key,
        skipped_not_in_group,
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


@safe_job
async def run_sunday_final(bot) -> None:
    logger.info("[TASKS] Sunday Final started...")
    await sunday_final_logic(bot)
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
    scheduler.add_job(run_sunday_final, "cron", day_of_week="sun", hour=20, minute=0, args=[bot])

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