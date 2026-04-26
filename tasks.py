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
    get_inactive_users,
    get_users_for_last_warning,
    get_users_for_auto_removal,
    get_kyiv_now,
)
from ratings import get_rating_data
from cache import get_data, set_data, delete_data

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

AUTO_REMOVE_BAN_DAYS = 7
AUTO_REMOVE_REDIS_PREFIX = "turbo:auto_removed"
LAST_WARNING_REDIS_PREFIX = "turbo:last_warning"


# ==============================================================================
# SAFE DECORATOR FOR SCHEDULED JOBS
# ==============================================================================

def safe_job(func):
    """
    Wrapper for APScheduler jobs.
    Ensures one failed job does not break the scheduler.
    """
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


# ==============================================================================
# HELPERS
# ==============================================================================

def _get_auto_removed_key(user_id: int) -> str:
    return f"{AUTO_REMOVE_REDIS_PREFIX}:{user_id}"


def _get_last_warning_key(user_id: int) -> str:
    return f"{LAST_WARNING_REDIS_PREFIX}:{user_id}"


async def build_top3_text() -> str:
    """
    Builds TOP-3 rating block for scheduled messages.
    HTML-safe version.
    Returns empty string if rating is unavailable.
    """
    try:
        data = await get_rating_data(0)
        if not data or "top" not in data:
            return ""

        top_list = data.get("top", [])
        if not top_list:
            return ""

        lines = ["", "🏆 <b>ТОП-3 ЗАРАЗ:</b>", ""]

        for i, player in enumerate(top_list[:3]):
            if i == 0:
                icon = "🥇"
            elif i == 1:
                icon = "🥈"
            else:
                icon = "🥉"

            nick = escape(str(player.get("nick", "Unknown")))
            hp = int(player.get("hp", 0) or 0)
            lines.append(f"{icon} {nick} — <b>{hp}</b> HP")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"[TASKS] Failed to build top-3 text: {e}", exc_info=True)
        return ""


async def build_training_action_keyboard(bot) -> InlineKeyboardMarkup:
    """
    Builds inline action buttons for motivation posts.
    """
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


def build_motivation_text(phrase_key: str, top3: str) -> str:
    """
    Builds HTML-safe motivation text.
    Escapes phrase text because phrases are plain text, not HTML.
    """
    phrase = escape(str(get_phrase(phrase_key)))
    return phrase + top3


# ==============================================================================
# SCHEDULED TASKS
# ==============================================================================

@safe_job
async def send_morning_motivation(bot) -> None:
    """08:00 Kyiv — morning motivation + top-3 + action buttons."""
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
async def send_day_motivation(bot) -> None:
    """15:00 Kyiv — day motivation + top-3 + action buttons."""
    top3 = await build_top3_text()
    text = build_motivation_text("day", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Day motivation sent")


@safe_job
async def send_evening_motivation(bot) -> None:
    """21:00 Kyiv — evening motivation + top-3."""
    top3 = await build_top3_text()
    text = build_motivation_text("evening", top3)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
    )
    logger.info("[TASKS] Evening motivation sent")


@safe_job
async def inactive_reminder(bot) -> None:
    """
    11:00 Kyiv every day — mention users inactive for 3+ days.
    Actual filtering is handled in database.get_inactive_users().
    """
    inactive_list = await get_inactive_users()
    if not inactive_list:
        logger.info("[TASKS] Inactive reminder: everyone is active")
        return

    mentions = " ".join(inactive_list)
    text = (
        f"🚨 <b>РОЗДУПЛЯТОР ТУРБОТІМ</b> 🚨\n\n"
        f"{mentions}\n\n"
        f"Бро, ти де зник? Вже 3 дні тиші! "
        f"Повертайся в стрій, HP самі себе не зароблять! 🔥"
    )
    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
    )
    logger.info(f"[TASKS] Inactive reminder triggered for {len(inactive_list)} users")


@safe_job
async def send_last_day_warning(bot) -> None:
    """
    19:00 Kyiv every day — final warning for users with exactly 7 days without activity.
    """
    warning_users = await get_users_for_last_warning()
    if not warning_users:
        logger.info("[TASKS] Last-day warning: no users")
        return

    warned_count = 0

    for user in warning_users:
        user_id = int(user["telegram_user_id"])

        removed_key = _get_auto_removed_key(user_id)
        already_removed = await get_data(removed_key)
        if already_removed is not None:
            continue

        warned_key = _get_last_warning_key(user_id)
        already_warned = await get_data(warned_key)
        if already_warned is not None:
            continue

        mention_html = str(user.get("mention_html") or escape(str(user.get("nickname") or user_id)))

        try:
            await bot.send_message(
                chat_id=REPORTS_GROUP_ID,
                text=(
                    f"⚠️ {mention_html}, це останній день без активності.\n"
                    f"У тебе вже <b>7 днів тиші</b>.\n"
                    f"Якщо сьогодні не буде жодної дії, завтра бот автоматично вилучить тебе з TurboTeam."
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

    logger.info(f"[TASKS] Last-day warning finished. Warned: {warned_count}")


@safe_job
async def auto_remove_inactive_users(bot) -> None:
    """
    Daily auto-removal for users with 8+ days without real activity.
    Bans user for 7 days and stores unban info in Redis.
    """
    removable_users = await get_users_for_auto_removal()
    if not removable_users:
        logger.info("[TASKS] Auto-removal: no users to remove")
        return

    now = get_kyiv_now()
    ban_until = now + timedelta(days=AUTO_REMOVE_BAN_DAYS)
    removed_count = 0

    for user in removable_users:
        user_id = int(user["telegram_user_id"])
        user_key = _get_auto_removed_key(user_id)

        existing = await get_data(user_key)
        if existing is not None:
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
                "silent_days": int(user.get("silent_days") or 0),
                "unban_at": ban_until.isoformat(),
            }
            await set_data(
                user_key,
                payload,
                ex=int(timedelta(days=AUTO_REMOVE_BAN_DAYS + 2).total_seconds()),
            )

            await delete_data(_get_last_warning_key(user_id))

            mention_html = str(user.get("mention_html") or escape(str(user.get("nickname") or user_id)))
            silent_days = int(user.get("silent_days") or 0)

            await bot.send_message(
                chat_id=REPORTS_GROUP_ID,
                text=(
                    f"🚪 {mention_html} вилучений із TurboTeam.\n"
                    f"Причина: <b>{silent_days} днів</b> без жодної активності.\n"
                    f"Бан: <b>{AUTO_REMOVE_BAN_DAYS} днів</b>.\n"
                    f"Після завершення блокування бот сам повідомить, що доступ знову відкритий."
                ),
                parse_mode="HTML",
            )

            removed_count += 1

        except Exception as e:
            logger.error(
                f"[TASKS] Failed to auto-remove user_id={user_id}: {e}",
                exc_info=True,
            )

    logger.info(f"[TASKS] Auto-removal finished. Removed: {removed_count}")


@safe_job
async def auto_unban_inactive_users(bot) -> None:
    """
    Daily unban for users whose temporary inactivity ban has expired.
    Sends private notification with return-to-group button.
    """
    logger.info("[TASKS] Auto-unban scan started")

    from cache import redis_client

    if redis_client is None:
        logger.warning("[TASKS] Auto-unban skipped: redis unavailable")
        return

    cursor = 0
    unbanned_count = 0
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
                continue

            user_id = int(payload.get("telegram_user_id") or 0)
            unban_at_raw = str(payload.get("unban_at") or "").strip()

            if not user_id or not unban_at_raw:
                await delete_data(key)
                continue

            try:
                unban_at = datetime.fromisoformat(unban_at_raw)
                if unban_at.tzinfo is None:
                    unban_at = KYIV_TZ.localize(unban_at)
                else:
                    unban_at = unban_at.astimezone(KYIV_TZ)
            except Exception:
                await delete_data(key)
                continue

            if now < unban_at:
                continue

            try:
                await bot.unban_chat_member(
                    chat_id=REPORTS_GROUP_ID,
                    user_id=user_id,
                    only_if_banned=True,
                )
            except Exception as e:
                logger.error(f"[TASKS] Failed to unban user_id={user_id}: {e}", exc_info=True)
                continue

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        "🏎️ Доступ до TurboTeam знову відкритий. "
                        "Якщо хочеш повернутись у стрій — залітай назад і не випадай із гри."
                    ),
                    reply_markup=build_return_group_keyboard(),
                )
            except Exception as e:
                logger.debug(f"[TASKS] Failed to notify unbanned user_id={user_id}: {e}")

            await delete_data(key)
            await delete_data(_get_last_warning_key(user_id))
            unbanned_count += 1

        if cursor == 0:
            break

    logger.info(f"[TASKS] Auto-unban finished. Unbanned: {unbanned_count}")


@safe_job
async def run_sunday_final(bot) -> None:
    """
    20:00 Kyiv every Sunday — weekly final.
    """
    logger.info("[TASKS] Sunday Final started...")
    await sunday_final_logic(bot)
    logger.info("[TASKS] Sunday Final finished.")


# ==============================================================================
# SCHEDULER SETUP
# ==============================================================================

def setup_scheduler(bot) -> AsyncIOScheduler:
    """
    Configures and starts APScheduler.
    Returns scheduler instance.
    """
    kyiv_tz = pytz.timezone("Europe/Kyiv")
    scheduler = AsyncIOScheduler(timezone=kyiv_tz)

    scheduler.add_job(
        send_morning_motivation, "cron", hour=8, minute=0, args=[bot]
    )
    scheduler.add_job(
        auto_unban_inactive_users, "cron", hour=9, minute=0, args=[bot]
    )
    scheduler.add_job(
        inactive_reminder, "cron", hour=11, minute=0, args=[bot]
    )
    scheduler.add_job(
        auto_remove_inactive_users, "cron", hour=12, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_day_motivation, "cron", hour=15, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_last_day_warning, "cron", hour=19, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_evening_motivation, "cron", hour=21, minute=0, args=[bot]
    )
    scheduler.add_job(
        run_sunday_final, "cron", day_of_week="sun", hour=20, minute=0, args=[bot]
    )

    # Temporary one-time Sunday final rerun.
    # Remove this block after 2026-04-26 23:00 Kyiv.
    scheduler.add_job(
        run_sunday_final,
        "cron",
        year=2026,
        month=4,
        day=26,
        hour=23,
        minute=0,
        args=[bot],
        id="manual_sunday_final_2026_04_26_23_00",
        replace_existing=True,
    )

    scheduler.start()

    now_str = datetime.now(kyiv_tz).strftime("%H:%M:%S")
    logger.info(f"[TASKS] Scheduler started. Kyiv time: {now_str}")

    return scheduler