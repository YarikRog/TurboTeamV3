import logging
import functools
from datetime import datetime, timedelta
from html import escape

import pytz
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from alerts import notify_admins_about_error
from config import REPORTS_GROUP_ID, GROUP_LINK
from phrases import get_phrase
from database import get_weekly_top_users
from cache import set_data

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

AUTO_REMOVE_BAN_DAYS = 7
AUTO_REMOVE_REDIS_PREFIX = "turbo:auto_removed"
LAST_WARNING_REDIS_PREFIX = "turbo:last_warning"
LAST_WARNING_TTL_SECONDS = int(timedelta(days=10).total_seconds())

SECOND_DAY_REMINDER_DAYS = 2
SECOND_DAY_REMINDER_LINK = "https://t.me/turboteampro/3746"
SECOND_DAY_REMINDER_REDIS_PREFIX = "turbo:second_day_reminder"
SECOND_DAY_REMINDER_TTL_SECONDS = int(timedelta(days=14).total_seconds())


def _extract_bot_from_args(args, kwargs):
    """
    Scheduled jobs in this file usually receive bot as the first argument.
    This helper keeps alerts safe if a job signature changes later.
    """
    if args:
        return args[0]

    if kwargs and "bot" in kwargs:
        return kwargs["bot"]

    return None


def safe_job(func):
    """
    Wrapper for APScheduler jobs.
    Ensures one failed job does not break the scheduler.
    Sends short Telegram alert to admins for critical scheduled job errors.
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

            bot = _extract_bot_from_args(args, kwargs)
            await notify_admins_about_error(
                bot=bot,
                place=f"tasks.{func.__name__}",
                error=e,
            )

    return wrapper


# ==============================================================================
# HELPERS
# ==============================================================================

def _get_auto_removed_key(user_id: int) -> str:
    return f"{AUTO_REMOVE_REDIS_PREFIX}:{user_id}"


def _get_last_warning_key(user_id: int) -> str:
    return f"{LAST_WARNING_REDIS_PREFIX}:{user_id}"


def _get_second_day_reminder_key(user_id: int, last_activity_marker: str) -> str:
    return f"{SECOND_DAY_REMINDER_REDIS_PREFIX}:{user_id}:{last_activity_marker}"


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
    return action_name in {"Gym", "Street", "Rest", "Skipped"}


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


def _get_last_activity_marker(last_activity_date) -> str:
    if last_activity_date is None:
        return "no_activity"

    return last_activity_date.isoformat()


async def _send_final_warning_message(bot, user: dict, place: str) -> bool:
    user_id = int(user["telegram_user_id"])
    mention_html = str(user.get("mention_html") or escape(str(user.get("nickname") or user_id)))
    silent_days = int(user.get("silent_days") or 0)

    try:
        await bot.send_message(
            chat_id=REPORTS_GROUP_ID,
            text=(
                f"⚠️ {mention_html}, фінальне попередження перед вилученням.\n"
                f"У тебе вже <b>{silent_days} днів тиші</b>.\n"
                f"Якщо сьогодні не буде жодної дії, завтра бот автоматично вилучить тебе з TurboTeam."
            ),
            parse_mode="HTML",
        )

        await set_data(
            _get_last_warning_key(user_id),
            "1",
            ex=LAST_WARNING_TTL_SECONDS,
        )

        logger.info(
            "[TASKS] Final warning sent. place=%s user_id=%s silent_days=%s",
            place,
            user_id,
            silent_days,
        )
        return True

    except Exception as e:
        logger.error(
            f"[TASKS] Failed to send final warning user_id={user_id}: {e}",
            exc_info=True,
        )
        await notify_admins_about_error(
            bot=bot,
            place=f"{place}.send_final_warning.user_id_{user_id}",
            error=e,
        )
        return False


async def build_top3_text() -> str:
    """
    Builds TOP-3 rating block for scheduled messages.
    HTML-safe version.

    Important:
    This uses the same weekly rating source as the main rating/final logic.
    It includes all weekly HP from the rating RPC:
    Gym, Street, Rest, Skip, referrals, bonuses, penalties, etc.
    """
    try:
        top_list = await get_weekly_top_users(finished_week=False)
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
    """
    Builds HTML-safe motivation text.
    Escapes phrase text because phrases are plain text, not HTML.
    """
    phrase = escape(str(get_phrase(phrase_key)))
    return phrase + top3


def build_html_phrase_text(phrase_key: str, top3: str) -> str:
    """
    Builds text for phrase categories that intentionally contain safe HTML tags.
    Use only for internally controlled phrases, not user-generated content.
    """
    phrase = str(get_phrase(phrase_key))
    return phrase + top3


