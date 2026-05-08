import logging
from datetime import datetime, timedelta
from html import escape

import pytz

from alerts import notify_admins_about_error
from config import REPORTS_GROUP_ID
from database import (
    get_inactive_users,
    get_users_for_last_warning,
    get_users_for_auto_removal,
    get_kyiv_now,
)
from supabase_db import get_all_users, get_user_activities
from cache import get_data, set_data, delete_data
from jobs.common import (
    KYIV_TZ,
    AUTO_REMOVE_BAN_DAYS,
    AUTO_REMOVE_REDIS_PREFIX,
    SECOND_DAY_REMINDER_DAYS,
    SECOND_DAY_REMINDER_TTL_SECONDS,
    _get_auto_removed_key,
    _get_last_warning_key,
    _get_second_day_reminder_key,
    _get_last_real_activity_date,
    _get_last_activity_marker,
    _send_final_warning_message,
    build_second_day_reminder_keyboard,
    build_training_action_keyboard,
    build_return_group_keyboard,
    safe_job,
)

logger = logging.getLogger(__name__)


@safe_job
async def send_second_day_private_reminder(bot) -> None:
    """
    19:30 Kyiv every day — private reminder for users with exactly 2 days
    without real activity: Gym, Street, Rest, Skipped.

    Sends once per inactivity cycle.
    Cycle marker is based on last real activity date, not current date.
    """
    try:
        users = await get_all_users()
        if not users:
            logger.info("[TASKS] Second-day private reminder: no users")
            return

        today = get_kyiv_now().date()
        sent_count = 0
        skipped_count = 0

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
                skipped_count += 1
                continue

            try:
                activities = await get_user_activities(str(user_uuid), limit=1000)
            except Exception as e:
                logger.error(
                    f"[TASKS] Failed to get activities for second-day reminder user_id={user_id}: {e}",
                    exc_info=True,
                )
                await notify_admins_about_error(
                    bot=bot,
                    place="tasks.send_second_day_private_reminder.get_user_activities",
                    error=e,
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

            last_activity_marker = _get_last_activity_marker(last_activity_date)
            reminder_key = _get_second_day_reminder_key(user_id, last_activity_marker)

            already_sent = await get_data(reminder_key)
            if already_sent is not None:
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
                await set_data(
                    reminder_key,
                    "1",
                    ex=SECOND_DAY_REMINDER_TTL_SECONDS,
                )
                sent_count += 1

                logger.info(
                    "[TASKS] Second-day private reminder sent. user_id=%s silent_days=%s marker=%s",
                    user_id,
                    silent_days,
                    last_activity_marker,
                )
            except Exception as e:
                logger.debug(
                    f"[TASKS] Failed to send second-day private reminder user_id={user_id}: {e}"
                )
                skipped_count += 1

        logger.info(
            "[TASKS] Second-day private reminder finished. Sent: %s, skipped: %s",
            sent_count,
            skipped_count,
        )

    except Exception as e:
        logger.error(f"[TASKS] Second-day private reminder failed: {e}", exc_info=True)
        await notify_admins_about_error(
            bot=bot,
            place="tasks.send_second_day_private_reminder",
            error=e,
        )


@safe_job
async def inactive_reminder(bot) -> None:
    """
    11:00 Kyiv every day — mention users inactive for 3 days.
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
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info(f"[TASKS] Inactive reminder triggered for {len(inactive_list)} users")


@safe_job
async def send_last_day_warning(bot) -> None:
    """
    19:00 Kyiv every day — final warning for users with 7+ days without activity.
    Repeats are blocked by Redis key.
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

        ok = await _send_final_warning_message(
            bot=bot,
            user=user,
            place="tasks.send_last_day_warning",
        )
        if ok:
            warned_count += 1

    logger.info(f"[TASKS] Last-day warning finished. Warned: {warned_count}")


@safe_job
async def auto_remove_inactive_users(bot) -> None:
    """
    Daily auto-removal for users with 8+ days without real activity.

    Safety rule:
    User cannot be removed if final warning was not sent before.
    If user is removable but warning key is missing, bot sends warning first and skips removal.
    """
    removable_users = await get_users_for_auto_removal()
    if not removable_users:
        logger.info("[TASKS] Auto-removal: no users to remove")
        return

    now = get_kyiv_now()
    ban_until = now + timedelta(days=AUTO_REMOVE_BAN_DAYS)
    removed_count = 0
    postponed_count = 0

    for user in removable_users:
        user_id = int(user["telegram_user_id"])
        user_key = _get_auto_removed_key(user_id)

        existing = await get_data(user_key)
        if existing is not None:
            continue

        warned_key = _get_last_warning_key(user_id)
        already_warned = await get_data(warned_key)

        if already_warned is None:
            ok = await _send_final_warning_message(
                bot=bot,
                user=user,
                place="tasks.auto_remove_inactive_users.postponed",
            )

            if ok:
                postponed_count += 1
                logger.info(
                    "[TASKS] Auto-removal postponed. Warning sent first. user_id=%s silent_days=%s",
                    user_id,
                    int(user.get("silent_days") or 0),
                )

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
            await notify_admins_about_error(
                bot=bot,
                place=f"tasks.auto_remove_inactive_users.user_id_{user_id}",
                error=e,
            )

    logger.info(
        "[TASKS] Auto-removal finished. Removed: %s, postponed: %s",
        removed_count,
        postponed_count,
    )


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
                await notify_admins_about_error(
                    bot=bot,
                    place=f"tasks.auto_unban_inactive_users.unban_user_id_{user_id}",
                    error=e,
                )
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


