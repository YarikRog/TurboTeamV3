import logging
from datetime import datetime
from typing import Any, Optional

import pytz
from aiogram import Bot, Router
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from cache import KeyManager, acquire_lock, get_data, set_data, delete_data
from config import REPORTS_GROUP_ID, PROFILE_WEB_APP_SHORT_NAME
from database import get_kyiv_now, update_user_activity
from supabase_db import get_user_by_telegram_id, get_user_activities

router = Router()
logger = logging.getLogger(__name__)

KYIV_TZ = pytz.timezone("Europe/Kyiv")

REPORT_THRESHOLD = 3
REPORT_TTL = 172800  # 48 hours

STREAK_BONUS_ACTION_PREFIX = "🔥 Streak Bonus"
STREAK_BONUS_ROLLBACK_PREFIX = "🔥 Streak Bonus Rollback"

USER_WARNING_TEXT = (
    "⚠️ Твоє останнє тренування не було зараховане.\n"
    "Схоже, відео не підтверджує тренування або було надіслане не за правилами.\n"
    "HP за цю спробу скасовано. Ти можеш ще раз пройти тренування сьогодні й надіслати коректний кружечок."
)


class ReportCallback(CallbackData, prefix="rep"):
    target_uid: int
    action_type: str


def build_report_keyboard(
    target_uid: int,
    action_type: str,
    reports_count: int = 0,
    bot_username: Optional[str] = None,
) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                text=f"🚩 Поскаржитись ({reports_count}/{REPORT_THRESHOLD})",
                callback_data=ReportCallback(
                    target_uid=target_uid,
                    action_type=action_type,
                ).pack(),
            )
        ]
    ]

    if bot_username:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text="👤 Мій профіль",
                    url=f"https://t.me/{bot_username}/{PROFILE_WEB_APP_SHORT_NAME}",
                ),
            ]
        )

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _parse_activity_created_at(value: Any) -> Optional[datetime]:
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


def _is_streak_bonus_action(action_name: str) -> bool:
    action = str(action_name or "").strip()
    return action.startswith(STREAK_BONUS_ACTION_PREFIX) and not action.startswith(STREAK_BONUS_ROLLBACK_PREFIX)


def _is_streak_bonus_rollback_action(action_name: str) -> bool:
    action = str(action_name or "").strip()
    return action.startswith(STREAK_BONUS_ROLLBACK_PREFIX)


def _is_valid_training_action(action_name: str) -> bool:
    action = str(action_name or "").strip()
    return action in {"Gym", "Street"}


async def _rollback_today_streak_bonus_if_needed(
    *,
    target_uid: int,
    date_str: str,
    rejected_action_type: str,
) -> int:
    """
    Rolls back one positive streak bonus for target user on selected Kyiv date
    only if rejected training was the only valid Gym/Street training for that date.

    Supports new weekly streak bonus names:
    - 🔥 Streak Bonus (3/14)
    - 🔥 Streak Bonus (7/14)
    - 🔥 Streak Bonus (14/14)

    Returns rolled back HP amount:
    - 0 if no bonus was found, rollback was already done, or another valid training remains
    - positive number if rollback activity was created
    """
    try:
        user_row = await get_user_by_telegram_id(target_uid)
        if not user_row:
            logger.warning("[REPORTS] streak rollback skipped: user not found target_uid=%s", target_uid)
            return 0

        user_uuid = user_row.get("id")
        if not user_uuid:
            logger.warning("[REPORTS] streak rollback skipped: user uuid missing target_uid=%s", target_uid)
            return 0

        activities = await get_user_activities(str(user_uuid), limit=1000)

        valid_training_count = 0
        bonus_rows = []
        rollback_rows = []

        for activity in activities:
            action_name = str(activity.get("action_name") or "").strip()
            created_at = _parse_activity_created_at(activity.get("created_at"))

            if not created_at:
                continue

            if created_at.strftime("%Y-%m-%d") != date_str:
                continue

            if _is_valid_training_action(action_name):
                valid_training_count += 1

            if _is_streak_bonus_action(action_name):
                try:
                    hp_change = int(activity.get("hp_change") or 0)
                except Exception:
                    hp_change = 0

                if hp_change > 0:
                    bonus_rows.append(activity)

            elif _is_streak_bonus_rollback_action(action_name):
                rollback_rows.append(activity)

        if valid_training_count > 1:
            logger.info(
                "[REPORTS] streak rollback skipped: another valid training exists target_uid=%s date=%s rejected_action=%s valid_count=%s",
                target_uid,
                date_str,
                rejected_action_type,
                valid_training_count,
            )
            return 0

        if not bonus_rows:
            return 0

        if len(rollback_rows) >= len(bonus_rows):
            logger.info(
                "[REPORTS] streak rollback skipped: already rolled back target_uid=%s date=%s bonuses=%s rollbacks=%s",
                target_uid,
                date_str,
                len(bonus_rows),
                len(rollback_rows),
            )
            return 0

        bonus_row = bonus_rows[-1]
        bonus_action_name = str(bonus_row.get("action_name") or "Streak Bonus").strip()

        try:
            bonus_hp = int(bonus_row.get("hp_change") or 0)
        except Exception:
            bonus_hp = 0

        if bonus_hp <= 0:
            return 0

        rollback_video_id = f"streak_rollback:{target_uid}:{date_str}:{bonus_action_name}:{bonus_hp}"

        rollback_ok = await update_user_activity(
            user_id=target_uid,
            nickname="system",
            action_name=f"{STREAK_BONUS_ROLLBACK_PREFIX} ({bonus_action_name})",
            hp_change=-abs(bonus_hp),
            video_id=rollback_video_id,
            is_check=False,
            skip_lock=True,
        )

        if not rollback_ok or rollback_ok == "already_done":
            logger.warning(
                "[REPORTS] streak rollback write failed target_uid=%s date=%s bonus_hp=%s",
                target_uid,
                date_str,
                bonus_hp,
            )
            return 0

        logger.info(
            "[REPORTS] streak bonus rolled back target_uid=%s date=%s bonus_hp=%s bonus_action=%s",
            target_uid,
            date_str,
            bonus_hp,
            bonus_action_name,
        )

        return abs(bonus_hp)

    except Exception as e:
        logger.error(
            "[REPORTS] _rollback_today_streak_bonus_if_needed failed target_uid=%s date=%s error=%s",
            target_uid,
            date_str,
            e,
            exc_info=True,
        )
        return 0


async def rollback_training_report(
    *,
    bot: Bot,
    group_message_id: int,
    moderator_name: str = "адмін",
    reason: str = "manual_reject",
    send_group_status: bool = False,
) -> bool:
    meta = await get_data(KeyManager.get_report_meta_key(group_message_id))
    if not isinstance(meta, dict):
        logger.warning("[REPORTS] rollback meta not found for group_message_id=%s", group_message_id)
        return False

    target_uid = int(meta.get("target_uid") or 0)
    action_type = str(meta.get("action_type") or "")
    hp = int(meta.get("hp") or 0)
    video_id = str(meta.get("video_id") or "")
    date_str = str(meta.get("date_str") or get_kyiv_now().strftime("%Y-%m-%d"))
    rollback_video_id = video_id or "no_video_id"
    group_chat_id = int(meta.get("group_chat_id") or REPORTS_GROUP_ID)
    video_group_message_id = meta.get("video_group_message_id")
    text_group_message_id = meta.get("text_group_message_id")

    if not target_uid or not action_type or hp <= 0:
        logger.warning("[REPORTS] rollback invalid meta for group_message_id=%s meta=%s", group_message_id, meta)
        return False

    rollback_key = KeyManager.get_training_rollback_key(
        target_uid,
        date_str,
        action_type,
        rollback_video_id,
    )
    rollback_lock_key = KeyManager.get_training_rollback_lock_key(
        target_uid,
        date_str,
        action_type,
        rollback_video_id,
    )

    rollback_lock = await acquire_lock(rollback_lock_key, ex=REPORT_TTL)
    if not rollback_lock:
        logger.info(
            "[REPORTS] rollback already processed/in progress target_uid=%s action=%s msg_id=%s",
            target_uid,
            action_type,
            group_message_id,
        )
        return False

    rollback_activity_id = f"rollback:{target_uid}:{date_str}:{action_type}:{rollback_video_id}"
    rollback_ok = await update_user_activity(
        user_id=target_uid,
        nickname="system",
        action_name=f"{action_type} Rollback",
        hp_change=-abs(hp),
        video_id=rollback_activity_id,
        is_check=False,
        skip_lock=True,
    )

    if not rollback_ok or rollback_ok == "already_done":
        await delete_data(rollback_lock_key)
        logger.warning(
            "[REPORTS] rollback activity write failed target_uid=%s action=%s msg_id=%s",
            target_uid,
            action_type,
            group_message_id,
        )
        return False

    streak_rollback_hp = await _rollback_today_streak_bonus_if_needed(
        target_uid=target_uid,
        date_str=date_str,
        rejected_action_type=action_type,
    )

    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Gym:{date_str}"))
    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Street:{date_str}"))
    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Rest:{date_str}"))
    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Skipped:{date_str}"))

    await delete_data(KeyManager.get_training_repeat_key(target_uid, f"Gym:{date_str}"))
    await delete_data(KeyManager.get_training_repeat_key(target_uid, f"Street:{date_str}"))
    await delete_data(KeyManager.get_training_repeat_key(target_uid, f"Rest:{date_str}"))
    await delete_data(KeyManager.get_training_repeat_key(target_uid, f"Skipped:{date_str}"))

    await delete_data(f"training_count:{target_uid}")
    await delete_data(rollback_key)

    if video_group_message_id:
        await delete_data(KeyManager.get_report_meta_key(int(video_group_message_id)))
    if text_group_message_id:
        await delete_data(KeyManager.get_report_meta_key(int(text_group_message_id)))

    try:
        if video_group_message_id:
            await bot.delete_message(chat_id=group_chat_id, message_id=int(video_group_message_id))
    except Exception as e:
        logger.debug(f"[REPORTS] Failed to delete group video msg: {e}")

    try:
        if text_group_message_id:
            await bot.delete_message(chat_id=group_chat_id, message_id=int(text_group_message_id))
    except Exception as e:
        logger.debug(f"[REPORTS] Failed to delete group text msg: {e}")

    try:
        if streak_rollback_hp > 0:
            await bot.send_message(
                chat_id=target_uid,
                text=(
                    USER_WARNING_TEXT
                    + "\n\n"
                    + f"🔥 Також скасовано streak bonus: -{streak_rollback_hp} HP."
                ),
            )
        else:
            await bot.send_message(chat_id=target_uid, text=USER_WARNING_TEXT)
    except Exception as e:
        logger.debug(f"[REPORTS] Failed to notify target_uid={target_uid}: {e}")

    if send_group_status:
        try:
            extra = ""
            if streak_rollback_hp > 0:
                extra = f"\n🔥 Streak bonus також скасовано: -{streak_rollback_hp} HP"

            await bot.send_message(
                chat_id=group_chat_id,
                text=(
                    f"🚫 Тренування скасовано: -{hp} HP\n"
                    f"Причина: {reason}\n"
                    f"Модератор: {moderator_name}\n"
                    f"Користувач може перездати тренування ще раз сьогодні."
                    f"{extra}"
                ),
            )
        except Exception as e:
            logger.warning(f"[REPORTS] Failed to send rollback status message: {e}")

    return True


@router.callback_query(ReportCallback.filter())
async def handle_report(callback: CallbackQuery, callback_data: ReportCallback):
    voter = callback.from_user
    target_uid = int(callback_data.target_uid)
    action_type = str(callback_data.action_type)

    if not callback.message:
        await callback.answer("⚠️ Повідомлення не знайдено.", show_alert=True)
        return

    report_msg_id = callback.message.message_id

    if voter.id == target_uid:
        await callback.answer("❌ Не можна скаржитися на себе.", show_alert=True)
        return

    penalty_key = KeyManager.get_report_penalty_key(target_uid, report_msg_id)
    if (await get_data(penalty_key)) is not None:
        await callback.answer("⚠️ Штраф за це відео вже застосовано.", show_alert=True)
        return

    report_key = KeyManager.get_report_key(target_uid, report_msg_id)
    raw_reports = await get_data(report_key)

    if isinstance(raw_reports, list):
        voters = raw_reports
    else:
        voters = []

    if voter.id in voters:
        await callback.answer("⚠️ Ти вже скаржився на це відео.", show_alert=True)
        return

    vote_key = KeyManager.get_report_vote_key(target_uid, report_msg_id, voter.id)
    vote_lock = await acquire_lock(vote_key, ex=REPORT_TTL)
    if not vote_lock:
        await callback.answer("⚠️ Ти вже скаржився на це відео.", show_alert=True)
        return

    voters.append(voter.id)

    await set_data(report_key, voters, ex=REPORT_TTL)
    current_count = len(voters)

    if current_count < REPORT_THRESHOLD:
        try:
            await callback.message.edit_reply_markup(
                reply_markup=build_report_keyboard(
                    target_uid=target_uid,
                    action_type=action_type,
                    reports_count=current_count,
                )
            )
        except Exception as e:
            logger.debug(f"[REPORTS] Failed to update report counter: {e}")

        await callback.answer(
            f"🚩 Скаргу зараховано ({current_count}/{REPORT_THRESHOLD})",
            show_alert=True,
        )
        return

    penalty_lock = await acquire_lock(penalty_key, ex=REPORT_TTL)
    if not penalty_lock:
        await callback.answer("⚠️ Штраф за це відео вже обробляється.", show_alert=True)
        return

    try:
        await callback.message.edit_reply_markup(
            reply_markup=build_report_keyboard(
                target_uid=target_uid,
                action_type=action_type,
                reports_count=current_count,
            )
        )
    except Exception as e:
        logger.debug(f"[REPORTS] Failed to update final report counter: {e}")

    rollback_ok = await rollback_training_report(
        bot=callback.bot,
        group_message_id=report_msg_id,
        moderator_name=f"community:{current_count}_reports",
        reason="community_reports",
    )

    if not rollback_ok:
        await delete_data(penalty_key)
        await callback.answer("⚠️ Не вдалося скасувати тренування. Спробуй ще раз.", show_alert=True)
        return

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.answer(
        "✅ Поріг скарг досягнуто. Тренування скасовано, юзеру дозволено перездати.",
        show_alert=True,
    )
