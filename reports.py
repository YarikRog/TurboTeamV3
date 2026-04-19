import logging

from aiogram import Bot, Router
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from cache import KeyManager, acquire_lock, get_data, set_data, delete_data
from config import REPORTS_GROUP_ID
from database import get_kyiv_now, update_user_activity

router = Router()
logger = logging.getLogger(__name__)

REPORT_THRESHOLD = 3
REPORT_TTL = 172800  # 48 hours

USER_WARNING_TEXT = (
    "⚠️ Твоє останнє тренування не було зараховане.\n"
    "Схоже, відео не підтверджує тренування або було надіслане не за правилами.\n"
    "HP за цю спробу скасовано. Ти можеш ще раз пройти тренування сьогодні й надіслати коректний кружечок."
)


class ReportCallback(CallbackData, prefix="rep"):
    target_uid: int
    action_type: str


def build_report_keyboard(target_uid: int, action_type: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚩 Поскаржитись",
                    callback_data=ReportCallback(
                        target_uid=target_uid,
                        action_type=action_type,
                    ).pack(),
                )
            ]
        ]
    )


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

    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Gym:{date_str}"))
    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Street:{date_str}"))
    await delete_data(KeyManager.get_training_repeat_key(target_uid, f"Gym:{date_str}"))
    await delete_data(KeyManager.get_training_repeat_key(target_uid, f"Street:{date_str}"))
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
        await bot.send_message(chat_id=target_uid, text=USER_WARNING_TEXT)
    except Exception as e:
        logger.debug(f"[REPORTS] Failed to notify target_uid={target_uid}: {e}")

    if send_group_status:
        try:
            await bot.send_message(
                chat_id=group_chat_id,
                text=(
                    f"🚫 Тренування скасовано: -{hp} HP\n"
                    f"Причина: {reason}\n"
                    f"Модератор: {moderator_name}\n"
                    f"Користувач може перездати тренування ще раз сьогодні."
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

    vote_key = KeyManager.get_report_vote_key(target_uid, report_msg_id, voter.id)
    vote_lock = await acquire_lock(vote_key, ex=REPORT_TTL)
    if not vote_lock:
        await callback.answer("⚠️ Ти вже скаржився на це відео.", show_alert=True)
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

    if voter.id not in voters:
        voters.append(voter.id)

    await set_data(report_key, voters, ex=REPORT_TTL)
    current_count = len(voters)

    if current_count < REPORT_THRESHOLD:
        await callback.answer(
            f"🚩 Скаргу зараховано ({current_count}/{REPORT_THRESHOLD})",
            show_alert=True,
        )
        return

    penalty_lock = await acquire_lock(penalty_key, ex=REPORT_TTL)
    if not penalty_lock:
        await callback.answer("⚠️ Штраф за це відео вже обробляється.", show_alert=True)
        return

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
