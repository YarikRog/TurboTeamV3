import logging

from aiogram import Router
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from architecture.events import EventEnvelope, PENALTY_APPLIED
from cache import KeyManager, acquire_lock, get_data, set_data, delete_data
from database import get_kyiv_now, penalty_user

router = Router()
logger = logging.getLogger(__name__)

REPORT_THRESHOLD = 3
REPORT_PENALTY_HP = 120
REPORT_TTL = 172800  # 48 hours


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

    ok = await penalty_user(target_uid, REPORT_PENALTY_HP)
    if not ok:
        await delete_data(penalty_key)
        await callback.answer("⚠️ Не вдалося застосувати штраф. Спробуй ще раз.", show_alert=True)
        return

    today = get_kyiv_now().strftime("%Y-%m-%d")
    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Gym:{today}"))
    await delete_data(KeyManager.get_action_lock_key(target_uid, f"Street:{today}"))

    from architecture.orchestrator import flow_event_bus

    await flow_event_bus.publish(
        EventEnvelope(
            name=PENALTY_APPLIED,
            user_id=target_uid,
            payload={
                "reason": "community_reports",
                "report_message_id": report_msg_id,
                "action_type": action_type,
                "penalty_hp": REPORT_PENALTY_HP,
                "reports_count": current_count,
            },
            idempotency_key=f"penalty:{target_uid}:{report_msg_id}",
        )
    )

    try:
        text = callback.message.caption or callback.message.text or ""
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(
            f"🚫 За це відео застосовано штраф: -{REPORT_PENALTY_HP} HP\n"
            f"Причина: {current_count} скарги від учасників.\n"
            f"Користувач може перездати тренування ще раз сьогодні."
        )
        if text:
            logger.info(
                "[REPORTS] Penalty applied target_uid=%s msg_id=%s action=%s text_preview=%s",
                target_uid,
                report_msg_id,
                action_type,
                text[:120],
            )
    except Exception as e:
        logger.warning(f"[REPORTS] Failed to update report message UI: {e}")

    await callback.answer(
        f"✅ Поріг скарг досягнуто. Штраф -{REPORT_PENALTY_HP} HP застосовано.",
        show_alert=True,
    )