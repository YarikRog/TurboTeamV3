import logging

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message

from architecture.events import REST_SELECTED, SKIP_SELECTED, VIDEO_UPLOADED, EventEnvelope
from architecture.orchestrator import flow_event_bus
from services import safe_create_task, auto_delete

router = Router()
logger = logging.getLogger(__name__)


@router.callback_query(F.data.in_(["action_rest", "action_skip"]))
async def handle_static_actions(callback: CallbackQuery):
    event_name = REST_SELECTED if callback.data == "action_rest" else SKIP_SELECTED
    try:
        await flow_event_bus.publish(
            EventEnvelope(
                name=event_name,
                user_id=callback.from_user.id,
                payload={"source": callback, "user": callback.from_user},
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
