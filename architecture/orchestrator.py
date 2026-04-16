import logging

from aiogram import types
from aiogram.types import CallbackQuery, Message

from architecture.event_bus import EventBus
from architecture.events import (
    EventEnvelope,
    PENALTY_APPLIED,
    REST_SELECTED,
    SKIP_SELECTED,
    TRAINING_SELECTED,
    USER_REGISTERED,
    VIDEO_UPLOADED,
)
from architecture.state_machine import UserFlowState, state_machine
from cache import KeyManager, delete_data, get_data, set_flag
from config import GROUP_LINK, HP_REST, HP_SKIP, REPORTS_GROUP_ID
from database import get_kyiv_now
from database import register_user_from_quiz
from phrases import get_phrase
from referral import process_referral_logic
from services import ActivityService, auto_delete, safe_create_task
from ui import get_inline_menu

logger = logging.getLogger(__name__)

flow_event_bus = EventBus()


def mention(user: types.User) -> str:
    return f"@{user.username or user.first_name}"


async def _reply_transport(source: Message | CallbackQuery, text: str, show_alert: bool = False):
    if isinstance(source, CallbackQuery):
        if show_alert:
            await source.answer(text, show_alert=True)
            return None

        sent = await source.message.answer(text)
        await source.answer()
        return sent

    return await source.answer(text)


async def on_user_registered(event: EventEnvelope) -> bool:
    message: Message = event.payload["message"]
    quiz_data = event.payload["quiz_data"]
    user_id = event.user_id
    nickname = event.payload["nickname"]

    success = await register_user_from_quiz(user_id, nickname, quiz_data)
    if not success:
        await message.answer("â ï¸ Ð¢Ð¸ Ð²Ð¶Ðµ Ð² Ð±Ð°Ð·Ñ.")
        return False

    await set_flag(KeyManager.get_reg_key(user_id), ex=86400)
    await state_machine.register_user(user_id)

    ref_key = KeyManager.get_ref_key(user_id)
    referrer_id = await get_data(ref_key)
    if referrer_id:
        await delete_data(ref_key)
        safe_create_task(
            process_referral_logic(user_id, nickname, int(str(referrer_id)), message.bot),
            name=f"referral_{user_id}",
        )

    await message.answer("â ÐÐÐ¢ÐÐÐÐ Ð ÐÐÐÐÐÐÐ!", reply_markup=types.ReplyKeyboardRemove())

    group_kb = types.InlineKeyboardMarkup(
        inline_keyboard=[[
            types.InlineKeyboardButton(text="ÐÐ¥ÐÐ Ð£ ÐÐ Ð£ÐÐ£ ðï¸", url=GROUP_LINK),
        ]]
    )
    await message.answer("Ð¢ÑÐ¸Ð¼Ð°Ð¹ Ð¿ÐµÑÐµÐ¿ÑÑÑÐºÑ: ð", reply_markup=group_kb)

    user_mention = mention(message.from_user)
    await message.bot.send_message(
        REPORTS_GROUP_ID,
        get_phrase("welcome", mention=user_mention) + "\n\nð *ÐÐ±Ð¸ÑÐ°Ð¹ ÑÑÐµÐ½ÑÐ²Ð°Ð½Ð½Ñ:*",
        reply_markup=get_inline_menu((await message.bot.get_me()).username),
    )
    return True


async def on_training_selected(event: EventEnvelope) -> bool:
    source = event.payload["source"]
    action = event.payload["action"]
    user = event.payload["user"]

    if await ActivityService.check_today_report(event.user_id, ignore_actions=["Ð ÐµÑÑÑÑÐ°ÑÑÑ"]):
        today = get_kyiv_now().strftime("%Y-%m-%d")
        repeat_key = KeyManager.get_training_repeat_key(event.user_id, today)
        repeat_count_raw = await get_data(repeat_key)
        repeat_count = int(str(repeat_count_raw)) if repeat_count_raw is not None else 0

        if repeat_count >= 1:
            return False

        await set_flag(
            repeat_key,
            ex=ActivityService.get_seconds_until_kyiv_midnight(),
        )
        await _reply_transport(source, get_phrase("stop", nickname=mention(user)), show_alert=isinstance(source, CallbackQuery))
        return False

    started = await state_machine.begin_training(event.user_id, action, ttl=120)
    if not started:
        await _reply_transport(source, "â ï¸ ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð°ÐºÑÐ¸Ð²ÑÐ²Ð°ÑÐ¸ ÑÐµÑÑÑ. Ð¡Ð¿ÑÐ¾Ð±ÑÐ¹ ÑÐµ ÑÐ°Ð·.")
        return False

    msg = await _reply_transport(source, get_phrase("training_start", nickname=mention(user)))
    if msg is not None:
        safe_create_task(auto_delete(msg, 15), name=f"auto_delete_start_{event.user_id}")
    return True


async def _handle_static_action(event: EventEnvelope, action_name: str, hp: int, phrase_key: str) -> bool:
    source = event.payload["source"]
    user = event.payload["user"]

    if await ActivityService.check_today_report(event.user_id, ignore_actions=["Ð ÐµÑÑÑÑÐ°ÑÑÑ"]):
        await _reply_transport(source, "Ð¡ÑÐ¾Ð³Ð¾Ð´Ð½Ñ Ð°ÐºÑÐ¸Ð²Ð½ÑÑÑÑ Ð²Ð¶Ðµ Ð±ÑÐ»Ð°! â", show_alert=isinstance(source, CallbackQuery))
        return False

    ok = await ActivityService.grant_hp(event.user_id, user.full_name, action_name, int(hp))
    if not ok:
        await _reply_transport(
            source,
            "â ï¸ ÐÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð¿Ð¸ÑÑ Ð² ÑÐ°Ð±Ð»Ð¸ÑÑ. Ð¡Ð¿ÑÐ¾Ð±ÑÐ¹ ÑÐµ ÑÐ°Ð·.",
            show_alert=isinstance(source, CallbackQuery),
        )
        return False

    await state_machine.complete(event.user_id)
    await _reply_transport(source, get_phrase(phrase_key, nickname=mention(user)))
    return True


async def on_rest_selected(event: EventEnvelope) -> bool:
    return await _handle_static_action(event, "Rest", HP_REST, "rest")


async def on_skip_selected(event: EventEnvelope) -> bool:
    return await _handle_static_action(event, "Skipped", HP_SKIP, "skip")


async def on_video_uploaded(event: EventEnvelope) -> bool:
    message: Message = event.payload["message"]
    current_state = await state_machine.get_state(event.user_id)
    if current_state != UserFlowState.VIDEO_WAITING:
        await message.answer("â° Ð¡Ð¿Ð¾ÑÐ°ÑÐºÑ Ð²Ð¸Ð±ÐµÑÐ¸ ÑÑÐµÐ½ÑÐ²Ð°Ð½Ð½Ñ Ð² Ð¼ÐµÐ½Ñ!")
        return False

    if message.forward_from or message.forward_date:
        await message.answer("â Ð¢ÑÐ»ÑÐºÐ¸ ÑÐ²ÑÐ¶Ñ ÐºÑÑÐ¶ÐµÑÐºÐ¸!")
        return False

    session_data = await state_machine.get_session(event.user_id)
    if not session_data:
        await message.answer("â° Ð¡ÐµÑÑÑ Ð²Ð¸ÑÐµÑÐ¿Ð°Ð½Ð°. ÐÐ¾ÑÐ½Ð¸ Ð·Ð°Ð½Ð¾Ð²Ð¾.")
        return False

    await state_machine.mark_processing(event.user_id, ttl=30)
    success = await ActivityService.process_training_full_cycle(message, session_data["action"])
    if success:
        await state_machine.complete(event.user_id)
        return True

    await state_machine.restore_video_waiting(event.user_id, ttl=60)
    await message.answer("â ï¸ ÐÐ¾Ð¼Ð¸Ð»ÐºÐ° Ð·Ð°Ð¿Ð¸ÑÑ Ð² ÑÐ°Ð±Ð»Ð¸ÑÑ. Ð¡Ð¿ÑÐ¾Ð±ÑÐ¹ ÑÐµ ÑÐ°Ð·.")
    return False


async def on_penalty_applied(event: EventEnvelope) -> bool:
    await state_machine.penalize(event.user_id)
    return True


flow_event_bus.subscribe(USER_REGISTERED, on_user_registered)
flow_event_bus.subscribe(TRAINING_SELECTED, on_training_selected)
flow_event_bus.subscribe(REST_SELECTED, on_rest_selected)
flow_event_bus.subscribe(SKIP_SELECTED, on_skip_selected)
flow_event_bus.subscribe(VIDEO_UPLOADED, on_video_uploaded)
flow_event_bus.subscribe(PENALTY_APPLIED, on_penalty_applied)
