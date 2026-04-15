import logging
from aiogram import F, Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from architecture.events import REST_SELECTED, SKIP_SELECTED, TRAINING_SELECTED, VIDEO_UPLOADED
from architecture.events import EventEnvelope
from architecture.orchestrator import flow_event_bus
from config import ADMIN_IDS
from referral import send_invite_prompt
from ratings import show_rating_for_user

router = Router()
logger = logging.getLogger(__name__)

@router.message(CommandStart(deep_link=True))
async def cmd_start_deep_link(m: Message, command: CommandObject):
    arg = command.args
    if arg not in ["gym", "street"]:
        return

    await flow_event_bus.publish(
        EventEnvelope(
            name=TRAINING_SELECTED,
            user_id=m.from_user.id,
            payload={
                "source": m,
                "user": m.from_user,
                "action": arg.capitalize(),
            },
            idempotency_key=f"training-select:{m.from_user.id}:{m.message_id}",
        )
    )


@router.callback_query(F.data.in_(["train_gym", "train_street"]))
async def handle_training_selection(callback: CallbackQuery):
    action = "Gym" if callback.data == "train_gym" else "Street"
    await flow_event_bus.publish(
        EventEnvelope(
            name=TRAINING_SELECTED,
            user_id=callback.from_user.id,
            payload={
                "source": callback,
                "user": callback.from_user,
                "action": action,
            },
            idempotency_key=f"training-select:{callback.from_user.id}:{callback.id}",
        )
    )


@router.callback_query(F.data == "show_rating")
async def handle_show_rating(callback: CallbackQuery):
    await show_rating_for_user(callback.message, callback.from_user)
    await callback.answer()


@router.message(F.text == "🏆 Рейтинг ТОП")
async def handle_show_rating_message(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.callback_query(F.data == "invite_friend")
async def handle_invite_friend(callback: CallbackQuery):
    await send_invite_prompt(callback.message, callback.from_user)
    await callback.answer()


@router.callback_query(F.data.in_(["action_rest", "action_skip"]))
async def handle_static_actions(callback: CallbackQuery):
    event_name = REST_SELECTED if callback.data == "action_rest" else SKIP_SELECTED
    await flow_event_bus.publish(
        EventEnvelope(
            name=event_name,
            user_id=callback.from_user.id,
            payload={
                "source": callback,
                "user": callback.from_user,
            },
            idempotency_key=f"{event_name}:{callback.from_user.id}:{callback.id}",
        )
    )


@router.message(F.video_note)
async def gateway_video_note(m: Message):
    await flow_event_bus.publish(
        EventEnvelope(
            name=VIDEO_UPLOADED,
            user_id=m.from_user.id,
            payload={"message": m},
            idempotency_key=f"video:{m.from_user.id}:{m.message_id}",
        )
    )


@router.message(Command("panel"))
async def send_panel(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    bot = await m.bot.get_me()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🏋️ Gym", url=f"https://t.me/{bot.username}?start=gym"),
                InlineKeyboardButton(text="🦾 Street", url=f"https://t.me/{bot.username}?start=street"),
            ],
            [
                InlineKeyboardButton(text="🧘 Rest", callback_data="action_rest"),
                InlineKeyboardButton(text="🚫 Skip", callback_data="action_skip"),
            ],
        ]
    )
    await m.answer("🔥 **TURBO PANEL**", reply_markup=kb)
