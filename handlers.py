import logging
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from architecture.events import REST_SELECTED, SKIP_SELECTED, VIDEO_UPLOADED
from architecture.events import EventEnvelope
from architecture.orchestrator import flow_event_bus
from config import ADMIN_IDS
from referral import send_invite_prompt
from ratings import show_rating_for_user

router = Router()
logger = logging.getLogger(__name__)


@router.message(F.text == "🏆 Рейтинг ТОП")
async def handle_show_rating_message(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(Command("rating"))
async def handle_show_rating_command(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.callback_query(F.data == "invite_friend")
async def handle_invite_friend(callback: CallbackQuery):
    await send_invite_prompt(callback.message, callback.from_user)
    await callback.answer()


@router.callback_query(F.data == "community_rules")
async def handle_community_rules(callback: CallbackQuery):
    rules_text = (
        "📘 Правила TurboTeam\n\n"
        "• Кидай тільки свіжі кружечки\n"
        "• Не заливай фейки й старі відео\n"
        "• Не спам у чаті та по кнопках\n"
        "• Спілкуйся нормально, без токсичності\n"
        "• Без політики та сварок\n"
        "• За порушення можуть зняти HP"
    )
    await callback.answer(rules_text, show_alert=True)


@router.callback_query(F.data == "turbo_rules")
async def handle_turbo_rules(callback: CallbackQuery):
    text = (
        "📘 *Правила користування TurboTeam*\n\n"
        "1. Щоб отримати HP за тренування, кидай тільки свіжий кружечок після вибору Gym або Street.\n\n"
        "2. Переслані, старі або фейкові відео не зараховуються.\n\n"
        "3. За день можна зробити Gym і Street окремо, але один і той самий тип тренування двічі не рахується.\n\n"
        "4. Якщо на сьогодні вже є активність, відпочинок або пропуск вдруге не записуються.\n\n"
        "5. Не спам кнопками, не кидай фейки й не намагайся накрутити HP.\n\n"
        "6. Якщо учасники поскаржаться на фейковий звіт, можуть зняти HP і доведеться перездати тренування.\n\n"
        "7. Спілкуйся нормально: без образ, токсичності, сварок і політики.\n\n"
        "8. TurboTeam — це про дисципліну, чесність і рух уперед. Тренуйся чесно й кайфуй від прогресу 💪"
    )

    try:
        await callback.message.answer(text, parse_mode="Markdown")
        await callback.answer()
    except Exception as e:
        logger.error(f"[HANDLERS] handle_turbo_rules error: {e}", exc_info=True)
        await callback.answer("⚠️ Не вдалося відкрити правила. Спробуй ще раз.", show_alert=True)


@router.callback_query(F.data.in_(["action_rest", "action_skip"]))
async def handle_static_actions(callback: CallbackQuery):
    event_name = REST_SELECTED if callback.data == "action_rest" else SKIP_SELECTED

    try:
        await callback.answer()

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
    except Exception as e:
        logger.error(f"[HANDLERS] handle_static_actions error: {e}", exc_info=True)
        await callback.message.answer("⚠️ Сталася помилка. Спробуй ще раз.")


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