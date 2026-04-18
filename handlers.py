import logging
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from architecture.events import REST_SELECTED, SKIP_SELECTED, VIDEO_UPLOADED
from architecture.events import EventEnvelope
from architecture.orchestrator import flow_event_bus
from config import ADMIN_IDS
from database import get_user_stats
from referral import send_invite_prompt
from ratings import show_rating_for_user
from supabase_db import get_user_by_telegram_id, get_user_activities, get_referrals_count

router = Router()
logger = logging.getLogger(__name__)


@router.message(F.text == "🏆 Рейтинг ТОП")
async def handle_show_rating_message(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(Command("rating"))
async def handle_show_rating_command(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(F.text == "👤 Мій профіль")
async def handle_my_profile(message: Message):
    try:
        telegram_user_id = message.from_user.id

        stats = await get_user_stats(telegram_user_id)
        user_row = await get_user_by_telegram_id(telegram_user_id)

        if not stats or not user_row:
            await message.answer("⚠️ Профіль не знайдено. Спробуй ще раз пізніше.")
            return

        user_uuid = user_row.get("id")
        if not user_uuid:
            await message.answer("⚠️ Профіль не знайдено. Спробуй ще раз пізніше.")
            return

        activities = await get_user_activities(str(user_uuid), limit=1000)
        referrals_count = await get_referrals_count(str(user_uuid))

        gym_count = 0
        street_count = 0
        rest_count = 0
        skip_count = 0

        for activity in activities:
            action_name = str(activity.get("action_name", ""))

            if action_name == "Gym":
                gym_count += 1
            elif action_name == "Street":
                street_count += 1
            elif action_name == "Rest":
                rest_count += 1
            elif action_name == "Skipped":
                skip_count += 1

        nickname = user_row.get("nickname") or message.from_user.first_name
        hp_total = int(stats.get("hp_total", 0) or 0)
        streak = int(stats.get("streak", 0) or 0)
        activities_count = int(stats.get("activities_count", 0) or 0)

        text = (
            f"👤 *МІЙ ПРОФІЛЬ*\n\n"
            f"🏷️ Нік: *{nickname}*\n"
            f"⚡ Загальний HP: *{hp_total}*\n"
            f"🔥 Streak: *{streak}*\n"
            f"📊 Усього активностей: *{activities_count}*\n\n"
            f"🏋️ Gym: *{gym_count}*\n"
            f"🦾 Street: *{street_count}*\n"
            f"🧘 Rest: *{rest_count}*\n"
            f"🚫 Skip: *{skip_count}*\n\n"
            f"🚀 Реферали: *{referrals_count}*"
        )

        await message.answer(text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"[HANDLERS] handle_my_profile error: {e}", exc_info=True)
        await message.answer("⚠️ Не вдалося завантажити профіль. Спробуй ще раз.")


@router.callback_query(F.data == "invite_friend")
async def handle_invite_friend(callback: CallbackQuery):
    await send_invite_prompt(callback.message, callback.from_user)
    await callback.answer()


@router.callback_query(F.data == "community_rules")
async def handle_community_rules(callback: CallbackQuery):
    rules_text = (
        "📘 Правила TurboTeam\n\n"
        "• Тільки свіжі кружечки\n"
        "• Без фейків\n"
        "• Без спаму\n"
        "• Без токсичності\n"
        "• За порушення знімаємо HP"
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