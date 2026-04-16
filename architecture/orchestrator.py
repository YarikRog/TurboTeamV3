import logging
import time

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
from database import get_kyiv_now, register_user_from_quiz, check_user_exists
from phrases import get_phrase
from referral import process_referral_logic
from services import ActivityService, auto_delete, safe_create_task
from ui import get_inline_menu

logger = logging.getLogger(__name__)

flow_event_bus = EventBus()


def mention(user: types.User) -> str:
    return f"@{user.username or user.first_name}"


def _ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


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
    total_started = time.perf_counter()

    message: Message = event.payload["message"]
    quiz_data = event.payload["quiz_data"]
    user_id = event.user_id
    nickname = event.payload["nickname"]

    logger.info("[REG] Start registration flow user_id=%s", user_id)

    t = time.perf_counter()
    already_exists = await check_user_exists(user_id)
    logger.info("[REG] check_user_exists user_id=%s took %sms", user_id, _ms(t))

    if already_exists:
        await message.answer("⚠️ Ти вже в базі.")
        logger.info("[REG] user already exists user_id=%s total=%sms", user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    success = await register_user_from_quiz(user_id, nickname, quiz_data)
    logger.info("[REG] register_user_from_quiz user_id=%s took %sms", user_id, _ms(t))

    if not success:
        await message.answer("⚠️ Не вдалося завершити реєстрацію. Спробуй ще раз.")
        logger.info("[REG] registration failed user_id=%s total=%sms", user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    await set_flag(KeyManager.get_reg_key(user_id), ex=86400)
    logger.info("[REG] set reg flag user_id=%s took %sms", user_id, _ms(t))

    t = time.perf_counter()
    await state_machine.register_user(user_id)
    logger.info("[REG] state_machine.register_user user_id=%s took %sms", user_id, _ms(t))

    t = time.perf_counter()
    ref_key = KeyManager.get_ref_key(user_id)
    referrer_id = await get_data(ref_key)
    logger.info("[REG] get pending ref user_id=%s took %sms", user_id, _ms(t))

    if referrer_id:
        t = time.perf_counter()
        await delete_data(ref_key)
        logger.info("[REG] delete pending ref user_id=%s took %sms", user_id, _ms(t))

        t = time.perf_counter()
        safe_create_task(
            process_referral_logic(user_id, nickname, int(str(referrer_id)), message.bot),
            name=f"referral_{user_id}",
        )
        logger.info("[REG] referral task scheduled user_id=%s took %sms", user_id, _ms(t))

    t = time.perf_counter()
    await message.answer("✅ ВІТАЄМО В КОМАНДІ!", reply_markup=types.ReplyKeyboardRemove())
    logger.info("[REG] welcome answer user_id=%s took %sms", user_id, _ms(t))

    group_kb = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="ВХІД У ГРУПУ 🏎️", url=GROUP_LINK),
            ],
            [
                types.InlineKeyboardButton(
                    text="📘 Правила користування TurboTeam",
                    callback_data="turbo_rules",
                ),
            ],
        ]
    )

    t = time.perf_counter()
    await message.answer("Тримай перепустку: 👇", reply_markup=group_kb)
    logger.info("[REG] pass answer user_id=%s took %sms", user_id, _ms(t))

    user_mention = mention(message.from_user)

    t = time.perf_counter()
    bot_me = await message.bot.get_me()
    logger.info("[REG] bot.get_me user_id=%s took %sms", user_id, _ms(t))

    t = time.perf_counter()
    await message.bot.send_message(
        REPORTS_GROUP_ID,
        get_phrase("welcome", mention=user_mention) + "\n\n🚀 *Обирай тренування:*",
        reply_markup=get_inline_menu(bot_me.username),
    )
    logger.info("[REG] group welcome send user_id=%s took %sms", user_id, _ms(t))

    logger.info("[REG] Finished registration flow user_id=%s total=%sms", user_id, _ms(total_started))
    return True


async def on_training_selected(event: EventEnvelope) -> bool:
    total_started = time.perf_counter()

    source = event.payload["source"]
    action = event.payload["action"]
    user = event.payload["user"]

    t = time.perf_counter()
    can_log = await ActivityService.can_user_log_activity(event.user_id, action)
    logger.info(
        "[TRAIN] can_user_log_activity user_id=%s action=%s took %sms",
        event.user_id,
        action,
        _ms(t),
    )

    if not can_log:
        today = get_kyiv_now().strftime("%Y-%m-%d")

        t = time.perf_counter()
        repeat_key = KeyManager.get_training_repeat_key(event.user_id, f"{action}:{today}")
        repeat_count_raw = await get_data(repeat_key)
        logger.info(
            "[TRAIN] get repeat key user_id=%s action=%s took %sms",
            event.user_id,
            action,
            _ms(t),
        )

        repeat_count = int(str(repeat_count_raw)) if repeat_count_raw is not None else 0

        if repeat_count >= 1:
            logger.info(
                "[TRAIN] duplicate suppressed silently user_id=%s action=%s total=%sms",
                event.user_id,
                action,
                _ms(total_started),
            )
            return False

        t = time.perf_counter()
        await set_flag(
            repeat_key,
            ex=ActivityService.get_seconds_until_kyiv_midnight(),
        )
        logger.info(
            "[TRAIN] set repeat key user_id=%s action=%s took %sms",
            event.user_id,
            action,
            _ms(t),
        )

        t = time.perf_counter()
        await _reply_transport(
            source,
            f"⚠️ Сьогодні ти вже робив {action}. Можеш обрати інший тип тренування або чекати до завтра.",
            show_alert=isinstance(source, CallbackQuery),
        )
        logger.info(
            "[TRAIN] duplicate reply user_id=%s action=%s took %sms total=%sms",
            event.user_id,
            action,
            _ms(t),
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    started = await state_machine.begin_training(event.user_id, action, ttl=120)
    logger.info(
        "[TRAIN] begin_training user_id=%s action=%s took %sms",
        event.user_id,
        action,
        _ms(t),
    )

    if not started:
        await _reply_transport(source, "⚠️ Не вдалося активувати сесію. Спробуй ще раз.")
        logger.info(
            "[TRAIN] begin_training failed user_id=%s action=%s total=%sms",
            event.user_id,
            action,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    msg = await _reply_transport(source, get_phrase("training_start", nickname=mention(user)))
    logger.info(
        "[TRAIN] training_start reply user_id=%s action=%s took %sms",
        event.user_id,
        action,
        _ms(t),
    )

    if msg is not None:
        safe_create_task(auto_delete(msg, 15), name=f"auto_delete_start_{event.user_id}")

    logger.info(
        "[TRAIN] Finished training selection user_id=%s action=%s total=%sms",
        event.user_id,
        action,
        _ms(total_started),
    )
    return True


async def _handle_static_action(event: EventEnvelope, action_name: str, hp: int, phrase_key: str) -> bool:
    total_started = time.perf_counter()

    source = event.payload["source"]
    user = event.payload["user"]

    t = time.perf_counter()
    today_has_activity = await ActivityService.check_today_report(event.user_id, ignore_actions=["Реєстрація"])
    logger.info(
        "[STATIC] check_today_report user_id=%s action=%s took %sms",
        event.user_id,
        action_name,
        _ms(t),
    )

    if today_has_activity:
        t = time.perf_counter()
        await _reply_transport(
            source,
            "⚠️ На сьогодні активність уже зафіксована. Відпочинок або пропуск вдруге не записуються.",
            show_alert=True,
        )
        logger.info(
            "[STATIC] already-had-activity reply user_id=%s action=%s took %sms total=%sms",
            event.user_id,
            action_name,
            _ms(t),
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    ok = await ActivityService.grant_hp(event.user_id, user.full_name, action_name, int(hp))
    logger.info(
        "[STATIC] grant_hp user_id=%s action=%s took %sms",
        event.user_id,
        action_name,
        _ms(t),
    )

    if not ok:
        await _reply_transport(
            source,
            "⚠️ Помилка запису в таблицю. Спробуй ще раз.",
            show_alert=isinstance(source, CallbackQuery),
        )
        logger.info(
            "[STATIC] grant_hp failed user_id=%s action=%s total=%sms",
            event.user_id,
            action_name,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    await state_machine.complete(event.user_id)
    logger.info(
        "[STATIC] state_machine.complete user_id=%s action=%s took %sms",
        event.user_id,
        action_name,
        _ms(t),
    )

    t = time.perf_counter()
    await _reply_transport(source, get_phrase(phrase_key, nickname=mention(user)))
    logger.info(
        "[STATIC] success reply user_id=%s action=%s took %sms total=%sms",
        event.user_id,
        action_name,
        _ms(t),
        _ms(total_started),
    )
    return True


async def on_rest_selected(event: EventEnvelope) -> bool:
    return await _handle_static_action(event, "Rest", HP_REST, "rest")


async def on_skip_selected(event: EventEnvelope) -> bool:
    return await _handle_static_action(event, "Skipped", HP_SKIP, "skip")


async def on_video_uploaded(event: EventEnvelope) -> bool:
    total_started = time.perf_counter()

    message: Message = event.payload["message"]

    t = time.perf_counter()
    current_state = await state_machine.get_state(event.user_id)
    logger.info("[VIDEO] get_state user_id=%s took %sms", event.user_id, _ms(t))

    if current_state != UserFlowState.VIDEO_WAITING:
        await message.answer("⏰ Спочатку вибери тренування в меню!")
        logger.info("[VIDEO] wrong state user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    if message.forward_from or message.forward_date:
        await message.answer("❌ Тільки свіжі кружечки!")
        logger.info("[VIDEO] forwarded video rejected user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    session_data = await state_machine.get_session(event.user_id)
    logger.info("[VIDEO] get_session user_id=%s took %sms", event.user_id, _ms(t))

    if not session_data:
        await message.answer("⏰ Сесія вичерпана. Почни заново.")
        logger.info("[VIDEO] no session user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    await state_machine.mark_processing(event.user_id, ttl=30)
    logger.info("[VIDEO] mark_processing user_id=%s took %sms", event.user_id, _ms(t))

    t = time.perf_counter()
    success = await ActivityService.process_training_full_cycle(message, session_data["action"])
    logger.info(
        "[VIDEO] process_training_full_cycle user_id=%s action=%s took %sms",
        event.user_id,
        session_data["action"],
        _ms(t),
    )

    if success:
        t = time.perf_counter()
        await state_machine.complete(event.user_id)
        logger.info(
            "[VIDEO] state_machine.complete user_id=%s took %sms total=%sms",
            event.user_id,
            _ms(t),
            _ms(total_started),
        )
        return True

    t = time.perf_counter()
    await state_machine.restore_video_waiting(event.user_id, ttl=60)
    logger.info("[VIDEO] restore_video_waiting user_id=%s took %sms", event.user_id, _ms(t))

    await message.answer("⚠️ Помилка запису в таблицю. Спробуй ще раз.")
    logger.info("[VIDEO] failed flow user_id=%s total=%sms", event.user_id, _ms(total_started))
    return False


async def on_penalty_applied(event: EventEnvelope) -> bool:
    t = time.perf_counter()
    await state_machine.penalize(event.user_id)
    logger.info("[PENALTY] state_machine.penalize user_id=%s took %sms", event.user_id, _ms(t))
    return True


flow_event_bus.subscribe(USER_REGISTERED, on_user_registered)
flow_event_bus.subscribe(TRAINING_SELECTED, on_training_selected)
flow_event_bus.subscribe(REST_SELECTED, on_rest_selected)
flow_event_bus.subscribe(SKIP_SELECTED, on_skip_selected)
flow_event_bus.subscribe(VIDEO_UPLOADED, on_video_uploaded)
flow_event_bus.subscribe(PENALTY_APPLIED, on_penalty_applied)
