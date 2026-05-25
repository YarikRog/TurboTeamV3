import logging
import time
from html import escape

from aiogram import types
from aiogram.types import CallbackQuery, Message

from architecture.event_bus import EventBus
from architecture.events import (
    CHALLENGE_SELECTED,
    EventEnvelope,
    PENALTY_APPLIED,
    REST_SELECTED,
    SKIP_SELECTED,
    TRAINING_SELECTED,
    USER_REGISTERED,
    VIDEO_UPLOADED,
)
from architecture.state_machine import UserFlowState, state_machine
from cache import KeyManager, acquire_lock, delete_data, get_data, set_flag, set_data
from challenge import (
    CHALLENGE_ACTION,
    build_challenge_group_report_text,
    build_challenge_text,
    get_challenge_stats,
    submit_weekly_challenge,
)
from config import GROUP_LINK, HP_REST, HP_SKIP, REPORTS_GROUP_ID
from database import get_kyiv_now, register_user_from_quiz, check_user_exists
from phrases import get_phrase
from referral import process_referral_logic
from services import ActivityService, auto_delete, safe_create_task

logger = logging.getLogger(__name__)

flow_event_bus = EventBus()

WELCOME_HP_BONUS = 50
RETURN_TO_GROUP_TEXT = "🏎️ ПОВЕРНУТИСЯ В ГРУПУ"

CHALLENGE_SESSION_TTL = 300
CHALLENGE_SESSION_LOCK_PREFIX = "turbo:challenge_session"

NOT_REGISTERED_TEXT = (
    "⚠️ Тебе ще немає в базі TurboTeam.\n\n"
    "Натисни /start у боті, пройди коротку реєстрацію — "
    "і тоді зможеш отримувати HP."
)


def mention(user: types.User) -> str:
    if user.username:
        return f"@{user.username}"
    return user.first_name or "Учасник"


def mention_html(user: types.User) -> str:
    display_name = user.username or user.full_name or user.first_name or "Учасник"
    display_name = escape(str(display_name))

    if user.username:
        return f'<a href="https://t.me/{escape(str(user.username))}">@{display_name}</a>'

    return f'<a href="tg://user?id={user.id}">{display_name}</a>'


def _ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def _get_challenge_session_lock_key(user_id: int) -> str:
    today = get_kyiv_now().strftime("%Y-%m-%d")
    return f"{CHALLENGE_SESSION_LOCK_PREFIX}:{user_id}:{today}"


def _get_return_to_group_reply_keyboard() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text=RETURN_TO_GROUP_TEXT)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def _get_group_inline_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
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


def _get_start_inline_keyboard(bot_username: str | None) -> types.InlineKeyboardMarkup | None:
    if not bot_username:
        return None

    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="🚀 Зареєструватися",
                    url=f"https://t.me/{bot_username}?start=register",
                )
            ]
        ]
    )


def _get_back_to_group_inline_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="🔙 Назад у банду", url=GROUP_LINK),
            ]
        ]
    )


async def _safe_send_group_message(bot, text: str, parse_mode: str | None = None) -> bool:
    try:
        await bot.send_message(
            chat_id=REPORTS_GROUP_ID,
            text=text,
            parse_mode=parse_mode,
        )
        return True
    except Exception as e:
        logger.error(f"[SAFE_SEND] Failed to send group message: {e}", exc_info=True)
        return False


async def _reply_transport(
    source: Message | CallbackQuery,
    text: str,
    show_alert: bool = False,
    parse_mode: str | None = None,
    reply_markup=None,
):
    if isinstance(source, CallbackQuery):
        if show_alert:
            await source.answer(text, show_alert=True)
            return None

        sent = await source.message.answer(
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
        await source.answer()
        return sent

    return await source.answer(
        text,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
    )


async def _reply_not_registered(source: Message | CallbackQuery) -> None:
    try:
        me = await source.bot.get_me()
        keyboard = _get_start_inline_keyboard(me.username)
    except Exception:
        keyboard = None

    if isinstance(source, CallbackQuery):
        try:
            await source.answer(NOT_REGISTERED_TEXT, show_alert=True)
        except Exception:
            pass

        try:
            await source.message.answer(
                NOT_REGISTERED_TEXT,
                reply_markup=keyboard,
                parse_mode=None,
            )
        except Exception as e:
            logger.debug(f"[AUTH] failed to send not registered message: {e}")
        return

    await source.answer(
        NOT_REGISTERED_TEXT,
        reply_markup=keyboard,
        parse_mode=None,
    )


async def _ensure_registered(user_id: int, source: Message | CallbackQuery, flow_name: str) -> bool:
    t = time.perf_counter()
    exists = await check_user_exists(user_id)
    logger.info(
        "[AUTH] check_user_exists user_id=%s flow=%s exists=%s took %sms",
        user_id,
        flow_name,
        exists,
        _ms(t),
    )

    if exists:
        return True

    await _reply_not_registered(source)
    return False


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
        await message.answer(
            "⚠️ Ти вже в базі.",
            reply_markup=_get_return_to_group_reply_keyboard(),
            parse_mode=None,
        )
        await message.answer(
            "Тримай перепустку в групу 👇",
            reply_markup=_get_group_inline_keyboard(),
            parse_mode=None,
        )
        logger.info("[REG] user already exists user_id=%s total=%sms", user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    success = await register_user_from_quiz(user_id, nickname, quiz_data)
    logger.info("[REG] register_user_from_quiz user_id=%s took %sms", user_id, _ms(t))

    if not success:
        await message.answer("⚠️ Не вдалося завершити реєстрацію. Спробуй ще раз.", parse_mode=None)
        logger.info("[REG] registration failed user_id=%s total=%sms", user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    welcome_bonus_ok, _, _ = await ActivityService.grant_hp(
        user_id=user_id,
        nickname=nickname,
        action_type="Welcome Bonus",
        hp=WELCOME_HP_BONUS,
    )
    logger.info("[REG] welcome bonus user_id=%s took %sms ok=%s", user_id, _ms(t), welcome_bonus_ok)

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
    await message.answer(
        f"✅ Реєстрацію завершено. Тобі нараховано +{WELCOME_HP_BONUS} HP",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode=None,
    )
    logger.info("[REG] registration success answer user_id=%s took %sms", user_id, _ms(t))

    t = time.perf_counter()
    await message.answer(
        "Тисни кнопку нижче, якщо треба швидко повернутися в групу 👇",
        reply_markup=_get_return_to_group_reply_keyboard(),
        parse_mode=None,
    )
    logger.info("[REG] return reply keyboard user_id=%s took %sms", user_id, _ms(t))

    t = time.perf_counter()
    await message.answer(
        "Тримай перепустку в групу 👇",
        reply_markup=_get_group_inline_keyboard(),
        parse_mode=None,
    )
    logger.info("[REG] onboarding answer user_id=%s took %sms", user_id, _ms(t))

    user_mention = mention_html(message.from_user)

    group_welcome_text = (
        get_phrase("welcome", mention=user_mention)
        + "\n\n"
        + f"🔥 Новачку одразу нараховано +{WELCOME_HP_BONUS} HP\n"
        + "Освоюйся в TurboTeam 👀\n"
        + "Усі дії, Turbo-панель і правила — у закріплених повідомленнях групи 👆"
    )

    t = time.perf_counter()
    group_sent = await _safe_send_group_message(
        message.bot,
        group_welcome_text,
        parse_mode="HTML",
    )
    logger.info(
        "[REG] group welcome send user_id=%s took %sms ok=%s",
        user_id,
        _ms(t),
        group_sent,
    )

    logger.info("[REG] Finished registration flow user_id=%s total=%sms", user_id, _ms(total_started))
    return True


async def on_training_selected(event: EventEnvelope) -> bool:
    total_started = time.perf_counter()

    source = event.payload["source"]
    action = event.payload["action"]
    user = event.payload["user"]

    if not await _ensure_registered(event.user_id, source, f"training:{action}"):
        logger.info(
            "[TRAIN] blocked unregistered user_id=%s action=%s total=%sms",
            event.user_id,
            action,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    current_state = await state_machine.get_state(event.user_id)
    logger.info(
        "[TRAIN] get_state user_id=%s action=%s state=%s took %sms",
        event.user_id,
        action,
        current_state,
        _ms(t),
    )

    if current_state == UserFlowState.VIDEO_WAITING:
        t = time.perf_counter()
        msg = await _reply_transport(source, "⏳ Спочатку надішли відео за попереднє тренування!")
        logger.info(
            "[TRAIN] blocked by VIDEO_WAITING user_id=%s action=%s took %sms total=%sms",
            event.user_id,
            action,
            _ms(t),
            _ms(total_started),
        )
        if msg is not None:
            safe_create_task(auto_delete(msg, 15), name=f"auto_delete_video_waiting_{event.user_id}")
        return False

    if current_state == UserFlowState.PROCESSING:
        t = time.perf_counter()
        msg = await _reply_transport(source, "⏳ Зачекай, попереднє тренування ще обробляється.")
        logger.info(
            "[TRAIN] blocked by PROCESSING user_id=%s action=%s took %sms total=%sms",
            event.user_id,
            action,
            _ms(t),
            _ms(total_started),
        )
        if msg is not None:
            safe_create_task(auto_delete(msg, 15), name=f"auto_delete_processing_{event.user_id}")
        return False

    t = time.perf_counter()
    today_has_rest_or_skip = await ActivityService.check_today_report(
        event.user_id,
        ignore_actions=["Gym", "Street"],
    )
    logger.info(
        "[TRAIN] check_today_report(rest/skip) user_id=%s action=%s took %sms",
        event.user_id,
        action,
        _ms(t),
    )

    if today_has_rest_or_skip:
        rest_text = "💆‍♂️ Бро, ти сьогодні вже відпочиваєш. Нове тренування можна зафіксувати завтра."

        msg = await _reply_transport(
            source,
            rest_text,
            parse_mode=None,
            reply_markup=_get_back_to_group_inline_keyboard(),
        )

        if msg is not None:
            safe_create_task(
                auto_delete(msg, 60),
                name=f"auto_delete_rest_block_{event.user_id}_{action}",
            )

        logger.info(
            "[TRAIN] blocked by rest/skip user_id=%s action=%s total=%sms",
            event.user_id,
            action,
            _ms(total_started),
        )
        return False

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
        repeat_key = KeyManager.get_training_repeat_key(event.user_id, f"{action}:{today}")

        t = time.perf_counter()
        repeat_count_raw = await get_data(repeat_key)
        logger.info(
            "[TRAIN] get repeat key user_id=%s action=%s took %sms",
            event.user_id,
            action,
            _ms(t),
        )

        repeat_count = int(str(repeat_count_raw)) if repeat_count_raw is not None else 0
        next_count = repeat_count + 1

        t = time.perf_counter()
        await set_data(
            repeat_key,
            next_count,
            ex=ActivityService.get_seconds_until_kyiv_midnight(),
        )
        logger.info(
            "[TRAIN] set repeat counter user_id=%s action=%s count=%s took %sms",
            event.user_id,
            action,
            next_count,
            _ms(t),
        )

        async def _send_with_button(text: str, parse_mode: str | None = None):
            return await _reply_transport(
                source,
                text,
                parse_mode=parse_mode,
                reply_markup=_get_back_to_group_inline_keyboard(),
            )

        if next_count == 1:
            t = time.perf_counter()
            msg = await _send_with_button(
                f"⚠️ Сьогодні ти вже робив {action}. Можеш обрати інший тип тренування або чекати до завтра."
            )
            logger.info(
                "[TRAIN] duplicate first warning user_id=%s action=%s took %sms total=%sms",
                event.user_id,
                action,
                _ms(t),
                _ms(total_started),
            )

            if msg is not None:
                safe_create_task(
                    auto_delete(msg, 60),
                    name=f"auto_delete_train_duplicate_{event.user_id}_{action}",
                )
            return False

        if next_count == 2:
            t = time.perf_counter()
            msg = await _send_with_button(
                get_phrase("spam", nickname=mention_html(user)),
                parse_mode="HTML",
            )
            logger.info(
                "[TRAIN] duplicate spam warning user_id=%s action=%s took %sms total=%sms",
                event.user_id,
                action,
                _ms(t),
                _ms(total_started),
            )

            if msg is not None:
                safe_create_task(
                    auto_delete(msg, 60),
                    name=f"auto_delete_train_spam_{event.user_id}_{action}",
                )
            return False

        if isinstance(source, CallbackQuery):
            await source.answer("На сьогодні досить ⛔️\nТи вже тренувався 🔥", show_alert=True)

        logger.info(
            "[TRAIN] duplicate hard-block user_id=%s action=%s total=%sms",
            event.user_id,
            action,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    started = await state_machine.begin_training(event.user_id, action, ttl=300)
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

    training_instruction = (
        f"Ти обрав {action} 💪\n\n"
        "Тепер запиши відео-кружечок і надішли його сюди в бот протягом 5 хвилин 🤳\n\n"
        "Кружечок потрібен як підтвердження, що ти реально тренувався 🔥\n\n"
        "Після цього тренування зарахується, і ти отримаєш HP ⚡️"
    )

    t = time.perf_counter()
    msg = await _reply_transport(source, training_instruction)
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


async def on_challenge_selected(event: EventEnvelope) -> bool:
    total_started = time.perf_counter()

    source = event.payload["source"]

    if not await _ensure_registered(event.user_id, source, "challenge"):
        logger.info(
            "[CHALLENGE] blocked unregistered user_id=%s total=%sms",
            event.user_id,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    current_state = await state_machine.get_state(event.user_id)
    logger.info(
        "[CHALLENGE] get_state user_id=%s state=%s took %sms",
        event.user_id,
        current_state,
        _ms(t),
    )

    if current_state == UserFlowState.VIDEO_WAITING:
        msg = await _reply_transport(
            source,
            "⏳ Спочатку надішли відео за попередню дію.",
            reply_markup=_get_back_to_group_inline_keyboard(),
        )
        if msg is not None:
            safe_create_task(auto_delete(msg, 15), name=f"auto_delete_challenge_waiting_{event.user_id}")
        return False

    if current_state == UserFlowState.PROCESSING:
        msg = await _reply_transport(
            source,
            "⏳ Зачекай, попередній звіт ще обробляється.",
            reply_markup=_get_back_to_group_inline_keyboard(),
        )
        if msg is not None:
            safe_create_task(auto_delete(msg, 15), name=f"auto_delete_challenge_processing_{event.user_id}")
        return False

    t = time.perf_counter()
    stats = await get_challenge_stats(event.user_id)
    logger.info(
        "[CHALLENGE] get_stats user_id=%s stats=%s took %sms",
        event.user_id,
        stats,
        _ms(t),
    )

    if not stats.get("can_submit"):
        text = build_challenge_text(
            weekly_count=int(stats.get("weekly_count", 0) or 0),
            done_today=bool(stats.get("done_today")),
        )

        msg = await _reply_transport(
            source,
            text,
            parse_mode="HTML",
            reply_markup=_get_back_to_group_inline_keyboard(),
        )
        if msg is not None:
            safe_create_task(auto_delete(msg, 120), name=f"auto_delete_challenge_done_{event.user_id}")

        logger.info(
            "[CHALLENGE] blocked cannot submit user_id=%s total=%sms",
            event.user_id,
            _ms(total_started),
        )
        return False

    challenge_lock_key = _get_challenge_session_lock_key(event.user_id)
    lock_acquired = await acquire_lock(challenge_lock_key, ex=CHALLENGE_SESSION_TTL)

    if not lock_acquired:
        msg = await _reply_transport(
            source,
            "⏳ Ти вже відкрив челендж-сесію. Скинь відео-кружечок або зачекай 5 хвилини.",
            reply_markup=_get_back_to_group_inline_keyboard(),
        )

        if msg is not None:
            safe_create_task(
                auto_delete(msg, 15),
                name=f"auto_delete_challenge_session_lock_{event.user_id}",
            )

        logger.info(
            "[CHALLENGE] blocked by session lock user_id=%s total=%sms",
            event.user_id,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    started = await state_machine.begin_training(event.user_id, CHALLENGE_ACTION, ttl=CHALLENGE_SESSION_TTL)
    logger.info(
        "[CHALLENGE] begin session user_id=%s took %sms",
        event.user_id,
        _ms(t),
    )

    if not started:
        await delete_data(challenge_lock_key)

        await _reply_transport(
            source,
            "⚠️ Не вдалося активувати челендж-сесію. Спробуй ще раз.",
            reply_markup=_get_back_to_group_inline_keyboard(),
        )
        logger.info("[CHALLENGE] begin failed user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    text = build_challenge_text(
        weekly_count=int(stats.get("weekly_count", 0) or 0),
        done_today=False,
    )

    instruction = (
        f"{text}\n\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"✅ <b>Сесія активована.</b>\n"
        f"Тепер надішли свіже відео/кружечок у бот протягом <b>5 хвилин</b>."
    )

    msg = await _reply_transport(
        source,
        instruction,
        parse_mode="HTML",
        reply_markup=_get_back_to_group_inline_keyboard(),
    )

    if msg is not None:
        safe_create_task(auto_delete(msg, 120), name=f"auto_delete_challenge_start_{event.user_id}")

    logger.info("[CHALLENGE] selected user_id=%s total=%sms", event.user_id, _ms(total_started))
    return True


async def _handle_static_action(event: EventEnvelope, action_name: str, hp: int, phrase_key: str) -> bool:
    total_started = time.perf_counter()

    source = event.payload["source"]
    user = event.payload["user"]

    if not await _ensure_registered(event.user_id, source, f"static:{action_name}"):
        logger.info(
            "[STATIC] blocked unregistered user_id=%s action=%s total=%sms",
            event.user_id,
            action_name,
            _ms(total_started),
        )
        return False

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
            "💪 На сьогодні вже досить. Ти вже зафіксував свою активність.",
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
    ok, _, _ = await ActivityService.grant_hp(event.user_id, user.full_name, action_name, int(hp))
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
    msg = await _reply_transport(
        source,
        get_phrase(phrase_key, nickname=mention_html(user)),
        parse_mode="HTML",
    )
    logger.info(
        "[STATIC] success reply user_id=%s action=%s took %sms total=%sms",
        event.user_id,
        action_name,
        _ms(t),
        _ms(total_started),
    )

    if msg is not None:
        delete_after = 300 if action_name == "Rest" else 60

        safe_create_task(
            auto_delete(msg, delete_after),
            name=f"auto_delete_static_{event.user_id}_{action_name}",
        )

    return True


async def on_rest_selected(event: EventEnvelope) -> bool:
    return await _handle_static_action(event, "Rest", HP_REST, "rest")


async def on_skip_selected(event: EventEnvelope) -> bool:
    return await _handle_static_action(event, "Skipped", HP_SKIP, "skip")


async def on_video_uploaded(event: EventEnvelope) -> bool:
    total_started = time.perf_counter()

    message: Message = event.payload["message"]

    if not await _ensure_registered(event.user_id, message, "video_upload"):
        logger.info(
            "[VIDEO] blocked unregistered user_id=%s total=%sms",
            event.user_id,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    current_state = await state_machine.get_state(event.user_id)
    logger.info("[VIDEO] get_state user_id=%s took %sms", event.user_id, _ms(t))

    if current_state != UserFlowState.VIDEO_WAITING:
        await message.answer("⏰ Спочатку вибери дію в меню!", parse_mode=None)
        logger.info("[VIDEO] wrong state user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    if message.forward_from or message.forward_date:
        await message.answer("❌ Тільки свіжі кружечки!", parse_mode=None)
        logger.info("[VIDEO] forwarded video rejected user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    session_data = await state_machine.get_session(event.user_id)
    logger.info("[VIDEO] get_session user_id=%s took %sms", event.user_id, _ms(t))

    if not session_data:
        await message.answer("⏰ Сесія вичерпана. Почни заново.", parse_mode=None)
        logger.info("[VIDEO] no session user_id=%s total=%sms", event.user_id, _ms(total_started))
        return False

    t = time.perf_counter()
    await state_machine.mark_processing(event.user_id, ttl=60)
    logger.info("[VIDEO] mark_processing user_id=%s took %sms", event.user_id, _ms(t))

    action = str(session_data.get("action") or "").strip()

    if action == CHALLENGE_ACTION:
        video_id = message.video_note.file_id if message.video_note else ""

        t = time.perf_counter()
        result = await submit_weekly_challenge(
            user_id=event.user_id,
            nickname=message.from_user.full_name,
            video_id=video_id,
        )
        logger.info(
            "[VIDEO] submit_weekly_challenge user_id=%s result=%s took %sms",
            event.user_id,
            result,
            _ms(t),
        )

        await delete_data(_get_challenge_session_lock_key(event.user_id))

        if result.get("ok"):
            await state_machine.complete(event.user_id)

            await message.answer(
                f"✅ Челендж зараховано. +{int(result.get('hp', 0) or 0)} HP",
                reply_markup=_get_back_to_group_inline_keyboard(),
                parse_mode=None,
            )

            try:
                await message.copy_to(REPORTS_GROUP_ID)
            except Exception as e:
                logger.warning("[CHALLENGE] Failed to copy video to group: %s", e)

            report_nickname = (
                f"@{message.from_user.username}"
                if message.from_user.username
                else (message.from_user.full_name or message.from_user.first_name or "Учасник")
            )

            await message.bot.send_message(
                REPORTS_GROUP_ID,
                build_challenge_group_report_text(
                    nickname=report_nickname,
                    hp=int(result.get("hp", 0) or 0),
                    weekly_count=int(result.get("weekly_count", 0) or 0),
                ),
                parse_mode="HTML",
            )

            logger.info("[VIDEO] challenge completed user_id=%s total=%sms", event.user_id, _ms(total_started))
            return True

        await state_machine.complete(event.user_id)

        reason = str(result.get("reason") or "unknown")
        if reason == "already_done_today":
            text = "⚠️ Сьогодні челендж уже здано. Наступна спроба — завтра."
        else:
            text = "⚠️ Не вдалося зарахувати челендж. Спробуй ще раз пізніше."

        await message.answer(
            text,
            reply_markup=_get_back_to_group_inline_keyboard(),
            parse_mode=None,
        )

        logger.info(
            "[VIDEO] challenge failed user_id=%s reason=%s total=%sms",
            event.user_id,
            reason,
            _ms(total_started),
        )
        return False

    t = time.perf_counter()
    success = await ActivityService.process_training_full_cycle(message, action)
    logger.info(
        "[VIDEO] process_training_full_cycle user_id=%s action=%s took %sms",
        event.user_id,
        action,
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

    await message.answer("⚠️ Помилка запису в таблицю. Спробуй ще раз.", parse_mode=None)
    logger.info("[VIDEO] failed flow user_id=%s total=%sms", event.user_id, _ms(total_started))
    return False


async def on_penalty_applied(event: EventEnvelope) -> bool:
    t = time.perf_counter()
    await state_machine.penalize(event.user_id)
    logger.info("[PENALTY] state_machine.penalize user_id=%s took %sms", event.user_id, _ms(t))
    return True


flow_event_bus.subscribe(USER_REGISTERED, on_user_registered)
flow_event_bus.subscribe(TRAINING_SELECTED, on_training_selected)
flow_event_bus.subscribe(CHALLENGE_SELECTED, on_challenge_selected)
flow_event_bus.subscribe(REST_SELECTED, on_rest_selected)
flow_event_bus.subscribe(SKIP_SELECTED, on_skip_selected)
flow_event_bus.subscribe(VIDEO_UPLOADED, on_video_uploaded)
flow_event_bus.subscribe(PENALTY_APPLIED, on_penalty_applied)