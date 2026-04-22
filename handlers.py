import asyncio
import logging
import time
from html import escape

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from architecture.events import REST_SELECTED, SKIP_SELECTED, VIDEO_UPLOADED
from architecture.events import EventEnvelope
from architecture.orchestrator import flow_event_bus
from config import ADMIN_IDS, REPORTS_GROUP_ID
from cache import get_data, set_flag, delete_data, KeyManager
from database import get_user_stats
from referral import send_invite_prompt
from ratings import show_rating_for_user
from reports import rollback_training_report
from services import safe_create_task, auto_delete
from supabase_db import (
    get_user_by_telegram_id,
    get_user_activities,
    get_referrals_count,
    get_user_achievements_count,
    get_last_user_achievement,
    get_all_users,
)

router = Router()
logger = logging.getLogger(__name__)

PROFILE_COOLDOWN = 7200
PROFILE_MESSAGE_TTL = 120
ADMIN_HELP_TTL = 120

TRAINING_STATUS_LEVELS = [
    (1, "Новачок"),
    (5, "Вкатався"),
    (10, "Боєць"),
    (25, "Стабільний"),
    (50, "Мотор"),
    (100, "Турбо"),
    (200, "Машина"),
    (350, "Термінатор"),
    (500, "Монстр"),
    (1000, "Легенда TurboTeam"),
]

TRAINING_GOALS = [1, 5, 10, 25, 50, 100, 200, 500, 1000]


def get_training_status(training_count: int) -> str:
    status = "Без статусу"

    for threshold, title in TRAINING_STATUS_LEVELS:
        if training_count >= threshold:
            status = title
        else:
            break

    return status


def get_next_training_goal(training_count: int) -> tuple[int | None, str]:
    for goal in TRAINING_GOALS:
        if training_count < goal:
            return goal, f"{training_count}/{goal}"

    return None, "MAX"


def _calc_percent(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100, 1)


def _count_values(users: list[dict], field_name: str, allowed_values: list[str]) -> dict[str, int]:
    result = {value: 0 for value in allowed_values}

    for user in users:
        raw_value = str(user.get(field_name) or "").strip()
        if raw_value in result:
            result[raw_value] += 1

    return result


def _count_filled(users: list[dict], field_name: str) -> int:
    total = 0
    for user in users:
        raw_value = str(user.get(field_name) or "").strip()
        if raw_value:
            total += 1
    return total


def _word_users(count: int) -> str:
    return "юзер" if count == 1 else "юзери" if 2 <= count <= 4 else "юзерів"


def _format_stat_block(
    title: str,
    stats: dict[str, int],
    total: int,
    ordered_items: list[tuple[str, str]],
) -> str:
    lines = [title]

    for value_key, display_label in ordered_items:
        count = int(stats.get(value_key, 0) or 0)
        percent = _calc_percent(count, total)
        lines.append(f"{display_label} — <b>{count}</b> {_word_users(count)} — <b>{percent}%</b>")

    return "\n".join(lines)


def _build_admin_help_text() -> str:
    return (
        "🛠️ <b>АДМІН-КОМАНДИ TURBOTEAM</b>\n\n"
        "📋 <b>ОСНОВНІ</b>\n"
        "/adminhelp — список усіх адмін-команд\n"
        "/panel — відкрити Turbo-панель\n"
        "/menu — вивести Turbo-меню в групі\n"
        "/rating — показати рейтинг\n"
        "/reject — скасувати тренування через reply\n"
        "/quizstats — статистика квізу\n\n"
        "🧪 <b>SUPABASE / ТЕСТИ</b>\n"
        "/sbtest — перевірка підключення Supabase\n"
        "/sbadd — створити себе в Supabase\n"
        "/sbaddactivity — додати тестову активність\n"
        "/sbaddref 123456789 — додати тестовий реферал\n"
        "/sbme — показати свої дані з Supabase\n"
        "/testaward — тестова FIFA-картка\n"
        "/loadtest 50 — безпечний тест паралельного навантаження\n\n"
        "🧹 <b>АДМІН-ДІЇ</b>\n"
        "/wipeuser 123456789 — видалити юзера за Telegram ID\n"
        "/wipeuser @username — видалити юзера за ніком\n\n"
        "📘 <b>ІНШЕ</b>\n"
        "/rules — текст правил"
    )


async def _run_single_load_job(job_id: int) -> dict:
    started = time.perf_counter()

    test_uid = 900000000 + job_id
    test_key = KeyManager.get_profile_warn_key(test_uid)

    dummy_users = [
        {"level": "Новачок", "goal": "Схуднення", "weekly_plan": "1-2 рази"},
        {"level": "Середній", "goal": "Набір маси", "weekly_plan": "3-4 рази"},
        {"level": "Профі", "goal": "Витривалість", "weekly_plan": "5+ разів"},
    ]

    try:
        await set_flag(test_key, ex=30)
        cached = await get_data(test_key)

        level_stats = _count_values(dummy_users, "level", ["Новачок", "Середній", "Профі"])
        total_level = _count_filled(dummy_users, "level")
        _ = _format_stat_block(
            "TEST",
            level_stats,
            total_level,
            [("Новачок", "Новачок"), ("Середній", "Середній"), ("Профі", "Профі")],
        )

        await asyncio.sleep(0)

        return {
            "ok": cached is not None,
            "job_id": job_id,
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
        }

    except Exception as e:
        logger.error(f"[LOADTEST] job failed: job_id={job_id}, error={e}", exc_info=True)
        return {
            "ok": False,
            "job_id": job_id,
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": str(e),
        }


async def _run_loadtest_batch(total_jobs: int) -> dict:
    started = time.perf_counter()

    tasks = [
        asyncio.create_task(_run_single_load_job(i + 1))
        for i in range(total_jobs)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=False)

    success_count = sum(1 for item in results if item.get("ok"))
    fail_count = total_jobs - success_count
    max_ms = max((item.get("duration_ms", 0) for item in results), default=0)
    min_ms = min((item.get("duration_ms", 0) for item in results), default=0)
    avg_ms = round(
        sum(item.get("duration_ms", 0) for item in results) / total_jobs,
        1,
    ) if total_jobs > 0 else 0.0

    return {
        "total_jobs": total_jobs,
        "success_count": success_count,
        "fail_count": fail_count,
        "total_duration_s": round(time.perf_counter() - started, 2),
        "min_ms": min_ms,
        "max_ms": max_ms,
        "avg_ms": avg_ms,
    }


@router.message(F.text == "🏆 Рейтинг ТОП")
async def handle_show_rating_message(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(Command("rating"))
async def handle_show_rating_command(message: Message):
    await show_rating_for_user(message, message.from_user)


@router.message(Command("adminhelp"))
async def handle_admin_help(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        try:
            await message.delete()
        except Exception:
            pass

        sent = await message.answer(_build_admin_help_text(), parse_mode="HTML")
        safe_create_task(auto_delete(sent, ADMIN_HELP_TTL))
    except Exception as e:
        logger.error(f"[HANDLERS] handle_admin_help error: {e}", exc_info=True)
        sent = await message.answer("⚠️ Не вдалося відкрити список адмін-команд.")
        safe_create_task(auto_delete(sent, 10))


@router.message(F.text == "👤 Мій профіль")
async def handle_my_profile(message: Message):
    telegram_user_id = message.from_user.id
    profile_limit_key = KeyManager.get_profile_limit_key(telegram_user_id)
    profile_warn_key = KeyManager.get_profile_warn_key(telegram_user_id)

    try:
        try:
            await message.delete()
        except Exception:
            pass

        if (await get_data(profile_limit_key)) is not None:
            if (await get_data(profile_warn_key)) is None:
                await set_flag(profile_warn_key, ex=PROFILE_COOLDOWN)
                sent_msg = await message.answer(
                    "⏳ Бро, профіль можна відкривати раз на 2 години. Спробуй пізніше."
                )
                safe_create_task(auto_delete(sent_msg, 1))
            return

        stats = await get_user_stats(telegram_user_id)
        user_row = await get_user_by_telegram_id(telegram_user_id)

        if not stats or not user_row:
            sent_msg = await message.answer("⚠️ Профіль не знайдено. Спробуй ще раз пізніше.")
            safe_create_task(auto_delete(sent_msg, 1))
            return

        user_uuid = user_row.get("id")
        if not user_uuid:
            sent_msg = await message.answer("⚠️ Профіль не знайдено. Спробуй ще раз пізніше.")
            safe_create_task(auto_delete(sent_msg, 1))
            return

        activities = await get_user_activities(str(user_uuid), limit=1000)
        referrals_count = await get_referrals_count(str(user_uuid))
        achievements_count = await get_user_achievements_count(str(user_uuid))
        last_achievement = await get_last_user_achievement(str(user_uuid))

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

        training_count = gym_count + street_count
        activities_count = gym_count + street_count + rest_count + skip_count

        status_title = get_training_status(training_count)
        next_goal, next_goal_progress = get_next_training_goal(training_count)

        last_achievement_title = "Поки немає"
        if last_achievement:
            last_achievement_title = str(last_achievement.get("achievement_title") or "Поки немає")

        next_goal_text = "MAX"
        if next_goal is not None:
            next_goal_text = f"{next_goal} тренувань ({next_goal_progress})"

        nickname = user_row.get("nickname") or message.from_user.first_name
        hp_total = int(stats.get("hp_total", 0) or 0)
        streak = int(stats.get("streak", 0) or 0)

        nickname_html = escape(str(nickname))
        status_title_html = escape(str(status_title))
        last_achievement_title_html = escape(str(last_achievement_title))
        next_goal_text_html = escape(str(next_goal_text))

        text = (
            f"👤 <b>МІЙ ПРОФІЛЬ</b>\n\n"
            f"🏷️ Нік: <b>{nickname_html}</b>\n"
            f"🎖️ Статус: <b>{status_title_html}</b>\n"
            f"⚡ Загальний HP: <b>{hp_total}</b>\n"
            f"🔥 Streak: <b>{streak}</b>\n\n"
            f"📊 <b>АКТИВНІСТЬ</b>\n"
            f"🏋️ Gym: <b>{gym_count}</b>\n"
            f"🦾 Street: <b>{street_count}</b>\n"
            f"🧘 Rest: <b>{rest_count}</b>\n"
            f"🚫 Skip: <b>{skip_count}</b>\n"
            f"📌 Усього дій: <b>{activities_count}</b>\n"
            f"🚀 Реферали: <b>{referrals_count}</b>\n\n"
            f"🏅 <b>ПРОГРЕС</b>\n"
            f"🏆 Досягнень: <b>{achievements_count}</b>\n"
            f"🕓 Останнє: <b>{last_achievement_title_html}</b>\n"
            f"🎯 Наступна ціль: <b>{next_goal_text_html}</b>"
        )

        sent_msg = await message.answer(text, parse_mode="HTML")
        await set_flag(profile_limit_key, ex=PROFILE_COOLDOWN)
        safe_create_task(auto_delete(sent_msg, PROFILE_MESSAGE_TTL))

    except Exception as e:
        logger.error(f"[HANDLERS] handle_my_profile error: {e}", exc_info=True)
        await delete_data(profile_limit_key)
        sent_msg = await message.answer("⚠️ Не вдалося завантажити профіль. Спробуй ще раз.")
        safe_create_task(auto_delete(sent_msg, 1))


@router.message(F.text == "🚀 Запросити друга 🔥")
async def handle_invite_friend_message(message: Message):
    await send_invite_prompt(message, message.from_user, delete_origin=True)


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


@router.message(Command("reject"))
async def handle_reject_training(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    if m.chat.id != REPORTS_GROUP_ID:
        return

    if not m.reply_to_message:
        sent = await m.answer("⚠️ Використання: reply на кружок або текст репорту командою /reject")
        safe_create_task(auto_delete(sent, 5))
        try:
            await m.delete()
        except Exception:
            pass
        return

    ok = await rollback_training_report(
        bot=m.bot,
        group_message_id=m.reply_to_message.message_id,
        moderator_name=m.from_user.full_name,
        reason="manual_reject",
    )

    try:
        await m.delete()
    except Exception:
        pass

    if not ok:
        sent = await m.answer("⚠️ Не вдалося скасувати саме це тренування. Reply має бути на кружок або текст репорту.")
        safe_create_task(auto_delete(sent, 5))
        return


@router.message(Command("quizstats"))
async def handle_quiz_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    try:
        users = await get_all_users()
        total_users = len(users)

        if total_users == 0:
            sent = await m.answer("📉 У базі поки немає юзерів.")
            safe_create_task(auto_delete(sent, 10))
            return

        level_values = ["Новачок", "Середній", "Профі"]
        goal_values = ["Схуднення", "Набір маси", "Витривалість"]

        weekly_plan_items = [
            ("1-2 рази", "1–2 рази"),
            ("3-4 рази", "3–4 рази"),
            ("5+ разів", "5+ разів"),
        ]

        training_place_items = [
            ("У залі", "У залі"),
            ("На вулиці / турніках", "На вулиці / турніках"),
            ("І там, і там", "І там, і там"),
        ]

        weekly_plan_values = [item[0] for item in weekly_plan_items]
        training_place_values = [item[0] for item in training_place_items]

        level_stats = _count_values(users, "level", level_values)
        goal_stats = _count_values(users, "goal", goal_values)
        weekly_plan_stats = _count_values(users, "weekly_plan", weekly_plan_values)
        training_place_stats = _count_values(users, "training_place", training_place_values)

        total_level = _count_filled(users, "level")
        total_goal = _count_filled(users, "goal")
        total_weekly_plan = _count_filled(users, "weekly_plan")
        total_training_place = _count_filled(users, "training_place")

        text = (
            f"📊 <b>СТАТИСТИКА КВІЗУ</b>\n\n"
            f"👥 Усього юзерів у базі: <b>{total_users}</b>\n\n"
            f"{_format_stat_block('🎖️ <b>РІВЕНЬ</b>', level_stats, total_level, [(x, x) for x in level_values])}\n\n"
            f"{_format_stat_block('🎯 <b>ЦІЛЬ</b>', goal_stats, total_goal, [(x, x) for x in goal_values])}\n\n"
            f"{_format_stat_block('📅 <b>ПЛАН НА ТИЖДЕНЬ</b>', weekly_plan_stats, total_weekly_plan, weekly_plan_items)}\n\n"
            f"{_format_stat_block('🏋️ <b>ДЕ ТРЕНУЮТЬСЯ</b>', training_place_stats, total_training_place, training_place_items)}"
        )

        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))

        try:
            await m.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_quiz_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати статистику квізу.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("loadtest"))
async def handle_loadtest(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return

    try:
        parts = (m.text or "").strip().split()
        total_jobs = 20

        if len(parts) >= 2:
            try:
                total_jobs = int(parts[1])
            except Exception:
                total_jobs = 20

        if total_jobs < 1:
            total_jobs = 1

        if total_jobs > 200:
            total_jobs = 200

        progress = await m.answer(
            f"⏳ Запускаю load test на <b>{total_jobs}</b> паралельних задач...",
            parse_mode="HTML",
        )

        result = await _run_loadtest_batch(total_jobs)

        text = (
            f"🧪 <b>LOAD TEST RESULT</b>\n\n"
            f"📦 Задач: <b>{result['total_jobs']}</b>\n"
            f"✅ Успішно: <b>{result['success_count']}</b>\n"
            f"❌ Помилок: <b>{result['fail_count']}</b>\n\n"
            f"⏱ Загальний час: <b>{result['total_duration_s']} c</b>\n"
            f"⚡ Найшвидша задача: <b>{result['min_ms']} ms</b>\n"
            f"🐢 Найтриваліша задача: <b>{result['max_ms']} ms</b>\n"
            f"📊 Середній час: <b>{result['avg_ms']} ms</b>\n\n"
            f"ℹ️ Це безпечний тест конкурентності без створення юзерів у базі."
        )

        try:
            await progress.delete()
        except Exception:
            pass

        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))

        try:
            await m.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[HANDLERS] handle_loadtest error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Load test впав.")
        safe_create_task(auto_delete(sent, 10))


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