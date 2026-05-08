import logging
from html import escape

from aiogram import F, Router
from aiogram.types import Message

from cache import get_data, set_flag, delete_data, KeyManager
from database import get_user_stats
from services import safe_create_task, auto_delete
from supabase_db import (
    get_user_by_telegram_id,
    get_user_activities,
    get_referrals_count,
    get_user_achievements_count,
    get_last_user_achievement,
)

router = Router()
logger = logging.getLogger(__name__)

PROFILE_COOLDOWN = 7200
PROFILE_MESSAGE_TTL = 120

TRAINING_STATUS_LEVELS = [
    (1, "Новачок"), (5, "Вкатався"), (10, "Боєць"), (25, "Стабільний"),
    (50, "Мотор"), (100, "Турбо"), (200, "Машина"), (350, "Термінатор"),
    (500, "Монстр"), (1000, "Легенда TurboTeam"),
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


def _word_days(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "день"
    if 2 <= count % 10 <= 4 and not (12 <= count % 100 <= 14):
        return "дні"
    return "днів"


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
                sent_msg = await message.answer("⏳ Бро, профіль можна відкривати раз на 2 години. Спробуй пізніше.")
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

        gym_count = street_count = rest_count = skip_count = 0
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

        next_goal_text = "MAX" if next_goal is None else f"{next_goal} тренувань ({next_goal_progress})"
        nickname = user_row.get("nickname") or message.from_user.first_name
        hp_total = int(stats.get("hp_total", 0) or 0)
        streak = int(stats.get("streak", 0) or 0)

        text = (
            f"👤 <b>МІЙ ПРОФІЛЬ</b>\n\n"
            f"🏷️ Нік: <b>{escape(str(nickname))}</b>\n"
            f"🎖️ Статус: <b>{escape(str(status_title))}</b>\n"
            f"⚡ Загальний HP: <b>{hp_total}</b>\n"
            f"🔥 Streak: <b>{streak}</b> {_word_days(streak)}\n\n"
            f"📊 <b>АКТИВНІСТЬ</b>\n"
            f"🏋️ Gym: <b>{gym_count}</b>\n"
            f"🦾 Street: <b>{street_count}</b>\n"
            f"🧘 Rest: <b>{rest_count}</b>\n"
            f"🚫 Skip: <b>{skip_count}</b>\n"
            f"📌 Усього дій: <b>{activities_count}</b>\n"
            f"🚀 Реферали: <b>{referrals_count}</b>\n\n"
            f"🏅 <b>ПРОГРЕС</b>\n"
            f"🏆 Досягнень: <b>{achievements_count}</b>\n"
            f"🕓 Останнє: <b>{escape(str(last_achievement_title))}</b>\n"
            f"🎯 Наступна ціль: <b>{escape(str(next_goal_text))}</b>"
        )

        sent_msg = await message.answer(text, parse_mode="HTML")
        await set_flag(profile_limit_key, ex=PROFILE_COOLDOWN)
        safe_create_task(auto_delete(sent_msg, PROFILE_MESSAGE_TTL))

    except Exception as e:
        logger.error(f"[HANDLERS] handle_my_profile error: {e}", exc_info=True)
        await delete_data(profile_limit_key)
        sent_msg = await message.answer("⚠️ Не вдалося завантажити профіль. Спробуй ще раз.")
        safe_create_task(auto_delete(sent_msg, 1))
