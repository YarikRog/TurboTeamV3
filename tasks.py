import logging
import asyncio
import functools
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import REPORTS_GROUP_ID
from phrases import get_phrase
from awards import sunday_final_logic
from database import get_inactive_users

logger = logging.getLogger(__name__)


# ==============================================================================
# ДЕКОРАТОР БЕЗПЕКИ ДЛЯ SCHEDULED JOBS
# functools.wraps зберігає __name__ функції — без нього в логах
# всі задачі показуються як "wrapper", що унеможливлює дебаг.
# ==============================================================================

def safe_job(func):
    """
    Обгортка для APScheduler задач.
    Гарантує, що помилка в одній задачі не зупиняє планувальник.
    Логує повний traceback для діагностики.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"[SCHEDULER] Помилка у задачі {func.__name__}: {e}",
                exc_info=True,
            )
    return wrapper


# ==============================================================================
# SCHEDULED TASKS
# ==============================================================================

@safe_job
async def send_morning_motivation(bot) -> None:
    """08:00 Kyiv — ранкова мотивація."""
    phrase = get_phrase("morning")
    await bot.send_message(chat_id=REPORTS_GROUP_ID, text=phrase)
    logger.info("[TASKS] Ранкова мотивація відправлена")


@safe_job
async def send_evening_motivation(bot) -> None:
    """21:00 Kyiv — вечірня мотивація."""
    phrase = get_phrase("evening")
    await bot.send_message(chat_id=REPORTS_GROUP_ID, text=phrase)
    logger.info("[TASKS] Вечірня мотивація відправлена")


@safe_job
async def inactive_reminder(bot) -> None:
    """
    11:00 Kyiv щодня — тегаємо тих, хто не тренувався 4+ дні.
    Список формує GAS (get_inactive_users).
    """
    inactive_list = await get_inactive_users()
    if not inactive_list:
        logger.info("[TASKS] Роздуплятор: всі в строю!")
        return

    mentions = " ".join(inactive_list)
    text = (
        f"🚨 **РОЗДУПЛЯТОР ТУРБОТІМ** 🚨\n\n"
        f"{mentions}\n\n"
        f"Бро, ти де зник? Вже 4 дні тиші! "
        f"Повертайся в стрій, HP самі себе не зароблять! 🔥"
    )
    await bot.send_message(
        chat_id=REPORTS_GROUP_ID, text=text, parse_mode="Markdown"
    )
    logger.info(f"[TASKS] Роздуплятор: спрацював на {len(inactive_list)} юзерів")


@safe_job
async def run_sunday_final(bot) -> None:
    """
    20:00 Kyiv щонеділі — недільний фінал.
    Обгорнуто окремо щоб логувати початок і кінець.
    """
    logger.info("[TASKS] Старт Недільного Фіналу...")
    await sunday_final_logic(bot)
    logger.info("[TASKS] Недільний Фінал завершено.")


# ==============================================================================
# ІНІЦІАЛІЗАЦІЯ ПЛАНУВАЛЬНИКА
# ==============================================================================

def setup_scheduler(bot) -> AsyncIOScheduler:
    """
    Налаштовує та запускає APScheduler.
    Повертає екземпляр scheduler (для можливого shutdown при зупинці бота).
    """
    kyiv_tz = pytz.timezone("Europe/Kyiv")
    scheduler = AsyncIOScheduler(timezone=kyiv_tz)

    scheduler.add_job(
        send_morning_motivation, "cron", hour=8, minute=0, args=[bot]
    )
    scheduler.add_job(
        inactive_reminder, "cron", hour=11, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_evening_motivation, "cron", hour=21, minute=0, args=[bot]
    )
    scheduler.add_job(
        run_sunday_final, "cron", day_of_week="sun", hour=20, minute=0, args=[bot]
    )

    scheduler.start()

    now_str = datetime.now(kyiv_tz).strftime("%H:%M:%S")
    logger.info(f"[TASKS] Планувальник запущено. Час у Києві: {now_str}")

    return scheduler
