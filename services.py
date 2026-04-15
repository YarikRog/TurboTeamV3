import logging
import random
import asyncio
import functools
from typing import Any, Callable, Optional
from datetime import datetime, timedelta
import pytz

from aiogram import types
from aiogram.types import Message

from config import RANDOM_HP_RANGE, HP_GYM, HP_STREET, HP_REST, HP_SKIP, REPORTS_GROUP_ID
from cache import KeyManager, acquire_lock, set_flag, get_data
from database import get_kyiv_now, add_activity, check_activity_limit, update_user_activity
from phrases import get_phrase
from config import GROUP_LINK

logger = logging.getLogger(__name__)
KYIV_TZ = pytz.timezone("Europe/Kyiv")

# ==============================================================================
# ПАРАМЕТРИ СТРІКІВ (Можеш винести в config.py)
# ==============================================================================
STREAK_BONUS_3_DAYS = 50   # Бонус за 3 дні поспіль
STREAK_BONUS_5_DAYS = 100  # Бонус за 5 днів поспіль
STREAK_BONUS_7_DAYS = 200  # Бонус за тиждень вогню

# ==============================================================================
# ВАЛІДАЦІЯ QUIZ-ДАНИХ
# ==============================================================================

def validate_quiz(data: dict) -> bool:
    """
    Перевіряє дані з WebApp-квізу.
    Приймає будь-які непорожні рядки — українські або англійські.
    """
    try:
        logger.debug(f"[VALIDATE] Quiz data: {data}")

        gender = data.get("gender")
        if not isinstance(gender, str) or len(gender.strip()) == 0:
            logger.warning(f"[VALIDATE] Невірний gender: {gender!r}")
            return False

        level = data.get("level")
        if not isinstance(level, str) or len(level.strip()) == 0:
            logger.warning(f"[VALIDATE] Невірний level: {level!r}")
            return False

        goal = data.get("goal")
        if not isinstance(goal, str) or not (0 < len(goal) < 200):
            logger.warning(f"[VALIDATE] Невірна ціль: {goal!r}")
            return False

        return True
    except Exception as e:
        logger.error(f"[VALIDATE] Критична помилка валідації: {e}", exc_info=True)
        return False


# ==============================================================================
# ДЕКОРАТОРИ
# ==============================================================================

def handle_exceptions(default_return: Any = None):
    """
    Декоратор: ловить виключення, логує з traceback, повертає default_return.
    functools.wraps зберігає ім'я функції в логах (без нього всі функції
    показуються як 'wrapper', що робить дебаг неможливим).
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"[SERVICE] Помилка у {func.__name__}: {e}", exc_info=True
                )
                return default_return
        return wrapper
    return decorator


# ==============================================================================
# УТИЛІТИ
# ==============================================================================

def safe_create_task(coro, name: str = "task") -> asyncio.Task:
    """
    Створює asyncio.Task з автоматичним логуванням помилок.
    Замінює голий asyncio.create_task() по всьому проєкту.
    Без цього виключення в фонових задачах (auto_delete тощо) 
    зникають мовчки і не потрапляють у Sentry.
    """
    task = asyncio.create_task(coro, name=name)

    @functools.wraps(coro.__class__.__call__)
    def _callback(t: asyncio.Task):
        try:
            exc = t.exception()
            if exc:
                logger.error(
                    f"[TASK] Задача {name!r} завершилась з помилкою: {exc}",
                    exc_info=exc,
                )
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

    task.add_done_callback(_callback)
    return task


async def auto_delete(message: Any, delay: int = 5) -> None:
    """
    Видаляє повідомлення через delay секунд.
    Помилки (повідомлення вже видалено, немає прав тощо) логуються як DEBUG,
    бо це очікувана ситуація.
    """
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"[AUTO_DELETE] Не вдалось видалити: {e}")


# ==============================================================================
# ACTIVITY SERVICE
# Вся бізнес-логіка активностей зосереджена тут.
# HTTP-запити делеговані до database.py (єдина відповідальність).
# ==============================================================================

class ActivityService:
    """
    Сервіс управління активностями користувачів.
    
    Ключове рішення — Redis SET NX як distributed lock:
    - Атомарна операція: перевірка + встановлення за один round-trip
    - Закриває race condition (TOCTOU) між check і write
    - Якщо GAS недоступний, Redis-lock все одно захищає від дублікатів
    """

    ACTION_HP_MAPPING: dict[str, int] = {
        "Rest": int(HP_REST),
        "Skipped": int(HP_SKIP),
        "Відпочинок": int(HP_REST),
        "Забив болт": int(HP_SKIP),
    }

    @staticmethod
    @handle_exceptions(default_return=False)
    async def can_user_log_activity(user_id: int, action_type: str) -> bool:
        """
        Перевіряє чи може юзер записати активність сьогодні.
        
        Логіка:
        1. Перевіряємо Redis lock (миттєво, без мережі)
        2. Якщо lock є — відмова (вже робив сьогодні)
        3. Якщо lock немає — йдемо в GAS для фінальної перевірки
        
        Lock НЕ встановлюється тут — він встановлюється в grant_hp()
        після успішного запису. Це дозволяє retry при помилці GAS.
        """
        today = get_kyiv_now().strftime("%Y-%m-%d")
        lock_key = KeyManager.get_action_lock_key(user_id, f"{action_type}:{today}")

        # Швидка перевірка кешу (без запиту до GAS)
        if (await get_data(lock_key)) is not None:
            logger.info(
                f"[SERVICE] Cache hit: uid={user_id} вже робив {action_type} сьогодні"
            )
            return False

        # Перевірка на стороні GAS (на випадок рестарту бота)
        result = await check_activity_limit(user_id, "system", action_type)
        return bool(result)

    @staticmethod
    @handle_exceptions(default_return=False)
    async def check_today_report(user_id: int, ignore_actions: Optional[list[str]] = None) -> bool:
        """
        Повертає True якщо юзер вже зробив денну активність.
        ignore_actions збережено в сигнатурі для сумісності контракту.
        """
        ignore_set = {str(item).strip().lower() for item in (ignore_actions or []) if str(item).strip()}
        today = get_kyiv_now().strftime("%Y-%m-%d")

        tracked_actions = [
            "Gym",
            "Street",
            "Rest",
            "Skipped",
            "Referral Bonus",
            "Welcome Bonus",
            "Реєстрація",
            "Registration",
        ]

        for action_name in tracked_actions:
            if action_name.strip().lower() in ignore_set:
                continue

            lock_key = KeyManager.get_action_lock_key(user_id, f"{action_name}:{today}")
            if (await get_data(lock_key)) is not None:
                return True

        return not await ActivityService.can_user_log_activity(user_id, "system")

    @staticmethod
    @handle_exceptions(default_return=0)
    async def check_and_grant_streak_bonus(user_id: int, nickname: str) -> int:
        """
        Перевіряє серію днів і нараховує бонус.
        Повертає суму нарахованого бонусу або 0.
        """
        from database import get_user_stats # Імпорт тут, щоб не було циклічності
        
        stats = await get_user_stats(user_id)
        if not stats:
            return 0
        
        # Витягуємо поточний стрік з відповіді GAS (припустимо, GAS його рахує)
        # Або рахуємо самі на основі дати останнього тренування
        streak = int(stats.get("streak", 0))
        
        bonus = 0
        if streak == 3:
            bonus = STREAK_BONUS_3_DAYS
        elif streak == 5:
            bonus = STREAK_BONUS_5_DAYS
        elif streak >= 7 and streak % 7 == 0: # Кожен 7-й день
            bonus = STREAK_BONUS_7_DAYS
            
        if bonus > 0:
            # Пишемо в базу окремим рядком через існуючу add_activity
            action_label = f"🔥 Streak Bonus ({streak} days)"
            await add_activity(user_id, nickname, action_label, bonus)
            logger.info(f"[STREAK] Нараховано бонус +{bonus} HP для {nickname} за {streak} днів!")
            
        return bonus

    @staticmethod
    @handle_exceptions(default_return=False)
    async def grant_hp(
        user_id: int,
        nickname: str,
        action_type: str,
        hp: int,
        video_id: str = "",
    ) -> bool:
        """
        Нараховує HP юзеру з atomic lock для запобігання дублікатів.
        
        Використовує SET NX (acquire_lock):
        - Якщо lock отримано → пишемо в GAS
        - Якщо lock вже є → відмова без запиту до GAS
        
        Це закриває race condition: навіть якщо два запити прийдуть
        одночасно, тільки один отримає lock.
        """
        today = get_kyiv_now().strftime("%Y-%m-%d")
        lock_key = KeyManager.get_action_lock_key(user_id, f"{action_type}:{today}")

        # Атомарний lock — основний захист від дублікатів
        lock_acquired = await acquire_lock(lock_key, ex=86400)
        if not lock_acquired:
            logger.info(
                f"[SERVICE] Lock зайнятий: uid={user_id} action={action_type} — дубль відхилено"
            )
            return False

        # Lock отримано — пишемо в GAS
        result = await update_user_activity(
            user_id,
            nickname,
            action_type,
            hp,
            video_id,
            False,
            skip_lock=True,
        )

        if result == "already_done" or result is False:
            # GAS відхилив — знімаємо lock щоб не блокувати юзера назавжди
            from cache import delete_data
            await delete_data(lock_key)
            logger.warning(
                f"[SERVICE] GAS відхилив запис uid={user_id}, lock знято"
            )
            return False

        # --- НОВИЙ БЛОК: СТРІКИ ---
        # Якщо це було тренування (Gym/Street), запускаємо перевірку бонусу
        if action_type in ["Gym", "Street"]:
            # Створюємо фонову задачу, щоб не змушувати юзера чекати відповіді бази
            safe_create_task(
                ActivityService.check_and_grant_streak_bonus(user_id, nickname),
                name=f"streak_bonus_{user_id}"
            )

        logger.info(f"[SERVICE] HP GRANTED: uid={user_id} +{hp} HP за {action_type}")
        return True

    @staticmethod
    def calculate_training_hp(action_type: str = "Gym") -> int:
        """Розраховує HP для тренування: база + рандомний бонус."""
        try:
            base = int(HP_GYM) if action_type == "Gym" else int(HP_STREET)
            bonus = random.randint(int(RANDOM_HP_RANGE[0]), int(RANDOM_HP_RANGE[1]))
            total = base + bonus
            logger.debug(
                f"[SERVICE] calculate_training_hp: action={action_type} "
                f"base={base} bonus={bonus} total={total}"
            )
            return total
        except Exception as e:
            logger.error(f"[SERVICE] calculate_training_hp error: {e}", exc_info=True)
            return int(HP_GYM)

    @staticmethod
    def get_action_hp(action_type: str) -> int:
        """Повертає фіксовані HP для відпочинку/пропуску."""
        for key, value in ActivityService.ACTION_HP_MAPPING.items():
            if key in action_type:
                return int(value)
        logger.warning(f"[SERVICE] Невідомий тип дії: {action_type!r}, повертаємо 0")
        return 0

    @staticmethod
    def get_kyiv_date_string() -> str:
        """Дата у форматі DD.MM.YYYY для Google Sheets."""
        return get_kyiv_now().strftime("%d.%m.%Y")

    @staticmethod
    def get_seconds_until_kyiv_midnight() -> int:
        now = get_kyiv_now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return max(1, int((next_midnight - now).total_seconds()))

    @staticmethod
    @handle_exceptions(default_return=False)
    async def process_training_full_cycle(message: Message, action_type: str) -> bool:
        """
        Оркестрація тренування без зміни бізнес-логіки нарахування:
        1. рахуємо HP
        2. пишемо активність
        3. публікуємо звіт у групу
        """
        user = message.from_user
        nickname = user.full_name
        hp = ActivityService.calculate_training_hp(action_type)
        video_id = message.video_note.file_id if message.video_note else ""

        granted = await ActivityService.grant_hp(
            user.id,
            nickname,
            action_type,
            hp,
            video_id=video_id,
        )
        if not granted:
            return False

        back_to_group_kb = types.InlineKeyboardMarkup(
            inline_keyboard=[[
                types.InlineKeyboardButton(text="😎 Повертайся в банду", url=GROUP_LINK)
            ]]
        )
        await message.answer(
            f"✅ {action_type} зафіксовано. +{hp} HP",
            reply_markup=back_to_group_kb,
        )

        try:
            await message.copy_to(REPORTS_GROUP_ID)
        except Exception as e:
            logger.warning("[SERVICE] Не вдалося скопіювати відео в групу: %s", e)

        await message.bot.send_message(
            REPORTS_GROUP_ID,
            f"{get_phrase('report', nickname=f'@{user.username or user.first_name}')}\n+{hp} HP",
        )
        return True
