import asyncio
import logging
import os
import json

import sentry_sdk
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.redis import RedisStorage, DefaultKeyBuilder

from architecture.events import EventEnvelope, TRAINING_SELECTED, USER_REGISTERED
from architecture.orchestrator import flow_event_bus
from config import BOT_TOKEN, WEB_APP_URL, GROUP_LINK, REPORTS_GROUP_ID, ADMIN_IDS
from database import check_user_exists, close_db_session
from handlers import router as action_router
from phrases import get_phrase
from referral import router as ref_router
from reports import router as reports_router
from tasks import setup_scheduler

from cache import redis_client, set_data, KeyManager, acquire_lock
from services import validate_quiz
from ui import get_inline_menu, get_quiz_reply_keyboard, get_rating_reply_keyboard
from supabase_db import get_supabase

# ==============================================================================
# LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ==============================================================================
# MONITORING (Sentry)
# ==============================================================================
SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
    )
    logger.info("🛡️ [MONITORING] Sentry initialized")

# ==============================================================================
# BOT + STORAGE
# ==============================================================================
storage = RedisStorage(
    redis=redis_client,
    key_builder=DefaultKeyBuilder(with_destiny=True, prefix="turbo_fsm"),
)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown", link_preview_is_disabled=True)
)
dp = Dispatcher(storage=storage)

# ==============================================================================
# COMMANDS
# ==============================================================================

@dp.message(Command("rules"))
async def cmd_rules(message: types.Message):
    await message.answer(get_phrase("rules_text"))


@dp.message(Command("menu"), F.chat.id == REPORTS_GROUP_ID)
async def show_menu_in_group(message: types.Message):
    bot_username = await bot.get_me()
    await message.answer(
        "🚀 *TURBO-МЕНЮ АКТИВОВАНЕ* \nОбирай свій шлях на сьогодні: 👇",
        reply_markup=get_inline_menu(bot_username.username)
    )
    await message.answer("🏆", reply_markup=get_rating_reply_keyboard())


@dp.message(Command("panel"))
async def admin_panel(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        me = await bot.get_me()
        await message.answer(
            "🔥 *Твій пульт керування TurboTeam!* \nТисни на газ, бро! 🏎️💨",
            reply_markup=get_inline_menu(me.username)
        )


@dp.message(Command("sbtest"))
async def supabase_test(message: types.Message):
    try:
        sb = get_supabase()
        response = sb.table("users").select("id", count="exact").limit(1).execute()
        await message.answer(
            f"✅ Supabase підключений\n"
            f"Таблиця users доступна\n"
            f"Кількість записів: {response.count or 0}"
        )
    except Exception as e:
        logger.error(f"[SUPABASE] /sbtest error: {e}", exc_info=True)
        await message.answer(f"❌ Supabase test failed:\n{e}")


@dp.message(CommandStart())
async def start_handler(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    args = (command.args or "").strip()
    start_payload = args or "plain"
    start_key = KeyManager.get_start_dedupe_key(user_id, start_payload)

    if not await acquire_lock(start_key, ex=2):
        return

    if args in {"gym", "street"}:
        await flow_event_bus.publish(
            EventEnvelope(
                name=TRAINING_SELECTED,
                user_id=user_id,
                payload={
                    "source": message,
                    "user": message.from_user,
                    "action": args.capitalize(),
                },
                idempotency_key=f"training-select:{user_id}:{message.message_id}",
            )
        )

        try:
            await message.delete()
        except Exception as e:
            logger.debug(f"[START] Failed to delete /start message: {e}")

        return

    progress_message = await message.answer("⏳ Перевіряю твої дані, зачекай кілька секунд...")

    try:
        is_registered = await check_user_exists(user_id)

        if not is_registered:
            if args and args.isdigit():
                referrer_id = int(args)
                if referrer_id != user_id:
                    await set_data(KeyManager.get_ref_key(user_id), str(referrer_id), ex=86400)
                else:
                    logger.info("[START] Self-referral blocked for user_id=%s", user_id)

            welcome_text = (
                f"Привіт, *{message.from_user.first_name}*! 💪\n\n"
                "Ти потрапив у TurboTeam. Пройди опитування: 👇"
            )

            kb = get_quiz_reply_keyboard(WEB_APP_URL)
            await progress_message.delete()
            return await message.answer(welcome_text, reply_markup=kb)

        await progress_message.delete()

        if not args:
            return await message.answer(
                f"Вітаю, {message.from_user.first_name}! Ти вже в команді. 🔥",
                reply_markup=types.ReplyKeyboardRemove()
            )

    except Exception as e:
        logger.error(f"[START] start_handler error: {e}", exc_info=True)
        try:
            await progress_message.delete()
        except Exception:
            pass
        await message.answer("⚠️ Сталася помилка під час перевірки. Спробуй ще раз.")

# ==============================================================================
# WEB APP RECEIVE
# ==============================================================================

@dp.message(F.web_app_data)
async def web_app_receive(message: types.Message):
    user_id = message.from_user.id
    nickname = message.from_user.username or message.from_user.first_name

    progress_message = None

    try:
        data = json.loads(message.web_app_data.data)
        if not validate_quiz(data):
            return await message.answer("❌ Дані некоректні.")

        progress_message = await message.answer("⏳ Реєструю тебе в TurboTeam...")

        await flow_event_bus.publish(
            EventEnvelope(
                name=USER_REGISTERED,
                user_id=user_id,
                payload={
                    "message": message,
                    "nickname": nickname,
                    "quiz_data": data,
                },
                idempotency_key=f"user-registered:{user_id}:{message.message_id}",
            )
        )

        if progress_message:
            try:
                await progress_message.delete()
            except Exception:
                pass

    except Exception as e:
        logger.error(f"❌ [WEBAPP ERROR] {e}", exc_info=True)

        if progress_message:
            try:
                await progress_message.delete()
            except Exception:
                pass

        await message.answer("❌ Критична помилка реєстрації.")

# ROUTERS
dp.include_router(reports_router)
dp.include_router(ref_router)
dp.include_router(action_router)


async def on_startup():
    me = await bot.get_me()
    await set_data(KeyManager.get_bot_username_key(), me.username)
    logger.info(f"🚀 Бот @{me.username} онлайн!")


async def on_shutdown():
    logger.info("🛑 Зупинка бота...")
    await close_db_session()
    if redis_client:
        await redis_client.aclose()
    await bot.session.close()


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    scheduler = setup_scheduler(bot)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
