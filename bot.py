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
from database import check_user_exists, close_db_session, get_kyiv_now
from handlers import router as action_router
from phrases import get_phrase
from referral import router as ref_router
from reports import router as reports_router
from tasks import setup_scheduler
from awards import send_test_fifa_card

from cache import redis_client, set_data, delete_data, KeyManager, acquire_lock
from services import validate_quiz
from ui import get_inline_menu, get_quiz_reply_keyboard, get_rating_reply_keyboard
from supabase_db import (
    get_supabase,
    get_user_by_telegram_id,
    get_user_by_nickname,
    create_user,
    delete_user_by_id,
    add_activity,
    add_referral,
    get_referrals_count,
    get_user_activities_count,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
    )
    logger.info("🛡️ [MONITORING] Sentry initialized")

storage = RedisStorage(
    redis=redis_client,
    key_builder=DefaultKeyBuilder(with_destiny=True, prefix="turbo_fsm"),
)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown", link_preview_is_disabled=True)
)
dp = Dispatcher(storage=storage)


@dp.message(Command("rules"))
async def cmd_rules(message: types.Message):
    await message.answer(get_phrase("rules_text"))


@dp.message(Command("menu"))
async def show_menu_in_group(message: types.Message):
    if message.chat.id != REPORTS_GROUP_ID:
        return

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


@dp.message(Command("testaward"))
async def test_award(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    await send_test_fifa_card(
        bot=message.bot,
        chat_id=message.chat.id,
        nickname=message.from_user.username or message.from_user.first_name or "yarik721",
        hp_score=678,
        user_id=message.from_user.id,
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
        await message.answer("❌ Supabase test failed. Дивись логи.")


@dp.message(Command("sbadd"))
async def supabase_add_user(message: types.Message):
    try:
        telegram_user_id = message.from_user.id
        nickname = message.from_user.username or message.from_user.first_name

        existing_user = await get_user_by_telegram_id(telegram_user_id)
        if existing_user:
            await message.answer(
                "ℹ️ Юзер уже є в Supabase\n"
                f"nickname: {existing_user.get('nickname')}\n"
                f"telegram_user_id: {existing_user.get('telegram_user_id')}"
            )
            return

        new_user = await create_user(
            telegram_user_id=telegram_user_id,
            nickname=nickname,
        )

        await message.answer(
            "✅ Юзера створено в Supabase\n"
            f"id: {new_user.get('id')}\n"
            f"nickname: {new_user.get('nickname')}\n"
            f"telegram_user_id: {new_user.get('telegram_user_id')}"
        )
    except Exception as e:
        logger.error(f"[SUPABASE] /sbadd error: {e}", exc_info=True)
        await message.answer("❌ Supabase add user failed. Дивись логи.")


@dp.message(Command("sbaddactivity"))
async def supabase_add_activity(message: types.Message):
    try:
        telegram_user_id = message.from_user.id
        existing_user = await get_user_by_telegram_id(telegram_user_id)

        if not existing_user:
            await message.answer("❌ Юзера немає в Supabase. Спочатку виконай /sbadd")
            return

        await add_activity(
            user_id=existing_user["id"],
            action_name="Gym",
            hp_change=100,
            video_status="✅",
            video_id="sbtest-video",
        )

        await message.answer("✅ Активність додано в Supabase")
    except Exception as e:
        logger.error(f"[SUPABASE] /sbaddactivity error: {e}", exc_info=True)
        await message.answer("❌ Supabase add activity failed. Дивись логи.")


@dp.message(Command("sbaddref"))
async def supabase_add_ref(message: types.Message, command: CommandObject):
    try:
        args = (command.args or "").strip()
        if not args.isdigit():
            await message.answer("❌ Використання: /sbaddref 1118823479")
            return

        new_user = await get_user_by_telegram_id(message.from_user.id)
        if not new_user:
            await message.answer("❌ Тебе немає в Supabase. Спочатку виконай /sbadd")
            return

        referrer_telegram_id = int(args)
        referrer_user = await get_user_by_telegram_id(referrer_telegram_id)
        if not referrer_user:
            await message.answer("❌ Реферера з таким Telegram ID немає в Supabase")
            return

        if referrer_user["id"] == new_user["id"]:
            await message.answer("❌ Не можна створити реферал самому собі")
            return

        await add_referral(
            referrer_user_id=referrer_user["id"],
            new_user_id=new_user["id"],
            points=150,
        )

        await message.answer("✅ Реферал додано в Supabase")
    except Exception as e:
        logger.error(f"[SUPABASE] /sbaddref error: {e}", exc_info=True)
        await message.answer("❌ Supabase add referral failed. Дивись логи.")


@dp.message(Command("sbme"))
async def supabase_me(message: types.Message):
    try:
        telegram_user_id = message.from_user.id
        user = await get_user_by_telegram_id(telegram_user_id)

        if not user:
            await message.answer("❌ Тебе немає в Supabase. Спочатку виконай /sbadd")
            return

        activities_count = await get_user_activities_count(user["id"])
        referrals_count = await get_referrals_count(user["id"])

        await message.answer(
            "🧠 Дані з Supabase\n"
            f"nickname: {user.get('nickname')}\n"
            f"telegram_user_id: {user.get('telegram_user_id')}\n"
            f"activities: {activities_count}\n"
            f"referrals: {referrals_count}"
        )
    except Exception as e:
        logger.error(f"[SUPABASE] /sbme error: {e}", exc_info=True)
        await message.answer("❌ Supabase read failed. Дивись логи.")


@dp.message(Command("wipeuser"))
async def wipe_user(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        args = (command.args or "").strip()
        if not args:
            await message.answer("❌ Використання: /wipeuser 123456789 або /wipeuser @username")
            return

        target_user = None

        if args.isdigit():
            target_user = await get_user_by_telegram_id(int(args))
        elif args.startswith("@"):
            username = args[1:].strip()
            target_user = await get_user_by_nickname(username)
        else:
            await message.answer("❌ Використання: /wipeuser 123456789 або /wipeuser @username")
            return

        if not target_user:
            await message.answer("❌ Юзера не знайдено.")
            return

        telegram_user_id = int(target_user.get("telegram_user_id"))
        user_uuid = str(target_user.get("id"))
        nickname = target_user.get("nickname") or f"ID:{telegram_user_id}"

        # Dependent rows are removed by Supabase ON DELETE CASCADE constraints.
        await delete_user_by_id(user_uuid)

        today = get_kyiv_now().strftime("%Y-%m-%d")

        redis_keys = [
            KeyManager.get_reg_key(telegram_user_id),
            KeyManager.get_ref_key(telegram_user_id),
            KeyManager.get_ref_cooldown_key(telegram_user_id),
            KeyManager.get_ref_warn_key(telegram_user_id),
            KeyManager.get_ref_processed_key(telegram_user_id),
            KeyManager.get_state_key(telegram_user_id),
            KeyManager.get_session_key(telegram_user_id),
            KeyManager.get_profile_limit_key(telegram_user_id),
            KeyManager.get_profile_warn_key(telegram_user_id),
            KeyManager.get_rating_limit_key(telegram_user_id),
            KeyManager.get_training_repeat_key(telegram_user_id, f"Gym:{today}"),
            KeyManager.get_training_repeat_key(telegram_user_id, f"Street:{today}"),
            KeyManager.get_action_lock_key(telegram_user_id, f"Gym:{today}"),
            KeyManager.get_action_lock_key(telegram_user_id, f"Street:{today}"),
            KeyManager.get_action_lock_key(telegram_user_id, f"Rest:{today}"),
            KeyManager.get_action_lock_key(telegram_user_id, f"Skipped:{today}"),
        ]

        for key in redis_keys:
            await delete_data(key)

        await message.answer(
            f"✅ Юзера видалено повністю\n"
            f"nickname: {nickname}\n"
            f"telegram_user_id: {telegram_user_id}"
        )

    except Exception as e:
        logger.error(f"[ADMIN] /wipeuser error: {e}", exc_info=True)
        await message.answer("❌ Не вдалося видалити юзера. Дивись логи.")


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
            group_return_kb = types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        types.InlineKeyboardButton(
                            text="ВХІД У ГРУПУ 🏎️",
                            url=GROUP_LINK,
                        )
                    ]
                ]
            )

            return await message.answer(
                f"Вітаю, {message.from_user.first_name}! Ти вже в команді. 🔥",
                reply_markup=group_return_kb
            )

    except Exception as e:
        logger.error(f"[START] start_handler error: {e}", exc_info=True)
        try:
            await progress_message.delete()
        except Exception:
            pass
        await message.answer("⚠️ Сталася помилка під час перевірки. Спробуй ще раз.")


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
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
