import logging
import asyncio
from urllib.parse import quote

from aiogram import Router, F, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, User

from config import HP_REF_BATA, HP_REF_NEWBIE, REPORTS_GROUP_ID
from cache import get_data, set_data, set_flag, KeyManager
from services import ActivityService, safe_create_task

router = Router()
logger = logging.getLogger(__name__)

REF_COOLDOWN = 60  # секунд між повторними натисканнями кнопки "Запросити"


# ==============================================================================
# ХЕЛПЕР: ЮЗЕРНеЙМ БОТА
# ==============================================================================

async def get_bot_username(bot: Bot) -> str:
    """
    Повертає юзернейм бота з Redis кешу.
    Fallback: запит до Telegram API (кешується на 1 годину).
    """
    cache_key = KeyManager.get_bot_username_key()
    cached = await get_data(cache_key)
    if cached:
        return str(cached)

    me = await bot.get_me()
    await set_data(cache_key, me.username, ex=3600)
    return me.username


# ==============================================================================
# HANDLER: КНОПКА "ЗАПРОСИТИ ДРУГА"
# ==============================================================================

async def send_invite_prompt(message: Message, actor: User, delete_origin: bool = False):
    uid = actor.id

    # Антиспам через KeyManager (консистентний ключ з префіксом)
    spam_key = KeyManager.get_ref_cooldown_key(uid)
    if (await get_data(spam_key)) is not None:
        return  # Тихий ігнор — не засмічуємо чат

    await set_flag(spam_key, ex=REF_COOLDOWN)

    bot_username = await get_bot_username(message.bot)
    referral_link = f"https://t.me/{bot_username}?start={uid}"

    share_text = (
        f"Бро, вривайся в TurboTeam! 🏎️\n"
        f"Тренуйся, заробляй HP та забирай топ 🔥\n\n"
        f"Твій інвайт: {referral_link}"
    )
    share_url = (
        f"https://t.me/share/url?"
        f"url={quote(referral_link)}&text={quote(share_text)}"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="ВІДПРАВИТИ ДРУГУ 🔗", url=share_url)
        ]]
    )

    if delete_origin:
        try:
            await message.delete()
        except Exception as e:
            logger.debug(f"[REFERRAL] message.delete failed: {e}")

    await message.answer(
        f"🚀 **ЧАС РОЗШИРЮВАТИ КОМАНДУ!**\n\n"
        f"За кожного кента, який вривається:\n"
        f"🏆 Тобі: **+{HP_REF_BATA} HP**\n"
        f"💪 Другу: **+{HP_REF_NEWBIE} HP**\n\n"
        "Тисни кнопку нижче, щоб відправити інвайт 👇",
        reply_markup=kb,
        parse_mode="Markdown",
    )


@router.message(F.text == "🚀 Запросити друга 🔥")
async def invite_friend_handler(message: Message):
    await send_invite_prompt(message, message.from_user, delete_origin=True)


# ==============================================================================
# ЛОГІКА РЕФЕРАЛА
# ==============================================================================

async def process_referral_logic(
    new_user_id: int,
    new_nickname: str,
    referrer_id: int,
    bot: Bot,
) -> None:
    """
    Нараховує HP обом сторонам і надсилає сповіщення.
    
    Антидублікат через KeyManager.get_ref_processed_key() —
    ключ живе 86400 секунд (1 день), що унеможливлює повторне нарахування
    навіть при рестарті бота.
    """
    # ВИПРАВЛЕНО: використовуємо KeyManager замість сирого f-string
    anti_spam_key = KeyManager.get_ref_processed_key(new_user_id)
    if (await get_data(anti_spam_key)) is not None:
        logger.info(f"[REFERRAL] Дублікат реферала для uid={new_user_id} — ігнорується")
        return

    # ВИПРАВЛЕНО: зберігаємо "1" а не True
    await set_flag(anti_spam_key, ex=86400)

    try:
        # Визначаємо ім'я реферера (безпечно)
        ref_name = f"ID:{referrer_id}"
        try:
            member = await bot.get_chat_member(REPORTS_GROUP_ID, referrer_id)
            ref_name = (
                f"@{member.user.username}"
                if member.user.username
                else member.user.first_name
            )
        except Exception as e:
            logger.debug(f"[REFERRAL] get_chat_member failed: {e}")

        # Нарахування HP через ActivityService (без прямих імпортів database)
        referrer_granted = await ActivityService.grant_hp(
            referrer_id, ref_name, "Referral Bonus", HP_REF_BATA
        )
        await asyncio.sleep(0.5)
        newbie_granted = await ActivityService.grant_hp(
            new_user_id, new_nickname, "Welcome Bonus", HP_REF_NEWBIE
        )

        if not referrer_granted or not newbie_granted:
            logger.warning(
                "[REFERRAL] HP grant failed referrer=%s newbie=%s new_user_id=%s referrer_id=%s",
                referrer_granted,
                newbie_granted,
                new_user_id,
                referrer_id,
            )
            return

        # Сповіщення в групу
        await bot.send_message(
            chat_id=REPORTS_GROUP_ID,
            text=(
                f"🏎️ **TURBO-ПОПОВНЕННЯ!**\n\n"
                f"Новий гравець @{new_nickname} (+{HP_REF_NEWBIE} HP)\n"
                f"Прийшов за запитом від: **{ref_name}** (+{HP_REF_BATA} HP) 🔥"
            ),
            parse_mode="Markdown",
        )

        # Сповіщення реферера (фонова задача — не блокуємо основний флоу)
        async def _notify_referrer():
            try:
                await bot.send_message(
                    chat_id=referrer_id,
                    text=(
                        f"🔥 Твій кент @{new_nickname} зайшов!\n"
                        f"+{HP_REF_BATA} HP тобі вже нарахований 💪"
                    ),
                )
            except Exception as e:
                # Очікувана помилка: юзер заблокував бота
                logger.debug(f"[REFERRAL] Не вдалось сповістити uid={referrer_id}: {e}")

        safe_create_task(_notify_referrer(), name=f"notify_referrer_{referrer_id}")

    except Exception as e:
        logger.error(f"[REFERRAL] process_referral_logic error: {e}", exc_info=True)
