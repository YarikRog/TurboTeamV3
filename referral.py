import logging
import asyncio
from urllib.parse import quote
from html import escape

from aiogram import Router, F, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, User

from config import HP_REF_BATA, HP_REF_NEWBIE, REPORTS_GROUP_ID
from cache import get_data, set_data, set_flag, delete_data, KeyManager
from supabase_db import (
    get_user_by_telegram_id,
    add_referral as supabase_add_referral,
)
from services import ActivityService, safe_create_task, auto_delete

router = Router()
logger = logging.getLogger(__name__)

REF_COOLDOWN = 600
REF_MESSAGE_TTL = 60


# ==============================================================================
# SAFE TELEGRAM SEND
# ==============================================================================

async def _safe_send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str | None = None,
) -> bool:
    """
    Sends Telegram message safely.
    Message errors must not break referral logic.
    """
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        return True
    except Exception as e:
        logger.error(
            "[REFERRAL] Failed to send message chat_id=%s error=%s",
            chat_id,
            e,
            exc_info=True,
        )
        return False


# ==============================================================================
# HELPER: BOT USERNAME
# ==============================================================================

async def get_bot_username(bot: Bot) -> str:
    """
    Returns bot username from Redis cache.
    Fallback: Telegram API request, then cache for 1 hour.
    """
    cache_key = KeyManager.get_bot_username_key()
    cached = await get_data(cache_key)
    if cached:
        return str(cached)

    me = await bot.get_me()
    await set_data(cache_key, me.username, ex=3600)
    return me.username


# ==============================================================================
# SUPABASE REFERRAL WRITE
# ==============================================================================

async def add_referral_bonus(referrer_id: int, new_user_id: int, new_user_name: str) -> bool:
    """
    Writes referral record to Supabase referrals table.

    new_user_name is kept in signature to avoid changing existing logic/calls.
    """
    try:
        referrer_row = await get_user_by_telegram_id(referrer_id)
        new_user_row = await get_user_by_telegram_id(new_user_id)

        if not referrer_row:
            logger.warning(f"[REFERRAL] Referrer not found in Supabase: telegram_user_id={referrer_id}")
            return False

        if not new_user_row:
            logger.warning(f"[REFERRAL] New user not found in Supabase: telegram_user_id={new_user_id}")
            return False

        referrer_user_uuid = referrer_row.get("id")
        new_user_uuid = new_user_row.get("id")

        if not referrer_user_uuid or not new_user_uuid:
            logger.warning(
                "[REFERRAL] Missing Supabase UUIDs: referrer_id=%s new_user_id=%s",
                referrer_id,
                new_user_id,
            )
            return False

        await supabase_add_referral(
            referrer_user_id=str(referrer_user_uuid),
            new_user_id=str(new_user_uuid),
            points=HP_REF_BATA,
        )

        return True

    except Exception as e:
        logger.error(f"[REFERRAL] Supabase referral write failed: {e}", exc_info=True)
        return False


# ==============================================================================
# HANDLER: "INVITE A FRIEND" BUTTON
# ==============================================================================

async def send_invite_prompt(message: Message, actor: User, delete_origin: bool = False):
    uid = actor.id

    if delete_origin:
        try:
            await message.delete()
        except Exception as e:
            logger.debug(f"[REFERRAL] message.delete failed: {e}")

    cooldown_key = KeyManager.get_ref_cooldown_key(uid)
    warn_key = KeyManager.get_ref_warn_key(uid)

    if (await get_data(cooldown_key)) is not None:
        if (await get_data(warn_key)) is None:
            await set_flag(warn_key, ex=REF_COOLDOWN)
            sent_msg = await message.answer(
                "⏳ Бро, запрошення друга можна відкривати раз на 10 хв. Спробуй пізніше.",
                parse_mode=None,
            )
            safe_create_task(auto_delete(sent_msg, 1))
        return

    await set_flag(cooldown_key, ex=REF_COOLDOWN)

    bot_username = await get_bot_username(message.bot)
    referral_link = f"https://t.me/{bot_username}?start={uid}"

    share_text = (
        "Запрошую тебе в TurboTeam 🏎️\n"
        "Тут можна тренуватись, заробляти HP і ловити азарт від прогресу 🔥"
    )
    share_url = (
        f"https://t.me/share/url?"
        f"url={quote(referral_link)}&text={quote(share_text)}"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="Натискай сюди 👈", url=share_url)
        ]]
    )

    sent_msg = await message.answer(
        (
            "🚀 ЧАС РОЗШИРЮВАТИ КОМАНДУ!\n\n"
            "За кожного нового учасника:\n"
            f"🏆 Тобі: +{HP_REF_BATA} HP\n"
            f"💪 Новачку: +{HP_REF_NEWBIE} HP\n\n"
            "Тисни кнопку нижче, щоб відправити запрошення 👇"
        ),
        reply_markup=kb,
        parse_mode=None,
    )
    safe_create_task(auto_delete(sent_msg, REF_MESSAGE_TTL))


@router.message(F.text == "🚀 Запросити друга 🔥")
async def invite_friend_handler(message: Message):
    await send_invite_prompt(message, message.from_user, delete_origin=True)


# ==============================================================================
# REFERRAL LOGIC
# ==============================================================================

async def process_referral_logic(
    new_user_id: int,
    new_nickname: str,
    referrer_id: int,
    bot: Bot,
) -> None:
    """
    Grants referral HP to both users, writes referral record to Supabase,
    and sends notifications.

    Important:
    - HP and Supabase record are the main logic.
    - Telegram notification errors must NOT rollback referral after HP is granted.
    - dedupe key prevents duplicate processing.
    - if flow fails before HP is granted, dedupe key is rolled back.
    """
    anti_spam_key = KeyManager.get_ref_processed_key(new_user_id)

    if (await get_data(anti_spam_key)) is not None:
        logger.info(f"[REFERRAL] Duplicate referral for uid={new_user_id} ignored")
        return

    lock_set = await set_flag(anti_spam_key, ex=86400)
    if not lock_set:
        logger.warning(f"[REFERRAL] Failed to set referral lock for uid={new_user_id}")
        return

    completed = False

    try:
        ref_name = f"ID:{referrer_id}"

        try:
            member = await bot.get_chat_member(REPORTS_GROUP_ID, referrer_id)
            if member.user.username:
                ref_name = f"@{member.user.username}"
            else:
                ref_name = member.user.first_name or ref_name
        except Exception as e:
            logger.debug(f"[REFERRAL] get_chat_member failed: {e}")

        clean_new_nickname = str(new_nickname or f"ID:{new_user_id}").strip()
        clean_ref_name = str(ref_name or f"ID:{referrer_id}").strip()

        referrer_action = f"Referral Bonus ({new_user_id})"

        referrer_granted, _, _ = await ActivityService.grant_hp(
            referrer_id,
            clean_ref_name,
            referrer_action,
            HP_REF_BATA,
        )

        await asyncio.sleep(0.5)

        newbie_granted, _, _ = await ActivityService.grant_hp(
            new_user_id,
            clean_new_nickname,
            "Referral Welcome Bonus",
            HP_REF_NEWBIE,
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

        referral_log_written = await add_referral_bonus(
            referrer_id=referrer_id,
            new_user_id=new_user_id,
            new_user_name=clean_new_nickname,
        )

        if not referral_log_written:
            logger.warning(
                "[REFERRAL] Referral Supabase write failed new_user_id=%s referrer_id=%s",
                new_user_id,
                referrer_id,
            )

        # From this point referral is considered successful.
        # Notification failures must not rollback the referral.
        completed = True

        new_display = escape(clean_new_nickname)
        ref_display = escape(clean_ref_name)

        await _safe_send_message(
            bot=bot,
            chat_id=REPORTS_GROUP_ID,
            text=(
                "🏎️ <b>TURBO-ПОПОВНЕННЯ!</b>\n\n"
                f"Новий гравець <b>{new_display}</b> (+{HP_REF_NEWBIE} HP)\n"
                f"Прийшов за запрошенням від: <b>{ref_display}</b> (+{HP_REF_BATA} HP) 🔥"
            ),
            parse_mode="HTML",
        )

        async def _notify_referrer():
            ref_text_newbie = escape(clean_new_nickname)

            await _safe_send_message(
                bot=bot,
                chat_id=referrer_id,
                text=(
                    "🔥 Твоє запрошення спрацювало!\n"
                    f"{ref_text_newbie} приєднався до TurboTeam,\n"
                    f"а тобі вже нараховано +{HP_REF_BATA} HP 💪"
                ),
                parse_mode="HTML",
            )

        safe_create_task(_notify_referrer(), name=f"notify_referrer_{referrer_id}")

        logger.info(
            "[REFERRAL] Completed new_user_id=%s referrer_id=%s ref_hp=%s newbie_hp=%s db_written=%s",
            new_user_id,
            referrer_id,
            HP_REF_BATA,
            HP_REF_NEWBIE,
            referral_log_written,
        )

    except Exception as e:
        logger.error(f"[REFERRAL] process_referral_logic error: {e}", exc_info=True)

    finally:
        if not completed:
            rollback_ok = await delete_data(anti_spam_key)
            if rollback_ok:
                logger.info(f"[REFERRAL] Rollback referral lock for uid={new_user_id}")
            else:
                logger.warning(f"[REFERRAL] Failed to rollback referral lock for uid={new_user_id}")