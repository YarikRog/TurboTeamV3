import logging

from aiogram import Bot, Router
from aiogram.types import MessageReactionUpdated, ReplyParameters

from cache import KeyManager, acquire_lock, get_data, redis_client
from config import ADMIN_IDS, REPORTS_GROUP_ID
from database import update_user_activity

router = Router()
logger = logging.getLogger(__name__)

REACTION_BONUS_THRESHOLD = 3
REACTION_BONUS_HP = 20
REACTION_BONUS_ACTION_NAME = "Reaction Bonus"
REACTION_BONUS_LOCK_TTL = 172800  # 48 hours, just an idempotency guard
REACTION_USERS_SET_TTL = 172800  # 48 hours, just cleanup hygiene

REACTION_BONUS_FEEDBACK_TEXT = (
    f"🔥 Суспільство одобрило твоє тренування! Нараховано +{REACTION_BONUS_HP} HP 💪"
)


@router.message_reaction()
async def handle_message_reaction(update: MessageReactionUpdated, bot: Bot):
    if update.chat.id != REPORTS_GROUP_ID:
        return

    if redis_client is None:
        return

    reactor_id = update.user.id if update.user else (update.actor_chat.id if update.actor_chat else None)
    if reactor_id is None:
        return

    message_id = update.message_id
    set_key = KeyManager.get_reaction_users_key(message_id)

    if update.new_reaction:
        await redis_client.sadd(set_key, reactor_id)
        await redis_client.expire(set_key, REACTION_USERS_SET_TTL)
    else:
        await redis_client.srem(set_key, reactor_id)

    total_reactors = await redis_client.scard(set_key)
    is_admin_bypass = reactor_id in ADMIN_IDS
    if total_reactors < REACTION_BONUS_THRESHOLD and not is_admin_bypass:
        return

    meta = await get_data(KeyManager.get_report_meta_key(message_id))
    if not isinstance(meta, dict):
        logger.info("[REACTIONS] no report_meta for message_id=%s, skipping", message_id)
        return

    window_open = await get_data(KeyManager.get_reaction_window_key(message_id))
    if not window_open:
        logger.info("[REACTIONS] reaction_window closed/missing for message_id=%s, skipping", message_id)
        return

    target_uid = int(meta.get("target_uid") or 0)
    nickname = str(meta.get("nickname") or "system")
    if not target_uid:
        logger.info("[REACTIONS] no target_uid in meta for message_id=%s, skipping", message_id)
        return

    lock_key = KeyManager.get_reaction_bonus_lock_key(message_id)
    if not await acquire_lock(lock_key, ex=REACTION_BONUS_LOCK_TTL):
        logger.info("[REACTIONS] bonus already locked for message_id=%s, skipping", message_id)
        return

    bonus_video_id = f"reaction_bonus:{message_id}"
    result = await update_user_activity(
        user_id=target_uid,
        nickname=nickname,
        action_name=REACTION_BONUS_ACTION_NAME,
        hp_change=REACTION_BONUS_HP,
        video_id=bonus_video_id,
        is_check=False,
        skip_lock=True,
    )

    if result is not True:
        return

    logger.info(
        "[REACTIONS] reaction bonus awarded target_uid=%s message_id=%s reactors=%s admin_bypass=%s",
        target_uid,
        message_id,
        total_reactors,
        is_admin_bypass,
    )

    try:
        await bot.send_message(
            chat_id=REPORTS_GROUP_ID,
            text=REACTION_BONUS_FEEDBACK_TEXT,
            reply_parameters=ReplyParameters(message_id=message_id),
        )
    except Exception as e:
        logger.warning("[REACTIONS] failed to send feedback message: %s", e)
