import logging

from aiogram import Bot, Router
from aiogram.types import MessageReactionCountUpdated, ReplyParameters

from cache import KeyManager, acquire_lock, get_data
from config import REPORTS_GROUP_ID
from database import update_user_activity

router = Router()
logger = logging.getLogger(__name__)

REACTION_BONUS_THRESHOLD = 3
REACTION_BONUS_HP = 20
REACTION_BONUS_ACTION_NAME = "Reaction Bonus"
REACTION_BONUS_LOCK_TTL = 172800  # 48 hours, matches report meta TTL

REACTION_BONUS_FEEDBACK_TEXT = (
    f"🔥 Суспільство одобрило твоє тренування! Нараховано +{REACTION_BONUS_HP} HP 💪"
)


@router.message_reaction_count()
async def handle_message_reaction_count(update: MessageReactionCountUpdated, bot: Bot):
    if update.chat.id != REPORTS_GROUP_ID:
        return

    total_reactions = sum(r.total_count for r in update.reactions)
    if total_reactions < REACTION_BONUS_THRESHOLD:
        return

    message_id = update.message_id

    meta = await get_data(KeyManager.get_report_meta_key(message_id))
    if not isinstance(meta, dict):
        return

    target_uid = int(meta.get("target_uid") or 0)
    nickname = str(meta.get("nickname") or "system")
    if not target_uid:
        return

    lock_key = KeyManager.get_reaction_bonus_lock_key(message_id)
    if not await acquire_lock(lock_key, ex=REACTION_BONUS_LOCK_TTL):
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
        "[REACTIONS] reaction bonus awarded target_uid=%s message_id=%s reactions=%s",
        target_uid,
        message_id,
        total_reactions,
    )

    try:
        await bot.send_message(
            chat_id=REPORTS_GROUP_ID,
            text=REACTION_BONUS_FEEDBACK_TEXT,
            reply_parameters=ReplyParameters(message_id=message_id),
        )
    except Exception as e:
        logger.warning("[REACTIONS] failed to send feedback message: %s", e)
