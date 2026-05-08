import logging

from awards import sunday_final_logic
from jobs.common import safe_job

logger = logging.getLogger(__name__)


@safe_job
async def run_sunday_final(bot) -> None:
    """
    20:00 Kyiv every Sunday — weekly final.
    """
    logger.info("[TASKS] Sunday Final started...")
    await sunday_final_logic(bot)
    logger.info("[TASKS] Sunday Final finished.")


