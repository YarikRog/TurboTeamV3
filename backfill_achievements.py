"""
One-time backfill: grants any training achievement thresholds that existing
users already passed but never received (e.g. after expanding the ladder,
or for users who stopped training before reaching their next milestone).

ActivityService.maybe_grant_training_achievement already loops every
threshold <= the user's training_count and grants whichever isn't recorded
yet in user_achievements, so this script just needs to call it once per user.

Run manually: python backfill_achievements.py
"""

import asyncio
import logging

from services import ActivityService
from supabase_db import get_all_users

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backfill_achievements")


async def main() -> None:
    users = await get_all_users()
    logger.info("Loaded %s users", len(users))

    granted_count = 0
    checked_count = 0

    for user in users:
        telegram_user_id = user.get("telegram_user_id")
        if not telegram_user_id:
            continue

        checked_count += 1

        try:
            granted_title = await ActivityService.maybe_grant_training_achievement(
                int(telegram_user_id)
            )
        except Exception as e:
            logger.error("Failed for telegram_user_id=%s: %s", telegram_user_id, e)
            continue

        if granted_title:
            granted_count += 1
            logger.info(
                "Granted achievement(s) up to '%s' for telegram_user_id=%s",
                granted_title,
                telegram_user_id,
            )

    logger.info(
        "Done. Checked %s users, granted new achievements to %s users.",
        checked_count,
        granted_count,
    )


if __name__ == "__main__":
    asyncio.run(main())
