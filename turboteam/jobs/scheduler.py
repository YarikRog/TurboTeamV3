import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from jobs.motivation import (
    send_morning_motivation,
    send_midday_motivation,
    send_day_motivation,
    send_peak_motivation,
    send_evening_motivation,
)
from jobs.inactivity import (
    send_second_day_private_reminder,
    inactive_reminder,
    send_last_day_warning,
    auto_remove_inactive_users,
    auto_unban_inactive_users,
)
from jobs.weekly import run_sunday_final

logger = logging.getLogger(__name__)


def setup_scheduler(bot) -> AsyncIOScheduler:
    """
    Configures and starts APScheduler.
    Returns scheduler instance.
    """
    kyiv_tz = pytz.timezone("Europe/Kyiv")
    scheduler = AsyncIOScheduler(timezone=kyiv_tz)

    scheduler.add_job(
        send_morning_motivation, "cron", hour=8, minute=0, args=[bot]
    )
    scheduler.add_job(
        auto_unban_inactive_users, "cron", hour=9, minute=0, args=[bot]
    )
    scheduler.add_job(
        inactive_reminder, "cron", hour=11, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_midday_motivation, "cron", hour=12, minute=0, args=[bot]
    )
    scheduler.add_job(
        auto_remove_inactive_users, "cron", hour=12, minute=5, args=[bot]
    )
    scheduler.add_job(
        send_day_motivation, "cron", hour=15, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_peak_motivation, "cron", hour=18, minute=30, args=[bot]
    )
    scheduler.add_job(
        send_last_day_warning, "cron", hour=19, minute=0, args=[bot]
    )
    scheduler.add_job(
        send_second_day_private_reminder, "cron", hour=19, minute=30, args=[bot]
    )
    scheduler.add_job(
        send_evening_motivation, "cron", hour=21, minute=0, args=[bot]
    )
    scheduler.add_job(
        run_sunday_final, "cron", day_of_week="sun", hour=20, minute=0, args=[bot]
    )

    scheduler.start()

    now_str = datetime.now(kyiv_tz).strftime("%H:%M:%S")
    logger.info(f"[TASKS] Scheduler started. Kyiv time: {now_str}")

    return scheduler