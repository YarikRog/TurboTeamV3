"""
Backward-compatible scheduler exports.

The scheduled job logic lives in jobs/*. Keep this file so bot.py and any
legacy imports can still use `from tasks import setup_scheduler`.
"""
from jobs.scheduler import setup_scheduler
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

__all__ = [
    "setup_scheduler",
    "send_morning_motivation",
    "send_midday_motivation",
    "send_day_motivation",
    "send_peak_motivation",
    "send_evening_motivation",
    "send_second_day_private_reminder",
    "inactive_reminder",
    "send_last_day_warning",
    "auto_remove_inactive_users",
    "auto_unban_inactive_users",
    "run_sunday_final",
]
