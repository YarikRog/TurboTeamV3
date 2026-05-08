from datetime import datetime, timedelta
import pytz

KYIV_TZ = pytz.timezone("Europe/Kyiv")


def get_kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def get_seconds_until_kyiv_midnight() -> int:
    now = get_kyiv_now()
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    return max(1, int((next_midnight - now).total_seconds()))


def get_current_week_period() -> tuple[datetime, datetime]:
    """Поточний активний TurboTeam-тиждень. Починається в неділю о 20:00 Київ."""
    now = get_kyiv_now()
    current_sunday_20 = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=20, minute=0, second=0, microsecond=0,
    )
    if now < current_sunday_20:
        week_start = current_sunday_20 - timedelta(days=7)
    else:
        week_start = current_sunday_20
    return week_start, week_start + timedelta(days=7)


def get_last_finished_week_period() -> tuple[datetime, datetime]:
    """Останній завершений тиждень (для Sunday final і promostats)."""
    now = get_kyiv_now()
    current_sunday_20 = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=20, minute=0, second=0, microsecond=0,
    )
    week_end = current_sunday_20 if now >= current_sunday_20 else current_sunday_20 - timedelta(days=7)
    return week_end - timedelta(days=7), week_end


def get_previous_finished_week_period() -> tuple[datetime, datetime]:
    """Передостанній завершений тиждень (для порівняння в promostats)."""
    last_start, _ = get_last_finished_week_period()
    return last_start - timedelta(days=7), last_start


def format_period(dt: datetime) -> str:
    return dt.strftime("%d.%m %H:%M")
