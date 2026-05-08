from datetime import datetime, date, timedelta
from typing import Optional
import pytz

KYIV_TZ = pytz.timezone("Europe/Kyiv")

REAL_ACTIVITY_ACTIONS = {"Gym", "Street", "Rest", "Skipped"}
TRAINING_ACTIONS = {"Gym", "Street"}


def is_real_activity(activity: dict) -> bool:
    return str(activity.get("action_name") or "").strip() in REAL_ACTIVITY_ACTIONS


def is_training_activity(activity: dict) -> bool:
    return str(activity.get("action_name") or "").strip() in TRAINING_ACTIONS


def parse_activity_created_at(value) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    return dt.astimezone(KYIV_TZ)


def get_last_real_activity_date(activities: list[dict]) -> Optional[date]:
    last = None
    for activity in activities:
        action = str(activity.get("action_name", "")).strip()
        if action.endswith("Rollback"):
            continue
        created_at = parse_activity_created_at(activity.get("created_at"))
        if not created_at:
            continue
        d = created_at.date()
        if last is None or d > last:
            last = d
    return last


def calculate_training_streak(activities: list[dict]) -> int:
    training_dates = set()
    for activity in activities:
        if str(activity.get("action_name", "")).strip() not in TRAINING_ACTIONS:
            continue
        created_at = parse_activity_created_at(activity.get("created_at"))
        if created_at:
            training_dates.add(created_at.date())

    streak = 0
    cursor = datetime.now(KYIV_TZ).date()
    while cursor in training_dates:
        streak += 1
        cursor -= timedelta(days=1)
    return streak
