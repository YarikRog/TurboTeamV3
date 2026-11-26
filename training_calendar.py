"""Signed, read-only subscriptions and iCalendar output for TurboTeam."""
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid5


FEED_PATH = "/api/training-calendar.ics"
TRAINING_TITLES = {"Gym": "Тренування в залі", "Street": "Тренування на вулиці"}


def calendar_token(user_id: UUID, secret: str) -> str:
    value = user_id.hex
    signature = hmac.new(
        secret.encode(), f"training-calendar:v1:{value}".encode(), hashlib.sha256
    ).hexdigest()
    return f"{value}.{signature}"


def calendar_user_id(token: str, secret: str) -> UUID | None:
    if len(token) != 97 or not token.isascii():
        return None
    try:
        user_id = UUID(hex=token[:32])
    except ValueError:
        return None
    if hmac.compare_digest(calendar_token(user_id, secret), token):
        return user_id
    return None


def render_training_calendar(user_id: UUID, trainings: list[tuple[str, str]]) -> str:
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//TurboTeam//Training Calendar//UK",
        "CALSCALE:GREGORIAN",
        "X-WR-CALNAME:TurboTeam",
    ]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for day, action in trainings:
        next_day = datetime.strptime(day, "%Y%m%d") + timedelta(days=1)
        event_id = uuid5(user_id, f"training-calendar:{day}:{action}")
        lines.extend([
            "BEGIN:VEVENT",
            f"UID:{event_id}@calendar.turboteam",
            f"DTSTAMP:{timestamp}",
            f"DTSTART;VALUE=DATE:{day}",
            f"DTEND;VALUE=DATE:{next_day:%Y%m%d}",
            f"SUMMARY:{TRAINING_TITLES[action]}",
            "TRANSP:TRANSPARENT",
            "END:VEVENT",
        ])
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"
