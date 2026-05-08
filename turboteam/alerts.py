import logging
import time
from datetime import datetime
from html import escape
from typing import Any

import pytz

from config import ADMIN_IDS

logger = logging.getLogger(__name__)

KYIV_TZ = pytz.timezone("Europe/Kyiv")

ALERT_COOLDOWN_SECONDS = 300
_last_alerts: dict[str, float] = {}


def _trim(value: Any, limit: int = 700) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _build_error_alert_text(place: str, error: Exception) -> str:
    now = datetime.now(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")
    error_type = type(error).__name__
    error_text = _trim(str(error), 700)

    return (
        "🚨 <b>TURBOTEAM ERROR</b>\n\n"
        f"📍 Місце: <code>{escape(str(place))}</code>\n"
        f"🧨 Тип: <code>{escape(str(error_type))}</code>\n"
        f"📝 Помилка: <code>{escape(str(error_text))}</code>\n"
        f"🕓 Час: <code>{escape(str(now))}</code>\n\n"
        "Повний traceback дивись у логах Render/Sentry."
    )


def _is_on_cooldown(key: str) -> bool:
    now = time.time()
    last_sent = _last_alerts.get(key)

    if last_sent is not None and now - last_sent < ALERT_COOLDOWN_SECONDS:
        return True

    _last_alerts[key] = now
    return False


async def notify_admins_about_error(bot, place: str, error: Exception) -> None:
    if bot is None:
        return

    error_key = f"{place}:{type(error).__name__}:{str(error)[:120]}"
    if _is_on_cooldown(error_key):
        return

    text = _build_error_alert_text(place, error)

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception as send_error:
            logger.debug(
                "[ALERTS] Failed to send alert to admin_id=%s: %s",
                admin_id,
                send_error,
            )