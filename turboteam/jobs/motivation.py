import logging

from config import REPORTS_GROUP_ID
from jobs.common import (
    safe_job,
    build_top3_text,
    build_training_action_keyboard,
    build_motivation_text,
    build_html_phrase_text,
)

logger = logging.getLogger(__name__)


@safe_job
async def send_morning_motivation(bot) -> None:
    """08:00 Kyiv — morning motivation + top-3 + action buttons."""
    top3 = await build_top3_text()
    text = build_motivation_text("morning", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Morning motivation sent")


@safe_job
async def send_midday_motivation(bot) -> None:
    """12:00 Kyiv — midday motivation + top-3 + action buttons."""
    top3 = await build_top3_text()
    text = build_motivation_text("midday", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Midday motivation sent")


@safe_job
async def send_day_motivation(bot) -> None:
    """15:00 Kyiv — TurboFact + top-3 + action buttons."""
    top3 = await build_top3_text()
    text = build_html_phrase_text("turbo_fact", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] TurboFact sent")


@safe_job
async def send_peak_motivation(bot) -> None:
    """18:30 Kyiv — peak evening motivation + top-3 + action buttons."""
    top3 = await build_top3_text()
    text = build_motivation_text("peak", top3)
    keyboard = await build_training_action_keyboard(bot)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    logger.info("[TASKS] Peak motivation sent")


@safe_job
async def send_evening_motivation(bot) -> None:
    """21:00 Kyiv — evening motivation + top-3."""
    top3 = await build_top3_text()
    text = build_motivation_text("evening", top3)

    await bot.send_message(
        chat_id=REPORTS_GROUP_ID,
        text=text,
        parse_mode="HTML",
    )
    logger.info("[TASKS] Evening motivation sent")


