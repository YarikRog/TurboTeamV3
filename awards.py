import logging
import os
import tempfile
from typing import Optional

from PIL import Image, ImageDraw, ImageFont
from aiogram import Bot
from aiogram.types import FSInputFile

from config import REPORTS_GROUP_ID
from phrases import get_phrase
from database import get_weekly_top_users, reset_weekly_stats

logger = logging.getLogger(__name__)

# ==============================================================================
# ШЛЯХИ ДО РЕСУРСІВ
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "card_template.png")
FONT_PATH = os.path.join(BASE_DIR, "font.ttf")


# ==============================================================================
# ГЕНЕРАЦІЯ FIFA КАРТКИ (CORE ENGINE)
# ==============================================================================

def create_fifa_card(nickname: str, hp_score: int) -> Optional[str]:
    """
    Генерує FIFA-картку переможця з автоматичним масштабуванням тексту.
    """
    if not os.path.exists(TEMPLATE_PATH):
        logger.error(f"[AWARDS] Шаблон не знайдено: {TEMPLATE_PATH}")
        return None

    try:
        img = Image.open(TEMPLATE_PATH).convert("RGBA")
        draw = ImageDraw.Draw(img)
        img_width, img_height = img.size

        def get_font(path, size):
            try:
                return ImageFont.truetype(path, size) if os.path.exists(path) else ImageFont.load_default()
            except Exception:
                return ImageFont.load_default()

        display_name = f"@{nickname}".upper()
        name_font_size = 55
        if len(display_name) > 12:
            name_font_size = 45
        if len(display_name) > 16:
            name_font_size = 35

        name_font = get_font(FONT_PATH, name_font_size)
        title_font = get_font(FONT_PATH, 42)
        hp_font = get_font(FONT_PATH, 85)

        def draw_centered_text(text, font, y, fill):
            bbox = draw.textbbox((0, 0), text, font=font)
            w = bbox[2] - bbox[0]
            draw.text(((img_width - w) // 2, y), text, font=font, fill=fill)

        draw_centered_text(display_name, name_font, y=110, fill="white")
        draw_centered_text("ЧЕМПІОН ТИЖНЯ", title_font, y=285, fill="#F0F0F0")
        draw_centered_text(str(hp_score), hp_font, y=465, fill="black")

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".png", dir=BASE_DIR)
        os.close(tmp_fd)

        img.convert("RGB").save(tmp_path, "PNG", optimize=True)
        logger.info(f"[AWARDS] Картка створена: {tmp_path}")
        return tmp_path

    except Exception as e:
        logger.error(f"[AWARDS] Помилка PIL: {e}", exc_info=True)
        return None


# ==============================================================================
# НЕДІЛЬНИЙ ФІНАЛ (TASK EXECUTION)
# ==============================================================================

async def sunday_final_logic(bot: Bot) -> None:
    """
    Автоматизована логіка підбиття підсумків тижня.
    """
    logger.info("🏁 [AWARDS] Початок фінальної обробки тижня...")
    card_path: Optional[str] = None

    try:
        top_users = await get_weekly_top_users()

        if not top_users or not isinstance(top_users, list):
            logger.warning("[AWARDS] Дані про лідерів порожні або невірні.")
            return

        leader = top_users[0]
        nickname = leader.get("nickname") or leader.get("nick") or "Анонім"
        hp_score = int(leader.get("hp") or 0)

        if hp_score <= 0:
            logger.info("[AWARDS] Активності за тиждень не було. Пропускаємо.")
            return

        card_path = create_fifa_card(nickname, hp_score)
        caption = get_phrase("winner", mention=f"@{nickname}")

        if card_path and os.path.exists(card_path):
            await bot.send_photo(
                chat_id=REPORTS_GROUP_ID,
                photo=FSInputFile(card_path),
                caption=caption,
                parse_mode="Markdown"
            )
        else:
            await bot.send_message(
                REPORTS_GROUP_ID,
                f"🏆 *ВІТАЄМО ЧЕМПІОНА!*\n\n{caption}\n💪 Результат: {hp_score} HP",
                parse_mode="Markdown"
            )

        success_reset = await reset_weekly_stats()

        if success_reset:
            logger.info("[AWARDS] Weekly final completed successfully.")
            await bot.send_message(
                REPORTS_GROUP_ID,
                "🔄 *Новий тиждень розпочато!*\nРейтинг тижня оновлено. Час знову ставати монстром! 🦾🏎️💨",
                parse_mode="Markdown"
            )
        else:
            logger.error("[AWARDS] Weekly final completed, but reset step reported failure.")

    except Exception as e:
        logger.error(f"[AWARDS] Критичний збій Sunday Final: {e}", exc_info=True)

    finally:
        if card_path and os.path.exists(card_path):
            try:
                os.remove(card_path)
                logger.debug(f"[AWARDS] Тимчасовий файл {card_path} видалено.")
            except Exception as e:
                logger.warning(f"[AWARDS] Не вдалося видалити файл: {e}")