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
# ГЕНЕРАЦІЯ ГРАМОТИ (CORE ENGINE)
# ==============================================================================

def create_fifa_card(nickname: str, hp_score: int) -> Optional[str]:
    """
    Генерує грамоту переможця тижня з автоматичним масштабуванням тексту.
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

        name_font_size = 44
        if len(display_name) > 12:
            name_font_size = 38
        if len(display_name) > 16:
            name_font_size = 32
        if len(display_name) > 20:
            name_font_size = 28

        name_font = get_font(FONT_PATH, name_font_size)
        title_font = get_font(FONT_PATH, 24)
        hp_font = get_font(FONT_PATH, 56)
        hp_label_font = get_font(FONT_PATH, 24)

        def draw_centered_text(text, font, y, fill, stroke_fill=None, stroke_width=0):
            bbox = draw.textbbox(
                (0, 0),
                text,
                font=font,
                stroke_width=stroke_width,
            )
            w = bbox[2] - bbox[0]
            x = (img_width - w) // 2
            draw.text(
                (x, y),
                text,
                font=font,
                fill=fill,
                stroke_fill=stroke_fill,
                stroke_width=stroke_width,
            )

        def draw_text_center_in_box(text, font, box, fill, stroke_fill=None, stroke_width=0):
            """
            box = (x1, y1, x2, y2)
            """
            bbox = draw.textbbox(
                (0, 0),
                text,
                font=font,
                stroke_width=stroke_width,
            )
            text_w = bbox[2] - bbox[0]
            text_h = bbox[3] - bbox[1]

            x1, y1, x2, y2 = box
            box_w = x2 - x1
            box_h = y2 - y1

            x = x1 + (box_w - text_w) // 2
            y = y1 + (box_h - text_h) // 2

            draw.text(
                (x, y),
                text,
                font=font,
                fill=fill,
                stroke_fill=stroke_fill,
                stroke_width=stroke_width,
            )

        # 1. НІК — зелена зона
        draw_centered_text(
            display_name,
            name_font,
            y=185,
            fill="white",
            stroke_fill="#0A1A4F",
            stroke_width=2,
        )

        # 2. ЧЕМПІОН ТИЖНЯ — червона зона
        draw_centered_text(
            "ЧЕМПІОН ТИЖНЯ",
            title_font,
            y=320,
            fill="#F4F4F4",
            stroke_fill="#0A1A4F",
            stroke_width=1,
        )

        # 3. HP-БЛОК — синя зона
        hp_box = (85, 690, 405, 930)

        draw_text_center_in_box(
            str(hp_score),
            hp_font,
            box=(hp_box[0], hp_box[1], hp_box[2], hp_box[1] + 130),
            fill="black",
            stroke_fill="#A86F00",
            stroke_width=1,
        )

        draw_text_center_in_box(
            "HP",
            hp_label_font,
            box=(hp_box[0], hp_box[1] + 110, hp_box[2], hp_box[3]),
            fill="#1A1A1A",
        )

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".png", dir=BASE_DIR)
        os.close(tmp_fd)

        img.convert("RGB").save(tmp_path, "PNG", optimize=True)
        logger.info(f"[AWARDS] Картка створена: {tmp_path}")
        return tmp_path

    except Exception as e:
        logger.error(f"[AWARDS] Помилка PIL: {e}", exc_info=True)
        return None


async def send_test_fifa_card(
    bot: Bot,
    chat_id: int,
    nickname: str = "yarik721",
    hp_score: int = 678,
    user_id: Optional[int] = None,
) -> bool:
    """
    Генерує тестову грамоту і надсилає її в чат.
    """
    card_path: Optional[str] = None

    try:
        card_path = create_fifa_card(
            nickname=nickname,
            hp_score=hp_score,
        )

        if not card_path or not os.path.exists(card_path):
            await bot.send_message(chat_id, "❌ Не вдалося згенерувати тестову грамоту.")
            return False

        await bot.send_photo(
            chat_id=chat_id,
            photo=FSInputFile(card_path),
            caption=f"🧪 Тест грамоти\n@{nickname} — {hp_score} HP",
        )
        return True

    except Exception as e:
        logger.error(f"[AWARDS] Помилка тестової грамоти: {e}", exc_info=True)
        await bot.send_message(chat_id, "❌ Помилка під час тесту грамоти.")
        return False

    finally:
        if card_path and os.path.exists(card_path):
            try:
                os.remove(card_path)
            except Exception as e:
                logger.warning(f"[AWARDS] Не вдалося видалити тестовий файл: {e}")


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