from aiogram import types

from config import PROFILE_WEB_APP_SHORT_NAME


def get_inline_menu(bot_username: str | None = None) -> types.InlineKeyboardMarkup:
    """Main TurboTeam inline menu."""
    gym_button: types.InlineKeyboardButton
    street_button: types.InlineKeyboardButton
    challenge_button: types.InlineKeyboardButton

    if bot_username:
        gym_button = types.InlineKeyboardButton(
            text="🏋️‍♂️ Gym Train",
            url=f"https://t.me/{bot_username}?start=gym",
        )

        street_button = types.InlineKeyboardButton(
            text="🦾 Street Train",
            url=f"https://t.me/{bot_username}?start=street",
        )

        challenge_button = types.InlineKeyboardButton(
            text="🔥 ЧЕЛЕНДЖ ТИЖНЯ",
            url=f"https://t.me/{bot_username}?start=challenge",
        )

    else:
        gym_button = types.InlineKeyboardButton(
            text="🏋️‍♂️ Gym Train",
            callback_data="train_gym",
        )

        street_button = types.InlineKeyboardButton(
            text="🦾 Street Train",
            callback_data="train_street",
        )

        challenge_button = types.InlineKeyboardButton(
            text="🔥 ЧЕЛЕНДЖ ТИЖНЯ",
            callback_data="weekly_challenge",
        )

    rows = [
        [
            gym_button,
            street_button,
        ],
        [
            challenge_button,
        ],
        [
            types.InlineKeyboardButton(
                text="🧘‍♂️ Відпочинок",
                callback_data="action_rest",
            ),
            types.InlineKeyboardButton(
                text="🚫 Забив болт",
                callback_data="action_skip",
            ),
        ],
        [
            types.InlineKeyboardButton(
                text="📘 ПРАВИЛА СПІЛЬНОТИ",
                callback_data="community_rules",
            ),
        ],
    ]

    if bot_username:
        # web_app-кнопки працюють лише в приватних чатах, тому в групі
        # відкриваємо Mini App через пряме посилання t.me/<bot>/<short_name>.
        rows.append([
            types.InlineKeyboardButton(
                text="👤 Мій профіль",
                url=f"https://t.me/{bot_username}/{PROFILE_WEB_APP_SHORT_NAME}",
            ),
        ])

    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def get_quiz_reply_keyboard(web_app_url: str) -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(
                    text="🚀 ПРОЙТИ ВІДБІР",
                    web_app=types.WebAppInfo(url=web_app_url),
                )
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_rating_reply_keyboard() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="🚀 Запросити друга 🔥"),
            ],
        ],
        resize_keyboard=True,
        input_field_placeholder="Запросити друга 👇",
    )