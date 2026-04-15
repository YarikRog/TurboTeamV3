from aiogram import types


def get_inline_menu(bot_username: str | None = None) -> types.InlineKeyboardMarkup:
    """Головне меню TurboTeam - тільки inline кнопки."""
    gym_button: types.InlineKeyboardButton
    street_button: types.InlineKeyboardButton

    if bot_username:
        gym_button = types.InlineKeyboardButton(
            text="🏋️‍♂️ Gym Train",
            url=f"https://t.me/{bot_username}?start=gym",
        )
        street_button = types.InlineKeyboardButton(
            text="🦾 Street Train",
            url=f"https://t.me/{bot_username}?start=street",
        )
    else:
        gym_button = types.InlineKeyboardButton(text="🏋️‍♂️ Gym Train", callback_data="train_gym")
        street_button = types.InlineKeyboardButton(text="🦾 Street Train", callback_data="train_street")

    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                gym_button,
                street_button,
            ],
            [
                types.InlineKeyboardButton(text="🧘‍♂️ Відпочинок", callback_data="action_rest"),
                types.InlineKeyboardButton(text="🚫 Забив болт", callback_data="action_skip"),
            ],
            [
                types.InlineKeyboardButton(text="🏆 Рейтинг ТОП", callback_data="show_rating"),
                types.InlineKeyboardButton(text="🚀 Запросити друга 🔥", callback_data="invite_friend"),
            ],
        ]
    )


def get_rating_reply_keyboard() -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="🏆 Рейтинг ТОП")]],
        resize_keyboard=True,
    )
