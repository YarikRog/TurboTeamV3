from aiogram import types


def get_inline_menu() -> types.InlineKeyboardMarkup:
    """Головне меню TurboTeam - тільки inline кнопки."""
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="🏋️‍♂️ Gym Train", callback_data="train_gym"),
                types.InlineKeyboardButton(text="🦾 Street Train", callback_data="train_street"),
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
