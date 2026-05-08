import asyncio
import logging
import time
from html import escape

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from cache import get_data, set_flag, KeyManager
from config import ADMIN_IDS, REPORTS_GROUP_ID
from database import check_user_exists
from reports import rollback_training_report
from services import safe_create_task, auto_delete, validate_quiz
from supabase_db import (
    get_all_users,
    get_all_activities,
    get_all_activities_in_period,
    get_referrals_in_period,
)
from utils.time_utils import get_current_week_period, get_last_finished_week_period, get_previous_finished_week_period, format_period
from utils.activity_utils import is_real_activity, is_training_activity

router = Router()
logger = logging.getLogger(__name__)

ADMIN_HELP_TTL = 120

REAL_ACTIVITY_ACTIONS = {"Gym", "Street", "Rest", "Skipped"}
TRAINING_ACTIONS = {"Gym", "Street"}


# ===================== HELPERS =====================

def _calc_percent(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100, 1)


def _count_values(users: list[dict], field_name: str, allowed_values: list[str]) -> dict[str, int]:
    result = {value: 0 for value in allowed_values}
    for user in users:
        raw_value = str(user.get(field_name) or "").strip()
        if raw_value in result:
            result[raw_value] += 1
    return result


def _count_filled(users: list[dict], field_name: str) -> int:
    return sum(1 for u in users if str(u.get(field_name) or "").strip())


def _word_users(count: int) -> str:
    return "юзер" if count == 1 else "юзери" if 2 <= count <= 4 else "юзерів"


def _word_actions(count: int) -> str:
    return "дія" if count == 1 else "дії" if 2 <= count <= 4 else "дій"


def _build_activity_counter(activities: list[dict]) -> dict[str, int]:
    result = {"Gym": 0, "Street": 0, "Rest": 0, "Skipped": 0}
    for activity in activities:
        action = str(activity.get("action_name") or "").strip()
        if action in result:
            result[action] += 1
    return result


def _sum_hp(activities: list[dict]) -> int:
    total = 0
    for activity in activities:
        if not is_real_activity(activity):
            continue
        try:
            total += int(activity.get("hp_change") or 0)
        except Exception:
            continue
    return total


def _count_active_users(activities: list[dict]) -> int:
    user_ids = set()
    for activity in activities:
        if not is_real_activity(activity):
            continue
        uid = str(activity.get("user_id") or "").strip()
        if uid:
            user_ids.add(uid)
    return len(user_ids)


def _format_stat_block(title, stats, total, ordered_items) -> str:
    lines = [title]
    for value_key, display_label in ordered_items:
        count = int(stats.get(value_key, 0) or 0)
        percent = _calc_percent(count, total)
        lines.append(f"{display_label} — <b>{count}</b> {_word_users(count)} — <b>{percent}%</b>")
    return "\n".join(lines)


def _calculate_turbo_index(total_users, active_users, training_count, video_reports, referrals_count, avg_activity) -> tuple[int, str]:
    active_score = _calc_percent(active_users, total_users)
    training_target = max(active_users * 3, 1)
    training_score = min(100.0, round((training_count / training_target) * 100, 1))
    video_score = min(100.0, round((video_reports / training_count) * 100, 1)) if training_count > 0 else 0.0
    referral_target = max(round(total_users * 0.2), 1)
    referral_score = min(100.0, round((referrals_count / referral_target) * 100, 1))
    avg_score = min(100.0, round((avg_activity / 5) * 100, 1))

    turbo_index = round(
        active_score * 0.40 + training_score * 0.30 +
        video_score * 0.15 + referral_score * 0.10 + avg_score * 0.05
    )

    if turbo_index >= 80:
        level = "ДУЖЕ ВИСОКИЙ 🔥"
    elif turbo_index >= 60:
        level = "ВИСОКИЙ 🔥"
    elif turbo_index >= 40:
        level = "СЕРЕДНІЙ ⚡"
    elif turbo_index > 0:
        level = "НИЗЬКИЙ 💤"
    else:
        level = "СТАРТ ТИЖНЯ 🏁"

    return turbo_index, level


async def _build_weekly_impact_data(finished_week: bool = True, previous_week: bool = False) -> dict:
    users = await get_all_users()
    total_users = len(users)

    if previous_week:
        week_start, week_end = get_previous_finished_week_period()
    elif finished_week:
        week_start, week_end = get_last_finished_week_period()
    else:
        week_start, week_end = get_current_week_period()

    activities_raw = await get_all_activities_in_period(
        created_at_from=week_start.isoformat(),
        created_at_to=week_end.isoformat(),
        limit=5000,
    )
    referrals_raw = await get_referrals_in_period(
        created_at_from=week_start.isoformat(),
        created_at_to=week_end.isoformat(),
        limit=5000,
    )

    real_activities = [a for a in activities_raw if is_real_activity(a)]
    training_activities = [a for a in real_activities if is_training_activity(a)]
    counts = _build_activity_counter(real_activities)

    active_users = _count_active_users(real_activities)
    real_total = len(real_activities)
    training_count = len(training_activities)
    referrals_count = len(referrals_raw)
    hp_total = _sum_hp(real_activities)
    avg_activity = round(real_total / active_users, 1) if active_users > 0 else 0.0
    active_percent = _calc_percent(active_users, total_users)

    user_map = {str(u.get("id")): u for u in users if u.get("id")}
    hp_by_user: dict[str, int] = {}
    for activity in real_activities:
        uid = str(activity.get("user_id") or "").strip()
        if uid:
            try:
                hp_by_user[uid] = hp_by_user.get(uid, 0) + int(activity.get("hp_change") or 0)
            except Exception:
                pass

    champion_nickname, champion_hp = "Немає", 0
    if hp_by_user:
        champion_uuid, champion_hp = max(hp_by_user.items(), key=lambda x: x[1])
        champion_row = user_map.get(champion_uuid, {})
        champion_nickname = champion_row.get("nickname") or champion_row.get("telegram_user_id") or "Невідомий"

    turbo_index, turbo_level = _calculate_turbo_index(
        total_users, active_users, training_count, training_count, referrals_count, avg_activity
    )

    return {
        "week_start": week_start, "week_end": week_end,
        "total_users": total_users, "active_users": active_users,
        "active_percent": active_percent, "real_total": real_total,
        "training_count": training_count, "video_reports": training_count,
        "referrals_count": referrals_count, "hp_total": hp_total,
        "avg_activity": avg_activity, "champion_nickname": str(champion_nickname),
        "champion_hp": int(champion_hp), "turbo_index": turbo_index,
        "turbo_level": turbo_level, "counts": counts,
    }


def _build_promo_stats_text(data: dict) -> str:
    champion = escape(str(data["champion_nickname"]))
    return (
        f"🔥 <b>TURBOTEAM WEEKLY IMPACT</b>\n\n"
        f"🔥 <b>Turbo Index: {data['turbo_index']}/100</b>\n"
        f"Рівень залучення: <b>{escape(str(data['turbo_level']))}</b>\n\n"
        f"📅 Період:\n{format_period(data['week_start'])} — {format_period(data['week_end'])}\n\n"
        f"👥 Учасників: <b>{data['total_users']}</b>\n"
        f"⚡ Активних: <b>{data['active_users']}</b> із <b>{data['total_users']}</b> — <b>{data['active_percent']}%</b>\n"
        f"🏋️ Підтверджених тренувань: <b>{data['training_count']}</b>\n"
        f"📹 Відео-звітів: <b>{data['video_reports']}</b>\n"
        f"🔁 Реферальних переходів: <b>{data['referrals_count']}</b>\n"
        f"🏆 HP видано: <b>{data['hp_total']}</b>\n"
        f"🔥 Середня активність: <b>{data['avg_activity']}</b> дії на активного учасника\n"
        f"🥇 Чемпіон тижня: <b>@{champion}</b> — <b>{data['champion_hp']} HP</b>\n\n"
        f"TurboTeam перетворює чат на гру: люди тренуються, звітують, змагаються і повертаються."
    )


def _format_delta(current_value, previous_value, suffix: str = "") -> str:
    try:
        diff = current_value - previous_value
    except Exception:
        return "—"
    diff_text = f"{diff:+.1f}" if isinstance(diff, float) else f"{int(diff):+d}"
    return f"{diff_text}{suffix}" if suffix else diff_text


def _build_promo_compare_text(current: dict, previous: dict) -> str:
    current_text = _build_promo_stats_text(current)
    previous_champion = escape(str(previous["champion_nickname"]))

    compare_text = (
        f"\n\n━━━━━━━━━━━━━━\n\n"
        f"📊 <b>ПОРІВНЯННЯ З МИНУЛИМ ТИЖНЕМ</b>\n\n"
        f"📅 Минулий період:\n{format_period(previous['week_start'])} — {format_period(previous['week_end'])}\n\n"
        f"🔥 Turbo Index: <b>{previous['turbo_index']}</b> → <b>{current['turbo_index']}</b> ({_format_delta(current['turbo_index'], previous['turbo_index'])})\n"
        f"⚡ Активних: <b>{previous['active_users']}</b> → <b>{current['active_users']}</b> ({_format_delta(current['active_users'], previous['active_users'])})\n"
        f"🏋️ Тренувань: <b>{previous['training_count']}</b> → <b>{current['training_count']}</b> ({_format_delta(current['training_count'], previous['training_count'])})\n"
        f"🔁 Рефералів: <b>{previous['referrals_count']}</b> → <b>{current['referrals_count']}</b> ({_format_delta(current['referrals_count'], previous['referrals_count'])})\n"
        f"🏆 HP: <b>{previous['hp_total']}</b> → <b>{current['hp_total']}</b> ({_format_delta(current['hp_total'], previous['hp_total'])})\n"
        f"🔥 Середня активність: <b>{previous['avg_activity']}</b> → <b>{current['avg_activity']}</b> ({_format_delta(float(current['avg_activity']), float(previous['avg_activity']))})\n\n"
        f"🥇 Чемпіон минулого тижня: <b>@{previous_champion}</b> — <b>{previous['champion_hp']} HP</b>"
    )

    if int(current["turbo_index"]) > int(previous["turbo_index"]):
        conclusion = "Висновок: тиждень сильніший за попередній. Є ріст, банда прокидається 🔥"
    elif int(current["turbo_index"]) < int(previous["turbo_index"]):
        conclusion = "Висновок: тиждень просів. Треба підсилювати мотивацію й повертати людей у гру 👀"
    else:
        conclusion = "Висновок: рівень тримається стабільно. Тепер задача — зробити наступний тиждень сильнішим 🏎️"

    return current_text + compare_text + "\n\n" + conclusion


def _build_admin_help_text() -> str:
    return (
        "🛠️ <b>АДМІН-КОМАНДИ TURBOTEAM</b>\n\n"
        "📋 <b>ОСНОВНІ</b>\n"
        "/adminhelp — список усіх адмін-команд\n"
        "/panel — відкрити Turbo-панель\n"
        "/menu — вивести Turbo-меню в групі\n"
        "/rating — показати рейтинг\n"
        "/reject — скасувати тренування через reply\n"
        "/quizstats — статистика квізу\n"
        "/activitystats — статистика активностей\n"
        "/promostats — короткий рекламний звіт\n"
        "/impactstats — повний impact-звіт\n\n"
        "🧪 <b>ТЕСТИ</b>\n"
        "/testaward — тестова FIFA-картка\n"
        "/testref — тест реферального повідомлення\n"
        "/loadtest 50 — безпечний тест паралельного навантаження\n\n"
        "🧹 <b>АДМІН-ДІЇ</b>\n"
        "/wipeuser 123456789 — видалити юзера за Telegram ID\n"
        "/wipeuser @username — видалити юзера за ніком\n\n"
        "📘 <b>ІНШЕ</b>\n"
        "/rules — текст правил"
    )


# ===================== HANDLERS =====================

@router.message(Command("adminhelp"))
async def handle_admin_help(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        try:
            await message.delete()
        except Exception:
            pass
        sent = await message.answer(_build_admin_help_text(), parse_mode="HTML")
        safe_create_task(auto_delete(sent, ADMIN_HELP_TTL))
    except Exception as e:
        logger.error(f"[ADMIN] handle_admin_help error: {e}", exc_info=True)
        sent = await message.answer("⚠️ Не вдалося відкрити список адмін-команд.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("reject"))
async def handle_reject_training(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    if m.chat.id != REPORTS_GROUP_ID:
        return
    if not m.reply_to_message:
        sent = await m.answer("⚠️ Використання: reply на кружок або текст репорту командою /reject")
        safe_create_task(auto_delete(sent, 5))
        try:
            await m.delete()
        except Exception:
            pass
        return

    ok = await rollback_training_report(
        bot=m.bot,
        group_message_id=m.reply_to_message.message_id,
        moderator_name=m.from_user.full_name,
        reason="manual_reject",
    )
    try:
        await m.delete()
    except Exception:
        pass
    if not ok:
        sent = await m.answer("⚠️ Не вдалося скасувати саме це тренування. Reply має бути на кружок або текст репорту.")
        safe_create_task(auto_delete(sent, 5))


@router.message(Command("quizstats"))
async def handle_quiz_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        users = await get_all_users()
        total_users = len(users)
        if total_users == 0:
            sent = await m.answer("📉 У базі поки немає юзерів.")
            safe_create_task(auto_delete(sent, 10))
            return

        level_values = ["Новачок", "Середній", "Профі"]
        goal_values = ["Схуднення", "Набір маси", "Витривалість"]
        weekly_plan_items = [("1-2 рази", "1–2 рази"), ("3-4 рази", "3–4 рази"), ("5+ разів", "5+ разів")]
        training_place_items = [("У залі", "У залі"), ("На вулиці / турніках", "На вулиці / турніках"), ("І там, і там", "І там, і там")]

        text = (
            f"📊 <b>СТАТИСТИКА КВІЗУ</b>\n\n"
            f"👥 Усього юзерів у базі: <b>{total_users}</b>\n\n"
            f"{_format_stat_block('🎖️ <b>РІВЕНЬ</b>', _count_values(users, 'level', level_values), _count_filled(users, 'level'), [(x, x) for x in level_values])}\n\n"
            f"{_format_stat_block('🎯 <b>ЦІЛЬ</b>', _count_values(users, 'goal', goal_values), _count_filled(users, 'goal'), [(x, x) for x in goal_values])}\n\n"
            f"{_format_stat_block('📅 <b>ПЛАН НА ТИЖДЕНЬ</b>', _count_values(users, 'weekly_plan', [i[0] for i in weekly_plan_items]), _count_filled(users, 'weekly_plan'), weekly_plan_items)}\n\n"
            f"{_format_stat_block('🏋️ <b>ДЕ ТРЕНУЮТЬСЯ</b>', _count_values(users, 'training_place', [i[0] for i in training_place_items]), _count_filled(users, 'training_place'), training_place_items)}"
        )
        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))
        try:
            await m.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] handle_quiz_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати статистику квізу.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("activitystats"))
async def handle_activity_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        users = await get_all_users()
        total_users = len(users)
        week_start, week_end = get_current_week_period()

        all_activities_raw = await get_all_activities(limit=10000)
        week_activities_raw = await get_all_activities_in_period(
            created_at_from=week_start.isoformat(),
            created_at_to=week_end.isoformat(),
            limit=5000,
        )

        all_real = [a for a in all_activities_raw if is_real_activity(a)]
        week_real = [a for a in week_activities_raw if is_real_activity(a)]
        week_counts = _build_activity_counter(week_real)
        active_users_week = _count_active_users(week_real)
        week_total = len(week_real)
        week_hp = _sum_hp(week_real)
        avg_activity = round(week_total / active_users_week, 1) if active_users_week > 0 else 0.0

        text = (
            f"📊 <b>СТАТИСТИКА АКТИВНОСТІ</b>\n\n"
            f"📅 <b>TurboTeam-тиждень:</b>\n{format_period(week_start)} — {format_period(week_end)}\n\n"
            f"👥 Усього юзерів у базі: <b>{total_users}</b>\n"
            f"🔥 Активних юзерів за тиждень: <b>{active_users_week}</b>\n\n"
            f"📦 Активностей за весь час: <b>{len(all_real)}</b> {_word_actions(len(all_real))}\n"
            f"📅 Активностей за тиждень: <b>{week_total}</b> {_word_actions(week_total)}\n\n"
            f"🏋️ Gym: <b>{week_counts['Gym']}</b>\n"
            f"🦾 Street: <b>{week_counts['Street']}</b>\n"
            f"🧘 Rest: <b>{week_counts['Rest']}</b>\n"
            f"🚫 Skip: <b>{week_counts['Skipped']}</b>\n\n"
            f"⚡ HP за тиждень: <b>{week_hp}</b>\n"
            f"📈 Середня активність: <b>{avg_activity}</b> дії на активного юзера"
        )
        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))
        try:
            await m.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] handle_activity_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати статистику активностей.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("promostats"))
async def handle_promo_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        data = await _build_weekly_impact_data(finished_week=True)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📊 Минулий тиждень", callback_data="promo_compare_previous")
        ]])
        sent = await m.answer(_build_promo_stats_text(data), parse_mode="HTML", reply_markup=kb)
        safe_create_task(auto_delete(sent, 900))
        try:
            await m.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] handle_promo_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати promo-статистику.")
        safe_create_task(auto_delete(sent, 10))


@router.callback_query(F.data == "promo_compare_previous")
async def handle_promo_compare_previous(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer()
        return
    try:
        current_data = await _build_weekly_impact_data(finished_week=True)
        previous_data = await _build_weekly_impact_data(finished_week=True, previous_week=True)
        await callback.message.edit_text(_build_promo_compare_text(current_data, previous_data), parse_mode="HTML")
        await callback.answer("Порівняння додано ✅")
    except Exception as e:
        logger.error(f"[ADMIN] handle_promo_compare_previous error: {e}", exc_info=True)
        await callback.answer("⚠️ Не вдалося зібрати порівняння.", show_alert=True)


@router.message(Command("impactstats"))
async def handle_impact_stats(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        data = await _build_weekly_impact_data(finished_week=True)
        champion = escape(str(data["champion_nickname"]))
        counts = data["counts"]

        text = (
            f"🔥 <b>TURBOTEAM WEEKLY IMPACT</b>\n\n"
            f"<b>Turbo Index: {data['turbo_index']}/100</b>\n"
            f"Рівень залучення: <b>{escape(str(data['turbo_level']))}</b>\n\n"
            f"За минулий тиждень бот не просто рахував активність — він змушував учасників "
            f"повертатися в гру, тренуватися, звітувати й змагатися.\n\n"
            f"📅 <b>Період:</b>\n{format_period(data['week_start'])} — {format_period(data['week_end'])}\n\n"
            f"👥 Учасників у базі: <b>{data['total_users']}</b>\n"
            f"⚡ Активних за тиждень: <b>{data['active_users']}</b> із <b>{data['total_users']}</b> — <b>{data['active_percent']}%</b>\n"
            f"🏋️ Підтверджених тренувань: <b>{data['training_count']}</b>\n"
            f"📹 Відео-звітів: <b>{data['video_reports']}</b>\n"
            f"🔁 Реферальних переходів: <b>{data['referrals_count']}</b>\n"
            f"🏆 HP видано: <b>{data['hp_total']}</b>\n"
            f"🔥 Середня активність: <b>{data['avg_activity']}</b> дії на активного учасника\n"
            f"🥇 Чемпіон тижня: <b>@{champion}</b> — <b>{data['champion_hp']} HP</b>\n\n"
            f"📊 <b>Розбивка дій:</b>\n"
            f"🏋️ Gym: <b>{counts['Gym']}</b>\n"
            f"🦾 Street: <b>{counts['Street']}</b>\n"
            f"🧘 Rest: <b>{counts['Rest']}</b>\n"
            f"🚫 Skip: <b>{counts['Skipped']}</b>\n\n"
            f"<b>Що це означає:</b>\n"
            f"• {data['active_percent']}% учасників не просто зайшли в чат, а виконали дію.\n"
            f"• {data['training_count']} тренувань підтверджені через бота.\n"
            f"• Рейтинг і HP створили змагання всередині комʼюніті.\n"
            f"• Реферали показують органічний ріст без додаткової реклами.\n\n"
            f"<b>Висновок:</b>\n"
            f"TurboTeam перетворює Telegram-групу з пасивного чату на фітнес-гру, "
            f"де люди тренуються, звітують, повертаються і тягнуть друзів."
        )
        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 300))
        try:
            await m.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] handle_impact_stats error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Не вдалося зібрати impact-статистику.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("testref"))
async def handle_test_referral_message(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        newbie_html = escape("test_user_[bad]*name_with_underscore")
        referrer_html = escape("@bad_ref_[name]*test_user")
        text = (
            "🧪 <b>TEST REFERRAL MESSAGE</b>\n\n"
            f"Новий гравець <b>{newbie_html}</b> (+50 HP)\n"
            f"Прийшов за запрошенням від: <b>{referrer_html}</b> (+150 HP) 🔥\n\n"
            "✅ Якщо ти бачиш це повідомлення — реферальні повідомлення не падають від спецсимволів."
        )
        sent = await m.bot.send_message(chat_id=REPORTS_GROUP_ID, text=text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 120))
        try:
            await m.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] handle_test_referral_message error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Test referral message failed. Дивись логи.")
        safe_create_task(auto_delete(sent, 10))


@router.message(Command("loadtest"))
async def handle_loadtest(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    try:
        parts = (m.text or "").strip().split()
        total_jobs = max(1, min(200, int(parts[1]) if len(parts) >= 2 else 20))

        progress = await m.answer(
            f"⏳ Запускаю load test на <b>{total_jobs}</b> паралельних задач...",
            parse_mode="HTML",
        )
        result = await _run_loadtest_batch(total_jobs)

        text = (
            f"🧪 <b>LOAD TEST RESULT</b>\n\n"
            f"📦 Задач: <b>{result['total_jobs']}</b>\n"
            f"✅ Успішно: <b>{result['success_count']}</b>\n"
            f"❌ Помилок: <b>{result['fail_count']}</b>\n\n"
            f"⏱ Загальний час: <b>{result['total_duration_s']} c</b>\n"
            f"⚡ Найшвидша задача: <b>{result['min_ms']} ms</b>\n"
            f"🐢 Найтриваліша задача: <b>{result['max_ms']} ms</b>\n"
            f"📊 Середній час: <b>{result['avg_ms']} ms</b>\n\n"
            f"ℹ️ Це безпечний тест конкурентності без створення юзерів у базі."
        )
        try:
            await progress.delete()
        except Exception:
            pass
        sent = await m.answer(text, parse_mode="HTML")
        safe_create_task(auto_delete(sent, 180))
        try:
            await m.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] handle_loadtest error: {e}", exc_info=True)
        sent = await m.answer("⚠️ Load test впав.")
        safe_create_task(auto_delete(sent, 10))


async def _run_single_load_job(job_id: int) -> dict:
    started = time.perf_counter()
    quiz_data = {"gender": "Чоловік", "level": "Новачок", "goal": "Схуднення", "weekly_plan": "3-4 рази", "training_place": "І там, і там"}
    test_uid = 900000000 + job_id
    try:
        quiz_ok = validate_quiz(quiz_data)
        if not quiz_ok:
            return {"ok": False, "job_id": job_id, "duration_ms": round((time.perf_counter() - started) * 1000, 1), "error": "quiz validation failed"}
        redis_key = KeyManager.get_profile_warn_key(test_uid)
        await set_flag(redis_key, ex=60)
        redis_value = await get_data(redis_key)
        exists = await check_user_exists(test_uid)
        await asyncio.sleep(0)
        return {"ok": redis_value is not None and exists is False, "job_id": job_id, "duration_ms": round((time.perf_counter() - started) * 1000, 1)}
    except Exception as e:
        logger.error(f"[LOADTEST] job failed: job_id={job_id}, error={e}", exc_info=True)
        return {"ok": False, "job_id": job_id, "duration_ms": round((time.perf_counter() - started) * 1000, 1), "error": str(e)}


async def _run_loadtest_batch(total_jobs: int) -> dict:
    started = time.perf_counter()
    tasks = [asyncio.create_task(_run_single_load_job(i + 1)) for i in range(total_jobs)]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    success_count = sum(1 for r in results if r.get("ok"))
    durations = [r.get("duration_ms", 0) for r in results]
    return {
        "total_jobs": total_jobs,
        "success_count": success_count,
        "fail_count": total_jobs - success_count,
        "total_duration_s": round(time.perf_counter() - started, 2),
        "min_ms": min(durations, default=0),
        "max_ms": max(durations, default=0),
        "avg_ms": round(sum(durations) / total_jobs, 1) if total_jobs > 0 else 0.0,
    }
