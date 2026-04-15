from __future__ import annotations
from datetime import timedelta
from typing import List
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from app.services.db import get_user, set_message_id
from app.services.lessons_loader import load_lessons_for_user_group
from app.utils.week_parity import week_parity_for_date
from app.utils.dt import now_tz
from app.utils.format_schedule import format_day, format_week_compact_mono
from app.config import settings

router = Router()

_NO_LESSONS_DEFAULT = [
    "Сегодня пар нет — самое время открыть YouTube через @kairavpn_bot 🎬",
    "Свободный день: сериальчик + @kairavpn_bot = идеальный план 🍿",
    "Без пар — без границ. Путешествуй по контенту с @kairavpn_bot 🌍",
    "Сегодня отдыхаем. Музыка и подкасты летят быстрее с @kairavpn_bot 🎧",
    "Можно поиграть без лагов — подключай @kairavpn_bot 🎮",
    "Время подтянуть английский на YouTube — поможет @kairavpn_bot 🇬🇧",
    "Пары отменились, а планы — нет. С @kairavpn_bot всё работает 🚀",
    "Лекции — завтра, а стримы — сегодня. Смотри с @kairavpn_bot 🔐",
    "День без пар — день для себя и @kairavpn_bot 💆‍♂️",
    "Скачай материалы без ограничений — выручит @kairavpn_bot ⬇️",
    "Кофе, плейлист, @kairavpn_bot — и идеальный день готов ☕",
    "Никаких пар, только релакс и безопасный интернет с @kairavpn_bot 🌈",
    "Пора обновить списки смотреть-позже? Вперёд с @kairavpn_bot 📺",
    "Учёба подождёт — контент не ждёт. В путь с @kairavpn_bot 🧭",
    "Готовься к сессии умно: курсы и лекции без блоков с @kairavpn_bot 🎓",
    "Свободное окно превратилось в свободный день — спасибо @kairavpn_bot ⏳",
    "Проверяй мемы из любых углов интернета с @kairavpn_bot 😎",
    "Сегодня можно навести порядок в закладках — и в приватности с @kairavpn_bot 🧹",
    "Хочешь стримы без «этот контент недоступен»? Подключай @kairavpn_bot 🛰️",
    "Работай из кофейни спокойно — шифрует @kairavpn_bot ☕🔒",
    "Виртуальное путешествие по кампусам мира? Легко с @kairavpn_bot ✈️",
    "Никаких дедлайнов сегодня — только безопасный серф с @kairavpn_bot 🏄",
    "Запланируй марафон лекций и фильмов — выручит @kairavpn_bot 🗂️",
    "Общажный роутер снова чудит? @kairavpn_bot наведёт порядок 🛠️",
    "Нулевой день по расписанию — но не по впечатлениям. Врубай @kairavpn_bot ✨",
    "Качай, смотри, слушай — без границ и лишних глаз с @kairavpn_bot 👀",
    "Хватит откладывать. С @kairavpn_bot твой список «посмотреть позже» стал «сейчас» ✅",
    "Сегодня идеальный день, чтобы настроить @kairavpn_bot один раз и забыть ⚙️",
]


# ---------- клавиатуры ----------
def _kb_main_menu():
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Показать расписание", callback_data="main:schedule")
    kb.button(text="🔍 Чужое расписание", callback_data="main:isu_schedule")
    kb.button(text="⚙️ Настройки", callback_data="main:settings")
    kb.button(text="📆 Google Calendar", callback_data="main:gcal")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()

def kb_schedule_root():
    kb = InlineKeyboardBuilder()
    kb.button(text="Сегодня", callback_data="sched:day:today")
    kb.button(text="Завтра", callback_data="sched:day:tomorrow")
    kb.button(text="Неделя", callback_data="sched:week:auto")
    kb.button(text="Назад", callback_data="start:to_main")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()

def kb_day_controls(day_name: str, parity: str):
    kb = InlineKeyboardBuilder()
    # показываем альтернативную чётность для этого же дня
    alt = "чёт" if parity == "нечёт" else "нечёт"
    kb.button(text=f"Другая чётность ({'Чёт' if alt=='чёт' else 'Нечёт'})", callback_data=f"sched:day:same:{day_name}:{alt}")
    kb.button(text="Показать неделю", callback_data="sched:week:auto")
    kb.button(text="Назад", callback_data="sched:root")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def kb_week_controls(parity: str):
    kb = InlineKeyboardBuilder()
    alt = "чёт" if parity == "нечёт" else "нечёт"
    kb.button(text=f"Сменить на {'Чётную' if alt == 'чёт' else 'Нечётную'}", callback_data=f"sched:week:{alt}")
    kb.button(text="Сегодня", callback_data="sched:day:today")
    kb.button(text="Назад", callback_data="sched:root")
    kb.adjust(1, 1, 1)  # каждая кнопка на своей строке, «Назад» — в самом низу
    return kb.as_markup()

# ---------- вспомогательное ----------

def _russian_day_name(dt) -> str:
    # 0=Mon → ПОНЕДЕЛЬНИК
    names = ["ПОНЕДЕЛЬНИК","ВТОРНИК","СРЕДА","ЧЕТВЕРГ","ПЯТНИЦА","СУББОТА","ВОСКРЕСЕНЬЕ"]
    return names[dt.weekday()]

async def _load_lessons_for_user_group(user: dict):
    return await load_lessons_for_user_group(user)


def _norm_parity(p: str) -> str:
    p = str(p or "").strip().lower().replace("ё", "е")
    if "неч" in p:
        return "нечёт"
    if "чет" in p:
        return "чёт"
    return p

def _filter_by_day_and_parity(lessons: List[dict], day_name: str, parity: str) -> List[dict]:
    np = _norm_parity(parity)
    return [it for it in lessons if str(it["day"]).strip().upper() == day_name and _norm_parity(it.get("parity")) == np]

def _filter_by_parity(lessons: List[dict], parity: str) -> List[dict]:
    np = _norm_parity(parity)
    return [it for it in lessons if _norm_parity(it.get("parity")) == np]

async def _send_or_edit(q: CallbackQuery, text: str, kb):
    user = get_user(q.from_user.id)
    # Режим 1 — редактируем одно сообщение
    if user and user.get("type") == 1 and user.get("message_id"):
        try:
            await q.message.bot.edit_message_text(
                chat_id=q.message.chat.id,
                message_id=user["message_id"],
                text=text,
                reply_markup=kb
            )
            try:
                await q.answer()
            except TelegramBadRequest:
                # callback мог протухнуть, если загрузка заняла >10-15 сек
                pass
            return
        except Exception:
            pass  # упало редактирование — отправим новое и обновим message_id

    m = await q.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
    if user and user.get("type") == 1:
        set_message_id(q.from_user.id, m.message_id)
    try:
        await q.answer()
    except TelegramBadRequest:
        pass

# ---------- вход из главного меню ----------
@router.callback_query(F.data == "start:to_main")
async def to_main(q: CallbackQuery):
    await q.message.edit_text("Готово! Что дальше?", reply_markup=_kb_main_menu())
    await q.answer()

@router.callback_query(F.data == "main:schedule")
async def schedule_entry(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала завершите онбординг: курс и группа.", show_alert=True)
        return
    await _send_or_edit(q, "Выберите период:", kb_schedule_root())

# ---------- корень выбора периода ----------
@router.callback_query(F.data == "sched:root")
async def sched_root(q: CallbackQuery):
    await _send_or_edit(q, "Выберите период:", kb_schedule_root())

# ---------- сегодня / завтра ----------
@router.callback_query(F.data.in_({"sched:day:today", "sched:day:tomorrow"}))
async def sched_day_today_tomorrow(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True)
        return

    tz = user.get("timezone") or settings.timezone
    now = now_tz(tz)
    is_tomorrow = q.data.endswith("tomorrow")
    target = now + timedelta(days=1 if is_tomorrow else 0)

    parity = week_parity_for_date(target, tz)
    # День недели в UPPER, как в парсере
    DAY_UP = ["ПОНЕДЕЛЬНИК","ВТОРНИК","СРЕДА","ЧЕТВЕРГ","ПЯТНИЦА","СУББОТА","ВОСКРЕСЕНЬЕ"]
    day_upper = DAY_UP[target.weekday()]

    # пары для пользователя
    lessons = await _load_lessons_for_user_group(user)
    day_lessons = [it for it in lessons if _norm_parity(it.get("parity")) == _norm_parity(parity) and it["day"] == day_upper]

    text = format_day(user["group_code"], day_upper, parity, day_lessons)

    # клавиатура под день (как и раньше)
    kb = kb_day_controls(day_upper, parity)  # если у тебя уже есть эта функция
    # или собери здесь InlineKeyboardBuilder

    await _send_or_edit(q, text, kb)

# тот же день, но принудительная смена чётности
@router.callback_query(F.data.startswith("sched:day:same:"))
async def sched_day_same(q: CallbackQuery):
    # data: sched:day:same:{DAY}:{parity}
    _, _, _, day_name, parity = q.data.split(":")
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True)
        return

    group_lessons = await _load_lessons_for_user_group(user)

    day_upper = str(day_name).strip().upper()
    day_lessons = [
        it for it in group_lessons
        if _norm_parity(it.get("parity")) == _norm_parity(parity) and it["day"] == day_upper
    ]

    text = format_day(user["group_code"], day_upper, parity, day_lessons)
    await _send_or_edit(q, text, kb_day_controls(day_upper, parity))

@router.callback_query(F.data.startswith("sched:week"))
async def sched_week(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True)
        return

    tz = user.get("timezone") or settings.timezone
    parity = q.data.split(":")[-1]
    if parity == "auto":
        parity = week_parity_for_date(None, tz)

    lessons_grp = await _load_lessons_for_user_group(user)
    week_lessons = [it for it in lessons_grp if _norm_parity(it.get("parity")) == _norm_parity(parity)]

    text = format_week_compact_mono(user["group_code"], parity, week_lessons)

    # клавиатура
    kb = InlineKeyboardBuilder()
    alt = "чёт" if parity == "нечёт" else "нечёт"
    kb.button(text=f"Сменить на {'Чётную' if alt=='чёт' else 'Нечётную'}", callback_data=f"sched:week:{alt}")
    kb.button(text="Сегодня", callback_data="sched:day:today")
    kb.button(text="Назад", callback_data="sched:root")
    kb.adjust(1, 1, 1)

    # режим редактируемого сообщения (type=1)
    if user.get("type") == 1 and user.get("message_id"):
        try:
            await q.message.bot.edit_message_text(
                chat_id=q.message.chat.id,
                message_id=user["message_id"],
                text=text,
                reply_markup=kb.as_markup()
            )
            try:
                await q.answer()
            except TelegramBadRequest:
                pass
            return
        except Exception:
            pass

    m = await q.message.answer(text, reply_markup=kb.as_markup(), disable_web_page_preview=True)
    if user.get("type") == 1:
        set_message_id(q.from_user.id, m.message_id)
    try:
        await q.answer()
    except TelegramBadRequest:
        pass
