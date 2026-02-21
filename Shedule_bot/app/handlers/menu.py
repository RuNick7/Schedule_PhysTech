from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
import asyncio
import re
from urllib.parse import urlparse, parse_qs

from app.config import settings
from app.services.db import (
    get_user,
    set_course,
    set_group,
    set_schedule_source_mode,
    set_myitmo_login,
    set_myitmo_tokens,
    set_user_sheet_source,
    clear_user_sheet_source,
)
from app.services.myitmo_client import exchange_password_for_tokens, MyItmoError
from app.services.groups import list_groups_for_course

router = Router()


class MyItmoSetup(StatesGroup):
    waiting_login = State()
    waiting_password = State()


class SheetLinkSetup(StatesGroup):
    waiting_link = State()

# --- keyboards ---

def _kb_settings(user) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Сменить группу", callback_data="settings:change_group")
    kb.button(text="🎓 Сменить курс", callback_data="settings:change_course")
    kb.button(text="🧭 Источник расписания", callback_data="settings:change_source")
    kb.button(text="📄 Таблица Google Sheets", callback_data="settings:sheet:open")
    kb.button(text="🔐 my.itmo аккаунт", callback_data="settings:myitmo:open")
    kb.button(text="📨 Настроить автоотправку", callback_data="autosend:open")
    kb.button(text="⬅️ Назад", callback_data="settings:back")
    kb.adjust(1, 1, 1, 1, 1, 1, 1)
    return kb

def _kb_courses():
    kb = InlineKeyboardBuilder()
    for i in (1, 2, 3, 4):
        kb.button(text=f"{i} курс", callback_data=f"settings:course:{i}")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(4, 1)
    return kb.as_markup()

def _kb_groups(course: int):
    groups = list_groups_for_course(course)
    kb = InlineKeyboardBuilder()
    # выводим рядами по 3
    for g in groups:
        kb.button(text=g, callback_data=f"settings:group:{g}")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(3, 1)
    return kb.as_markup()

# --- text ---

def _autosend_summary(u) -> str:
    enabled = bool(u.get("autosend_enabled"))
    if not enabled:
        return "⛔️ Выключена"
    return f"✅"

def _source_mode_label(mode: str | None) -> str:
    m = (mode or "sheets").strip().lower()
    if m == "myitmo_full":
        return "🎓 Только my.itmo"
    if m == "hybrid":
        return "🔀 Таблица + my.itmo"
    return "🧩 Только таблица"


def _mask_login(login: str | None) -> str:
    s = (login or "").strip()
    if not s:
        return "не задан"
    if len(s) <= 2:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 2)

def _kb_source_modes():
    kb = InlineKeyboardBuilder()
    kb.button(text="🧩 Только таблица (как раньше)", callback_data="settings:source:sheets")
    kb.button(text="🎓 Только my.itmo (полная интеграция)", callback_data="settings:source:myitmo_full")
    kb.button(text="🔀 Таблица + my.itmo (частичное дополнение)", callback_data="settings:source:hybrid")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()


def _kb_myitmo_manage():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔐 Подключить / переподключить", callback_data="settings:myitmo:connect")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(1, 1)
    return kb.as_markup()


def _kb_sheet_manage():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Указать ссылку на таблицу", callback_data="settings:sheet:set")
    kb.button(text="🗑 Сбросить ссылку таблицы", callback_data="settings:sheet:clear")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def _parse_sheet_link(raw: str) -> tuple[str, int] | None:
    s = (raw or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    if not m:
        return None
    spreadsheet_id = m.group(1)
    p = urlparse(s)
    gid = None
    q = parse_qs(p.query or "")
    if q.get("gid"):
        gid = q["gid"][0]
    if gid is None and "gid=" in (p.fragment or ""):
        frag_q = parse_qs(p.fragment)
        if frag_q.get("gid"):
            gid = frag_q["gid"][0]
    try:
        sheet_gid = int(gid) if gid is not None else 0
    except ValueError:
        sheet_gid = 0
    return spreadsheet_id, sheet_gid

def _settings_text(u) -> str:
    group = u.get("group_code") or "—"
    course = u.get("course") or "—"
    source = _source_mode_label(u.get("schedule_source_mode"))
    sheet_src = (
        f"персональная (gid={u.get('user_sheet_gid')})"
        if u.get("user_spreadsheet_id")
        else "из .env"
    )
    myitmo_login = _mask_login(u.get("myitmo_username"))
    myitmo_conn = "подключен" if u.get("myitmo_refresh_token") else "не подключен"
    auto = _autosend_summary(u)
    lines = [
        "⚙️ <b>Настройки</b>",
        f"👥 Группа: <b>{group}</b>",
        f"🎓 Курс: <b>{course}</b>",
        f"🧭 Источник расписания: {source}",
        f"📄 Таблица: <b>{sheet_src}</b>",
        f"🔐 my.itmo: логин <b>{myitmo_login}</b>, токен <b>{myitmo_conn}</b>",
        f"📨 Автоотправка: {auto}",
        "",
        "Выберите, что изменить:",
    ]
    return "\n".join(lines)

# --- handlers ---

@router.callback_query(F.data == "main:settings")
@router.callback_query(F.data == "settings:open")
async def open_settings(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u:
        await q.answer("Сначала /start", show_alert=True); return
    kb = _kb_settings(u).as_markup()
    await q.message.edit_text(_settings_text(u), reply_markup=kb)
    await q.answer()

@router.callback_query(F.data == "settings:back")
async def settings_back(q: CallbackQuery):
    # назад в главное меню
    from app.handlers.start import _kb_main_menu
    await q.message.edit_text("Что дальше?", reply_markup=_kb_main_menu())
    await q.answer()

@router.callback_query(F.data == "settings:change_course")
async def settings_change_course(q: CallbackQuery):
    await q.message.edit_text("Выберите курс:", reply_markup=_kb_courses())
    await q.answer()

@router.callback_query(F.data.startswith("settings:course:"))
async def settings_set_course(q: CallbackQuery):
    course = int(q.data.split(":")[-1])
    set_course(q.from_user.id, course)
    # сразу предложим выбрать группу
    await q.message.edit_text(
        f"Курс: <b>{course}</b>\nТеперь выберите группу:",
        reply_markup=_kb_groups(course)
    )
    await q.answer()

@router.callback_query(F.data == "settings:change_group")
async def settings_change_group(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u or not u.get("course"):
        await q.message.edit_text("Сначала выберите курс:", reply_markup=_kb_courses())
        await q.answer()
        return
    await q.message.edit_text(
        f"Курс: <b>{u['course']}</b>\nВыберите группу:",
        reply_markup=_kb_groups(int(u["course"]))
    )
    await q.answer()

@router.callback_query(F.data == "settings:change_source")
async def settings_change_source(q: CallbackQuery):
    await q.message.edit_text(
        "Выберите источник расписания:\n\n"
        "🧩 <b>Только таблица</b> — как раньше.\n"
        "🎓 <b>Только my.itmo</b> — всё расписание берём из my.itmo.\n"
        "🔀 <b>Таблица + my.itmo</b> — таблица как основной источник, "
        "my.itmo дополняет пары с недостающими данными.",
        reply_markup=_kb_source_modes(),
    )
    await q.answer()

@router.callback_query(F.data.startswith("settings:source:"))
async def settings_set_source(q: CallbackQuery):
    mode = q.data.split(":")[-1]
    u = get_user(q.from_user.id) or {}
    if mode in ("myitmo_full", "hybrid"):
        if not (u.get("myitmo_username") and u.get("myitmo_refresh_token")):
            await q.answer(
                "Сначала подключите my.itmo в «🔐 my.itmo аккаунт».",
                show_alert=True,
            )
            return
    set_schedule_source_mode(q.from_user.id, mode)
    u = get_user(q.from_user.id)
    await q.message.edit_text(
        f"Источник расписания обновлён: <b>{_source_mode_label(mode)}</b>\n\n"
        "Можно продолжить настройку:",
        reply_markup=_kb_settings(u).as_markup(),
    )
    await q.answer("Сохранено")


@router.callback_query(F.data == "settings:myitmo:open")
async def settings_myitmo_open(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u:
        await q.answer("Сначала /start", show_alert=True)
        return
    login = _mask_login(u.get("myitmo_username"))
    token_state = "да" if u.get("myitmo_refresh_token") else "нет"
    await q.message.edit_text(
        "🔐 <b>my.itmo аккаунт</b>\n\n"
        f"Логин: <b>{login}</b>\n"
        f"Токен подключен: <b>{token_state}</b>\n\n"
        "Данные используются для режимов:\n"
        "• Только my.itmo\n"
        "• Таблица + my.itmo\n\n"
        "Пароль используется только один раз для получения токена и не хранится в базу данных.\n"
        "Подключение выполняется в 2 шага: сначала логин, затем пароль.",
        reply_markup=_kb_myitmo_manage(),
    )
    await q.answer()


@router.callback_query(F.data == "settings:sheet:open")
async def settings_sheet_open(q: CallbackQuery):
    u = get_user(q.from_user.id) or {}
    if u.get("user_spreadsheet_id"):
        text = (
            "📄 <b>Таблица Google Sheets</b>\n\n"
            "Сейчас: персональная таблица.\n"
            f"gid: <b>{u.get('user_sheet_gid')}</b>\n\n"
            "Можно обновить ссылку или сбросить на .env."
        )
    else:
        text = (
            "📄 <b>Таблица Google Sheets</b>\n\n"
            "Сейчас используется таблица из .env.\n"
            "Можно задать персональную ссылку."
        )
    await q.message.edit_text(text, reply_markup=_kb_sheet_manage())
    await q.answer()


@router.callback_query(F.data == "settings:sheet:set")
async def settings_sheet_set(q: CallbackQuery, state: FSMContext):
    await state.set_state(SheetLinkSetup.waiting_link)
    await q.message.answer(
        "Пришлите ссылку на таблицу, например:\n"
        "<code>https://docs.google.com/spreadsheets/d/.../edit?gid=0#gid=0</code>"
    )
    await q.answer()


@router.callback_query(F.data == "settings:sheet:clear")
async def settings_sheet_clear(q: CallbackQuery):
    clear_user_sheet_source(q.from_user.id)
    await q.message.edit_text(
        "Персональная таблица отключена. Используется таблица из .env.",
        reply_markup=_kb_settings(get_user(q.from_user.id)).as_markup(),
    )
    await q.answer("Сброшено")


@router.callback_query(F.data == "settings:myitmo:connect")
async def settings_myitmo_connect(q: CallbackQuery, state: FSMContext):
    await state.set_state(MyItmoSetup.waiting_login)
    await q.message.answer(
        "Введите логин my.itmo (ИСУ)."
    )
    await q.answer()


@router.message(MyItmoSetup.waiting_login)
async def settings_myitmo_login_msg(msg: Message, state: FSMContext):
    login = (msg.text or "").strip()
    if not login:
        await msg.answer("Логин пустой, попробуйте ещё раз.")
        return
    set_myitmo_login(msg.from_user.id, login)
    await state.set_state(MyItmoSetup.waiting_password)
    await msg.answer(
        "Введите пароль my.itmo.\n"
        "Пароль нужен только для получения token и не сохраняется в базе."
    )


@router.message(MyItmoSetup.waiting_password)
async def settings_myitmo_password_msg(msg: Message, state: FSMContext):
    pwd = (msg.text or "").strip()
    if not pwd:
        await msg.answer("Пароль пустой, попробуйте ещё раз.")
        return
    u = get_user(msg.from_user.id) or {}
    login = (u.get("myitmo_username") or "").strip()
    if not login:
        await state.clear()
        await msg.answer("Не найден логин. Сначала задайте логин в Настройки → my.itmo аккаунт.")
        return
    try:
        bundle = await asyncio.to_thread(
            exchange_password_for_tokens,
            username=login,
            password=pwd,
            timeout=settings.myitmo_timeout_sec,
        )
    except MyItmoError as e:
        await msg.answer(f"Не удалось подключить my.itmo: {e}")
        return
    except Exception:
        await msg.answer("Не удалось подключить my.itmo из-за сетевой ошибки. Попробуйте ещё раз.")
        return
    set_myitmo_tokens(
        msg.from_user.id,
        bundle["access_token"],
        bundle["refresh_token"],
        bundle["token_expiry"],
    )
    await state.clear()
    await msg.answer("my.itmo подключён ✅ Теперь можно включить режим my.itmo в Источнике расписания.")


@router.message(SheetLinkSetup.waiting_link)
async def settings_sheet_link_msg(msg: Message, state: FSMContext):
    parsed = _parse_sheet_link(msg.text or "")
    if not parsed:
        await msg.answer("Не удалось распознать ссылку. Проверьте формат и отправьте снова.")
        return
    spreadsheet_id, gid = parsed
    set_user_sheet_source(msg.from_user.id, spreadsheet_id, gid)
    await state.clear()
    await msg.answer(
        "Ссылка на таблицу сохранена ✅\n"
        f"SPREADSHEET_ID: <code>{spreadsheet_id}</code>\n"
        f"SHEET_GID: <code>{gid}</code>"
    )

@router.callback_query(F.data.startswith("settings:group:"))
async def settings_set_group(q: CallbackQuery):
    group = q.data.split(":", 2)[-1]
    set_group(q.from_user.id, group)
    u = get_user(q.from_user.id)
    await q.message.edit_text(
        f"Группа обновлена: <b>{group}</b>\n",
        reply_markup=_kb_settings(u).as_markup()
    )
    await q.answer()
