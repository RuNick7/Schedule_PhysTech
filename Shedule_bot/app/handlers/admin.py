from __future__ import annotations

import re
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.config import settings
from app.services.db import (
    list_user_ids_for_broadcast,
    get_bot_mode,
    set_bot_mode,
    get_bot_setting,
    set_bot_setting,
)
from app.bot import bot

router = Router()

_SHEETS_URL_RE = re.compile(
    r"spreadsheets/d/([A-Za-z0-9_-]+).*?(?:gid=(\d+))?(?:[&#]|$)"
)


class AdminBroadcast(StatesGroup):
    waiting_text    = State()
    waiting_confirm = State()


class AdminExamSheet(StatesGroup):
    waiting_url = State()


def _is_admin(user_id: int) -> bool:
    admin_id = settings.admin_telegram_id
    return bool(admin_id and int(user_id) == int(admin_id))


# ─── клавиатуры ───────────────────────────────────────────────────────────────

def _kb_admin_root():
    mode = get_bot_mode()
    mode_label = {"normal": "🟢 Обычный", "exams": "📋 Экзамены", "holidays": "🏖 Каникулы"}.get(mode, mode)
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Рассылка",               callback_data="admin:broadcast:start")
    kb.button(text=f"🔄 Режим: {mode_label}",   callback_data="admin:mode:menu")
    kb.button(text="📋 Настроить расписание экзаменов", callback_data="admin:exam:set")
    kb.button(text="📊 Статистика",              callback_data="admin:stats")
    kb.adjust(1)
    return kb.as_markup()


def _kb_mode_menu():
    current = get_bot_mode()
    kb = InlineKeyboardBuilder()
    for key, label in [("normal", "🟢 Обычный"), ("exams", "📋 Экзамены"), ("holidays", "🏖 Каникулы")]:
        text = f"✅ {label}" if current == key else label
        kb.button(text=text, callback_data=f"admin:mode:set:{key}")
    kb.button(text="◀️ Назад", callback_data="admin:root")
    kb.adjust(1)
    return kb.as_markup()


def _kb_broadcast_confirm():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Отправить", callback_data="admin:broadcast:send")
    kb.button(text="❌ Отмена",    callback_data="admin:broadcast:cancel")
    kb.adjust(1)
    return kb.as_markup()


def _kb_back():
    kb = InlineKeyboardBuilder()
    kb.button(text="◀️ Назад", callback_data="admin:root")
    kb.adjust(1)
    return kb.as_markup()


# ─── /admin ───────────────────────────────────────────────────────────────────

@router.message(Command("admin"))
async def admin_cmd(msg: Message):
    if not _is_admin(msg.from_user.id):
        await msg.answer("Нет доступа.")
        return
    if not settings.admin_telegram_id:
        await msg.answer("ADMIN_TELEGRAM_ID не задан в .env.")
        return
    await msg.answer("🔧 <b>Админ-панель</b>\nВыберите действие:", reply_markup=_kb_admin_root())


@router.callback_query(F.data == "admin:root")
async def admin_root(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await q.message.edit_text("🔧 <b>Админ-панель</b>\nВыберите действие:", reply_markup=_kb_admin_root())
    await q.answer()


# ─── Статистика ───────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:stats")
async def admin_stats(q: CallbackQuery):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    user_ids = list_user_ids_for_broadcast()
    mode = get_bot_mode()
    sid  = get_bot_setting("exam_spreadsheet_id") or "не задан"
    gid  = get_bot_setting("exam_sheet_gid") or "не задан"
    mode_label = {"normal": "🟢 Обычный", "exams": "📋 Экзамены", "holidays": "🏖 Каникулы"}.get(mode, mode)
    text = (
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{len(user_ids)}</b>\n"
        f"🔄 Режим: <b>{mode_label}</b>\n"
        f"📋 Spreadsheet ID: <code>{sid}</code>\n"
        f"📋 Sheet GID: <code>{gid}</code>"
    )
    await q.message.edit_text(text, reply_markup=_kb_back())
    await q.answer()


# ─── Режим бота ───────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:mode:menu")
async def admin_mode_menu(q: CallbackQuery):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    await q.message.edit_text("🔄 Выберите режим работы бота:", reply_markup=_kb_mode_menu())
    await q.answer()


@router.callback_query(F.data.startswith("admin:mode:set:"))
async def admin_mode_set(q: CallbackQuery):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    mode = q.data.split(":")[-1]
    try:
        set_bot_mode(mode)
    except ValueError as e:
        await q.answer(str(e), show_alert=True)
        return
    label = {"normal": "Обычный", "exams": "Экзамены", "holidays": "Каникулы"}.get(mode, mode)
    await q.answer(f"Режим изменён: {label}", show_alert=True)
    await q.message.edit_text("🔄 Выберите режим работы бота:", reply_markup=_kb_mode_menu())


# ─── Расписание экзаменов ─────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:exam:set")
async def admin_exam_set(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    sid = get_bot_setting("exam_spreadsheet_id") or "—"
    gid = get_bot_setting("exam_sheet_gid") or "—"
    await state.set_state(AdminExamSheet.waiting_url)
    await q.message.answer(
        f"📋 <b>Расписание экзаменов</b>\n\n"
        f"Текущий spreadsheet: <code>{sid}</code>\n"
        f"Текущий GID: <code>{gid}</code>\n\n"
        "Отправьте ссылку на Google Sheets с расписанием экзаменов.\n"
        "Формат: <code>https://docs.google.com/spreadsheets/d/ID/edit#gid=GID</code>\n\n"
        "Либо отправьте просто <code>ID:GID</code> (например, <code>1h4F0dD...:0</code>).\n\n"
        "Отправьте /cancel для отмены."
    )
    await q.answer()


@router.message(AdminExamSheet.waiting_url)
async def admin_exam_url(msg: Message, state: FSMContext):
    if not _is_admin(msg.from_user.id):
        await state.clear()
        return
    text = (msg.text or "").strip()
    if text.lower() in ("/cancel", "отмена"):
        await state.clear()
        await msg.answer("Отменено.", reply_markup=_kb_admin_root())
        return

    spreadsheet_id: str | None = None
    sheet_gid: int | None = None

    # Попытка разобрать URL Google Sheets
    m = _SHEETS_URL_RE.search(text)
    if m:
        spreadsheet_id = m.group(1)
        sheet_gid = int(m.group(2)) if m.group(2) else 0
    elif ":" in text:
        # Формат ID:GID
        parts = text.split(":", 1)
        spreadsheet_id = parts[0].strip()
        try:
            sheet_gid = int(parts[1].strip())
        except ValueError:
            pass
    else:
        await msg.answer(
            "Не удалось разобрать ссылку. Пришлите URL Google Sheets или <code>ID:GID</code>."
        )
        return

    if not spreadsheet_id or sheet_gid is None:
        await msg.answer("Не удалось разобрать ссылку. Попробуйте ещё раз.")
        return

    set_bot_setting("exam_spreadsheet_id", spreadsheet_id)
    set_bot_setting("exam_sheet_gid", str(sheet_gid))
    await state.clear()
    await msg.answer(
        f"✅ Расписание экзаменов обновлено!\n\n"
        f"Spreadsheet ID: <code>{spreadsheet_id}</code>\n"
        f"Sheet GID: <code>{sheet_gid}</code>",
        reply_markup=_kb_admin_root(),
    )


# ─── Рассылка ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:broadcast:start")
async def admin_broadcast_start(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminBroadcast.waiting_text)
    await q.message.answer("Введите текст рассылки одним сообщением.\nОтправьте /cancel для отмены.")
    await q.answer()


@router.message(AdminBroadcast.waiting_text)
async def admin_broadcast_text(msg: Message, state: FSMContext):
    if not _is_admin(msg.from_user.id):
        await state.clear()
        return
    if (msg.text or "").strip().lower() in ("/cancel", "отмена"):
        await state.clear()
        await msg.answer("Отменено.", reply_markup=_kb_admin_root())
        return
    text = (msg.html_text or msg.text or "").strip()
    if not text:
        await msg.answer("Сообщение пустое. Отправьте текст рассылки.")
        return
    await state.update_data(broadcast_text=text)
    await state.set_state(AdminBroadcast.waiting_confirm)
    user_count = len(list_user_ids_for_broadcast())
    await msg.answer(
        f"Подтвердите рассылку ({user_count} получателей):\n\n{text}",
        reply_markup=_kb_broadcast_confirm(),
    )


@router.callback_query(F.data == "admin:broadcast:cancel")
async def admin_broadcast_cancel(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await q.message.answer("Рассылка отменена.", reply_markup=_kb_admin_root())
    await q.answer()


@router.callback_query(F.data == "admin:broadcast:send")
async def admin_broadcast_send(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    data = await state.get_data()
    text = (data.get("broadcast_text") or "").strip()
    if not text:
        await state.clear()
        await q.message.answer("Текст рассылки не найден. Начните заново через /admin.")
        await q.answer()
        return

    user_ids = list_user_ids_for_broadcast()
    ok = 0
    fail = 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
            ok += 1
        except Exception:
            fail += 1

    await state.clear()
    await q.message.answer(
        "Рассылка завершена.\n"
        f"✅ Отправлено: <b>{ok}</b>\n"
        f"❌ Ошибок: <b>{fail}</b>",
        reply_markup=_kb_admin_root(),
    )
    await q.answer("Готово")
