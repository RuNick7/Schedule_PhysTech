from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.config import settings
from app.services.db import list_user_ids_for_broadcast
from app.bot import bot

router = Router()


class AdminBroadcast(StatesGroup):
    waiting_text = State()
    waiting_confirm = State()


def _is_admin(user_id: int) -> bool:
    admin_id = settings.admin_telegram_id
    return bool(admin_id and int(user_id) == int(admin_id))


def _kb_admin_root():
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Рассылка", callback_data="admin:broadcast:start")
    kb.adjust(1)
    return kb.as_markup()


def _kb_broadcast_confirm():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Отправить", callback_data="admin:broadcast:send")
    kb.button(text="❌ Отмена", callback_data="admin:broadcast:cancel")
    kb.adjust(1, 1)
    return kb.as_markup()


@router.message(Command("admin"))
async def admin_cmd(msg: Message):
    if not _is_admin(msg.from_user.id):
        await msg.answer("Нет доступа.")
        return
    if not settings.admin_telegram_id:
        await msg.answer("ADMIN_TELEGRAM_ID не задан в .env.")
        return
    await msg.answer("🔧 <b>Админ-панель</b>\nВыберите действие:", reply_markup=_kb_admin_root())


@router.callback_query(F.data == "admin:broadcast:start")
async def admin_broadcast_start(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminBroadcast.waiting_text)
    await q.message.answer("Введите текст рассылки одним сообщением.")
    await q.answer()


@router.message(AdminBroadcast.waiting_text)
async def admin_broadcast_text(msg: Message, state: FSMContext):
    if not _is_admin(msg.from_user.id):
        await state.clear()
        await msg.answer("Нет доступа.")
        return
    text = (msg.html_text or msg.text or "").strip()
    if not text:
        await msg.answer("Сообщение пустое. Отправьте текст рассылки.")
        return
    await state.update_data(broadcast_text=text)
    await state.set_state(AdminBroadcast.waiting_confirm)
    await msg.answer(
        "Подтвердите рассылку:\n\n"
        f"{text}",
        reply_markup=_kb_broadcast_confirm(),
    )


@router.callback_query(F.data == "admin:broadcast:cancel")
async def admin_broadcast_cancel(q: CallbackQuery, state: FSMContext):
    if not _is_admin(q.from_user.id):
        await q.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await q.message.answer("Рассылка отменена.")
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
        f"❌ Ошибок: <b>{fail}</b>"
    )
    await q.answer("Готово")
