from __future__ import annotations

import asyncio
import logging
from typing import Dict, List

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.services.db import get_user
from app.services.isu_client import (
    IsuSession,
    IsuSessionError,
    fetch_potok_schedule_html,
)
from app.services.isu_db import (
    get_cached_schedule,
    get_index_progress,
    save_schedule_html,
    search_groups,
    search_potoks_by_group,
    search_students_by_fio,
)
from app.services.isu_indexer import get_service_isu_session
from app.services.isu_schedule_parser import parse_schedule_html

log = logging.getLogger("isu.handler")

router = Router()

_ATTRIBUTION = (
    '\n\n<i>При поддержке <a href="https://github.com/Stunnerer/ITMOStalk">'
    "ITMOStalk</a> by Stunnerer</i>"
)

_DAY_ORDER = [
    "ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ",
    "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ",
]


class IsuSearch(StatesGroup):
    waiting_group_query = State()
    waiting_fio_query = State()


# ── keyboards ───────────────────────────────────────────────────────────

def _kb_isu_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="По группе", callback_data="isu:search:group")
    kb.button(text="По ФИО", callback_data="isu:search:fio")
    kb.button(text="Назад", callback_data="start:to_main")
    kb.adjust(2, 1)
    return kb.as_markup()


def _kb_back_to_isu():
    kb = InlineKeyboardBuilder()
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.button(text="Главное меню", callback_data="start:to_main")
    kb.adjust(1, 1)
    return kb.as_markup()


def _kb_connect_myitmo():
    kb = InlineKeyboardBuilder()
    kb.button(text="Подключить my.itmo", callback_data="settings:myitmo:open")
    kb.button(text="Назад", callback_data="start:to_main")
    kb.adjust(1, 1)
    return kb.as_markup()


def _kb_groups_list(groups: List[Dict], prefix: str = "isu:select:group"):
    kb = InlineKeyboardBuilder()
    for g in groups[:20]:
        enc = g["group_enc"]
        name = g["group_name"]
        kb.button(text=name, callback_data=f"{prefix}:{enc}")
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.adjust(3)
    return kb.as_markup()


def _kb_potoks_list(potoks: List[Dict]):
    kb = InlineKeyboardBuilder()
    for p in potoks[:20]:
        pid = p["potok_id"]
        name = _short_potok_name(p["potok_name"])
        kb.button(text=name, callback_data=f"isu:potok:{pid}")
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.adjust(2)
    return kb.as_markup()


def _kb_fio_results(students: List[Dict]):
    kb = InlineKeyboardBuilder()
    seen_groups: set = set()
    for s in students[:20]:
        group_enc = s.get("group_enc", "")
        group_name = s.get("group_name", "")
        if group_enc in seen_groups:
            continue
        seen_groups.add(group_enc)
        label = f"{s['student_name']} ({group_name})" if group_name else s["student_name"]
        if len(label) > 48:
            label = label[:45] + "..."
        kb.button(text=label, callback_data=f"isu:select:group:{group_enc}")
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.adjust(1)
    return kb.as_markup()


# ── entry point ─────────────────────────────────────────────────────────

@router.callback_query(F.data == "main:isu_schedule")
async def isu_schedule_entry(q: CallbackQuery, state: FSMContext):
    await state.clear()

    user = get_user(q.from_user.id)
    if not user or not user.get("myitmo_refresh_token"):
        await q.message.edit_text(
            "🔍 <b>Просмотр чужого расписания</b>\n\n"
            "Для этой функции нужно подключить аккаунт my.itmo.\n"
            "Перейдите в настройки и подключите my.itmo."
            + _ATTRIBUTION,
            reply_markup=_kb_connect_myitmo(),
            disable_web_page_preview=True,
        )
        await q.answer()
        return

    progress = get_index_progress()
    status_line = _format_index_status(progress)

    await q.message.edit_text(
        "🔍 <b>Просмотр чужого расписания</b>\n\n"
        "Вы можете найти расписание по названию группы или ФИО студента.\n\n"
        f"{status_line}"
        + _ATTRIBUTION,
        reply_markup=_kb_isu_main(),
        disable_web_page_preview=True,
    )
    await q.answer()


# ── search by group ─────────────────────────────────────────────────────

@router.callback_query(F.data == "isu:search:group")
async def isu_search_group_prompt(q: CallbackQuery, state: FSMContext):
    await state.set_state(IsuSearch.waiting_group_query)
    await q.message.answer("Введите название группы (или часть названия):")
    await q.answer()


@router.message(IsuSearch.waiting_group_query)
async def isu_search_group_handler(msg: Message, state: FSMContext):
    query = (msg.text or "").strip()
    if not query:
        await msg.answer("Запрос пустой. Введите название группы:")
        return
    await state.clear()

    groups = search_groups(query)
    if not groups:
        await msg.answer(
            f"Группы по запросу «{_esc(query)}» не найдены.\n"
            "Попробуйте другой запрос или дождитесь завершения индексации."
            + _ATTRIBUTION,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        return

    if len(groups) == 1:
        await _show_potoks_for_group(msg, groups[0])
        return

    await msg.answer(
        f"Найдено групп: <b>{len(groups)}</b>\nВыберите группу:",
        reply_markup=_kb_groups_list(groups),
    )


# ── search by FIO ───────────────────────────────────────────────────────

@router.callback_query(F.data == "isu:search:fio")
async def isu_search_fio_prompt(q: CallbackQuery, state: FSMContext):
    progress = get_index_progress()
    total = progress.get("groups_total", 0)
    indexed = progress.get("groups_indexed", 0)
    if total > 0 and indexed < total:
        await q.message.answer(
            f"Индексация студентов: {indexed}/{total} групп.\n"
            "Результаты поиска могут быть неполными.\n\n"
            "Введите ФИО (или часть — фамилию):"
        )
    else:
        await q.message.answer("Введите ФИО (или часть — фамилию):")
    await state.set_state(IsuSearch.waiting_fio_query)
    await q.answer()


@router.message(IsuSearch.waiting_fio_query)
async def isu_search_fio_handler(msg: Message, state: FSMContext):
    query = (msg.text or "").strip()
    if not query:
        await msg.answer("Запрос пустой. Введите ФИО:")
        return
    await state.clear()

    students = search_students_by_fio(query)
    if not students:
        await msg.answer(
            f"Студенты по запросу «{_esc(query)}» не найдены.\n"
            "Возможно, индексация ещё не завершена."
            + _ATTRIBUTION,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        return

    await msg.answer(
        f"Найдено: <b>{len(students)}</b>\nВыберите:",
        reply_markup=_kb_fio_results(students),
    )


# ── select group → show potoks ──────────────────────────────────────────

@router.callback_query(F.data.startswith("isu:select:group:"))
async def isu_select_group(q: CallbackQuery):
    group_enc = q.data.split(":", 3)[-1]

    from app.services.isu_db import _conn
    with _conn() as con:
        row = con.execute(
            "SELECT * FROM groups_ WHERE group_enc = ?", (group_enc,)
        ).fetchone()
        target = dict(row) if row else None

    if target is None:
        results = search_students_by_fio("")
        for s in results:
            if s.get("group_enc") == group_enc:
                target = {"group_enc": group_enc, "group_name": s.get("group_name", group_enc)}
                break

    if target is None:
        target = {"group_enc": group_enc, "group_name": group_enc}

    await _show_potoks_for_group_callback(q, target)


async def _show_potoks_for_group(msg: Message, group: Dict) -> None:
    group_name = group["group_name"]
    potoks = search_potoks_by_group(group_name)

    if not potoks:
        await msg.answer(
            f"Потоки для группы <b>{_esc(group_name)}</b> не найдены.\n"
            "Возможно, индексация потоков ещё не завершена."
            + _ATTRIBUTION,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        return

    if len(potoks) == 1:
        await _fetch_and_show_schedule_msg(msg, potoks[0]["potok_id"], potoks[0]["potok_name"])
        return

    await msg.answer(
        f"Группа <b>{_esc(group_name)}</b> — найдено потоков: <b>{len(potoks)}</b>\n"
        "Выберите поток для просмотра расписания:",
        reply_markup=_kb_potoks_list(potoks),
    )


async def _show_potoks_for_group_callback(q: CallbackQuery, group: Dict) -> None:
    group_name = group["group_name"]
    potoks = search_potoks_by_group(group_name)

    if not potoks:
        await q.message.edit_text(
            f"Потоки для группы <b>{_esc(group_name)}</b> не найдены.\n"
            "Возможно, индексация потоков ещё не завершена."
            + _ATTRIBUTION,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        await q.answer()
        return

    if len(potoks) == 1:
        await _fetch_and_show_schedule_cb(q, potoks[0]["potok_id"], potoks[0]["potok_name"])
        return

    await q.message.edit_text(
        f"Группа <b>{_esc(group_name)}</b> — найдено потоков: <b>{len(potoks)}</b>\n"
        "Выберите поток для просмотра расписания:",
        reply_markup=_kb_potoks_list(potoks),
    )
    await q.answer()


# ── select potok → show schedule ────────────────────────────────────────

@router.callback_query(F.data.startswith("isu:potok:"))
async def isu_select_potok(q: CallbackQuery):
    potok_id = int(q.data.split(":")[-1])
    from app.services.isu_db import _conn
    with _conn() as con:
        row = con.execute(
            "SELECT potok_name FROM potoks WHERE potok_id = ?", (potok_id,)
        ).fetchone()
    potok_name = row["potok_name"] if row else str(potok_id)
    await _fetch_and_show_schedule_cb(q, potok_id, potok_name)


# ── schedule fetching and formatting ────────────────────────────────────

async def _get_isu_session_for_user(_telegram_id: int) -> IsuSession:
    """
    Загрузка расписаний с ИСУ — только через сервисный аккаунт (ISU_INDEX_* в .env).
    Доступ к кнопке у пользователя всё равно только при подключённом my.itmo.
    """
    isu = await get_service_isu_session()
    if isu:
        return isu
    raise IsuSessionError(
        "Сервер не настроен: укажите ISU_INDEX_LOGIN и ISU_INDEX_PASSWORD в .env."
    )


async def _fetch_and_show_schedule_cb(
    q: CallbackQuery, potok_id: int, potok_name: str
) -> None:
    await q.answer("Загружаю расписание...")
    text = await _get_schedule_text(q.from_user.id, potok_id, potok_name)
    try:
        await q.message.edit_text(
            text, reply_markup=_kb_back_to_isu(), disable_web_page_preview=True
        )
    except Exception:
        await q.message.answer(
            text, reply_markup=_kb_back_to_isu(), disable_web_page_preview=True
        )


async def _fetch_and_show_schedule_msg(
    msg: Message, potok_id: int, potok_name: str
) -> None:
    text = await _get_schedule_text(msg.from_user.id, potok_id, potok_name)
    await msg.answer(
        text, reply_markup=_kb_back_to_isu(), disable_web_page_preview=True
    )


async def _get_schedule_text(telegram_id: int, potok_id: int, potok_name: str) -> str:
    html_content = get_cached_schedule(potok_id)
    if not html_content:
        try:
            isu = await _get_isu_session_for_user(telegram_id)
        except IsuSessionError as e:
            return f"{_esc(str(e))}" + _ATTRIBUTION

        try:
            html_content = await asyncio.to_thread(
                fetch_potok_schedule_html, isu, potok_id
            )
            save_schedule_html(potok_id, html_content)
        except IsuSessionError as e:
            return f"Ошибка ISU-сессии: {_esc(str(e))}" + _ATTRIBUTION
        except Exception as e:
            log.exception("Failed to fetch schedule for potok %d", potok_id)
            return f"Ошибка загрузки расписания: {_esc(str(e))}" + _ATTRIBUTION

    lessons = parse_schedule_html(html_content)
    if not lessons:
        return (
            f"Расписание для потока <b>{_esc(potok_name)}</b> не найдено или пусто."
            + _ATTRIBUTION
        )

    return _format_potok_schedule(potok_name, lessons)


def _format_potok_schedule(potok_name: str, lessons: List[Dict]) -> str:
    header = f"📆 <b>{_esc(potok_name)}</b>"
    sep = "—" * 40

    by_day: Dict[str, List[Dict]] = {}
    for les in lessons:
        d = les["day"]
        by_day.setdefault(d, []).append(les)

    lines = [header, sep]
    for day in _DAY_ORDER:
        day_lessons = by_day.get(day)
        if not day_lessons:
            continue
        lines.append(f"\n📌 <b>{day.title()}</b>")
        day_lessons.sort(key=lambda x: x.get("time", ""))
        for les in day_lessons:
            time_s = les.get("time", "")
            subj = les.get("subject", "")
            room = les.get("room", "")
            teacher = les.get("teacher", "")
            ltype = les.get("lesson_type", "")
            parity = les.get("parity", "")

            parts = [f"⏰ {_esc(time_s)}"]
            subj_line = f"📚 {_esc(subj)}"
            if ltype:
                subj_line += f" ({_esc(ltype)})"
            if parity:
                subj_line += f" [{_esc(parity)}]"
            parts.append(subj_line)
            if teacher:
                parts.append(f"👤 {_esc(teacher)}")
            if room:
                parts.append(f"📍 {_esc(room)}")

            lines.append("\n".join(parts))
            lines.append(sep)

    result = "\n".join(lines) + _ATTRIBUTION

    if len(result) > 4000:
        result = result[:3950] + "\n...(обрезано)" + _ATTRIBUTION

    return result


# ── helpers ─────────────────────────────────────────────────────────────

def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _short_potok_name(name: str) -> str:
    s = (name or "").strip()
    if len(s) > 40:
        return s[:37] + "..."
    return s


def _format_index_status(progress: Dict) -> str:
    status = progress.get("indexer_status", "idle")
    g_total = progress.get("groups_total", 0)
    g_indexed = progress.get("groups_indexed", 0)
    p_total = progress.get("potoks_total", 0)

    if status == "waiting_credentials":
        return (
            "Индексатор ждёт учётные данные: задайте ISU_INDEX_LOGIN и "
            "ISU_INDEX_PASSWORD в .env на сервере.\n"
        )

    if status == "idle" and g_total == 0:
        return "Индексация ещё не запускалась.\n"

    if status in ("authenticating", "fetching_groups", "fetching_potoks", "indexing_students"):
        pct = (g_indexed / g_total * 100) if g_total else 0
        return (
            f"Индексация: {status}\n"
            f"Группы: {g_total}, потоки: {p_total}\n"
            f"Студенты проиндексированы: {g_indexed}/{g_total} ({pct:.0f}%)\n"
        )

    if status == "error":
        err = progress.get("last_error", "")
        return f"Индексатор: ошибка ({_esc(err[:100])})\n"

    pct = (g_indexed / g_total * 100) if g_total else 0
    return (
        f"Группы: {g_total}, потоки: {p_total}\n"
        f"Студенты: {g_indexed}/{g_total} групп ({pct:.0f}%)\n"
    )
