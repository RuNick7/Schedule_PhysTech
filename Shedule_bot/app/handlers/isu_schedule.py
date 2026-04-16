from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, List

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
    get_cached_schedule_entries,
    get_group_by_enc,
    get_index_progress,
    get_potok_name,
    get_potoks_by_student,
    get_student_by_id,
    save_schedule_html,
    save_schedule_entries,
    search_groups,
    search_potoks_by_group,
    search_students_by_fio,
)
from app.services.isu_indexer import get_service_isu_session
from app.services.isu_schedule_parser import parse_schedule_html
from app.utils.dt import now_tz
from app.utils.format_schedule import format_day, format_week_compact_mono
from app.utils.week_parity import week_parity_for_date

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
_DAY_UP = _DAY_ORDER


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
        kb.button(text=name, callback_data=f"isu:potok:{pid}:all")
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.adjust(2)
    return kb.as_markup()


def _kb_fio_results(students: List[Dict]):
    kb = InlineKeyboardBuilder()
    for s in students[:20]:
        group_name = s.get("group_name", "")
        sid = s.get("student_id")
        if not sid:
            continue
        label = f"{s['student_name']} ({group_name})" if group_name else s["student_name"]
        if len(label) > 48:
            label = label[:45] + "..."
        kb.button(text=label, callback_data=f"isu:select:student:{sid}")
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.adjust(1)
    return kb.as_markup()


def _kb_schedule_periods(kind: str, entity_id: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="Сегодня", callback_data=f"isu:view:{kind}:{entity_id}:today")
    kb.button(text="Завтра", callback_data=f"isu:view:{kind}:{entity_id}:tomorrow")
    kb.button(text="Неделя", callback_data=f"isu:view:{kind}:{entity_id}:week")
    kb.button(text="Всё", callback_data=f"isu:view:{kind}:{entity_id}:all")
    kb.button(text="Новый поиск", callback_data="main:isu_schedule")
    kb.adjust(2, 2, 1)
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
            ,
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
            ,
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
    target = get_group_by_enc(group_enc) or {"group_enc": group_enc, "group_name": group_enc}
    await _show_group_actions(q, target)


async def _show_potoks_for_group(msg: Message, group: Dict) -> None:
    group_enc = group["group_enc"]
    group_name = group["group_name"]
    potoks = search_potoks_by_group(group_enc)
    if not potoks:
        await msg.answer(
            f"Для группы <b>{_esc(group_name)}</b> не найдены связанные потоки.\n"
            "Скорее всего, индексация потоков ещё не завершена."
            ,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        return
    await msg.answer(
        f"Группа <b>{_esc(group_name)}</b>\n"
        f"Найдено потоков: <b>{len(potoks)}</b>\n"
        "Можно сразу посмотреть агрегированное расписание группы или открыть конкретный поток.",
        reply_markup=_kb_schedule_periods("group", group_enc),
        disable_web_page_preview=True,
    )


async def _show_group_actions(q: CallbackQuery, group: Dict) -> None:
    group_enc = group["group_enc"]
    group_name = group["group_name"]
    potoks = search_potoks_by_group(group_enc)
    if not potoks:
        await q.message.edit_text(
            f"Для группы <b>{_esc(group_name)}</b> не найдены связанные потоки.\n"
            "Скорее всего, индексация потоков ещё не завершена."
            ,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        await q.answer()
        return

    await q.message.edit_text(
        f"Группа <b>{_esc(group_name)}</b>\n"
        f"Найдено потоков: <b>{len(potoks)}</b>\n"
        "Можно сразу посмотреть агрегированное расписание группы или открыть конкретный поток.",
        reply_markup=_kb_schedule_periods("group", group_enc),
        disable_web_page_preview=True,
    )
    await q.answer()


# ── select student / potok / period ─────────────────────────────────────

@router.callback_query(F.data.startswith("isu:select:student:"))
async def isu_select_student(q: CallbackQuery):
    student_id = int(q.data.split(":")[-1])
    potoks = get_potoks_by_student(student_id)
    if not potoks:
        await q.message.edit_text(
            "Для этого студента пока не найдены потоки.\n"
            "Скорее всего, индексация потоков ещё не завершена."
            ,
            reply_markup=_kb_back_to_isu(),
            disable_web_page_preview=True,
        )
        await q.answer()
        return

    student = get_student_by_id(student_id)
    student_label = student["student_name"] if student else str(student_id)
    if student and student.get("group_name"):
        student_label = f"{student_label} ({student['group_name']})"

    await q.message.edit_text(
        f"Студент <b>{_esc(student_label)}</b>\n"
        f"Найдено потоков: <b>{len(potoks)}</b>\n"
        "Выберите период для общего расписания по всем потокам.",
        reply_markup=_kb_schedule_periods("student", str(student_id)),
        disable_web_page_preview=True,
    )
    await q.answer()


@router.callback_query(F.data.startswith("isu:potok:"))
async def isu_select_potok(q: CallbackQuery):
    _, _, pid_s, period = q.data.split(":", 3)
    await _show_period_schedule(q, "potok", pid_s, period)


@router.callback_query(F.data.startswith("isu:view:"))
async def isu_view_schedule(q: CallbackQuery):
    _, _, kind, entity_id, period = q.data.split(":", 4)
    await _show_period_schedule(q, kind, entity_id, period)


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


async def _show_period_schedule(
    q: CallbackQuery, kind: str, entity_id: str, period: str
) -> None:
    await q.answer("Загружаю расписание...")
    text = await _render_schedule_text(q.from_user.id, kind, entity_id, period)
    reply = _kb_schedule_periods(kind, entity_id)
    try:
        await q.message.edit_text(
            text, reply_markup=reply, disable_web_page_preview=True
        )
    except Exception:
        await q.message.answer(
            text, reply_markup=reply, disable_web_page_preview=True
        )


async def _render_schedule_text(
    telegram_id: int, kind: str, entity_id: str, period: str
) -> str:
    lessons, label = await _resolve_lessons_for_entity(telegram_id, kind, entity_id)
    if not lessons:
        return f"Расписание для <b>{_esc(label)}</b> не найдено или пока пусто."

    period = (period or "all").lower()
    if period == "all":
        return _format_all_lessons(label, lessons)

    tz = None
    try:
        from app.services.db import get_user as _get_user
        user = _get_user(telegram_id) or {}
        tz = user.get("timezone")
    except Exception:
        tz = None
    now = now_tz(tz)

    if period in {"today", "tomorrow"}:
        target = now + timedelta(days=1 if period == "tomorrow" else 0)
        parity = week_parity_for_date(target, tz)
        day_upper = _DAY_UP[target.weekday()]
        day_lessons = _filter_isu_day_lessons(lessons, day_upper, parity)
        return _adapt_day_format(label, day_upper, parity, day_lessons)

    parity = week_parity_for_date(None, tz)
    week_lessons = _filter_isu_week_lessons(lessons, parity)
    return _adapt_week_format(label, parity, week_lessons)


async def _resolve_lessons_for_entity(
    telegram_id: int, kind: str, entity_id: str
) -> tuple[List[Dict[str, Any]], str]:
    if kind == "potok":
        potok_id = int(entity_id)
        potok_name = get_potok_name(potok_id) or str(potok_id)
        lessons = await _get_potok_lessons(telegram_id, potok_id)
        return lessons, potok_name

    if kind == "student":
        student_id = int(entity_id)
        student = get_student_by_id(student_id)
        label = student["student_name"] if student else str(student_id)
        potoks = get_potoks_by_student(student_id)
        lessons = await _get_many_potok_lessons(
            telegram_id, potoks, include_source=len(potoks) > 1
        )
        return lessons, label

    if kind == "group":
        group = get_group_by_enc(entity_id) or {"group_name": entity_id}
        potoks = search_potoks_by_group(entity_id)
        lessons = await _get_many_potok_lessons(
            telegram_id, potoks, include_source=len(potoks) > 1
        )
        return lessons, group["group_name"]

    return [], entity_id


async def _get_potok_lessons(telegram_id: int, potok_id: int) -> List[Dict[str, Any]]:
    cached = get_cached_schedule_entries(potok_id)
    if cached:
        return cached

    html_content = get_cached_schedule(potok_id)
    if not html_content:
        try:
            isu = await _get_isu_session_for_user(telegram_id)
        except IsuSessionError as e:
            log.warning("ISU schedule fetch unavailable for potok %d: %s", potok_id, e)
            return []
        try:
            html_content = await asyncio.to_thread(fetch_potok_schedule_html, isu, potok_id)
            save_schedule_html(potok_id, html_content)
        except Exception:
            log.exception("Failed to fetch schedule for potok %d", potok_id)
            return []

    lessons = parse_schedule_html(html_content)
    save_schedule_entries(potok_id, lessons)
    return lessons


async def _get_many_potok_lessons(
    telegram_id: int,
    potoks: List[Dict[str, Any]],
    include_source: bool = False,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for potok in potoks:
        pid = int(potok["potok_id"])
        pname = potok.get("potok_name") or str(pid)
        lessons = await _get_potok_lessons(telegram_id, pid)
        for item in lessons:
            clone = dict(item)
            clone["potok_name"] = pname
            if include_source:
                subj = clone.get("subject") or ""
                clone["subject"] = f"{subj} [{pname}]"
            out.append(clone)
    return out


def _adapt_day_format(label: str, day_upper: str, parity: str, lessons: List[Dict[str, Any]]) -> str:
    formatted = format_day(label, day_upper, parity, [_to_common_lesson(it) for it in lessons])
    return _retitle_schedule_header(formatted)


def _adapt_week_format(label: str, parity: str, lessons: List[Dict[str, Any]]) -> str:
    formatted = format_week_compact_mono(label, parity, [_to_common_lesson(it) for it in lessons])
    return _retitle_schedule_header(formatted)


def _filter_isu_day_lessons(
    lessons: List[Dict[str, Any]], day_upper: str, parity: str
) -> List[Dict[str, Any]]:
    np = _norm_parity(parity)
    return [
        it for it in lessons
        if str(it.get("day") or "").strip().upper() == day_upper
        and _isu_lesson_matches_parity(it, np)
    ]


def _filter_isu_week_lessons(
    lessons: List[Dict[str, Any]], parity: str
) -> List[Dict[str, Any]]:
    np = _norm_parity(parity)
    return [it for it in lessons if _isu_lesson_matches_parity(it, np)]


def _isu_lesson_matches_parity(lesson: Dict[str, Any], parity: str) -> bool:
    lp = _norm_parity(lesson.get("parity") or "")
    if not lp:
        return True
    return lp == parity


def _to_common_lesson(lesson: Dict[str, Any]) -> Dict[str, str]:
    subject = str(lesson.get("subject") or "").strip()
    lesson_type = str(lesson.get("lesson_type") or "").strip()
    teacher = str(lesson.get("teacher") or "").strip()
    room = str(lesson.get("room") or "").strip()

    lecture = subject
    if lesson_type:
        lecture = f"{lecture} ({lesson_type})"
    if teacher:
        lecture = f"{lecture} {teacher}"

    text = lecture
    if room:
        text = f"{lecture} — {room}"

    parity = _norm_parity(lesson.get("parity") or "")
    return {
        "day": str(lesson.get("day") or "").strip().upper(),
        "time": str(lesson.get("time") or "").strip(),
        "text": text,
        "parity": parity,
        "special": False,
        "room_is_zoom": False,
        "room_link": "",
    }


def _format_all_lessons(label: str, lessons: List[Dict[str, Any]]) -> str:
    header = f"📆 <b>{_esc(label)}</b>"
    sep = "—" * 40
    by_day: Dict[str, List[Dict[str, Any]]] = {}
    for les in lessons:
        by_day.setdefault(str(les.get("day") or "").strip().upper(), []).append(les)

    lines = [header, sep]
    for day in _DAY_ORDER:
        day_lessons = by_day.get(day)
        if not day_lessons:
            continue
        lines.append(f"\n📌 <b>{day.title()}</b>")
        for les in sorted(day_lessons, key=lambda x: x.get("time", "")):
            parts = [f"⏰ {_esc(str(les.get('time') or ''))}"]
            subj = str(les.get("subject") or "")
            if les.get("lesson_type"):
                subj += f" ({les['lesson_type']})"
            if les.get("parity"):
                subj += f" [{str(les.get('parity'))}]"
            parts.append(f"📚 {_esc(subj)}")
            if les.get("teacher"):
                parts.append(f"👤 {_esc(str(les.get('teacher')))}")
            if les.get("room"):
                parts.append(f"📍 {_esc(str(les.get('room')))}")
            lines.append("\n".join(parts))
            lines.append(sep)

    result = "\n".join(lines).rstrip(sep + "\n")
    if len(result) > 4000:
        result = result[:3950] + "\n...(обрезано)"
    return result


def _retitle_schedule_header(text: str) -> str:
    lines = text.splitlines()
    if lines:
        lines[0] = lines[0].replace("Группа ", "", 1)
    return "\n".join(lines)


# ── helpers ─────────────────────────────────────────────────────────────

def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _norm_parity(value: Any) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    if "неч" in text:
        return "нечёт"
    if "чет" in text:
        return "чёт"
    return text


def _short_potok_name(name: str) -> str:
    s = (name or "").strip()
    if len(s) > 40:
        return s[:37] + "..."
    return s


_STATUS_LABELS = {
    "authenticating": "вход в ИСУ…",
    "fetching_groups": "загрузка списка групп…",
    "fetching_potoks": "загрузка списка потоков…",
    "indexing_potoks": "индексация участников потоков…",
    "indexing_students": "индексация студентов по группам…",
    "idle": "готово",
    "waiting_credentials": "ожидание настроек",
    "error": "ошибка",
}


def _format_index_status(progress: Dict) -> str:
    status = progress.get("indexer_status", "idle")
    g_total = progress.get("groups_total", 0)
    g_indexed = progress.get("groups_indexed", 0)
    p_total = progress.get("potoks_total", 0)
    p_indexed = progress.get("potoks_indexed", 0)
    label = _STATUS_LABELS.get(status, status)

    if status == "waiting_credentials":
        return (
            "Индексатор ждёт учётные данные: задайте ISU_INDEX_LOGIN и "
            "ISU_INDEX_PASSWORD в .env на сервере.\n"
        )

    if status == "idle" and g_total == 0:
        return "Индексация ещё не запускалась.\n"

    # Пока не загружен список групп, цифры 0/0 не показываем — это не «прогресс»
    if status in ("authenticating", "fetching_groups", "fetching_potoks", "indexing_potoks"):
        if g_total == 0:
            return (
                f"Индексатор: <b>{label}</b>\n"
                "Счётчики появятся после загрузки списков с ИСУ.\n"
            )
    if status == "indexing_students" and g_total == 0:
        return (
            f"Индексатор: <b>{label}</b>\n"
            "Ожидается список групп…\n"
        )

    if status in ("authenticating", "fetching_groups", "fetching_potoks", "indexing_potoks", "indexing_students"):
        pct = (g_indexed / g_total * 100) if g_total else 0
        potok_pct = (p_indexed / p_total * 100) if p_total else 0
        return (
            f"Индексатор: <b>{label}</b>\n"
            f"Групп в списке: {g_total}, потоков: {p_total}\n"
            f"Потоки: {p_indexed}/{p_total} ({potok_pct:.0f}%)\n"
            f"Студенты: {g_indexed}/{g_total} групп ({pct:.0f}%)\n"
        )

    if status == "error":
        err = progress.get("last_error", "")
        return f"Индексатор: ошибка ({_esc(err[:100])})\n"

    pct = (g_indexed / g_total * 100) if g_total else 0
    return (
        f"Групп в списке: {g_total}, потоков: {p_total}\n"
        f"Студенты: {g_indexed}/{g_total} групп ({pct:.0f}%)\n"
    )
