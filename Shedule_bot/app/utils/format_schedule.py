# app/utils/format_schedule.py
from __future__ import annotations

from typing import List, Dict, Tuple
import re
import random
import os
try:
    # если есть «умный» парсер
    from app.utils.teacher_parser import extract_teachers_smart as _extract_teachers
except Exception:
    # fallback на базовый
    from app.utils.teacher_parser import extract_teachers as _extract_teachers


DAY_ORDER = ["ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ", "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ"]

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
    "Кофе, плейлист, @kairavpn_bot — и идеальный день готов ☕️",
    "Никаких пар, только релакс и безопасный интернет с @kairavpn_bot 🌈",
    "Пора обновить списки смотреть-позже? Вперёд с @kairavpn_bot 📺",
    "Учёба подождёт — контент не ждёт. В путь с @kairavpn_bot 🧭",
    "Готовься к сессии умно: курсы и лекции без блоков с @kairavpn_bot 🎓",
    "Свободное окно превратилось в свободный день — спасибо @kairavpn_bot ⏳",
    "Проверяй мемы из любых углов интернета с @kairavpn_bot 😎",
    "Сегодня можно навести порядок в закладках — и в приватности с @kairavpn_bot 🧹",
    "Хочешь стримы без «этот контент недоступен»? Подключай @kairavpn_bot 🛰",
    "Работай из кофейни спокойно — шифрует @kairavpn_bot ☕️🔒",
    "Виртуальное путешествие по кампусам мира? Легко с @kairavpn_bot ✈️",
    "Никаких дедлайнов сегодня — только безопасный серф с @kairavpn_bot 🏄",
    "Запланируй марафон лекций и фильмов — выручит @kairavpn_bot 🗂",
    "Общажный роутер снова чудит? @kairavpn_bot наведёт порядок 🛠",
    "Нулевой день по расписанию — но не по впечатлениям. Врубай @kairavpn_bot ✨",
    "Качай, смотри, слушай — без границ и лишних глаз с @kairavpn_bot 👀",
    "Хватит откладывать. С @kairavpn_bot твой список «посмотреть позже» стал «сейчас» ✅",
    "Сегодня идеальный день, чтобы настроить @kairavpn_bot один раз и забыть ⚙️",
]

LONG_SEP = "-" * 50
WEEK_SEP = "-" * 50

_TIME_RE = re.compile(r"\s*(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})\s*")

# ===== helpers =====
def _format_empty_day(group_code: str, day_upper: str, parity: str) -> str:
    parity_label = "Чётная неделя" if str(parity).lower().startswith("ч") else "Нечётная неделя"
    header = f"📅 Группа {group_code} • {parity_label} • {day_upper}"
    sep = "—" * 50
    return f"{header}\n{sep}\n{_pick_no_lessons_message()}"

def _pick_no_lessons_message() -> str:
    """
    Берём случайную фразу из ENV NO_LESSONS_MESSAGES (через |),
    иначе из дефолтного списка выше.
    Пример ENV: NO_LESSONS_MESSAGES="— Пар не найдено —|Свободно!|День без пар 🎉"
    """
    raw = os.getenv("NO_LESSONS_MESSAGES", "")
    if raw.strip():
        choices = [s.strip() for s in raw.split("|") if s.strip()]
        if choices:
            return random.choice(choices)
    return random.choice(_NO_LESSONS_DEFAULT)

def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _html_escape_attr(s: str) -> str:
    """Эскейп для значений в атрибутах HTML (href)."""
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def _safe_anchor(url: str, label: str = "Zoom") -> str:
    """Возвращает безопасную ссылку <a href="...">label</a> только для http/https."""
    u = (url or "").strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return _html_escape(label)
    return f'<a href="{_html_escape_attr(u)}">{_html_escape(label)}</a>'

def _line_with_room_html(time_s: str, subject_s: str, room_html: str) -> str:
    """
    Строка «время — предмет — аудитория», где аудитория может содержать HTML (ссылку).
    Время и предмет эскейпятся, аудитория — уже готовый HTML.
    """
    time_txt = (time_s or "").strip()
    subj_txt = _ellipsize(_abbrev_subject(_one_line(subject_s or "")), SUBJECT_MAX)
    parts = [_html_escape(time_txt), _html_escape(subj_txt)]
    if room_html:
        parts.append(room_html)
    return " — ".join(parts)

def _parse_time_key(time_range: str) -> Tuple[int, int]:
    m = _TIME_RE.match(str(time_range))
    if not m:
        return (9999, 9999)
    h1, m1, h2, m2 = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
    return (h1 * 60 + m1, h2 * 60 + m2)

def _ellipsize(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    if max_len <= 1:
        return s[:max_len]
    return s[: max_len - 1] + "…"

def _split_lecture_room(text: str) -> tuple[str, str]:
    parts = text.split(" — ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return text.strip(), ""

def _one_line(s: str) -> str:
    """Убираем переносы строк и лишние пробелы, чтобы не ломало строки в неделе."""
    s = (s or "").replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().replace("ё", "е")).strip()

def _is_history_subject(subj: str) -> bool:
    return "истор" in _norm(subj)

def _dedupe_teachers_prefer_rich(teachers: list[str]) -> list[str]:
    """
    Склеиваем дубликаты преподавателей:
    - группируем по (фамилия, первая инициала) — чтобы 'Иванов А.А.' и 'Иванов Б.Б.' не схлопнулись,
    - если вариантов несколько, берём тот, где БОЛЬШЕ инициалов (3 > 2 > 1 > 0),
      при равенстве — более длинную строку.
    """
    if not teachers:
        return []

    RUS_U = "А-ЯЁ"; RUS_L = "а-яё"
    SURNAME = rf"[{RUS_U}][{RUS_L}]+(?:-[{RUS_U}][{RUS_L}]+)?"
    # «Фамилия», «Фамилия И», «Фамилия И.О.», «Фамилия И.О.Б.»
    pat = re.compile(rf"^\s*({SURNAME})\s*(?:([{RUS_U}])(?:\.|\s*)?([{RUS_U}])?(?:\.|\s*)?([{RUS_U}])?(?:\.|\s*)?)?\s*$")

    def parse(token: str):
        m = pat.match(token or "")
        if not m:
            # не распарсили — считаем уникальным «как есть»
            return (token.strip().lower(), ""), 0, token.strip()
        surname = m.group(1)
        inits = [x for x in (m.group(2), m.group(3), m.group(4)) if x]
        key = (surname.lower(), (inits[0] if inits else ""))
        richness = len(inits)              # чем больше инициалов — тем «лучше»
        # нормализованный вывод (Фамилия И.О.)
        if not inits:
            display = surname
        else:
            display = surname + " " + ".".join(ch.upper() for ch in inits) + "."
        return key, richness, display

    order: list[tuple[str, str]] = []      # порядок первых появлений ключей
    best: dict[tuple[str, str], tuple[int, str]] = {}  # key -> (richness, display)
    for t in teachers:
        key, rich, disp = parse(t)
        if key not in best:
            order.append(key)
            best[key] = (rich, disp)
        else:
            cur_rich, cur_disp = best[key]
            if (rich > cur_rich) or (rich == cur_rich and len(disp) > len(cur_disp)):
                best[key] = (rich, disp)

    # Если по одной фамилии есть вариант с инициалами,
    # голую фамилию убираем (иначе получаем "Иванов И.И., Иванов").
    has_detailed_surname = {k[0] for k in best if k[1]}
    filtered_order = [k for k in order if not (k[1] == "" and k[0] in has_detailed_surname)]
    return [best[k][1] for k in filtered_order]

def _strip_teachers(text: str) -> str:
    """
    Очищаем предмет от ФИО, оставляя только название.
    Удаляем:
      • «Фамилия И.О.» / «И.О. Фамилия» (smart/base парсер),
      • «Фамилия И.» (одна инициала),
      • «лек./практ./лаб./сем. Фамилия» в хвосте.
    """
    s = text or ""

    # 1) убираем то, что распознал наш парсер (_extract_teachers)
    for t in _extract_teachers(s):
        patt = re.escape(t).replace(r"\.", r"\.?")
        s = re.sub(rf"(?<!\w){patt}(?!\w)", " ", s)
        if " " in t:
            surname, initials = t.split()
            rev = f"{initials} {surname}"
            patt2 = re.escape(rev).replace(r"\.", r"\.?")
            s = re.sub(rf"(?<!\w){patt2}(?!\w)", " ", s)

    # 2) фамилия + одна инициала (например, «Бадриева З.» / «Салтыкова Д»)
    RUS_U = "А-ЯЁ"; RUS_L = "а-яё"
    SURNAME_RE = rf"[{RUS_U}][{RUS_L}]+(?:-[{RUS_U}][{RUS_L}]+)?"
    PAT_SURNAME_INIT1 = re.compile(rf"\b{SURNAME_RE}\s+([{RUS_U}])\.?\b")
    s = PAT_SURNAME_INIT1.sub(" ", s)

    # 3) роль + фамилия в конце («лек./практ./лаб./сем. Фамилия»)
    ROLE_BEFORE_SURNAME = re.compile(
        rf"(?:\b(?:лек\.?|практ\.?|лаб\.?|сем\.?|преп\.?)\b[ ,.:;-]*)({SURNAME_RE})(?=$|\s|[,.;:])",
        re.I,
    )
    s = ROLE_BEFORE_SURNAME.sub("", s)

    # 4) подчистка
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"\s*[.,;:]\s*$", "", s)
    return s.strip(" ,;/|")



# --- сокращения названий, чтобы влезало в строку ---
_ABBR = [
    # предметы/направления
    (re.compile(r"\bматематическ\w*\s+анализ\b", re.I), "Мат. анализ"),
    (re.compile(r"\bлинейная\s+алгебра\b", re.I), "Лин. алгебра"),
    (re.compile(r"\bосновы\s+аналитическ\w*\s+вычисл\w*\b", re.I), "Осн. аналит. вычисл."),
    (re.compile(r"\bанглийск\w*(?:\s+язык)?\b", re.I), "Англ. язык"),

    # СНАЧАЛА нормализуем 'лабораторная' -> 'лаб.' (универсально)
    (re.compile(r"\bлабораторн\w*\b", re.I), "лаб."),

    # Затем — частные случаи ФИЗИКИ:
    # 'Физика лабораторная' или 'Физика лаб.' -> 'Физика лаб.'
    (re.compile(r"\bфизика\s+лабораторн\w*\b", re.I), "Физика лаб."),
    (re.compile(r"\bфизика\s+лаб\.\b", re.I), "Физика лаб."),
    # И только после — голая 'Физика' -> 'Физика'
    (re.compile(r"\bфизика\b", re.I), "Физика"),

    # Тип занятия в короткую форму (если встречается отдельно)
    (re.compile(r"\bлекци(я|и|он\w*)\b", re.I), "лек."),
    (re.compile(r"\bпрактик\w*\b", re.I), "практ."),
    (re.compile(r"\bсеминар\w*\b", re.I), "сем."),
]

def _abbrev_subject(s: str) -> str:
    t = s
    for rx, repl in _ABBR:
        t = rx.sub(repl, t)
    return t

# ===== DAY (оставляем как было — многострочным, но без переносов внутри предмета) =====

def format_day(group: str, day: str, parity: str, lessons: List[Dict[str, str]]) -> str:
    """
    Дневной вид (Сегодня/Завтра):
      ⏰ HH:MM–HH:MM
      📚 Предмет — Преп.: Фамилия..., Фамилия...    (одна строка)
      📍 Аудитория   (или ⚠️ для спец-пар)
    Между парами — LONG_SEP. Повторы «Истории» в один день скрываем.
    """
    header = (
        f"📅 <b>Группа { _html_escape(group) }</b> • "
        f"{('Чётная' if parity=='чёт' else 'Нечётная')} неделя • {_html_escape(day.title())}"
    )
    if not lessons:
        return f"{header}\n{LONG_SEP}\n{_pick_no_lessons_message()}"

    blocks: List[str] = [header, LONG_SEP]
    history_emitted = False

    for it in sorted(lessons, key=lambda x: _parse_time_key(x['time'])):
        lecture_raw, room_raw = _split_lecture_room(it['text'])

        # предмет — чистим от ФИО, склеиваем в одну строку и сокращаем
        subj = _abbrev_subject(_one_line(_strip_teachers(lecture_raw)))

        # «Историю» показываем один раз в день (берём первую по времени)
        if it.get("special") and _is_history_subject(subj):
            if history_emitted:
                continue
            history_emitted = True

        # строки блока
        lines = [f"⏰ <b>{_html_escape(it['time'])}</b>"]

        # предмет + преподы в одной строке
        teachers_raw = _extract_teachers(lecture_raw)
        teachers = _dedupe_teachers_prefer_rich(teachers_raw)

        subj_line = f"📚 {_html_escape(subj)}"
        if teachers:
            # без слова «Преп.» — фамилии сразу после предмета
            subj_line += f" — {_html_escape(', '.join(teachers))}"
        lines.append(subj_line)

        # аудитория / предупреждение
        if it.get("special"):
            lines.append("⚠️ <i>Проверьте детали в приложении</i>")
        elif room_raw:
            lines.append(f"📍 {_html_escape(_one_line(room_raw))}")

        if it.get("room_is_zoom") and it.get("room_link"):
            lines.append(f"🔗 Zoom: {_html_escape(it['room_link'])}")

        blocks.append("\n".join(lines))
        blocks.append(LONG_SEP)

    return "\n".join(blocks).rstrip()

# ===== WEEK: одна строка на пару (время — предмет — аудитория), всё короче и без переносов =====
SUBJECT_MAX = 28  # ← лимит названия предмета
ROOM_MAX    = 18  # ← лимит аудитории

def _mono_line(time_s: str, subject_s: str, room_s: str) -> str:
    """Обычная текстовая строка без <code>/<pre>, с ограничением длины и без переносов."""
    time_txt = _one_line(time_s)
    subj_txt = _ellipsize(_abbrev_subject(_one_line(subject_s)), SUBJECT_MAX)
    room_txt = _ellipsize(_one_line(room_s), ROOM_MAX)

    parts = [time_txt, subj_txt]
    if room_txt:
        parts.append(room_txt)

    return " — ".join(_html_escape(p) for p in parts)

def format_week_compact_mono(group: str, parity: str, lessons: List[Dict[str, str]]) -> str:
    """
    Неделя: компактные строки «время — предмет — аудитория».
    • Дни разделены длинной полосой.
    • Повторы «Истории» в одном дне скрываем.
    • Спец-пары показываем '⚠️ прил.'.
    • Если аудитория — Zoom с ссылкой, выводим «🔗 <a href="...">Zoom</a>».
    """
    header = f"📆 <b>Группа { _html_escape(group) }</b> • {('Чётная' if parity=='чёт' else 'Нечётная')} неделя"
    if not lessons:
        return f"{header}\n{WEEK_SEP}\n— Пар не найдено —"

    # группируем по дням
    by_day: Dict[str, List[Dict[str, str]]] = {}
    for it in lessons:
        d = str(it["day"]).strip().upper()
        by_day.setdefault(d, []).append(it)

    out: List[str] = [header, WEEK_SEP]

    for d in DAY_ORDER:
        day_items = by_day.get(d, [])
        if not day_items:
            continue

        out.append(f"📌 {d.title()}")

        history_emitted = False
        for it in sorted(day_items, key=lambda x: _parse_time_key(x["time"])):
            lecture, room = _split_lecture_room(it["text"])
            subj_raw = _strip_teachers(lecture)

            # «Историю» показываем один раз в день
            if it.get("special") and _is_history_subject(subj_raw):
                if history_emitted:
                    continue
                history_emitted = True

            # аудитория (с HTML для Zoom)
            if it.get("special"):
                room_html = _html_escape("⚠️ прил.")
            else:
                if it.get("room_is_zoom") and it.get("room_link"):
                    room_html = f"🔗 {_safe_anchor(it['room_link'], 'Zoom')}"
                else:
                    room_disp = _ellipsize(_one_line(room or ""), ROOM_MAX) if room else ""
                    room_html = _html_escape(room_disp) if room_disp else ""

            out.append(_line_with_room_html(it["time"], subj_raw, room_html))

        out.append(WEEK_SEP)

    if out and out[-1] == WEEK_SEP:
        out.pop()

    return "\n".join(out)
