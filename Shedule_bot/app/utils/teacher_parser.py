# app/utils/teacher_parser.py
from __future__ import annotations

import re
from typing import Iterable, List, Tuple

# Базовые классы символов (кириллица + Ё/ё)
RUS_U = "А-ЯЁ"
RUS_L = "а-яё"

# Фамилия и Имя/Отчество (одно слово, с дефисом по желанию)
SURNAME_RE = rf"[{RUS_U}][{RUS_L}]+(?:-[{RUS_U}][{RUS_L}]+)?"
NAME_RE    = rf"[{RUS_U}][{RUS_L}]+(?:-[{RUS_U}][{RUS_L}]+)?"

# Инициалы: допускаем точки и/или пробелы между буквами, а также вариант БЕЗ точек
# Примеры: "Н.А.", "Н. А.", "НА", "Н А", "Н.А.Б."
INIT2_RE = rf"([{RUS_U}])\.?\s*([{RUS_U}])\.?"
INIT3_RE = rf"([{RUS_U}])\.?\s*([{RUS_U}])\.?\s*([{RUS_U}])\.?"

# 1) «Фамилия И.О.» или «Фамилия ИО» (без точек)
PAT_SURNAME_INITS3 = re.compile(rf"\b({SURNAME_RE})\s+{INIT3_RE}\b")
PAT_SURNAME_INITS2 = re.compile(rf"\b({SURNAME_RE})\s+{INIT2_RE}\b")

# 2) «И.О. Фамилия» или «ИО Фамилия» (без точек)
PAT_INITS_SURNAME3 = re.compile(rf"\b{INIT3_RE}\s+({SURNAME_RE})\b")
PAT_INITS_SURNAME2 = re.compile(rf"\b{INIT2_RE}\s+({SURNAME_RE})\b")

# 3) Полное ФИО: «Фамилия Имя Отчество»
# По умолчанию используем ОСТОРОЖНО: только рядом с маркёрами вроде «лектор/преподаватель/ведёт»
PAT_FULL_FIO = re.compile(rf"\b({SURNAME_RE})\s+({NAME_RE})\s+({NAME_RE})\b")

# Маркёры, около которых разрешаем ловить «полное ФИО»
ROLE_MARKERS = re.compile(r"\b(лектор|преп(одавател[ья])?|вед[её]т|семинарист|учит)\b", re.I)

# Служебные титулы/степени — удаляем перед поиском ФИО (регистронезависимо)
TITLES_RE = re.compile(
    r"\b(?:проф\.?|доц\.?|асс\.?|ст\.?\s*преп\.?|преп\.?|к\.?\s*[а-я]\.?\s*[а-я]\.?\s*н\.?|"
    r"д\.?\s*[а-я]\.?\s*[а-я]\.?\s*н\.?|канд\.?\s*наук|phd|dr\.?)\b\.?",
    re.I,
)

# Разделители внутри ячейки часто не мешают, но иногда полезно их привести к пробелам
NORMALIZE_SEPARATORS = re.compile(r"[;,/|]+")


def _normalize_source(text: str) -> str:
    """
    Лёгкая нормализация: прибираем титулы, схлопываем разделители и пробелы.
    ВАЖНО: не трогаем регистр, он нужен для распознавания.
    """
    t = TITLES_RE.sub(" ", text)
    t = t.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    t = NORMALIZE_SEPARATORS.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _mk_display(surname: str, initials: Iterable[str]) -> str:
    """Единый формат вывода: 'Фамилия И.О.' (или с 3 инициалами)."""
    inits = [ch.upper() for ch in initials if ch]
    if not inits:
        return surname
    return surname + " " + ".".join(inits) + "."


def _add(result: List[str], item: str, seen: set[Tuple[str, str]]) -> None:
    """
    Добавить в список без дублей. `seen` хранит кортеж (фамилия_lower, initials).
    """
    item = item.strip()
    if not item:
        return
    # распил на фамилию и инициалы для ключа
    m = re.match(rf"^\s*({SURNAME_RE})\s+([A-ZА-ЯЁ]\.[A-ZА-ЯЁ](?:\.[A-ZА-ЯЁ])?\.)\s*$", item, re.IGNORECASE)
    key = None
    if m:
        key = (m.group(1).lower(), m.group(2).upper())
    else:
        key = (item.lower(), "")
    if key in seen:
        return
    seen.add(key)
    result.append(item)


def extract_teachers(raw_text: str, allow_full_fio_near_roles: bool = True) -> List[str]:
    """
    Извлекает список преподавателей из текста ячейки.
    Возвращает список строк в едином формате: 'Фамилия И.О.' (без должностей/титулов).
    Поддерживаются варианты с/без точек в инициалах, а также 'И.О. Фамилия'.

    Порядок:
      1) Фамилия + инициалы (2/3).
      2) Инициалы + фамилия (2/3).
      3) (опционально) Полное ФИО, но только рядом с роль-маркёрами.

    Пример:
      'Английский язык / Олехно Н.А.; Петров П П' -> ['Олехно Н.А.', 'Петров П.П.']
    """
    text = _normalize_source(raw_text)

    results: List[str] = []
    seen: set[Tuple[str, str]] = set()

    # 1) «Фамилия И.О.» / «Фамилия ИО»
    for pat in (PAT_SURNAME_INITS3, PAT_SURNAME_INITS2):
        for m in pat.finditer(text):
            surname = m.group(1)
            inits = [g for g in m.groups()[1:] if g and len(g) == 1]  # только буквенные группы
            _add(results, _mk_display(surname, inits), seen)

    # 2) «И.О. Фамилия» / «ИО Фамилия»
    for pat in (PAT_INITS_SURNAME3, PAT_INITS_SURNAME2):
        for m in pat.finditer(text):
            # первые группы — инициалы, последняя — фамилия
            *letters, surname = m.groups()
            inits = [g for g in letters if g and len(g) == 1]
            _add(results, _mk_display(surname, inits), seen)

    # 3) Полное ФИО (ограниченно): ловим только в небольших окнах после маркёров ролей
    if allow_full_fio_near_roles:
        for role in ROLE_MARKERS.finditer(text):
            start = role.end()
            window = text[start:start + 80]  # небольшое окно справа от маркёра
            for m in PAT_FULL_FIO.finditer(window):
                surname, name, patronymic = m.groups()
                inits = [name[0], patronymic[0]]
                _add(results, _mk_display(surname, inits), seen)

    return results


# ------- Небольшая ручная проверка -------
if __name__ == "__main__":
    samples = [
        "Английский язык / Олехно Н.А.",
        "История / Н.А. Олехно",
        "Матан — ауд. 2207 / асс. Иванов И.И.; Петров П П",
        "Физика / проф. Гречановский С.В.",
        "Лекции (лектор: Иванова-Петрова Елена Викторовна) — 2530",
        "Практика: Е В Иванова | Zoom",
    ]
    for s in samples:
        print(s, "→", extract_teachers(s))

# app/utils/teacher_parser.py — ДОБАВЬ НИЖЕ СУЩЕСТВУЮЩЕГО КОДА

# --- Доп. паттерны для одинарной инициалы и фамилии после роли ---
SURNAME_ONLY_RE = rf"[{RUS_U}][{RUS_L}]+(?:-[{RUS_U}][{RUS_L}]+)?"

# «Фамилия И.» или «Фамилия И» (одна инициала, с/без точки)
PAT_SURNAME_INIT1 = re.compile(rf"\b({SURNAME_ONLY_RE})\s+([{RUS_U}])\.?\b")

# Роль + фамилия (без инициалов) в конце/перед разделителем: «практ. Салтыкова», «лек. Иванов»
PAT_ROLE_SURNAME_TAIL = re.compile(
    rf"(?:\b(?:лек\.?|практ\.?|лаб\.?|сем\.?|преп\.?)\b[ ,.:;-]*)({SURNAME_ONLY_RE})(?=$|\s|[,.;:])",
    re.I,
)

def _dedup_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for t in items:
        key = t.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(t.strip())
    return out

def extract_teachers_smart(raw_text: str) -> List[str]:
    """
    Расширенный сбор преподавателей:
      1) базовые варианты (Фамилия И.О., И.О. Фамилия, полное ФИО рядом с ролями),
      2) «Фамилия И.» (одна инициала),
      3) «практ./лек./лаб./сем. Фамилия» (фамилия без инициалов).
    Возвращает формат для вывода: «Фамилия И.О.» / «Фамилия И.» / «Фамилия».
    """
    base = extract_teachers(raw_text)  # из имеющейся функции
    extra: List[str] = []

    # 2) Фамилия + одна инициала
    for m in PAT_SURNAME_INIT1.finditer(raw_text):
        surname, ini = m.group(1), m.group(2)
        extra.append(f"{surname} {ini}.")

    # 3) Роль + фамилия (без инициалов)
    for m in PAT_ROLE_SURNAME_TAIL.finditer(raw_text):
        surname = m.group(1)
        extra.append(surname)

    return _dedup_preserve_order(base + extra)
