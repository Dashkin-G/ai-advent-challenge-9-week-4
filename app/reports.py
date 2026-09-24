"""Сводка найденного и запись отчёта в файл — второй и третий шаги пайплайна.

Пайплайн — три инструмента MCP подряд: `search_code` находит строки в коде
репозитория (см. `github_api.py`), `summarize` отдаёт найденное модели и
собирает отчёт, `save_to_file` кладёт отчёт на диск. Здесь живут два последних
шага; о протоколе MCP этот файл ничего не знает, а цепочку ведёт агент.

Чтобы было видно, что данные между шагами дошли без потерь, каждый шаг
возвращает отпечаток — короткий хеш — того, что получил на вход. Агент
сравнивает его с отпечатком того, что отправлял.
"""
import asyncio
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import OpenAI

from . import config

# Номер состояния для интерфейса: растёт, когда меняются отчёты на диске.
revision = 0

SUMMARY_SYSTEM = (
    "Ты делаешь сводку по результатам поиска в коде репозитория. На входе — запрос "
    "и найденные строки: файл, номер строки, в квадратных скобках функция или класс, "
    "внутри которых стоит строка, и сам текст. Напиши по-русски 3–6 коротких строк: "
    "что это такое, где объявлено, где и зачем используется. Имена файлов и функций "
    "пиши как есть. Опирайся только на эти строки и ничего не придумывай. Без "
    "вступлений и советов."
)

_model: OpenAI | None = None


class ReportError(RuntimeError):
    """Ошибка шага — уже человеческими словами, для агента."""


def _touch() -> None:
    global revision
    revision += 1


def fingerprint(data: Any) -> str:
    """Короткий отпечаток данных: совпал у отправителя и получателя — дошло без искажений."""
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]


# --- шаг 2: сводка ------------------------------------------------------------

def _ask_model(text: str):
    """Один вызов модели. Клиент синхронный — зовём его из отдельного потока."""
    global _model
    if _model is None:
        if not config.DASHSCOPE_API_KEY:
            raise ReportError("DASHSCOPE_API_KEY не задан: сводку писать некому")
        _model = OpenAI(api_key=config.DASHSCOPE_API_KEY, base_url=config.DASHSCOPE_BASE_URL)
    return _model.chat.completions.create(model=config.MODEL, messages=[
        {"role": "system", "content": SUMMARY_SYSTEM},
        {"role": "user", "content": text},
    ])


async def summarize(found: dict) -> dict:
    """Сводка моделью и отчёт в Markdown по ответу search_code."""
    # Отпечаток, который приложил отправитель, в сами данные не входит.
    data = {key: value for key, value in found.items() if key != "отпечаток"}
    matches = data.get("совпадения")
    if not isinstance(matches, list) or not all(isinstance(m, dict) for m in matches):
        raise ReportError("на входе нет списка «совпадения»: передайте ответ search_code целиком")
    if not matches:
        raise ReportError("совпадений нет — сводить нечего")

    by_file: dict[str, list[dict]] = defaultdict(list)
    for m in matches:
        by_file[str(m.get("файл"))].append(m)

    lines = ["Запрос: «{0}». Репозиторий: {1}. Совпадений: {2}, файлов: {3}.".format(
        data.get("запрос"), data.get("репозиторий"), len(matches), len(by_file))]
    for m in matches:
        inside = " [{0}]".format(m["внутри"]) if m.get("внутри") else ""
        lines.append("{0}:{1}{2}  {3}".format(m.get("файл"), m.get("строка"), inside, m.get("текст")))
    try:
        response = await asyncio.to_thread(_ask_model, "\n".join(lines))
    except ReportError:
        raise
    except Exception as e:  # сеть, квота, ключ — говорим словами, а не падением
        raise ReportError("модель не ответила: {0}".format(e)) from e
    text = (response.choices[0].message.content or "").strip()
    if not text:
        raise ReportError("модель вернула пустую сводку")

    report = _markdown(data, text, by_file)
    return {
        "запрос": data.get("запрос"),
        "сводка": text,
        "отчёт": report,
        "токенов": response.usage.total_tokens if response.usage else 0,
        "получено": {"совпадений": len(matches), "отпечаток": fingerprint(data)},
        "отпечаток": fingerprint(report),
    }


def _markdown(data: dict, text: str, by_file: dict[str, list[dict]]) -> str:
    """Отчёт целиком: что искали, сводка модели и все найденные строки по файлам."""
    lines = [
        "# Поиск по коду: «{0}»".format(data.get("запрос")),
        "",
        "Репозиторий: {0} · где искали: {1} · {2}".format(
            data.get("репозиторий"), data.get("папка"), datetime.now().strftime("%d.%m.%Y %H:%M")),
        "Совпадений: {0}, файлов с совпадениями: {1}, просмотрено файлов: {2}.".format(
            data.get("совпадений"), data.get("файлов с совпадениями"), data.get("просмотрено файлов")),
        "",
        "## Сводка",
        "",
        text,
        "",
        "## Где встречается",
    ]
    for name, rows in by_file.items():
        lines += ["", "### {0} — {1}".format(name, len(rows)), ""]
        for m in rows:
            inside = " ({0})".format(m["внутри"]) if m.get("внутри") else ""
            line = str(m.get("текст")).replace("`", "'")
            lines.append("- строка {0}{1}: `{2}`".format(m.get("строка"), inside, line))
    if data.get("показаны"):
        lines += ["", "В отчёт вошли {0} совпадений.".format(data["показаны"])]
    return "\n".join(lines) + "\n"


# --- шаг 3: файл --------------------------------------------------------------

def _slug(name: str) -> str:
    """Имя файла из произвольной строки: буквы, цифры, точка, дефис, подчёркивание."""
    name = re.sub(r"\.md$", "", name.strip(), flags=re.IGNORECASE)
    return re.sub(r"[^\w.-]+", "-", name).strip("-.")[:60] or "report"


def save(name: str, content: str) -> dict:
    """Записать отчёт в data/reports/<имя>.md. То же имя перезаписывает файл."""
    path = Path(config.REPORTS_DIR) / (_slug(name) + ".md")
    existed = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")
    _touch()
    return {
        "файл": path.as_posix(),
        "байт": path.stat().st_size,
        "строк": len(content.splitlines()),
        "перезаписан": existed,
        "получено": {"символов": len(content), "отпечаток": fingerprint(content)},
    }


# --- отчёты на диске: для правой панели ---------------------------------------

def find(name: str) -> Path | None:
    """Файл отчёта по имени — только из папки отчётов, без выхода за её пределы."""
    path = Path(config.REPORTS_DIR) / Path(name).name
    return path if path.suffix == ".md" and path.is_file() else None


def remove(name: str) -> bool:
    path = find(name)
    if path is None:
        return False
    path.unlink()
    _touch()
    return True


def listing() -> list[dict]:
    """Отчёты на диске, свежие сверху."""
    folder = Path(config.REPORTS_DIR)
    if not folder.is_dir():
        return []
    rows = []
    for path in folder.glob("*.md"):
        stat = path.stat()
        rows.append({"name": path.name, "size": stat.st_size, "mtime": stat.st_mtime,
                     "time": datetime.fromtimestamp(stat.st_mtime).strftime("%d.%m %H:%M")})
    rows.sort(key=lambda row: row["mtime"], reverse=True)
    return rows
