"""Клиент PyPI и OSV.dev — внешние сервисы под вторым MCP-сервером.

PyPI отвечает, какая версия пакета последняя и когда вышла каждая; OSV.dev —
какие уязвимости известны для конкретной версии. Оба API открытые, ключ не
нужен. Как и `github_api.py`, этот файл о протоколе MCP ничего не знает:
регистрация инструментов и выключатели — в `mcp_server.py`.
"""
import asyncio
import re
import time
from datetime import datetime
from typing import Any

import httpx

from . import config


class DepsError(RuntimeError):
    """Ошибка обращения к PyPI или OSV.dev — уже человеческими словами, для агента."""


# Строка из requirements.txt: имя пакета, необязательные extras и условие на
# версию. «uvicorn[standard]>=0.30» → uvicorn и 0.30.
_REQUIREMENT = re.compile(
    r"^\s*([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)\s*(?:\[[^\]]*\])?"
    r"\s*(?:(?:===|==|>=|~=|<=|!=|>|<)\s*([A-Za-z0-9.*+!_-]+))?"
)

SEVERITY = {"LOW": "низкая", "MODERATE": "средняя", "MEDIUM": "средняя",
            "HIGH": "высокая", "CRITICAL": "критическая"}

# Ответ PyPI на популярный пакет весит до 700 КБ. Держим выжимку из него десять
# минут: повторная проверка не качает его заново.
PYPI_TTL = 600
_pypi_cache: dict[str, tuple[float, dict]] = {}

_client: httpx.AsyncClient | None = None


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=20, follow_redirects=True,
                                    headers={"User-Agent": "mcp-deps-tool"})
    return _client


async def close() -> None:
    """Закрыть соединения при остановке приложения."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def _request(service: str, method: str, url: str, **kwargs: Any) -> httpx.Response:
    """Запрос к PyPI или OSV. Оборванное соединение повторяем один раз, как и для GitHub."""
    for attempt in (1, 2):
        try:
            response = await _http().request(method, url, **kwargs)
            break
        except (httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadError) as e:
            if attempt == 2:
                raise DepsError("{0} не ответил ({1}); проверьте сеть или прокси".format(
                    service, type(e).__name__)) from e
            await asyncio.sleep(0.3)
        except httpx.HTTPError as e:
            raise DepsError("{0} не ответил ({1}); проверьте сеть или прокси".format(
                service, type(e).__name__)) from e
    return response


# --- мелкие помощники -------------------------------------------------------

def parse(requirement: str) -> tuple[str, str | None]:
    """«uvicorn[standard]>=0.30» → («uvicorn», «0.30»); «fastapi» → («fastapi», None)."""
    match = _REQUIREMENT.match(requirement or "")
    if not match:
        raise DepsError("«{0}» не похоже на имя пакета Python".format(requirement))
    return match.group(1), match.group(2)


def canonical(name: str) -> str:
    """Имя пакета по правилам PyPI: регистр и знаки «-», «_», «.» не различаются."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _number(version: str) -> tuple[int, ...] | None:
    """Номер обычного выпуска для сравнения: 1.0 и 1.0.0 — одно и то же.

    Предварительные выпуски (rc, beta, dev) — None: их не сравниваем и в счёт
    «выпусков новее» не берём.
    """
    parts = version.strip().split(".")
    if not all(part.isdigit() for part in parts):
        return None
    numbers = [int(part) for part in parts]
    while len(numbers) > 1 and numbers[-1] == 0:
        numbers.pop()
    return tuple(numbers)


def _date(stamp: str | None) -> str:
    """«2024-09-17T20:39:31» → «17.09.2024»."""
    return datetime.fromisoformat(stamp[:19]).strftime("%d.%m.%Y") if stamp else "—"


def _license(info: dict) -> str:
    text = (info.get("license_expression") or info.get("license") or "").strip()
    # Бывает, что в поле лежит весь текст лицензии: берём первую строку.
    first = text.splitlines()[0].strip() if text else ""
    return first if len(first) <= 60 else first[:60] + "…"


def _first_sentence(text: str | None) -> str:
    """Первая фраза описания уязвимости: в OSV оно бывает в Markdown с заголовками."""
    plain = " ".join(line.strip() for line in (text or "").splitlines()
                     if line.strip() and not line.lstrip().startswith("#"))
    head, dot, _ = plain.partition(". ")
    return head + ("." if dot else "")


# --- PyPI -------------------------------------------------------------------

async def _pypi(name: str) -> dict:
    """Выжимка из ответа PyPI: описание пакета и дата каждого выпуска."""
    key = canonical(name)
    cached = _pypi_cache.get(key)
    if cached and time.monotonic() - cached[0] < PYPI_TTL:
        return cached[1]
    response = await _request("PyPI", "GET", "{0}/{1}/json".format(config.PYPI_API, name))
    if response.status_code == 404:
        raise DepsError("на PyPI нет пакета «{0}»".format(name))
    if response.status_code >= 400:
        raise DepsError("PyPI ответил HTTP {0}".format(response.status_code))
    data = response.json()
    # Версия → когда выложена: время первого файла. Пустые и отозванные — мимо.
    released = {}
    for version, files in (data.get("releases") or {}).items():
        stamps = [f["upload_time"] for f in files if f.get("upload_time") and not f.get("yanked")]
        if stamps:
            released[version] = min(stamps)
    slim = {"info": data.get("info") or {}, "released": released}
    _pypi_cache[key] = (time.monotonic(), slim)
    return slim


def _compare(asked: str, released: dict[str, str]) -> dict[str, Any]:
    """Когда вышла заданная версия и сколько обычных выпусков вышло после неё."""
    wanted = _number(asked)
    found = asked if asked in released else next(
        (v for v in released if wanted is not None and _number(v) == wanted), None)
    if found is None:
        return {"номер": asked, "ошибка": "такой версии на PyPI нет"}
    newer = sum(1 for version, stamp in released.items()
                if stamp > released[found] and _number(version) is not None)
    return {"номер": found, "вышла": _date(released[found]), "выпусков новее": newer}


async def package_info(requirement: str, version: str | None = None) -> dict[str, Any]:
    """Пакет на PyPI: последняя версия и, если задана версия, насколько она отстала."""
    name, pinned = parse(requirement)
    data = await _pypi(name)
    info, released = data["info"], data["released"]
    title = info.get("name") or name
    latest = info.get("version") or "—"
    result: dict[str, Any] = {
        "пакет": title,
        "последняя версия": latest,
        "вышла": _date(released.get(latest)),
        "описание": (info.get("summary") or "—").strip(),
        "лицензия": _license(info) or "не указана",
        "нужен python": info.get("requires_python") or "не указано",
        "страница": "https://pypi.org/project/{0}/".format(title),
    }
    asked = (version or pinned or "").strip().lstrip("=<>~! ")
    if asked:
        result["проверенная версия"] = _compare(asked, released)
    return result


# --- OSV.dev ----------------------------------------------------------------

def _vulnerability(record: dict, name: str) -> dict[str, Any]:
    """Одна запись OSV — в несколько понятных полей."""
    ids = [i for i in [record.get("id"), *(record.get("aliases") or [])] if i]
    number = next((i for i in ids if i.startswith("CVE-")), record.get("id"))
    severity = str((record.get("database_specific") or {}).get("severity") or "").upper()
    fixed: list[str] = []
    for affected in record.get("affected") or []:
        if canonical((affected.get("package") or {}).get("name", "")) != canonical(name):
            continue
        for span in affected.get("ranges") or []:
            for event in span.get("events") or []:
                if event.get("fixed") and event["fixed"] not in fixed:
                    fixed.append(event["fixed"])
    summary = (record.get("summary") or "").strip() or _first_sentence(record.get("details"))
    return {
        "номер": number,
        "другие номера": [i for i in ids if i != number],
        "суть": (summary[:220] + "…" if len(summary) > 220 else summary) or "—",
        "опасность": SEVERITY.get(severity, "не указана"),
        "исправлено в": fixed or ["исправления пока нет"],
        "опубликована": _date(record.get("published")),
    }


async def vulnerabilities(requirement: str, version: str | None = None) -> dict[str, Any]:
    """Известные уязвимости конкретной версии пакета по базе OSV.dev."""
    name, pinned = parse(requirement)
    asked = (version or pinned or "").strip().lstrip("=<>~! ")
    if not asked:
        raise DepsError("нужна версия: без неё непонятно, что проверять")
    response = await _request(
        "OSV.dev", "POST", config.OSV_API + "/query",
        json={"package": {"name": name, "ecosystem": "PyPI"}, "version": asked})
    if response.status_code >= 400:
        raise DepsError("OSV.dev ответил HTTP {0}".format(response.status_code))

    # Одна и та же уязвимость лежит в базе под разными номерами — GHSA, PYSEC,
    # CVE, — и записи ссылаются друг на друга. Склеиваем их в одну, а пустые поля
    # одной записи дополняем из другой.
    found: list[dict[str, Any]] = []
    owner: dict[str, int] = {}
    for record in (response.json() or {}).get("vulns") or []:
        entry = _vulnerability(record, name)
        ids = [entry["номер"], *entry["другие номера"]]
        index = next((owner[i] for i in ids if i in owner), None)
        if index is None:
            index = len(found)
            found.append(entry)
        else:
            kept = found[index]
            for key, value in entry.items():
                if kept.get(key) in ("—", "не указана", ["исправления пока нет"]):
                    kept[key] = value
            kept["другие номера"] = [i for i in dict.fromkeys(kept["другие номера"] + ids)
                                     if i != kept["номер"]]
        for i in ids:
            owner[i] = index
    return {
        "пакет": name,
        "версия": asked,
        "уязвимостей": len(found),
        "список": found[:10],
        "источник": "osv.dev",
    }
