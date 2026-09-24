"""Клиент GitHub REST API — настоящий внешний сервис под нашим MCP-сервером.

Здесь только работа с сетью и сжатие ответа: GitHub отдаёт на один коммит
сотню полей, а модели нужно несколько понятных. Регистрация инструментов, их
параметры и выключатели — в `mcp_server.py`; этот файл о протоколе MCP ничего
не знает и его можно дёргать откуда угодно.

Токен ищется по очереди в трёх местах:

1. `GITHUB_TOKEN` в `.env` — обычный путь;
2. `gh auth token` — если на машине стоит GitHub CLI и в нём выполнен вход;
3. никакого — публичный репозиторий читается и анонимно, но лимит падает
   с 5000 до 60 запросов в час.
"""
import asyncio
import base64
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

import httpx

from . import config


class GitHubError(RuntimeError):
    """Ошибка обращения к API — уже человеческими словами, для агента."""


# Откуда взялся токен: строка уходит в правую панель интерфейса.
auth_source = "не проверялся"

# Что GitHub в последний раз сказал об остатке лимита (заголовки X-RateLimit-*).
rate: dict[str, Any] = {"limit": None, "remaining": None, "reset": None}

_token: str | None = None
_client: httpx.AsyncClient | None = None

FILE_STATUS = {
    "added": "добавлен",
    "modified": "изменён",
    "removed": "удалён",
    "renamed": "переименован",
    "copied": "скопирован",
    "changed": "изменён",
}


# --- токен и http-клиент ----------------------------------------------------

def _token_from_gh_cli() -> str:
    """Спросить токен у GitHub CLI: человек уже вошёл в `gh` — ключ не нужен."""
    exe = shutil.which("gh")
    if not exe:
        return ""
    try:
        done = subprocess.run([exe, "auth", "token"], capture_output=True,
                              text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return ""
    return done.stdout.strip() if done.returncode == 0 else ""


def token() -> str:
    """Токен для запросов; пустая строка означает анонимный режим."""
    global _token, auth_source
    if _token is None:
        _token = config.GITHUB_TOKEN.strip()
        auth_source = "из .env"
        if not _token:
            _token = _token_from_gh_cli()
            auth_source = "из gh CLI" if _token else "без токена"
    return _token


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "mcp-github-tool",
        }
        key = token()
        if key:
            headers["Authorization"] = "Bearer " + key
        _client = httpx.AsyncClient(base_url=config.GITHUB_API, headers=headers, timeout=20)
    return _client


async def close() -> None:
    """Закрыть соединения при остановке приложения."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _remember_rate(response: httpx.Response) -> None:
    headers = response.headers
    if "x-ratelimit-limit" not in headers:
        return
    rate["limit"] = int(headers["x-ratelimit-limit"])
    rate["remaining"] = int(headers.get("x-ratelimit-remaining", 0))
    reset = headers.get("x-ratelimit-reset")
    rate["reset"] = (
        datetime.fromtimestamp(int(reset)).astimezone().strftime("%H:%M") if reset else None
    )


async def get(endpoint: str, **params: Any) -> Any:
    """GET к api.github.com. Любой сбой превращаем в понятное сообщение.

    Параметры запроса идут через **params, поэтому первый аргумент назван
    endpoint: у GitHub есть свой параметр `path` (фильтр коммитов по файлу).
    """
    query = {key: value for key, value in params.items() if value is not None}
    try:
        response = await _http().get(endpoint, params=query)
    except httpx.HTTPError as e:
        raise GitHubError(
            "GitHub не ответил ({0}); проверьте сеть или прокси".format(type(e).__name__)
        ) from e

    _remember_rate(response)
    if response.status_code == 404:
        raise GitHubError("GitHub отвечает «не найдено» на " + endpoint)
    if response.status_code == 403 and rate.get("remaining") == 0:
        raise GitHubError(
            "исчерпан лимит запросов к GitHub ({0} в час), он обновится в {1}; "
            "помогает токен в GITHUB_TOKEN".format(rate.get("limit"), rate.get("reset"))
        )
    if response.status_code >= 400:
        message = ""
        try:
            message = (response.json() or {}).get("message", "")
        except ValueError:
            pass
        raise GitHubError("GitHub ответил HTTP {0}: {1}".format(
            response.status_code, message or "без пояснения"))
    return response.json()


# --- мелкие помощники -------------------------------------------------------

def repo_name(repo: str | None) -> str:
    """«владелец/имя»: из параметра инструмента либо из настроек."""
    name = (repo or config.GITHUB_REPO).strip().strip("/")
    parts = name.split("/")
    if len(parts) != 2 or not all(parts):
        raise GitHubError(
            "«{0}» не похоже на репозиторий: нужен вид «владелец/имя»".format(name))
    return name


def _when(iso: str | None) -> str:
    """Время GitHub (UTC, ISO) → местное «22.09.2026 17:40»."""
    if not iso:
        return "—"
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone().strftime(
        "%d.%m.%Y %H:%M")


def _commit_row(item: dict) -> dict:
    commit = item.get("commit") or {}
    author = commit.get("author") or {}
    message = (commit.get("message") or "").strip()
    return {
        "хеш": (item.get("sha") or "")[:7],
        "автор": author.get("name") or "—",
        "когда": _when(author.get("date")),
        "сообщение": message.splitlines()[0] if message else "—",
    }


# --- то, что вызывают инструменты MCP ---------------------------------------

async def repo_info(repo: str | None = None) -> dict:
    """Карточка репозитория."""
    name = repo_name(repo)
    data = await get("/repos/" + name)
    return {
        "репозиторий": data.get("full_name"),
        "описание": data.get("description") or "—",
        "ветка по умолчанию": data.get("default_branch"),
        "основной язык": data.get("language") or "—",
        "звёзд": data.get("stargazers_count"),
        "форков": data.get("forks_count"),
        "открытых задач и pull request": data.get("open_issues_count"),
        "последняя запись в репозиторий": _when(data.get("pushed_at")),
        "создан": _when(data.get("created_at")),
        "приватный": data.get("private"),
        "ссылка": data.get("html_url"),
    }


async def list_commits(repo: str | None = None, limit: int = 5,
                       branch: str | None = None, path: str | None = None) -> dict:
    """Последние коммиты — по желанию только по ветке и только по файлу."""
    name = repo_name(repo)
    data = await get("/repos/{0}/commits".format(name),
                     per_page=max(1, min(limit, 30)), sha=branch, path=path)
    return {
        "репозиторий": name,
        "ветка": branch or "по умолчанию",
        "фильтр по пути": path or "нет",
        "коммитов": len(data),
        "коммиты": [_commit_row(item) for item in data],
    }


async def commit_details(sha: str, repo: str | None = None,
                         max_files: int = 10, patch_lines: int = 30) -> dict:
    """Что именно изменил коммит: файлы, счётчики строк и куски диффа."""
    name = repo_name(repo)
    data = await get("/repos/{0}/commits/{1}".format(name, sha.strip()))
    commit = data.get("commit") or {}
    author = commit.get("author") or {}
    stats = data.get("stats") or {}
    all_files = data.get("files") or []

    files = []
    for item in all_files[:max_files]:
        row = {
            "файл": item.get("filename"),
            "что с ним": FILE_STATUS.get(item.get("status", ""), item.get("status")),
            "добавлено строк": item.get("additions"),
            "удалено строк": item.get("deletions"),
        }
        patch = item.get("patch")
        if patch:
            lines = patch.splitlines()
            row["изменения"] = "\n".join(lines[:patch_lines])
            if len(lines) > patch_lines:
                row["изменения"] += "\n… дальше обрезано"
        files.append(row)

    result = {
        "репозиторий": name,
        "хеш": (data.get("sha") or "")[:7],
        "автор": author.get("name") or "—",
        "когда": _when(author.get("date")),
        "сообщение": (commit.get("message") or "").strip() or "—",
        "файлов изменено": len(all_files),
        "добавлено строк": stats.get("additions"),
        "удалено строк": stats.get("deletions"),
        "файлы": files,
        "ссылка": data.get("html_url"),
    }
    if len(all_files) > max_files:
        result["показаны файлы"] = "первые {0} из {1}".format(max_files, len(all_files))
    return result


async def list_issues(repo: str | None = None, state: str = "open",
                      limit: int = 10) -> dict:
    """Задачи репозитория. GitHub отдаёт в этом же списке и pull request'ы."""
    name = repo_name(repo)
    data = await get("/repos/{0}/issues".format(name), state=state,
                     per_page=max(1, min(limit, 30)))
    rows = [
        {
            "номер": item.get("number"),
            "заголовок": item.get("title"),
            "тип": "pull request" if item.get("pull_request") else "задача",
            "состояние": "открыта" if item.get("state") == "open" else "закрыта",
            "автор": (item.get("user") or {}).get("login"),
            "метки": [label.get("name") for label in item.get("labels") or []],
            "комментариев": item.get("comments"),
            "создана": _when(item.get("created_at")),
            "ссылка": item.get("html_url"),
        }
        for item in data
    ]
    return {"репозиторий": name, "отбор": state, "найдено": len(rows), "задачи": rows}


async def list_files(repo: str | None = None, path: str = "",
                     ref: str | None = None) -> dict:
    """Содержимое папки репозитория (пустой путь — корень)."""
    name = repo_name(repo)
    clean = path.strip().strip("/")
    data = await get("/repos/{0}/contents/{1}".format(name, clean), ref=ref)
    if isinstance(data, dict):
        raise GitHubError(
            "«{0}» — это файл, а не папка; его содержимое вернёт read_file".format(clean))
    rows = [
        {
            "имя": item.get("name"),
            "путь": item.get("path"),
            "это": "папка" if item.get("type") == "dir" else "файл",
            "размер, байт": item.get("size"),
        }
        for item in data
    ]
    rows.sort(key=lambda row: (row["это"] != "папка", row["имя"].lower()))
    return {
        "репозиторий": name,
        "папка": clean or "корень",
        "ветка или коммит": ref or "по умолчанию",
        "записей": len(rows),
        "содержимое": rows,
    }


async def read_file(path: str, repo: str | None = None, ref: str | None = None,
                    max_chars: int = 12000) -> dict:
    """Текст файла из репозитория."""
    name = repo_name(repo)
    clean = path.strip().strip("/")
    if not clean:
        raise GitHubError("не указан путь к файлу")
    data = await get("/repos/{0}/contents/{1}".format(name, clean), ref=ref)
    if isinstance(data, list):
        raise GitHubError(
            "«{0}» — это папка, а не файл; её содержимое вернёт list_files".format(clean))
    if not data.get("content"):
        raise GitHubError(
            "«{0}» не отдаётся текстом: слишком большой файл ({1} байт)".format(
                clean, data.get("size")))

    raw = base64.b64decode(data["content"])
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise GitHubError("«{0}» — двоичный файл, текстом его не прочитать".format(clean))

    cut = len(text) > max_chars
    return {
        "репозиторий": name,
        "файл": data.get("path", clean),
        "ветка или коммит": ref or "по умолчанию",
        "размер, байт": data.get("size"),
        "строк": text.count("\n") + 1,
        "обрезан": cut,
        "содержимое": text[:max_chars] + ("\n… файл обрезан" if cut else ""),
    }


# --- поиск по коду: первый шаг пайплайна ------------------------------------

# Двоичные файлы не читаем: текста в них нет, а запрос к API тратится.
BINARY = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".webp", ".pdf", ".zip", ".gz",
          ".exe", ".dll", ".pyc", ".db", ".sqlite", ".woff", ".woff2", ".ttf", ".mp3", ".mp4"}

# Объявление функции или класса — в Python и в JavaScript.
_SCOPE = re.compile(r"^(\s*)(?:async\s+)?(?:def|class|function)\s+(\w+)")

# Содержимое по хешу не меняется, поэтому прочитанное помним: повторный поиск
# по тому же репозиторию стоит один запрос (дерево файлов) вместо десятка.
_blobs: dict[str, str | None] = {}


async def _blob_text(name: str, sha: str, limit: asyncio.Semaphore) -> str | None:
    """Текст файла по его хешу; None — файл двоичный."""
    if sha not in _blobs:
        async with limit:
            data = await get("/repos/{0}/git/blobs/{1}".format(name, sha))
        if len(_blobs) > 2000:
            _blobs.clear()
        try:
            _blobs[sha] = base64.b64decode(data.get("content") or "").decode("utf-8")
        except UnicodeDecodeError:
            _blobs[sha] = None
    return _blobs[sha]


def _grep(text: str, needle: str) -> list[tuple[int, str, str]]:
    """Строки с запросом без учёта регистра и где каждая стоит: функция или класс."""
    low = needle.lower()
    scope: list[tuple[int, str]] = []
    found = []
    for number, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        # Строка вровень с объявлением или левее — объявление кончилось. Закрывающая
        # скобка не в счёт: это хвост сигнатуры или конец тела того же объявления.
        if stripped and stripped[0] not in ")]}":
            indent = len(line) - len(line.lstrip())
            while scope and scope[-1][0] >= indent:
                scope.pop()
        if low in line.lower():
            found.append((number, stripped, ".".join(name for _, name in scope)))
        head = _SCOPE.match(line)
        if head:
            scope.append((len(head.group(1)), head.group(2)))
    return found


async def search_code(query: str, repo: str | None = None, path: str = "",
                      max_files: int = 60, max_matches: int = 60) -> dict:
    """Все строки репозитория (или одной его папки), где встречается запрос.

    Поиск самого GitHub (/search/code) не используем: у него отдельный лимит, а
    свежие репозитории он индексирует с опозданием. Берём дерево файлов и читаем
    их сами — так находится ровно то, что лежит в репозитории сейчас.
    """
    name = repo_name(repo)
    needle = query.strip()
    if len(needle) < 2:
        raise GitHubError("запрос короче двух символов: так найдётся всё подряд")
    folder = path.strip().strip("/")
    tree = await get("/repos/{0}/git/trees/HEAD".format(name), recursive=1)
    files = [
        item for item in tree.get("tree") or []
        if item.get("type") == "blob"
        and (not folder or item["path"] == folder or item["path"].startswith(folder + "/"))
        and 0 < (item.get("size") or 0) <= 300_000
        and PurePosixPath(item["path"]).suffix.lower() not in BINARY
    ]
    if not files:
        raise GitHubError("в «{0}» нет текстовых файлов для поиска".format(folder or name))
    rest = len(files) - max_files
    files = files[:max_files]

    limit = asyncio.Semaphore(8)
    texts = await asyncio.gather(*(_blob_text(name, item["sha"], limit) for item in files))

    matches = []
    for item, text in zip(files, texts):
        for number, line, inside in _grep(text or "", needle):
            row = {"файл": item["path"], "строка": number, "текст": line[:160]}
            if inside:
                row["внутри"] = inside
            matches.append(row)

    result = {
        "запрос": needle,
        "репозиторий": name,
        "папка": folder or "весь репозиторий",
        "просмотрено файлов": len(files),
        "файлов с совпадениями": len({row["файл"] for row in matches}),
        "совпадений": len(matches),
        "совпадения": matches[:max_matches],
    }
    if len(matches) > max_matches:
        result["показаны"] = "первые {0} из {1}".format(max_matches, len(matches))
    if rest > 0:
        result["не просмотрено"] = "ещё {0} файлов: сузьте поиск параметром path".format(rest)
    return result


# --- сводка для правой панели интерфейса ------------------------------------

async def overview(repo: str | None = None) -> dict:
    """Тот же репозиторий, но для показа человеку: карточка, коммиты, задачи.

    Панель ходит в GitHub только по кнопке и при старте, а не на каждый опрос
    состояния, — иначе интерфейс сам выест весь лимит запросов.
    """
    name = repo_name(repo)
    info, commits, issues = await asyncio.gather(
        get("/repos/" + name),
        get("/repos/{0}/commits".format(name), per_page=5),
        get("/repos/{0}/issues".format(name), state="open", per_page=5),
    )
    return {
        "repo": name,
        "url": info.get("html_url"),
        "description": info.get("description") or "",
        "branch": info.get("default_branch"),
        "language": info.get("language") or "—",
        "stars": info.get("stargazers_count"),
        "forks": info.get("forks_count"),
        "pushed": _when(info.get("pushed_at")),
        "open_issues": info.get("open_issues_count"),
        "commits": [
            {"sha": row["хеш"], "message": row["сообщение"],
             "author": row["автор"], "when": row["когда"]}
            for row in (_commit_row(item) for item in commits)
        ],
        "issues": [
            {"number": item.get("number"), "title": item.get("title"),
             "author": (item.get("user") or {}).get("login"),
             "kind": "pull request" if item.get("pull_request") else "задача"}
            for item in issues
        ],
    }


async def pulse(repo: str | None = None) -> dict:
    """Пульс репозитория для наблюдения по расписанию: звёзды, коммиты, задачи.

    Три запроса за раз. Коммитов берём 30, открытых задач — до 100: по
    разнице между двумя такими снимками и видно, что появилось и что закрылось.
    """
    name = repo_name(repo)
    info, commits, issues = await asyncio.gather(
        get("/repos/" + name),
        get("/repos/{0}/commits".format(name), per_page=30),
        get("/repos/{0}/issues".format(name), state="open", per_page=100),
    )
    return {
        "repo": name,
        "stars": info.get("stargazers_count"),
        "commits": [_commit_row(item) for item in commits],
        "issues": [
            {"number": item.get("number"), "title": item.get("title"),
             "author": (item.get("user") or {}).get("login"),
             "kind": "pull request" if item.get("pull_request") else "задача"}
            for item in issues
        ],
    }


def auth_state() -> dict:
    """Как мы ходим в GitHub и сколько запросов осталось — для панели."""
    return {
        "source": auth_source,
        "authorized": bool(_token),
        "limit": rate["limit"],
        "remaining": rate["remaining"],
        "reset": rate["reset"],
    }
