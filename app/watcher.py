"""Наблюдение за репозиторием по расписанию: сбор, хранение, сводка.

Задание на наблюдение говорит «смотри в этот репозиторий каждые N минут».
Планировщик (`run_forever`) крутится фоном всё время жизни приложения и в срок
снимает пульс репозитория через `github_api.pulse`. Снимок сравнивается с
предыдущим, разница превращается в события: новый коммит, открытая или
закрытая задача, изменение числа звёзд.

Всё хранится в SQLite (`data/watch.db`): задания, снимки, события. Поэтому
наблюдение переживает перезапуск и продолжается с того места, где остановилось.

`summary()` сворачивает события за период в один агрегированный ответ — его
отдаёт инструмент `watch_summary`. О протоколе MCP этот файл ничего не знает.
"""
import asyncio
import json
import logging
import sqlite3
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from . import config
from . import github_api as gh

logger = logging.getLogger("app.watcher")

# Номер состояния для интерфейса: растёт на каждое изменение заданий и событий.
revision = 0

# Что сделать, когда наблюдение нашло новое (main.py обновляет правую панель).
on_news: Callable[[], Awaitable[Any]] | None = None

TIME = "%Y-%m-%d %H:%M:%S"

# События, о которых стоит рассказывать. «start» — служебная отметка начала.
NEWS = ("commit", "issue_opened", "issue_closed", "stars")

SCHEMA = """
create table if not exists watches (
    id            integer primary key,
    repo          text    not null unique,
    every_minutes integer not null,
    active        integer not null default 1,
    created       text    not null,
    next_run      text    not null,
    last_run      text,
    runs          integer not null default 0,
    last_error    text
);
create table if not exists snapshots (
    id       integer primary key,
    watch_id integer not null,
    taken    text    not null,
    stars    integer,
    commits  text    not null,  -- короткие хеши последних коммитов, JSON
    issues   text    not null   -- открытые задачи {номер: заголовок}, JSON
);
create table if not exists events (
    id       integer primary key,
    watch_id integer not null,
    at       text    not null,
    kind     text    not null,
    ref      text,
    title    text,
    author   text
);
"""

_db: sqlite3.Connection | None = None
# Сбор идёт и по расписанию, и по кнопке, и из инструмента — не одновременно.
_lock = asyncio.Lock()


def _touch() -> None:
    global revision
    revision += 1


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def db() -> sqlite3.Connection:
    global _db
    if _db is None:
        path = Path(config.WATCH_DB)
        path.parent.mkdir(parents=True, exist_ok=True)
        _db = sqlite3.connect(path, check_same_thread=False)
        _db.row_factory = sqlite3.Row
        _db.executescript(SCHEMA)
    return _db


def _watch(watch_id: int) -> sqlite3.Row | None:
    return db().execute("select * from watches where id = ?", (watch_id,)).fetchone()


# --- сбор данных -------------------------------------------------------------

async def collect(watch_id: int) -> None:
    """Снять пульс репозитория, сравнить с прошлым снимком, записать события."""
    async with _lock:
        watch = _watch(watch_id)
        if watch is None:
            return
        now = _now()
        next_run = (now + timedelta(minutes=watch["every_minutes"])).strftime(TIME)
        try:
            pulse = await gh.pulse(watch["repo"])
        except Exception as e:  # GitHub, сеть, прокси — запомним и попробуем в срок
            error = str(e) if isinstance(e, gh.GitHubError) else "{0}: {1}".format(type(e).__name__, e)
            db().execute("update watches set last_run = ?, next_run = ?, runs = runs + 1, "
                         "last_error = ? where id = ?",
                         (now.strftime(TIME), next_run, error, watch_id))
            db().commit()
            _touch()
            logger.warning("Наблюдение %s: %s", watch["repo"], error)
            return

        stamp = now.strftime(TIME)
        commits = pulse["commits"]
        issues = {str(i["number"]): i for i in pulse["issues"]}
        previous = db().execute(
            "select * from snapshots where watch_id = ? order by id desc limit 1",
            (watch_id,)).fetchone()

        events: list[tuple] = []
        if previous is None:
            head = commits[0]["хеш"] if commits else "—"
            events.append(("start", head, "начало наблюдения: последний коммит {0}, открытых "
                           "задач {1}, звёзд {2}".format(head, len(issues), pulse["stars"]), None))
        else:
            # Известные коммиты — из прошлого снимка и из уже записанных событий:
            # после force-push старый коммит не должен посчитаться новым дважды.
            known = set(json.loads(previous["commits"])) | {
                row["ref"] for row in db().execute(
                    "select ref from events where watch_id = ? and kind = 'commit'", (watch_id,))}
            for c in reversed(commits):  # от старых к новым
                if c["хеш"] not in known:
                    events.append(("commit", c["хеш"], c["сообщение"], c["автор"]))
            before = json.loads(previous["issues"])
            for number, issue in issues.items():
                if number not in before:
                    events.append(("issue_opened", "#" + number,
                                   "{0} ({1})".format(issue["title"], issue["kind"]), issue["author"]))
            for number, title in before.items():
                if number not in issues:
                    events.append(("issue_closed", "#" + number, title, None))
            delta = (pulse["stars"] or 0) - (previous["stars"] or 0)
            if delta:
                events.append(("stars", "{0:+d}".format(delta),
                               "звёзд стало {0}".format(pulse["stars"]), None))

        db().execute(
            "insert into snapshots (watch_id, taken, stars, commits, issues) values (?, ?, ?, ?, ?)",
            (watch_id, stamp, pulse["stars"],
             json.dumps([c["хеш"] for c in commits]),
             json.dumps({n: i["title"] for n, i in issues.items()}, ensure_ascii=False)))
        db().executemany(
            "insert into events (watch_id, at, kind, ref, title, author) values (?, ?, ?, ?, ?, ?)",
            [(watch_id, stamp, kind, ref, title, author) for kind, ref, title, author in events])
        db().execute("update watches set last_run = ?, next_run = ?, runs = runs + 1, "
                     "last_error = null where id = ?", (stamp, next_run, watch_id))
        db().commit()
        _touch()

    news = [e for e in events if e[0] in NEWS]
    if news:
        logger.info("Наблюдение %s: новых событий %d", watch["repo"], len(news))
        if on_news is not None:
            await on_news()


async def run_forever() -> None:
    """Планировщик: раз в несколько секунд запускает задания, чей срок подошёл."""
    while True:
        try:
            due = db().execute(
                "select id from watches where active = 1 and next_run <= ? order by next_run",
                (_now().strftime(TIME),)).fetchall()
            for row in due:
                await collect(row["id"])
        except Exception:  # планировщик не должен умирать от одной ошибки
            logger.exception("Сбой планировщика наблюдения")
        await asyncio.sleep(5)


# --- управление заданиями ----------------------------------------------------

async def add(repo: str, every_minutes: int) -> dict:
    """Начать наблюдение или поменять интервал уже существующего."""
    name = gh.repo_name(repo)
    now = _now().strftime(TIME)
    existing = db().execute("select id from watches where repo = ?", (name,)).fetchone()
    if existing:
        watch_id = existing["id"]
        db().execute("update watches set every_minutes = ?, active = 1, next_run = ? where id = ?",
                     (every_minutes, now, watch_id))
        db().commit()
        _touch()
        # Первый снимок уже есть — просто перезапускаем расписание.
        await collect(watch_id)
        return {"результат": "интервал изменён", **_describe(_watch(watch_id))}

    cursor = db().execute(
        "insert into watches (repo, every_minutes, created, next_run) values (?, ?, ?, ?)",
        (name, every_minutes, now, now))
    db().commit()
    _touch()
    await collect(cursor.lastrowid)
    watch = _watch(cursor.lastrowid)
    start = db().execute("select title from events where watch_id = ? and kind = 'start'",
                         (watch["id"],)).fetchone()
    return {"результат": "наблюдение начато", **_describe(watch),
            "начальная точка": start["title"] if start else watch["last_error"]}


def remove(watch_id: int) -> bool:
    """Удалить задание вместе с его снимками и событиями."""
    if _watch(watch_id) is None:
        return False
    for table, column in (("events", "watch_id"), ("snapshots", "watch_id"), ("watches", "id")):
        db().execute("delete from {0} where {1} = ?".format(table, column), (watch_id,))
    db().commit()
    _touch()
    return True


def set_active(watch_id: int, on: bool) -> None:
    """Поставить на паузу или снять с неё (снятое с паузы проверяется сразу)."""
    db().execute("update watches set active = ?, next_run = ? where id = ?",
                 (int(on), _now().strftime(TIME), watch_id))
    db().commit()
    _touch()


# --- агрегированная сводка ---------------------------------------------------

def _describe(watch: sqlite3.Row) -> dict:
    return {
        "номер наблюдения": watch["id"],
        "репозиторий": watch["repo"],
        "каждые, минут": watch["every_minutes"],
        "активно": bool(watch["active"]),
        "проверок всего": watch["runs"],
        "последняя проверка": watch["last_run"] or "ещё не было",
        "следующая проверка": watch["next_run"] if watch["active"] else "на паузе",
        **({"ошибка последней проверки": watch["last_error"]} if watch["last_error"] else {}),
    }


def summary(minutes: int = 60, repo: str | None = None, after_id: int = 0) -> dict:
    """Свернуть события за период в одну сводку: сколько, чего и от кого."""
    watches = db().execute("select * from watches order by id").fetchall()
    if repo:
        name = gh.repo_name(repo)
        watches = [w for w in watches if w["repo"] == name]
    if not watches:
        return {"наблюдений нет": "попросите следить за репозиторием — инструмент watch_repo",
                "есть изменения": False, "последнее событие №": after_id}

    ids = [w["id"] for w in watches]
    marks = ",".join("?" * len(ids))
    now = _now()
    since = (now - timedelta(minutes=minutes)).strftime(TIME)
    events = db().execute(
        "select e.*, w.repo from events e join watches w on w.id = e.watch_id "
        "where e.watch_id in ({0}) and e.at >= ? and e.id > ? order by e.id".format(marks),
        (*ids, since, after_id)).fetchall()
    checks = db().execute(
        "select count(*) from snapshots where watch_id in ({0}) and taken >= ?".format(marks),
        (*ids, since)).fetchone()[0]

    by_kind: dict[str, list] = {kind: [] for kind in NEWS}
    for e in events:
        if e["kind"] in by_kind:
            by_kind[e["kind"]].append(e)
    commits = by_kind["commit"]
    stars = sum(int(e["ref"]) for e in by_kind["stars"])
    last_id = db().execute("select max(id) from events").fetchone()[0] or 0

    return {
        "период": "последние {0} мин: с {1} по {2}".format(
            minutes, since[11:16], now.strftime("%H:%M")),
        "наблюдения": [_describe(w) for w in watches],
        "проверок за период": checks,
        "есть изменения": any(by_kind.values()),
        "новых коммитов": len(commits),
        "авторы коммитов": dict(Counter(e["author"] for e in commits)),
        "коммиты": [{"хеш": e["ref"], "автор": e["author"], "сообщение": e["title"],
                     "замечен": e["at"][11:16], "репозиторий": e["repo"]} for e in commits],
        "задачи открыты": [{"номер": e["ref"], "заголовок": e["title"], "автор": e["author"]}
                           for e in by_kind["issue_opened"]],
        "задачи закрыты": [{"номер": e["ref"], "заголовок": e["title"]}
                           for e in by_kind["issue_closed"]],
        "изменение звёзд": "{0:+d}".format(stars) if stars else "без изменений",
        "последнее событие №": max(last_id, after_id),
    }


# --- состояние для правой панели ---------------------------------------------

def snapshot() -> dict:
    watches = db().execute("select * from watches order by id").fetchall()
    events = db().execute(
        "select e.*, w.repo from events e join watches w on w.id = e.watch_id "
        "order by e.id desc limit 8").fetchall()
    counts = dict(db().execute(
        "select watch_id, count(*) from events where kind != 'start' group by watch_id").fetchall())
    return {
        "revision": revision,
        "watches": [
            {"id": w["id"], "repo": w["repo"], "every": w["every_minutes"],
             "active": bool(w["active"]), "runs": w["runs"], "events": counts.get(w["id"], 0),
             "last_run": (w["last_run"] or "")[11:19], "error": w["last_error"],
             "next_ts": datetime.strptime(w["next_run"], TIME).timestamp()}
            for w in watches
        ],
        "events": [
            {"time": e["at"][11:16], "kind": e["kind"], "ref": e["ref"],
             "title": e["title"], "author": e["author"], "repo": e["repo"]}
            for e in events
        ],
    }
