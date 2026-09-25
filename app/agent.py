"""Агент с чатом: MCP-клиент плюс вызов модели.

Это вторая половина проекта. Агент подключается к нескольким MCP-серверам из
реестра (`config.MCP_SERVERS`) официальным клиентом из SDK (`mcp.Client`) по
сети — у каждого сервера своя сессия. Инструменты всех серверов он отдаёт
модели одним списком, а вызовы сам разводит по серверам: имя инструмента для
модели начинается с имени сервера (`github__read_file`, `deps__package_info`).
Всё, что уходит по протоколу и приходит обратно, попадает в общий журнал
обмена с пометкой сервера — его видно в интерфейсе.

Кроме ответов на вопросы агент умеет работать сам: `AutoReport` раз в N минут
спрашивает у сервера GitHub сводку наблюдения и присылает её в чат, а
`Pipeline` проводит цепочку из трёх инструментов «поиск → сводка → файл».
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import PurePosixPath

from mcp import Client
from openai import OpenAI

from . import config

logger = logging.getLogger("app.agent")

SYSTEM = (
    "Ты — ассистент по репозиторию на GitHub и его зависимостям. Отвечай по-русски, "
    "коротко и по делу.\n"
    "Инструменты приходят с нескольких MCP-серверов: имя инструмента начинается с "
    "имени сервера и двух подчёркиваний, например github__read_file. Всё о "
    "репозитории и пакетах ты узнаёшь ТОЛЬКО через инструменты. Ничего не "
    "придумывай: нет инструмента — так и скажи.\n"
    "Если нужен хеш коммита или путь к файлу, сначала возьми список, а потом "
    "запрашивай подробности. Опирайся на то, что вернули инструменты: хеши, даты, "
    "имена файлов, номера задач, версии.\n"
    "Где в коде встречается слово или имя — ищи через github__search_code.\n"
    "Вызовы, которые не зависят друг от друга (например, проверку нескольких "
    "пакетов), делай за один ход.\n"
    "Если просят следить за репозиторием или присылать сводку — заведи наблюдение "
    "через github__watch_repo. На вопросы «что произошло за час / за день» отвечай "
    "по github__watch_summary."
)
NO_TOOLS_NOTE = (
    "\nСейчас ни одного соединения с MCP-серверами нет и инструментов у тебя нет. "
    "Скажи об этом прямо и предложи нажать «Подключиться»."
)

# Имя инструмента для модели: «сервер__инструмент». По приставке агент и
# отправляет вызов нужному серверу.
SEP = "__"

# Переписка живёт в памяти процесса: перезапуск начинает разговор заново.
history: list[dict] = []
revision = 0

# Общий журнал обмена со всеми серверами: у каждой строки пометка, с каким
# сервером шёл обмен.
journal: list[dict] = []

# Ход текущего обращения к агенту: интерфейс показывает вызовы по мере того,
# как они идут. None — агент свободен.
working: dict | None = None


def _touch() -> None:
    global revision
    revision += 1


def _note(server: str, method: str, summary: str, detail: object = None, ok: bool = True) -> None:
    """Строка журнала обмена: время, сервер, метод протокола, суть и подробности."""
    detail_text = ""
    if detail is not None:
        detail_text = json.dumps(detail, ensure_ascii=False, indent=2, default=str)
    journal.append({
        "time": datetime.now().strftime("%H:%M:%S"),
        "server": server,
        "method": method,
        "summary": summary,
        "ok": ok,
        "detail": detail_text,
    })
    del journal[:-60]  # на экране нужна свежая часть, а не весь день
    _touch()


def _reason(error: BaseException) -> str:
    """Человеческая причина сбоя.

    Клиент SDK работает через группу задач, и наружу вылетает ExceptionGroup —
    в интерфейсе от неё толку нет, поэтому разворачиваем до настоящей ошибки.
    """
    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    # Ошибку протокола сервер присылает уже человеческим текстом.
    message = getattr(error, "message", None)
    if isinstance(message, str) and message:
        return message
    status = getattr(getattr(error, "response", None), "status_code", None)
    if status is not None:
        return "сервер ответил HTTP {0}".format(status)
    if isinstance(error, ConnectionError) or type(error).__name__ == "ConnectError":
        return "сервер не отвечает по этому адресу"
    return "{0}: {1}".format(type(error).__name__, error)


class McpLink:
    """Живое соединение с одним MCP-сервером.

    Сессия висит в фоновой задаче: войти в неё и выйти нужно в одной и той же
    задаче, иначе anyio ругается на чужой cancel scope. Остальной код работает
    с уже открытым клиентом из любой задачи — в том числе несколькими вызовами
    сразу.
    """

    def __init__(self, key: str, url: str) -> None:
        self.key = key      # имя сервера в реестре — приставка к его инструментам
        self.url = url
        self.info: dict | None = None
        self.tools: list[dict] = []
        self.error: str | None = None
        self._client: Client | None = None
        self._task: asyncio.Task | None = None
        self._stop: asyncio.Event | None = None

    def note(self, method: str, summary: str, detail: object = None, ok: bool = True) -> None:
        _note(self.key, method, summary, detail, ok)

    # --- жизненный цикл соединения -----------------------------------------

    @property
    def connected(self) -> bool:
        return self._client is not None

    async def _hold(self, ready: asyncio.Event) -> None:
        """Держать сессию открытой, пока не попросят закрыть."""
        try:
            # Тайм-аут с запасом: сводку в пайплайне пишет модель, и такой
            # tools/call длится дольше обычного похода в GitHub.
            async with Client(self.url, read_timeout_seconds=120) as client:
                self._client = client
                info = client.server_info
                self.info = {
                    "server": info.name if info else "?",
                    "version": (info.version or "—") if info else "—",
                    "protocol": client.protocol_version,
                    "instructions": client.instructions or "",
                }
                ready.set()
                await self._stop.wait()
        except Exception as e:
            self.error = _reason(e)
            logger.warning("MCP: соединение не установлено — %s", self.error)
        finally:
            self._client = None
            self.info = None
            ready.set()

    async def connect(self) -> None:
        if self.connected:
            return
        self.error = None
        self._stop = asyncio.Event()
        ready = asyncio.Event()
        self._task = asyncio.create_task(self._hold(ready))
        try:
            await asyncio.wait_for(ready.wait(), timeout=15)
        except TimeoutError:
            self.error = "сервер не ответил за 15 секунд"
        if not self.connected:
            self.note("initialize", "соединение не установлено: " + str(self.error),
                      {"url": self.url, "ошибка": self.error}, ok=False)
            raise RuntimeError(self.error or "соединение не установлено")
        self.note(
            "initialize",
            "сервер {0} · протокол {1}".format(self.info["server"], self.info["protocol"]),
            {"url": self.url, "serverInfo": self.info},
        )
        await self.list_tools()

    async def disconnect(self, reason: str = "по кнопке") -> None:
        if self._task is None:  # соединения не было — закрывать нечего
            return
        if self._stop is not None:
            self._stop.set()
        if self._task is not None:
            try:
                await self._task
            except Exception:  # сессию закрывают на уже выключенном сервере
                pass
        self._task = None
        self.tools = []
        self.note("shutdown", "соединение закрыто (" + reason + ")")

    # --- запросы к серверу --------------------------------------------------

    async def list_tools(self) -> list[dict]:
        """Запросить у сервера список инструментов (tools/list)."""
        if self._client is None:
            raise RuntimeError("соединение с MCP-сервером не установлено")
        result = await self._client.list_tools()
        self.tools = [
            {"name": t.name, "description": t.description or "", "schema": t.input_schema}
            for t in result.tools
        ]
        names = ", ".join(t["name"] for t in self.tools) or "нет ни одного"
        self.note(
            "tools/list",
            "{0} инструментов: {1}".format(len(self.tools), names),
            {"tools": [{"name": t["name"], "description": t["description"],
                        "inputSchema": t["schema"]} for t in self.tools]},
        )
        return self.tools

    async def call(self, name: str, arguments: dict) -> tuple[object, bool]:
        """Вызвать инструмент (tools/call): структурированный ответ и признак успеха."""
        if self._client is None:
            raise RuntimeError("соединение с MCP-сервером не установлено")
        result = await self._client.call_tool(name, arguments)
        payload = result.structured_content
        if payload is None:
            payload = "\n".join(getattr(c, "text", "") for c in result.content)
        # В строке журнала — начало аргументов: в пайплайне они бывают большими,
        # целиком они видны в раскрытой строке.
        args = json.dumps(arguments, ensure_ascii=False)
        self.note(
            "tools/call",
            "{0}({1})".format(name, args if len(args) <= 160 else args[:160] + "…"),
            {"инструмент": name, "аргументы": arguments, "результат": payload},
            ok=not result.is_error,
        )
        return payload, not result.is_error

    async def call_tool(self, name: str, arguments: dict) -> str:
        """Вызвать инструмент и вернуть результат текстом — так его читает модель."""
        payload, _ = await self.call(name, arguments)
        return payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str)

    def snapshot(self) -> dict:
        return {
            "key": self.key,
            "connected": self.connected,
            "url": self.url,
            "info": self.info,
            "error": self.error,
            "tools": [{"name": t["name"], "description": t["description"]} for t in self.tools],
        }


# Соединения по реестру серверов: имя → живая сессия.
links: dict[str, McpLink] = {key: McpLink(key, url) for key, url in config.MCP_SERVERS.items()}


async def connect_all() -> None:
    """Подключиться ко всем серверам реестра, которые ещё не подключены.

    Сервер, до которого достучаться не вышло, не мешает остальным: причина
    остаётся у его соединения (`link.error`) и строкой в журнале.
    """
    for link in links.values():
        if not link.connected:
            try:
                await link.connect()
            except Exception:
                pass


async def disconnect_all(reason: str = "по кнопке") -> None:
    for link in links.values():
        await link.disconnect(reason)


async def refresh_all() -> str | None:
    """Перезапросить tools/list у всех подключённых серверов."""
    errors = []
    for link in links.values():
        if link.connected:
            try:
                await link.list_tools()
            except Exception as e:
                errors.append("{0}: {1}".format(link.key, _reason(e)))
    return "; ".join(errors) or None


_client: OpenAI | None = None


def _model_client() -> OpenAI:
    global _client
    if _client is None:
        if not config.DASHSCOPE_API_KEY:
            raise RuntimeError("DASHSCOPE_API_KEY не задан: скопируйте .env.example в .env и впишите ключ")
        _client = OpenAI(api_key=config.DASHSCOPE_API_KEY, base_url=config.DASHSCOPE_BASE_URL)
    return _client


def _call_model(messages: list[dict], tools: list[dict]):
    """Один вызов модели. Клиент синхронный — зовём его из отдельного потока.

    `parallel_tool_calls` разрешает модели за один ход попросить несколько
    независимых вызовов: шесть пакетов проверяются за один круг, а не за шесть.
    """
    extra = {"tools": tools, "tool_choice": "auto", "parallel_tool_calls": True} if tools else {}
    return _model_client().chat.completions.create(
        model=config.MODEL, messages=messages, **extra,
    )


def _tools_for_model() -> list[dict]:
    """Инструменты всех подключённых серверов в формате function calling.

    Имя для модели — «сервер__инструмент»: по приставке агент потом и решает,
    какому серверу отдать вызов.
    """
    return [
        {"type": "function",
         "function": {"name": link.key + SEP + t["name"], "description": t["description"],
                      "parameters": t["schema"]}}
        for link in links.values() if link.connected
        for t in link.tools
    ]


def _system() -> str:
    """Системный промпт: общие правила плюс то, что о себе сказали подключённые серверы."""
    up = [link for link in links.values() if link.connected]
    if not up:
        return SYSTEM + NO_TOOLS_NOTE
    lines = [SYSTEM, "", "Подключённые MCP-серверы — что они сказали о себе при подключении:"]
    lines += ["• {0}: {1}".format(link.key, link.info["instructions"] or "без описания") for link in up]
    down = [link.key for link in links.values() if not link.connected]
    if down:
        lines.append("Не подключены: {0}. Если для ответа нужны их инструменты — скажи "
                     "об этом прямо.".format(", ".join(down)))
    return "\n".join(lines)


class RouteError(RuntimeError):
    """Вызов некуда отправить — причина уже человеческими словами."""


def _route(function: str) -> tuple[McpLink, str]:
    """Маршрутизация вызова: приставка до «__» — сервер, остальное — инструмент."""
    key, sep, tool = function.partition(SEP)
    link = links.get(key)
    if not sep or link is None:
        raise RouteError("инструмента {0} нет ни на одном сервере из реестра".format(function))
    if not link.connected:
        raise RouteError("сервер {0} не подключён".format(key))
    return link, tool


async def _run_call(call: dict) -> str:
    """Один вызов из хода модели: отдать нужному серверу и вернуть ответ текстом."""
    try:
        link, tool = _route(call["function"])
        payload, ok = await link.call(tool, call["arguments"])
        # Ошибку GitHub или PyPI сервер отдаёт словами внутри ответа — для
        # маршрута это тоже неудавшийся шаг.
        ok = ok and not (isinstance(payload, dict) and "ошибка" in payload)
        text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str)
    except Exception as e:
        reason = str(e) if isinstance(e, RouteError) else _reason(e)
        _note(call["server"], "tools/call", "{0} — {1}".format(call["name"], reason),
              {"инструмент": call["function"], "аргументы": call["arguments"], "ошибка": reason},
              ok=False)
        text, ok = "ОШИБКА вызова инструмента: " + reason, False
    call.update(state="done" if ok else "error", ok=ok)
    _touch()
    return text


async def ask(text: str) -> dict:
    """Обращение к агенту: модель плюс круги вызовов инструментов через MCP."""
    global working
    history.append({"role": "user", "text": text})

    tools = _tools_for_model()
    messages: list[dict] = [{"role": "system", "content": _system()}]
    messages += [{"role": m["role"], "content": m["text"]} for m in history]

    # Маршрут обращения: каждый вызов — с сервером, инструментом и кругом, на
    # котором модель его попросила. Интерфейс видит его, пока агент работает.
    calls: list[dict] = []
    working = {"started": time.time(), "calls": calls, "phase": "model"}
    _touch()
    tokens = 0
    started = time.perf_counter()
    answer = "Не уложился в отведённые круги работы с инструментами."

    try:
        for round_no in range(config.TOOL_ROUNDS):
            response = await asyncio.to_thread(_call_model, messages, tools)
            if response.usage is not None:
                tokens += response.usage.total_tokens
            message = response.choices[0].message

            if not message.tool_calls:
                answer = (message.content or "").strip() or "Модель вернула пустой ответ."
                break

            messages.append({
                "role": "assistant",
                "content": message.content or "",
                "tool_calls": [
                    {"id": tc.id, "type": "function",
                     "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                    for tc in message.tool_calls
                ],
            })
            batch = []
            for tc in message.tool_calls:
                try:
                    arguments = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    arguments = {}
                server, sep, tool = tc.function.name.partition(SEP)
                batch.append({
                    "round": round_no,
                    "server": server if sep else "?",
                    "name": tool if sep else tc.function.name,
                    "function": tc.function.name,
                    "arguments": arguments if isinstance(arguments, dict) else {},
                    "state": "run",
                    "ok": None,
                })
            calls.extend(batch)
            working["phase"] = "tools"
            _touch()
            # Вызовы одного хода друг от друга не зависят — идут параллельно, каждый
            # на свой сервер. Ответы возвращаются модели в том же порядке.
            results = await asyncio.gather(*(_run_call(call) for call in batch))
            for tc, result in zip(message.tool_calls, results):
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
            working["phase"] = "model"
            _touch()
    finally:
        working = None

    reply = {
        "role": "assistant",
        "text": answer,
        "calls": [{key: call[key] for key in ("round", "server", "name", "arguments", "ok")}
                  for call in calls],
        "tokens": tokens,
        "seconds": round(time.perf_counter() - started, 1),
    }
    history.append(reply)
    _touch()
    return reply


REPORT_SYSTEM = (
    "Ты пишешь регулярную сводку по репозиторию на GitHub для команды. На входе — "
    "агрегированный ответ инструмента watch_summary. Напиши 2–5 коротких строк "
    "по-русски: что изменилось, кто автор, сколько. Хеши коммитов и номера задач "
    "оставляй. Без вступлений и без советов; ничего сверх данных не придумывай."
)


class AutoReport:
    """Агент, который работает сам: раз в N минут спрашивает сводку у сервера.

    Цикл живёт фоновой задачей и не зависит от того, открыт ли браузер. Каждый
    круг — вызов watch_summary по MCP (виден в журнале). Модель зовётся, только
    если с прошлой сводки что-то изменилось: тишина квоту не тратит.
    """

    def __init__(self) -> None:
        self.every = 0            # минут; 0 — выключена
        self.last_id = 0          # последнее событие, о котором уже рассказали
        self.since: datetime | None = None   # с какого момента собирать события
        self.status = "выключена"
        self.next_ts: float | None = None
        self._task: asyncio.Task | None = None

    def _say(self, status: str) -> None:
        self.status = status
        _touch()

    def set(self, minutes: int) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
        self.every = minutes
        self.next_ts = None
        if minutes:
            self._task = asyncio.create_task(self._loop())
            self._say("включена: сводка каждые {0} мин".format(minutes))
        else:
            self._say("выключена")

    async def _loop(self) -> None:
        while True:
            self.next_ts = time.time() + self.every * 60
            _touch()
            await asyncio.sleep(self.every * 60)
            await self.run_once()

    async def run_once(self) -> None:
        """Один круг: спросить сводку и, если есть новости, написать её в чат."""
        now = datetime.now().strftime("%H:%M")
        link = links["github"]   # наблюдение живёт на сервере GitHub
        if not link.connected:
            self._say("{0} — пропуск: нет соединения с сервером github".format(now))
            return
        if "watch_summary" not in {t["name"] for t in link.tools}:
            self._say("{0} — пропуск: инструмент watch_summary выключен на сервере".format(now))
            return

        started = time.perf_counter()
        # Первая сводка — за последний час, дальше — с момента прошлой сводки.
        minutes = 60 if self.since is None else max(1, min(
            10080, int((datetime.now() - self.since).total_seconds() // 60) + 1))
        arguments = {"minutes": minutes, "after_id": self.last_id}
        try:
            raw = await link.call_tool("watch_summary", arguments)
            data = json.loads(raw)
        except Exception as e:
            self._say("{0} — ошибка: {1}".format(now, _reason(e)))
            return
        if not data.get("есть изменения"):
            self.last_id = data.get("последнее событие №", self.last_id)
            self._say("{0} — изменений нет, модель не вызывалась".format(now))
            return

        try:
            response = await asyncio.to_thread(_call_model, [
                {"role": "system", "content": REPORT_SYSTEM},
                {"role": "user", "content": raw},
            ], [])
        except Exception as e:  # модель недоступна — события не теряем, скажем в следующий раз
            self._say("{0} — модель не ответила: {1}".format(now, e))
            return
        self.last_id = data.get("последнее событие №", self.last_id)
        self.since = datetime.now()
        history.append({
            "role": "assistant",
            "kind": "report",
            "time": now,
            "text": (response.choices[0].message.content or "").strip() or "Модель вернула пустую сводку.",
            "calls": [{"round": 0, "server": "github", "name": "watch_summary",
                       "arguments": arguments, "ok": True}],
            "tokens": response.usage.total_tokens if response.usage else 0,
            "seconds": round(time.perf_counter() - started, 1),
        })
        self._say("{0} — сводка отправлена в чат".format(now))

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def snapshot(self) -> dict:
        return {"every": self.every, "status": self.status, "next_ts": self.next_ts}


report = AutoReport()


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Число со словом в нужной форме: 1 файл, 2 файла, 5 файлов."""
    if n % 10 == 1 and n % 100 != 11:
        word = one
    elif 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        word = few
    else:
        word = many
    return "{0} {1}".format(n, word)


class Pipeline:
    """Пайплайн из трёх инструментов MCP: поиск → сводка → файл.

    Цепочку ведёт код агента, а не модель: каждый шаг — отдельный tools/call по
    протоколу, и ответ шага уходит на вход следующему. Сервер на каждом шаге
    сообщает отпечаток того, что получил; совпал с отпечатком отправленного —
    данные дошли без потерь, не совпал — цепочка останавливается.
    """

    # Инструмент, название шага, что он сделает и что берёт → что отдаёт.
    STEPS = (
        ("search_code", "Поиск", "найдёт строки в коде", "запрос → совпадения"),
        ("summarize", "Сводка", "модель опишет найденное", "совпадения → отчёт"),
        ("save_to_file", "Файл", "отчёт ляжет на диск", "отчёт → файл .md"),
    )

    def __init__(self) -> None:
        self.query = ""
        self.running = False
        self.status = ""                # итог прошлого прогона одной строкой
        self.file: str | None = None    # имя сохранённого отчёта
        self.steps = self._fresh()
        self._task: asyncio.Task | None = None

    def _fresh(self) -> list[dict]:
        return [{"tool": tool, "title": title, "state": "idle", "text": hint, "sub": flow,
                 "check": None, "started": None, "seconds": None}
                for tool, title, hint, flow in self.STEPS]

    def start(self, query: str) -> None:
        """Запустить цепочку фоном: интерфейс видит, как шаги проходят по очереди."""
        if self.running:
            raise RuntimeError("пайплайн уже выполняется")
        if not links["github"].connected:
            raise RuntimeError("нет соединения с сервером github — нажмите «Подключиться»")
        self.query, self.running, self.status, self.file = query, True, "", None
        self.steps = self._fresh()
        _touch()
        self._task = asyncio.create_task(self._run())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def _set(self, index: int, **fields) -> None:
        self.steps[index].update(fields)
        _touch()

    def _stop(self, index: int, reason: str) -> None:
        """Шаг не удался: следующие не выполняются, причина — в карточке шага."""
        reason = reason if len(reason) <= 160 else reason[:160] + "…"
        self._set(index, state="error", text=reason)
        for step in self.steps[index + 1:]:
            step.update(state="skip", text="не выполнялся")
        self.status = "остановлен на шаге {0}".format(index + 1)

    async def _call(self, index: int, arguments: dict) -> dict | None:
        """Один шаг — один tools/call. None — шаг не удался и цепочка встала."""
        tool = self.steps[index]["tool"]
        self._set(index, state="run", text="выполняется…", started=time.time())
        link = links["github"]   # все три шага живут на сервере GitHub
        if not link.connected:
            self._stop(index, "нет соединения с сервером github")
            return None
        if tool not in {t["name"] for t in link.tools}:
            self._stop(index, "инструмент {0} выключен на сервере".format(tool))
            return None
        started = time.perf_counter()
        try:
            payload, ok = await link.call(tool, arguments)
        except Exception as e:
            self._stop(index, _reason(e))
            return None
        self.steps[index]["seconds"] = round(time.perf_counter() - started, 1)
        if not isinstance(payload, dict):  # ошибку валидации сервер присылает текстом
            self._stop(index, str(payload) or "сервер вернул ошибку")
            return None
        if not ok or "ошибка" in payload:
            self._stop(index, str(payload.get("ошибка") or "сервер вернул ошибку"))
            return None
        return payload

    async def _run(self) -> None:
        at = datetime.now().strftime("%H:%M")
        try:
            await self._chain(at)
        except Exception as e:  # ответ неожиданной формы — не роняем задачу молча
            logger.exception("Сбой пайплайна")
            index = next((i for i, s in enumerate(self.steps) if s["state"] == "run"), 0)
            self._stop(index, "{0}: {1}".format(type(e).__name__, e))
        finally:
            self.running = False
            self.status = "{0} — {1}".format(at, self.status)
            _touch()

    async def _chain(self, at: str) -> None:
        started = time.perf_counter()

        # ① Поиск: первый инструмент получает данные — строки кода из GitHub.
        found = await self._call(0, {"query": self.query})
        if found is None:
            return
        count = found["совпадений"]
        self._set(0, state="done",
                  text="{0} в {1}".format(
                      _plural(count, "совпадение", "совпадения", "совпадений"),
                      _plural(found["файлов с совпадениями"], "файле", "файлах", "файлах"))
                  if count else "совпадений нет",
                  sub="просмотрено файлов: {0} · {1} с".format(
                      found["просмотрено файлов"], self.steps[0]["seconds"]))
        if not count:
            for step in self.steps[1:]:
                step.update(state="skip", text="сводить нечего")
            self.status = "«{0}» в коде не встречается".format(self.query)
            return

        # ② Сводка: второй обрабатывает — получает ответ поиска целиком.
        summary = await self._call(1, {"found": found})
        if summary is None:
            return
        got = summary["получено"]
        if got["отпечаток"] != found["отпечаток"]:
            self._set(1, check=False)
            self._stop(1, "данные изменились по дороге: отправлено {0}, получено {1}".format(
                found["отпечаток"], got["отпечаток"]))
            return
        self._set(1, state="done", check=True, text="сводка готова",
                  sub="получено {0} из {1} ✓ · {2} с".format(
                      got["совпадений"], len(found["совпадения"]), self.steps[1]["seconds"]))

        # ③ Файл: третий сохраняет — получает отчёт, который собрала сводка.
        saved = await self._call(2, {"name": self.query, "content": summary["отчёт"]})
        if saved is None:
            return
        got = saved["получено"]
        if got["отпечаток"] != summary["отпечаток"]:
            self._set(2, check=False)
            self._stop(2, "данные изменились по дороге: отправлено {0}, получено {1}".format(
                summary["отпечаток"], got["отпечаток"]))
            return
        self.file = PurePosixPath(saved["файл"]).name
        self._set(2, state="done", check=True, text=self.file,
                  sub="получено {0:.1f} КБ ✓ · {1} с".format(saved["байт"] / 1024, self.steps[2]["seconds"]))

        seconds = round(time.perf_counter() - started, 1)
        self.status = "готово за {0} с, сводка пришла в чат".format(seconds)
        history.append({
            "role": "assistant",
            "kind": "pipeline",
            "time": at,
            "query": self.query,
            "text": "{0}\n\nОтчёт сохранён: {1}".format(summary["сводка"], saved["файл"]),
            "calls": [
                {"round": 0, "server": "github", "name": "search_code",
                 "arguments": {"query": self.query}, "ok": True},
                {"round": 1, "server": "github", "name": "summarize",
                 "arguments": {"found": "ответ search_code целиком"}, "ok": True},
                {"round": 2, "server": "github", "name": "save_to_file",
                 "arguments": {"name": self.query, "content": "поле «отчёт»"}, "ok": True},
            ],
            "tokens": summary["токенов"],
            "seconds": seconds,
        })

    def snapshot(self) -> dict:
        return {"query": self.query, "running": self.running, "status": self.status,
                "file": self.file, "steps": self.steps}


pipeline = Pipeline()


def clear_history() -> None:
    history.clear()
    _touch()


def snapshot() -> dict:
    return {"revision": revision, "messages": history,
            "links": [link.snapshot() for link in links.values()], "log": journal,
            "working": working, "model": config.MODEL,
            "report": report.snapshot(), "pipeline": pipeline.snapshot()}
