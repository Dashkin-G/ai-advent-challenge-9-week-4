"""Агент с чатом: MCP-клиент плюс вызов модели.

Это вторая половина проекта. Агент подключается к MCP-серверу официальным
клиентом из SDK (`mcp.Client`) по сети, получает от него список инструментов и
отдаёт этот список модели. Всё, что уходит по протоколу и приходит обратно,
попадает в журнал обмена — его видно в интерфейсе.

Кроме ответов на вопросы агент умеет работать сам: `AutoReport` раз в N минут
спрашивает у сервера сводку наблюдения и присылает её в чат.
"""
import asyncio
import json
import logging
import time
from datetime import datetime

from mcp import Client
from openai import OpenAI

from . import config

logger = logging.getLogger("app.agent")

SYSTEM = (
    "Ты — ассистент по репозиторию на GitHub. Отвечай по-русски, коротко и по делу.\n"
    "Всё о репозитории — коммиты, файлы, задачи — ты узнаёшь ТОЛЬКО через "
    "инструменты MCP-сервера. Ничего не придумывай: нет инструмента — так и скажи.\n"
    "Если нужен хеш коммита или путь к файлу, сначала возьми список, а потом "
    "запрашивай подробности. Опирайся на то, что вернули инструменты: хеши, "
    "даты, имена файлов, номера задач.\n"
    "Если просят следить за репозиторием или присылать сводку — заведи наблюдение "
    "инструментом watch_repo. На вопросы «что произошло за час / за день» отвечай "
    "по watch_summary."
)
NO_TOOLS_NOTE = (
    "\nСейчас соединение с MCP-сервером не установлено и инструментов у тебя нет. "
    "Скажи об этом прямо и предложи нажать «Подключиться»."
)

# Переписка живёт в памяти процесса: перезапуск начинает разговор заново.
history: list[dict] = []
revision = 0


def _touch() -> None:
    global revision
    revision += 1


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
    """Живое соединение с MCP-сервером.

    Сессия висит в фоновой задаче: войти в неё и выйти нужно в одной и той же
    задаче, иначе anyio ругается на чужой cancel scope. Остальной код работает
    с уже открытым клиентом из любой задачи.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self.info: dict | None = None
        self.tools: list[dict] = []
        self.error: str | None = None
        self.log: list[dict] = []
        self._client: Client | None = None
        self._task: asyncio.Task | None = None
        self._stop: asyncio.Event | None = None

    # --- журнал обмена ------------------------------------------------------

    def note(self, method: str, summary: str, detail: object = None, ok: bool = True) -> None:
        detail_text = ""
        if detail is not None:
            detail_text = json.dumps(detail, ensure_ascii=False, indent=2, default=str)
        self.log.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "method": method,
            "summary": summary,
            "ok": ok,
            "detail": detail_text,
        })
        del self.log[:-40]  # на экране нужна свежая часть, а не весь день
        _touch()

    # --- жизненный цикл соединения -----------------------------------------

    @property
    def connected(self) -> bool:
        return self._client is not None

    async def _hold(self, ready: asyncio.Event) -> None:
        """Держать сессию открытой, пока не попросят закрыть."""
        try:
            async with Client(self.url, read_timeout_seconds=30) as client:
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

    async def call_tool(self, name: str, arguments: dict) -> str:
        """Вызвать инструмент (tools/call) и вернуть результат текстом для модели."""
        if self._client is None:
            raise RuntimeError("соединение с MCP-сервером не установлено")
        result = await self._client.call_tool(name, arguments)
        payload = result.structured_content
        if payload is None:
            payload = "\n".join(getattr(c, "text", "") for c in result.content)
        text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str)
        self.note(
            "tools/call",
            "{0}({1})".format(name, json.dumps(arguments, ensure_ascii=False)),
            {"инструмент": name, "аргументы": arguments, "результат": payload},
            ok=not result.is_error,
        )
        return text

    def snapshot(self) -> dict:
        return {
            "connected": self.connected,
            "url": self.url,
            "info": self.info,
            "error": self.error,
            "tools": [{"name": t["name"], "description": t["description"]} for t in self.tools],
            "log": self.log,
        }


link = McpLink(config.MCP_URL)

_client: OpenAI | None = None


def _model_client() -> OpenAI:
    global _client
    if _client is None:
        if not config.DASHSCOPE_API_KEY:
            raise RuntimeError("DASHSCOPE_API_KEY не задан: скопируйте .env.example в .env и впишите ключ")
        _client = OpenAI(api_key=config.DASHSCOPE_API_KEY, base_url=config.DASHSCOPE_BASE_URL)
    return _client


def _call_model(messages: list[dict], tools: list[dict]):
    """Один вызов модели. Клиент синхронный — зовём его из отдельного потока."""
    extra = {"tools": tools, "tool_choice": "auto"} if tools else {}
    return _model_client().chat.completions.create(
        model=config.MODEL, messages=messages, **extra,
    )


def _tools_for_model() -> list[dict]:
    """Инструменты MCP в формате function calling — как их видит модель."""
    return [
        {"type": "function",
         "function": {"name": t["name"], "description": t["description"], "parameters": t["schema"]}}
        for t in link.tools
    ]


async def ask(text: str) -> dict:
    """Обращение к агенту: модель плюс круги вызовов инструментов через MCP."""
    history.append({"role": "user", "text": text})
    _touch()

    tools = _tools_for_model()
    system = SYSTEM if tools else SYSTEM + NO_TOOLS_NOTE
    messages: list[dict] = [{"role": "system", "content": system}]
    messages += [{"role": m["role"], "content": m["text"]} for m in history]

    calls: list[dict] = []
    tokens = 0
    started = time.perf_counter()
    answer = "Не уложился в отведённые круги работы с инструментами."

    for _ in range(config.TOOL_ROUNDS):
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
        for tc in message.tool_calls:
            try:
                arguments = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                arguments = {}
            try:
                result = await link.call_tool(tc.function.name, arguments)
                ok = True
            except Exception as e:
                reason = _reason(e)
                result = "ОШИБКА вызова инструмента: " + reason
                ok = False
                link.note("tools/call", "{0} — {1}".format(tc.function.name, reason),
                          {"инструмент": tc.function.name, "аргументы": arguments, "ошибка": reason},
                          ok=False)
            calls.append({"name": tc.function.name, "arguments": arguments, "ok": ok})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

    reply = {
        "role": "assistant",
        "text": answer,
        "calls": calls,
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
        if not link.connected:
            self._say("{0} — пропуск: нет соединения с MCP-сервером".format(now))
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
            "calls": [{"name": "watch_summary", "arguments": arguments, "ok": True}],
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


def clear_history() -> None:
    history.clear()
    _touch()


def snapshot() -> dict:
    return {"revision": revision, "messages": history, "mcp": link.snapshot(),
            "model": config.MODEL, "report": report.snapshot()}
