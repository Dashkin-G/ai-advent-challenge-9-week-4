"""Агент с чатом: MCP-клиент плюс вызов модели.

Это вторая половина проекта. Агент подключается к MCP-серверу официальным
клиентом из SDK (`mcp.Client`) по сети, получает от него список инструментов и
отдаёт этот список модели. Всё, что уходит по протоколу и приходит обратно,
попадает в журнал обмена — его видно в интерфейсе.

Кроме ответов на вопросы агент умеет работать сам: `AutoReport` раз в N минут
спрашивает у сервера сводку наблюдения и присылает её в чат, а `Pipeline`
проводит цепочку из трёх инструментов «поиск → сводка → файл».
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
    "Ты — ассистент по репозиторию на GitHub. Отвечай по-русски, коротко и по делу.\n"
    "Всё о репозитории — коммиты, файлы, задачи — ты узнаёшь ТОЛЬКО через "
    "инструменты MCP-сервера. Ничего не придумывай: нет инструмента — так и скажи.\n"
    "Если нужен хеш коммита или путь к файлу, сначала возьми список, а потом "
    "запрашивай подробности. Опирайся на то, что вернули инструменты: хеши, "
    "даты, имена файлов, номера задач.\n"
    "Где в коде встречается слово или имя — ищи инструментом search_code.\n"
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
        if not link.connected:
            raise RuntimeError("нет соединения с MCP-сервером — нажмите «Подключиться»")
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
        if not link.connected:
            self._stop(index, "нет соединения с MCP-сервером")
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
                {"name": "search_code", "arguments": {"query": self.query}, "ok": True},
                {"name": "summarize", "arguments": {"found": "ответ search_code целиком"}, "ok": True},
                {"name": "save_to_file", "arguments": {"name": self.query, "content": "поле «отчёт»"},
                 "ok": True},
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
    return {"revision": revision, "messages": history, "mcp": link.snapshot(),
            "model": config.MODEL, "report": report.snapshot(), "pipeline": pipeline.snapshot()}
