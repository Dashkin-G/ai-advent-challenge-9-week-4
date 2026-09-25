"""Одно приложение — несколько сервисов на одном порту.

  * MCP-сервер GitHub       — смонтирован на /mcp/github (транспорт streamable HTTP);
  * MCP-сервер зависимостей — на /mcp/deps, за ним PyPI и OSV.dev;
  * чат с агентом           — интерфейс на / и небольшой HTTP-API на /api.

Агент ходит в оба сервера по сети, как ходил бы любой внешний клиент, и сам
решает, какой вызов отдать какому серверу. Фоном всё время работают
планировщик наблюдения (watcher.py) и, если включена, автосводка агента; по
кнопке агент проводит пайплайн «поиск → сводка → файл».

Запуск: python -m app.main  →  http://127.0.0.1:8000
"""
import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

from . import agent, config, deps_api, reports, watcher
from . import github_api as gh
from . import mcp_server as srv

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("app")

INDEX = Path(__file__).resolve().parent.parent / "static" / "index.html"

# Что видит человек, открывший адрес MCP-сервера в браузере.
BROWSER_HINT = """<!doctype html><html lang="ru"><meta charset="utf-8">
<title>Это адрес MCP-сервера</title>
<body style="margin:0;display:grid;place-items:center;height:100vh;background:#9fb086;
             color:#1b2417;font:17px/1.6 system-ui,'Segoe UI',sans-serif">
<div style="max-width:520px;background:#b9c9a2;border:1px solid #7e9166;border-radius:14px;padding:28px 32px">
<h1 style="margin:0 0 10px;font-size:21px">Это адрес MCP-сервера</h1>
<p style="margin:0 0 14px">По нему разговаривают программы: MCP-клиент шлёт JSON-RPC и
получает ответ. Браузеру сервер отвечает «Missing session ID» — это не поломка.</p>
<p style="margin:0"><b>Интерфейс приложения — на <a href="/" style="color:#2f5a38">
http://127.0.0.1:8000/</a></b></p></div></body></html>"""

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        # У смонтированных приложений свой lifespan не запускается, поэтому
        # менеджеры сессий MCP поднимаем здесь — без них серверы отвечают ошибкой.
        for server in srv.SERVERS.values():
            await stack.enter_async_context(server.mcp.session_manager.run())
            logger.info("MCP-сервер %s слушает на %s", server.key, server.url)
        logger.info("Репозиторий под сервером github: %s", config.GITHUB_REPO)
        # Сводку для правой панели тянем фоном: старт приложения не должен
        # ждать, пока ответит GitHub.
        warmup = asyncio.create_task(srv.refresh_overview())
        # Планировщик наблюдения работает всё время жизни приложения; нашёл
        # новое — правая панель перечитывает репозиторий сама.
        watcher.on_news = srv.refresh_overview
        scheduler = asyncio.create_task(watcher.run_forever())
        yield
        warmup.cancel()
        scheduler.cancel()
        agent.report.stop()
        agent.pipeline.stop()
        await agent.disconnect_all("остановка приложения")
        await gh.close()
        await deps_api.close()


app = FastAPI(title="MCP-серверы GitHub и зависимостей + агент", lifespan=lifespan)
# Каждый сервер — свой ASGI-роут на своём адресе того же порта.
for _server in srv.SERVERS.values():
    app.mount(_server.path, _server.app)


def _server_at(path: str) -> srv.Server | None:
    """Какому MCP-серверу адресован запрос."""
    return next((s for s in srv.SERVERS.values()
                 if path == s.path or path.startswith(s.path + "/")), None)


@app.middleware("http")
async def mcp_power_switch(request, call_next):
    """Выключатель сервера из правой панели: выключен — его адрес отвечает отказом.

    Отказ отдаём телом JSON-RPC: клиент MCP разбирает его и показывает причину
    словами, а не «внутренней ошибкой сервера».
    """
    server = _server_at(request.url.path)
    if server is not None:
        if not server.on:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": 0,
                 "error": {"code": -32000,
                           "message": "MCP-сервер {0} выключен в правой панели".format(server.key)}},
                status_code=503,
            )
        # Человек, открывший этот адрес в браузере, получил бы «Missing session
        # ID»: сюда ходят MCP-клиенты, а не браузер. Объясняем и показываем путь.
        if request.method == "GET" and "text/html" in request.headers.get("accept", ""):
            return HTMLResponse(BROWSER_HINT)
    return await call_next(request)


# --- состояние для интерфейса ------------------------------------------------

@app.get("/")
def index() -> FileResponse:
    return FileResponse(INDEX)


@app.get("/api/state")
def state() -> dict:
    return {"agent": agent.snapshot(), "server": srv.snapshot(), "watch": watcher.snapshot(),
            "reports": reports.listing(),
            "revision": agent.revision + srv.revision + watcher.revision + reports.revision}


def _done(error: str | None = None) -> dict:
    """Ответ на действие: интерфейс перечитает состояние сам."""
    return {"ok": error is None, "error": error}


# --- левая половина: агент ---------------------------------------------------

@app.post("/api/mcp/connect")
async def mcp_connect() -> dict:
    """Подключиться ко всем серверам реестра. Кто не ответил — причина в его строке."""
    await agent.connect_all()
    return _done()


@app.post("/api/mcp/disconnect")
async def mcp_disconnect() -> dict:
    await agent.disconnect_all()
    return _done()


@app.post("/api/mcp/refresh")
async def mcp_refresh() -> dict:
    """Перезапросить список инструментов у всех подключённых серверов (tools/list)."""
    return _done(await agent.refresh_all())


class Ask(BaseModel):
    text: str


@app.post("/api/chat")
async def chat(req: Ask) -> dict:
    text = req.text.strip()
    if not text:
        return _done("пустой вопрос")
    try:
        return {"ok": True, "reply": await agent.ask(text)}
    except Exception as e:
        logger.warning("Ошибка обращения к агенту: %s", e)
        return _done("{0}: {1}".format(type(e).__name__, e))


@app.post("/api/chat/clear")
def chat_clear() -> dict:
    agent.clear_history()
    return _done()


class Every(BaseModel):
    minutes: int


@app.post("/api/report")
async def report_every(req: Every) -> dict:
    """Автосводка агента: раз в сколько минут (0 — выключить).

    Обработчик асинхронный намеренно: цикл автосводки заводится задачей в
    событийном цикле, а синхронный обработчик FastAPI выполнил бы в потоке.
    """
    agent.report.set(max(0, req.minutes))
    return _done()


@app.post("/api/report/now")
async def report_now() -> dict:
    await agent.report.run_once()
    return _done()


@app.post("/api/pipeline")
async def pipeline_run(req: Ask) -> dict:
    """Пайплайн «поиск → сводка → файл» по запросу. Ход шагов виден в /api/state.

    Обработчик асинхронный: цепочка заводится задачей в событийном цикле.
    """
    query = req.text.strip()
    if len(query) < 2:
        return _done("запрос короче двух символов")
    try:
        agent.pipeline.start(query)
    except RuntimeError as e:
        return _done(str(e))
    return _done()


@app.get("/api/reports/{name}")
def report_file(name: str):
    """Отчёт, который записал save_to_file, — открывается в браузере текстом."""
    path = reports.find(name)
    if path is None:
        return JSONResponse({"ok": False, "error": "такого отчёта нет"}, status_code=404)
    return FileResponse(path, media_type="text/plain; charset=utf-8")


@app.post("/api/reports/{name}/delete")
def report_delete(name: str) -> dict:
    return _done(None if reports.remove(name) else "такого отчёта нет")


# --- правая половина: серверы, их инструменты, репозиторий и пакеты ---------

class Switch(BaseModel):
    on: bool


class ServerSwitch(BaseModel):
    server: str
    on: bool


class ToolSwitch(BaseModel):
    server: str
    name: str
    on: bool


@app.post("/api/server")
async def server_switch(req: ServerSwitch) -> dict:
    server = srv.SERVERS.get(req.server)
    if server is None:
        return _done("такого сервера нет")
    # Соединение закрываем до выключения: иначе прощальный запрос клиента
    # упрётся в 503 и сессия останется висеть на сервере.
    link = agent.links.get(req.server)
    if not req.on and link is not None and link.connected:
        await link.disconnect("MCP-сервер выключен")
    server.set_on(req.on)
    return _done()


@app.post("/api/tool")
async def tool_switch(req: ToolSwitch) -> dict:
    server = srv.SERVERS.get(req.server)
    if server is None:
        return _done("такого сервера нет")
    server.set_tool(req.name, req.on)
    # Набор инструментов на сервере изменился — подключённый агент тут же
    # перезапрашивает tools/list и показывает новый список.
    link = agent.links.get(req.server)
    if link is not None and link.connected:
        try:
            await link.list_tools()
        except Exception as e:
            return _done(str(e))
    return _done()


@app.post("/api/packages/clear")
def packages_clear() -> dict:
    """Очистить карточку проверенных пакетов — например, перед показом с нуля."""
    srv.clear_packages()
    return _done()


@app.post("/api/repo/refresh")
async def repo_refresh() -> dict:
    """Перечитать репозиторий для правой панели (тот же GitHub, но для глаз)."""
    return _done(await srv.refresh_overview())


@app.post("/api/watch/{watch_id}/run")
async def watch_run(watch_id: int) -> dict:
    """Проверить репозиторий сейчас, не дожидаясь срока."""
    await watcher.collect(watch_id)
    return _done()


@app.post("/api/watch/{watch_id}/active")
async def watch_active(watch_id: int, req: Switch) -> dict:
    watcher.set_active(watch_id, req.on)
    return _done()


@app.post("/api/watch/{watch_id}/delete")
async def watch_delete(watch_id: int) -> dict:
    watcher.remove(watch_id)
    return _done()


if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT, log_level="info")
