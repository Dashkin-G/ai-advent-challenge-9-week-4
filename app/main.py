"""Одно приложение — два сервиса на одном порту.

  * MCP-сервер GitHub — смонтирован на /mcp (транспорт streamable HTTP);
  * чат с агентом     — интерфейс на / и небольшой HTTP-API на /api.

Агент ходит в MCP-сервер по сети, как ходил бы любой внешний клиент, а сервер
за каждым ответом ходит в api.github.com.

Запуск: python -m app.main  →  http://127.0.0.1:8000
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

from . import agent, config
from . import github_api as gh
from . import mcp_server as srv
from .mcp_server import mcp

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

# Приложение MCP-сервера: свой ASGI-роут, который мы подвешиваем к общему порту.
mcp_app = mcp.streamable_http_app(streamable_http_path="/")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # У смонтированного приложения свой lifespan не запускается, поэтому
    # менеджер сессий MCP поднимаем здесь — без него /mcp отвечает ошибкой.
    async with mcp.session_manager.run():
        logger.info("MCP-сервер GitHub слушает на %s", config.MCP_URL)
        logger.info("Репозиторий под сервером: %s", config.GITHUB_REPO)
        # Сводку для правой панели тянем фоном: старт приложения не должен
        # ждать, пока ответит GitHub.
        warmup = asyncio.create_task(srv.refresh_overview())
        yield
        warmup.cancel()
        await agent.link.disconnect("остановка приложения")
        await gh.close()


app = FastAPI(title="MCP GitHub + агент", lifespan=lifespan)
app.mount(config.MCP_PATH, mcp_app)


@app.middleware("http")
async def mcp_power_switch(request, call_next):
    """Выключатель сервера из правой панели: выключен — /mcp отвечает отказом.

    Отказ отдаём телом JSON-RPC: клиент MCP разбирает его и показывает причину
    словами, а не «внутренней ошибкой сервера».
    """
    if request.url.path.startswith(config.MCP_PATH):
        if not srv.server_on:
            return JSONResponse(
                {"jsonrpc": "2.0", "id": 0,
                 "error": {"code": -32000, "message": "MCP-сервер выключен в правой панели"}},
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
    return {"agent": agent.snapshot(), "server": srv.snapshot(),
            "revision": agent.revision + srv.revision}


def _done(error: str | None = None) -> dict:
    """Ответ на действие: интерфейс перечитает состояние сам."""
    return {"ok": error is None, "error": error}


# --- левая половина: агент ---------------------------------------------------

@app.post("/api/mcp/connect")
async def mcp_connect() -> dict:
    try:
        await agent.link.connect()
    except Exception as e:
        return _done(str(e))
    return _done()


@app.post("/api/mcp/disconnect")
async def mcp_disconnect() -> dict:
    await agent.link.disconnect()
    return _done()


@app.post("/api/mcp/refresh")
async def mcp_refresh() -> dict:
    """Перезапросить список инструментов у сервера (tools/list)."""
    try:
        await agent.link.list_tools()
    except Exception as e:
        return _done(str(e))
    return _done()


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


# --- правая половина: сервер, его инструменты и сам репозиторий -------------

class Switch(BaseModel):
    on: bool


class ToolSwitch(BaseModel):
    name: str
    on: bool


@app.post("/api/server")
async def server_switch(req: Switch) -> dict:
    # Соединение закрываем до выключения: иначе прощальный запрос клиента
    # упрётся в 503 и сессия останется висеть на сервере.
    if not req.on and agent.link.connected:
        await agent.link.disconnect("MCP-сервер выключен")
    srv.set_server(req.on)
    return _done()


@app.post("/api/tool")
async def tool_switch(req: ToolSwitch) -> dict:
    srv.set_tool(req.name, req.on)
    # Набор инструментов на сервере изменился — подключённый агент тут же
    # перезапрашивает tools/list и показывает новый список.
    if agent.link.connected:
        try:
            await agent.link.list_tools()
        except Exception as e:
            return _done(str(e))
    return _done()


@app.post("/api/repo/refresh")
async def repo_refresh() -> dict:
    """Перечитать репозиторий для правой панели (тот же GitHub, но для глаз)."""
    return _done(await srv.refresh_overview())


if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT, log_level="info")
