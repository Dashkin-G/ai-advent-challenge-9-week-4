"""MCP-сервер «CRM»: данные клиентов и инструменты поверх них.

Это первая половина проекта. Сервер поднимается внутри того же процесса, что и
чат (см. `main.py`), но общается с агентом по протоколу MCP — транспорт
streamable HTTP на `/mcp`. Данные живут в памяти процесса: перезапуск
возвращает их к исходным, и это удобно для показа.

Сервер можно выключить целиком и можно выключить любой отдельный инструмент —
тогда агент перестаёт его видеть в ответе `tools/list`.
"""
from collections.abc import Callable
from typing import Any

from mcp.server.mcpserver import MCPServer

STATUSES = ["новый", "в работе", "оплачен", "отказ"]

# Номер состояния: растёт на каждое изменение данных или настроек сервера.
# По нему интерфейс понимает, что пора перерисовать правую панель.
revision = 0

# Выключатель всего сервера. Проверяется в `main.py` перед тем, как пустить
# запрос на /mcp: выключенный сервер отвечает 503, и агент честно теряет связь.
server_on = True

clients: list[dict] = []
_next_id = 1


def _touch() -> None:
    global revision
    revision += 1


def reset() -> None:
    """Вернуть исходные данные CRM (кнопка «Сбросить» в панели оператора)."""
    global clients, _next_id
    clients = [
        {"id": 1, "name": "ООО «Ромашка»", "status": "в работе", "amount": 120000,
         "note": "поставка кофе в офис, ждут счёт"},
        {"id": 2, "name": "ИП Соколов", "status": "новый", "amount": 45000,
         "note": "пришёл с сайта, нужен звонок"},
        {"id": 3, "name": "ООО «Вектор»", "status": "оплачен", "amount": 310000,
         "note": "годовой договор, оплата прошла"},
    ]
    _next_id = 4
    _touch()


reset()


def find(client_id: int) -> dict | None:
    return next((c for c in clients if c["id"] == client_id), None)


def add_client(name: str, amount: int = 0, note: str = "") -> dict:
    """Завести клиента. Общая точка для инструмента агента и кнопки оператора."""
    global _next_id
    client = {"id": _next_id, "name": name.strip() or f"Клиент {_next_id}",
              "status": "новый", "amount": int(amount), "note": note}
    clients.append(client)
    _next_id += 1
    _touch()
    return client


def change_status(client_id: int, status: str) -> dict:
    """Сменить статус клиента. Тоже общая точка для агента и для оператора."""
    client = find(client_id)
    if client is None:
        return {"ошибка": f"клиента №{client_id} нет в CRM"}
    if status not in STATUSES:
        return {"ошибка": f"статус «{status}» недопустим",
                "допустимые": STATUSES}
    was = client["status"]
    client["status"] = status
    _touch()
    return {"клиент": client["name"], "было": was, "стало": status}


# --- Инструменты, которые сервер отдаёт агенту -------------------------------
# Возвращают простые словари: они уедут к модели как JSON, и русские ключи
# читаются ею так же хорошо, как английские.

def tool_list_clients(status: str | None = None) -> dict:
    rows = [c for c in clients if status is None or c["status"] == status]
    return {"всего": len(rows), "клиенты": rows}


def tool_get_client(client_id: int) -> dict:
    client = find(client_id)
    return client or {"ошибка": f"клиента №{client_id} нет в CRM"}


def tool_create_client(name: str, amount: int = 0) -> dict:
    return {"создан": add_client(name, amount)}


def tool_set_status(client_id: int, status: str) -> dict:
    return change_status(client_id, status)


def tool_stats() -> dict:
    by_status = {s: {"клиентов": 0, "сумма": 0} for s in STATUSES}
    for c in clients:
        row = by_status[c["status"]]
        row["клиентов"] += 1
        row["сумма"] += c["amount"]
    return {"клиентов всего": len(clients),
            "сумма всех сделок": sum(c["amount"] for c in clients),
            "по статусам": by_status}


# Имя → функция и описание. Описание уходит модели как есть, поэтому в нём
# сразу сказано, что инструмент делает и какие значения допустимы.
TOOL_SPECS: dict[str, tuple[Callable[..., Any], str]] = {
    "list_clients": (
        tool_list_clients,
        "Список клиентов CRM с их статусом и суммой сделки. Необязательный "
        "параметр status фильтрует список; допустимые значения: "
        "новый, в работе, оплачен, отказ.",
    ),
    "get_client": (
        tool_get_client,
        "Карточка одного клиента по его номеру (client_id): имя, статус, "
        "сумма сделки и заметка менеджера.",
    ),
    "create_client": (
        tool_create_client,
        "Завести в CRM нового клиента: name — название, amount — сумма сделки "
        "в рублях. Новый клиент получает статус «новый».",
    ),
    "set_status": (
        tool_set_status,
        "Сменить статус клиента с номером client_id. Допустимые значения "
        "status: новый, в работе, оплачен, отказ.",
    ),
    "stats": (
        tool_stats,
        "Сводка по CRM: сколько всего клиентов, общая сумма сделок и разбивка "
        "по каждому статусу.",
    ),
}

mcp = MCPServer(
    name="crm",
    version="1.0",
    instructions="CRM отдела продаж: клиенты, статусы сделок и суммы.",
)

# Какие инструменты сейчас включены. Выключенный снимается с сервера целиком,
# поэтому в ответе tools/list его нет — агент о нём даже не знает.
enabled: dict[str, bool] = {name: True for name in TOOL_SPECS}
for _name, (_fn, _description) in TOOL_SPECS.items():
    mcp.add_tool(_fn, name=_name, description=_description)


def set_tool(name: str, on: bool) -> None:
    """Включить или выключить инструмент на сервере."""
    if name not in TOOL_SPECS or enabled[name] == on:
        return
    if on:
        fn, description = TOOL_SPECS[name]
        mcp.add_tool(fn, name=name, description=description)
    else:
        mcp.remove_tool(name)
    enabled[name] = on
    _touch()


def set_server(on: bool) -> None:
    """Включить или выключить весь MCP-сервер."""
    global server_on
    if server_on != on:
        server_on = on
        _touch()


def snapshot() -> dict:
    """Состояние сервера для правой панели интерфейса."""
    return {
        "revision": revision,
        "server_on": server_on,
        "clients": clients,
        "statuses": STATUSES,
        "tools": [
            {"name": name, "description": description, "on": enabled[name]}
            for name, (_fn, description) in TOOL_SPECS.items()
        ],
    }
