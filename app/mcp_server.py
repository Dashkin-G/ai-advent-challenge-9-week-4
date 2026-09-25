"""MCP-серверы проекта: GitHub и зависимости Python.

Это первая половина проекта. Оба сервера поднимаются внутри того же процесса,
что и чат (см. `main.py`), но общаются с агентом по протоколу MCP — транспорт
streamable HTTP, у каждого свой адрес: /mcp/github/ и /mcp/deps/. Для агента
это два разных сервера: своя сессия, своё имя и свой список инструментов.

* **github** — репозиторий. Шесть инструментов делают живой запрос к
  api.github.com (см. `github_api.py`), три складываются в пайплайн «поиск →
  сводка → файл» (см. `reports.py`), а три инструмента наблюдения заводят
  задание по расписанию и отдают накопленную в SQLite сводку (см. `watcher.py`).
* **deps** — пакеты Python: последняя версия на PyPI и известные уязвимости из
  базы OSV.dev (см. `deps_api.py`).

Что здесь происходит по пунктам задания:

* **регистрация инструмента** — `mcp.add_tool(...)` в конструкторе `Server`;
* **описание входных параметров** — аннотации `Annotated[..., Field(...)]` у
  функций: из них SDK сам собирает JSON-схему, которая уезжает агенту в ответе
  `tools/list` и дальше модели;
* **возврат результата** — функции отдают обычный словарь, SDK превращает его
  в структурированный ответ `tools/call`.

Любой сервер можно выключить целиком и можно выключить любой его инструмент —
тогда агент перестаёт его видеть в ответе `tools/list`.
"""
import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from . import config, deps_api
from . import github_api as gh
from . import reports, watcher

# Номер состояния: растёт на каждое изменение настроек серверов или панели.
# По нему интерфейс понимает, что пора перерисовать правую половину.
revision = 0

# Сводка по репозиторию для правой панели. Обновляется при старте и по кнопке:
# панель не должна ходить в GitHub на каждый опрос состояния.
overview: dict | None = None
overview_error: str | None = None
overview_time: str | None = None
overview_loading = False

# Что сервер «Зависимости» уже проверил — тоже для правой панели: строка на
# пакет, свежий ответ поверх прошлого. Живёт в памяти процесса.
packages: dict[str, dict[str, Any]] = {}


def _touch() -> None:
    global revision
    revision += 1


async def refresh_overview() -> str | None:
    """Перечитать репозиторий для правой панели. Возвращает текст ошибки или None."""
    global overview, overview_error, overview_time, overview_loading
    overview_loading = True
    _touch()
    try:
        overview = await gh.overview()
        overview_error = None
    except gh.GitHubError as e:
        overview_error = str(e)
    except Exception as e:  # сеть, прокси, неожиданный ответ — показываем как есть
        overview_error = "{0}: {1}".format(type(e).__name__, e)
    finally:
        overview_loading = False
        overview_time = datetime.now().strftime("%H:%M:%S")
        _touch()
    return overview_error


# --- Инструменты сервера GitHub ----------------------------------------------
# Каждый параметр описан прямо в аннотации: это описание уходит в JSON-схему,
# а оттуда — модели, поэтому пишем так, будто объясняем человеку. Возвращаем
# простые словари: они уедут к модели как JSON, и русские ключи читаются ею
# так же хорошо, как английские.

RepoArg = Annotated[str | None, Field(
    description="Репозиторий в виде «владелец/имя». Можно не указывать: "
                "по умолчанию берётся репозиторий из настроек сервера.")]
RefArg = Annotated[str | None, Field(
    description="Ветка, тег или хеш коммита. По умолчанию — ветка репозитория "
                "по умолчанию.")]


async def _guard(work: Awaitable[dict[str, Any]]) -> dict[str, Any]:
    """Ошибку GitHub или шага пайплайна отдаём агенту словами, а не падением."""
    try:
        return await work
    except (gh.GitHubError, reports.ReportError) as e:
        return {"ошибка": str(e)}


async def tool_repo_info(repo: RepoArg = None) -> dict[str, Any]:
    return await _guard(gh.repo_info(repo))


async def tool_list_commits(
    limit: Annotated[int, Field(
        ge=1, le=30,
        description="Сколько последних коммитов вернуть, от 1 до 30.")] = 5,
    branch: Annotated[str | None, Field(
        description="Ветка, из которой брать коммиты. По умолчанию — основная.")] = None,
    path: Annotated[str | None, Field(
        description="Путь к файлу или папке: тогда вернутся только коммиты, "
                    "которые их трогали. Например app/agent.py.")] = None,
    repo: RepoArg = None,
) -> dict[str, Any]:
    return await _guard(gh.list_commits(repo=repo, limit=limit, branch=branch, path=path))


async def tool_commit_details(
    sha: Annotated[str, Field(
        description="Хеш коммита, полный или короткий (например 7bf2895). "
                    "Берётся из ответа list_commits.")],
    repo: RepoArg = None,
) -> dict[str, Any]:
    return await _guard(gh.commit_details(sha, repo=repo))


async def tool_list_issues(
    state: Annotated[Literal["open", "closed", "all"], Field(
        description="Какие задачи показать: open — открытые, closed — "
                    "закрытые, all — любые.")] = "open",
    limit: Annotated[int, Field(
        ge=1, le=30, description="Сколько задач вернуть, от 1 до 30.")] = 10,
    repo: RepoArg = None,
) -> dict[str, Any]:
    return await _guard(gh.list_issues(repo=repo, state=state, limit=limit))


async def tool_list_files(
    path: Annotated[str, Field(
        description="Папка внутри репозитория, например app. Пустая строка — "
                    "корень репозитория.")] = "",
    ref: RefArg = None,
    repo: RepoArg = None,
) -> dict[str, Any]:
    return await _guard(gh.list_files(repo=repo, path=path, ref=ref))


async def tool_read_file(
    path: Annotated[str, Field(
        description="Путь к файлу от корня репозитория, например "
                    "app/mcp_server.py или README.md.")],
    ref: RefArg = None,
    repo: RepoArg = None,
) -> dict[str, Any]:
    return await _guard(gh.read_file(path, repo=repo, ref=ref))


# --- Пайплайн: поиск → сводка → файл -----------------------------------------
# Три инструмента складываются в цепочку: ответ одного целиком уходит на вход
# следующему. Каждый сообщает отпечаток того, что получил, — по нему агент
# проверяет, что данные дошли без потерь.

async def tool_search_code(
    query: Annotated[str, Field(
        min_length=2,
        description="Что искать: слово, имя функции или кусок строки, от 2 символов. "
                    "Регистр не важен.")],
    path: Annotated[str, Field(
        description="Искать только в этой папке или файле, например app. Пустая "
                    "строка — во всём репозитории.")] = "",
    repo: RepoArg = None,
) -> dict[str, Any]:
    found = await _guard(gh.search_code(query, repo=repo, path=path))
    if "ошибка" not in found:
        found["отпечаток"] = reports.fingerprint(found)
    return found


async def tool_summarize(
    found: Annotated[dict[str, Any], Field(
        description="Ответ search_code целиком, как он пришёл: запрос, репозиторий "
                    "и список «совпадения» (файл, строка, текст).")],
) -> dict[str, Any]:
    return await _guard(reports.summarize(found))


async def tool_save_to_file(
    name: Annotated[str, Field(
        min_length=1, max_length=80,
        description="Имя файла без расширения, например сводка. Файл ляжет в "
                    "data/reports/<имя>.md; то же имя перезапишет прошлый отчёт.")],
    content: Annotated[str, Field(
        min_length=1, max_length=200_000,
        description="Что сохранить, в Markdown: поле «отчёт» из ответа summarize "
                    "или отчёт, который ты собрал сам.")],
) -> dict[str, Any]:
    try:
        return reports.save(name, content)
    except OSError as e:
        return {"ошибка": "файл не записан: {0}".format(e)}


# --- Инструменты с отложенным выполнением: наблюдение по расписанию ----------
# Вызов не отвечает на вопрос сразу, а заводит задание: дальше планировщик сам
# снимает пульс репозитория и копит события в SQLite (см. watcher.py).

async def tool_watch_repo(
    every_minutes: Annotated[int, Field(
        ge=1, le=1440,
        description="Как часто проверять репозиторий, в минутах: от 1 до 1440 "
                    "(раз в сутки).")] = 60,
    repo: RepoArg = None,
) -> dict[str, Any]:
    return await _guard(watcher.add(repo or config.GITHUB_REPO, every_minutes))


async def tool_watch_summary(
    minutes: Annotated[int, Field(
        ge=1, le=10080,
        description="За какой период свести события, в минутах: 60 — за час, "
                    "1440 — за сутки, до 10080 — за неделю.")] = 60,
    after_id: Annotated[int, Field(
        ge=0,
        description="Вернуть только события новее этого номера. Нужен для "
                    "регулярной сводки, чтобы не повторяться; 0 — все за период.")] = 0,
    repo: Annotated[str | None, Field(
        description="Свести только по этому репозиторию («владелец/имя»). "
                    "По умолчанию — по всем наблюдениям.")] = None,
) -> dict[str, Any]:
    try:
        return watcher.summary(minutes=minutes, repo=repo, after_id=after_id)
    except gh.GitHubError as e:
        return {"ошибка": str(e)}


async def tool_stop_watch(
    watch_id: Annotated[int, Field(
        description="Номер наблюдения из ответа watch_repo или watch_summary.")],
) -> dict[str, Any]:
    if watcher.remove(watch_id):
        return {"результат": "наблюдение №{0} остановлено и удалено".format(watch_id)}
    return {"ошибка": "наблюдения №{0} нет".format(watch_id)}


# Имя → функция, описание и группа в правой панели. Описание уходит модели как
# есть, поэтому в нём сразу сказано, что инструмент делает и когда его звать.
GITHUB_TOOLS: dict[str, tuple[Callable[..., Any], str, str]] = {
    "repo_info": (
        tool_repo_info,
        "Карточка репозитория на GitHub: описание, основной язык, ветка по "
        "умолчанию, число звёзд и открытых задач, время последней записи.",
        "live",
    ),
    "list_commits": (
        tool_list_commits,
        "Последние коммиты репозитория: короткий хеш, автор, дата и первая "
        "строка сообщения. С этого инструмента начинают, когда спрашивают, "
        "что нового или что менялось.",
        "live",
    ),
    "commit_details": (
        tool_commit_details,
        "Подробности одного коммита по его хешу: полное сообщение, список "
        "изменённых файлов, сколько строк добавлено и удалено, куски диффа.",
        "live",
    ),
    "list_issues": (
        tool_list_issues,
        "Задачи (issues) и pull request'ы репозитория с их номером, "
        "заголовком, автором и состоянием.",
        "live",
    ),
    "list_files": (
        tool_list_files,
        "Содержимое папки репозитория: файлы и вложенные папки с размерами. "
        "Нужен, чтобы найти путь к файлу перед чтением.",
        "live",
    ),
    "read_file": (
        tool_read_file,
        "Текст файла из репозитория по его пути. Большие файлы обрезаются, "
        "двоичные не читаются.",
        "live",
    ),
    "search_code": (
        tool_search_code,
        "Поиск по коду репозитория: все строки, где встречается слово или имя, с "
        "файлом, номером строки и функцией, внутри которой стоит строка. Первый "
        "шаг пайплайна: его ответ целиком передают в summarize.",
        "pipeline",
    ),
    "summarize": (
        tool_summarize,
        "Сводка найденного: модель коротко описывает, что это и где используется, "
        "а инструмент собирает отчёт в Markdown. На вход — ответ search_code "
        "целиком; поле «отчёт» из ответа передают в save_to_file.",
        "pipeline",
    ),
    "save_to_file": (
        tool_save_to_file,
        "Сохранить отчёт в Markdown в файл data/reports/<имя>.md на сервере: "
        "последний шаг пайплайна или любой отчёт, собранный по ходу работы.",
        "pipeline",
    ),
    "watch_repo": (
        tool_watch_repo,
        "Поставить репозиторий на наблюдение по расписанию: сервер будет сам "
        "проверять его каждые N минут и записывать новые коммиты, задачи и "
        "звёзды. Повторный вызов для того же репозитория меняет интервал.",
        "schedule",
    ),
    "watch_summary": (
        tool_watch_summary,
        "Агрегированная сводка наблюдения за период: сколько было проверок, "
        "какие коммиты появились и от кого, какие задачи открыты и закрыты, "
        "как изменились звёзды. Отвечай по ней на «что произошло за час / день».",
        "schedule",
    ),
    "stop_watch": (
        tool_stop_watch,
        "Остановить наблюдение и удалить его историю. Номер наблюдения — из "
        "ответа watch_repo или watch_summary.",
        "schedule",
    ),
}


# --- Инструменты сервера «Зависимости»: PyPI и OSV.dev -----------------------

PackageArg = Annotated[str, Field(
    min_length=1, max_length=120,
    description="Имя пакета на PyPI, например fastapi или python-dotenv. Можно "
                "строку из requirements.txt целиком: extras и условие на версию "
                "отрежутся — uvicorn[standard]>=0.30.")]


def _remember(name: str, **fields: Any) -> None:
    """Запомнить ответ по пакету для правой панели."""
    row = packages.setdefault(deps_api.canonical(name), {
        "name": name, "checked": None, "checked_date": None, "newer": None,
        "latest": None, "latest_date": None, "vulns": None, "found": [],
    })
    row.update(fields, time=datetime.now().strftime("%H:%M:%S"))
    _touch()


async def tool_package_info(
    name: PackageArg,
    version: Annotated[str | None, Field(
        description="С какой версией сравнить, например 0.115 — нижняя граница из "
                    "requirements.txt. Тогда в ответе будет, когда она вышла и "
                    "сколько выпусков новее. Можно не указывать.")] = None,
) -> dict[str, Any]:
    try:
        info = await deps_api.package_info(name, version)
    except deps_api.DepsError as e:
        return {"ошибка": str(e)}
    fields = {"latest": info["последняя версия"], "latest_date": info["вышла"]}
    checked = info.get("проверенная версия")
    if checked and "ошибка" not in checked:
        fields.update(checked=checked["номер"], checked_date=checked["вышла"],
                      newer=checked["выпусков новее"])
    _remember(info["пакет"], **fields)
    return info


async def tool_vulnerabilities(
    name: PackageArg,
    version: Annotated[str, Field(
        min_length=1, max_length=40,
        description="Какую версию проверить, например 0.115 или 1.0.0. Для строки из "
                    "requirements.txt вида пакет>=X — это X: нижняя граница, "
                    "которую допускает проект.")],
) -> dict[str, Any]:
    try:
        found = await deps_api.vulnerabilities(name, version)
    except deps_api.DepsError as e:
        return {"ошибка": str(e)}
    _remember(found["пакет"], checked=found["версия"], vulns=found["уязвимостей"],
              found=[{"id": v["номер"], "severity": v["опасность"], "fixed": v["исправлено в"]}
                     for v in found["список"]])
    return found


DEPS_TOOLS: dict[str, tuple[Callable[..., Any], str, str]] = {
    "package_info": (
        tool_package_info,
        "Пакет Python на PyPI: последняя версия и когда вышла, описание, лицензия, "
        "нужная версия Python. С параметром version — ещё когда вышла эта версия "
        "и сколько выпусков новее: так видно, насколько устарела зависимость.",
        "packages",
    ),
    "vulnerabilities": (
        tool_vulnerabilities,
        "Известные уязвимости конкретной версии пакета Python по базе OSV.dev: "
        "номер (CVE или GHSA), суть, опасность и версия, где исправлено. Одна "
        "уязвимость под разными номерами считается один раз.",
        "packages",
    ),
}


# --- Сами серверы --------------------------------------------------------------

def _params(fn: Callable[..., Any]) -> list[dict]:
    """Параметры инструмента для правой панели: имя и обязателен ли он."""
    return [
        {"name": name, "required": parameter.default is inspect.Parameter.empty}
        for name, parameter in inspect.signature(fn).parameters.items()
    ]


class Server:
    """Один MCP-сервер со своим адресом и выключателями: целиком и по инструменту."""

    def __init__(self, key: str, title: str, source: str, instructions: str,
                 tools: dict[str, tuple[Callable[..., Any], str, str]]) -> None:
        self.key = key          # имя в протоколе; у агента — приставка к инструментам
        self.title = title      # как сервер называется на экране
        self.source = source    # откуда берёт данные — строка для правой панели
        self.path = "{0}/{1}".format(config.MCP_PATH, key)
        self.url = "{0}/{1}/".format(config.MCP_BASE_URL, key)
        self.tools = tools
        # Выключатель всего сервера. Проверяется в `main.py` перед тем, как пустить
        # запрос на его адрес: выключенный отвечает 503, и агент честно теряет связь.
        self.on = True
        # Выключенный инструмент снимается с сервера целиком, поэтому в ответе
        # tools/list его нет — агент о нём даже не знает.
        self.enabled = {name: True for name in tools}
        self.mcp = MCPServer(name=key, version="1.0", instructions=instructions)
        for name, (fn, description, _group) in tools.items():
            self.mcp.add_tool(fn, name=name, description=description)
        # Приложение сервера — свой ASGI-роут, `main.py` подвешивает его к общему
        # порту. Менеджер сессий у сервера появляется только после этого вызова.
        self.app = self.mcp.streamable_http_app(streamable_http_path="/")

    def set_tool(self, name: str, on: bool) -> None:
        """Включить или выключить инструмент на сервере."""
        if name not in self.tools or self.enabled[name] == on:
            return
        if on:
            fn, description, _group = self.tools[name]
            self.mcp.add_tool(fn, name=name, description=description)
        else:
            self.mcp.remove_tool(name)
        self.enabled[name] = on
        _touch()

    def set_on(self, on: bool) -> None:
        """Включить или выключить весь сервер."""
        if self.on != on:
            self.on = on
            _touch()

    def snapshot(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "source": self.source,
            "url": self.url,
            "on": self.on,
            "tools": [
                {"name": name, "description": description, "on": self.enabled[name],
                 "params": _params(fn), "group": group}
                for name, (fn, description, group) in self.tools.items()
            ],
        }


github = Server(
    "github", "GitHub", config.GITHUB_API,
    instructions="Репозиторий на GitHub: коммиты, файлы, задачи и поиск по коду. "
                 "Данные берутся из api.github.com в момент вызова. Пайплайн "
                 "search_code → summarize → save_to_file ищет по коду, сводит "
                 "найденное и сохраняет отчёт в файл; save_to_file сохраняет и "
                 "любой другой отчёт. Наблюдение по расписанию копит события и "
                 "отдаёт сводку.",
    tools=GITHUB_TOOLS,
)
deps = Server(
    "deps", "Зависимости", "pypi.org и osv.dev · ключ не нужен",
    instructions="Пакеты Python: последняя версия и даты выпусков на PyPI, "
                 "известные уязвимости конкретной версии по базе OSV.dev. Данные "
                 "берутся в момент вызова. Список пакетов проекта сервер не знает — "
                 "его дают снаружи.",
    tools=DEPS_TOOLS,
)

# Все серверы по имени: main.py монтирует каждый на свой адрес.
SERVERS: dict[str, Server] = {server.key: server for server in (github, deps)}


def clear_packages() -> None:
    """Очистить карточку проверенных пакетов."""
    packages.clear()
    _touch()


def snapshot() -> dict[str, Any]:
    """Состояние серверов для правой панели интерфейса."""
    return {
        "revision": revision,
        "servers": [server.snapshot() for server in SERVERS.values()],
        "repo": config.GITHUB_REPO,
        "auth": gh.auth_state(),
        "overview": overview,
        "error": overview_error,
        "updated": overview_time,
        "loading": overview_loading,
        "packages": list(packages.values()),
    }
