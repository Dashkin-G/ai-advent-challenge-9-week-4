"""Настройки приложения. Секреты — только из окружения (.env)."""
import os

from dotenv import load_dotenv

load_dotenv()

# --- Модель (Alibaba Model Studio / DashScope, OpenAI-совместимый режим) ---
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
DASHSCOPE_BASE_URL = os.getenv(
    "DASHSCOPE_BASE_URL",
    "https://ws-q1vxaj37wm4fa9q8.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1",
)
MODEL = os.getenv("MODEL", "qwen3.8-2.4t-a95b")

# Сколько раз за одно обращение агент может сходить в инструменты и вернуться
# к модели. Длинному флоу по двум серверам нужно около шести кругов: «прочитал
# requirements.txt → проверил пакеты → посмотрел код → сохранил отчёт →
# ответил»; десять — с запасом.
TOOL_ROUNDS = int(os.getenv("TOOL_ROUNDS", "10"))

# --- GitHub: внешний API, вокруг которого построен MCP-сервер ---
# Репозиторий по умолчанию — этого же курса. Каждый инструмент принимает
# необязательный параметр repo, так что агент может заглянуть и в чужой.
GITHUB_REPO = os.getenv("GITHUB_REPO", "Dashkin-G/ai-advent-challenge-9-week-4")
GITHUB_API = os.getenv("GITHUB_API", "https://api.github.com")

# Токен необязателен: публичный репозиторий читается и без него, только лимит
# тогда 60 запросов в час вместо 5000. Если переменной нет, клиент попробует
# взять токен у GitHub CLI (`gh auth token`) — см. github_api.py.
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# --- Второй MCP-сервер: зависимости Python ---
# PyPI знает версии пакетов, OSV.dev — известные уязвимости. Оба API открытые,
# ключ не нужен.
PYPI_API = os.getenv("PYPI_API", "https://pypi.org/pypi")
OSV_API = os.getenv("OSV_API", "https://api.osv.dev/v1")

# --- Наблюдение по расписанию ---
# Задания, снимки и события живут в SQLite: наблюдение переживает перезапуск
# приложения и продолжается с того места, где остановилось.
WATCH_DB = os.getenv("WATCH_DB", os.path.join("data", "watch.db"))

# --- Пайплайн: поиск → сводка → файл ---
# Сюда инструмент save_to_file пишет отчёты. Тот же запрос перезаписывает свой
# файл, поэтому отчёты не копятся.
REPORTS_DIR = os.getenv("REPORTS_DIR", os.path.join("data", "reports"))

# --- Сеть ---
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))

# MCP-серверы живут в этом же процессе и на этом же порту, но агент ходит в них
# по сети — так работает настоящий протокол, а не прямой вызов функций в обход
# него. У каждого сервера свой адрес: /mcp/github/ и /mcp/deps/.
MCP_PATH = "/mcp"
MCP_BASE_URL = os.getenv("MCP_BASE_URL", f"http://127.0.0.1:{PORT}{MCP_PATH}")

# Реестр серверов для агента — как в конфиге любого MCP-клиента: имя и адрес.
# Имя сервера становится приставкой к именам его инструментов: github__read_file.
MCP_SERVERS = {
    "github": MCP_BASE_URL + "/github/",
    "deps": MCP_BASE_URL + "/deps/",
}

# Локальные адреса — мимо системного прокси. Если в окружении задан HTTP_PROXY
# (VPN-клиенты ставят его глобально), запрос приложения к самому себе уходит на
# прокси и соединение не встаёт. Запросы к api.github.com прокси не минуют:
# туда как раз можно и нужно идти обычным путём.
os.environ["NO_PROXY"] = ",".join(
    filter(None, [os.getenv("NO_PROXY", ""), "127.0.0.1", "localhost"])
)
