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
# к модели. Четырёх кругов хватает на «посмотрел → изменил → проверил → ответил».
TOOL_ROUNDS = int(os.getenv("TOOL_ROUNDS", "4"))

# --- Сеть ---
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))

# MCP-сервер живёт в этом же процессе, но агент ходит в него по сети — так
# работает настоящий протокол, а не прямой вызов функций в обход него.
MCP_PATH = "/mcp"
MCP_URL = os.getenv("MCP_URL", f"http://127.0.0.1:{PORT}{MCP_PATH}/")

# Локальные адреса — мимо системного прокси. Если в окружении задан HTTP_PROXY
# (VPN-клиенты ставят его глобально), запрос приложения к самому себе уходит на
# прокси и соединение не встаёт.
os.environ["NO_PROXY"] = ",".join(
    filter(None, [os.getenv("NO_PROXY", ""), "127.0.0.1", "localhost"])
)
