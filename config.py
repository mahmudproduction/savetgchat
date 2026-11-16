"""Чтение настроек из окружения и файла .env."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

NEON_SETUP_HELP = """
DATABASE_URL не задан. Локальный PostgreSQL не используется: база должна быть в Neon.

Как получить строку подключения:
1. Откройте https://console.neon.tech и зарегистрируйтесь или войдите.
2. Создайте проект на бесплатном тарифе Free. Регион можно оставить предлагаемый.
3. На странице проекта откройте Connect и скопируйте connection string.
4. Вставьте строку в ~/projects/neon_bot/.env:
   DATABASE_URL=postgresql://USER:PASSWORD@HOST/neondb?sslmode=verify-full
5. Не присылайте пароль в чат и не добавляйте .env в Git.

Подключение в коде всегда использует sslmode=verify-full:
канал шифруется, сертификат сервера проверяется. Отключить проверку SSL нельзя.
""".strip()

BOTFATHER_HELP = """
TELEGRAM_BOT_TOKEN не задан.

Как получить токен:
1. В Telegram откройте @BotFather.
2. Отправьте команду /newbot и задайте имя и username бота.
3. BotFather пришлёт токен. Вставьте его в ~/projects/neon_bot/.env:
   TELEGRAM_BOT_TOKEN=токен_от_BotFather
4. Не присылайте токен в чат и не добавляйте .env в Git.
""".strip()


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL не задан")
    return url


def get_telegram_token() -> str:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")
    return token


USERBOT_HELP = """
Не заданы TELEGRAM_API_ID, TELEGRAM_API_HASH или TELEGRAM_PHONE.

Их выдаёт https://my.telegram.org (API development tools).
Впишите в ~/projects/neon_bot/.env и не отправляйте значения в чат:
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_PHONE=+90...
""".strip()


def get_telegram_api_id() -> int:
    raw = os.environ.get("TELEGRAM_API_ID", "").strip()
    if not raw.isdigit():
        raise RuntimeError("TELEGRAM_API_ID не задан")
    return int(raw)


def get_telegram_api_hash() -> str:
    value = os.environ.get("TELEGRAM_API_HASH", "").strip()
    if not value:
        raise RuntimeError("TELEGRAM_API_HASH не задан")
    return value


def get_telegram_phone() -> str:
    value = "".join(os.environ.get("TELEGRAM_PHONE", "").split())
    if not value.startswith("+") or not value[1:].isdigit():
        raise RuntimeError("TELEGRAM_PHONE не задан")
    return value


def get_telegram_password() -> str:
    return os.environ.get("TELEGRAM_PASSWORD", "").strip()
