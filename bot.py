"""Минимальный Telegram-бот: статус Neon и чтение daily_records."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from config import BOTFATHER_HELP, get_telegram_token
from database import (
    DatabaseError,
    check_connection,
    count_records,
    get_latest_record,
    redact,
)

TZ = ZoneInfo("Europe/Istanbul")
LOG = logging.getLogger("neon_bot")


class RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        cleaned = redact(message)
        if cleaned != message:
            record.msg = cleaned
            record.args = ()
        return True


def install_quiet_stop() -> None:
    def _stop(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger().addFilter(RedactFilter())


def format_record(row: dict) -> str:
    created = row["created_at"]
    if isinstance(created, datetime):
        created = created.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    payload = json.dumps(row["data"], ensure_ascii=False, default=str)
    if len(payload) > 3000:
        payload = payload[:3000] + "…"
    return (
        f"id: {row['id']}\n"
        f"дата записи: {row['record_date']}\n"
        f"создано: {created}\n"
        f"данные: {payload}"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        "Бот читает ежедневные записи из Neon PostgreSQL.\n"
        "/status — проверка подключения к базе\n"
        "/latest — последняя запись\n"
        "/count — сколько записей в daily_records\n"
        "Новую запись за сутки добавляет скрипт daily_task.py."
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    try:
        await asyncio.to_thread(check_connection)
    except RuntimeError as exc:
        if "DATABASE_URL" in str(exc):
            await message.reply_text("DATABASE_URL не задан. Добавьте строку Neon в файл .env.")
            return
        await message.reply_text("Не удалось прочитать настройку подключения.")
        return
    except DatabaseError as exc:
        await message.reply_text(f"База недоступна. {exc}")
        return
    await message.reply_text("Подключение к Neon есть. Сертификат сервера проверяется.")


async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    try:
        row = await asyncio.to_thread(get_latest_record)
    except RuntimeError as exc:
        if "DATABASE_URL" in str(exc):
            await message.reply_text("DATABASE_URL не задан. Добавьте строку Neon в файл .env.")
            return
        await message.reply_text("Не удалось прочитать настройку подключения.")
        return
    except DatabaseError as exc:
        await message.reply_text(f"Не удалось прочитать запись. {exc}")
        return
    if row is None:
        await message.reply_text("Записей пока нет.")
        return
    await message.reply_text(format_record(row))


async def count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    try:
        total = await asyncio.to_thread(count_records)
    except RuntimeError as exc:
        if "DATABASE_URL" in str(exc):
            await message.reply_text("DATABASE_URL не задан. Добавьте строку Neon в файл .env.")
            return
        await message.reply_text("Не удалось прочитать настройку подключения.")
        return
    except DatabaseError as exc:
        await message.reply_text(f"Не удалось посчитать записи. {exc}")
        return
    await message.reply_text(f"Записей в daily_records: {total}")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOG.error("сбой команды: %s", redact(str(context.error)))
    if isinstance(update, Update) and update.effective_message is not None:
        await update.effective_message.reply_text(
            "Команда не выполнена. Секреты в журнал не записываются."
        )


def main() -> int:
    install_quiet_stop()
    setup_logging()
    try:
        token = get_telegram_token()
    except RuntimeError:
        print(BOTFATHER_HELP)
        return 2

    application = (
        Application.builder()
        .token(token)
        .connect_timeout(10.0)
        .read_timeout(10.0)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("latest", latest))
    application.add_handler(CommandHandler("count", count))
    application.add_error_handler(on_error)
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
