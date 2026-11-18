"""Юзербот Telegram на Telethon.

Секреты читаются только из .env. Файл сессии — userbot.session.
Код входа Telegram приходит в приложение на этот номер, затем:

    .venv/bin/python userbot.py --code 12345

Если включена облачная двухфакторная защита, пароль берётся из TELEGRAM_PASSWORD.
Ctrl+C останавливает процесс без traceback.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from telethon import TelegramClient

from stream_probe import (
    bind_stream_command,
    leave_joined_call,
    recover_local_wavs,
    shutdown_requested,
    start_call_engine,
)
from telethon.errors import (
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.tl.functions.account import GetPasswordRequest

from config import (
    USERBOT_HELP,
    get_telegram_api_hash,
    get_telegram_api_id,
    get_telegram_password,
    get_telegram_phone,
)

ROOT = Path(__file__).resolve().parent
SESSION = ROOT / "userbot"
CODE_FILE = ROOT / ".userbot_code"
PASSWORD_STEP = ROOT / ".userbot_need_password"
_stop_requested = False


def install_quiet_stop() -> None:
    def _stop(signum: int, frame: object) -> None:
        global _stop_requested
        _stop_requested = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def hide_secrets(text: str) -> str:
    cleaned = text
    for secret in (
        get_telegram_api_hash() if _env_present("TELEGRAM_API_HASH") else "",
        get_telegram_phone() if _env_present("TELEGRAM_PHONE") else "",
        get_telegram_password(),
    ):
        if secret:
            cleaned = cleaned.replace(secret, "***")
    return cleaned


def _env_present(name: str) -> bool:
    import os

    return bool(os.environ.get(name, "").strip())


def save_code_hash(phone_code_hash: str) -> None:
    CODE_FILE.write_text(phone_code_hash + "\n", encoding="utf-8")
    CODE_FILE.chmod(0o600)


def load_code_hash() -> str:
    if not CODE_FILE.is_file():
        return ""
    return CODE_FILE.read_text(encoding="utf-8").strip()


def clear_code_hash() -> None:
    CODE_FILE.unlink(missing_ok=True)


def mark_password_step() -> None:
    PASSWORD_STEP.write_text("1\n", encoding="utf-8")
    PASSWORD_STEP.chmod(0o600)


def password_step_pending() -> bool:
    return PASSWORD_STEP.is_file()


def clear_password_step() -> None:
    PASSWORD_STEP.unlink(missing_ok=True)


def describe_me(me: object) -> str:
    first = getattr(me, "first_name", "") or ""
    last = getattr(me, "last_name", "") or ""
    username = getattr(me, "username", "") or ""
    user_id = getattr(me, "id", "")
    name = " ".join(part for part in (first, last) if part).strip() or "без имени"
    login = f" @{username}" if username else ""
    return f"{name}{login}, id={user_id}"


async def connect_client() -> TelegramClient:
    client = TelegramClient(str(SESSION), get_telegram_api_id(), get_telegram_api_hash())
    await client.connect()
    return client


async def send_login_code(client: TelegramClient, phone: str) -> int:
    sent = await client.send_code_request(phone)
    save_code_hash(sent.phone_code_hash)
    print("Код входа отправлен в Telegram на указанный номер.")
    print("Пришлите код сюда или запустите: .venv/bin/python userbot.py --code 12345")
    return 2


async def password_hint(client: TelegramClient) -> str:
    info = await client(GetPasswordRequest())
    return (getattr(info, "hint", None) or "").strip()


async def sign_in_password(client: TelegramClient) -> int:
    password = get_telegram_password()
    if not password:
        hint = await password_hint(client)
        print(
            "На аккаунте включена двухфакторная защита. "
            "Впишите пароль в TELEGRAM_PASSWORD в .env и повторите вход. "
            "Пароль в чат не присылайте."
        )
        if hint:
            print(f"Подсказка: {hint}")
        return 2
    try:
        await client.sign_in(password=password)
    except PasswordHashInvalidError:
        hint = await password_hint(client)
        if hint:
            print(f"Облачный пароль не подошёл. Подсказка: {hint}")
        else:
            print("Облачный пароль не подошёл. Подсказка в Telegram не задана.")
        return 1
    clear_password_step()
    clear_code_hash()
    return 0


async def sign_in(client: TelegramClient, phone: str, code: str) -> int:
    phone_code_hash = load_code_hash()
    if not phone_code_hash:
        print("Сначала запросите код: .venv/bin/python userbot.py --send-code")
        return 2
    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
    except SessionPasswordNeededError:
        mark_password_step()
        return await sign_in_password(client)
    except PhoneCodeInvalidError:
        print("Код не подошёл. Запросите новый: .venv/bin/python userbot.py --send-code")
        return 1
    except PhoneCodeExpiredError:
        clear_code_hash()
        print("Код истёк. Запросите новый: .venv/bin/python userbot.py --send-code")
        return 1
    clear_code_hash()
    return 0


async def run(args: argparse.Namespace) -> int:
    try:
        phone = get_telegram_phone()
        get_telegram_api_id()
        get_telegram_api_hash()
    except RuntimeError:
        print(USERBOT_HELP)
        return 2

    client = await connect_client()
    try:
        authorized = await client.is_user_authorized()
        if not authorized and password_step_pending():
            status = await sign_in_password(client)
            if status != 0:
                return status
            authorized = True
        if not authorized:
            if args.code:
                status = await sign_in(client, phone, args.code.strip())
                if status != 0:
                    return status
            elif args.send_code or not args.code:
                if args.send_code or not authorized:
                    return await send_login_code(client, phone)
        me = await client.get_me()
        print(f"Юзербот подключён: {describe_me(me)}")
        if args.once:
            return 0
        bind_stream_command(client)
        await start_call_engine()
        asyncio.create_task(recover_local_wavs(client))
        print("В Избранное: !rec имя_чата. Каждые 30 минут wav уходит туда и удаляется с диска.")
        print("Раньше срока: !rec 0. В чате эфира по-прежнему !str и !str 0.")
        print("Если из эфира выкинет другая сессия, в Избранном ответьте 0 или 1.")
        print("Сессия сохранена. Ожидание. Остановка: Ctrl+C")
        await client.run_until_disconnected()
        if _stop_requested or shutdown_requested():
            return 0
        print("Соединение закрыто.")
        return 1
    finally:
        try:
            await leave_joined_call()
        except Exception:
            pass
        if client.is_connected():
            await client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Юзербот Telegram на Telethon")
    parser.add_argument(
        "--send-code",
        action="store_true",
        help="Отправить код входа на TELEGRAM_PHONE",
    )
    parser.add_argument("--code", default="", help="Код из Telegram")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Проверить сессию и сразу выйти",
    )
    return parser.parse_args()


def main() -> int:
    install_quiet_stop()
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Юзербот остановлен.")
        return 0
    except Exception as exc:
        print(hide_secrets(str(exc)) or exc.__class__.__name__)
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Юзербот остановлен.")
        sys.exit(0)
