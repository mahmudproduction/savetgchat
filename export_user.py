"""Снимок своего аккаунта Telegram в таблицу userbot.

Останавливать службу перед запуском нужно снаружи: файл сессии один.
В базу попадают профиль и диалоги (чаты, группы, каналы, права админа).
Текст чужих и своих сообщений не сохраняется.
Ctrl+C завершает процесс без traceback и без записи, если снимок ещё не начат.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from datetime import date, datetime
from typing import Any

from telethon import TelegramClient
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    Channel,
    ChannelForbidden,
    Chat,
    ChatForbidden,
    User,
)

from config import USERBOT_HELP, get_telegram_api_hash, get_telegram_api_id, get_telegram_phone
from database import DatabaseError, redact, replace_userbot_snapshot, userbot_summary
from userbot import SESSION, describe_me

_stop_requested = False


def install_quiet_stop() -> None:
    def _stop(signum: int, frame: object) -> None:
        global _stop_requested
        _stop_requested = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def plain(value: Any, *, keep_phone: bool = False) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return None
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            name = str(key)
            if name == "access_hash":
                continue
            if name == "phone" and not keep_phone:
                continue
            result[name] = plain(item, keep_phone=keep_phone)
        return result
    if isinstance(value, (list, tuple)):
        return [plain(item, keep_phone=keep_phone) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return plain(to_dict(), keep_phone=keep_phone)
    return str(value)


def chat_kind(entity: object) -> str:
    if isinstance(entity, User):
        if entity.bot:
            return "bot"
        return "user"
    if isinstance(entity, ChatForbidden):
        return "group_forbidden"
    if isinstance(entity, ChannelForbidden):
        return "channel_forbidden"
    if isinstance(entity, Chat):
        return "group"
    if isinstance(entity, Channel):
        if entity.megagroup and entity.forum:
            return "forum"
        if entity.megagroup and entity.gigagroup:
            return "gigagroup"
        if entity.megagroup:
            return "supergroup"
        return "channel"
    return "unknown"


def membership(entity: object, is_admin: bool, is_creator: bool) -> str:
    if isinstance(entity, User):
        return "bot" if entity.bot else "private"
    if isinstance(entity, (ChatForbidden, ChannelForbidden)):
        return "forbidden"
    if bool(getattr(entity, "left", False)):
        return "left"
    if is_creator:
        return "creator"
    if is_admin:
        return "admin"
    if isinstance(entity, Channel) and entity.broadcast and not entity.megagroup:
        if getattr(entity, "banned_rights", None):
            return "restricted"
        return "subscriber"
    if getattr(entity, "banned_rights", None):
        return "restricted"
    return "member"


def flags(entity: object) -> tuple[bool, bool]:
    is_creator = bool(getattr(entity, "creator", False))
    rights = getattr(entity, "admin_rights", None)
    is_admin = is_creator or rights is not None
    if isinstance(entity, User):
        return False, False
    return is_admin, is_creator


async def collect(client: TelegramClient) -> tuple[int, list[dict[str, Any]]]:
    me = await client.get_me()
    if me is None:
        raise RuntimeError("Telegram не вернул профиль")
    full = await client(GetFullUserRequest(me))
    folders: Any
    try:
        folders = plain(await client(GetDialogFiltersRequest()))
    except Exception as exc:
        folders = {"error": redact(str(exc))}

    account_data: dict[str, Any] = {
        "user": plain(me, keep_phone=True),
        "full_user": plain(getattr(full, "full_user", full), keep_phone=True),
        "folders": folders,
    }
    name = " ".join(
        part for part in (me.first_name or "", me.last_name or "") if part
    ).strip()
    rows: list[dict[str, Any]] = [
        {
            "record_type": "account",
            "telegram_id": int(me.id),
            "title": name or None,
            "username": me.username,
            "chat_type": "user",
            "is_admin": None,
            "is_creator": None,
            "data": account_data,
        }
    ]

    stats = {
        "dialogs": 0,
        "admins": 0,
        "creators": 0,
        "private": 0,
        "groups": 0,
        "channels": 0,
    }
    async for dialog in client.iter_dialogs():
        if _stop_requested:
            raise KeyboardInterrupt
        entity = dialog.entity
        kind = chat_kind(entity)
        is_admin, is_creator = flags(entity)
        role = membership(entity, is_admin, is_creator)
        stats["dialogs"] += 1
        if is_admin:
            stats["admins"] += 1
        if is_creator:
            stats["creators"] += 1
        if kind in {"user", "bot"}:
            stats["private"] += 1
        elif kind in {"channel", "channel_forbidden"}:
            stats["channels"] += 1
        else:
            stats["groups"] += 1
        notify = getattr(getattr(dialog, "dialog", None), "notify_settings", None)
        rows.append(
            {
                "record_type": "dialog",
                "telegram_id": int(dialog.id),
                "title": dialog.name or getattr(entity, "title", None),
                "username": getattr(entity, "username", None),
                "chat_type": kind,
                "is_admin": is_admin,
                "is_creator": is_creator,
                "data": {
                    "membership": role,
                    "entity_id": getattr(entity, "id", None),
                    "archived": bool(dialog.archived),
                    "pinned": bool(dialog.pinned),
                    "unread_count": int(dialog.unread_count or 0),
                    "unread_mentions_count": int(getattr(dialog, "unread_mentions_count", 0) or 0),
                    "folder_id": getattr(dialog, "folder_id", None),
                    "last_activity": plain(dialog.date),
                    "left": bool(getattr(entity, "left", False)),
                    "participants_count": getattr(entity, "participants_count", None),
                    "admin_rights": plain(getattr(entity, "admin_rights", None)),
                    "banned_rights": plain(getattr(entity, "banned_rights", None)),
                    "default_banned_rights": plain(getattr(entity, "default_banned_rights", None)),
                    "mute_until": plain(getattr(notify, "mute_until", None)) if notify else None,
                    "entity": plain(entity),
                },
            }
        )
        if stats["dialogs"] % 200 == 0:
            print(f"прочитано диалогов: {stats['dialogs']}", flush=True)

    account_data["stats"] = stats
    print(f"Юзербот: {describe_me(me)}", flush=True)
    print(
        "Диалогов: {dialogs}. Личных: {private}. Групп: {groups}. "
        "Каналов: {channels}. Админ: {admins}. Создатель: {creators}.".format(**stats),
        flush=True,
    )
    return int(me.id), rows


async def run() -> int:
    try:
        get_telegram_api_id()
        get_telegram_api_hash()
        get_telegram_phone()
    except RuntimeError:
        print(USERBOT_HELP)
        return 2

    client = TelegramClient(str(SESSION), get_telegram_api_id(), get_telegram_api_hash())
    await client.connect()
    try:
        if not await client.is_user_authorized():
            print("Сессия не авторизована. Сначала войдите через userbot.py.")
            return 2
        user_id, rows = await collect(client)
    finally:
        if client.is_connected():
            await client.disconnect()

    saved = replace_userbot_snapshot(user_id, rows)
    print(f"В таблицу userbot записано строк: {saved}", flush=True)
    for row in userbot_summary(user_id):
        print(
            "сводка: type={record_type} chat={chat_type} "
            "admin={is_admin} creator={is_creator} count={total}".format(**row),
            flush=True,
        )
    return 0


def main() -> int:
    install_quiet_stop()
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        print("Выгрузка остановлена. Снимок в базу не записан.")
        return 0
    except DatabaseError as exc:
        print(redact(str(exc)))
        return 1
    except Exception as exc:
        print(redact(str(exc)) or exc.__class__.__name__)
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Выгрузка остановлена.")
        sys.exit(0)
