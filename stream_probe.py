"""Проба stream mode: звук эфира кусками с media DC, без микрофона.

Сначала ищет живой видеочат или эфир среди своих диалогов.
Входит с выключенным микрофоном только если сервер может отдать stream mode:
RTMP, эфир канала от 30 участников или видеочат от 1000.
Снимает несколько звуковых кусков в log/ и выходит из эфира.
Ctrl+C останавливает процесс без traceback и выходит из звонка, если вход уже был.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
import signal
import struct
import sys
import threading
import time
from pathlib import Path

from pytgcalls import PyTgCalls
from pytgcalls.exceptions import NoActiveGroupCall, NotInCallError
from pytgcalls.types import Device, Direction, GroupCallConfig, RecordStream, StreamFrames

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.phone import (
    GetGroupCallRequest,
    GetGroupCallStreamChannelsRequest,
    JoinGroupCallRequest,
    LeaveGroupCallRequest,
)
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import (
    Channel,
    Chat,
    DataJSON,
    GroupCall,
    GroupCallDiscarded,
    InputGroupCall,
    InputGroupCallStream,
    InputPeerSelf,
    UpdateGroupCall,
    UpdateGroupCallConnection,
)

from config import USERBOT_HELP, get_telegram_api_hash, get_telegram_api_id, get_telegram_phone
from database import redact

ROOT = Path(__file__).resolve().parent
SESSION = ROOT / "userbot"
OUT_DIR = ROOT / "log"
SEGMENT_MS = 1000
BUFFER_MS = 2000
CHUNK_LIMIT = 1024 * 1024
_stop_requested = False


def install_quiet_stop() -> None:
    def _stop(signum: int, frame: object) -> None:
        global _stop_requested
        _stop_requested = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def join_payload(ssrc: int) -> str:
    fingerprint = ":".join(f"{byte:02X}" for byte in secrets.token_bytes(32))
    return json.dumps(
        {
            "ssrc": ssrc,
            "ufrag": secrets.token_hex(4),
            "pwd": secrets.token_hex(16),
            "fingerprints": [
                {
                    "hash": "sha-256",
                    "fingerprint": fingerprint,
                    "setup": "passive",
                }
            ],
        },
        separators=(",", ":"),
    )


def stream_likely(broadcast: bool, participants: int, rtmp: bool) -> bool:
    if rtmp:
        return True
    if broadcast and participants >= 30:
        return True
    return participants >= 1000


async def call_on_dc(client: TelegramClient, dc_id: int, request: object) -> object:
    if dc_id == client.session.dc_id:
        return await client(request)
    sender = await client._borrow_exported_sender(dc_id)
    try:
        return await client._call(sender, request)
    finally:
        await client._return_exported_sender(sender)


async def active_calls(client: TelegramClient) -> list[dict]:
    found: list[dict] = []
    checked = 0
    async for dialog in client.iter_dialogs():
        if _stop_requested:
            raise KeyboardInterrupt
        entity = dialog.entity
        if not isinstance(entity, (Channel, Chat)):
            continue
        checked += 1
        if checked % 100 == 0:
            print(f"проверено чатов: {checked}", flush=True)
        try:
            if isinstance(entity, Channel):
                full = await client(GetFullChannelRequest(entity))
            else:
                full = await client(GetFullChatRequest(entity.id))
        except RPCError:
            continue
        call = getattr(full.full_chat, "call", None)
        if call is None:
            continue
        try:
            details = await client(GetGroupCallRequest(call, limit=1))
        except RPCError as exc:
            print(f"эфир без подробностей: {dialog.name}: {redact(exc.message)}", flush=True)
            continue
        group_call = details.call
        if isinstance(group_call, GroupCallDiscarded) or not isinstance(group_call, GroupCall):
            continue
        broadcast = isinstance(entity, Channel) and bool(entity.broadcast)
        row = {
            "title": dialog.name or "",
            "chat_id": int(dialog.id),
            "broadcast": broadcast,
            "participants": int(group_call.participants_count or 0),
            "rtmp": bool(group_call.rtmp_stream),
            "conference": bool(group_call.conference),
            "stream_dc_id": group_call.stream_dc_id,
            "call_id": int(group_call.id),
            "access_hash": int(group_call.access_hash),
            "scheduled": group_call.schedule_date is not None,
        }
        found.append(row)
        kind = "канал" if broadcast else "группа"
        mode = "rtmp" if row["rtmp"] else "webrtc-или-stream"
        print(
            f"эфир: {row['title']} ({kind}), участников {row['participants']}, {mode}",
            flush=True,
        )
    print(f"проверено чатов: {checked}, живых эфиров: {len(found)}", flush=True)
    return found


def choose_call(rows: list[dict]) -> dict | None:
    ready = [
        row
        for row in rows
        if not row["conference"]
        and not row["scheduled"]
        and stream_likely(row["broadcast"], row["participants"], row["rtmp"])
    ]
    if not ready:
        return None
    ready.sort(key=lambda row: (row["rtmp"], row["participants"]), reverse=True)
    return ready[0]


def connection_params(updates: object) -> dict | None:
    for update in getattr(updates, "updates", []) or []:
        if isinstance(update, UpdateGroupCallConnection) and not update.presentation:
            return json.loads(update.params.data)
    return None


async def download_audio(
    client: TelegramClient,
    call: InputGroupCall,
    dc_id: int,
    start_ms: int,
    pieces: int,
    destination: Path,
) -> int:
    written = 0
    time_ms = start_ms
    with destination.open("wb") as handle:
        saved = 0
        while saved < pieces:
            if _stop_requested:
                raise KeyboardInterrupt
            request = GetFileRequest(
                InputGroupCallStream(call=call, time_ms=time_ms, scale=0),
                offset=0,
                limit=CHUNK_LIMIT,
            )
            try:
                result = await call_on_dc(client, dc_id, request)
            except RPCError as exc:
                name = exc.message or ""
                if name.startswith("FLOOD_WAIT_") or name == "TIME_TOO_BIG":
                    await asyncio.sleep(0.1)
                    continue
                raise
            data = getattr(result, "bytes", b"") or b""
            if not data:
                await asyncio.sleep(0.1)
                continue
            handle.write(data)
            written += len(data)
            saved += 1
            time_ms += SEGMENT_MS
            print(f"кусок {saved}: {len(data)} байт, отметка {time_ms - SEGMENT_MS}", flush=True)
    return written


def stream_dc_from_updates(updates: object, fallback: int | None) -> int | None:
    for update in getattr(updates, "updates", []) or []:
        call = getattr(update, "call", None)
        if isinstance(update, UpdateGroupCall) and isinstance(call, GroupCall) and call.stream_dc_id:
            return int(call.stream_dc_id)
    return fallback


_seat: dict | None = None
_seat_lock = asyncio.Lock()
_calls: PyTgCalls | None = None
_wav: "PcmWav | None" = None


class PcmWav:
    """Пишет PCM 16 бит в wav без ffmpeg."""

    def __init__(self, path: Path, rate: int = 48000, channels: int = 2) -> None:
        self.path = path
        self.rate = rate
        self.channels = channels
        self.size = 0
        self.chat_id: int | None = None
        self._device = None
        self._ssrc: int | None = None
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("wb")
        self._file.write(b"\0" * 44)

    def write(self, data: bytes, device: Device, ssrc: int) -> None:
        if not data:
            return
        with self._lock:
            if self._file.closed:
                return
            if self._device is None:
                self._device = device
                self._ssrc = ssrc
            elif device != self._device or ssrc != self._ssrc:
                return
            self._file.write(data)
            self.size += len(data)
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            if self._file.closed:
                return
            block = self.channels * 2
            header = struct.pack(
                "<4sI4s4sIHHIIHH4sI",
                b"RIFF",
                36 + self.size,
                b"WAVE",
                b"fmt ",
                16,
                1,
                self.channels,
                self.rate,
                self.rate * block,
                block,
                16,
                b"data",
                self.size,
            )
            self._file.seek(0)
            self._file.write(header)
            self._file.close()


async def _record_until_stop(
    client: TelegramClient,
    call: InputGroupCall,
    dc_id: int,
    start_ms: int,
    destination: Path,
    seat: dict,
) -> None:
    time_ms = start_ms
    stop = seat["stop"]
    with destination.open("ab") as handle:
        while not stop.is_set():
            request = GetFileRequest(
                InputGroupCallStream(call=call, time_ms=time_ms, scale=0),
                offset=0,
                limit=CHUNK_LIMIT,
            )
            try:
                result = await call_on_dc(client, dc_id, request)
            except RPCError as exc:
                name = exc.message or ""
                if name.startswith("FLOOD_WAIT_") or name == "TIME_TOO_BIG":
                    await asyncio.sleep(0.2)
                    continue
                print(f"Кусок эфира: {redact(name)}", flush=True)
                await asyncio.sleep(1)
                continue
            data = getattr(result, "bytes", b"") or b""
            if not data:
                await asyncio.sleep(0.2)
                continue
            handle.write(data)
            handle.flush()
            seat["bytes"] += len(data)
            time_ms += SEGMENT_MS
            print(f"кусок: {len(data)} байт, всего {seat['bytes']}", flush=True)


async def leave_joined_call() -> str:
    """Выходит из эфира, в котором сидит команда !str. Повторный вызов безопасен."""
    global _seat, _wav
    async with _seat_lock:
        seat = _seat
        wav = _wav
        _seat = None
        _wav = None
    if seat is None and wav is None:
        return "Сейчас не в эфире."
    if wav is not None:
        wav.close()
    if seat is not None and seat.get("stop") is not None:
        seat["stop"].set()
    task = None if seat is None else seat.get("task")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    chat_id = None if seat is None else seat.get("chat_id")
    if _calls is not None and chat_id is not None and seat is not None and seat.get("engine") == "tgcalls":
        try:
            await _calls.leave_call(chat_id)
        except NotInCallError:
            pass
        except Exception as exc:
            print(f"Выход из эфира: {redact(str(exc))}", flush=True)
    elif seat is not None and seat.get("call") is not None:
        try:
            await seat["client"](LeaveGroupCallRequest(seat["call"], seat["ssrc"]))
        except RPCError as exc:
            print(f"Выход из эфира: {redact(exc.message)}", flush=True)
    size = 0 if wav is None else wav.size
    if wav is not None and size:
        return f"Вышел из эфира. Записано {size} байт в {wav.path.name}."
    if wav is not None:
        return f"Вышел из эфира. Файл {wav.path.name} без звука: кадров не было."
    return "Вышел из эфира."


async def enter_and_wait(client: TelegramClient, entity: object, chat_id: int) -> str:
    """Входит в эфир чата без микрофона и остаётся там до !str 0."""
    global _seat
    if not isinstance(entity, (Channel, Chat)):
        return "Это не группа и не канал. Группового эфира здесь нет."
    try:
        if isinstance(entity, Channel):
            full = await client(GetFullChannelRequest(entity))
        else:
            full = await client(GetFullChatRequest(entity.id))
    except RPCError as exc:
        return f"Чат не прочитан: {redact(exc.message)}"

    raw_call = getattr(full.full_chat, "call", None)
    if raw_call is None:
        return "В этом чате нет активного эфира."
    try:
        details = await client(GetGroupCallRequest(raw_call, limit=1))
    except RPCError as exc:
        return f"Эфир не открылся: {redact(exc.message)}"

    group_call = details.call
    if isinstance(group_call, GroupCallDiscarded) or not isinstance(group_call, GroupCall):
        return "Эфир уже завершён."
    if group_call.conference:
        return "Это конференция. Запись этого типа эфира не подключена."
    if group_call.schedule_date is not None and int(group_call.participants_count or 0) == 0:
        return "Эфир только запланирован, сейчас его нет."

    async with _seat_lock:
        if _seat is not None and _seat["chat_id"] == chat_id:
            return "Уже сижу в эфире этого чата. Выход: !str 0"
    if _seat is not None:
        await leave_joined_call()
    if _calls is None:
        return "Движок звонка не запущен."

    wav = PcmWav(OUT_DIR / f"call_{entity.id}.wav")
    wav.chat_id = chat_id
    global _wav
    async with _seat_lock:
        _wav = wav
    try:
        await _calls.record(
            chat_id,
            RecordStream(audio=True),
            GroupCallConfig(auto_start=False),
        )
    except NoActiveGroupCall:
        async with _seat_lock:
            if _wav is wav:
                _wav = None
        wav.close()
        wav.path.unlink(missing_ok=True)
        return "В этом чате нет активного эфира."
    except Exception as exc:
        async with _seat_lock:
            if _wav is wav:
                _wav = None
        wav.close()
        wav.path.unlink(missing_ok=True)
        try:
            await _calls.leave_call(chat_id)
        except Exception:
            pass
        return f"Вход в эфир не удался: {redact(str(exc))}"
    try:
        await _calls.mute(chat_id)
    except Exception as exc:
        print(f"Микрофон: {redact(str(exc))}", flush=True)

    async with _seat_lock:
        _seat = {
            "client": client,
            "chat_id": chat_id,
            "stop": asyncio.Event(),
            "task": None,
            "engine": "tgcalls",
            "wav": wav,
        }
    return (
        f"Вошёл, участников {group_call.participants_count}. "
        f"Пишу разговор в {wav.path.name}, микрофон выключен. Выход: !str 0"
    )


async def tell_saved(client: TelegramClient, text: str) -> None:
    """Статус команд уходит в Избранное, а не ответом в чат."""
    await client.send_message("me", text)


async def _on_call_audio(_client: PyTgCalls, update: object) -> None:
    wav = _wav
    if wav is None or not isinstance(update, StreamFrames):
        return
    if update.chat_id != wav.chat_id or update.direction is None:
        return
    if not (update.direction & Direction.INCOMING):
        return
    if update.device not in (Device.MICROPHONE, Device.SPEAKER):
        return
    for frame in update.frames:
        wav.write(frame.frame or b"", update.device, frame.ssrc)


async def start_call_engine() -> None:
    if _calls is not None:
        await _calls.start()


def bind_stream_command(client: TelegramClient) -> None:
    """!str входит в эфир этого чата и пишет звук в wav. !str 0 выходит."""
    global _calls
    from telethon import events

    _calls = PyTgCalls(client)
    _calls.on_update()(_on_call_audio)

    @client.on(events.NewMessage(outgoing=True, pattern=r"(?i)^!str(?:\s+0)?\s*$"))
    async def on_stream_command(event: events.NewMessage.Event) -> None:
        text = (event.raw_text or "").strip().lower()
        print(f"Команда {text}", flush=True)
        try:
            chat = await event.get_chat()
            title = getattr(chat, "title", None) or "чат"
            if text == "!str 0":
                message = await leave_joined_call()
            else:
                await tell_saved(client, f"{title}: вхожу в эфир без микрофона и остаюсь там.")
                message = await enter_and_wait(client, chat, event.chat_id)
            print(message, flush=True)
            await tell_saved(client, f"{title}: {message}")
        except Exception as exc:
            message = redact(str(exc)) or "ошибка проверки"
            print(message, flush=True)
            await tell_saved(client, message)


async def probe(client: TelegramClient, pieces: int) -> int:
    rows = await active_calls(client)
    target = choose_call(rows)
    if target is None:
        print(
            "Вход не выполнялся: нет RTMP и нет эфира достаточно большого, "
            "чтобы сервер отдал stream mode.",
            flush=True,
        )
        return 0

    call = InputGroupCall(target["call_id"], target["access_hash"])
    ssrc = secrets.randbelow(2**31 - 1) + 1
    print(
        f"Вход без микрофона: {target['title']}, участников {target['participants']}",
        flush=True,
    )
    joined = False
    try:
        updates = await client(
            JoinGroupCallRequest(
                call=call,
                join_as=InputPeerSelf(),
                params=DataJSON(data=join_payload(ssrc)),
                muted=True,
                video_stopped=True,
            )
        )
        joined = True
        params = connection_params(updates)
        if not params or not params.get("stream"):
            print("Сервер оставил WebRTC. Куски stream mode недоступны.", flush=True)
            return 2
        rtmp = bool(params.get("rtmp"))
        dc_id = target["stream_dc_id"]
        if not dc_id:
            print("В ответе stream mode, но stream_dc_id пустой.", flush=True)
            return 2
        print(f"stream mode, rtmp={rtmp}, dc={dc_id}", flush=True)
        if rtmp:
            channels = await call_on_dc(
                client, dc_id, GetGroupCallStreamChannelsRequest(call)
            )
            if not channels.channels:
                print("RTMP-поток ещё без кусков.", flush=True)
                return 2
            last_ms = max(item.last_timestamp_ms for item in channels.channels)
            start_ms = (last_ms // SEGMENT_MS) * SEGMENT_MS - BUFFER_MS
        else:
            stamp = getattr(updates, "date", None)
            now_ms = int(stamp.timestamp() * 1000) if stamp else int(time.time() * 1000)
            start_ms = (now_ms // SEGMENT_MS) * SEGMENT_MS - BUFFER_MS
        if start_ms <= 0:
            print("Стартовая отметка куска ещё не готова.", flush=True)
            return 2
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        destination = OUT_DIR / "stream_audio.bin"
        size = await download_audio(client, call, dc_id, start_ms, pieces, destination)
        print(f"Записано {size} байт в {destination}", flush=True)
        return 0
    finally:
        if joined:
            try:
                await client(LeaveGroupCallRequest(call, ssrc))
                print("Из эфира вышли.", flush=True)
            except RPCError as exc:
                print(f"Выход из эфира: {redact(exc.message)}", flush=True)


async def run(pieces: int) -> int:
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
            print("Сессия не авторизована.")
            return 2
        return await probe(client, pieces)
    finally:
        if client.is_connected():
            await client.disconnect()


def main() -> int:
    install_quiet_stop()
    parser = argparse.ArgumentParser(description="Проба записи stream mode")
    parser.add_argument("--pieces", type=int, default=5, help="Сколько звуковых кусков снять")
    args = parser.parse_args()
    pieces = args.pieces if 1 <= args.pieces <= 30 else 5
    try:
        return asyncio.run(run(pieces))
    except KeyboardInterrupt:
        print("Проба остановлена.")
        return 0
    except RPCError as exc:
        print(redact(exc.message or exc.__class__.__name__))
        return 1
    except Exception as exc:
        print(redact(str(exc)) or exc.__class__.__name__)
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Проба остановлена.")
        sys.exit(0)
