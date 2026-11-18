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
import os
import secrets
import signal
import struct
import sys
import threading
import time
from array import array
from pathlib import Path

from pytgcalls import PyTgCalls
from pytgcalls.exceptions import NoActiveGroupCall, NotInCallError
from pytgcalls.types import Device, Direction, GroupCallConfig, RecordStream, StreamFrames

from telethon import TelegramClient, utils
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
    PeerChannel,
    PeerChat,
    UpdateGroupCall,
    UpdateGroupCallConnection,
    UpdateGroupCallParticipants,
    UpdateNewAuthorization,
)

from config import USERBOT_HELP, get_telegram_api_hash, get_telegram_api_id, get_telegram_phone
from database import redact

ROOT = Path(__file__).resolve().parent
SESSION = ROOT / "userbot"
OUT_DIR = ROOT / "log"
SEGMENT_MS = 1000
BUFFER_MS = 2000
CHUNK_LIMIT = 1024 * 1024
SEGMENT_SECONDS = 30 * 60
PCM_RATE = 48000
PCM_CHANNELS = 2
PCM_BYTES_PER_SECOND = PCM_RATE * PCM_CHANNELS * 2
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
_expect_drop = False
_drop_lock = asyncio.Lock()
_self_id: int | None = None
_shutdown = asyncio.Event()
_pending_choice: dict | None = None
_other_session: dict | None = None


def wav_header(size: int, rate: int = PCM_RATE, channels: int = PCM_CHANNELS) -> bytes:
    block = channels * 2
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + size,
        b"WAVE",
        b"fmt ",
        16,
        1,
        channels,
        rate,
        rate * block,
        block,
        16,
        b"data",
        size,
    )


def safe_chat_title(title: str) -> str:
    """Имя файла без символов, которые ломают путь. Эпохи дописываются следом."""
    cleaned = []
    for char in title.strip():
        if char in '\\/:*?"<>|' or ord(char) < 32:
            continue
        cleaned.append(char)
    text = "".join(cleaned).strip().strip(".")
    return (text or "chat")[:80]


def segment_filename(title: str, started_at: float, stopped_at: float) -> str:
    start = int(started_at)
    stop = max(start, int(stopped_at))
    return f"{safe_chat_title(title)}{start}{stop}.wav"


def repair_wav(path: Path) -> int:
    """Дописывает заголовок, если процесс остановился до close. Возвращает байты PCM."""
    with path.open("r+b") as handle:
        marker = handle.read(12)
        handle.seek(0, os.SEEK_END)
        total = handle.tell()
        if marker.startswith(b"RIFF"):
            return max(0, total - 44)
        if total <= 44:
            return 0
        size = total - 44
        handle.seek(0)
        handle.write(wav_header(size))
        return size


class PcmWav:
    """Пишет PCM 16 бит в wav без ffmpeg."""

    def __init__(self, path: Path, rate: int = PCM_RATE, channels: int = PCM_CHANNELS) -> None:
        self.path = path
        self.rate = rate
        self.channels = channels
        self.size = 0
        self.chat_id: int | None = None
        self.chat_title = "chat"
        self.started_at = time.time()
        self._device = None
        self._known: set[int] = set()
        self._tracks: dict[int, tuple[int, array]] = {}
        self._origin_ms: int | None = None
        self._use_time = False
        self._emitted = 0
        self._delay_frames = self.rate // 5
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("wb")
        self._file.write(b"\0" * 44)

    def write(self, data: bytes, device: Device, ssrc: int, capture_ms: int = 0) -> None:
        if not data:
            return
        with self._lock:
            if self._file.closed:
                return
            if self._device is None:
                self._device = device
            elif device != self._device:
                return
            self._mix(data, ssrc, capture_ms, final=False)

    def close(self) -> None:
        with self._lock:
            if self._file.closed:
                return
            self._mix(b"", 0, 0, final=True)
            self._file.seek(0)
            self._file.write(wav_header(self.size, self.rate, self.channels))
            self._file.close()

    def _mix(self, data: bytes, ssrc: int, capture_ms: int, final: bool) -> None:
        channels = self.channels
        width = channels * 2
        if data:
            data = data[: len(data) - (len(data) % width)]
        if data:
            incoming = array("h")
            incoming.frombytes(data)
            frames = len(incoming) // channels
            if self._origin_ms is None:
                self._origin_ms = capture_ms if capture_ms else 0
                self._use_time = bool(capture_ms)
            if self._use_time and capture_ms:
                pos = int((capture_ms - self._origin_ms) * self.rate / 1000)
                if pos < 0:
                    pos = 0
            else:
                current = self._tracks.get(ssrc)
                pos = self._emitted if current is None else current[0] + len(current[1]) // channels
            if pos < self._emitted:
                skip = self._emitted - pos
                if skip < frames:
                    incoming = incoming[skip * channels :]
                    pos = self._emitted
                    frames = len(incoming) // channels
                else:
                    incoming = array("h")
                    frames = 0
            if frames:
                if ssrc not in self._known:
                    self._known.add(ssrc)
                    print(f"В записи голосов: {len(self._known)}", flush=True)
                self._place(ssrc, pos, incoming)
        if not self._tracks:
            return
        horizon = max(start + len(samples) // channels for start, samples in self._tracks.values())
        limit = horizon if final else horizon - self._delay_frames
        if limit <= self._emitted:
            return
        count = limit - self._emitted
        mixed = [0] * (count * channels)
        for start, samples in self._tracks.values():
            begin = max(self._emitted, start)
            end = min(limit, start + len(samples) // channels)
            if begin >= end:
                continue
            src = (begin - start) * channels
            dst = (begin - self._emitted) * channels
            span = (end - begin) * channels
            for index in range(span):
                mixed[dst + index] += samples[src + index]
        out = array("h", (0,)) * (count * channels)
        for index, value in enumerate(mixed):
            if value > 32767:
                value = 32767
            elif value < -32768:
                value = -32768
            out[index] = value
        payload = out.tobytes()
        self._file.write(payload)
        self.size += len(payload)
        self._file.flush()
        self._emitted = limit
        kept: dict[int, tuple[int, array]] = {}
        for key, (start, samples) in self._tracks.items():
            total = len(samples) // channels
            if start + total <= self._emitted:
                continue
            drop = self._emitted - start
            if drop > 0:
                del samples[: drop * channels]
                start = self._emitted
            kept[key] = (start, samples)
        self._tracks = kept

    def _place(self, ssrc: int, pos: int, incoming: array) -> None:
        channels = self.channels
        current = self._tracks.get(ssrc)
        if current is None:
            self._tracks[ssrc] = (pos, incoming)
            return
        start, samples = current
        end = start + len(samples) // channels
        frames = len(incoming) // channels
        if pos > end:
            samples.extend([0] * ((pos - end) * channels))
        elif pos < start:
            shift = (start - pos) * channels
            blank = array("h", [0] * shift)
            blank.extend(samples)
            samples = blank
            start = pos
        local = (pos - start) * channels
        need = local + len(incoming)
        if need > len(samples):
            samples.extend([0] * (need - len(samples)))
        samples[local : local + len(incoming)] = incoming
        self._tracks[ssrc] = (start, samples)


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


def open_recording(title: str, chat_id: int) -> PcmWav:
    started = time.time()
    path = OUT_DIR / f".recording_{chat_id}_{int(started)}.wav"
    wav = PcmWav(path)
    wav.chat_id = chat_id
    wav.chat_title = title or "chat"
    wav.started_at = started
    return wav


def active_recording_path() -> Path | None:
    wav = _wav
    if wav is None:
        return None
    return wav.path


async def deliver_recording(
    client: TelegramClient,
    wav: PcmWav,
    caption: str | None = None,
) -> str:
    """Закрывает wav, шлёт его в Избранное и удаляет локальную копию после успеха."""
    wav.close()
    if wav.size <= 0 or not wav.path.exists():
        wav.path.unlink(missing_ok=True)
        return "Сегмент без звука. Файл в Избранное не отправлял."
    stopped = time.time()
    final = OUT_DIR / segment_filename(wav.chat_title, wav.started_at, stopped)
    if final.exists():
        final = OUT_DIR / f"{final.stem}_{int(stopped)}.wav"
    wav.path.replace(final)
    wav.path = final
    try:
        await client.send_file(
            "me",
            str(final),
            caption=caption or final.name,
            force_document=True,
        )
    except Exception as exc:
        return f"Не отправился, файл оставлен в log: {final.name}. {redact(str(exc))}"
    try:
        final.unlink()
    except OSError as exc:
        return f"Файл отправлен, но с диска не удалился: {final.name}. {exc}"
    return f"Отправлен в Избранное и удалён с диска: {final.name}"


async def roll_segment() -> None:
    """Закрывает текущие 30 минут и сразу продолжает запись следующим файлом."""
    global _wav
    async with _seat_lock:
        if _seat is None or _wav is None:
            return
        old = _wav
        client = _seat["client"]
        fresh = open_recording(old.chat_title, int(old.chat_id or 0))
        _wav = fresh
        _seat["wav"] = fresh
    note = await deliver_recording(client, old)
    print(note, flush=True)
    await tell_saved(client, note)


async def _segment_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=SEGMENT_SECONDS)
        except asyncio.TimeoutError:
            if stop.is_set():
                return
            try:
                await roll_segment()
            except Exception as exc:
                print(f"Смена файла: {redact(str(exc))}", flush=True)
            continue
        return


def shutdown_requested() -> bool:
    return _shutdown.is_set()


async def request_shutdown(client: TelegramClient) -> None:
    """Останавливает юзербота без перезапуска службой: выход 0."""
    _shutdown.set()
    if client.is_connected():
        await client.disconnect()


def _remember_other_session(update: UpdateNewAuthorization) -> None:
    global _other_session
    _other_session = {
        "at": time.time(),
        "device": (update.device or "").strip(),
        "location": (update.location or "").strip(),
    }
    print("Замечено подключение другой сессии.", flush=True)


def _session_note() -> str:
    info = _other_session
    if info is None or time.time() - float(info["at"]) > 180:
        return "Подключение с другой сессии выкинуло из записываемого эфира."
    lines = ["Подключение с другой сессии выкинуло из записываемого эфира."]
    if info["device"]:
        lines.append(f"Устройство: {info['device']}")
    if info["location"]:
        lines.append(f"Место: {info['location']}")
    return "\n".join(lines)


async def _stop_seat_tasks(seat: dict | None) -> None:
    if seat is None:
        return
    current = asyncio.current_task()
    for task in (seat.get("task"), seat.get("watch")):
        if task is None or task is current:
            continue
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _hang_up(seat: dict | None) -> None:
    if seat is None:
        return
    chat_id = seat.get("chat_id")
    if _calls is not None and chat_id is not None and seat.get("engine") == "tgcalls":
        try:
            await _calls.leave_call(chat_id)
        except NotInCallError:
            pass
        except Exception as exc:
            print(f"Выход из эфира: {redact(str(exc))}", flush=True)
    elif seat.get("call") is not None:
        try:
            await seat["client"](LeaveGroupCallRequest(seat["call"], seat["ssrc"]))
        except RPCError as exc:
            print(f"Выход из эфира: {redact(exc.message)}", flush=True)


def _choice_text(title: str) -> str:
    return (
        f"Вернуться в записываемый чат «{title}» — 0\n"
        "Завершить работу юзербота — 1"
    )


async def dropped_by_other_session() -> None:
    """Эфир оборвался не нашей командой: файл в Избранное и вопрос 0 или 1."""
    global _seat, _wav, _expect_drop, _pending_choice
    if _expect_drop or _shutdown.is_set():
        return
    async with _drop_lock:
        if _expect_drop or _seat is None:
            return
        _expect_drop = True
        try:
            async with _seat_lock:
                seat = _seat
                wav = _wav
                _seat = None
                _wav = None
                if seat is not None and seat.get("stop") is not None:
                    seat["stop"].set()
            if seat is None:
                return
            await _stop_seat_tasks(seat)
            await _hang_up(seat)
            title = "чат"
            chat_id = seat.get("chat_id")
            if wav is not None:
                title = wav.chat_title or title
                chat_id = wav.chat_id if wav.chat_id is not None else chat_id
            note = _session_note()
            question = _choice_text(str(title))
            sent = "Файл записи не открыт."
            if wav is not None:
                sent = await deliver_recording(
                    seat["client"],
                    wav,
                    caption=f"{note}\n\n{question}",
                )
            _pending_choice = {
                "chat_id": chat_id,
                "title": str(title),
                "client": seat["client"],
            }
            text = f"{note}\n{sent}\n{question}"
            print(text, flush=True)
            await tell_saved(seat["client"], text)
        finally:
            _expect_drop = False


async def _watch_call(stop: asyncio.Event, chat_id: int) -> None:
    """Если звонок исчез без команды выхода, это выход из другой сессии."""
    await asyncio.sleep(5)
    missing = 0
    while not stop.is_set():
        await asyncio.sleep(2)
        if stop.is_set() or _expect_drop or _calls is None:
            continue
        try:
            active = _calls._binding.calls()
            if asyncio.iscoroutine(active):
                active = await active
        except Exception:
            continue
        if chat_id in active:
            missing = 0
            continue
        missing += 1
        if missing >= 2:
            await dropped_by_other_session()
            return


async def leave_joined_call() -> str:
    """Выходит из эфира. Текущий кусок уходит в Избранное и удаляется после отправки."""
    global _seat, _wav, _expect_drop
    _expect_drop = True
    try:
        async with _seat_lock:
            seat = _seat
            wav = _wav
            _seat = None
            _wav = None
            if seat is not None and seat.get("stop") is not None:
                seat["stop"].set()
        if seat is None and wav is None:
            return "Сейчас не в эфире."
        await _stop_seat_tasks(seat)
        sent = "Файл записи не открыт."
        if wav is not None and seat is not None:
            try:
                sent = await deliver_recording(seat["client"], wav)
            except Exception as exc:
                sent = redact(str(exc)) or "ошибка отправки"
            print(sent, flush=True)
        await _hang_up(seat)
        return f"Вышел из эфира. {sent}"
    finally:
        _expect_drop = False


async def legacy_title(client: TelegramClient, path: Path) -> str:
    name = path.name
    if not (name.startswith("call_") and name.endswith(".wav")):
        return "chat"
    raw = name[len("call_") : -len(".wav")]
    head = raw.split("_", 1)[0]
    if not head.isdigit():
        return "chat"
    chat_id = int(head)
    for peer in (PeerChannel(chat_id), PeerChat(chat_id)):
        try:
            entity = await client.get_entity(peer)
        except Exception:
            continue
        title = getattr(entity, "title", None)
        if title:
            return str(title)
    return f"chat{chat_id}"


async def send_finished_file(client: TelegramClient, path: Path, title: str | None) -> None:
    if not path.is_file():
        return
    if active_recording_path() == path:
        return
    pcm = repair_wav(path)
    if pcm <= 0:
        path.unlink(missing_ok=True)
        return
    stopped = path.stat().st_mtime
    started = stopped - (pcm / PCM_BYTES_PER_SECOND)
    label = title or await legacy_title(client, path)
    final = OUT_DIR / segment_filename(label, started, stopped)
    if final.resolve() != path.resolve():
        if final.exists():
            final = OUT_DIR / f"{final.stem}_{int(stopped)}.wav"
        path.replace(final)
    try:
        await client.send_file("me", str(final), caption=final.name, force_document=True)
    except Exception as exc:
        note = f"Не отправился, файл оставлен в log: {final.name}. {redact(str(exc))}"
        print(note, flush=True)
        await tell_saved(client, note)
        return
    try:
        final.unlink()
    except OSError as exc:
        note = f"Файл отправлен, но с диска не удалился: {final.name}. {exc}"
        print(note, flush=True)
        await tell_saved(client, note)
        return
    note = f"Отправлен в Избранное и удалён с диска: {final.name}"
    print(note, flush=True)
    await tell_saved(client, note)


async def recover_local_wavs(client: TelegramClient) -> None:
    """Отправляет wav, оставшиеся после прошлой записи, и удаляет их после успеха."""
    try:
        paths = sorted(OUT_DIR.glob("*.wav")) + sorted(OUT_DIR.glob(".recording_*.wav"))
        for path in paths:
            await send_finished_file(client, path, None)
    except Exception as exc:
        print(f"Разбор старых записей: {redact(str(exc))}", flush=True)


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
            return "Уже сижу в эфире этого чата. Каждые 30 минут файл уходит в Избранное. Выход: !rec 0"
    if _seat is not None:
        await leave_joined_call()
    if _calls is None:
        return "Движок звонка не запущен."

    title = getattr(entity, "title", None) or "чат"
    wav = open_recording(str(title), chat_id)
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

    stop = asyncio.Event()
    timer = asyncio.create_task(_segment_loop(stop))
    watch = asyncio.create_task(_watch_call(stop, chat_id))
    async with _seat_lock:
        _seat = {
            "client": client,
            "chat_id": chat_id,
            "stop": stop,
            "task": timer,
            "watch": watch,
            "engine": "tgcalls",
            "wav": wav,
        }
    return (
        f"Вошёл, участников {group_call.participants_count}. "
        "Пишу по 30 минут. Каждый файл придёт в Избранное и удалится с диска. "
        "Микрофон выключен. Раньше: !rec 0"
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
        capture = int(getattr(getattr(frame, "info", None), "capture_time", 0) or 0)
        wav.write(frame.frame or b"", update.device, frame.ssrc, capture)


async def start_call_engine() -> None:
    global _self_id
    if _calls is None:
        return
    await _calls.start()
    me = await _calls.mtproto_client.get_me()
    if me is not None:
        _self_id = int(me.id)


def rec_target(raw: str) -> str:
    """Оставляет имя чата из !rec: без @, схемы и хвоста ссылки t.me."""
    text = raw.strip()
    lower = text.lower()
    for prefix in ("https://", "http://"):
        if lower.startswith(prefix):
            text = text[len(prefix) :]
            lower = text.lower()
            break
    for prefix in ("t.me/", "telegram.me/"):
        if lower.startswith(prefix):
            text = text[len(prefix) :]
            break
    text = text.split("/", 1)[0].split("?", 1)[0].strip()
    if text.startswith("@"):
        text = text[1:]
    return text


async def answer_resume(client: TelegramClient, choice: str) -> None:
    """0 возвращает в тот же эфир, 1 останавливает юзербота."""
    global _pending_choice
    pending = _pending_choice
    if pending is None:
        return
    _pending_choice = None
    title = str(pending["title"])
    chat_id = pending["chat_id"]
    if choice == "1":
        await tell_saved(client, "Останавливаю юзербота.")
        await request_shutdown(client)
        return
    await tell_saved(client, f"Возвращаюсь в «{title}».")
    try:
        entity = await client.get_entity(chat_id)
    except Exception as exc:
        _pending_choice = pending
        await tell_saved(
            client,
            f"Чат не открылся: {redact(str(exc)) or 'нет доступа'}\n{_choice_text(title)}",
        )
        return
    message = await enter_and_wait(client, entity, int(chat_id))
    if message.startswith("Вошёл"):
        await tell_saved(client, f"{title}: {message}")
        return
    _pending_choice = pending
    await tell_saved(client, f"{title}: {message}\n{_choice_text(title)}")


def bind_stream_command(client: TelegramClient) -> None:
    """!str пишет эфир текущего чата. !rec имя в Избранном пишет эфир этого чата."""
    global _calls
    from telethon import events

    _calls = PyTgCalls(client)
    _calls.on_update()(_on_call_audio)
    saved_chat_id: list[int | None] = [None]

    async def in_saved(event: events.NewMessage.Event) -> bool:
        if saved_chat_id[0] is None:
            me = await client.get_me()
            saved_chat_id[0] = int(me.id) if me is not None else 0
        return bool(event.out and event.is_private and event.chat_id == saved_chat_id[0])

    @client.on(events.Raw)
    async def on_raw_call(update: object) -> None:
        if isinstance(update, UpdateNewAuthorization):
            _remember_other_session(update)
            return
        if _self_id is None or _expect_drop or _seat is None:
            return
        if not isinstance(update, UpdateGroupCallParticipants):
            return
        for participant in update.participants:
            peer = getattr(participant, "peer", None)
            if getattr(peer, "user_id", None) != _self_id:
                continue
            if getattr(participant, "left", False):
                await dropped_by_other_session()
                return

    @client.on(events.NewMessage(outgoing=True, pattern=r"^\s*([01])\s*$"))
    async def on_digit(event: events.NewMessage.Event) -> None:
        if _pending_choice is None or not await in_saved(event):
            return
        digit = event.pattern_match.group(1)
        print(f"Выбор {digit}", flush=True)
        try:
            await answer_resume(client, digit)
        except Exception as exc:
            message = redact(str(exc)) or "ошибка выбора"
            print(message, flush=True)
            await tell_saved(client, message)

    @client.on(events.NewMessage(outgoing=True, pattern=r"(?i)^!rec(?:\s+(\S+))?\s*$"))
    async def on_saved_record(event: events.NewMessage.Event) -> None:
        if not await in_saved(event):
            return
        raw = event.pattern_match.group(1) or ""
        target = rec_target(raw)
        print("Команда !rec", flush=True)
        try:
            if not target:
                await tell_saved(
                    client,
                    "Формат: !rec имя_чата. Пример: !rec debati_life. "
                    "Каждые 30 минут файл приходит в Избранное. Стоп: !rec 0",
                )
                return
            if target == "0":
                message = await leave_joined_call()
                print(message, flush=True)
                await tell_saved(client, message)
                return
            try:
                entity = await client.get_entity(target)
            except Exception as exc:
                message = f"Чат не найден: {redact(str(exc)) or 'нет такого имени'}"
                print(message, flush=True)
                await tell_saved(client, message)
                return
            title = getattr(entity, "title", None) or target
            chat_id = utils.get_peer_id(entity)
            await tell_saved(client, f"{title}: вхожу в эфир без микрофона и остаюсь там.")
            message = await enter_and_wait(client, entity, chat_id)
            print(message, flush=True)
            await tell_saved(client, f"{title}: {message}")
        except Exception as exc:
            message = redact(str(exc)) or "ошибка проверки"
            print(message, flush=True)
            await tell_saved(client, message)

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
