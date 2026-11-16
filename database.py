"""Операции с облачной базой Neon PostgreSQL."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Json

from config import get_database_url

SSL_MODE = "verify-full"
CONNECT_TIMEOUT_SECONDS = 10
SSL_ROOT_CERT = "/etc/ssl/certs/ca-certificates.crt"
APPLICATION_NAME = "neon_bot"

_USERINFO = re.compile(r"(://)[^/@\s]+@")
_TOKEN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b")
_PASSWORD_KV = re.compile(r"(?i)(\bpassword=)('[^']*'|\"[^\"]*\"|\S+)")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_records (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    record_date DATE NOT NULL UNIQUE,
    data JSONB NOT NULL
)
"""

INSERT_SQL = """
INSERT INTO daily_records (record_date, data)
VALUES (%s, %s)
ON CONFLICT (record_date) DO NOTHING
RETURNING id, created_at, record_date, data
"""

SELECT_RECORDS_SQL = """
SELECT id, created_at, record_date, data
FROM daily_records
ORDER BY record_date DESC, id DESC
LIMIT %s
"""


class DatabaseError(Exception):
    """Ошибка базы. В тексте сообщения нет пароля и токена."""


def redact(text: str) -> str:
    cleaned = _USERINFO.sub(r"\1***@", text)
    cleaned = _PASSWORD_KV.sub(r"\1***", cleaned)
    cleaned = _TOKEN.sub("***", cleaned)
    return cleaned


def secure_conninfo(database_url: str) -> str:
    """Собирает параметры подключения с проверкой сертификата.

    sslmode из исходной строки не может ослабить проверку: итог всегда verify-full.
    """
    scheme = database_url.split("://", 1)[0].lower() if "://" in database_url else ""
    if scheme not in {"postgresql", "postgres"}:
        raise DatabaseError("DATABASE_URL должен начинаться с postgresql://")

    conninfo = make_conninfo(
        database_url,
        sslmode=SSL_MODE,
        sslrootcert=SSL_ROOT_CERT,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        application_name=APPLICATION_NAME,
    )
    if conninfo_to_dict(conninfo).get("sslmode") != SSL_MODE:
        raise DatabaseError("Не удалось включить проверку сертификата SSL")
    return conninfo


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    try:
        conn = psycopg.connect(secure_conninfo(get_database_url()), row_factory=dict_row)
    except DatabaseError:
        raise
    except RuntimeError:
        raise
    except psycopg.Error as exc:
        raise DatabaseError(
            "Не удалось подключиться к Neon PostgreSQL за "
            f"{CONNECT_TIMEOUT_SECONDS} с. Проверьте адрес, пароль и сеть. "
            f"Причина: {redact(str(exc))}"
        ) from None

    try:
        yield conn
        conn.commit()
    except psycopg.Error as exc:
        conn.rollback()
        raise DatabaseError(
            f"Ошибка запроса к базе. Причина: {redact(str(exc))}"
        ) from None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_tables() -> None:
    with get_connection() as conn:
        conn.execute(CREATE_TABLE_SQL)


def insert_record(record_date: date, data: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(record_date, datetime) or not isinstance(record_date, date):
        raise DatabaseError("record_date должен быть датой календаря")
    if not isinstance(data, dict):
        raise DatabaseError("data должен быть объектом JSON")

    with get_connection() as conn:
        row = conn.execute(INSERT_SQL, (record_date, Json(data))).fetchone()
    return dict(row) if row else None


def get_records(limit: int = 30) -> list[dict[str, Any]]:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise DatabaseError("limit должен быть целым числом от 1 до 500")

    with get_connection() as conn:
        rows = conn.execute(SELECT_RECORDS_SQL, (limit,)).fetchall()
    return [dict(row) for row in rows]


def get_latest_record() -> dict[str, Any] | None:
    rows = get_records(limit=1)
    return rows[0] if rows else None


def count_records() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM daily_records").fetchone()
    if row is None:
        raise DatabaseError("База не вернула количество записей")
    return int(row["total"])


def check_connection() -> None:
    with get_connection() as conn:
        row = conn.execute("SELECT 1 AS ok").fetchone()
    if row is None or int(row["ok"]) != 1:
        raise DatabaseError("База ответила неожиданно на проверочный запрос")


CREATE_USERBOT_SQL = """
CREATE TABLE IF NOT EXISTS userbot (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL,
    record_type TEXT NOT NULL,
    telegram_id BIGINT,
    title TEXT,
    username TEXT,
    chat_type TEXT,
    is_admin BOOLEAN,
    is_creator BOOLEAN,
    data JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_USERBOT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS userbot_user_id_idx ON userbot (user_id)
"""

INSERT_USERBOT_SQL = """
INSERT INTO userbot (
    user_id, record_type, telegram_id, title, username,
    chat_type, is_admin, is_creator, data
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def ensure_userbot_table(conn: psycopg.Connection) -> None:
    conn.execute(CREATE_USERBOT_SQL)
    columns = {
        row["column_name"]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'userbot'
            """
        ).fetchall()
    }
    if "user_id" in columns and "data" in columns:
        conn.execute(CREATE_USERBOT_INDEX_SQL)
        return
    total = conn.execute("SELECT COUNT(*) AS total FROM userbot").fetchone()
    if total is None or int(total["total"]) != 0:
        raise DatabaseError(
            "Таблица userbot уже занята другой схемой и в ней есть строки"
        )
    conn.execute("DROP TABLE userbot")
    conn.execute(CREATE_USERBOT_SQL)
    conn.execute(CREATE_USERBOT_INDEX_SQL)


def replace_userbot_snapshot(user_id: int, rows: list[dict[str, Any]]) -> int:
    """Заменяет предыдущий снимок этого user_id строками профиля и чатов."""
    if isinstance(user_id, bool) or not isinstance(user_id, int):
        raise DatabaseError("user_id должен быть целым числом")
    payload: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("data"), dict):
            raise DatabaseError("строка снимка должна содержать объект data")
        payload.append(
            (
                user_id,
                row["record_type"],
                row.get("telegram_id"),
                row.get("title"),
                row.get("username"),
                row.get("chat_type"),
                row.get("is_admin"),
                row.get("is_creator"),
                Json(row["data"]),
            )
        )

    with get_connection() as conn:
        ensure_userbot_table(conn)
        conn.execute("DELETE FROM userbot WHERE user_id = %s", (user_id,))
        if payload:
            conn.cursor().executemany(INSERT_USERBOT_SQL, payload)
    return len(payload)


def userbot_summary(user_id: int) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT record_type, chat_type, is_admin, is_creator, COUNT(*) AS total
            FROM userbot
            WHERE user_id = %s
            GROUP BY record_type, chat_type, is_admin, is_creator
            ORDER BY record_type, chat_type
            """,
            (user_id,),
        ).fetchall()
    return [dict(row) for row in rows]
