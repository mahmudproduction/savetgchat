"""Проверка SSL-параметров и, если задан DATABASE_URL, работы с Neon."""

from __future__ import annotations

import signal
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

from psycopg.conninfo import conninfo_to_dict

from config import NEON_SETUP_HELP, get_database_url
from database import (
    SSL_MODE,
    DatabaseError,
    check_connection,
    count_records,
    create_tables,
    get_latest_record,
    get_records,
    insert_record,
    redact,
    secure_conninfo,
)


def install_quiet_stop() -> None:
    def _stop(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def fail(message: str) -> int:
    print(message)
    return 1


def check_redaction() -> int | None:
    secret = "SuperSecretPassword"
    token = "123456789:AAFakeTokenValueForRedactionOnly000"
    sample = (
        f"postgresql://appuser:{secret}@ep-example.neon.tech/neondb "
        f"password={secret} token={token}"
    )
    cleaned = redact(sample)
    if secret in cleaned or token in cleaned:
        return fail("Очистка текста не убрала секрет.")
    print("Локально: пароль и токен вырезаются из текста ошибки.")
    return None


def check_ssl_policy() -> int | None:
    weakened = "postgresql://appuser:not-printed@example.neon.tech/neondb?sslmode=disable"
    required = "postgresql://appuser:not-printed@example.neon.tech/neondb?sslmode=require"
    for source in (weakened, required):
        params = conninfo_to_dict(secure_conninfo(source))
        if params.get("sslmode") != SSL_MODE:
            return fail("sslmode не закрепился как verify-full.")
        if str(params.get("connect_timeout")) != "10":
            return fail("Таймаут подключения не равен 10 секундам.")
        if params.get("sslrootcert") != "/etc/ssl/certs/ca-certificates.crt":
            return fail("Не указано хранилище доверенных сертификатов.")
    try:
        secure_conninfo("mysql://appuser:not-printed@localhost/db")
    except DatabaseError:
        pass
    else:
        return fail("Чужая схема базы не была отклонена.")
    print("Локально: sslmode=verify-full, таймаут 10 с, проверка сертификата не отключается.")
    return None


def check_date_guard() -> int | None:
    try:
        insert_record(datetime.now(), {"source": "invalid"})
    except DatabaseError:
        pass
    except RuntimeError:
        return fail("Проверка типа даты дошла до подключения.")
    else:
        return fail("datetime принят вместо календарной даты.")
    try:
        insert_record(date.today(), ["не объект"])  # type: ignore[arg-type]
    except DatabaseError:
        pass
    except RuntimeError:
        return fail("Проверка типа JSON дошла до подключения.")
    else:
        return fail("Список принят вместо JSON-объекта.")
    print("Локально: дата и JSON проверяются до подключения.")
    return None


def check_neon() -> int:
    try:
        get_database_url()
    except RuntimeError:
        print(NEON_SETUP_HELP)
        print("Подключение к Neon, создание таблицы и тестовая запись не выполнялись: DATABASE_URL пуст.")
        return 2

    try:
        check_connection()
        print("Подключение к Neon установлено.")
        create_tables()
        print("Таблица daily_records создана или уже была.")
        today = datetime.now(ZoneInfo("Europe/Istanbul")).date()
        row = insert_record(
            today,
            {"source": "test_database", "note": "проверочная запись"},
        )
        if row is None:
            print(f"Запись за {today.isoformat()} уже существовала, дубликат не создан.")
        else:
            print(f"Создана запись id={row['id']} за {row['record_date']}.")
        latest = get_latest_record()
        rows = get_records(limit=5)
        total = count_records()
    except DatabaseError as exc:
        print(exc)
        return 1

    if latest is None or total < 1 or not rows:
        return fail("Чтение записей не вернуло данные после вставки.")
    print(f"Чтение: последняя дата {latest['record_date']}, всего записей {total}.")
    return 0


def main() -> int:
    install_quiet_stop()
    for check in (check_redaction, check_ssl_policy, check_date_guard):
        result = check()
        if result is not None:
            return result
    return check_neon()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
