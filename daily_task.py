"""Одна тестовая запись в Neon за календарные сутки Europe/Istanbul.

Запуск вручную пишет запись и завершается.
Флаг --schedule держит планировщик в текущем терминале до Ctrl+C.
Служба systemd и фоновый запуск не настраиваются.
"""

from __future__ import annotations

import argparse
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import NEON_SETUP_HELP
from database import DatabaseError, insert_record, redact

TIMEZONE_NAME = "Europe/Istanbul"
TZ = ZoneInfo(TIMEZONE_NAME)


def install_quiet_stop() -> None:
    def _stop(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def run_once() -> int:
    """Пишет одну запись на сегодняшнюю дату Стамбула. Повтор в тот же день пропускается."""
    today = datetime.now(TZ).date()
    payload = {
        "source": "daily_task",
        "timezone": TIMEZONE_NAME,
        "note": "тестовая ежедневная запись",
    }
    try:
        row = insert_record(today, payload)
    except RuntimeError as exc:
        if "DATABASE_URL" in str(exc):
            print(NEON_SETUP_HELP)
            return 2
        print(redact(str(exc)))
        return 1
    except DatabaseError as exc:
        print(exc)
        return 1

    if row is None:
        print(f"Запись за {today.isoformat()} уже есть. Повторно не записываю.")
        return 0

    print(f"Записано: id={row['id']}, дата={row['record_date']}.")
    return 0


def run_scheduler() -> int:
    """Планировщик только в этом процессе. После закрытия терминала он останавливается."""
    scheduler = BlockingScheduler(timezone=TZ)
    scheduler.add_job(
        run_once,
        CronTrigger(hour=0, minute=5, timezone=TZ),
        id="daily_record",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    print(
        "Планировщик запущен в этом терминале: каждый день в 00:05 Europe/Istanbul. "
        "Ctrl+C останавливает процесс. systemd не используется."
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Планировщик остановлен.")
        return 0
    return 0


def main(argv: list[str] | None = None) -> int:
    install_quiet_stop()
    parser = argparse.ArgumentParser(
        description="Записать одну тестовую строку в Neon за текущие сутки."
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Держать планировщик в этом терминале. Службу не создаёт.",
    )
    args = parser.parse_args(argv)
    if args.schedule:
        return run_scheduler()
    return run_once()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
