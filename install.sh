#!/usr/bin/env bash
# Спрашивает ключи Telegram и строку Neon, клонирует репозиторий при необходимости,
# создаёт .venv и ставит библиотеки. Секреты остаются только в .env.
set -euo pipefail

trap 'printf "\nУстановка прервана.\n"; exit 130' INT TERM

REPO_URL="https://github.com/mahmudproduction/savetgchat.git"

say() {
  printf '%s\n' "$*"
}

ask() {
  local value
  read -r -p "$1" value
  printf '%s' "$value"
}

ask_secret() {
  local value
  read -r -s -p "$1" value
  printf '\n'
  printf '%s' "$value"
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    say "Не найдена команда: $1"
    exit 1
  fi
}

SOURCE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SOURCE/userbot.py" && -f "$SOURCE/requirements.txt" ]]; then
  ROOT="$SOURCE"
else
  ROOT="${1:-$HOME/savetgchat}"
  if [[ ! -f "$ROOT/userbot.py" ]]; then
    need_cmd git
    say "Клонирую репозиторий в $ROOT"
    git clone "$REPO_URL" "$ROOT"
  fi
fi

cd "$ROOT"
need_cmd python3

if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
  say "Нужен Python 3.10 или новее."
  exit 1
fi

say ""
say "Ключи API берутся на https://my.telegram.org в разделе API development tools."
say "Пароль и строка базы в терминале не повторяются."
say ""

while true; do
  API_ID="$(ask "API ID: ")"
  if [[ "$API_ID" =~ ^[0-9]+$ ]]; then
    break
  fi
  say "API ID состоит только из цифр."
done

while true; do
  API_HASH="$(ask_secret "API hash: ")"
  if [[ -n "$API_HASH" ]]; then
    break
  fi
  say "API hash не может быть пустым."
done

while true; do
  PHONE="$(ask "Номер телефона, например +905551112233: ")"
  PHONE="${PHONE// /}"
  if [[ "$PHONE" =~ ^\+[0-9]{8,15}$ ]]; then
    break
  fi
  say "Номер начинается с + и дальше только цифры."
done

say ""
say "Облачный пароль — это пароль двухэтапной проверки Telegram."
say "Если он не включён, ответьте н."
PASSWORD=""
PASSWORD_HINT=""
while true; do
  HAS_PASSWORD="$(ask "Облачный пароль есть? [д/н]: ")"
  case "${HAS_PASSWORD,,}" in
    д|y|yes|да)
      PASSWORD_HINT="$(ask "Подсказка, которую показывает Telegram: ")"
      while true; do
        PASSWORD="$(ask_secret "Облачный пароль: ")"
        AGAIN="$(ask_secret "Повторите облачный пароль: ")"
        if [[ "$PASSWORD" == "$AGAIN" && -n "$PASSWORD" ]]; then
          break
        fi
        say "Пароли не совпали или пустые. Введите ещё раз."
      done
      break
      ;;
    н|n|no|нет|"")
      break
      ;;
    *)
      say "Ответьте д или н."
      ;;
  esac
done

say ""
say "Строка Neon: console.neon.tech → проект → Connect."
while true; do
  DATABASE_URL="$(ask_secret "Строка подключения Neon: ")"
  if [[ "$DATABASE_URL" == postgresql://* || "$DATABASE_URL" == postgres://* ]]; then
    break
  fi
  say "Строка должна начинаться с postgresql:// или postgres://."
done

umask 077
python3 - "$API_ID" "$API_HASH" "$PHONE" "$PASSWORD" "$PASSWORD_HINT" "$DATABASE_URL" <<'PY'
import sys
from pathlib import Path

keys = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE",
    "TELEGRAM_PASSWORD",
    "TELEGRAM_PASSWORD_HINT",
    "DATABASE_URL",
)
values = dict(zip(keys, sys.argv[1:]))

def quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

lines = [
    f"DATABASE_URL={quote(values['DATABASE_URL'])}",
    "TELEGRAM_BOT_TOKEN=",
    f"TELEGRAM_API_ID={quote(values['TELEGRAM_API_ID'])}",
    f"TELEGRAM_API_HASH={quote(values['TELEGRAM_API_HASH'])}",
    f"TELEGRAM_PHONE={quote(values['TELEGRAM_PHONE'])}",
    f"TELEGRAM_PASSWORD={quote(values['TELEGRAM_PASSWORD'])}",
    f"TELEGRAM_PASSWORD_HINT={quote(values['TELEGRAM_PASSWORD_HINT'])}",
]
Path(".env").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
chmod 600 .env
unset DATABASE_URL API_HASH PASSWORD AGAIN

say ""
say "Создаю виртуальное окружение и ставлю библиотеки."
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
mkdir -p log img incl tpl

ENABLE="$(ask "Добавить юзербота в автозапуск этого пользователя? [Д/н]: ")"
case "${ENABLE,,}" in
  н|n|no|нет)
    say "Автозапуск не включён. Запуск вручную: .venv/bin/python userbot.py"
    ;;
  *)
    UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
    mkdir -p "$UNIT_DIR"
    cat > "$UNIT_DIR/savetgchat.service" <<EOF
[Unit]
Description=Юзербот savetgchat (Telethon)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${ROOT}
ExecStart=${ROOT}/.venv/bin/python -u ${ROOT}/userbot.py
Restart=on-failure
RestartSec=5
SuccessExitStatus=2
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable --now savetgchat.service
    say "Служба savetgchat запущена. Код входа Telegram, если он понадобится, придёт в приложение."
    say "После кода: .venv/bin/python userbot.py --code КОД"
    ;;
esac

say ""
say "Готово. Секреты лежат в ${ROOT}/.env и в Git не попадают."
say "В чате с живым эфиром: !str пишет звук в log/call_<id>.wav, !str 0 останавливает запись."
