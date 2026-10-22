#!/bin/sh
set -eu

/app/.venv/bin/python /opt/condor-deploy/sync_assets.py --state /state

if [ -n "${TELEGRAM_TRADER_URL:-}" ]; then
    /app/.venv/bin/python /opt/condor-deploy/telegram_bootstrap.py --bind
    /app/.venv/bin/python /opt/condor-deploy/telegram_bootstrap.py &
fi

exec "$@"
