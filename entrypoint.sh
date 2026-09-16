#!/bin/sh
set -eu

/app/.venv/bin/python /opt/condor-deploy/sync_assets.py --state /state

exec "$@"
