#!/bin/sh
set -eu

/app/.venv/bin/python /opt/condor-deploy/sync_assets.py \
  --assets /opt/condor-assets \
  --state /state

exec "$@"
