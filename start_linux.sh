#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
if [[ ! -x ".venv/bin/python" ]]; then
  echo "[ERROR] .venv not found. Create the environment and install requirements.txt first."
  exit 1
fi
exec .venv/bin/python bot.py
