#!/usr/bin/env bash
# Thin wrapper: see scripts/setup.py for options.
cd "$(dirname "$0")/.." && exec python3 scripts/setup.py "$@"
