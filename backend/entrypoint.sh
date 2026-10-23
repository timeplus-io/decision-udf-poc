#!/bin/sh
# entrypoint.sh — verify the bundled model digest, then start llama-server.
set -eu

if [ -n "${MODEL_SHA256:-}" ]; then
  actual="$(sha256sum "$MODEL_PATH" | cut -d' ' -f1)"
  if [ "$actual" != "$MODEL_SHA256" ]; then
    echo "model digest mismatch: expected $MODEL_SHA256 got $actual" >&2
    exit 1
  fi
  echo "model digest ok: $actual" >&2
fi

exec llama-server -m "$MODEL_PATH" "$@"
