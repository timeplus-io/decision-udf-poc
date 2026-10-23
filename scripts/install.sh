#!/usr/bin/env bash
# install.sh — apply the SQL files to the engine container, in order, with the client that ships inside it.
# Nothing runs on the host but docker. Each file can equally be pasted into the Timeplus console.
#
#   ./scripts/install.sh               # 01 streams, 02 UDF, 03 smoke test, 10 QoE, 20 DDoS
#   ./scripts/install.sh --only qoe    # or --only ddos
#   ./scripts/install.sh --teardown    # 99_teardown.sql
#   ./scripts/install.sh sql/03_smoke.sql sql/04_demo_queries.sql   # any files you name
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
CONTAINER="${ENGINE_CONTAINER:-tp-decision-poc}"
USER_="${TIMEPLUS_USER:-proton}"
PASS_="${TIMEPLUS_PASSWORD:-timeplus@t+}"

files=()
case "${1:-}" in
  "")          files=(sql/01_streams.sql sql/02_udf.sql sql/03_smoke.sql sql/10_qoe.sql sql/20_ddos.sql) ;;
  --only)      files=(sql/01_streams.sql sql/02_udf.sql sql/03_smoke.sql "sql/$([ "${2:-}" = ddos ] && echo 20_ddos || echo 10_qoe).sql") ;;
  --teardown)  files=(sql/99_teardown.sql) ;;
  *)           files=("$@") ;;
esac

for f in "${files[@]}"; do
  echo "== $f"
  docker exec -i "$CONTAINER" timeplusd client -u "$USER_" --password "$PASS_" -mn < "$f"
done
[ "${1:-}" = --teardown ] || echo "== done. Decisions start within ~10 s:  SELECT * FROM decision_log   (more in sql/04_demo_queries.sql)"
