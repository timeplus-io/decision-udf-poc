#!/usr/bin/env bash
# install.sh — apply the SQL files to the engine container, in order, with the client that ships inside it.
# Nothing runs on the host but docker. Each file can equally be pasted into the Timeplus console.
#
#   ./scripts/install.sh                   # shared UDF + smoke test, then both pipelines (databases qoe and ddos)
#   ./scripts/install.sh --only qoe        # shared UDF + smoke test + one pipeline (qoe | ddos)
#   ./scripts/install.sh --teardown        # everything: both databases and the UDF
#   ./scripts/install.sh --teardown ddos   # one pipeline's database only
#   ./scripts/install.sh sql/90_results.sql sql/qoe/20_queries.sql   # any files you name
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
CONTAINER="${ENGINE_CONTAINER:-tp-decision-poc}"
USER_="${TIMEPLUS_USER:-proton}"
PASS_="${TIMEPLUS_PASSWORD:-timeplus@t+}"
SHARED=(sql/01_udf.sql sql/02_smoke.sql)

files=()
case "${1:-}" in
  "")          files=("${SHARED[@]}" sql/qoe/10_pipeline.sql sql/ddos/10_pipeline.sql) ;;
  --only)      files=("${SHARED[@]}" "sql/${2:?usage: --only qoe|ddos}/10_pipeline.sql") ;;
  --teardown)  files=("sql/${2:+$2/}99_teardown.sql") ;;
  *)           files=("$@") ;;
esac

for f in "${files[@]}"; do
  echo "== $f"
  docker exec -i "$CONTAINER" timeplusd client -u "$USER_" --password "$PASS_" -mn < "$f"
done
[ "${1:-}" = --teardown ] || echo "== done. Decisions start within ~10 s:  SELECT * FROM qoe.decision_log  /  ddos.decision_log   (more in sql/<pipeline>/20_queries.sql)"
