#!/usr/bin/env bash
# pull_model.sh — pull the decision model into the Ollama container (ollama profile) once.
#   ./scripts/pull_model.sh                # uses DECISION_MODEL from .env (default tev1:0.8b)
#   ./scripts/pull_model.sh clef-flash     # or name one
# Air-gapped hosts: run this on a connected machine, then copy the `ollama-models` Docker volume
# (or ~/.ollama/models for a native Ollama) to the target host.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
MODEL="${1:-${DECISION_MODEL:-tev1:0.8b}}"
echo "pulling $MODEL into tp-decision-ollama"
docker exec tp-decision-ollama ollama pull "$MODEL"
echo "models present:"
docker exec tp-decision-ollama ollama list
echo "smoke test:"
curl -s http://localhost:11434/v1/systemone -H 'Content-Type: application/json' -d "{
  \"model\": \"$MODEL\",
  \"state\": \"Edge flow window: pps=48000 (normal ~2500), unique_sources=25000, syn_only_ratio=0.91, udp_share=0.02\",
  \"questions\": {\"act\": {\"type\": \"choice\", \"instructions\": \"What should the edge do now?\",
                  \"criteria\": {\"mitigate\": null, \"observe\": null, \"escalate\": null}}}
}" | head -c 600; echo
