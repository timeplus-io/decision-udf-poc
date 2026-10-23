# decision-udf-poc

Real-time decisions inside a Timeplus streaming pipeline, made by a **decision model** (System One: typed
`choice` / `score` / `noul` answers with calibrated probabilities, not generated text) called from one
Python UDF. The backend is swappable: **Jev Cloud** or an **open-source decision model served locally by
Ollama** (no outbound network). The engine is **Proton OSS** or **Timeplus Enterprise**. Data is generated
inside the engine with random streams. One `docker compose up`.

Two use cases are built; three more are documented as references in the proposal.

```
                 ┌──────────────── inside the engine (streaming SQL) ────────────────┐
random stream ──► generator view ──► events ──► window view (5 s, rule flag, truth) ──► decision view
 (seeds)          (scenario by time)                                                      │ decide_choice / decide_score
                                                                                          ▼
                                             Ollama · Jev Cloud · fake  ◄──── Python UDF ──► decision_log ──► scorecard
```

| Use case | Window | Questions to the model | Designed rule gaps |
| --- | --- | --- | --- |
| **1. Video QoE incident triage** | global, 5 s | `choice` cause: cdn_fault / isp_congestion / player_bug / encoding_issue / traffic_spike_benign / no_incident · `score` severity: negligible…critical | threshold fires on a flash crowd and cannot name a cause |
| **2. Edge DDoS response** | per protected service, 5 s, candidates only | `choice` action: mitigate / observe / escalate · `choice` kind: syn_flood / udp_amplification / http_flood / port_scan / legitimate_surge / normal | threshold fires on a flash crowd, misses a port scan |

Schemas are generic (public QoE and NetFlow conventions). The scenario schedule is a function of the wall
clock (one scenario per 60 s period, windows period-aligned), so **ground truth is exact and free**: the
scorecard needs no labels.

## Quick start (laptop): Proton OSS + Ollama

```bash
cp .env.example .env                              # backend=local, model=tev1:0.8b
docker compose --profile ollama up -d             # engine + Ollama
./scripts/pull_model.sh                           # pulls tev1:0.8b (~0.8B, CPU-friendly), smoke-tests /v1/systemone
python3 scripts/setup.py                          # streams, UDFs, both use cases
```

Then run queries from `sql/04_demo_queries.sql` in the console or `proton-client`. Query **B** is the
one-screen scorecard (model accuracy vs rule accuracy, auto-decide share, p50/p99 latency, per minute);
**D** shows the two designed rule gaps live; **G** shows the exact text the model is given.

- No model at all (wiring/chaos test): `docker compose --profile fake up -d`, set `DECISION_MODEL=fake-systemone-v0`.
- Jev Cloud: `DECISION_BACKEND=jev DECISION_MODEL=jev-1.13.0 TYPESAFE_API_KEY=... python3 scripts/setup.py`.
- Timeplus Enterprise: `ENGINE_IMAGE=timeplus/timeplus-enterprise:latest` in `.env` (plus `TIMEPLUS_USER/PASSWORD`); same SQL.
- Mac with Apple GPU: run Ollama natively (`brew install ollama`, `ollama pull tev1:0.8b`) and set
  `DECISION_BASE_URL=http://host.docker.internal:11434` — Docker on macOS has no GPU access.
- Chaos: `docker compose stop decision-ollama`; `err` becomes `conn_error`, `p` NULL, views stay alive; `start` it again.
- Tear down: `python3 scripts/setup.py --teardown`.

## Local models (Ollama library, Oct 2026)

| Tag | Size | Runs on | Notes |
| --- | --- | --- | --- |
| `tev1:0.8b` | ~0.8B | CPU | default; fastest local option |
| `tev1` | 4B | GPU, or CPU at low rate | stronger on hard items |
| `clef-flash` | 9B, ~11 GB | GPU (8 GB+ VRAM) | Apache-2.0, Cloudflare; Jev-compatible API |
| `pplx-decider` | 27B, ~17 GB Q4 | big GPU | Apache-2.0; top open model on the Decision Index |

Not used by default: `nimble` (9B) is in the library but its adapter is reported non-commercial. **Re-check the
license on each model page before a customer ships.** Ollama's `/v1/systemone` caps: 26 options, 26 score
levels, 64 questions and 64 KiB per request; the UDF packs 8 rows per request for Ollama.

**Air-gapped hosts**: pull on a connected machine, then copy the `ollama-models` Docker volume (or
`~/.ollama/models`) to the target. Alternative packaging with a digest check at start: `backend/Dockerfile.local`
builds a `llama-server` image with a GGUF baked in (`--profile llama`).

## Layout

| Path | What |
| --- | --- |
| `udf/decide_udf.py` | The UDF module: `decide_noul`, `decide_choice`, `decide_score`; packing, bounded fan-out, timeouts, NULL-on-failure, model pinning, calibration, cache. Importable and unit-tested outside the engine. |
| `udf/build_sql.py` | Wraps the module into three `CREATE FUNCTION ... LANGUAGE PYTHON` statements with backend settings in `init_function_parameters`. |
| `sql/01_streams.sql` | `decision_log`, the shared append-only audit trail. |
| `sql/10_qoe.sql` | Use case 1: `qoe_raw` → `mv_qoe_gen` → `qoe_events` → `mv_qoe_windows` → `qoe_windows` → `mv_qoe_decide_cause`, `mv_qoe_decide_severity`. |
| `sql/20_ddos.sql` | Use case 2: `flow_raw` → `mv_flow_gen` → `flow_records` → `mv_flow_windows` → `flow_windows` → `mv_ddos_decide_action`, `mv_ddos_decide_kind`. |
| `sql/04_demo_queries.sql` | Live feed, scorecard, confusion by scenario, rule-gap view, calibration buckets, backend health, results. |
| `backend/fake_systemone.py` | Stdlib `/v1/systemone` stand-in (noisy heuristic) for wiring and chaos tests. **Not a model.** |
| `backend/Dockerfile.local`, `entrypoint.sh` | llama-server image with a GGUF baked in and SHA-256 verified at start. |
| `scripts/setup.py` | Creates everything for the chosen backend and smoke-tests the UDFs. `--only qoe|ddos`, `--no-init-hook`, `--teardown`. |
| `scripts/pull_model.sh` | Pulls the Ollama model and smoke-tests the endpoint. |
| `scripts/offline_eval.py` | Runs the UDFs over synthetic window states with no engine; prints the scorecard. |
| `tests/test_udf.py` | 18 tests: three question types, packing, fan-out, deadline, retries, pinning, backend down, calibration, cache, DDL. |

## UDF contract

```sql
decide_choice(state string, question string, options string)  -- options = '["a","b"]' or 'a,b'
decide_score (state string, question string, levels string)
decide_noul  (state string, question string)
-- each returns one JSON string per row, parsed in the view:
-- {"choice":"mitigate","p":0.81,"p_raw":0.79,"probabilities":{...},"model":"tev1:0.8b","backend":"local","latency_ms":212,"batch":"b7f1","err":""}
-- {"level":"critical","score":2.7, ...}   {"p":0.93,"p_raw":0.91, ...}
-- on failure the decision fields are null and err is a code: conn_error, timeout, http_503, model_mismatch, block_deadline, ...
```

| Concern | Behavior | Setting |
| --- | --- | --- |
| Batching | Up to N rows per request: one state with `items`, one question per item | `pack_rows` (8 for Ollama, 16 otherwise) |
| Fan-out | Bounded thread pool per block | `max_in_flight` (6) |
| Timeouts | Per request, plus a whole-block deadline | `timeout_s` (5), `block_deadline_s` (20) |
| Retries | On 429/5xx/timeout with backoff | `retries` (1) |
| Failure | Never raises (a raised exception kills the view) | — |
| Pinning | `model` sent on the request and asserted on the response | `model` |
| Calibration | Temperature per question on the logit (noul) or option logits (choice/score); identity until fitted | `temperatures` |
| Cache | Optional, keyed by sha256(type, question, options, state); hits report `model=cache` | `cache_ttl_s` (0 = off) |
| Init | `_tp_init(params_json)` via `SETTINGS init_function_name`; lazy fallback for engines without hooks | `--no-init-hook` |

## How the simulation works

`CREATE RANDOM STREAM` emits only seeds (`rand()`), so all cross-column logic lives in the generator view,
which derives cohorts from the seeds and shifts metrics only for the affected cohort of the active scenario:

- QoE: `sid = int_div(unix_ts, 60) % 6` → none, cdn_fault (one CDN), isp_congestion (one ISP), player_bug (one
  player version), encoding_issue (one asset), flash_crowd (everyone, load up, metrics normal). The window view
  computes aggregates plus **concentration** signals (`uniq_if` of each dimension among degraded sessions), which is
  what the model reasons from: "degraded sessions span 1 of 3 CDNs, 6 of 6 ISPs…".
- DDoS: same clock → none, syn_flood, udp_amplification, http_flood (one victim service each), port_scan (one source
  pool sweeping every service), flash_crowd (one service, normal mix). The window view computes pps, unique sources,
  SYN-only ratio, UDP share, mean packet size, distinct ports, flows by port, per service.

Random streams generate only while a view reads them, so stopping the generator views stops the load.

## Known limits

- Returns JSON strings parsed in the view; a `tuple` return type is a later optimization.
- Accuracy numbers from the `mock` or fake backends say nothing about model quality.
- Written to the documented Python UDF contract (lists in, strings as bytes, init hook) and tested outside the engine;
  the first in-engine run is the smoke test in `scripts/setup.py`. If the engine rejects `SETTINGS init_function_name`,
  use `--no-init-hook`.
- The `tev1` models have no published CPU latency; expect roughly 0.3–0.6 s per 8-row request on a modern 8-core laptop
  for the 0.8B model (estimate), and 100–250 ms on an L4/T4-class GPU for 2B-class models.
