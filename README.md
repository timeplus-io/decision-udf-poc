# decision-udf-poc

Real-time decisions inside a Timeplus streaming pipeline, made by a **decision model** (System One: typed
`choice` / `score` / `noul` answers with calibrated probabilities, not generated text) called from one
Python UDF **that runs inside the engine**. The backend is swappable: **Jev Cloud** or an **open-source decision
model served locally by Ollama** (no outbound network). Data is generated inside the engine with random streams.

**Nothing runs on your machine except Docker and SQL.** The install is five SQL files applied in order; the
only Python in the project is the UDF body in `sql/02_udf.sql`, executed by Timeplus. The engine is **Timeplus
Enterprise (amd64 build)**: Python UDFs are not built into Proton OSS or the arm64 images.

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

## Quick start (laptop): Timeplus Enterprise + Ollama

```bash
cp .env.example .env                              # backend=local, model=tev1:0.8b
docker compose --profile ollama up -d             # engine + Ollama
./scripts/pull_model.sh                           # pulls tev1:0.8b (~0.8B, CPU-friendly), smoke-tests /v1/systemone
./scripts/install.sh                              # sql/01 02 03 10 20: streams, UDF, smoke test, both use cases
```

`install.sh` is a ten-line loop that pipes each file into the `timeplusd client` that ships inside the engine
container. You can equally paste the files into the console at <http://localhost:8000> (user `proton`,
password `timeplus@t+`) or run `docker exec -it tp-decision-poc timeplusd client -u proton --password 'timeplus@t+'`.

Then run queries from `sql/04_demo_queries.sql`. Query **B** is the one-screen scorecard (model accuracy vs
rule accuracy, auto-decide share, p50/p99 latency, per minute); **D** shows the two designed rule gaps live;
**G** shows the exact text the model is given.

- No model at all (wiring/chaos test): `docker compose --profile fake up -d`, set `DECISION_MODEL=fake-systemone-v0`.
- Jev Cloud: `DECISION_BACKEND=jev`, `DECISION_MODEL=jev-1.13.0`, `TYPESAFE_API_KEY=...` in `.env`; `docker compose up -d`.
- Mac with Apple GPU: run Ollama natively (`brew install ollama`, `ollama pull tev1:0.8b`), set
  `DECISION_BASE_URL=http://host.docker.internal:11434` and start the engine alone (`docker compose up -d`).
  Docker on macOS has no GPU access, and the `ollama` profile would clash with the native one on port 11434.
- Apple silicon: the engine image is pinned to `linux/amd64` and runs under emulation. It is slower but fine for the demo.
- Changing `DECISION_*` in `.env`: `docker compose up -d` recreates the engine with the new environment; the UDF reads it at first call.
- Chaos: `docker compose stop decision-ollama`; `err` becomes `conn_error`, `p` NULL, views stay alive; `start` it again.
- Tear down: `./scripts/install.sh --teardown` (runs `sql/99_teardown.sql`).

## The SQL files

| File | What | Apply |
| --- | --- | --- |
| `sql/01_streams.sql` | `decision_log`, the shared append-only audit trail. | always |
| `sql/02_udf.sql` | **The UDF.** One `CREATE FUNCTION decide(...) LANGUAGE PYTHON` with the whole module inline, plus three SQL lambdas (`decide_noul`, `decide_choice`, `decide_score`) that give the views typed names. Edit the Python here; there is no generator. | always |
| `sql/03_smoke.sql` | One `SELECT` calling each question type: the first in-engine check, before any stream exists. | always |
| `sql/10_qoe.sql` | Use case 1: `qoe_raw` → `mv_qoe_gen` → `qoe_events` → `mv_qoe_windows` → `qoe_windows` → `mv_qoe_decide_cause`, `mv_qoe_decide_severity`. | either / both |
| `sql/20_ddos.sql` | Use case 2: `flow_raw` → `mv_flow_gen` → `flow_records` → `mv_flow_windows` → `flow_windows` → `mv_ddos_decide_action`, `mv_ddos_decide_kind`. | either / both |
| `sql/04_demo_queries.sql` | Live feed, scorecard, confusion by scenario, rule-gap view, calibration buckets, backend health, results. | by hand |
| `sql/99_teardown.sql` | Drops everything in the right order (views, streams, functions, log). | to reset |

Other files: `backend/fake_systemone.py` is a stdlib `/v1/systemone` stand-in (noisy heuristic, **not a model**)
that runs in its own container for wiring and chaos tests; `backend/Dockerfile.local` + `entrypoint.sh` build a
llama-server image with a GGUF baked in and SHA-256 verified at start (`--profile llama`); `scripts/pull_model.sh`
pulls the Ollama model and smoke-tests the endpoint; `docs/PoC.md` is the proposal.

## UDF contract

```sql
decide_choice(state string, question string, options string)  -- options = '["a","b"]' or 'a,b'
decide_score (state string, question string, levels string)
decide_noul  (state string, question string)
decide       (state string, question string, kind string, spec string)   -- the Python function the three wrap
-- each returns one JSON string per row, parsed in the view:
-- {"choice":"mitigate","p":0.81,"p_raw":0.79,"probabilities":{...},"model":"tev1:0.8b","backend":"local","latency_ms":212,"batch":"b7f1","err":""}
-- {"level":"critical","score":2.7, ...}   {"p":0.93,"p_raw":0.91, ...}
-- on failure the decision fields are null and err is a code: conn_error, timeout, http_503, model_mismatch, block_deadline, ...
```

The UDF is configured from the engine container's environment (set in `.env`, passed by compose):

| Concern | Behavior | Variable (default) |
| --- | --- | --- |
| Backend | `local` (any `/v1/systemone` server), `jev` (cloud, needs `TYPESAFE_API_KEY`), `mock` (in-process heuristic) | `DECISION_BACKEND` (`local`), `DECISION_BASE_URL` (`http://decision:11434`) |
| Pinning | `model` sent on the request and asserted on the response, else `err=model_mismatch` | `DECISION_MODEL` (empty = accept any) |
| Batching | Up to N rows per request: one state with `items`, one question per item | `DECISION_PACK_ROWS` (8 for Ollama URLs, else 16) |
| Fan-out | Bounded thread pool per block | `DECISION_MAX_IN_FLIGHT` (6) |
| Timeouts | Per request, plus a whole-block deadline | `DECISION_TIMEOUT_S` (5), `DECISION_BLOCK_DEADLINE_S` (20) |
| Retries | On 429/5xx/timeout with backoff | `DECISION_RETRIES` (1) |
| Calibration | Temperature per question on the logit (noul) or option logits (choice/score); identity until fitted | `DECISION_TEMPERATURES` (`{}`) |
| Cache | Optional, keyed by sha256(type, question, options, state); hits report `model=cache` | `DECISION_CACHE_TTL_S` (0 = off) |
| Failure | Never raises (a raised exception kills the view); standard library HTTP client, no package install | — |

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

## Engine notes (Timeplus Enterprise 3.4)

- `window_start` / `window_end` are reserved column names in `CREATE STREAM`; the stored column is `win_start`.
  Window functions (`tumble`, `hop`) still produce `window_start` in queries.
- By default timeplusd preallocates ~2 GB of native log per stream. Compose sets `ENABLE_DATA_PREALLOCATE=false`,
  which keeps this PoC's data volume in the hundreds of MB. The engine refuses `CREATE` when its disk is over 90% full
  (`DISK_USAGE_RATIO_THRESHOLD_EXCEEDED`); on Docker Desktop that is the VM disk, not your Mac's.
- The image has no `curl`; the compose healthcheck uses `timeplusd client`.
- Strings arrive in the UDF as `bytes`; the module decodes at the boundary.

## Known limits

- Returns JSON strings parsed in the view; a `tuple` return type is a later optimization.
- Accuracy numbers from the `mock` or fake backends say nothing about model quality.
- No unit tests outside the engine by design (nothing runs on the host). `sql/03_smoke.sql` and the `err` column of
  `decision_log` are the checks.
- The `tev1` models have no published CPU latency; expect roughly 0.3–0.6 s per 8-row request on a modern 8-core laptop
  for the 0.8B model (estimate), and 100–250 ms on an L4/T4-class GPU for 2B-class models.
