# PoC Proposal: Decision UDF for Timeplus (Jev Cloud or Local Open-Source Service)

Oct 20, 2026 · updated Oct 23, 2026 · @Timeplus

## Goal and scope

The PoC proves that a Timeplus streaming pipeline can hand a typed decision model the condensed state of a detected anomaly and get a calibrated action back in real time, on two use cases that map to existing Timeplus domains: streaming video QoE incident triage and edge flow-anomaly (DDoS) response. Data is generated inside the engine with random streams, the model backend is Jev Cloud or an open-source decision model served locally by Ollama, the engine is Timeplus Enterprise (the amd64 build, the only one with Python UDFs), and everything starts with `docker compose up` plus five SQL files from a dedicated GitHub repository. Nothing runs on the host but Docker and SQL: the only Python is the UDF body, executed inside the engine.

**What we prove**

1. Two use cases, one contract: QoE triage (`choice` over cause plus `score` for severity, per anomalous cohort-window) and DDoS response (`choice` over mitigate/observe/escalate, per flagged victim-window) run through the same `decide_*` UDFs and the same decision log.
2. Backend swap: the SQL is unchanged whether the backend is `api.typesafe.ai` or an Ollama (or llama-server) container on the same host, and the local path runs with outbound network disabled.
3. Pure SQL: the install is SQL files applied with the client inside the engine container or pasted into the console; no host-side scripts, generators, package installs or tests. (Engine portability to Proton OSS was dropped: it has no Python UDF, see the risks section.)
4. Streaming fit: decisions are made on windowed, SQL-condensed state (tens per minute), never on the raw firehose (thousands of events per second), and the pipeline degrades to NULL-plus-fallback when the backend is slow or down.
5. Quality: ground truth is known by construction from the scenario schedule, so accuracy, calibration (ECE) and the auto-decide share at a confidence gate are exact for both backends.

**Success criteria**

| Criterion | Target | How measured |
| --- | --- | --- |
| One-command start | `docker compose up` plus `./scripts/install.sh` (pure SQL) brings up engine, backend, UDF and generators on Timeplus Enterprise | Fresh clone on an amd64 host and on Apple silicon under emulation |
| Backend swap | Zero SQL changes | Same views, `.env` switch only |
| Decision latency, local | p50 under 250 ms per window-decision on one L4/T4-class GPU; under 1.5 s on a 16-core CPU with a 0.8B model | UDF timers in `decision_log`, 10-minute run |
| Decision rate | Both use cases together sustain 60 window-decisions/min with a flat pipeline lag | Rows/min into `decision_log` vs windows produced |
| Resilience | Views stay alive through a backend kill and restart | Chaos test, row continuity |
| Accuracy | Cause/action accuracy within 5 points of Jev Cloud on the same scenario schedule | Ground truth from the schedule |
| Rule gap | The model fixes at least half of the designed rule false alarms and misses | Disagreement query by scenario |
| Cost | Local under $5 per 1M decisions on-prem | Measured CPU/GPU seconds, amortized hardware |

**Reference use cases (documented, not built)**: transaction and payments risk on text-rich events, AI-agent action monitoring (AgentGuard), and ingest-time classification and routing of unstructured events. They share the same cascade and UDF contract; see the Demo flow section.

**Out of scope**: native C++ embedding of llama.cpp into Proton (future option), training custom decision heads, a production model registry, the Remote UDF adapter variant (noted as an alternative in the risks section), UI work. Simulator schemas are generic (public QoE and NetFlow conventions) and carry no customer fields.

## Architecture

The pipeline is a plain Timeplus materialized view; the only new piece is one Python UDF that owns all backend knowledge, so the SQL never changes when the backend does.

&#91;embedded content: PoC architecture · random streams to windowed decisions, one UDF, two backends\]

Everything to the left of the UDF is plain streaming SQL: random streams emit seeds, generator views turn them into generic QoE events and flow records following a time-based scenario schedule, window views condense them into 5-second windows with a rule flag and the ground-truth label, and decision views call the UDF per window (every global window for QoE, candidate windows only for DDoS). The UDF fans each block out to whichever backend `DECISION_BACKEND` names; every decision lands in the append-only decision log that the scorecard and the action gate read.

**Why this shape**

- The UDF is the seam. Both backends speak the same `/v1/systemone` request shape (state + typed questions), which llama.cpp, Ollama and vLLM adopted in September and October 2026, so one client class covers both.
- The model sees condensed state, never the firehose. Random streams run at thousands of events per second; the model is asked tens of times per minute, once per flagged cohort-window, which is what keeps a 100 to 500 ms decision inside the pipeline's budget.
- Ground truth is free. The scenario schedule is a function of time, so the window view can label each window with the active incident and the scorecard needs no human labels.
- Block-scoped batching comes free. The engine hands the UDF a whole block; the UDF packs rows into requests (one state, one question per row), the pattern the pg-jev and duckdb-jev extensions measured at roughly 27x fewer calls than per-row.
- The decision log is the audit trail. `decision, probability, model, version, latency_ms, err, truth` per row makes every decision replayable against a new model version, which is what regulated buyers ask for.
- Nothing synchronous is added to the engine. The local service is a separate container, so a model crash or a slow model shows up as NULLs and backpressure, never as a `timeplusd` crash.

## Backend A: Jev Cloud

Jev Cloud is the accuracy and latency reference for the PoC, and the right backend for customers who allow outbound HTTPS; it is not an option for air-gapped sites.

| Item | Value | Note |
| --- | --- | --- |
| Endpoint | `POST https://api.typesafe.ai/v1/systemone` | Same request shape as the local backend |
| Model to pin | `jev-1.13.0` | `jev-latest` rolls to 1.14 without notice; log the `model` field of every response |
| Request | `model`, `state` (string, JSON object or array), `questions` map of `{type, instructions, criteria}` | Types used: `choice` (up to 255 options), `noul` (0 to 1), `score` (ordered levels) |
| Response | per question: `choice` or `noul` or `score`, `probabilities`, `confidence`; plus `usage.input_tokens` | No rationale; valid-but-wrong answers are possible |
| Limits | 64k tokens per request, 32k per question; roughly 1,200 req/min and 250k tokens/s per org (conflicting public figures) | Numbers visible only in the console; no published SLA |
| Latency (independent) | p50 125 to 250 ms, p99 350 to 470 ms | TypeSafe says 70 to 500 ms; the "sub-10 ms" figures on aggregator sites have no methodology |
| Price | $0.042 per 1M input tokens, output free | About $0.04 to $0.10 per 1,000 decisions at 1,000 to 2,000 tokens each |
| Data handling | US-hosted; zero data retention and SOC 2 Type II on enterprise terms | Needs a sales conversation for ZDR |
| Client | Python standard library (`urllib`) inside the UDF | Plain HTTP, no package install in the engine, one client serves both backends |

**Batching rule that matters**: the API evaluates all questions in one request against one state in a single pass, so the cheap pattern is many questions per request, not many requests. TypeSafe's own numbers: 15 nouls in one call cost about 1/12 and ran about 10x faster than 15 separate calls. The UDF exploits this by packing rows.

**When to use it**: customers with outbound access who want the strongest zero-shot quality today, and as the gold reference when building the labeled set.

## Backend B: local open-source decision service

The local backend is Ollama serving an open decision model from its library over the same `/v1/systemone` endpoint; the model is pulled once (or shipped as a copied model volume for air-gapped hosts), and the host needs no outbound network to run.

**Server choice**

| Server | `/v1/systemone` status | Fit for the PoC |
| --- | --- | --- |
| Ollama 0.35+ | Shipped Sep 29, 2026; decision models in its library; caps of 26 options, 26 levels, 64 questions, 64 KiB per request | Chosen: one `ollama pull`, one container, CPU and GPU, native on Macs |
| llama.cpp `llama-server` | Shipped in v0.6.0 (Oct 5, 2026) | Alternative packaging kept in the repo: single static binary with the GGUF baked in and a digest check at start |
| vLLM | On `main` since Oct 14, 2026, not in a release | GPU-only, revisit when released |

**Model choice** (all Apache-2.0; licenses re-checked from the model cards before any customer ships)

| Model (Ollama tag) | Size | Where it runs well | Why |
| --- | --- | --- | --- |
| `tev1:0.8b` | about 0.8B | CPU laptop or server | Default for the demo; fastest local option |
| `tev1` | 4B | One GPU, or CPU at low rate | Stronger on hard items |
| `clef-flash` | 9B, about 11 GB | GPU with 8 GB+ VRAM | Apache-2.0, Cloudflare, Jev-compatible API; no CPU latency published |
| `pplx-decider` | 27B, about 17 GB Q4 | Large GPU | Apache-2.0; top open model on the independent Decision Index |

Not used by default: `nimble` (9B) is in Ollama's library but its adapter is reported CC BY-NC. Licenses are re-read from each model page before any customer ships. Q8-class quantizations are treated as lossless for decisions; Q4 costs 0.3 to 2 points on 1B to 3B models.

**Air-gapped packaging**

1. `docker compose --profile ollama up -d` starts the engine and an `ollama/ollama` container reachable from the engine as `decision:11434`; `scripts/pull_model.sh` pulls the pinned model once and smoke-tests the endpoint.
2. The UDF pins `DECISION_MODEL` and marks rows `model_mismatch` if the server answers with another model id.
3. Air-gapped hosts: pull on a connected machine, copy the `ollama-models` Docker volume (or `~/.ollama/models`) to the target, start with no outbound network.
4. GPU: `--gpus` on Linux with NVIDIA; on a Mac run Ollama natively and point the engine at `host.docker.internal:11434`, since Docker on macOS has no GPU access.
5. Alternative with a digest check at start: `backend/Dockerfile.local` builds a `llama-server` image with the GGUF baked in (`--profile llama`).

**What CPU vs GPU means for this backend** (from measured llama.cpp prefill rates; L4 is an estimate)

| Host | 2B model, 1,000-token state | Decisions/s | Fit |
| --- | --- | --- | --- |
| 16-core x86 CPU | 1 to 3 s per decision | 5 to 15 | Demo and low-volume only |
| 16-core CPU with 0.8B model | about 0.5 s | 20 to 40 | Acceptable for alert-rate traffic |
| NVIDIA T4 | about 250 ms | 50 to 100 | Entry GPU |
| NVIDIA L4 (estimate, 2 to 3x T4) | about 100 ms | 150 to 300 | Target on-prem GPU |
| RTX 4090 | 20 to 30 ms | 500+ | Lab box |

If a customer has no GPU and needs more than alert-rate volume, the follow-on is an INT8 encoder head via ONNX Runtime (40 to 120 ms on 4 CPU cores); that is a different server and out of scope here.

## Python UDF design

One Python UDF, `decide(state, question, kind, spec)`, runs inside the engine. It reads its configuration from the engine's environment on first call, packs each block into a few requests, and turns every failure into a NULL row rather than an exception. Three SQL lambda functions give the views typed names without a second copy of the Python. The whole thing is one file, `sql/01_udf.sql`, applied like any other SQL file; there is no generator, no package install and no host-side tooling.

**SQL surface**

```sql
CREATE OR REPLACE FUNCTION decide(state string, question string, kind string, spec string)
RETURNS string
LANGUAGE PYTHON AS $$ ... $$;                       -- the module, inline, once

CREATE OR REPLACE FUNCTION decide_noul   AS (state, question)          -> decide(state, question, 'noul',   '');
CREATE OR REPLACE FUNCTION decide_choice AS (state, question, options) -> decide(state, question, 'choice', options);
CREATE OR REPLACE FUNCTION decide_score  AS (state, question, levels)  -> decide(state, question, 'score',  levels);
```

Each call returns one JSON string per row (`choice`/`level`/`p`, `p_raw`, `probabilities`, `model`, `backend`, `latency_ms`, `batch`, `err`), which the view parses with `json_extract_*`. A `tuple` return type is a later optimization. A materialized view then reads:

```sql
CREATE MATERIALIZED VIEW triage.mv_decide INTO triage.decision_log AS
SELECT win_start, cohort,
       json_extract_string(d, 'choice')              AS decision,
       to_float32_or_null(json_extract_raw(d, 'p'))  AS p,
       json_extract_string(d, 'model')               AS model,
       json_extract_string(d, 'err')                 AS err
FROM (SELECT win_start, cohort,
             decide_choice(state, 'Route this event', '["fraud_review","auto_approve","needs_info"]') AS d
      FROM triage.windows);
```

The state is built in SQL, which keeps the UDF generic: callers decide what context goes in, and can trim it, since accuracy degrades as irrelevant state grows.

**Inside the module**

| Concern | Design | Reason |
| --- | --- | --- |
| Config | Lazy init on first call from `DECISION_BACKEND` (`jev`, `local`, `mock`), `DECISION_BASE_URL`, `DECISION_MODEL`, `DECISION_PACK_ROWS`, `DECISION_MAX_IN_FLIGHT`, `DECISION_TIMEOUT_S`, `DECISION_BLOCK_DEADLINE_S`, `DECISION_RETRIES`, `DECISION_CACHE_TTL_S`, `DECISION_TEMPERATURES` (JSON), `TYPESAFE_API_KEY`; compose passes them into the engine container | Switching backends is an `.env` edit and `docker compose up -d`; no DDL changes, no init hook, nothing secret in `SHOW CREATE FUNCTION` |
| Batching | Group the block's rows by question text; pack up to `pack_rows` (8 for Ollama URLs, else 16) rows into one `state` as numbered items with one question per item; send with a `ThreadPoolExecutor` of `max_in_flight` (6) | Mirrors pg-jev/duckdb-jev; local servers accept one state and up to 64 questions, 64 KiB per request |
| Ordering | Results written back by row index; a block returns only when all its requests finish or the block deadline passes | UDF is synchronous per block; ordering is preserved by construction |
| Timeouts | Per-request `timeout_s` (5), retries on 429/5xx/timeout with backoff (1); the whole block bounded by `block_deadline_s` (20) | Must never block the view indefinitely |
| Failure semantics | Any exception, timeout, bad JSON, HTTP error or model mismatch yields NULL decision fields and an `err` code (`conn_error`, `timeout`, `http_503`, `model_mismatch`, `block_deadline`, `no_answer`, `udf_<Exception>`); the function never raises, and strings arrive as `bytes` and are decoded at the boundary | A raised exception kills the materialized view (verified: a backend outage produced `conn_error` rows and all eight views stayed alive) |
| Calibration | Temperature per question on the logit (noul) or option logits (choice/score), from `DECISION_TEMPERATURES`; identity until fitted | Thresholds do not transfer across models or quantizations; refit from `decision_log` per model version |
| Version pinning | Send `model` on every request, assert the response `model` matches, else `err=model_mismatch`; Ollama and Jev report the tag per response, so no separate digest probe is needed | Jev deprecates versions on 30 to 60 day windows; the llama-server image recipe keeps its own SHA-256 check at start |
| Cache | Optional, keyed by `sha256(kind, question, options, state)`, `cache_ttl_s` default 0 (off); hits report `model=cache` | Repeated identical states are common in alert streams |
| Observability | Each row carries `latency_ms` (per request) and a 4-character `batch` id, so the scorecard can see packing and p50/p99 directly in `decision_log` | The benchmark reads these directly; no separate metrics stream |
| HTTP client | Python standard library (`urllib`), no `requests`, no `numpy` | No `SYSTEM INSTALL PYTHON PACKAGE` step, no wheel vendoring for air-gapped installs, nothing to wait for on first call |

**Install** is SQL files in order: the shared `sql/01_udf.sql` and `02_smoke.sql` (one `SELECT` per question type, the first in-engine check), then one file per pipeline, `sql/qoe/10_pipeline.sql` and `sql/ddos/10_pipeline.sql`. Each pipeline is a separate namespace: its streams, views and its own `decision_log` live in a database of the same name, every reference is database-qualified, and `DROP DATABASE … CASCADE` (`sql/<pipeline>/99_teardown.sql`) removes it in one statement. Functions are global in Timeplus, so the UDF is shared; `sql/99_teardown.sql` removes both databases and the UDF. `scripts/install.sh` is a bash loop that pipes each file into the `timeplusd client` that ships inside the engine container; the files can equally be pasted into the console.

**Engine facts settled while building** (Timeplus Enterprise 3.4.1): Python UDFs exist only in the amd64 build, so compose pins `platform: linux/amd64` (Apple silicon runs it under emulation); Proton OSS and the arm64 images reject `LANGUAGE PYTHON`. `window_start` and `window_end` are reserved column names in `CREATE STREAM`, so stored columns are named `win_start`. timeplusd preallocates about 2 GB of native log per stream unless `ENABLE_DATA_PREALLOCATE=false`, which compose sets, and it refuses `CREATE` when its disk is over 90% full. The init hook (`SETTINGS init_function_name`) works on this build but is not used.

## Demo flow: two use cases, one pipeline shape

Both use cases follow the same four-stage shape inside the engine: random stream, generator view with a scenario schedule, window view with a rule flag and the truth label, decision view that asks the model only about flagged windows. The schemas are generic, built from public QoE and NetFlow conventions, and carry no customer fields; the scenarios are inspired by the shape of the existing video-quality and edge-DDoS engagements, not by their data.

**Scenario schedule**: a scenario id is a function of wall-clock time (`int_div(to_unix_timestamp(now()), 60) % N`), so every 60-second period has one active incident (or none) on one affected cohort. Windows are 5 s and period-aligned, so a window never straddles two scenarios and its truth label is exact. The generator draws metrics from a shifted distribution only for the affected cohort, so the signal is concentrated along one dimension, which is the fact the SQL surfaces and the model reasons from.

**Use case 1: streaming video QoE incident triage**

| Stage | Content |
| --- | --- |
| Random stream `qoe.seeds` | Seeds only, 2,000 events/s |
| Generator view `qoe.mv_generate` → `qoe.events` | `session_id`, `cdn` (3), `isp` (6), `region` (5), `player_version` (3), `asset_id` (20), `rebuffer_ms`, `startup_ms`, `bitrate_kbps`, `error_code` (`none`, `edge_503`, `segment_timeout`, `decoder_error`, `abr_null_ref`) |
| Scenarios (6) | `none`; `cdn_fault` (one CDN: rebuffer up, `edge_503` and `segment_timeout`); `isp_congestion` (one ISP: bitrate down, rebuffer up, no errors); `player_bug` (one player version: `abr_null_ref`, startup up); `encoding_issue` (one asset: `decoder_error`); `flash_crowd` (all cohorts: sessions up, metrics normal, the designed rule false alarm) |
| Window view `qoe.mv_windows` → `qoe.windows` | Per 5 s, global: events, sessions, load ratio, degraded share, p95 rebuffer, p50 startup and bitrate, error mix, plus how many CDNs/ISPs/regions/players/assets the degraded sessions span (the concentration signal); `rule_flag` when p95 rebuffer, degraded share or load ratio crosses a fixed threshold; `truth`, `expected_cause`, `expected_sev` from the schedule; `state` is the text handed to the model |
| Decision views | For every window: `decide_choice(state, "What is the most likely cause…?", [cdn_fault, isp_congestion, player_bug, encoding_issue, traffic_spike_benign, no_incident])` and `decide_score(state, "How severe is this window for viewers?", [negligible, minor, major, critical])`, one row each into `qoe.decision_log` |
| Rule pack gap | Thresholds fire on `flash_crowd` (benign) and cannot name a cause; the model is scored on cause accuracy and on clearing the false alarm |

**Use case 2: edge flow-anomaly (DDoS) response**

| Stage | Content |
| --- | --- |
| Random stream `ddos.seeds` | Seeds only, 5,000 records/s |
| Generator view `ddos.mv_generate` → `ddos.records` | NetFlow-like: `src_ip` (synthetic /16 pools), `dst_ip` (8 protected services), `dst_port`, `proto`, `packets`, `bytes`, `tcp_flags`, `duration_ms` |
| Scenarios (6) | `none`; `syn_flood` (one dst: many sources, SYN-only, tiny packets); `udp_amplification` (one dst: few sources, UDP 53/123/11211, large packets); `http_flood` (one dst: moderate sources, full handshakes, port 443, high pps); `port_scan` (one src pool sweeping ports on many dsts, low volume, the designed rule miss); `flash_crowd` (one dst: many sources, normal packet mix, the designed rule false alarm) |
| Window view `ddos.mv_windows` → `ddos.windows` | Per 5 s and `dst_ip`: pps, mbps, unique sources, SYN-only ratio, UDP share, mean packet size, distinct ports, flows by port; `rule_flag` when pps or unique sources cross a threshold; a looser `candidate` gate decides which windows the model sees; `truth`, `expected_action`, `expected_kind` from the schedule; `state` is the text handed to the model |
| Decision views | For candidate windows: `decide_choice(state, "What should the edge do now…?", [mitigate, observe, escalate])` and `decide_choice(state, "What kind of traffic is this service receiving…?", [syn_flood, udp_amplification, http_flood, port_scan, legitimate_surge, normal])` |
| Rule pack gap | Thresholds fire on `flash_crowd` and miss `port_scan`; the model is scored on action accuracy against the schedule's expected action (`mitigate` for floods and amplification, `observe` for scans and surges) |

**State handed to the model** is one compact text block per window (about 300 to 600 tokens): the window's aggregates, the dimension concentration ("92% of degraded sessions on cdn-b"), the top error codes or ports, and the rule that fired. No raw events.

**Backends in the demo**

| Backend | What runs | Use |
| --- | --- | --- |
| `local` + Ollama | `ollama/ollama` container (`--profile ollama`) or a native Ollama on the host, model `tev1:0.8b` by default | The default demo and the air-gapped result (copy the model volume) |
| `local` + fake server | `backend/fake_systemone.py`, a stdlib `/v1/systemone` stand-in with a noisy heuristic, in its own container (`--profile fake`) | Wiring, chaos and latency tests with no model; its accuracy means nothing. The in-engine verification so far ran against it |
| `mock` | In-process keyword heuristic inside the UDF, no network | Pipeline tests with no backend container at all; not a model |
| `local` + llama-server | `backend/Dockerfile.local` with an Apache-2.0 decision GGUF baked in and SHA-256 verified at start (`--profile llama`) | Alternative air-gapped packaging with a digest check |
| `jev` | `api.typesafe.ai`, model pinned to `jev-1.13.0` | The cloud reference; simulated data only |

**Reference use cases (same cascade, not built in this PoC)**

| Use case | State the SQL would build | Typed question | Why a decision model |
| --- | --- | --- | --- |
| Transaction and payments risk on text-rich events | Enriched transaction (velocity, device, account age from mutable streams) plus memo, merchant descriptor, counterparty name | `noul`: mule, invoice fraud or sanctions evasion pattern? | Classifiers own the structured part; the model covers the unstructured part; must run locally with decision logs for audit |
| AI-agent action monitoring (AgentGuard, monitor-only) | Tool name, arguments, user intent | `noul`: is this action risky? plus `choice` for category | Catches paraphrased exfiltration and PII in prose that regex misses; a second signal beside the rule verdict |
| Ingest-time classification and routing | Log lines, tickets, chat messages, third-party alerts | `choice`: queue or severity; `noul`: PII present, duplicate of an open incident | Intent routing is the best-measured decision-model task; runs on the encoder tier at high volume |

## Benchmark and evaluation plan

Every run writes to its pipeline's `decision_log` (`qoe.decision_log`, `ddos.decision_log`, with per-row `latency_ms` and `batch`), so the benchmark is a set of Timeplus queries over the same streams the demo uses, repeated per backend and per host.

**Test environment**

| Host | Spec | Backends run |
| --- | --- | --- |
| CPU box | 16 vCPU x86 with AVX-512, 64 GB RAM, Timeplus Enterprise 3.4.1 | Jev Cloud; local `tev1:0.8b`; local `tev1` (4B) |
| GPU box | Same CPU plus one NVIDIA L4 (24 GB), or an RTX 4090 if the lab has one | Local `tev1`; local `clef-flash`; local `pplx-decider` |
| Air-gap check | CPU box with outbound traffic dropped by iptables | Local only; proves no hidden egress |

**Measurements**

| Metric | Definition | Pass line |
| --- | --- | --- |
| Decision latency | p50/p99 per request from the UDF timer, 1,000-token states, 10-minute steady run | See targets in Goal and scope |
| Block latency | Wall time per packed request (8 rows for Ollama, 16 otherwise) and per block as seen by the view | Under 1.5 s CPU, under 150 ms GPU |
| Throughput | Rows/s through the decision views at the generator rate where lag stays flat | 50/s CPU, 500/s GPU |
| Pipeline impact | `timeplusd` CPU and memory with the UDF on vs off, cores pinned so model and engine do not share | Engine throughput drop under 20% at the target rate |
| Resilience | Kill the local container for 60 s mid-run; count NULL rows, confirm the view resumes and no rows are lost | View alive; NULL rows equal the outage window |
| Accuracy | Share of correct decisions on the 500-item held-out set, per backend | Local within 5 points of Jev Cloud |
| Calibration | ECE on the held-out set before and after per-question temperature fitting on the 300-item slice | ECE under 0.05 after fitting |
| Auto-decide share | Share of traffic with calibrated p at or above 0.85, and accuracy within that share | Report; the gate is the product lever |
| Cost | Cloud: input tokens times price; local: CPU or GPU seconds times on-demand price, plus an amortized owned-hardware line | Local under $5 per 1M decisions on-prem |
| Quantization check | `tev1` at Q8\_0 vs Q4\_K\_M on the same set | Row agreement over 98% |

**Reference points to beat or match** (from published measurements, all offline replays): Jev Cloud p50 125 to 250 ms and p99 350 to 470 ms; Banking77 zero-shot 80 to 81% for Jev; cascades gated at 0.80 to 0.90 automate 65 to 80% of traffic at frontier-level accuracy on that share.

**Deliverable of this section**: one results tab in this doc with the two tables filled per backend and host, plus the raw `decision_log` export, so the go/no-go is argued from numbers the team produced.

## Implementation plan

The PoC is three weeks of one engineer plus part-time reviewers, starting Oct 26, 2026 and ending in a go/no-go review on Nov 16, 2026.

&#91;embedded content: PoC timeline · 8 workstreams, review Nov 16\]

Engineering runs top to bottom; the two use cases share the UDF and the compose file, so use case 2 is mostly SQL once use case 1 works.

**Effort and owners**

| Workstream | Effort | Owner |
| --- | --- | --- |
| Repo scaffold, compose for Timeplus Enterprise, UDF with `choice`/`score`/`noul`, pure-SQL install, Jev Cloud path | 4 engineer-days | Backend engineer (to assign) |
| Use case 1 SQL (database `qoe`): seeds, generator, windows, decision view, scorecard | 3 engineer-days | Same engineer |
| Use case 2 SQL (database `ddos`): seeds, generator, windows, decision view, scorecard | 3 engineer-days | Same engineer |
| Local image: llama-server build, candidate GGUFs, SHA pinning, air-gap test | 3 engineer-days | Same engineer |
| Latency, throughput, resilience runs on CPU and GPU boxes, both engines | 3 engineer-days | Same engineer, plus infra for the L4 host |
| Calibration fitting, accuracy and ECE from the scenario schedule, quantization check | 2 engineer-days | Same engineer |
| Results tab, write-up, review | 2 engineer-days | Same engineer with product |

**Prerequisites before Oct 26, 2026**: a dedicated GitHub repository under the Timeplus org; a TypeSafe API key with ZDR confirmed (simulated data only); one L4 or 4090 host; access to the amd64 Timeplus Enterprise image (Python UDFs are not in Proton OSS or the arm64 builds). No labeling workstream is needed: truth comes from the scenario schedule.

## Risks, open questions and evidence gaps

The biggest technical risk is CPU-only hosts: a 2B decoder is 1 to 3 s per decision there, so the PoC must show the 0.8B model holds up or the product plan needs an encoder tier.

**Risks and mitigations**

| Risk | Likelihood | Impact | Mitigation in the PoC |
| --- | --- | --- | --- |
| CPU-only latency too high for per-event use | High | Product scope | Measure Jeff-0.8B and Decider 2B on the 16-core box; if neither meets 1.5 s per block, record the encoder tier as the CPU answer |
| UDF exception kills the materialized view | Medium | Pipeline outage | Catch-all in the UDF, NULL rows, chaos test in the plan |
| Jev version drift changes answers mid-run | Medium | Invalid comparison | Pin `jev-1.13.0`, assert the response model, log it per row |
| Local servers accept one state per request | Certain | Throughput | Row packing with one question per row; verify the 64-question and 64 KiB caps on each server |
| Quantization shifts calibration | Medium | Wrong thresholds | Fit temperatures per model and quant; Q8\_0 vs Q4\_K\_M agreement check |
| Prompt injection through state text | Medium | Wrong decisions | Out of scope to fix; log it, and keep decisions advisory (shadow mode) in the PoC |
| License surprises on a model card | Low | Blocks shipping | Re-read each model card from Hugging Face on a non-proxied machine before the review; the research round could not open several cards |
| Rate-limit contention on the shared Jev org key | Medium | Noisy cloud numbers | Dedicated key, token bucket at 80% of RPS, runs outside other teams' usage |
| Python UDF availability | Settled | Engine choice | Only the amd64 Timeplus Enterprise build has Python UDFs (verified: Proton OSS 3.0.29 and arm64 timeplusd 3.4.1 reject `LANGUAGE PYTHON`); compose pins `linux/amd64`. The UDF uses only the standard library, so no wheels are involved |
| Engine disk and schema quirks | Settled | Setup friction | `window_start`/`window_end` are reserved column names (stored columns are `win_start`); native-log preallocation of ~2 GB per stream is disabled via `ENABLE_DATA_PREALLOCATE=false`; `CREATE` fails when the engine disk is over 90% full |

**Open questions to settle during the PoC**

- Settled: Proton OSS 3.0.x has no Python UDF at all, and neither do the arm64 Enterprise images. The init hook works on Enterprise 3.4.1 amd64, but the UDF no longer needs it (configuration comes from environment variables).
- What is the engine's effective per-block UDF execution limit, and is it configurable? `DECISION_BLOCK_DEADLINE_S` (20 s) must sit below it; 5 s windows with ~200 ms fake-backend requests never came near it.
- Should the production shape be a Remote UDF adapter container instead of in-process Python? It removes CPython from `timeplusd` and gets block batching from the JSONColumns protocol, at the cost of owning retries and the fixed 2 s connect / 5 s send timeouts. The PoC notes the choice; it does not build the adapter.
- Settled for Ollama and Jev: both report the model tag in every response, which the UDF asserts (`model_mismatch`). A content digest check remains only in the llama-server image recipe.

**Known evidence gaps behind the numbers above**: no published L4 or A10G prefill rates for any 2B to 9B model (L4 rows are 2 to 3x T4 estimates); no CPU latency published for Clef-flash or Kev; Jev rate-limit figures conflict between sources (1,200 req/min versus 80 req/s); every cross-model accuracy figure is an offline replay from a single team on different hardware. The PoC replaces each of these with a measurement on our own hosts.

## Deliverables and go/no-go

The PoC ends with a working, switchable decision UDF, a measured comparison of the two backends, and a one-page recommendation on whether to productize and in what shape.

**Status** (Oct 23, 2026): the repository `timeplus-io/decision-udf-poc` holds the UDF as one SQL file (`decide` plus the three lambdas), the two use cases as two independent streaming-SQL pipelines, each in its own database (`qoe`, `ddos`) with its own decision log (random streams, generator views, window views, decision views, scorecards), a smoke test and per-pipeline teardown files, the fake backend, the llama-server image recipe and a compose file for Timeplus Enterprise. The install is pure SQL; the host-side Python (setup script, DDL generator, offline evaluator, unit tests) was removed. Verified inside Timeplus Enterprise 3.4.1 (amd64) against the fake backend: the smoke test answers all three question types, both use cases write to `decision_log` with empty `err` and 8-row packing, a backend outage yields `conn_error` rows while all eight views stay alive and decisions resume on restart, and teardown plus reinstall is clean. Next: the same run against Ollama `tev1:0.8b` and against Jev Cloud, then the results tab.

**Deliverables**

- [x] GitHub repository under the Timeplus org, runnable with `docker compose up` plus a pure-SQL install on Timeplus Enterprise (amd64)
- [x] `sql/01_udf.sql`: one Python UDF `decide` plus the `decide_choice`, `decide_score`, `decide_noul` lambdas; env-based config, packing, fallback, calibration hooks, model pinning (verified in-engine with the fake backend)
- [x] Use case 1 and 2 as pure streaming SQL, one database each: random streams, generator views with the scenario schedule, window views, decision views, scorecard queries (verified in-engine)
- [ ] Local backend image with an Apache-2.0 decision GGUF, SHA-pinned, plus the air-gap test script
- [ ] Results tab in this doc: latency, throughput, resilience, accuracy, ECE, auto-decide share and cost per backend, engine and host
- [ ] Fitted temperatures per model and quantization (`DECISION_TEMPERATURES`), fitted from `decision_log` with SQL, no host tooling
- [ ] Recommendation memo: productize or not, and if so whether as in-process Python, Remote UDF adapter, or a native engine function later

**Go criteria for productization** (all must hold)

1. The same SQL ran unchanged against both backends and against an air-gapped host.
2. At least one local configuration met its latency and throughput target: 0.8B or 2B on CPU under 1.5 s per block at 50 decisions/s, or any model on one L4-class GPU under 150 ms per block at 500 decisions/s.
3. Local accuracy on the held-out set was within 5 points of Jev Cloud, and calibrated ECE was under 0.05.
4. The materialized view survived a 60-second backend outage with no lost rows.
5. Every shipped model's license was confirmed Apache-2.0 or equivalent from its own model card.

**No-go signals**: local models trail Jev Cloud by more than 10 points on the AgentGuard set, or the CPU tier cannot reach 20 decisions/s with the 0.8B model; either result redirects the product plan toward encoder heads or a GPU requirement rather than a decoder-based local service.

**If go**, the next step is to turn the UDF contract into engine-level functions with a model registry (`CREATE DECISION MODEL`), a separately versioned calibration layer, and a `REPLAY DECISIONS` path over the decision log, as laid out in the two research reports behind this proposal.
