# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Ground rules

- **Nothing runs on the host but Docker and SQL.** Do not add Python scripts, pytest, requirements files or
  generators. The only Python is the UDF body inside `sql/02_udf.sql`, executed by Timeplus.
- `sql/02_udf.sql` is the source of truth for the UDF: one `CREATE FUNCTION decide(...) LANGUAGE PYTHON` plus
  three SQL lambdas (`decide_noul/choice/score`). Edit the Python there. The body must not contain `$$`, must
  never raise (a raised exception kills the materialized view: return `err` codes instead), and uses only the
  standard library (no `SYSTEM INSTALL PYTHON PACKAGE`).
- UDF configuration is `DECISION_*` environment variables in the engine container (set in `.env`, passed by
  `docker-compose.yml`, documented at the top of `02_udf.sql`). No init hook, no `init_function_parameters`.
- Every object created in any `sql/*.sql` must also be dropped in `sql/99_teardown.sql`, views before streams.

## Engine facts (verified in this repo)

- Only the **amd64 build of Timeplus Enterprise** has Python UDFs. Proton OSS and the arm64 `timeplusd` images
  reject `LANGUAGE PYTHON` with "Expected JAVASCRIPT". Compose pins `platform: linux/amd64` for this reason.
- Default credentials of the image: user `proton`, password `timeplus@t+`. Client inside the container:
  `timeplusd client -u proton --password 'timeplus@t+' -mn` (multi-statement files with `$$` bodies split
  correctly). The image has no `curl`, `wget` or `python3`.
- `window_start` and `window_end` are reserved column names in `CREATE STREAM`; stored columns use `win_start`.
- timeplusd preallocates ~2 GB of native log per stream unless `ENABLE_DATA_PREALLOCATE=false` (compose sets
  it). `CREATE` fails with `DISK_USAGE_RATIO_THRESHOLD_EXCEEDED` when the Docker VM disk is over 90% full.
- Strings reach Python UDFs as `bytes`; constant arguments may arrive as a 1-element list (the module broadcasts).

## Running and checking

```bash
docker compose --profile fake up -d      # engine + deterministic fake backend (set DECISION_MODEL=fake-systemone-v0)
./scripts/install.sh                     # 01 streams, 02 UDF, 03 smoke, 10 QoE, 20 DDoS
./scripts/install.sh sql/03_smoke.sql    # any file(s) by name; --only qoe|ddos; --teardown
```

Verification is in-engine: `sql/03_smoke.sql` must return `"err":""` for all three calls, and after ~10 s
`SELECT * FROM table(decision_log)` must show rows with empty `err`. There are no host-side tests by design.

`docs/PoC.md` is the original proposal and has drifted from the code (10 s windows there, 5 s here; llama-server
models there, Ollama `tev1` here). Treat the SQL as the spec.
