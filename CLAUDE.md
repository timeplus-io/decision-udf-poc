# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Ground rules

- **Nothing runs on the host but Docker and SQL.** Do not add Python scripts, pytest, requirements files or
  generators. The only Python is the UDF body inside `sql/01_udf.sql`, executed by Timeplus.
- `sql/01_udf.sql` is the source of truth for the UDF: one `CREATE FUNCTION decide(...) LANGUAGE PYTHON` plus
  three SQL lambdas (`decide_noul/choice/score`). Edit the Python there. The body must not contain `$$`, must
  never raise (a raised exception kills the materialized view: return `err` codes instead), and uses only the
  standard library (no `SYSTEM INSTALL PYTHON PACKAGE`).
- UDF configuration is `DECISION_*` environment variables in the engine container (set in `.env`, passed by
  `docker-compose.yml`, documented at the top of `01_udf.sql`). No init hook, no `init_function_parameters`.
- **One database per pipeline.** `sql/qoe/10_pipeline.sql` creates everything under `qoe.` and `sql/ddos/10_pipeline.sql` under `ddos.`, each with its own `decision_log`. Every stream and view reference inside a pipeline file must be database-qualified (a materialized view body resolves bare names against `default`). A new pipeline gets its own database, folder and `99_teardown.sql` (`DROP DATABASE … CASCADE`); nothing goes in `default`. Functions are global, so the UDF is shared and only `sql/99_teardown.sql` drops it.

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
./scripts/install.sh                     # 01 UDF, 02 smoke, qoe/10_pipeline, ddos/10_pipeline
./scripts/install.sh --only qoe          # one pipeline; --teardown [qoe|ddos]; or any file(s) by name
```

Verification is in-engine: `sql/02_smoke.sql` must return `"err":""` for all three calls, and after ~10 s
`SELECT * FROM table(qoe.decision_log)` and `table(ddos.decision_log)` must show rows with empty `err`. There are no host-side tests by design.

`docs/PoC.md` is the original proposal and has drifted from the code (10 s windows there, 5 s here; llama-server
models there, Ollama `tev1` here). Treat the SQL as the spec.
