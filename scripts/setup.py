#!/usr/bin/env python3
"""
setup.py — create the shared streams, install the three UDFs for the chosen backend, and build both use cases.

    python scripts/setup.py                        # backend/model from .env (default local + tev1:0.8b)
    python scripts/setup.py --only qoe             # one use case
    python scripts/setup.py --no-init-hook         # engine without SETTINGS init_function_name support
    python scripts/setup.py --teardown             # drop everything this script created

SQL is sent over the engine's HTTP interface (TIMEPLUS_HTTP, default http://localhost:8123); works for
Proton OSS and Timeplus Enterprise (set TIMEPLUS_USER/TIMEPLUS_PASSWORD for Enterprise).
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEP = "-- ;;"

TEARDOWN = [
    "DROP VIEW IF EXISTS mv_qoe_decide_cause", "DROP VIEW IF EXISTS mv_qoe_decide_severity",
    "DROP VIEW IF EXISTS mv_qoe_windows", "DROP VIEW IF EXISTS mv_qoe_gen",
    "DROP STREAM IF EXISTS qoe_windows", "DROP STREAM IF EXISTS qoe_events", "DROP STREAM IF EXISTS qoe_raw",
    "DROP VIEW IF EXISTS mv_ddos_decide_action", "DROP VIEW IF EXISTS mv_ddos_decide_kind",
    "DROP VIEW IF EXISTS mv_flow_windows", "DROP VIEW IF EXISTS mv_flow_gen",
    "DROP STREAM IF EXISTS flow_windows", "DROP STREAM IF EXISTS flow_records", "DROP STREAM IF EXISTS flow_raw",
    "DROP FUNCTION IF EXISTS decide_noul", "DROP FUNCTION IF EXISTS decide_choice", "DROP FUNCTION IF EXISTS decide_score",
    "DROP STREAM IF EXISTS decision_log",
]


def load_dotenv():
    p = os.path.join(ROOT, ".env")
    if os.path.exists(p):
        for line in open(p):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def sql(stmt, quiet=False):
    base = os.environ.get("TIMEPLUS_HTTP", "http://localhost:8123")
    req = urllib.request.Request(base + "/", data=stmt.encode("utf-8"), method="POST")
    user, pw = os.environ.get("TIMEPLUS_USER"), os.environ.get("TIMEPLUS_PASSWORD")
    if user:
        req.add_header("X-ClickHouse-User", user)
        req.add_header("X-ClickHouse-Key", pw or "")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            out = r.read().decode("utf-8", "replace").strip()
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", "replace").strip()
        raise RuntimeError("%s\n  -> HTTP %s: %s" % (stmt.strip().splitlines()[0][:100], e.code, msg[:1200]))
    if out and not quiet:
        print(out)
    return out


def statements(path):
    """Split a .sql file into statements on ';\\n' outside of $$ bodies and after stripping -- comments."""
    text = open(os.path.join(ROOT, path), encoding="utf-8").read()
    if SEP in text:
        parts = [p for p in text.split(SEP)]
    else:
        parts = text.split(";\n")
    for chunk in parts:
        lines = []
        in_body = False
        for l in chunk.splitlines():
            if "$$" in l:
                in_body = not in_body if l.count("$$") % 2 else in_body
            if not in_body and l.strip().startswith("--"):
                continue
            lines.append(l)
        body = "\n".join(lines).strip()
        if body:
            yield body if body.endswith(";") else body + ";"


def run_file(path):
    for s in statements(path):
        print("..", s.splitlines()[0][:110])
        sql(s, quiet=True)


def smoke():
    out = sql("""SELECT
      decide_choice('Edge flow window (5 s) for service svc-3. pps=48000 (normal ~2500) unique_sources=25000 (normal ~600) syn_only_ratio=0.91 (normal ~0.2) udp_share=0.02 mean_packet_bytes=60 unique_dst_ports=2. Volume threshold alert fired.',
                    'What should the edge do now?', '["mitigate","observe","escalate"]') AS ddos,
      decide_score('Video QoE window (5 s). degraded_share=0.41 p95_rebuffer_ms=3900 errors: edge_503=812 segment_timeout=790. Degraded sessions span 1 of 3 CDNs, 6 of 6 ISPs, 5 of 5 regions. Threshold alert fired.',
                   'How severe is this window for viewers?', '["negligible","minor","major","critical"]') AS qoe
    FORMAT JSONEachRow""", quiet=True)
    row = json.loads(out.splitlines()[0])
    print("   ddos ->", row["ddos"][:160])
    print("   qoe  ->", row["qoe"][:160])
    if '"err":""' not in row["ddos"]:
        print("   WARNING: the UDF returned an error code; check that the engine container can reach the backend")


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["qoe", "ddos"])
    ap.add_argument("--no-init-hook", action="store_true")
    ap.add_argument("--teardown", action="store_true")
    ap.add_argument("--skip-package", action="store_true")
    a = ap.parse_args()

    backend = os.environ.get("DECISION_BACKEND", "local")
    base_url = os.environ.get("DECISION_BASE_URL", "http://decision:11434")
    model = os.environ.get("DECISION_MODEL", "")
    print("== engine %s | backend %s | base_url %s | model %s" % (
        os.environ.get("TIMEPLUS_HTTP", "http://localhost:8123"), backend, base_url, model or "(any)"))

    if a.teardown:
        for s in TEARDOWN:
            print("..", s); sql(s, quiet=True)
        return

    if not a.skip_package and backend != "mock":
        print("== 0. python package 'requests'")
        try:
            sql("SYSTEM INSTALL PYTHON PACKAGE 'requests'", quiet=True)
        except RuntimeError as e:
            print("   (skipped: %s)" % str(e).splitlines()[-1][:160])

    print("== 1. shared streams")
    run_file("sql/01_streams.sql")

    print("== 2. UDFs (generated into sql/02_udf.sql)")
    cmd = [sys.executable, os.path.join(ROOT, "udf", "build_sql.py"), "--backend", backend,
           "--base-url", base_url, "--model", model, "--pack-rows", "8" if "ollama" in base_url or ":11434" in base_url else "16"]
    if a.no_init_hook:
        cmd.append("--no-init-hook")
    ddl = subprocess.check_output(cmd, text=True)
    open(os.path.join(ROOT, "sql", "02_udf.sql"), "w", encoding="utf-8").write(ddl)
    run_file("sql/02_udf.sql")

    print("== 3. smoke test")
    for attempt in range(3):
        try:
            smoke(); break
        except RuntimeError as e:
            if "ModuleNotFoundError" in str(e) and attempt < 2:
                print("   package still installing, retrying in 5 s"); time.sleep(5); continue
            raise

    if a.only in (None, "qoe"):
        print("== 4. use case 1: QoE triage")
        run_file("sql/10_qoe.sql")
    if a.only in (None, "ddos"):
        print("== 5. use case 2: DDoS response")
        run_file("sql/20_ddos.sql")

    print("== done. Decisions start within ~10 s. Watch:  SELECT * FROM decision_log   (more in sql/04_demo_queries.sql)")


if __name__ == "__main__":
    main()
