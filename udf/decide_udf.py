"""
decide_udf.py — Timeplus Python UDFs that ask a typed decision model one question per row.

This module is the whole body of each CREATE FUNCTION statement (see build_sql.py). It also imports as a
normal Python module so it can be unit-tested outside the engine.

SQL contract (each returns one JSON string per row; the view parses it)
------------
    decide_noul(state, question)                     -> {"p":0.93,"p_raw":0.91, ...common}
    decide_choice(state, question, options)          -> {"choice":"cdn_fault","p":0.81,"probabilities":{...}, ...common}
    decide_score(state, question, levels)            -> {"score":2.3,"level":"major","p":0.66,"probabilities":{...}, ...common}
    common: "model","backend","latency_ms","batch","err"   (on failure the decision fields are null, err is a code)

    `options` / `levels` is a JSON array string, e.g. '["mitigate","observe","escalate"]' (or comma-separated).
    `p` is the calibrated probability of the chosen answer (noul: of "yes").

Backends (the "backend" key of the init parameters)
----------
    "jev"   : POST https://api.typesafe.ai/v1/systemone  (needs TYPESAFE_API_KEY)
    "local" : POST <base_url>/v1/systemone               (llama-server, Ollama, vLLM, or fake_systemone.py)
    "mock"  : in-process keyword heuristic, no network    (pipeline wiring tests only; NOT a model)

Design rules
------------
    * never raise: a raised exception kills the materialized view, so every failure becomes NULL fields + err code
    * pack rows: up to pack_rows rows share one request (one state with items, one question per item)
    * bounded fan-out: at most max_in_flight requests at once
    * pin the model: the response "model" must equal the configured one, else err="model_mismatch"
    * calibrate: per-question temperature on the logit (noul) or on the option logits (choice/score)
    * timeouts: per-request timeout_s and a whole-block block_deadline_s
"""

import hashlib
import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

try:
    import requests  # SYSTEM INSTALL PYTHON PACKAGE 'requests'
except Exception:  # pragma: no cover
    requests = None

DEFAULTS = {
    "backend": "mock",
    "base_url": "http://localhost:8080",
    "model": "",                 # jev: "jev-1.13.0"; local: id the server reports; "" = accept any
    "api_key_env": "TYPESAFE_API_KEY",
    "pack_rows": 16,             # rows per request (local servers cap questions at 64)
    "max_in_flight": 6,
    "timeout_s": 5.0,
    "block_deadline_s": 20.0,
    "retries": 1,
    "temperatures": {},          # {"<question text>": T}
    "cache_ttl_s": 0,
    "cache_max": 10000,
}

_CFG = dict(DEFAULTS)
_SESSION = None
_INIT_LOCK = threading.Lock()
_INITIALIZED = False
_CACHE = {}
_CACHE_LOCK = threading.Lock()


def _tp_init(params=None):
    """Init hook: `params` is the single string from SETTINGS init_function_parameters (JSON)."""
    global _CFG, _SESSION, _INITIALIZED
    with _INIT_LOCK:
        cfg = dict(DEFAULTS)
        if params:
            try:
                cfg.update(json.loads(params) if isinstance(params, str) else dict(params))
            except Exception:
                pass
        for key in ("backend", "base_url", "model"):
            env = os.environ.get("DECISION_" + key.upper())
            if env:
                cfg[key] = env
        _CFG = cfg
        if requests is not None and cfg["backend"] in ("jev", "local"):
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=int(cfg["max_in_flight"]),
                                                    pool_maxsize=int(cfg["max_in_flight"]) * 2)
            s.mount("http://", adapter)
            s.mount("https://", adapter)
            _SESSION = s
        _INITIALIZED = True


def _ensure_init():
    if not _INITIALIZED:
        _tp_init(os.environ.get("DECISION_UDF_PARAMS"))


# ----------------------------------------------------------------------------- helpers

def _as_str(x):
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return "" if x is None else str(x)


def _parse_options(s):
    s = _as_str(s).strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            return [str(o) for o in json.loads(s)]
        except Exception:
            pass
    return [o.strip() for o in s.split(",") if o.strip()]


def _temp(question):
    t = _CFG.get("temperatures", {}).get(question)
    return float(t) if t and float(t) > 0 else 1.0


def _cal_noul(p, question):
    t = _temp(question)
    if t == 1.0 or p is None:
        return p
    p = min(max(float(p), 1e-6), 1 - 1e-6)
    return 1.0 / (1.0 + math.exp(-math.log(p / (1 - p)) / t))


def _cal_dist(probs, question):
    """Temperature-scale a probability distribution over options; returns a new dict."""
    t = _temp(question)
    if not probs:
        return probs
    if t == 1.0:
        return dict(probs)
    logits = {k: math.log(max(float(v), 1e-9)) / t for k, v in probs.items()}
    m = max(logits.values())
    ex = {k: math.exp(v - m) for k, v in logits.items()}
    z = sum(ex.values())
    return {k: v / z for k, v in ex.items()}


def _endpoint():
    if _CFG["backend"] == "jev":
        return "https://api.typesafe.ai/v1/systemone"
    return _CFG["base_url"].rstrip("/") + "/v1/systemone"


def _headers():
    h = {"Content-Type": "application/json"}
    if _CFG["backend"] == "jev":
        key = os.environ.get(_CFG.get("api_key_env", "TYPESAFE_API_KEY"), "") or _CFG.get("api_key", "")
        h["Authorization"] = "Bearer " + key
    return h


# ----------------------------------------------------------------------------- request / response

def _build_request(states, question, qtype, options):
    """One state holding N items, one question per item. Same shape for Jev Cloud and local servers."""
    items = [{"item": i, "event": s} for i, s in enumerate(states)]
    questions = {}
    for i in range(len(states)):
        q = {"type": qtype, "instructions": "%s Consider only the event with \"item\": %d." % (question, i)}
        if qtype == "choice":
            q["criteria"] = {o: None for o in options}
        elif qtype == "score":
            q["criteria"] = list(options)
        questions["q%d" % i] = q
    body = {"state": {"items": items}, "questions": questions}
    if _CFG.get("model"):
        body["model"] = _CFG["model"]
    return body


def _parse_answer(a, qtype, options):
    """Normalise one answer object into a dict of decision fields (raw, uncalibrated) or None."""
    if not isinstance(a, dict):
        return None
    try:
        if qtype == "noul":
            return {"p_raw": float(a["noul"])}
        if qtype == "choice":
            probs = {str(k): float(v) for k, v in (a.get("probabilities") or {}).items()}
            choice = a.get("choice")
            if choice is None and probs:
                choice = max(probs, key=probs.get)
            if choice is None:
                return None
            return {"choice": str(choice), "probabilities": probs, "p_raw": float(a.get("confidence", probs.get(str(choice), 0.0)))}
        if qtype == "score":
            probs = {str(k): float(v) for k, v in (a.get("probabilities") or {}).items()}  # keys "0","1",...
            score = a.get("score")
            if score is None and probs:
                score = sum(int(k) * v for k, v in probs.items())
            if score is None:
                return None
            idx = max(probs, key=probs.get) if probs else str(int(round(float(score))))
            level = options[int(idx)] if options and 0 <= int(idx) < len(options) else idx
            return {"score": float(score), "level": level, "probabilities": probs,
                    "p_raw": float(a.get("confidence", probs.get(idx, 0.0)))}
    except Exception:
        return None
    return None


def _http_backend(states, question, qtype, options, deadline):
    """Returns (answers: list[dict|None], model, err). Never raises."""
    if _SESSION is None:
        return [None] * len(states), "", "no_http_client"
    body = _build_request(states, question, qtype, options)
    attempts = int(_CFG.get("retries", 1)) + 1
    last_err = "unknown"
    for attempt in range(attempts):
        remaining = deadline - time.time()
        if remaining <= 0:
            return [None] * len(states), "", "block_deadline"
        try:
            r = _SESSION.post(_endpoint(), headers=_headers(), json=body, timeout=min(float(_CFG["timeout_s"]), remaining))
        except Exception as e:
            last_err = "timeout" if "timeout" in type(e).__name__.lower() or "timed out" in str(e).lower() else "conn_error"
        else:
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    return [None] * len(states), "", "bad_json"
                model = data.get("model", "")
                want = _CFG.get("model")
                if want and model and model != want:
                    return [None] * len(states), model, "model_mismatch"
                answers = data.get("answers", {})
                return [_parse_answer(answers.get("q%d" % i), qtype, options) for i in range(len(states))], model, ""
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = "http_%d" % r.status_code
            else:
                return [None] * len(states), "", "http_%d" % r.status_code
        if attempt + 1 < attempts:
            time.sleep(min(0.2 * (2 ** attempt), max(0.0, deadline - time.time())))
    return [None] * len(states), "", last_err


# Mock backend: a transparent keyword heuristic so the SQL pipeline can be exercised with no model at all.
# It is NOT a model and its accuracy on the simulators means nothing; use it only for wiring and chaos tests.
_KW = {
    # QoE causes (substrings of the window state text produced by sql/10_qoe.sql)
    "cdn_fault": ("1 of 3 cdns",), "isp_congestion": ("1 of 6 isps",),
    "player_bug": ("1 of 3 player",), "encoding_issue": ("1 of 20 assets",),
    "traffic_spike_benign": ("load_ratio=3", "load_ratio=4", "load_ratio=5", "load_ratio=6"),
    "no_incident": ("no threshold alert",),
    "negligible": ("no threshold alert",), "minor": ("1 of 20 assets",), "major": ("1 of 6 isps", "1 of 3 player"),
    "critical": ("1 of 3 cdns",),
    # DDoS kinds / actions (substrings of the state text produced by sql/20_ddos.sql)
    "syn_flood": ("syn_only_ratio=0.8", "syn_only_ratio=0.9", "syn_only_ratio=1"),
    "udp_amplification": ("udp_share=0.6", "udp_share=0.7", "udp_share=0.8", "udp_share=0.9"),
    "http_flood": ("mean_packet_bytes=6", "pps=5"), "port_scan": ("unique_dst_ports=1", "unique_dst_ports=2", "unique_dst_ports=3", "unique_dst_ports=4", "unique_dst_ports=5"),
    "legitimate_surge": ("no volume threshold",), "normal": ("no volume threshold",),
    "mitigate": ("volume threshold alert fired",), "observe": ("no volume threshold",), "escalate": (),
}


def _mock_backend(states, question, qtype, options, deadline):
    out = []
    for s in states:
        t = s.lower()
        if qtype == "noul":
            risky = sum(1 for kw in ("rm -rf", "drop table", "credentials", ".ssh", "pastebin", "| sh", "ssn", "akia", "prod") if kw in t)
            out.append({"p_raw": min(0.95, 0.1 + 0.3 * risky)})
            continue
        scores = {}
        for o in options:
            scores[o] = 0.5 + sum(1.0 for kw in _KW.get(o, ()) if kw in t) + (0.3 if o.lower() in t else 0)
        z = sum(scores.values()) or 1.0
        probs = {o: v / z for o, v in scores.items()}
        best = max(probs, key=probs.get)
        if qtype == "choice":
            out.append({"choice": best, "probabilities": probs, "p_raw": probs[best]})
        else:
            idx = options.index(best)
            p_idx = {str(i): probs[o] for i, o in enumerate(options)}
            out.append({"score": sum(i * p for i, p in ((int(k), v) for k, v in p_idx.items())),
                        "level": best, "probabilities": p_idx, "p_raw": probs[best]})
    return out, "mock-heuristic-v0", ""


# ----------------------------------------------------------------------------- block driver

def _cache_get(key):
    ttl = float(_CFG.get("cache_ttl_s", 0) or 0)
    if ttl <= 0:
        return None
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and hit[0] > time.time():
            return hit[1]
        if hit:
            _CACHE.pop(key, None)
    return None


def _cache_put(key, ans):
    ttl = float(_CFG.get("cache_ttl_s", 0) or 0)
    if ttl <= 0 or ans is None:
        return
    with _CACHE_LOCK:
        if len(_CACHE) >= int(_CFG.get("cache_max", 10000)):
            _CACHE.clear()
        _CACHE[key] = (time.time() + ttl, ans)


def _decide_block(states, questions, qtype, options_list):
    """Lists of str in, list of result dicts out. Never raises."""
    _ensure_init()
    n = len(states)
    results = [None] * n
    t_block = time.time()
    deadline = t_block + float(_CFG["block_deadline_s"])
    backend = _CFG["backend"]
    batch_id = hashlib.sha1(("%f%d" % (t_block, n)).encode()).hexdigest()[:4]

    groups = {}
    for i in range(n):
        opts_key = json.dumps(options_list[i])
        key = hashlib.sha256((qtype + "\x00" + questions[i] + "\x00" + opts_key + "\x00" + states[i]).encode()).hexdigest()
        cached = _cache_get(key)
        if cached is not None:
            results[i] = {"ans": cached, "model": "cache", "latency_ms": 0, "err": ""}
            continue
        groups.setdefault((questions[i], opts_key), []).append((i, key))

    jobs = []
    pack = max(1, int(_CFG["pack_rows"]))
    for (q, opts_key), rows in groups.items():
        for k in range(0, len(rows), pack):
            jobs.append((q, json.loads(opts_key), rows[k:k + pack]))

    def run(job):
        q, opts, rows = job
        t0 = time.time()
        fn = _mock_backend if backend == "mock" else _http_backend
        answers, model, err = fn([states[i] for i, _ in rows], q, qtype, opts, deadline)
        return rows, answers, model, err, int((time.time() - t0) * 1000)

    if jobs:
        with ThreadPoolExecutor(max_workers=max(1, min(int(_CFG["max_in_flight"]), len(jobs)))) as ex:
            pending = {ex.submit(run, j) for j in jobs}
            while pending:
                done, pending = wait(pending, timeout=max(0.05, deadline - time.time()), return_when=FIRST_COMPLETED)
                for f in done:
                    try:
                        rows, answers, model, err, ms = f.result()
                    except Exception:
                        continue
                    for (i, key), ans in zip(rows, answers):
                        e = err or ("no_answer" if ans is None else "")
                        results[i] = {"ans": ans, "model": model, "latency_ms": ms, "err": e}
                        if not e:
                            _cache_put(key, ans)
                if time.time() > deadline and pending:
                    for f in pending:
                        f.cancel()
                    break

    out = []
    for i in range(n):
        r = results[i] or {"ans": None, "model": "", "latency_ms": int((time.time() - t_block) * 1000), "err": "block_deadline"}
        ans = r["ans"] or {}
        q = questions[i]
        row = {"model": r["model"], "backend": backend, "latency_ms": r["latency_ms"], "batch": batch_id, "err": r["err"]}
        if qtype == "noul":
            p_raw = ans.get("p_raw")
            row.update({"p": _cal_noul(p_raw, q) if p_raw is not None else None, "p_raw": p_raw})
        elif qtype == "choice":
            probs = _cal_dist(ans.get("probabilities") or {}, q)
            choice = ans.get("choice")
            row.update({"choice": choice, "p": probs.get(choice, ans.get("p_raw")) if choice is not None else None,
                        "p_raw": ans.get("p_raw"), "probabilities": probs or None})
        else:
            probs = _cal_dist(ans.get("probabilities") or {}, q)
            level = ans.get("level")
            idx = max(probs, key=probs.get) if probs else None
            row.update({"score": ans.get("score"), "level": level,
                        "p": probs.get(idx, ans.get("p_raw")) if idx is not None else ans.get("p_raw"),
                        "p_raw": ans.get("p_raw"), "probabilities": probs or None})
        out.append(row)
    return out


def _run(state, question, qtype, options):
    try:
        n = len(state)
        states = [_as_str(s) for s in state]
        questions = [_as_str(q) for q in question]
        opts = [_parse_options(o) for o in options] if options is not None else [[] for _ in range(n)]
        return [json.dumps(r, separators=(",", ":")) for r in _decide_block(states, questions, qtype, opts)]
    except Exception as e:  # last line of defence: never let an exception reach the engine
        err = json.dumps({"p": None, "p_raw": None, "model": "", "backend": _CFG.get("backend", ""),
                          "latency_ms": 0, "batch": "", "err": "udf_" + type(e).__name__}, separators=(",", ":"))
        return [err] * len(state)


# ----------------------------------------------------------------------------- SQL-facing functions
# (names must match the CREATE FUNCTION names; build_sql.py emits one statement per function)

def decide_noul(state, question):
    return _run(state, question, "noul", None)


def decide_choice(state, question, options):
    return _run(state, question, "choice", options)


def decide_score(state, question, levels):
    return _run(state, question, "score", levels)
