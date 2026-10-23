-- 02_udf.sql — the decision UDF. One Python UDF (`decide`) that runs inside Timeplus, plus three SQL
-- lambda wrappers that give the views the typed names. This file is the source of truth: edit the Python
-- here. Requires Timeplus Enterprise (Python UDFs are not built into Proton OSS).
--
--   decide_noul  (state, question)           -> {"p":0.93,"p_raw":0.91, ...common}
--   decide_choice(state, question, options)  -> {"choice":"cdn_fault","p":0.81,"probabilities":{...}, ...common}
--   decide_score (state, question, levels)   -> {"score":2.3,"level":"major","p":0.66,"probabilities":{...}, ...common}
--   common: "model","backend","latency_ms","batch","err"  (on failure the decision fields are null, err is a code)
--
--   `options` / `levels` is a JSON array string, e.g. '["mitigate","observe","escalate"]' (or comma-separated).
--   `p` is the calibrated probability of the chosen answer (noul: of "yes").
--
-- Configuration comes from the engine process environment (docker-compose passes these in):
--   DECISION_BACKEND          jev | local | mock          (default mock)
--   DECISION_BASE_URL         local: http://host:port of the /v1/systemone server (default http://decision:11434)
--   DECISION_MODEL            pin the model id; "" = accept whatever the server reports
--   DECISION_PACK_ROWS        rows per request (default 8 for Ollama URLs, else 16)
--   DECISION_MAX_IN_FLIGHT    concurrent requests per block (default 6)
--   DECISION_TIMEOUT_S        per-request timeout (default 5)
--   DECISION_BLOCK_DEADLINE_S whole-block deadline (default 20)
--   DECISION_RETRIES          retries on 429/5xx/timeout (default 1)
--   DECISION_CACHE_TTL_S      answer cache TTL, 0 = off (default 0)
--   DECISION_TEMPERATURES     JSON {"<question text>": T} for calibration (default {})
--   TYPESAFE_API_KEY          jev only
--
-- Design rules: never raise (a raised exception kills the materialized view); pack rows into one request;
-- bounded fan-out; pin the model; per-question temperature calibration; per-request and per-block timeouts.
-- Standard library only (urllib), so no SYSTEM INSTALL PYTHON PACKAGE step is needed.

CREATE OR REPLACE FUNCTION decide(state string, question string, kind string, spec string)
RETURNS string
LANGUAGE PYTHON AS $$
import hashlib
import json
import math
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

DEFAULTS = {
    "backend": "mock",
    "base_url": "http://decision:11434",
    "model": "",
    "pack_rows": 0,              # 0 = 8 for Ollama-looking URLs, else 16 (local servers cap questions at 64)
    "max_in_flight": 6,
    "timeout_s": 5.0,
    "block_deadline_s": 20.0,
    "retries": 1,
    "temperatures": {},
    "cache_ttl_s": 0,
    "cache_max": 10000,
}

_CFG = dict(DEFAULTS)
_INIT_LOCK = threading.Lock()
_INITIALIZED = False
_CACHE = {}
_CACHE_LOCK = threading.Lock()


def _init():
    """Read DECISION_* from the engine environment once; defaults otherwise. Never raises."""
    global _CFG, _INITIALIZED
    with _INIT_LOCK:
        if _INITIALIZED:
            return
        cfg = dict(DEFAULTS)
        for key in ("backend", "base_url", "model"):
            v = os.environ.get("DECISION_" + key.upper())
            if v:
                cfg[key] = v.strip()
        for key in ("pack_rows", "max_in_flight", "retries", "cache_max"):
            v = os.environ.get("DECISION_" + key.upper())
            if v:
                try:
                    cfg[key] = int(v)
                except Exception:
                    pass
        for key in ("timeout_s", "block_deadline_s", "cache_ttl_s"):
            v = os.environ.get("DECISION_" + key.upper())
            if v:
                try:
                    cfg[key] = float(v)
                except Exception:
                    pass
        v = os.environ.get("DECISION_TEMPERATURES")
        if v:
            try:
                cfg["temperatures"] = dict(json.loads(v))
            except Exception:
                pass
        if cfg["backend"] not in ("jev", "local", "mock"):
            cfg["backend"] = "mock"
        if int(cfg["pack_rows"]) <= 0:
            u = cfg["base_url"].lower()
            cfg["pack_rows"] = 8 if ("ollama" in u or ":11434" in u) else 16
        _CFG = cfg
        _INITIALIZED = True


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
        h["Authorization"] = "Bearer " + os.environ.get("TYPESAFE_API_KEY", "")
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
            return {"choice": str(choice), "probabilities": probs,
                    "p_raw": float(a.get("confidence", probs.get(str(choice), 0.0)))}
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


def _post(url, body, timeout):
    """Returns (status, text). Raises on connection problems and timeouts only."""
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            text = e.read().decode("utf-8", "replace")
        except Exception:
            text = ""
        return e.code, text


def _http_backend(states, question, qtype, options, deadline):
    """Returns (answers: list[dict|None], model, err). Never raises."""
    body = _build_request(states, question, qtype, options)
    attempts = int(_CFG.get("retries", 1)) + 1
    last_err = "unknown"
    for attempt in range(attempts):
        remaining = deadline - time.time()
        if remaining <= 0:
            return [None] * len(states), "", "block_deadline"
        try:
            status, text = _post(_endpoint(), body, min(float(_CFG["timeout_s"]), remaining))
        except Exception as e:
            name = type(e).__name__.lower()
            msg = str(e).lower()
            last_err = "timeout" if ("timeout" in name or "timed out" in msg) else "conn_error"
        else:
            if status == 200:
                try:
                    data = json.loads(text)
                except Exception:
                    return [None] * len(states), "", "bad_json"
                model = data.get("model", "")
                want = _CFG.get("model")
                if want and model and model != want:
                    return [None] * len(states), model, "model_mismatch"
                answers = data.get("answers", {})
                return [_parse_answer(answers.get("q%d" % i), qtype, options) for i in range(len(states))], model, ""
            if status in (429, 500, 502, 503, 504):
                last_err = "http_%d" % status
            else:
                return [None] * len(states), "", "http_%d" % status
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
    "http_flood": ("mean_packet_bytes=6", "pps=5"),
    "port_scan": ("unique_dst_ports=1", "unique_dst_ports=2", "unique_dst_ports=3", "unique_dst_ports=4", "unique_dst_ports=5"),
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
        if not options:
            out.append(None)
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
            p_idx = {str(i): probs[o] for i, o in enumerate(options)}
            out.append({"score": sum(int(k) * v for k, v in p_idx.items()),
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
    """Lists of str in, list of result dicts out (one per row, same order). Never raises."""
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


def _broadcast(col, n):
    """The engine may hand a constant argument as a 1-element list; stretch it to the block length."""
    col = list(col) if col is not None else []
    if len(col) == n:
        return col
    if len(col) == 1 and n > 1:
        return col * n
    return (col + [None] * n)[:n]


# ----------------------------------------------------------------------------- SQL-facing function

def decide(state, question, kind, spec):
    """Block UDF: lists in, list of JSON strings out. kind is 'noul' | 'choice' | 'score'; spec is the option list."""
    try:
        _init()
        n = len(state)
        states = [_as_str(s) for s in state]
        questions = [_as_str(q) for q in _broadcast(question, n)]
        kinds = [_as_str(k).strip().lower() or "noul" for k in _broadcast(kind, n)]
        opts = [_parse_options(o) for o in _broadcast(spec, n)]
        out = [None] * n
        for qtype in set(kinds):                       # normally one kind per block (one view, one function)
            idx = [i for i in range(n) if kinds[i] == qtype]
            if qtype not in ("noul", "choice", "score"):
                for i in idx:
                    out[i] = {"p": None, "p_raw": None, "model": "", "backend": _CFG.get("backend", ""),
                              "latency_ms": 0, "batch": "", "err": "bad_kind"}
                continue
            rows = _decide_block([states[i] for i in idx], [questions[i] for i in idx], qtype, [opts[i] for i in idx])
            for i, r in zip(idx, rows):
                out[i] = r
        return [json.dumps(r, separators=(",", ":")) for r in out]
    except Exception as e:  # last line of defence: never let an exception reach the engine
        err = json.dumps({"p": None, "p_raw": None, "model": "", "backend": _CFG.get("backend", ""),
                          "latency_ms": 0, "batch": "", "err": "udf_" + type(e).__name__}, separators=(",", ":"))
        return [err] * len(state)
$$;

-- Typed names used by the views. SQL lambdas: no second copy of the Python.
CREATE OR REPLACE FUNCTION decide_noul   AS (state, question)          -> decide(state, question, 'noul',   '');
CREATE OR REPLACE FUNCTION decide_choice AS (state, question, options) -> decide(state, question, 'choice', options);
CREATE OR REPLACE FUNCTION decide_score  AS (state, question, levels)  -> decide(state, question, 'score',  levels);
