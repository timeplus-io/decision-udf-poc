#!/usr/bin/env python3
"""
fake_systemone.py — a stdlib-only stand-in for a /v1/systemone decision server (noul, choice, score).

Same request/response shape as Jev Cloud, Ollama and llama-server, so the UDF's HTTP path can be exercised
end-to-end with no model, no GPU and no network. Answers come from a deliberately incomplete, noisy keyword
heuristic: it is NOT a model and its accuracy means nothing. Use it for wiring, chaos and latency tests.

    python backend/fake_systemone.py --port 8080 --latency-ms 120 --jitter-ms 60 --fail-rate 0.0
"""
import argparse
import json
import math
import random
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL = "fake-systemone-v0"
NOISE = 0.8  # on logits

# option -> keywords that raise its score (incomplete on purpose)
_KW = {
    "cdn_fault": ("edge_503", "1 of 3 cdns"), "isp_congestion": ("1 of 6 isps", "bitrate_kbps=4", "bitrate_kbps=5", "bitrate_kbps=6"),
    "player_bug": ("abr_null_ref", "1 of 3 player"), "encoding_issue": ("decoder_error", "1 of 20 assets"),
    "traffic_spike_benign": ("load_ratio=3", "load_ratio=4", "load_ratio=5"), "no_incident": ("no threshold alert",),
    "negligible": ("no threshold alert",), "minor": ("decoder_error",), "major": ("abr_null_ref", "1 of 6 isps"), "critical": ("edge_503",),
    "syn_flood": ("syn_only_ratio=0.6", "syn_only_ratio=0.7", "syn_only_ratio=0.8", "syn_only_ratio=0.9"),
    "udp_amplification": ("udp_share=0.5", "udp_share=0.6", "udp_share=0.7", "udp_share=0.8", "11211=", "123="),
    "http_flood": ("443=", "mean_packet_bytes=6", "mean_packet_bytes=7"), "port_scan": ("unique_dst_ports=1", "unique_dst_ports=2", "unique_dst_ports=3", "unique_dst_ports=4"),
    "legitimate_surge": ("no volume threshold",), "normal": ("no volume threshold",),
    "mitigate": ("syn_only_ratio=0.6", "syn_only_ratio=0.7", "udp_share=0.6", "volume threshold alert fired"),
    "observe": ("no volume threshold",), "escalate": (),
}
_NOUL_RISKY = ("rm -rf", "drop table", "credentials", ".ssh", "pastebin", "| sh", "ssn", "akia", "prod")


def _dist(text, options):
    t = text.lower()
    logits = {}
    for o in options:
        s = 0.0 + sum(1.2 for kw in _KW.get(o, ()) if kw in t)
        logits[o] = s + random.gauss(0, NOISE)
    m = max(logits.values())
    ex = {k: math.exp(v - m) for k, v in logits.items()}
    z = sum(ex.values())
    return {k: round(v / z, 4) for k, v in ex.items()}


def answer(q, text):
    qt = q.get("type")
    if qt == "noul":
        s = 0.1 + 0.3 * sum(1 for kw in _NOUL_RISKY if kw in text.lower())
        z = math.log(min(max(s, 0.03), 0.97) / (1 - min(max(s, 0.03), 0.97))) + random.gauss(0, NOISE)
        return {"type": "noul", "noul": round(1 / (1 + math.exp(-z)), 4)}
    if qt == "choice":
        opts = list((q.get("criteria") or {}).keys())
        if not opts:
            return {"type": "choice", "error": "criteria required"}
        probs = _dist(text, opts)
        best = max(probs, key=probs.get)
        return {"type": "choice", "choice": best, "confidence": probs[best], "probabilities": probs}
    if qt == "score":
        levels = list(q.get("criteria") or [])
        if not levels:
            return {"type": "score", "error": "criteria required"}
        probs = _dist(text, levels)
        by_idx = {str(i): probs[l] for i, l in enumerate(levels)}
        score = sum(i * probs[l] for i, l in enumerate(levels))
        best = max(by_idx, key=by_idx.get)
        return {"type": "score", "score": round(score, 3), "confidence": by_idx[best], "probabilities": by_idx,
                "legend": {str(i): l for i, l in enumerate(levels)}}
    return {"type": qt, "error": "unsupported type"}


class Handler(BaseHTTPRequestHandler):
    cfg = {"latency_ms": 100, "jitter_ms": 50, "fail_rate": 0.0}

    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/v1/models") or self.path.startswith("/api/tags"):
            return self._send(200, {"data": [{"id": MODEL}], "models": [{"name": MODEL}]})
        if self.path.startswith("/health"):
            return self._send(200, {"status": "ok"})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/v1/systemone"):
            return self._send(404, {"error": "not found"})
        n = int(self.headers.get("Content-Length", 0))
        try:
            req = json.loads(self.rfile.read(n))
        except Exception:
            return self._send(400, {"error": "bad json"})
        if random.random() < self.cfg["fail_rate"]:
            return self._send(503, {"error": "simulated failure"})
        time.sleep(max(0.0, (self.cfg["latency_ms"] + random.uniform(-1, 1) * self.cfg["jitter_ms"]) / 1000.0))
        state, questions = req.get("state"), req.get("questions", {})
        if not isinstance(questions, dict) or not questions:
            return self._send(400, {"error": "questions required"})
        if len(questions) > 64:
            return self._send(400, {"error": "too many questions (max 64)"})
        items = state.get("items") if isinstance(state, dict) else None
        answers = {}
        for name, q in questions.items():
            text = json.dumps(state)
            instr = str(q.get("instructions", ""))
            if items and '"item": ' in instr:
                try:
                    text = json.dumps(items[int(instr.split('"item": ')[1].split(".")[0].strip())])
                except Exception:
                    pass
            answers[name] = answer(q, text)
        self._send(200, {"model": MODEL, "answers": answers,
                         "usage": {"input_tokens": len(json.dumps(state)) // 4, "output_tokens": len(answers)}})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--latency-ms", type=float, default=100)
    ap.add_argument("--jitter-ms", type=float, default=50)
    ap.add_argument("--fail-rate", type=float, default=0.0)
    a = ap.parse_args()
    Handler.cfg = {"latency_ms": a.latency_ms, "jitter_ms": a.jitter_ms, "fail_rate": a.fail_rate}
    print("fake /v1/systemone on %s:%d (latency %.0f±%.0f ms, fail %.0f%%)" %
          (a.host, a.port, a.latency_ms, a.jitter_ms, a.fail_rate * 100), file=sys.stderr)
    ThreadingHTTPServer((a.host, a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
