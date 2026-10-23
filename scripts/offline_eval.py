#!/usr/bin/env python3
"""
offline_eval.py — try a backend on synthetic window states WITHOUT an engine, and print the scorecard.

The states mimic what sql/10_qoe.sql and sql/20_ddos.sql hand to the model (same wording, same numbers'
ranges), with truth from the same scenario lists, so a backend can be sanity-checked before wiring it in.

    python scripts/offline_eval.py --backend local --base-url http://localhost:11434 --model tev1:0.8b
    python scripts/offline_eval.py --backend jev --model jev-1.13.0            # needs TYPESAFE_API_KEY
    python scripts/offline_eval.py --backend mock --n 200
"""
import argparse
import json
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "udf"))
import decide_udf as udf  # noqa: E402

QOE_OPTS = ["cdn_fault", "isp_congestion", "player_bug", "encoding_issue", "traffic_spike_benign", "no_incident"]
SEV = ["negligible", "minor", "major", "critical"]
ACT = ["mitigate", "observe", "escalate"]
KIND = ["syn_flood", "udp_amplification", "http_flood", "port_scan", "legitimate_surge", "normal"]
Q_CAUSE = "What is the most likely cause of the viewer-quality situation in this window?"
Q_SEV = "How severe is this window for viewers?"
Q_ACT = "What should the edge do now for this service? mitigate = rate-limit or block the offending traffic, observe = keep watching, escalate = page a human."
Q_KIND = "What kind of traffic is this service receiving in this window?"


def qoe_window(truth):
    r = random.Random()
    ev = 10000
    base = dict(load=1.0, deg=0.03, p95=350, startup=1400, bitrate=4000, e503=0, eto=ev // 100, edec=0, eabr=0,
                cdns=3, isps=6, regions=5, players=3, assets=20)
    if truth == "cdn_fault":
        base.update(deg=0.38, p95=3800, startup=2900, e503=1900, eto=2000, cdns=1)
    elif truth == "isp_congestion":
        base.update(deg=0.18, p95=2600, bitrate=2900, isps=1)
    elif truth == "player_bug":
        base.update(deg=0.16, startup=4800, eabr=1300, players=1)
    elif truth == "encoding_issue":
        base.update(deg=0.06, p95=900, edec=300, assets=1)
    elif truth == "flash_crowd":
        base.update(load=4.5)
    j = lambda v, s: round(v * (1 + r.uniform(-s, s)), 3) if isinstance(v, float) else int(v * (1 + r.uniform(-s, s)))
    b = {k: (j(v, 0.15) if k in ("load", "deg", "p95", "startup", "bitrate", "e503", "eto", "edec", "eabr") else v) for k, v in base.items()}
    flag = b["p95"] > 1200 or b["deg"] > 0.08 or b["load"] > 2.0
    state = ("Video QoE window (5 s). events=%d sessions=%d load_ratio=%.2f (1.0 is normal) degraded_share=%.3f (0.03 is normal) "
             "p95_rebuffer_ms=%d (normal < 500) p50_startup_ms=%d (normal ~1400) p50_bitrate_kbps=%d (normal ~4000). "
             "errors: edge_503=%d segment_timeout=%d decoder_error=%d abr_null_ref=%d. Degraded sessions span %d of 3 CDNs, "
             "%d of 6 ISPs, %d of 5 regions, %d of 3 player versions, %d of 20 assets.%s"
             % (ev, int(ev * 0.9), b["load"], b["deg"], b["p95"], b["startup"], b["bitrate"], b["e503"], b["eto"], b["edec"], b["eabr"],
                b["cdns"], b["isps"], b["regions"], b["players"], b["assets"],
                " Threshold alert fired." if flag else " No threshold alert."))
    exp_cause = {"flash_crowd": "traffic_spike_benign", "none": "no_incident"}.get(truth, truth)
    exp_sev = {"cdn_fault": "critical", "isp_congestion": "major", "player_bug": "major", "encoding_issue": "minor"}.get(truth, "negligible")
    return state, flag, exp_cause, exp_sev


def flow_window(truth):
    r = random.Random()
    b = dict(pps=2500, mbps=13.0, src=600, syn=0.2, udp=0.05, pkt=650, ports=3, p443=2100, p80=600, p53=10, p123=0, p11211=0)
    if truth == "syn_flood":
        b.update(pps=45000, mbps=22.0, src=28000, syn=0.9, pkt=70, p443=44000)
    elif truth == "udp_amplification":
        b.update(pps=38000, mbps=420.0, src=640, udp=0.8, pkt=1380, ports=5, p53=9000, p123=8800, p11211=9100)
    elif truth == "http_flood":
        b.update(pps=52000, mbps=260.0, src=2600, syn=0.1, pkt=620, p443=24000)
    elif truth == "port_scan":
        b.update(pps=2700, src=610, syn=0.3, ports=240)
    elif truth == "flash_crowd":
        b.update(pps=12000, mbps=60.0, src=9000, syn=0.2, p443=10000, p80=2800)
    j = lambda v, s: round(v * (1 + r.uniform(-s, s)), 3)
    b = {k: j(v, 0.15) for k, v in b.items()}
    flag = b["pps"] > 20000 or b["src"] > 6000
    state = ("Edge flow window (5 s) for service svc-3. records=%d pps=%d (normal ~2500) mbps=%.1f unique_sources=%d (normal ~600) "
             "syn_only_ratio=%.2f (normal ~0.2) udp_share=%.2f (normal ~0.05) mean_packet_bytes=%d (normal ~650) unique_dst_ports=%d (normal ~3). "
             "flows by port: 443=%d 80=%d 53=%d 123=%d 11211=%d.%s"
             % (int(b["pps"] / 20), b["pps"], b["mbps"], b["src"], b["syn"], b["udp"], b["pkt"], b["ports"],
                b["p443"], b["p80"], b["p53"], b["p123"], b["p11211"],
                " Volume threshold alert fired." if flag else " No volume threshold alert."))
    exp_act = "mitigate" if truth in ("syn_flood", "udp_amplification", "http_flood") else "observe"
    exp_kind = {"flash_crowd": "legitimate_surge", "none": "normal"}.get(truth, truth)
    return state, flag, exp_act, exp_kind


def run(fn, states, question, options):
    t0 = time.time()
    out = [json.loads(x) for x in fn([s.encode() for s in states], [question.encode()] * len(states), [json.dumps(options).encode()] * len(states))]
    return out, time.time() - t0


def report(name, rows, expected, key):
    ok = [(r, e) for r, e in zip(rows, expected) if r["err"] == ""]
    lat = sorted(r["latency_ms"] for r, _ in ok) or [0]
    acc = sum(r[key] == e for r, e in ok) / max(len(ok), 1)
    conf = [(r, e) for r, e in ok if (r["p"] or 0) >= 0.85]
    auto_acc = sum(r[key] == e for r, e in conf) / max(len(conf), 1)
    print("%-22s n=%3d ok=%3d acc=%5.1f%%  p50=%4d ms p99=%4d ms  auto@0.85=%4.0f%% (acc %4.0f%%)  model=%s" % (
        name, len(rows), len(ok), 100 * acc, lat[len(lat) // 2], lat[int(len(lat) * 0.99) - 1],
        100 * len(conf) / max(len(ok), 1), 100 * auto_acc, ok[0][0]["model"] if ok else "-"))
    conf_m = {}
    for r, e in ok:
        conf_m.setdefault(e, {}).setdefault(r[key], 0)
        conf_m[e][r[key]] += 1
    for e in sorted(conf_m):
        print("    %-22s -> %s" % (e, ", ".join("%s:%d" % kv for kv in sorted(conf_m[e].items(), key=lambda kv: -kv[1]))))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", default="mock", choices=["mock", "local", "jev"])
    ap.add_argument("--base-url", default="http://localhost:11434")
    ap.add_argument("--model", default="")
    ap.add_argument("--n", type=int, default=120, help="windows per use case")
    ap.add_argument("--pack-rows", type=int, default=8)
    ap.add_argument("--max-in-flight", type=int, default=4)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dump", default="")
    a = ap.parse_args()
    random.seed(a.seed)
    udf._tp_init(json.dumps({"backend": a.backend, "base_url": a.base_url, "model": a.model,
                             "pack_rows": a.pack_rows, "max_in_flight": a.max_in_flight}))

    qoe_truths = ["none", "cdn_fault", "isp_congestion", "player_bug", "encoding_issue", "flash_crowd"]
    ddos_truths = ["none", "syn_flood", "udp_amplification", "http_flood", "port_scan", "flash_crowd"]
    qw = [(t,) + qoe_window(t) for t in (random.choice(qoe_truths) for _ in range(a.n))]
    fw = [(t,) + flow_window(t) for t in (random.choice(ddos_truths) for _ in range(a.n))]

    print("backend=%s  base_url=%s  model=%s" % (a.backend, a.base_url if a.backend == "local" else "-", a.model or "(any)"))
    cause, _ = run(udf.decide_choice, [w[1] for w in qw], Q_CAUSE, QOE_OPTS)
    report("qoe cause (choice)", cause, [w[3] for w in qw], "choice")
    sev, _ = run(udf.decide_score, [w[1] for w in qw], Q_SEV, SEV)
    report("qoe severity (score)", sev, [w[4] for w in qw], "level")
    act, _ = run(udf.decide_choice, [w[1] for w in fw], Q_ACT, ACT)
    report("ddos action (choice)", act, [w[3] for w in fw], "choice")
    kind, _ = run(udf.decide_choice, [w[1] for w in fw], Q_KIND, KIND)
    report("ddos kind (choice)", kind, [w[4] for w in fw], "choice")
    rf = sum(1 for w in fw if w[2] != (w[0] not in ("none", "flash_crowd")))
    print("rule-pack mistakes on ddos windows: %d of %d (flash_crowd alarms + port_scan misses)" % (rf, len(fw)))

    if a.dump:
        with open(a.dump, "w") as f:
            for name, rows, ws, key in (("qoe_cause", cause, qw, 3), ("qoe_sev", sev, qw, 4), ("ddos_act", act, fw, 3), ("ddos_kind", kind, fw, 4)):
                for r, w in zip(rows, ws):
                    r.update({"question": name, "truth": w[0], "expected": w[key], "rule_flag": w[2]})
                    f.write(json.dumps(r, separators=(",", ":")) + "\n")
        print("wrote", a.dump)


if __name__ == "__main__":
    main()
