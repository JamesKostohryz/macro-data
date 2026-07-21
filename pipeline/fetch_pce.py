#!/usr/bin/env python3
"""
fetch_pce.py — build data/pce_series.json from BEA's monthly NIPA bulk file.

Keyless. One source file covers full history AND the release month, so this single
script serves both the scheduled rebuild and the release-morning run.

Usage:
    python3 fetch_pce.py [out_path] [--local NipaDataM.txt]
"""
import sys, os, json, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pce_common as C

def build(nipa_text, last_modified=None, source="BEA:NipaDataM.txt"):
    wanted = ({s["index"] for s in C.SERIES} | {s["contrib"] for s in C.SERIES}
              | {s["nominal"] for s in C.SERIES})
    parsed = C.parse_nipa(nipa_text, wanted)

    series, diagnostics = {}, {}
    for s in C.SERIES:
        idx  = parsed.get(s["index"]) or []
        con  = parsed.get(s["contrib"]) or []
        nom  = parsed.get(s["nominal"]) or []
        series[s["name"]] = {
            "sa": idx,
            "nsa": [],                      # BEA publishes no NSA monthly PCE price index
            "contrib": con,
            "nominal": nom,
            "sa_source": f"BEA:{s['index']} (T2.8.4 line {s['line']})",
            "contrib_source": f"BEA:{s['contrib']} (T2.8.8 line {s['line']})",
            "nominal_source": f"BEA:{s['nominal']} (T2.8.5 line {s['line']})",
            "role": s["role"], "line": s["line"], "parent": s["parent"],
        }
        bits = []
        if not idx: bits.append(f"EMPTY index {s['index']}")
        if not con: bits.append(f"EMPTY contrib {s['contrib']}")
        if not nom: bits.append(f"EMPTY nominal {s['nominal']}")
        diagnostics[s["name"]] = ("; ".join(bits) if bits
                                  else f"ok idx={len(idx)}m contrib={len(con)}m "
                                       f"[{idx[0]['date']}..{idx[-1]['date']}]")

    master = {"meta": {
        "generated_utc": datetime.datetime.now(datetime.timezone.utc)
                          .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "release": "PCE",
        "source": source,
        "source_last_modified": last_modified,
        "contact": C.CONTACT,
        "basis": "SA only; BEA publishes no NSA monthly PCE price index. YoY computed "
                 "on SA, BEA's published convention (confirmed with James 2026-07-20).",
        "weights": "PUBLISHED nominal expenditure shares, BEA Table 2.8.5 — no roll-"
                   "forward, no hardcoded relative-importance table.",
        "contributions": "PUBLISHED, BEA Table 2.8.8 — not computed as weight x MoM. "
                         "There is no weights layer in this pipeline.",
        "diagnostics": diagnostics,
    }, "series": series}
    return master

def main():
    args  = [a for a in sys.argv[1:] if not a.startswith("--")]
    out   = args[0] if args else C.OUT_PATH
    local = None
    if "--local" in sys.argv:
        local = sys.argv[sys.argv.index("--local") + 1]

    if local:
        text, lastmod = open(local).read(), None
        src = f"local:{local}"
    else:
        text, lastmod = C.http_get(C.BEA_DATA)
        src = C.BEA_DATA
    print(f"fetched {len(text):,} chars  last-modified={lastmod}")

    master = build(text, lastmod, src)
    latest = C.latest_month(master)
    ok, why = C.check_contribution_identity(master)
    print(f"latest month: {latest}")
    print(f"identity check: {'PASS' if ok else 'FAIL'} — {why}")
    for n, d in master["meta"]["diagnostics"].items():
        if d.startswith("EMPTY") or "EMPTY" in d:
            print(f"  !! {n}: {d}")
    print(C.save_guarded(master, out))

if __name__ == "__main__":
    main()
