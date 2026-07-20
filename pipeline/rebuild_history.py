#!/usr/bin/env python3
"""
rebuild_history.py — FULL keyless rebuild of the CPI master (data/cpi_series.json).

Grabs EVERYTHING (all series, full history) so BLS's seasonal-adjustment revisions are
fully absorbed — never a one-month patch. Keyless: FRED CSV for everything FRED carries;
the BLS 'Current' flat file for the handful FRED doesn't (detailed sub-items, which need
only recent history for Figures 4-6). Guarded, self-diagnosing.

Runs on GitHub Actions (open internet). Cadence: weekly + after the Feb seasonal
revision + manual. This is the deliberate "heavy" job — NOT run on every push.
"""
import json, datetime
import common as C

def main():
    diags, out = {}, {}
    bls_gap_sa, bls_gap_nsa = {}, {}     # name -> BLS CU id still needed after FRED

    # Pass 1 — FRED (full history, one call per series)
    for s in C.SERIES:
        name = s["name"]; sa = nsa = []; ssrc = nsrc = ""
        if s["fred"]:
            try:
                sa = C.fred_fetch(s["fred"]["sa"]);  ssrc = f"FRED:{s['fred']['sa']}" if sa else ""
            except Exception as e:
                diags[name] = f"FRED SA error: {e}"
            try:
                nsa = C.fred_fetch(s["fred"]["nsa"]); nsrc = f"FRED:{s['fred']['nsa']}" if nsa else ""
            except Exception as e:
                diags[name] = (diags.get(name, "") + f" | FRED NSA error: {e}").strip(" |")
        if not sa:  bls_gap_sa[name]  = s["bls"]["sa"]
        if not nsa: bls_gap_nsa[name] = s["bls"]["nsa"]
        out[name] = {"sa": sa, "nsa": nsa, "sa_source": ssrc, "nsa_source": nsrc, "role": s["role"]}

    # Pass 2 — BLS flat file fills the gaps (one download, parse for all needed ids)
    if bls_gap_sa or bls_gap_nsa:
        wanted = set(bls_gap_sa.values()) | set(bls_gap_nsa.values())
        try:
            parsed = C.parse_bls_flatfile(C.bls_current_text(), wanted)
        except Exception as e:
            parsed = {}
            diags["_bls"] = f"BLS flat-file download failed: {e}"
        for name, bid in bls_gap_sa.items():
            if parsed.get(bid): out[name]["sa"] = parsed[bid]; out[name]["sa_source"] = f"BLS:{bid}"
            else: diags[name] = (diags.get(name, "") + f" | no SA (BLS {bid} empty)").strip(" |")
        for name, bid in bls_gap_nsa.items():
            if parsed.get(bid): out[name]["nsa"] = parsed[bid]; out[name]["nsa_source"] = f"BLS:{bid}"
            else: diags[name] = (diags.get(name, "") + f" | no NSA (BLS {bid} empty)").strip(" |")

    master = {"meta": {"generated_utc": datetime.datetime.utcnow().isoformat()+"Z",
                       "mode": "rebuild_history", "contact": C.CONTACT, "diagnostics": diags},
              "series": out}
    reason = C.save_guarded(master)   # aborts (no write) if degraded
    empty = [n for n, v in out.items() if not v["sa"]]
    print(f"rebuild_history OK — {reason}")
    if empty: print("EMPTY series (need item-code fix):", empty)
    if diags: print("diagnostics:", json.dumps(diags))

if __name__ == "__main__":
    main()
