#!/usr/bin/env python3
"""
rebuild_history.py — FULL keyless rebuild of the CPI master (data/cpi_series.json).

Grabs EVERYTHING (all series, full history) so BLS's seasonal-adjustment revisions are
fully absorbed — never a one-month patch. Guarded, self-diagnosing.

SOURCE: BLS deep-history flat files only. The original v2 design pulled history from
FRED's fredgraph.csv, but that endpoint times out from GitHub Actions runners (verified
2026-07-20: 6/6 consecutive timeouts at 25s, identical for browser and bot User-Agents,
while the same URL serves fine from other networks — i.e. IP-level, not header-fixable).
BLS carries every series this pipeline needs, at full depth, with no API key. Staying
keyless is the whole point of v2, so BLS replaced FRED rather than a key replacing it.

Runs on GitHub Actions (open internet). Cadence: weekly + after the Feb seasonal
revision + on any pipeline/** change + manual.
"""
import json, datetime
import common as C


def main():
    diags = {}

    # One pass: every id we need, merged across the deep-history flat files.
    wanted = set()
    for s in C.SERIES:
        wanted.add(s["bls"]["sa"])
        wanted.add(s["bls"]["nsa"])
    parsed, file_errs = C.bls_fetch_history(wanted)
    if file_errs:
        diags["_files"] = file_errs

    out = {}
    for s in C.SERIES:
        name = s["name"]
        sid_sa, sid_nsa = s["bls"]["sa"], s["bls"]["nsa"]
        sa, nsa = parsed.get(sid_sa, []), parsed.get(sid_nsa, [])
        out[name] = {
            "sa": sa,
            "nsa": nsa,
            "sa_source": f"BLS:{sid_sa}" if sa else "",
            "nsa_source": f"BLS:{sid_nsa}" if nsa else "",
            "role": s["role"],
        }
        notes = []
        if not sa:
            notes.append(f"no SA (BLS {sid_sa} not found in history files)")
        if not nsa:
            notes.append(f"no NSA (BLS {sid_nsa} not found in history files)")
        if notes:
            diags[name] = " | ".join(notes)

    master = {"meta": {"generated_utc": datetime.datetime.now(datetime.timezone.utc)
                                        .replace(tzinfo=None).isoformat() + "Z",
                       "mode": "rebuild_history",
                       "source": "BLS flat files (keyless)",
                       "contact": C.CONTACT,
                       "files": C.BLS_HISTORY_FILES,
                       "diagnostics": diags},
              "series": out}

    reason = C.save_guarded(master)   # aborts (no write) if degraded
    empty = [n for n, v in out.items() if not v["sa"]]
    print(f"rebuild_history OK — {reason}")
    print(f"series: {len(out)} | All Items months: {len(out[C.REQUIRED]['sa'])}")
    if empty:
        print("EMPTY series (need item-code fix):", empty)
    if diags:
        print("diagnostics:", json.dumps(diags))


if __name__ == "__main__":
    main()
