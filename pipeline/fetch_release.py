#!/usr/bin/env python3
"""
fetch_release.py — release-morning patch of the newest print (keyless, timely).

On CPI release morning FRED lags a day or two, but the BLS bulk flat file is fresh.
This pulls the BLS 'Current' flat file and patches the last ~3 months of every series
into the EXISTING master (data/cpi_series.json). Light and guarded. The full history +
revisions come later from rebuild_history.py (FRED).

Requires an existing deep master (it patches, it does not create). Guard rejects a write
if the master isn't deep — so a stray run can never manufacture a shallow file.
"""
import datetime
import common as C

TAIL = 3   # months of tail to refresh from the flat file each release

def main():
    master = C.load_master()
    if not master.get("series", {}).get(C.REQUIRED, {}).get("sa"):
        raise SystemExit("No deep master present — run rebuild_history.py first.")

    ids = set()
    for s in C.SERIES:
        ids.add(s["bls"]["sa"]); ids.add(s["bls"]["nsa"])
    parsed = C.parse_bls_flatfile(C.bls_current_text(), ids)

    patched, missing = 0, []
    for s in C.SERIES:
        name = s["name"]
        cur = master["series"].setdefault(name, {"sa": [], "nsa": [], "role": s["role"]})
        for kind, bid in (("sa", s["bls"]["sa"]), ("nsa", s["bls"]["nsa"])):
            new = parsed.get(bid, [])
            if not new:
                missing.append(f"{name}:{kind}"); continue
            cur[kind] = C.merge_points(cur.get(kind, []), new[-TAIL:])
            cur[kind + "_source"] = f"BLS:{bid} (release patch)"
            patched += 1

    latest = master["series"][C.REQUIRED]["sa"][-1]["date"]
    master.setdefault("meta", {})
    master["meta"].update({"release_patch_utc": datetime.datetime.utcnow().isoformat()+"Z",
                           "release_patched_kinds": patched, "latest_month": latest})
    if missing: master["meta"]["release_missing"] = missing
    reason = C.save_guarded(master)
    print(f"fetch_release OK — {reason}; patched {patched} series-kinds; latest month {latest}")
    if missing: print("missing from flat file this run:", missing)

if __name__ == "__main__":
    main()
