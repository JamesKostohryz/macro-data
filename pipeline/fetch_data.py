#!/usr/bin/env python3
"""
fetch_data.py — pull CPI history for the economic-release-report engine.

Runs on GitHub Actions (which has open internet), NOT in the Cowork sandbox.
It fetches seasonally-adjusted (SA) and not-seasonally-adjusted (NSA) monthly index
levels for the CPI aggregate tree plus detailed item strata, full history, from FRED
(primary, one call) with BLS overlaying the fresh recent months. Output is written to
data/cpi_series.json, which the report engine reads (via raw GitHub).

Auth (set as repo secrets, injected as env vars by the workflow):
    BLS_API_KEY   — from https://data.bls.gov/registrationEngine/
    FRED_API_KEY  — from https://fredaccount.stlouisfed.org/apikeys
"""
import os, json, time, urllib.request, urllib.error, datetime

BLS_URL  = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
FRED_OBS = "https://api.stlouisfed.org/fred/series/observations"
OUT_PATH = "data/cpi_series.json"
START_YEAR = 1947          # each series pulls from its actual FRED start; full available history

AGG_TREE = {
    "All Items":           {"bls_sa":"CUSR0000SA0",     "bls_nsa":"CUUR0000SA0",     "fred_sa":"CPIAUCSL",       "fred_nsa":"CPIAUCNS"},
    "Core":                {"bls_sa":"CUSR0000SA0L1E",  "bls_nsa":"CUUR0000SA0L1E",  "fred_sa":"CPILFESL",       "fred_nsa":"CPILFENS"},
    "Food":                {"bls_sa":"CUSR0000SAF1",    "bls_nsa":"CUUR0000SAF1",    "fred_sa":"CPIUFDSL",       "fred_nsa":"CPIUFDNS"},
    "Energy":              {"bls_sa":"CUSR0000SA0E",    "bls_nsa":"CUUR0000SA0E",    "fred_sa":"CPIENGSL",       "fred_nsa":"CPIENGNS"},
    "Core Goods":          {"bls_sa":"CUSR0000SACL1E",  "bls_nsa":"CUUR0000SACL1E",  "fred_sa":"CUSR0000SACL1E", "fred_nsa":"CUUR0000SACL1E"},
    "Core Services":       {"bls_sa":"CUSR0000SASLE",   "bls_nsa":"CUUR0000SASLE",   "fred_sa":"CUSR0000SASLE",  "fred_nsa":"CUUR0000SASLE"},
    "Housing Services":    {"bls_sa":"CUSR0000SAH1",    "bls_nsa":"CUUR0000SAH1",    "fred_sa":"CUSR0000SAH1",   "fred_nsa":"CUUR0000SAH1"},
    "Owners Equiv Rent":   {"bls_sa":"CUSR0000SEHC",    "bls_nsa":"CUUR0000SEHC",    "fred_sa":"CUSR0000SEHC",   "fred_nsa":"CUUR0000SEHC"},
    "Rent Primary Res":    {"bls_sa":"CUSR0000SEHA",    "bls_nsa":"CUUR0000SEHA",    "fred_sa":"CUSR0000SEHA",   "fred_nsa":"CUUR0000SEHA"},
}

def _ids(code): return {"bls_sa":f"CUSR0000{code}","bls_nsa":f"CUUR0000{code}",
                        "fred_sa":f"CUSR0000{code}","fred_nsa":f"CUUR0000{code}"}
DETAIL_ITEMS = {n:_ids(c) for n,c in {
    "Owner's equivalent rent":"SEHC", "Rent of primary residence":"SEHA",
    "Lodging away from home":"SEHB", "Airline fares":"SETG01",
    "Motor vehicle insurance":"SETE", "Motor vehicle maintenance and repair":"SETD",
    "Physicians' services":"SEMC01", "Hospital services":"SEMD01",
    "Water, sewer and trash":"SEHG", "Tobacco and smoking products":"SEGA",
    "New vehicles":"SETA01", "Used cars and trucks":"SETA02", "Apparel":"SAA",
    "Medical care commodities":"SAM1", "Alcoholic beverages":"SAF116",
    "Gasoline (all types)":"SETB01", "Electricity":"SEHF01",
    "Utility (piped) gas service":"SEHF02", "Fuel oil":"SEHE01",
    "Food at home":"SAF11", "Food away from home":"SEFV",
}.items()}

def log(m): print(m, flush=True)

_STATE = {"bls_dead": False}  # once BLS returns an invalid-key/quota error, stop hammering it this run

def bls_fetch(series_ids, start, end, key):
    body = {"seriesid": series_ids, "startyear": str(start), "endyear": str(end)}
    if key: body["registrationkey"] = key
    req = urllib.request.Request(BLS_URL, data=json.dumps(body).encode(),
                                 headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    if d.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS: {d.get('status')} {d.get('message')}")
    out = {}
    for s in d["Results"]["series"]:
        rows = []
        for pt in s["data"]:
            if pt["period"].startswith("M") and pt["period"] != "M13":
                rows.append((f'{pt["year"]}-{pt["period"][1:]}', float(pt["value"])))
        out[s["seriesID"]] = sorted(rows)
    return out

def bls_series(series_id, key, start_year=START_YEAR):
    now = datetime.date.today().year
    acc = {}
    if _STATE["bls_dead"]:
        return []
    y = start_year
    while y <= now:
        end = min(y+19, now)
        for attempt in range(3):
            try:
                got = bls_fetch([series_id], y, end, key)
                for k, v in got.get(series_id, []): acc[k] = v
                break
            except Exception as e:
                msg = str(e)
                if "REQUEST_NOT_PROCESSED" in msg or "invalid" in msg.lower() or "threshold" in msg.lower():
                    if not _STATE["bls_dead"]:
                        log(f"    BLS unusable this run (key invalid or quota exhausted): {msg[:150]}")
                    _STATE["bls_dead"] = True
                    return [{"date": k, "value": acc[k]} for k in sorted(acc)]
                log(f"    BLS {series_id} {y}-{end} try{attempt+1}: {e}")
                time.sleep(2)
        y = end + 1
    return [{"date":k, "value":acc[k]} for k in sorted(acc)]

def fred_series(series_id, key):
    if not key: return []
    url = f"{FRED_OBS}?series_id={series_id}&api_key={key}&file_type=json&observation_start={START_YEAR}-01-01"
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.load(r)
    rows = []
    for o in d.get("observations", []):
        if o["value"] not in (".", ""):
            rows.append({"date": o["date"][:7], "value": float(o["value"])})
    return rows

def get_one(ids, kind, bls_key, fred_key):
    now = datetime.date.today().year
    fred_id, bls_id = ids.get(f"fred_{kind}"), ids.get(f"bls_{kind}")
    m, src = {}, []
    if fred_id and fred_key:
        try:
            for r in fred_series(fred_id, fred_key): m[r["date"]] = r["value"]
            if m: src.append(f"FRED:{fred_id}")
        except Exception as e:
            detail = ""
            try:
                if isinstance(e, urllib.error.HTTPError):
                    detail = " | FRED says: " + e.read().decode("utf-8", "replace")[:200].replace(chr(10), " ")
            except Exception:
                pass
            log(f"    FRED failed for {fred_id}: {e}{detail}")
    if bls_id:
        try:
            start = (now - 2) if m else START_YEAR
            got = bls_series(bls_id, bls_key, start_year=start)
            for r in got: m[r["date"]] = r["value"]
            if got: src.append(f"BLS:{bls_id}")
        except Exception as e:
            log(f"    BLS failed for {bls_id}: {e}")
    rows = [{"date":k, "value":m[k]} for k in sorted(m)]
    return rows, "+".join(src) or "NONE"

def main():
    bls_key  = os.environ.get("BLS_API_KEY", "").strip()
    fred_key = os.environ.get("FRED_API_KEY", "").strip()
    if not bls_key:  log("WARN: BLS_API_KEY not set — will rely on FRED.")
    if not fred_key: log("WARN: FRED_API_KEY not set — no fallback if BLS fails.")

    out = {"meta": {"generated_utc": datetime.datetime.utcnow().isoformat()+"Z",
                    "start_year": START_YEAR, "release": "CPI"},
           "series": {}}
    allmap = {**AGG_TREE, **DETAIL_ITEMS}
    for name, ids in allmap.items():
        log(f"[{name}]")
        sa, sa_src   = get_one(ids, "sa",  bls_key, fred_key)
        nsa, nsa_src = get_one(ids, "nsa", bls_key, fred_key)
        out["series"][name] = {"sa": sa, "nsa": nsa, "sa_source": sa_src, "nsa_source": nsa_src,
                               "ids": ids}
        lt = sa[-1]["date"] if sa else "—"
        log(f"    SA {len(sa):>4} pts to {lt} ({sa_src})   NSA {len(nsa):>4} pts ({nsa_src})")

    n = sum(1 for v in out["series"].values() if v["sa"])
    # Safety valve: never overwrite good data with a degraded/empty fetch (e.g. a
    # transient BLS/FRED outage). Require the headline series plus a healthy count.
    MIN_OK = 20
    if n < MIN_OK or not out["series"].get("All Items", {}).get("sa"):
        log(f"\nABORT: only {n}/{len(allmap)} series resolved (need >= {MIN_OK} and 'All Items' present).")
        log("Refusing to overwrite data/cpi_series.json — leaving existing data intact.")
        raise SystemExit(1)
    os.makedirs("data", exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    log(f"\nWrote {OUT_PATH}: {n}/{len(allmap)} series with SA history.")

if __name__ == "__main__":
    main()
