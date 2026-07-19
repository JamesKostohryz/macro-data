#!/usr/bin/env python3
"""
fetch_data.py — pull CPI history for the economic-release-report engine.

Runs on GitHub Actions (which has open internet), NOT in the Cowork sandbox.
It fetches seasonally-adjusted (SA) and not-seasonally-adjusted (NSA) monthly index
levels for the CPI aggregate tree, full history, from the BLS Public Data API
(primary) with FRED as a fallback. Output is written to data/cpi_series.json, which
the report engine reads (via raw GitHub) to compute annualized rates, percentile
ranks, YoY, and the attribution tables.

Auth (set as repo secrets, injected as env vars by the workflow):
    BLS_API_KEY   — from https://data.bls.gov/registrationEngine/  (registration key)
    FRED_API_KEY  — from https://fredaccount.stlouisfed.org/apikeys

Design notes:
  * BLS v2 allows 50 series and 20 years per request, 500 requests/day. We window the
    history in <=20-year chunks and batch series.
  * Every series lists BOTH a BLS id and a FRED id. If BLS fails for a series (bad id,
    rate limit, outage), we fall back to FRED so a single bad id never sinks the run.
  * The script is defensive and idempotent: it logs per-series success/failure and
    writes whatever it got. The Action commits only if the data actually changed.

This v1 covers the AGGREGATE TREE (enough to lock the percentile window and drive
Figures 1–3 live, plus sparklines). The ~200 detailed item series behind Figures 4–6
are a documented follow-up (see DETAIL_ITEMS below — expand and re-run).
"""
import os, json, time, urllib.request, urllib.error, datetime

BLS_URL  = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
FRED_OBS = "https://api.stlouisfed.org/fred/series/observations"
OUT_PATH = "data/cpi_series.json"
START_YEAR = 1947          # each series pulls from its actual FRED start; full available history

# name -> ids. bls_sa/bls_nsa are BLS series IDs; fred_sa/fred_nsa are FRED series IDs.
# "constructed" nodes are derived by the engine from other nodes (no direct series).
AGG_TREE = {
    "All Items":           {"bls_sa":"CUSR0000SA0",     "bls_nsa":"CUUR0000SA0",     "fred_sa":"CPIAUCSL",       "fred_nsa":"CPIAUCNS"},
    "Core":                {"bls_sa":"CUSR0000SA0L1E",  "bls_nsa":"CUUR0000SA0L1E",  "fred_sa":"CPILFESL",       "fred_nsa":"CPILFENS"},
    "Food":                {"bls_sa":"CUSR0000SAF1",    "bls_nsa":"CUUR0000SAF1",    "fred_sa":"CPIUFDSL",       "fred_nsa":"CPIUFDNS"},
    "Energy":              {"bls_sa":"CUSR0000SA0E",    "bls_nsa":"CUUR0000SA0E",    "fred_sa":"CPIENGSL",       "fred_nsa":"CPIENGNS"},
    "Core Goods":          {"bls_sa":"CUSR0000SACL1E",  "bls_nsa":"CUUR0000SACL1E",  "fred_sa":"CUSR0000SACL1E", "fred_nsa":"CUUR0000SACL1E"},
    "Core Services":       {"bls_sa":"CUSR0000SASLE",   "bls_nsa":"CUUR0000SASLE",   "fred_sa":"CUSR0000SASLE",  "fred_nsa":"CUUR0000SASLE"},
    "Housing Services":    {"bls_sa":"CUSR0000SAH1",    "bls_nsa":"CUUR0000SAH1",    "fred_sa":"CUSR0000SAH1",   "fred_nsa":"CUUR0000SAH1"},
    # For super-core (core services ex-housing) the engine constructs the series from
    # Core Services and Housing Services using the relative-importance weights.
    "Owners Equiv Rent":   {"bls_sa":"CUSR0000SEHC",    "bls_nsa":"CUUR0000SEHC",    "fred_sa":"CUSR0000SEHC",   "fred_nsa":"CUUR0000SEHC"},
    "Rent Primary Res":    {"bls_sa":"CUSR0000SEHA",    "bls_nsa":"CUUR0000SEHA",    "fred_sa":"CUSR0000SEHA",   "fred_nsa":"CUUR0000SEHA"},
}

# Detailed item strata behind Figures 4-6 (top contributors / movers). Curated set of
# the major components (confident BLS item codes); expand as live runs confirm more.
# BLS item code -> series CUSR0000<code> (SA) / CUUR0000<code> (NSA). FRED mirrors these.
def _ids(code): return {"bls_sa":f"CUSR0000{code}","bls_nsa":f"CUUR0000{code}",
                        "fred_sa":f"CUSR0000{code}","fred_nsa":f"CUUR0000{code}"}
DETAIL_ITEMS = {n:_ids(c) for n,c in {
    # --- core services ---
    "Owner's equivalent rent":"SEHC", "Rent of primary residence":"SEHA",
    "Lodging away from home":"SEHB", "Airline fares":"SETG01",
    "Motor vehicle insurance":"SETE", "Motor vehicle maintenance and repair":"SETD",
    "Physicians' services":"SEMC01", "Hospital services":"SEMD01",
    "Water, sewer and trash":"SEHG", "Tobacco and smoking products":"SEGA",
    # --- core goods ---
    "New vehicles":"SETA01", "Used cars and trucks":"SETA02", "Apparel":"SAA",
    "Medical care commodities":"SAM1", "Alcoholic beverages":"SAF116",
    # --- non-core (food & energy) — for Top Movers ---
    "Gasoline (all types)":"SETB01", "Electricity":"SEHF01",
    "Utility (piped) gas service":"SEHF02", "Fuel oil":"SEHE01",
    "Food at home":"SAF11", "Food away from home":"SEFV",
}.items()}

def log(m): print(m, flush=True)

# --------------------------------------------------------------------------- BLS
def bls_fetch(series_ids, start, end, key):
    """Return {series_id: [(YYYY-MM, value), ...]} for a batch/window via BLS v2."""
    body = {"seriesid": series_ids, "startyear": str(start), "endyear": str(end)}
    if key: body["registrationkey"] = key
    req = urllib.request.Request(BLS_URL, data=json.dumps(body).encode(),
                                 headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
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
    y = start_year
    while y <= now:                      # <=20-year windows
        end = min(y+19, now)
        for attempt in range(3):
            try:
                got = bls_fetch([series_id], y, end, key)
                for k, v in got.get(series_id, []): acc[k] = v
                break
            except Exception as e:
                log(f"    BLS {series_id} {y}-{end} try{attempt+1}: {e}")
                time.sleep(2)
        y = end + 1
    return [{"date":k, "value":acc[k]} for k in sorted(acc)]

# --------------------------------------------------------------------------- FRED
def fred_series(series_id, key):
    if not key: return []
    url = f"{FRED_OBS}?series_id={series_id}&api_key={key}&file_type=json&observation_start={START_YEAR}-01-01"
    with urllib.request.urlopen(url, timeout=60) as r:
        d = json.load(r)
    rows = []
    for o in d.get("observations", []):
        if o["value"] not in (".", ""):
            rows.append({"date": o["date"][:7], "value": float(o["value"])})
    return rows

# --------------------------------------------------------------------------- driver
def get_one(ids, kind, bls_key, fred_key):
    """kind='sa'|'nsa'. FRED supplies the long history in one reliable call; BLS
    overlays the most recent ~2 years (authoritative + fresh for release day) and
    wins on any overlapping month. If FRED is unavailable, BLS does the full history
    as a fallback. Either source is allowed to fail without sinking the series."""
    now = datetime.date.today().year
    fred_id, bls_id = ids.get(f"fred_{kind}"), ids.get(f"bls_{kind}")
    m, src = {}, []
    if fred_id and fred_key:
        try:
            for r in fred_series(fred_id, fred_key): m[r["date"]] = r["value"]
            if m: src.append(f"FRED:{fred_id}")
        except Exception as e:
            log(f"    FRED failed for {fred_id}: {e}")
    if bls_id:
        try:
            start = (now - 2) if m else START_YEAR      # fresh tail if FRED gave history; else full
            got = bls_series(bls_id, bls_key, start_year=start)
            for r in got: m[r["date"]] = r["value"]     # BLS wins on overlap
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

    os.makedirs("data", exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    n = sum(1 for v in out["series"].values() if v["sa"])
    log(f"\nWrote {OUT_PATH}: {n}/{len(allmap)} series with SA history.")

if __name__ == "__main__":
    main()
