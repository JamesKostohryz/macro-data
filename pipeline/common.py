#!/usr/bin/env python3
"""
common.py — shared core for the macro-data CPI pipeline (v2, incremental + keyless).

Two runners import this:
  * rebuild_history.py — full keyless rebuild of the master (FRED CSV; BLS flat files
    for series FRED doesn't carry). Grabs EVERYTHING → absorbs revisions. Off-peak.
  * fetch_release.py   — release-morning: pull the BLS bulk flat file (timely, keyless),
    patch the newest month(s) into the master. Light.

Design principles (post-mortem of the v1 full-re-pull-every-run design):
  * KEYLESS sources only — no API keys, so crossed/expired/quota'd secrets can't take
    the pipeline down. FRED CSV (fredgraph.csv) and BLS bulk flat files.
  * INCREMENTAL routine; full rebuild is a separate deliberate job.
  * ANTI-CLOBBER guard — never overwrite the master with a degraded fetch.
  * SELF-DIAGNOSING — per-series status written into meta.diagnostics in the committed
    file, because the executor can read committed files but not Actions logs.
  * STABLE SCHEMA — the report engine reads data/cpi_series.json unchanged:
      {"meta":{...}, "series":{"<Name>":{"sa":[{"date":"YYYY-MM","value":x},...],
                                          "nsa":[...], "sa_source":str, "nsa_source":str}}}

Network calls run ONLY on GitHub Actions (open internet). The parse_* / merge / guard
functions are pure and unit-tested offline (see test_pipeline.py).
"""
import os, io, csv, json, time, urllib.request, datetime

OUT_PATH   = "data/cpi_series.json"
FRED_CSV   = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={id}"
BLS_CURRENT= "https://download.bls.gov/pub/time.series/cu/cu.data.0.Current"
CONTACT    = os.environ.get("CONTACT_EMAIL", "macro-data-bot@example.com")
UA         = f"macro-data-pipeline/2.0 (+contact: {CONTACT})"
MIN_OK      = 20       # anti-clobber: require at least this many series with SA data
MIN_HISTORY = 500      # required series must have at least this many SA months
REQUIRED    = "All Items"

# --- series map -------------------------------------------------------------------
# fred: FRED id or None (None -> fetch from BLS flat file). bls: {"sa","nsa"} CU ids.
# role: "aggregate" needs deep history (percentiles); "detail" feeds Figures 4-6.
def _cu(code): return {"sa": f"CUSR0000{code}", "nsa": f"CUUR0000{code}"}
def S(name, fred_sa, fred_nsa, code, role):
    return {"name":name, "fred":({"sa":fred_sa,"nsa":fred_nsa} if fred_sa else None),
            "bls":_cu(code), "role":role}

SERIES = [
    # aggregate tree (deep history; all on FRED)
    S("All Items",       "CPIAUCSL","CPIAUCNS","SA0","aggregate"),
    S("Core",            "CPILFESL","CPILFENS","SA0L1E","aggregate"),
    S("Food",            "CPIUFDSL","CPIUFDNS","SAF1","aggregate"),
    S("Energy",          "CPIENGSL","CPIENGNS","SA0E","aggregate"),
    S("Core Goods",      "CUSR0000SACL1E","CUUR0000SACL1E","SACL1E","aggregate"),
    S("Core Services",   "CUSR0000SASLE","CUUR0000SASLE","SASLE","aggregate"),
    S("Housing Services","CUSR0000SAH1","CUUR0000SAH1","SAH1","aggregate"),
    # detailed items (Figures 4/5/6). FRED where it carries them; else None -> BLS.
    S("Owner's equivalent rent","CUSR0000SEHC","CUUR0000SEHC","SEHC","detail"),
    S("Rent of primary residence","CUSR0000SEHA","CUUR0000SEHA","SEHA","detail"),
    # legacy aliases: v1 emitted these two names and the report engine may still read
    # them. Same underlying ids as the two entries above; kept so the rebuild (which
    # writes the series dict from scratch) cannot drop keys the engine depends on.
    S("Owners Equiv Rent","CUSR0000SEHC","CUUR0000SEHC","SEHC","detail"),
    S("Rent Primary Res","CUSR0000SEHA","CUUR0000SEHA","SEHA","detail"),
    S("Lodging away from home","CUSR0000SEHB","CUUR0000SEHB","SEHB","detail"),
    S("Airline fares","CUSR0000SETG01","CUUR0000SETG01","SETG01","detail"),
    S("Motor vehicle insurance",None,None,"SETE","detail"),          # not on FRED
    S("Motor vehicle maintenance and repair","CUSR0000SETD","CUUR0000SETD","SETD","detail"),
    S("Physicians' services",None,None,"SEMC01","detail"),           # not on FRED
    S("Hospital services",None,None,"SEMD01","detail"),              # exact sub-item: BLS only
    S("Water, sewer and trash","CUSR0000SEHG","CUUR0000SEHG","SEHG","detail"),
    S("Tobacco and smoking products","CUSR0000SEGA","CUUR0000SEGA","SEGA","detail"),
    S("New vehicles","CUSR0000SETA01","CUUR0000SETA01","SETA01","detail"),
    S("Used cars and trucks","CUSR0000SETA02","CUUR0000SETA02","SETA02","detail"),
    S("Apparel","CPIAPPSL","CPIAPPNS","SAA","detail"),               # FRED fix
    S("Medical care commodities","CUSR0000SAM1","CUUR0000SAM1","SAM1","detail"),
    S("Alcoholic beverages",None,None,"SAF116","detail"),            # not on FRED
    S("Gasoline (all types)","CUSR0000SETB01","CUUR0000SETB01","SETB01","detail"),
    S("Electricity","CUSR0000SEHF01","CUUR0000SEHF01","SEHF01","detail"),
    S("Utility (piped) gas service","CUSR0000SEHF02","CUUR0000SEHF02","SEHF02","detail"),
    S("Fuel oil",None,None,"SEHE01","detail"),                       # exact sub-item: BLS only
    S("Food at home","CUSR0000SAF11","CUUR0000SAF11","SAF11","detail"),
    S("Food away from home","CUSR0000SEFV","CUUR0000SEFV","SEFV","detail"),
]
BY_NAME = {s["name"]: s for s in SERIES}

# --- pure parsers (unit-tested offline) -------------------------------------------
def parse_fred_csv(text):
    """FRED fredgraph.csv -> [{'date':'YYYY-MM','value':float}] (monthly)."""
    rows = []
    rdr = csv.reader(io.StringIO(text))
    header = next(rdr, None)
    for r in rdr:
        if len(r) < 2: continue
        d, v = r[0].strip(), r[1].strip()
        if v in (".", ""): continue
        try: rows.append({"date": d[:7], "value": float(v)})
        except ValueError: continue
    return rows

def parse_bls_flatfile(text, wanted_ids):
    """BLS cu.data.* (tab-delimited: series_id year period value footnotes)
    -> {series_id: [{'date':'YYYY-MM','value':float}]} for wanted_ids only.
    Monthly periods are M01..M12 (M13 = annual average, skipped)."""
    want = set(wanted_ids); out = {sid: [] for sid in want}
    for line in text.splitlines():
        if not line or line.startswith("series_id"): continue
        parts = line.split("\t")
        if len(parts) < 4: parts = line.split()
        if len(parts) < 4: continue
        sid = parts[0].strip()
        if sid not in want: continue
        year, period, value = parts[1].strip(), parts[2].strip(), parts[3].strip()
        if not period.startswith("M") or period == "M13": continue
        try: out[sid].append({"date": f"{year}-{period[1:]}", "value": float(value)})
        except ValueError: continue
    for sid in out: out[sid].sort(key=lambda x: x["date"])
    return out

# --- merge / master helpers -------------------------------------------------------
def load_master(path=OUT_PATH):
    try:
        with open(path) as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"meta": {}, "series": {}}

def merge_points(existing, incoming):
    """Union by date; incoming (fresher/authoritative) wins on overlap. Returns sorted."""
    m = {p["date"]: p["value"] for p in (existing or [])}
    for p in (incoming or []): m[p["date"]] = p["value"]
    return [{"date": k, "value": m[k]} for k in sorted(m)]

def guard(master):
    """Anti-clobber: return (ok, reason). Never write over good data with a bad fetch.
    Also rejects a SHALLOW fetch (e.g., transient FRED failure that left only recent
    BLS data) so it can't clobber deep history the percentiles depend on."""
    series = master.get("series", {})
    req = series.get(REQUIRED, {})
    if not req.get("sa"):
        return False, f"required series '{REQUIRED}' missing/empty"
    if len(req["sa"]) < MIN_HISTORY:
        return False, f"'{REQUIRED}' has only {len(req['sa'])} SA months (need >= {MIN_HISTORY}) — shallow fetch"
    n_ok = sum(1 for v in series.values() if v.get("sa"))
    if n_ok < MIN_OK:
        return False, f"only {n_ok} series have SA data (need >= {MIN_OK})"
    return True, f"{n_ok} series OK, {REQUIRED}={len(req['sa'])} months"

def save_guarded(master, path=OUT_PATH):
    ok, reason = guard(master)
    master.setdefault("meta", {})["guard"] = reason
    if not ok:
        raise SystemExit(f"ABORT (anti-clobber): {reason} — refusing to overwrite {path}")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f: json.dump(master, f, separators=(",", ":"))
    return reason

# --- network (Actions only) -------------------------------------------------------
def http_get(url, timeout=90):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def fred_fetch(series_id):
    return parse_fred_csv(http_get(FRED_CSV.format(id=series_id)))

def bls_current_text():
    """Download the BLS 'Current' flat file once (recent years, all CU series)."""
    return http_get(BLS_CURRENT, timeout=180)
