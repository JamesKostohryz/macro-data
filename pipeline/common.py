#!/usr/bin/env python3
"""
common.py — shared core for the macro-data CPI pipeline (v2, incremental + keyless).

Two runners import this:
  * rebuild_history.py — full keyless rebuild of the master from the BLS deep-history
    flat files. Grabs EVERYTHING → absorbs revisions. Off-peak.
  * fetch_release.py   — release-morning: pull the BLS bulk flat file (timely, keyless),
    patch the newest month(s) into the master. Light.

Design principles (post-mortem of the v1 full-re-pull-every-run design):
  * KEYLESS sources only — no API keys, so crossed/expired/quota'd secrets can't take
    the pipeline down. BLS bulk flat files only (FRED's fredgraph.csv is IP-blocked
    from GitHub Actions runners, so it is not used at all).
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
import os, json, time, hashlib, urllib.request, datetime

OUT_PATH   = "data/cpi_series.json"
BLS_BASE   = "https://download.bls.gov/pub/time.series/cu/"
BLS_CURRENT= BLS_BASE + "cu.data.0.Current"

# Deep-history flat files. FRED's fredgraph.csv endpoint times out from GitHub Actions
# runners (IP-level; browser and bot User-Agents fail identically), so history comes from
# BLS instead. Verified: these nine files contain all 56 CU ids this pipeline needs, with
# All Items back to 1947-01. A series may appear in SEVERAL files — merge_points dedupes
# by date, so files must be merged, never concatenated.
BLS_HISTORY_FILES = [
    "cu.data.1.AllItems", "cu.data.2.Summaries", "cu.data.11.USFoodBeverage",
    "cu.data.12.USHousing", "cu.data.13.USApparel", "cu.data.14.USTransportation",
    "cu.data.15.USMedical", "cu.data.18.USOtherGoodsAndServices",
    "cu.data.20.USCommoditiesServicesSpecial",
]
# NOTE: `or`, not os.environ.get(key, default). A GitHub Actions step that references a
# secret which does not exist sets the env var to the EMPTY STRING rather than leaving it
# unset, so .get()'s default never fires. That shipped a UA of "(+contact: )" to BLS,
# which 403s it — nine instant rejections and an anti-clobber abort in half a second.
CONTACT    = os.environ.get("CONTACT_EMAIL") or "macro-data-bot@example.com"
# BLS 403s browser-like agents and REQUIRES an identifying bot UA carrying contact info.
# (A missing CONTACT_EMAIL secret yields an empty contact and an instant 403 — see below.)
UA_BLS     = f"macro-data-pipeline/2.0 (+contact: {CONTACT})"
UA         = UA_BLS            # sole User-Agent; BLS is the only source
MIN_HISTORY = 500      # required series must have at least this many SA months
REQUIRED    = "All Items"
# NOTE: MIN_OK and REGRESSION_TOL are defined AFTER the SERIES list below — MIN_OK is
# derived from len(SERIES) and would NameError if placed here.

# --- series map -------------------------------------------------------------------
# bls: {"sa","nsa"} CU ids, derived from the item code.
# role: "aggregate" needs deep history (percentiles); "detail" feeds Figures 4-6.
def _cu(code): return {"sa": f"CUSR0000{code}", "nsa": f"CUUR0000{code}"}
def S(name, code, role):
    return {"name":name, "bls":_cu(code), "role":role}

SERIES = [
    # aggregate tree (deep history)
    S("All Items","SA0","aggregate"),
    S("Core","SA0L1E","aggregate"),
    S("Food","SAF1","aggregate"),
    S("Energy","SA0E","aggregate"),
    S("Core Goods","SACL1E","aggregate"),
    S("Core Services","SASLE","aggregate"),
    S("Housing Services","SAH1","aggregate"),
    # detailed items (Figures 4/5/6).
    S("Owner's equivalent rent","SEHC","detail"),
    S("Rent of primary residence","SEHA","detail"),
    # legacy aliases: v1 emitted these two names and the report engine may still read
    # them. Same underlying ids as the two entries above; kept so the rebuild (which
    # writes the series dict from scratch) cannot drop keys the engine depends on.
    S("Owners Equiv Rent","SEHC","detail"),
    S("Rent Primary Res","SEHA","detail"),
    S("Lodging away from home","SEHB","detail"),
    S("Airline fares","SETG01","detail"),
    S("Motor vehicle insurance","SETE","detail"),
    S("Motor vehicle maintenance and repair","SETD","detail"),
    S("Physicians' services","SEMC01","detail"),
    S("Hospital services","SEMD01","detail"),
    S("Water, sewer and trash","SEHG","detail"),
    S("Tobacco and smoking products","SEGA","detail"),
    S("New vehicles","SETA01","detail"),
    S("Used cars and trucks","SETA02","detail"),
    S("Apparel","SAA","detail"),
    S("Medical care commodities","SAM1","detail"),
    S("Alcoholic beverages","SAF116","detail"),
    S("Gasoline (all types)","SETB01","detail"),
    S("Electricity","SEHF01","detail"),
    S("Utility (piped) gas service","SEHF02","detail"),
    S("Fuel oil","SEHE01","detail"),
    S("Food at home","SAF11","detail"),
    S("Food away from home","SEFV","detail"),
]
BY_NAME = {s["name"]: s for s in SERIES}

# Anti-clobber thresholds. MIN_OK is derived from the series map so it tracks additions
# automatically: at most 2 of the defined series may be transiently missing SA data.
# (Defined here, not with the other constants, because it needs len(SERIES).)
MIN_OK         = max(20, len(SERIES) - 2)
REGRESSION_TOL = 1     # months of slack on per-series history length vs the last commit

# --- pure parsers (unit-tested offline) -------------------------------------------
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

def series_hash(master):
    """SHA-256 over canonical JSON of the `series` object ONLY (sorted keys).
    Excludes meta.* on purpose — generated_utc/guard/diagnostics change every run
    and must NOT count as a data change."""
    blob = json.dumps(master.get("series", {}), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def guard(master, prior=None):
    """(ok, reason). Absolute floors + regression-vs-prior.
    prior = last committed master (load_master(path)); None skips the regression pass."""
    series = master.get("series", {})
    req = series.get(REQUIRED, {})
    # --- absolute floors: a shallow/degraded fetch can never pass ---
    if not req.get("sa"):
        return False, f"required series '{REQUIRED}' missing/empty"
    if len(req["sa"]) < MIN_HISTORY:
        return False, f"'{REQUIRED}' has only {len(req['sa'])} SA months (need >= {MIN_HISTORY}) — shallow fetch"
    n_ok = sum(1 for v in series.values() if v.get("sa"))
    if n_ok < MIN_OK:
        return False, f"only {n_ok} series have SA data (need >= {MIN_OK})"
    # --- regression floor: never accept a fetch that lost ground vs the last good commit ---
    if prior and prior.get("series"):
        p_series = prior["series"]
        p_ok = sum(1 for v in p_series.values() if v.get("sa"))
        if n_ok < p_ok:
            return False, f"series regression: {n_ok} SA series now vs {p_ok} in last commit"
        p_req = p_series.get(REQUIRED, {}).get("sa") or []
        if len(req["sa"]) < len(p_req) - REGRESSION_TOL:
            return False, (f"'{REQUIRED}' history shrank: {len(req['sa'])} vs {len(p_req)} months "
                           f"(tol {REGRESSION_TOL})")
        for name, pv in p_series.items():
            pn = len(pv.get("sa") or [])
            nn = len(series.get(name, {}).get("sa") or [])
            if pn and nn < pn - REGRESSION_TOL:
                return False, f"series '{name}' SA truncated: {nn} vs {pn} months (was in last commit)"
    return True, f"{n_ok} series OK, {REQUIRED}={len(req['sa'])} months"

def save_guarded(master, path=OUT_PATH):
    prior = load_master(path)                       # last committed good file (or empty)
    ok, reason = guard(master, prior)
    new_hash = series_hash(master)
    m = master.setdefault("meta", {})
    m["guard"] = reason
    m["series_hash"] = new_hash
    if not ok:
        raise SystemExit(f"ABORT (anti-clobber): {reason} — refusing to overwrite {path}")
    if prior.get("meta", {}).get("series_hash") == new_hash:
        # series identical to last commit -> do NOT rewrite: meta.generated_utc doesn't
        # churn, so `git diff --staged --quiet` is truly empty and commit-only-on-change holds.
        return f"UNCHANGED (series_hash match) — {reason}"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f: json.dump(master, f, separators=(",", ":"))
    return reason

# --- network (Actions only) -------------------------------------------------------
def http_get(url, timeout=90, ua=None):
    req = urllib.request.Request(url, headers={"User-Agent": ua or UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def bls_current_text():
    """Download the BLS 'Current' flat file once (recent years, all CU series)."""
    return http_get(BLS_CURRENT, timeout=180, ua=UA_BLS)

def bls_fetch_history(wanted_ids, files=None, timeout=240):
    """Download the BLS deep-history flat files and return
    ({series_id: [{date,value}] merged+deduped across files}, {filename: error}).

    A series can appear in more than one file (e.g. All Items is in AllItems,
    Summaries AND CommoditiesServicesSpecial), so results are merged by date rather
    than concatenated — otherwise every duplicated month lands in the output twice.
    A single file failing is not fatal: whatever was retrieved still merges, and the
    anti-clobber guard makes the final call on whether the result is good enough."""
    wanted = set(wanted_ids)
    out = {sid: [] for sid in wanted}
    errs = {}
    for fname in (files or BLS_HISTORY_FILES):
        try:
            text = http_get(BLS_BASE + fname, timeout=timeout, ua=UA_BLS)
        except Exception as e:
            errs[fname] = f"{type(e).__name__}: {e}"
            continue
        for sid, pts in parse_bls_flatfile(text, wanted).items():
            if pts:
                out[sid] = merge_points(out[sid], pts)
    return out, errs
