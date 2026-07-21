#!/usr/bin/env python3
"""
pce_common.py — shared core for the macro-data PCE pipeline.

Mirrors the CPI pipeline's contracts (stable schema, anti-clobber guard, self-diagnosing
meta, keyless) but is structurally simpler in two ways:

  1. ONE SOURCE FILE. BEA publishes the entire monthly NIPA universe — full history AND
     the newest month — in a single flat file that is rewritten at the release instant
     (verified last-modified: Thu, 25 Jun 2026 12:30:02 GMT = 8:30 ET on the May-PCE
     release day). So there is no rebuild-vs-patch split the way CPI needs: one fetch
     gets 1959 through the current month. rebuild and release-day runs are the same code.

  2. NO WEIGHTS LAYER. BEA PUBLISHES contributions to percent change (Table 2.8.8), so
     the engine reads them instead of computing weight x MoM. This removes the CPI
     build's single most error-prone component (relative-importance roll-forward + drift
     check). Contribution-to-acceleration is just contrib(t) - contrib(t-1).

  3. SA ONLY. There is no NSA monthly PCE price index — verified against BEA's full
     TablesRegister: the only NSA monthly tables are GDP-related (T801xx, T803, T804).
     So `nsa` is carried as an empty list for schema compatibility and YoY is computed
     on SA, which is BEA's published convention. Decision confirmed with James
     2026-07-20. See references/methodology-pce.md.

Schema (deliberately CPI-shaped so build_payload/build_report mechanics carry over):
    {"meta": {...},
     "series": {"<Name>": {"sa":    [{"date":"YYYY-MM","value":x}, ...],
                           "nsa":   [],                # PCE has no NSA twin
                           "contrib":[{"date":"YYYY-MM","value":x}, ...],  # pp, T2.8.8
                           "sa_source": str, "contrib_source": str,
                           "role": "aggregate"|"detail", "line": int}}}
"""
import os, json, csv, io, hashlib, urllib.request, datetime

OUT_PATH = "data/pce_series.json"

BEA_BASE   = "https://apps.bea.gov/national/Release/TXT/"
BEA_DATA   = BEA_BASE + "NipaDataM.txt"        # ~36 MB, all monthly NIPA series
BEA_SERIES = BEA_BASE + "SeriesRegister.txt"   # code -> label, TableId:LineNo
BEA_TABLES = BEA_BASE + "TablesRegister.txt"   # TableId -> title

# Weights are PUBLISHED, not invented: nominal PCE levels (Table 2.8.5, T20805) give
# each component's share of total nominal PCE — the direct analogue of BLS relative
# importance, and BEA's own expenditure basis. There is no roll-forward, no drift
# check and no hardcoded weights table anywhere in this pipeline.

CONTACT = os.environ.get("CONTACT_EMAIL") or "macro-data-bot@example.com"
UA      = f"macro-data-pipeline/2.0 (+contact: {CONTACT})"

MIN_HISTORY = 700          # DPCERG has 809 months from 1959-01; a shallow fetch can't pass
REQUIRED    = "PCE"

# --- series map ---------------------------------------------------------------------
# T20804 (price index levels) and T20808 (contributions to % change) are line-for-line
# identical, 31 lines each — verified against SeriesRegister on 2026-07-20. So each entry
# pairs the two codes by line number. `name` is the engine-facing key.
#
# role="aggregate": deep-history series that get percentile ranks and momentum (Fig 2).
# role="detail":    component lines that feed the attribution figures (Fig 3-6).
#
# Market-based PCE (lines 30/31) start 1987-02, not 1959-02. Percentiles are ranked
# against each series' OWN available history, so the shorter start is correct as-is —
# do NOT truncate the other 29 series to match.
# T20805 (nominal levels) shares the same 31-line structure as T20804/T20808, so the
# nominal code is derived from the index code by swapping the trailing "RG"->"RC"
# (DPCERG->DPCERC) and the IA-prefixed specials to LA. Verified all 31 resolve.
_NOMINAL_OVERRIDE = {"IA001176": "LA001176", "IA001260": "LA001260"}
def _nominal(idx_code):
    return _NOMINAL_OVERRIDE.get(idx_code, idx_code[:-2] + "RC"
                                 if idx_code.endswith("RG") else idx_code)

def S(line, name, idx, contrib, role, parent=None, sign=1):
    return {"line": line, "name": name, "index": idx, "contrib": contrib,
            "nominal": _nominal(idx), "role": role, "parent": parent, "sign": sign}

SERIES = [
    S( 1, "PCE",                         "DPCERG",   "DPCERGM",  "aggregate"),
    S( 2, "Goods",                       "DGDSRG",   "CE000885", "aggregate", "PCE"),
    S( 3, "Durable goods",               "DDURRG",   "CE000886", "aggregate", "Goods"),
    S( 4, "Motor vehicles and parts",    "DMOTRG",   "CE001058", "detail", "Durable goods"),
    S( 5, "Furnishings and durable household equipment",
                                         "DFDHRG",   "CE001044", "detail", "Durable goods"),
    S( 6, "Recreational goods and vehicles",
                                         "DREQRG",   "CE001075", "detail", "Durable goods"),
    S( 7, "Other durable goods",         "DODGRG",   "CE001068", "detail", "Durable goods"),
    S( 8, "Nondurable goods",            "DNDGRG",   "CE000887", "aggregate", "Goods"),
    S( 9, "Food and beverages purchased for off-premises consumption",
                                         "DFXARG",   "CE001047", "detail", "Nondurable goods"),
    S(10, "Clothing and footwear",       "DCLORG",   "CE001040", "detail", "Nondurable goods"),
    S(11, "Gasoline and other energy goods",
                                         "DGOERG",   "CE001049", "detail", "Nondurable goods"),
    S(12, "Other nondurable goods",      "DONGRG",   "CE001069", "detail", "Nondurable goods"),
    S(13, "Services",                    "DSERRG",   "CE000888", "aggregate", "PCE"),
    S(14, "Household consumption expenditures (services)",
                                         "DHCERG",   "CE001050", "aggregate", "Services"),
    S(15, "Housing and utilities",       "DHUTRG",   "CE001053", "detail", "Household consumption expenditures (services)"),
    S(16, "Health care",                 "DHLCRG",   "CE001048", "detail", "Household consumption expenditures (services)"),
    S(17, "Transportation services",     "DTRSRG",   "CE001078", "detail", "Household consumption expenditures (services)"),
    S(18, "Recreation services",         "DRCARG",   "CE001074", "detail", "Household consumption expenditures (services)"),
    S(19, "Food services and accommodations",
                                         "DFSARG",   "CE001045", "detail", "Household consumption expenditures (services)"),
    S(20, "Financial services and insurance",
                                         "DIFSRG",   "CE001055", "detail", "Household consumption expenditures (services)"),
    S(21, "Other services",              "DOTSRG",   "CE001070", "detail", "Household consumption expenditures (services)"),
    # NPISH nesting — CORRECTED against the data 2026-07-20. The scouting brief's
    # indentation had lines 22/23 inverted (it showed Gross output as the parent of
    # Final consumption expenditures). The published numbers say otherwise:
    #     HH services + NPISH_final -> Services      max err 0.010pp over 360 months
    #     HH services + NPISH_gross -> Services      max err 0.070pp, 82/360 outside tol
    #     Gross output - Receipts   -> NPISH_final   max err 0.010pp over 360 months
    # i.e. Services = HH consumption + FINAL consumption of NPISH, and Final = Gross
    # output LESS receipts. That is also the NIPA Table 2.8.5 definition. Getting this
    # backwards would have mis-attributed up to 0.07pp of the services contribution.
    S(22, "Final consumption expenditures of NPISH",
                                         "DNPIRG",   "CE001063", "aggregate", "Services"),
    S(23, "Gross output of nonprofit institutions",
                                         "DNPERG",   "CE001062", "detail", "Final consumption expenditures of NPISH"),
    # "Less:" line — SUBTRACTED from its parent. sign=-1 so tree walkers don't double-add.
    S(24, "Less: Receipts from sales of goods and services by nonprofits",
                                         "DNPSRG",   "CE001064", "detail",
                                         "Final consumption expenditures of NPISH", -1),
    S(25, "Core PCE",                    "DPCCRG",   "CE001071", "aggregate"),
    S(26, "PCE ex food, energy and housing",
                                         "IA001176", "CE001176", "aggregate"),
    S(27, "Energy goods and services",   "DNRGRG",   "CE001066", "aggregate"),
    # James's super-core (confirmed 2026-07-20): the structural analogue of CPI's
    # Core Services ex-Housing. Drives dashboard tile 3 and the prose regime classifier.
    S(28, "Super-core (services ex energy and housing)",
                                         "IA001260", "CE001260", "aggregate"),
    S(29, "Housing",                     "DHSGRG",   "CE001177", "aggregate"),
    S(30, "Market-based PCE",            "DPCMRG",   "CE001072", "aggregate"),
    S(31, "Market-based core PCE",       "DPCXRG",   "CE001073", "aggregate"),
]
BY_NAME = {s["name"]: s for s in SERIES}
SUPER_CORE = "Super-core (services ex energy and housing)"

# Anti-clobber. At most 2 of the 31 may be transiently missing.
MIN_OK         = max(26, len(SERIES) - 2)
REGRESSION_TOL = 1

# --- pure parsers (unit-testable offline) --------------------------------------------
def parse_nipa(text, wanted_codes):
    """NipaDataM.txt (CSV: %SeriesCode,Period,Value) -> {code: [{'date','value'}]}.

    Two traps, both real:
      * Values are QUOTED and carry thousands separators ("15,164.2") — strip both
        before float() or every large series silently drops out.
      * Periods are YYYYMmm, not YYYY-MM."""
    want = set(wanted_codes)
    out  = {c: [] for c in want}
    rd = csv.reader(io.StringIO(text))
    hdr = next(rd, None)
    if not hdr or hdr[0].strip().lstrip("%").lower() != "seriescode":
        raise ValueError(f"unexpected NipaDataM header: {hdr!r} — BEA changed the layout")
    for row in rd:
        if len(row) < 3: continue
        code = row[0].strip()
        if code not in want: continue
        period = row[1].strip()
        if len(period) != 7 or period[4] != "M": continue
        try:
            val = float(row[2].replace(",", "").replace('"', "").strip())
        except ValueError:
            continue
        out[code].append({"date": f"{period[:4]}-{period[5:]}", "value": val})
    for c in out: out[c].sort(key=lambda p: p["date"])
    return out

def latest_month(master):
    """Newest month present on the required series."""
    pts = (master.get("series", {}).get(REQUIRED, {}) or {}).get("sa") or []
    return pts[-1]["date"] if pts else None

# --- integrity check ------------------------------------------------------------------
def _contrib_map(master):
    return {n: {p["date"]: p["value"] for p in (o.get("contrib") or [])}
            for n, o in master.get("series", {}).items()}

# Every parent -> children relationship implied by SERIES, checked as a sum identity.
# Tolerance is 0.011pp for the top identity (children vs an independently computed MoM)
# and 0.021pp for internal node sums, where two independently 2dp-rounded children are
# compared against a 2dp-rounded parent (max representable error 0.015pp).
def tree_identity_failures(master, month=None, tol=0.021):
    D = _contrib_map(master)
    month = month or latest_month(master)
    kids = {}
    for s in SERIES:
        if s["parent"]: kids.setdefault(s["parent"], []).append(s)
    fails = []
    for parent, children in kids.items():
        # Skip the root: its stored line-1 value (DPCERGM) is rounded to 1dp by BEA, so
        # its own 2dp children can never reconcile to it. The root is validated against
        # the independently computed DPCERG MoM in check_contribution_identity instead.
        if parent == REQUIRED: continue
        if parent not in D or month not in D.get(parent, {}): continue
        if any(month not in D.get(c["name"], {}) for c in children): continue
        tot = sum(c["sign"] * D[c["name"]][month] for c in children)
        if abs(tot - D[parent][month]) > tol:
            fails.append(f"{parent}: children sum {tot:+.3f} vs {D[parent][month]:+.3f}pp")
    return fails

def check_contribution_identity(master, month=None, tol=0.011):
    """Regression test, same discipline as CPI's Figure 3: published child contributions
    must sum to the independently-computed headline MoM, and every internal node of the
    tree must sum to its parent.

    NOTE — do NOT use line 1 (DPCERGM) as the total. BEA stores it rounded to ONE
    decimal (0.4, 0.7) while the children carry two, so it disagrees with its own
    children by up to 0.05pp. Verified 2026-05: line 1 = 0.4, Goods+Services = 0.45,
    DPCERG MoM = 0.4498. The children are right; the total line is a display rounding.
    """
    ser = master.get("series", {})
    month = month or latest_month(master)
    D = _contrib_map(master)
    idx = {p["date"]: p["value"] for p in (ser.get(REQUIRED, {}).get("sa") or [])}
    months = sorted(idx)
    if month not in months or months.index(month) == 0:
        return False, f"cannot check {month}: missing index level or prior month"
    prev = months[months.index(month) - 1]
    mom_pct = 100.0 * (idx[month] / idx[prev] - 1.0)
    g, s_ = D.get("Goods", {}).get(month), D.get("Services", {}).get(month)
    if g is None or s_ is None:
        return False, f"{month}: missing Goods/Services contribution"
    diff = abs((g + s_) - mom_pct)
    msg = (f"{month}: Goods {g:+.2f} + Services {s_:+.2f} = {g+s_:+.2f}pp vs "
           f"DPCERG MoM {mom_pct:+.4f}% (diff {diff:.4f}pp, tol {tol})")
    if diff > tol:
        return False, msg
    sub = tree_identity_failures(master, month)
    if sub:
        return False, msg + " | subtree: " + "; ".join(sub)
    return True, msg + f"; all {len(SERIES)} tree nodes reconcile"

# --- master helpers (identical contracts to the CPI pipeline) --------------------------
def load_master(path=OUT_PATH):
    try:
        with open(path) as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"meta": {}, "series": {}}

def merge_points(existing, incoming):
    m = {p["date"]: p["value"] for p in (existing or [])}
    for p in (incoming or []): m[p["date"]] = p["value"]
    return [{"date": k, "value": m[k]} for k in sorted(m)]

_HASH_IGNORE = ("sa_source", "contrib_source", "nominal_source", "role", "line", "parent", "sign")

def series_hash(master):
    """SHA-256 over DATA only (sa/nsa/contrib arrays). Excludes meta.* and provenance —
    otherwise generated_utc churn makes 'commit only on change' never hold."""
    data = {n: {k: v for k, v in o.items() if k not in _HASH_IGNORE}
            for n, o in (master.get("series") or {}).items()}
    return hashlib.sha256(json.dumps(data, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()

def guard(master, prior=None):
    """(ok, reason). Absolute floors + regression-vs-last-good-commit."""
    ser = master.get("series", {})
    req = ser.get(REQUIRED, {})
    if not req.get("sa"):
        return False, f"required series '{REQUIRED}' missing/empty"
    if len(req["sa"]) < MIN_HISTORY:
        return False, (f"'{REQUIRED}' has only {len(req['sa'])} SA months "
                       f"(need >= {MIN_HISTORY}) — shallow fetch")
    n_sa      = sum(1 for v in ser.values() if v.get("sa"))
    n_contrib = sum(1 for v in ser.values() if v.get("contrib"))
    if n_sa < MIN_OK:
        return False, f"only {n_sa} series have index data (need >= {MIN_OK})"
    if n_contrib < MIN_OK:
        return False, f"only {n_contrib} series have contribution data (need >= {MIN_OK})"
    ok_id, why_id = check_contribution_identity(master)
    if not ok_id:
        return False, f"contribution identity failed — {why_id}"
    if prior and prior.get("series"):
        p = prior["series"]
        p_sa = sum(1 for v in p.values() if v.get("sa"))
        if n_sa < p_sa:
            return False, f"series regression: {n_sa} vs {p_sa} in last commit"
        p_req = p.get(REQUIRED, {}).get("sa") or []
        if len(req["sa"]) < len(p_req) - REGRESSION_TOL:
            return False, (f"'{REQUIRED}' history shrank: {len(req['sa'])} vs "
                           f"{len(p_req)} months (tol {REGRESSION_TOL})")
        for name, pv in p.items():
            pn = len(pv.get("sa") or [])
            nn = len(ser.get(name, {}).get("sa") or [])
            if pn and nn < pn - REGRESSION_TOL:
                return False, f"series '{name}' truncated: {nn} vs {pn} months"
    return True, f"{n_sa} index + {n_contrib} contribution series OK; {why_id}"

def save_guarded(master, path=OUT_PATH):
    prior = load_master(path)
    ok, reason = guard(master, prior)
    new_hash = series_hash(master)
    m = master.setdefault("meta", {})
    m["guard"] = reason
    m["series_hash"] = new_hash
    if not ok:
        raise SystemExit(f"ABORT (anti-clobber): {reason} — refusing to overwrite {path}")
    if prior.get("meta", {}).get("series_hash") == new_hash:
        # Data is byte-identical to the last commit, so do NOT rewrite: generated_utc
        # would churn and "commit only on change" would never hold.
        #
        # EXCEPTION — provenance. meta.source / source_last_modified describe WHERE the
        # committed numbers came from, and the executor can read committed files but not
        # Actions logs, so a stale label is actively misleading. (Bootstrap case: the
        # first pce_series.json was built from a local copy of the BEA file, then the
        # first Action run fetched from BEA, got identical data, skipped the write, and
        # left the file claiming "local:...". Data right, label wrong.) These fields are
        # stable between real releases, so refreshing them when they differ does not
        # reintroduce per-run churn.
        pm = prior.get("meta", {})
        drift = {k: m.get(k) for k in ("source", "source_last_modified")
                 if m.get(k) != pm.get(k)}
        if not drift:
            return f"UNCHANGED (series_hash match) — {reason}"
        merged = dict(prior)
        merged["meta"] = {**pm, **drift, "guard": reason, "series_hash": new_hash}
        with open(path, "w") as f: json.dump(merged, f, separators=(",", ":"))
        return (f"UNCHANGED data; refreshed provenance ({', '.join(drift)}) — {reason}")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f: json.dump(master, f, separators=(",", ":"))
    return reason

# --- network --------------------------------------------------------------------------
def http_get(url, timeout=240):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace"), r.headers.get("last-modified")
