#!/usr/bin/env python3
"""
test_pipeline.py — offline proof of the pure logic (no network). Run: python test_pipeline.py
Feeds realistic samples of the ACTUAL BLS flat-file format through the
parsers, merge, and anti-clobber guard, and asserts correct behavior.
"""
import json
import common as C

def check(name, cond):
    print(("PASS" if cond else "FAIL") + "  " + name)
    assert cond, name

# --- BLS flat file (tab-delimited, space-padded series_id, M13 annual must skip) ---
BLS_SAMPLE = (
    "series_id                     \tyear\tperiod\t       value\tfootnote_codes\n"
    "CUSR0000SA0                   \t2026\tM05\t         320.100\t\n"
    "CUSR0000SA0                   \t2026\tM06\t         320.900\t\n"
    "CUSR0000SA0                   \t2026\tM13\t         319.000\t\n"   # annual avg -> skip
    "CUSR0000SETE                  \t2026\tM05\t         700.000\t\n"
    "CUSR0000SETE                  \t2026\tM06\t         705.000\t\n"
    "CUUR0000SA0                   \t2026\tM06\t         321.500\t\n"   # not requested -> ignore
)
parsed = C.parse_bls_flatfile(BLS_SAMPLE, wanted_ids={"CUSR0000SA0", "CUSR0000SETE"})
check("BLS: only requested ids returned", set(parsed) == {"CUSR0000SA0", "CUSR0000SETE"})
check("BLS: M13 annual average skipped", len(parsed["CUSR0000SA0"]) == 2)
check("BLS: padded series_id stripped & dated", parsed["CUSR0000SA0"][0]["date"] == "2026-05")
check("BLS: value parsed", parsed["CUSR0000SETE"][-1]["value"] == 705.0)

# --- merge: incoming wins on overlap, union, sorted --------------------------------
existing = [{"date": "2026-05", "value": 1.0}, {"date": "2026-06", "value": 2.0}]
incoming = [{"date": "2026-06", "value": 9.0}, {"date": "2026-07", "value": 3.0}]
merged = C.merge_points(existing, incoming)
check("merge: union of dates", [p["date"] for p in merged] == ["2026-05", "2026-06", "2026-07"])
check("merge: incoming wins overlap", merged[1]["value"] == 9.0)

# --- guard: passes deep, rejects shallow & sparse ----------------------------------
deep = {"series": {"All Items": {"sa": [{"date": f"19{y:02d}-01", "value": 1.0} for y in range(0, 100)]
                                 + [{"date": f"20{y:02d}-01", "value": 1.0} for y in range(0, 26)] * 5}}}
# pad All Items to >= MIN_HISTORY and add >= MIN_OK series
deep["series"]["All Items"]["sa"] = [{"date": str(1947+i//12)+f"-{i%12+1:02d}", "value": 1.0} for i in range(C.MIN_HISTORY+5)]
for i in range(C.MIN_OK):
    deep["series"][f"s{i}"] = {"sa": [{"date": "2026-06", "value": 1.0}]}
ok, why = C.guard(deep); check(f"guard: passes a deep/full master ({why})", ok)

shallow = {"series": {"All Items": {"sa": [{"date": "2026-06", "value": 1.0}]}}}
for i in range(C.MIN_OK):
    shallow["series"][f"s{i}"] = {"sa": [{"date": "2026-06", "value": 1.0}]}
ok, why = C.guard(shallow); check(f"guard: REJECTS shallow All Items ({why})", not ok)

sparse = {"series": {"All Items": {"sa": [{"date": str(1947+i//12)+f"-{i%12+1:02d}", "value": 1.0} for i in range(600)]}}}
ok, why = C.guard(sparse); check(f"guard: REJECTS too-few-series ({why})", not ok)

# --- multi-file history merge: same series in several files must NOT duplicate --------
# All Items really does appear in AllItems, Summaries and CommoditiesServicesSpecial;
# concatenating instead of merging would triple every month. Also proves one bad file
# is survivable and gets recorded rather than swallowed.
_FILES = {
    "f_a": ("CUSR0000SA0\t2026\tM05\t320.100\t\n"
            "CUSR0000SA0\t2026\tM06\t320.900\t\n"),
    "f_b": ("CUSR0000SA0\t2026\tM06\t320.900\t\n"     # duplicate month, same value
            "CUSR0000SA0\t2026\tM07\t321.400\t\n"),   # and one genuinely new month
    "f_bad": None,                                     # simulates a download failure
}
_orig_http_get = C.http_get
def _fake_http_get(url, timeout=90, ua=None):
    body = _FILES[url.rsplit("/", 1)[-1]]
    if body is None:
        raise OSError("simulated download failure")
    return body
C.http_get = _fake_http_get
try:
    hist, errs = C.bls_fetch_history({"CUSR0000SA0"}, files=["f_a", "f_b", "f_bad"])
finally:
    C.http_get = _orig_http_get

pts = hist["CUSR0000SA0"]
check("history: files merged, duplicate month not double-counted", len(pts) == 3)
check("history: dates unique and sorted",
      [p["date"] for p in pts] == ["2026-05", "2026-06", "2026-07"])
check("history: new month from the later file is picked up", pts[-1]["value"] == 321.4)
check("history: a failed file is survivable, not fatal", "CUSR0000SA0" in hist)
check("history: the failed file is reported in errs", "f_bad" in errs)

# --- every configured series must have BOTH ids resolvable to a name (no typos) -------
_ids = {}
for _s in C.SERIES:
    _ids[_s["bls"]["sa"]] = _s["name"]; _ids[_s["bls"]["nsa"]] = _s["name"]
check(f"config: {len(C.SERIES)} series -> {len(_ids)} distinct BLS ids, none blank",
      all(k and k.startswith("CU") for k in _ids))
check("config: required series present in SERIES", C.REQUIRED in [s["name"] for s in C.SERIES])

# --- regression guard: never accept a fetch that lost ground vs the last commit -------
def _master(n_series, req_months, extra_len=None):
    m = {"series": {C.REQUIRED: {"sa": [{"date": str(1947 + i // 12) + f"-{i%12+1:02d}", "value": 1.0}
                                        for i in range(req_months)]}}}
    for i in range(n_series):
        m["series"][f"s{i}"] = {"sa": [{"date": "2026-06", "value": 1.0}] * (extra_len or 1)}
    return m

_prior = _master(C.MIN_OK + 2, 953)
ok, why = C.guard(_master(C.MIN_OK + 2, 953), _prior)
check("regression: identical-to-prior passes", ok)

ok, why = C.guard(_master(C.MIN_OK - 1, 953), _prior)          # lost several series
check(f"regression: REJECTS dropped series ({why})", not ok)

ok, why = C.guard(_master(C.MIN_OK + 2, 620), _prior)          # 953 -> 620 truncation
check(f"regression: REJECTS truncated All Items ({why})", not ok)

ok, why = C.guard(_master(C.MIN_OK + 2, 953 - C.REGRESSION_TOL), _prior)
check("regression: tolerates a 1-month boundary wobble", ok)

ok, why = C.guard(_master(C.MIN_OK + 2, 954), _prior)          # grew by a month
check("regression: a longer history passes", ok)

# a series present before that comes back short must be caught even if counts are fine
_short = _master(C.MIN_OK + 2, 953)
_prior_long = _master(C.MIN_OK + 2, 953, extra_len=10)
ok, why = C.guard(_short, _prior_long)
check(f"regression: REJECTS per-series truncation ({why})", not ok)

# --- series_hash / idempotent write-skip ---------------------------------------------
import tempfile, os as _os
_a = _master(C.MIN_OK + 2, 953)
_b = json.loads(json.dumps(_a))                       # deep copy, identical series
_b["meta"] = {"generated_utc": "different-every-run"} # meta churn must NOT count
check("hash: ignores meta churn", C.series_hash(_a) == C.series_hash(_b))
_c = json.loads(json.dumps(_a)); _c["series"]["s0"]["sa"][0]["value"] = 2.0
check("hash: detects a real series change", C.series_hash(_a) != C.series_hash(_c))

_tmp = _os.path.join(tempfile.mkdtemp(), "cpi_series.json")
r1 = C.save_guarded(json.loads(json.dumps(_a)), _tmp)
_mtime1 = _os.path.getmtime(_tmp)
r2 = C.save_guarded(json.loads(json.dumps(_a)), _tmp)          # same series, second run
check(f"idempotence: second identical run skips the write ({r2[:24]}...)",
      r2.startswith("UNCHANGED") and _os.path.getmtime(_tmp) == _mtime1)
r3 = C.save_guarded(json.loads(json.dumps(_c)), _tmp)          # genuinely changed series
check("idempotence: a real change still writes", not r3.startswith("UNCHANGED"))

print("\nALL TESTS PASSED — parsing, merge, multi-file history, anti-clobber guard,\n"
      "regression floor, and idempotent write-skip are proven.")
