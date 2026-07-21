#!/usr/bin/env python3
"""Offline self-test for the PCE pipeline. No network. Run: python3 test_pce.py"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pce_common as C

FAIL = []
def check(name, cond, detail=""):
    print(f"{'PASS' if cond else 'FAIL'}  {name}{'' if cond else '  <- ' + detail}")
    if not cond: FAIL.append(name)

# --- parser: the two documented traps ------------------------------------------------
SAMPLE = ('%SeriesCode,Period,Value\n'
          'DPCERG,2026M04,"130.938"\n'
          'DPCERG,2026M05,"131.527"\n'
          'BIGONE,2026M05,"1,234,567.8"\n'      # thousands separators
          'DPCERG,2026Q2,"131.0"\n'             # quarterly row must be skipped
          'DPCERG,2026M05,\n')                  # unparseable value must be skipped
got = C.parse_nipa(SAMPLE, {"DPCERG", "BIGONE"})
check("parser strips quotes and thousands separators",
      got["BIGONE"] == [{"date": "2026-05", "value": 1234567.8}], str(got["BIGONE"]))
check("parser converts YYYYMmm -> YYYY-MM",
      [p["date"] for p in got["DPCERG"]] == ["2026-04", "2026-05"], str(got["DPCERG"]))
check("parser skips non-monthly periods", all(p["date"][4] == "-" for p in got["DPCERG"]))
try:
    C.parse_nipa("wrong,header,here\nA,2026M01,1\n", {"A"}); ok = False
except ValueError: ok = True
check("parser rejects an unexpected BEA header rather than silently returning nothing", ok)

# --- series map integrity ------------------------------------------------------------
check("31 series mapped", len(C.SERIES) == 31, str(len(C.SERIES)))
check("line numbers are 1..31 with no gaps",
      sorted(s["line"] for s in C.SERIES) == list(range(1, 32)))
check("every series has index, contrib and nominal codes",
      all(s["index"] and s["contrib"] and s["nominal"] for s in C.SERIES))
check("no duplicate engine-facing names", len({s["name"] for s in C.SERIES}) == 31)
check("super-core constant resolves to a mapped series", C.SUPER_CORE in C.BY_NAME)
parents = {s["parent"] for s in C.SERIES if s["parent"]}
check("every declared parent exists in the map", parents <= set(C.BY_NAME), str(parents - set(C.BY_NAME)))
check("NPISH nesting is final-consumption-as-parent (brief had it inverted)",
      C.BY_NAME["Gross output of nonprofit institutions"]["parent"]
      == "Final consumption expenditures of NPISH")
check("the 'Less:' receipts line carries sign = -1",
      C.BY_NAME["Less: Receipts from sales of goods and services by nonprofits"]["sign"] == -1)

# --- merge / hash --------------------------------------------------------------------
a = [{"date": "2026-04", "value": 1.0}, {"date": "2026-05", "value": 2.0}]
b = [{"date": "2026-05", "value": 9.0}, {"date": "2026-06", "value": 3.0}]
check("merge_points: incoming wins on overlap, union sorted",
      C.merge_points(a, b) == [{"date": "2026-04", "value": 1.0},
                               {"date": "2026-05", "value": 9.0},
                               {"date": "2026-06", "value": 3.0}])
m1 = {"meta": {"generated_utc": "A"}, "series": {"X": {"sa": a, "sa_source": "one"}}}
m2 = {"meta": {"generated_utc": "B"}, "series": {"X": {"sa": a, "sa_source": "two"}}}
check("series_hash ignores meta churn and provenance labels",
      C.series_hash(m1) == C.series_hash(m2))
m3 = {"meta": {}, "series": {"X": {"sa": b}}}
check("series_hash changes when the data changes", C.series_hash(m1) != C.series_hash(m3))

# --- guard ---------------------------------------------------------------------------
ok, why = C.guard({"meta": {}, "series": {}})
check("guard rejects an empty master", not ok, why)
shallow = {"meta": {}, "series": {"PCE": {"sa": a, "contrib": a}}}
check("guard rejects a shallow fetch", not C.guard(shallow)[0])

# --- live file, if present -----------------------------------------------------------
here = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(here, "..", "data", "pce_series.json")
if os.path.exists(path):
    d = C.load_master(path)
    ok, why = C.guard(d)
    check("committed pce_series.json passes the guard", ok, why)
    check("all 31 series carry index data",
          sum(1 for v in d["series"].values() if v.get("sa")) == 31)
    check("all 31 series carry published contributions",
          sum(1 for v in d["series"].values() if v.get("contrib")) == 31)
    check("all 31 series carry published nominal levels (weights)",
          sum(1 for v in d["series"].values() if v.get("nominal")) == 31)
    check("nsa is empty everywhere (BEA publishes no NSA monthly PCE price index)",
          all(v.get("nsa") == [] for v in d["series"].values()))
    months = [p["date"] for p in d["series"]["PCE"]["sa"]][1:]
    broken = [mo for mo in months if C.tree_identity_failures(d, mo)]
    check(f"contribution tree reconciles in all {len(months)} months",
          not broken, f"{len(broken)} failures, first: {broken[:3]}")
    # weight x MoM should reproduce the published contribution — an independent check
    # that the three BEA tables are mutually consistent and correctly aligned.
    idx = {n: {p["date"]: p["value"] for p in o["sa"]} for n, o in d["series"].items()}
    nom = {n: {p["date"]: p["value"] for p in o["nominal"]} for n, o in d["series"].items()}
    con = {n: {p["date"]: p["value"] for p in o["contrib"]} for n, o in d["series"].items()}
    mo, prev = months[-1], months[-2]
    worst, worst_n = 0.0, None
    for n in ("Goods", "Services", "Core PCE", C.SUPER_CORE, "Housing"):
        w = nom[n][mo] / nom["PCE"][mo]
        implied = 100 * w * (idx[n][mo] / idx[n][prev] - 1)
        e = abs(implied - con[n][mo])
        if e > worst: worst, worst_n = e, n
    check("published weight x published MoM reproduces published contribution (<=0.02pp)",
          worst <= 0.02, f"worst {worst_n} off by {worst:.4f}pp")
else:
    print("SKIP  live-file checks (data/pce_series.json not present)")

print(f"\n{'ALL TESTS PASSED' if not FAIL else str(len(FAIL)) + ' FAILURE(S): ' + ', '.join(FAIL)}")
sys.exit(1 if FAIL else 0)
