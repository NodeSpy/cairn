#!/usr/bin/env python3
"""Summarise e12.sh output: medians per (label, workload) and request mixes.
usage: e12-report.py <outdir> [labels,comma,separated]"""
import sys, glob, os, statistics, collections, re

d = sys.argv[1]
labels = sys.argv[2].split(",") if len(sys.argv) > 2 else None
res = collections.defaultdict(list)
for line in open(os.path.join(d, "results.tsv")):
    lab, w, run, val, unit = line.rstrip("\n").split("\t")
    res[(lab, w)].append(float(val))
labs = labels or sorted({k[0] for k in res})
ws = []
for (l, w) in res:
    if w not in ws:
        ws.append(w)
print("| workload | " + " | ".join(labs) + " |")
print("|---|" + "---:|" * len(labs))
for w in ws:
    row = []
    for l in labs:
        v = res.get((l, w))
        row.append(f"{statistics.median(v):.3f}" if v and statistics.median(v) < 100 else (f"{statistics.median(v):.1f}" if v else "-"))
    print(f"| {w} | " + " | ".join(row) + " |")

print()
mixes = collections.defaultdict(dict)  # (label, workload) -> op -> count (median over runs)
raw = collections.defaultdict(lambda: collections.defaultdict(list))
for f in glob.glob(os.path.join(d, "mix-*.tsv")):
    m = re.match(r"mix-(.+?)-(.+)-(\d+)\.tsv", os.path.basename(f))
    if not m:
        continue
    lab, w, run = m.groups()
    for line in open(f):
        if line.startswith("#"):
            continue
        p = line.rstrip("\n").split("\t")
        if p[0] == "TOTAL":
            raw[(lab, w)]["TOTAL"].append(int(p[1]))
        else:
            raw[(lab, w)][p[0]].append(int(p[1]))
for k, ops in raw.items():
    mixes[k] = {o: int(statistics.median(v)) for o, v in ops.items()}
wl = sorted({k[1] for k in mixes})
print("request mixes (median count per workload run)")
for l in labs:
    for w in wl:
        m = mixes.get((l, w))
        if not m:
            continue
        s = ", ".join(f"{o} {c}" for o, c in sorted(m.items(), key=lambda x: -x[1]) if o != "TOTAL")
        print(f"- {l} / {w}: total {m.get('TOTAL', 0)}: {s}")
