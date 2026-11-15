#!/usr/bin/env python3
"""Markdown table (median per workload) from bench.sh TSV: native vs cairn, with ratio.
usage: report.py results.tsv"""
import sys, statistics, collections
res = collections.defaultdict(list); unit = {}; order = []
for line in open(sys.argv[1]):
    p = line.rstrip("\n").split("\t")
    if len(p) < 6: continue
    vm, cfg, w, run, val, u = p[:6]
    res[(cfg, w)].append(float(val)); unit[w] = u
    if w not in order: order.append(w)

cfgs = [c for c in ["native", "cairn", "cairn-uring"] if any(k[0] == c for k in res)]
print("| workload | unit | " + " | ".join(cfgs) + " | " + " | ".join(f"{c}/native" for c in cfgs[1:]) + " |")
print("|---|---|" + "---:|" * (2 * len(cfgs) - 1))
def f(v): return "-" if v is None else (f"{v:.3f}" if v < 100 else f"{v:.1f}")
for w in order:
    m = {c: (statistics.median(res[(c, w)]) if res.get((c, w)) else None) for c in cfgs}
    ratios = [f"{m[c]/m['native']:.2f}x" if m[c] and m['native'] else "-" for c in cfgs[1:]]
    print(f"| {w} | {unit[w]} | " + " | ".join(f(m[c]) for c in cfgs) + " | " + " | ".join(ratios) + " |")
