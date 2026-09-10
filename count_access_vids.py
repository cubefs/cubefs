#!/usr/bin/env python3
# count unique vids per cluster per 10-minute window from access audit logs

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

WINDOW = 600
VID_RE = re.compile(r'"vid"\s*:\s*(\d+)')
CID_RE = re.compile(r'"cluster_id"\s*:\s*(\d+)')
PUTAT_RE = re.compile(
    r"clusterid=(\d+).*?volumeid=(\d+)|volumeid=(\d+).*?clusterid=(\d+)"
)


def unix_sec_from_start(st):
    # access HTTP audit StartTime = UnixNano / 100  (~1.8e16 in 2026)
    st = int(st)
    if st > 10**17:
        return st // 10**9
    if st > 10**15:
        return st // 10**7
    if st > 10**14:
        return st // 10**6
    if st > 10**12:
        return st // 10**4
    if st > 10**11:
        return st // 10**3
    return st


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def extract(path, req_header, req_params, resp_body):
    out = []
    if path in ("/putat",):
        m = PUTAT_RE.search(req_header or "")
        if m:
            cid = m.group(1) or m.group(4)
            vid = m.group(2) or m.group(3)
            if cid and vid:
                out.append((cid, vid))
        return out
    blob = (req_params or "") + " " + (resp_body or "")
    cids = CID_RE.findall(blob)
    vids = VID_RE.findall(blob)
    if not vids:
        return out
    cid = cids[0] if cids else "?"
    for v in vids:
        out.append((cid, v))
    return out


def parse_line(line):
    line = line.strip()
    if not line:
        return None
    if line.startswith("{"):
        o = json.loads(line)
        return (
            o.get("path"),
            str(o.get("start_time", 0)),
            json.dumps(o.get("req_header") or {}),
            o.get("req_params") or "",
            o.get("resp_body") or "",
        )
    p = line.split("\t")
    if len(p) < 10:
        return None
    return p[4], p[2], p[5], p[6], p[9]


def pct(xs, p):
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def main(root):
    buckets = defaultdict(lambda: defaultdict(set))
    nlines = 0
    nfiles = 0
    nused = 0
    min_ts = None
    max_ts = None
    for dirpath, _, files in os.walk(root):
        for fn in files:
            fp = os.path.join(dirpath, fn)
            try:
                f = open(fp, "r", errors="replace")
            except OSError:
                continue
            nfiles += 1
            with f:
                for line in f:
                    nlines += 1
                    parsed = parse_line(line)
                    if not parsed:
                        continue
                    path, st, hdr, params, body = parsed
                    if path not in ("/get", "/put", "/alloc", "/putat"):
                        continue
                    try:
                        sec = unix_sec_from_start(st)
                        w = sec // WINDOW * WINDOW
                    except ValueError:
                        continue
                    nused += 1
                    if min_ts is None or sec < min_ts:
                        min_ts = sec
                    if max_ts is None or sec > max_ts:
                        max_ts = sec
                    for cid, vid in extract(path, hdr, params, body):
                        buckets[w][cid].add(vid)

    print("# files", nfiles, "parsed_lines", nlines, "used_lines", nused)
    if min_ts is not None:
        hours = (max_ts - min_ts) / 3600.0
        print("# time_range", iso(min_ts), iso(max_ts), "hours", round(hours, 2))
        print("# windows", len(buckets), "expect_10min", int(hours * 6) + 1)
    print("window_start\twindow_utc\tcluster\tunique_vids")
    series = defaultdict(list)
    for w in sorted(buckets):
        for cid in sorted(buckets[w], key=lambda x: (x == "?", x)):
            n = len(buckets[w][cid])
            series[cid].append(n)
            print("%s\t%s\t%s\t%s" % (w, iso(w), cid, n))

    print("")
    print("# summary per cluster (10min unique vids over the day)")
    print("cluster\twindows\tmin\tp50\tp95\tp99\tmax")
    for cid, xs in series.items():
        xs = sorted(xs)
        print(
            "%s\t%s\t%s\t%s\t%s\t%s\t%s"
            % (cid, len(xs), xs[0], pct(xs, 0.50), pct(xs, 0.95), pct(xs, 0.99), xs[-1])
        )


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
