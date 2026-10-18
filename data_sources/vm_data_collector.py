"""Compact raw tick files (ticks_*.jsonl.gz) into Parquet.

This is the step behind the "Parquet, 30 days" column in the storage table:
it turns each completed hourly file written by tick_collector.py into a few
columnar tables that are several times smaller and directly queryable.

Output layout (under <ticks dir>/parquet/):
    price_change/<YYYYMMDDTHH>.parquet   one row per level change (a message with
                                         N changes becomes N rows)
    trades/<...>.parquet                 last_trade_price events
    bbo/<...>.parquet                    best_bid_ask events
    books/<...>.parquet                  full book snapshots (list columns)
    other/<...>.parquet                  everything else, verbatim as JSON text:
                                         tick_size_change, new_market,
                                         market_resolved, collector markers
                                         (#CONNECT/#DISCONNECT/...), unknown
                                         event types, unparseable or malformed
                                         messages -- so schema drift loses nothing
    _done/<YYYYMMDDTHH>.json             per-hour stats; written last, so an
                                         hour is either fully compacted or not

Why it is small: ids are dictionary-encoded, rows are sorted by (asset,
receive time) so timestamps delta-encode to a byte or two, prices/sizes are
float32 (--exact for float64), the high-entropy order `hash` is dropped
(--keep-hash to keep it), and everything is zstd-compressed.

Query it with DuckDB / pandas / polars, e.g.
    duckdb.sql("select * from 'ticks/parquet/trades/*.parquet' where asset_id = '...'")

Safety
  * only completed hours are touched (not the current UTC hour, nothing modified
    in the last --min-age-minutes), and already-compacted hours are skipped;
  * outputs are written to temp files, renamed, and the row counts are read back
    and checked against what was parsed before the hour is marked done;
  * raw files are deleted only with --delete-raw, and only after that check;
  * --max-gb limits the parquet directory (--on-full stop|delete-oldest).

What is NOT identical to the raw data: the dropped `hash`, float32 rounding of
prices/sizes, and ordering (sorted by asset, then receive time). Keep the raw
files until you are happy with a few hours of output.

Needs: pip install pyarrow numpy

Usage
    python compact_ticks.py                         # compact everything that's ready
    python compact_ticks.py --limit 1               # try one hour first
    python compact_ticks.py --delete-raw --loop-minutes 30 --max-gb 28 --on-full delete-oldest
    python compact_ticks.py --report                # measured bytes/row, GB/day, days in budget
"""

import argparse
import array
import gzip
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

DEFAULT_DIR = os.environ.get("TICKS_DIR", "/var/lib/collector/ticks")
FILE_RE = re.compile(r"^ticks_(\d{8}T\d{2})\.jsonl\.gz$")
GB = 1024 ** 3
NAN = float("nan")
NP_DTYPE = {"q": np.int64, "i": np.int32, "b": np.int8, "f": np.float32, "d": np.float64}
SIDES = {"BUY": 0, "SELL": 1}
KNOWN_EVENTS = ("price_change", "last_trade_price", "best_bid_ask", "book")
ROW_GROUP = 2_000_000


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def num(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return NAN


def to_int(x) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return -1


class Interner:
    """Maps strings to small integer codes (ids repeat millions of times)."""

    def __init__(self) -> None:
        self.codes: dict[str, int] = {}
        self.values: list[str] = []

    def code(self, s) -> int:
        s = "" if s is None else str(s)
        c = self.codes.get(s)
        if c is None:
            c = self.codes[s] = len(self.values)
            self.values.append(s)
        return c


class Parsed:
    """Column buffers for one hour of raw messages (compact `array` storage)."""

    def __init__(self, exact: bool, keep_hash: bool) -> None:
        ft = "d" if exact else "f"
        self.keep_hash = keep_hash
        self.assets, self.markets = Interner(), Interner()
        a = array.array
        self.pc = {k: a(t) for k, t in (
            ("recv", "q"), ("src", "q"), ("asset", "i"), ("market", "i"), ("side", "b"),
            ("price", ft), ("size", ft), ("best_bid", ft), ("best_ask", ft))}
        self.pc_hash: list[str] = []
        self.tr = {k: a(t) for k, t in (
            ("recv", "q"), ("src", "q"), ("asset", "i"), ("market", "i"), ("side", "b"),
            ("price", ft), ("size", ft), ("fee", "i"))}
        self.bbo = {k: a(t) for k, t in (
            ("recv", "q"), ("src", "q"), ("asset", "i"), ("market", "i"),
            ("best_bid", ft), ("best_ask", ft))}
        self.bk = {k: a(t) for k, t in (
            ("recv", "q"), ("src", "q"), ("asset", "i"), ("market", "i"),
            ("n_bids", "i"), ("n_asks", "i"),
            ("bid_px", ft), ("bid_sz", ft), ("ask_px", ft), ("ask_sz", ft))}
        self.bk_hash: list[str] = []
        self.other_recv = array.array("q")
        self.other_type: list[str] = []
        self.other_payload: list[str] = []
        self.stats: Counter = Counter()

    def other(self, recv: int, kind: str, payload: str) -> None:
        self.other_recv.append(recv)
        self.other_type.append(kind)
        self.other_payload.append(payload)

    def rows(self) -> dict[str, int]:
        return {"price_change": len(self.pc["recv"]), "trades": len(self.tr["recv"]),
                "bbo": len(self.bbo["recv"]), "books": len(self.bk["recv"]),
                "other": len(self.other_recv)}


def parse_event(P: Parsed, recv: int, ev) -> None:
    """Route one decoded event into its table; anything odd goes to `other`.

    All rows of an event are built before any is appended, so a malformed
    event can never leave half its rows behind and then also appear in `other`.
    """
    et = ev.get("event_type") if isinstance(ev, dict) else None
    if et not in KNOWN_EVENTS:
        P.other(recv, et if isinstance(et, str) else "unknown", json.dumps(ev, ensure_ascii=False, default=str))
        return
    try:
        src = to_int(ev.get("timestamp"))
        market = P.markets.code(ev.get("market"))
        if et == "price_change":
            rows = [(P.assets.code(c["asset_id"]), SIDES.get(c.get("side"), -1), num(c.get("price")),
                     num(c.get("size")), num(c.get("best_bid")), num(c.get("best_ask")), c.get("hash") or "")
                    for c in ev["price_changes"]]
            for asset, side, price, size, bb, ba, h in rows:
                d = P.pc
                d["recv"].append(recv); d["src"].append(src); d["asset"].append(asset)
                d["market"].append(market); d["side"].append(side); d["price"].append(price)
                d["size"].append(size); d["best_bid"].append(bb); d["best_ask"].append(ba)
                if P.keep_hash:
                    P.pc_hash.append(h)
        elif et == "last_trade_price":
            asset = P.assets.code(ev["asset_id"])
            side, price, size = SIDES.get(ev.get("side"), -1), num(ev.get("price")), num(ev.get("size"))
            fee = to_int(ev.get("fee_rate_bps"))
            d = P.tr
            d["recv"].append(recv); d["src"].append(src); d["asset"].append(asset)
            d["market"].append(market); d["side"].append(side); d["price"].append(price)
            d["size"].append(size); d["fee"].append(fee)
        elif et == "best_bid_ask":
            asset = P.assets.code(ev["asset_id"])
            bb, ba = num(ev.get("best_bid")), num(ev.get("best_ask"))
            d = P.bbo
            d["recv"].append(recv); d["src"].append(src); d["asset"].append(asset)
            d["market"].append(market); d["best_bid"].append(bb); d["best_ask"].append(ba)
        elif et == "book":
            asset = P.assets.code(ev["asset_id"])
            bids = [(num(l["price"]), num(l["size"])) for l in ev["bids"]]
            asks = [(num(l["price"]), num(l["size"])) for l in ev["asks"]]
            d = P.bk
            d["recv"].append(recv); d["src"].append(src); d["asset"].append(asset)
            d["market"].append(market); d["n_bids"].append(len(bids)); d["n_asks"].append(len(asks))
            d["bid_px"].extend(p for p, _ in bids); d["bid_sz"].extend(s for _, s in bids)
            d["ask_px"].extend(p for p, _ in asks); d["ask_sz"].extend(s for _, s in asks)
            if P.keep_hash:
                P.bk_hash.append(ev.get("hash") or "")
        P.stats["events_" + et] += 1
    except (KeyError, TypeError, ValueError, AttributeError):
        P.stats["malformed"] += 1
        P.other(recv, f"malformed:{et}", json.dumps(ev, ensure_ascii=False, default=str))


def parse_hour_file(path: Path, exact: bool = False, keep_hash: bool = False) -> Parsed:
    """Read one hourly raw file into column buffers.

    A truncated final gzip member (crash) ends the read quietly and is flagged
    in stats["truncated"]; lines before it are kept.
    """
    P = Parsed(exact, keep_hash)
    with gzip.open(path, "rt", encoding="utf-8") as f:
        try:
            for line in f:
                ts, _, payload = line.rstrip("\n").partition("\t")
                if not ts.isdigit():
                    P.stats["bad_lines"] += 1
                    continue
                P.stats["lines"] += 1
                recv = int(ts)
                if payload.startswith("#"):
                    P.other(recv, "marker", payload)
                    continue
                try:
                    msg = json.loads(payload)
                except ValueError:
                    P.other(recv, "unparsed", payload)
                    P.stats["unparsed"] += 1
                    continue
                for ev in (msg if isinstance(msg, list) else [msg]):
                    parse_event(P, recv, ev)
        except (EOFError, gzip.BadGzipFile):
            P.stats["truncated"] = 1
    return P


# --- numpy-level helpers ------------------------------------------------------

def np_of(a: array.array) -> np.ndarray:
    return np.frombuffer(a, dtype=NP_DTYPE[a.typecode]) if len(a) else np.empty(0, NP_DTYPE[a.typecode])


def sort_order(recv: np.ndarray, asset: Optional[np.ndarray]) -> np.ndarray:
    """Group by asset, then by receive time (stable, so same-instant rows keep
    their original order -- which matters when replaying book updates)."""
    if asset is None:
        return np.argsort(recv, kind="stable")
    return np.lexsort((recv, asset))


def gather_lists(counts: np.ndarray, flat: np.ndarray, order: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Reorder a ragged column (flat values + per-row counts) by `order`.

    Returns (offsets, values) for the re-ordered rows, vectorised -- no Python
    loop over rows.
    """
    counts = counts.astype(np.int64)
    starts = (np.cumsum(counts) - counts)[order]
    new_counts = counts[order]
    new_off = np.concatenate(([0], np.cumsum(new_counts)))
    idx = np.repeat(starts - new_off[:-1], new_counts) + np.arange(new_off[-1])
    return new_off.astype(np.int32), flat[idx]


# --- Arrow / Parquet ----------------------------------------------------------

def build_tables(P: Parsed, sort: bool = True) -> dict:
    """Turn the buffers into Arrow tables (one per non-empty table)."""
    import pyarrow as pa

    def dict_col(codes: np.ndarray, values: list[str]):
        uniq, inv = np.unique(codes, return_inverse=True)  # keep only ids used in this table
        return pa.DictionaryArray.from_arrays(
            pa.array(inv.astype(np.int32)), pa.array([values[i] for i in uniq], type=pa.string()))

    def side_col(codes: np.ndarray):
        return pa.DictionaryArray.from_arrays(
            np.where(codes < 0, 0, codes).astype(np.int8), pa.array(["BUY", "SELL"]), mask=codes < 0)

    def float_col(x: np.ndarray):
        nan = np.isnan(x)
        return pa.array(x, mask=nan if nan.any() else None)

    def int_col(x: np.ndarray):          # -1 marks "missing"
        neg = x < 0
        return pa.array(x, mask=neg if neg.any() else None)

    tables = {}
    for name, d in (("price_change", P.pc), ("trades", P.tr), ("bbo", P.bbo)):
        if not len(d["recv"]):
            continue
        cols = {k: np_of(v) for k, v in d.items()}
        order = sort_order(cols["recv"], cols["asset"]) if sort else np.arange(len(cols["recv"]))
        cols = {k: v[order] for k, v in cols.items()}
        out = {"recv_ns": pa.array(cols["recv"]), "src_ts_ms": int_col(cols["src"]),
               "asset_id": dict_col(cols["asset"], P.assets.values),
               "market": dict_col(cols["market"], P.markets.values)}
        if "side" in cols:
            out["side"] = side_col(cols["side"])
        for k in ("price", "size", "best_bid", "best_ask"):
            if k in cols:
                out[k] = float_col(cols[k])
        if "fee" in cols:
            out["fee_rate_bps"] = int_col(cols["fee"])
        if name == "price_change" and P.keep_hash:
            out["hash"] = pa.array([P.pc_hash[i] for i in order])
        tables[name] = pa.table(out)

    if len(P.bk["recv"]):
        d = {k: np_of(v) for k, v in P.bk.items()}
        order = sort_order(d["recv"], d["asset"]) if sort else np.arange(len(d["recv"]))
        out = {"recv_ns": pa.array(d["recv"][order]), "src_ts_ms": int_col(d["src"][order]),
               "asset_id": dict_col(d["asset"][order], P.assets.values),
               "market": dict_col(d["market"][order], P.markets.values)}
        for side in ("bid", "ask"):
            off, px = gather_lists(d["n_" + side + "s"], d[side + "_px"], order)
            _, sz = gather_lists(d["n_" + side + "s"], d[side + "_sz"], order)
            out[side + "_px"] = pa.ListArray.from_arrays(pa.array(off), pa.array(px))
            out[side + "_sz"] = pa.ListArray.from_arrays(pa.array(off), pa.array(sz))
        if P.keep_hash:
            out["hash"] = pa.array([P.bk_hash[i] for i in order])
        tables["books"] = pa.table(out)

    if len(P.other_recv):
        recv = np_of(P.other_recv)
        order = np.argsort(recv, kind="stable")
        tables["other"] = pa.table({
            "recv_ns": pa.array(recv[order]),
            "event_type": pa.array([P.other_type[i] for i in order]),
            "payload": pa.array([P.other_payload[i] for i in order])})
    return tables


def write_parquet(table, path: Path, level: int) -> str:
    """Write one table atomically. Returns which encoding path was used.

    First choice delta-encodes the timestamp columns (tiny when sorted); if
    this pyarrow version rejects that combination, falls back to defaults
    (still dictionary + zstd, just larger timestamps).
    """
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    base = dict(compression="zstd", compression_level=level, row_group_size=ROW_GROUP)
    delta = {c: "DELTA_BINARY_PACKED" for c in ("recv_ns", "src_ts_ms") if c in table.column_names}
    try:
        pq.write_table(table, tmp, column_encoding=delta,
                       use_dictionary=[c for c in table.column_names if c not in delta], **base)
        mode = "delta"
    except Exception:  # older pyarrow, or an unsupported encoding combination
        pq.write_table(table, tmp, **base)
        mode = "default"
    os.replace(tmp, path)
    return mode


def compact_hour(raw: Path, out_root: Path, args) -> dict:
    """Compact one raw hourly file; returns the stats dict written to _done/.

    Raises RuntimeError (after removing partial outputs) if a written file's
    row count doesn't match what was parsed.
    """
    import pyarrow.parquet as pq

    hour = FILE_RE.match(raw.name).group(1)
    t0 = time.time()
    P = parse_hour_file(raw, args.exact, args.keep_hash)
    expected = P.rows()
    tables = build_tables(P, sort=not args.no_sort)
    written, sizes, modes = [], {}, set()
    try:
        for name, table in tables.items():
            path = out_root / name / f"{hour}.parquet"
            modes.add(write_parquet(table, path, args.zstd_level))
            written.append(path)
            sizes[name] = path.stat().st_size
            got = pq.ParquetFile(path).metadata.num_rows
            if got != expected[name]:
                raise RuntimeError(f"{name}: parsed {expected[name]} rows but file has {got}")
    except Exception:
        for p in written:
            p.unlink(missing_ok=True)
        raise
    info = {
        "hour": hour, "compacted_at": datetime.now(timezone.utc).isoformat(),
        "raw_bytes": raw.stat().st_size, "raw_lines": P.stats["lines"],
        "rows": {k: v for k, v in expected.items() if v}, "parquet_bytes": sizes,
        "events": sum(v for k, v in expected.items() if k != "other") +
                  sum(1 for t in P.other_type if t != "marker"),
        "truncated": bool(P.stats["truncated"]), "malformed": P.stats["malformed"],
        "unparsed": P.stats["unparsed"], "encoding": sorted(modes), "seconds": round(time.time() - t0, 1),
    }
    done = out_root / "_done"
    done.mkdir(parents=True, exist_ok=True)
    tmp = done / f"{hour}.json.tmp"
    tmp.write_text(json.dumps(info))
    os.replace(tmp, done / f"{hour}.json")
    return info


# --- selection, budget, reporting --------------------------------------------

def ready_files(ticks_dir: Path, out_root: Path, min_age_minutes: float, now: Optional[float] = None) -> list[Path]:
    """Completed raw hours that haven't been compacted yet, oldest first."""
    now = time.time() if now is None else now
    current = datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%dT%H")
    done = {p.stem for p in (out_root / "_done").glob("*.json")} if (out_root / "_done").exists() else set()
    out = []
    for p in sorted(ticks_dir.iterdir()):
        m = FILE_RE.match(p.name)
        if not m or m.group(1) in done or m.group(1) >= current:
            continue
        if now - p.stat().st_mtime < min_age_minutes * 60:
            continue
        out.append(p)
    return out


def parquet_bytes(out_root: Path) -> int:
    return sum(p.stat().st_size for p in out_root.rglob("*.parquet")) if out_root.exists() else 0


def enforce_budget(out_root: Path, max_gb: Optional[float], on_full: str) -> Optional[str]:
    """Return an error string if the parquet directory is over budget and
    can't be brought back under it; with delete-oldest, whole hours are
    removed (all their tables) oldest first."""
    if max_gb is None:
        return None
    limit = max_gb * GB
    if parquet_bytes(out_root) <= limit:
        return None
    if on_full != "delete-oldest":
        return f"parquet directory is {parquet_bytes(out_root) / GB:.1f} GB, over --max-gb {max_gb:g}"
    for marker in sorted((out_root / "_done").glob("*.json")):
        hour = marker.stem
        for f in out_root.glob(f"*/{hour}.parquet"):
            f.unlink()
        marker.unlink()
        log(f"budget: deleted oldest compacted hour {hour}")
        if parquet_bytes(out_root) <= limit:
            return None
    return f"still over --max-gb {max_gb:g} with nothing left to delete"


def report(out_root: Path, max_gb: float) -> None:
    """Measured compression and retention projection from the _done records."""
    marks = sorted((out_root / "_done").glob("*.json")) if (out_root / "_done").exists() else []
    if not marks:
        print("no compacted hours yet; run compaction first")
        return
    infos = [json.loads(m.read_text()) for m in marks]
    raw = sum(i["raw_bytes"] for i in infos)
    pq_bytes = sum(sum(i["parquet_bytes"].values()) for i in infos)
    events = sum(i["events"] for i in infos)
    rows = Counter()
    sizes = Counter()
    for i in infos:
        rows.update(i["rows"])
        sizes.update(i["parquet_bytes"])
    hours = len(infos)
    per_day = pq_bytes / hours * 24 / GB
    print(f"\n--- {out_root} : {hours} compacted hours ({infos[0]['hour']} .. {infos[-1]['hour']}) ---")
    print(f"  raw gzip      {raw / GB:8.3f} GB  ->  parquet {pq_bytes / GB:8.3f} GB   ({raw / max(pq_bytes, 1):.1f}x smaller)")
    print(f"  events        {events:,}   =>  {pq_bytes / max(events, 1):.1f} bytes/event in parquet"
          f"   (raw gzip: {raw / max(events, 1):.1f} bytes/event)")
    for name in ("price_change", "trades", "bbo", "books", "other"):
        if rows[name]:
            print(f"    {name:<13}{rows[name]:>14,} rows  {sizes[name] / GB:7.3f} GB  {sizes[name] / rows[name]:6.1f} B/row")
    print(f"  average       {per_day:.2f} GB/day of parquet  ->  30 days = {per_day * 30:.0f} GB")
    print(f"  at a {max_gb:g} GB budget: about {max_gb / per_day:.1f} days of data")
    flags = [i["hour"] for i in infos if i["truncated"] or i["malformed"] or i["unparsed"]]
    if flags:
        print(f"  hours with truncated/malformed/unparsed data: {', '.join(flags[:8])}"
              f"{' ...' if len(flags) > 8 else ''}  (details are kept in the `other` table)")
    if any(i["encoding"] != ["delta"] for i in infos):
        print("  note: some files used default encodings (this pyarrow rejected delta encoding);"
              " upgrading pyarrow should shrink them")


def run_pass(args) -> int:
    ticks_dir = Path(args.ticks_dir)
    out_root = ticks_dir / "parquet"
    todo = ready_files(ticks_dir, out_root, args.min_age_minutes)
    if args.limit:
        todo = todo[:args.limit]
    if not todo:
        log("nothing ready to compact")
        return 0
    failures = 0
    for raw in todo:
        problem = enforce_budget(out_root, args.max_gb, args.on_full)
        if problem:
            log(f"STOPPING: {problem}")
            return 1
        try:
            info = compact_hour(raw, out_root, args)
        except Exception as e:  # keep going: one bad hour shouldn't block the rest
            failures += 1
            log(f"FAILED {raw.name}: {type(e).__name__}: {e}")
            continue
        total_pq = sum(info["parquet_bytes"].values())
        log(f"{raw.name}: {info['events']:,} events | {info['raw_bytes'] / 1e6:,.1f} MB gz -> "
            f"{total_pq / 1e6:,.1f} MB parquet ({info['raw_bytes'] / max(total_pq, 1):.1f}x, "
            f"{total_pq / max(info['events'], 1):.1f} B/event) in {info['seconds']}s"
            + ("  [TRUNCATED input]" if info["truncated"] else ""))
        if args.delete_raw:
            raw.unlink()
            log(f"deleted raw {raw.name}")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--ticks-dir", default=DEFAULT_DIR, help=f"collector output dir (default: {DEFAULT_DIR})")
    ap.add_argument("--delete-raw", action="store_true", help="delete each raw file after it is verified")
    ap.add_argument("--exact", action="store_true", help="float64 prices/sizes instead of float32")
    ap.add_argument("--keep-hash", action="store_true", help="keep the order hash (high-entropy, ~20 B/row)")
    ap.add_argument("--no-sort", action="store_true", help="keep arrival order (larger files)")
    ap.add_argument("--zstd-level", type=int, default=9, help="zstd level 1-22 (default: 9)")
    ap.add_argument("--min-age-minutes", type=float, default=10.0,
                    help="skip files modified more recently than this (default: 10)")
    ap.add_argument("--limit", type=int, default=0, help="compact at most N hours per pass")
    ap.add_argument("--max-gb", type=float, default=None, help="budget for the parquet directory")
    ap.add_argument("--on-full", choices=("stop", "delete-oldest"), default="stop")
    ap.add_argument("--loop-minutes", type=float, default=0, help="repeat every N minutes (0 = run once)")
    ap.add_argument("--report", action="store_true", help="show measured sizes and projections, then exit")
    args = ap.parse_args()

    out_root = Path(args.ticks_dir) / "parquet"
    if args.report:
        report(out_root, args.max_gb or 30.0)
        return 0
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        log("ERROR: pyarrow is required (pip install pyarrow)")
        return 1
    if not Path(args.ticks_dir).is_dir():
        log(f"ERROR: {args.ticks_dir} does not exist")
        return 1
    while True:
        code = run_pass(args)
        if not args.loop_minutes:
            return code
        time.sleep(args.loop_minutes * 60)


if __name__ == "__main__":
    sys.exit(main())