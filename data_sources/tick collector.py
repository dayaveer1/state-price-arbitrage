"""Polymarket sports tick collector.

Streams every update for every active sports market, as fast as Polymarket
publishes it, and stores the raw messages losslessly. Two feeds are recorded:

  * market channel  (wss://ws-subscriptions-clob.polymarket.com/ws/market)
        book snapshots, price_change, last_trade_price, best_bid_ask,
        tick_size_change, new_market, market_resolved for every subscribed
        token. Sharded over several connections.
  * sports channel  (wss://sports-api.polymarket.com/ws)
        live scores, periods and game status for all active sports events --
        the "a goal was scored" signal to line up against the price moves.

Both are public: no API key, no wallet. Depends on requests and websockets
(pip install requests websockets).

Storage format -- hourly gzip files in --out-dir:
    ticks_YYYYMMDDTHH.jsonl.gz     one line per market-channel message
    scores_YYYYMMDDTHH.jsonl.gz    one line per sports-channel message
    universe.jsonl                 token id -> event/market/question mapping
Each line is  "<local receive time, unix nanoseconds><TAB><payload exactly as
received>". Nothing is parsed or altered, so the files are robust to schema
changes and can be converted to Parquet/DuckDB later. Lines starting with '#'
are collector markers (#CONNECT, #DISCONNECT, #SUBSCRIBE ...): any gap in the
data is recorded as a marker, so missing data is never mistaken for a quiet
market. gzip is flushed every few seconds, so a crash loses seconds, not an
hour; restarts append to the current hour's file.

Which markets: every active event under the Gamma tag(s) in --tags (default
"sports"), re-discovered every --refresh-seconds. New markets are subscribed
on the live connections; resolved ones are unsubscribed. By default one token
per market is streamed (--both-tokens for both): Yes and No books are
mirror images, so this should lose no information and roughly halves volume.

Game-window mode (--game-window): instead of streaming every market all the
time, stream a market only from --window-before-hours before its scheduled
game start (Gamma's gameStartTime, default 1h) to --window-after-hours after
it (default 6h). Most messages are in-play and just-before-kickoff, so this
keeps every fixture while skipping the days of quiet pre-game quoting.
Markets with no gameStartTime (outrights, futures) are skipped unless
--keep-undated; the Gamma archiver's periodic snapshots cover those.

Disk safety: --max-gb is a hard budget for the tick files. --on-full stop
(default) exits with an error when it's reached; --on-full delete-oldest turns
the directory into a rolling window of the most recent data. Free disk below
--min-free-gb is treated the same way.

Every minute it logs message rate, MB written and the projected GB/day and
days of retention for the budget, from what it has actually measured.

Usage
    python tick_collector.py                      # run
    python tick_collector.py --report             # what's on disk, no network
    python tick_collector.py --assets-per-conn 200
Reading data back:
    from tick_collector import read_ticks
    for ts_ns, payload in read_ticks("ticks_20261018T14.jsonl.gz"): ...
"""

import argparse
import asyncio
import gzip
import json
import os
import queue
import random
import re
import shutil
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import requests
import websockets

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SPORTS_WS_URL = "wss://sports-api.polymarket.com/ws"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

PAGE_SIZE = 500
MAX_PAGES = 200
MAX_RETRIES = 3
HEARTBEAT_SECONDS = 10      # documented: send PING every 10s on the market channel
FLUSH_SECONDS = 5
STATS_SECONDS = 60
QUEUE_MAX = 2_000_000       # bounded so a slow disk can't eat all RAM
GB = 1024 ** 3

DEFAULT_OUT_DIR = os.environ.get("TICKS_DIR", "/var/lib/collector/ticks")
FILE_RE = re.compile(r"^(ticks|scores)_(\d{8}T\d{2})\.jsonl\.gz$")


class FetchError(RuntimeError):
    """The universe couldn't be read reliably from the Gamma API."""


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# --- Universe discovery -----------------------------------------------------

def _get_json(session: requests.Session, params: dict) -> list:
    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(GAMMA_EVENTS_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise FetchError(f"expected a list, got {type(data).__name__}")
            return data
        except (requests.RequestException, ValueError, FetchError) as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1))
    raise FetchError(f"{params} failed after {MAX_RETRIES} attempts: {last_error}")


def _json_list(value) -> list:
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, list) else []


def parse_game_start(value) -> Optional[datetime]:
    """Parse Gamma's gameStartTime into an aware UTC datetime, or None.

    It arrives as '2026-09-23 08:00:00+00' on some markets and as ISO-8601
    with 'Z' on others; both are normalised here (older Pythons reject them).
    """
    if not value or not isinstance(value, str):
        return None
    text = value.strip().replace(" ", "T", 1)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    elif re.search(r"[+-]\d{2}$", text) and "T" in text:
        text += ":00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def add_event_assets(universe: dict, event, both_tokens: bool,
                     window: Optional[tuple[timedelta, timedelta]] = None,
                     now: Optional[datetime] = None, keep_undated: bool = False,
                     counts: Optional[Counter] = None) -> None:
    """Add the streamable tokens of one event to `universe` (token id -> info).

    With `window` = (before, after), a market is only added while `now` is
    within [gameStartTime - before, gameStartTime + after]. `counts` collects
    markets_seen / undated / outside_window for logging.
    """
    if not isinstance(event, dict):
        return
    if counts is None:
        counts = Counter()
    tags = [str(t.get("label") or t.get("slug")) for t in (event.get("tags") or [])
            if isinstance(t, dict) and (t.get("label") or t.get("slug"))]
    for market in event.get("markets") or []:
        if not isinstance(market, dict) or market.get("closed") or market.get("enableOrderBook") is False:
            continue
        try:
            token_ids = _json_list(market.get("clobTokenIds"))
            outcomes = _json_list(market.get("outcomes"))
        except ValueError:
            continue
        if not token_ids:
            continue
        counts["markets_seen"] += 1
        if window is not None:
            start = parse_game_start(market.get("gameStartTime") or event.get("gameStartTime"))
            if start is None:
                if not keep_undated:
                    counts["undated"] += 1
                    continue
            elif not (start - window[0] <= now <= start + window[1]):
                counts["outside_window"] += 1
                continue
        for i, token in enumerate(token_ids if both_tokens else token_ids[:1]):
            universe.setdefault(str(token), {
                "asset_id": str(token),
                "outcome": outcomes[i] if i < len(outcomes) else None,
                "condition_id": market.get("conditionId"),
                "market_id": market.get("id"),
                "event_id": event.get("id"),
                "event_slug": event.get("slug"),
                "event_title": event.get("title"),
                "question": market.get("question"),
                "tags": tags,
            })


def fetch_universe(tags: list[str], both_tokens: bool,
                   window: Optional[tuple[timedelta, timedelta]] = None,
                   keep_undated: bool = False) -> tuple[dict[str, dict], int]:
    """Streamable tokens for active events under the given Gamma tags.

    Returns:
        (universe, markets_seen). markets_seen counts every open market found,
        before any game-window filtering, so "nothing in the window right now"
        can be told apart from "wrong tag / blocked".

    Raises:
        FetchError: On any failed page, or if the page cap is reached while pages
            are still full (never returns a truncated universe).
    """
    universe: dict[str, dict] = {}
    counts: Counter = Counter()
    now = datetime.now(timezone.utc)
    with requests.Session() as session:
        for tag in tags:
            before, seen_before = len(universe), counts["markets_seen"]
            for page in range(MAX_PAGES):
                batch = _get_json(session, {
                    "tag_slug": tag, "active": "true", "closed": "false",
                    "limit": PAGE_SIZE, "offset": page * PAGE_SIZE,
                    "order": "startDate", "ascending": "false",
                })
                for event in batch:
                    add_event_assets(universe, event, both_tokens, window, now, keep_undated, counts)
                if len(batch) < PAGE_SIZE:
                    break
                time.sleep(0.25)
            else:
                raise FetchError(f"tag {tag!r}: still full pages after {MAX_PAGES} pages")
            found, seen = len(universe) - before, counts["markets_seen"] - seen_before
            note = ""
            if seen == 0:
                note = "  <-- ZERO open markets: wrong slug?"
            elif found == 0 and window is None:
                note = "  <-- 0 new: already covered by an earlier tag"
            log(f"  tag {tag!r}: {found:,} new tokens"
                + (f" in window (of {seen:,} open markets)" if window else "") + note)
    if window is not None:
        log(f"  window filter: {counts['outside_window']:,} outside window, "
            f"{counts['undated']:,} without gameStartTime{' (kept)' if keep_undated else ' (skipped)'}")
    return universe, counts["markets_seen"]


# --- Disk writer ------------------------------------------------------------

class TickWriter(threading.Thread):
    """Single writer thread: queue in, hourly gzip files out.

    The network side only does queue.put_nowait, so disk or compression
    stalls can't delay reading from the sockets. If the queue ever fills,
    messages are dropped and counted (reported in the stats line) rather than
    blocking or exhausting memory.
    """

    def __init__(self, out_dir: Path):
        super().__init__(daemon=True, name="tick-writer")
        self.out_dir = out_dir
        self.q: "queue.Queue" = queue.Queue(maxsize=QUEUE_MAX)
        self.dropped = 0
        self._files: dict[str, tuple[str, gzip.GzipFile]] = {}
        self._lock = threading.Lock()

    def put(self, stream: str, ts_ns: int, payload: str) -> None:
        try:
            self.q.put_nowait((stream, ts_ns, payload))
        except queue.Full:
            self.dropped += 1

    def open_paths(self) -> set[Path]:
        with self._lock:
            return {Path(f.name) for _, f in self._files.values()}

    def _file_for(self, stream: str) -> gzip.GzipFile:
        hour = datetime.now(timezone.utc).strftime("%Y%m%dT%H")
        current = self._files.get(stream)
        if current and current[0] == hour:
            return current[1]
        with self._lock:
            if current:
                current[1].close()
            path = self.out_dir / f"{stream}_{hour}.jsonl.gz"
            handle = gzip.open(path, "ab", compresslevel=6)  # append: restarts join the hour
            self._files[stream] = (hour, handle)
        return handle

    def run(self) -> None:
        last_flush = time.time()
        while True:
            try:
                item = self.q.get(timeout=1.0)
            except queue.Empty:
                item = "idle"
            if item is None:
                break
            if item != "idle":
                stream, ts_ns, payload = item
                self._file_for(stream).write(f"{ts_ns}\t{payload}\n".encode("utf-8"))
            if time.time() - last_flush >= FLUSH_SECONDS:
                for _, handle in list(self._files.values()):
                    handle.flush()  # sync-flush: data is recoverable even after a crash
                last_flush = time.time()
        with self._lock:
            for _, handle in self._files.values():
                handle.close()
            self._files.clear()

    def stop(self) -> None:
        self.q.put(None)


def read_ticks(path) -> Iterator[tuple[int, str]]:
    """Yield (receive time in unix ns, payload) from one hourly file.

    Marker lines (payload starting with '#') are included. A truncated final
    member from a crash ends iteration quietly instead of raising.
    """
    with gzip.open(path, "rt", encoding="utf-8") as f:
        try:
            for line in f:
                ts, _, payload = line.rstrip("\n").partition("\t")
                if ts.isdigit():
                    yield int(ts), payload
        except (EOFError, gzip.BadGzipFile):
            return


# --- Shards: market channel connections -------------------------------------

class Shard:
    def __init__(self, shard_id: int):
        self.id = shard_id
        self.assets: set[str] = set()
        self.ws = None


class Collector:
    """Owns the shard connections, the universe and the statistics."""

    def __init__(self, writer: TickWriter, capacity: int, universe_path: Path):
        self.writer = writer
        self.capacity = capacity
        self.universe_path = universe_path
        self.shards: list[Shard] = []
        self.tasks: list[asyncio.Task] = []
        self.known: set[str] = set()
        self.stats: Counter = Counter()

    def marker(self, stream: str, text: str) -> None:
        self.writer.put(stream, time.time_ns(), f"#{text}")

    # -- universe changes
    def add_assets(self, universe: dict[str, dict], new_ids: list[str]) -> None:
        with open(self.universe_path, "a", encoding="utf-8") as f:
            for asset in new_ids:
                f.write(json.dumps({**universe[asset], "first_seen_ns": time.time_ns()},
                                   ensure_ascii=False) + "\n")
        pending = list(new_ids)
        self.known.update(new_ids)
        for shard in self.shards:
            room = self.capacity - len(shard.assets)
            if room > 0 and pending:
                chunk, pending = pending[:room], pending[room:]
                shard.assets.update(chunk)
                asyncio.create_task(self._send(shard, "subscribe", chunk))
        while pending:
            chunk, pending = pending[:self.capacity], pending[self.capacity:]
            shard = Shard(len(self.shards))
            shard.assets.update(chunk)
            self.shards.append(shard)
            self.tasks.append(asyncio.create_task(self.run_shard(shard)))

    def remove_assets(self, gone_ids: list[str]) -> None:
        gone = set(gone_ids)
        self.known -= gone
        for shard in self.shards:
            chunk = list(shard.assets & gone)
            if chunk:
                shard.assets -= set(chunk)
                asyncio.create_task(self._send(shard, "unsubscribe", chunk))

    async def _send(self, shard: Shard, operation: str, assets: list[str]) -> None:
        """Dynamic (un)subscribe on a live connection. If the shard isn't
        connected, nothing is lost: it subscribes to shard.assets on connect."""
        ws = shard.ws
        if ws is None or not assets:
            return
        message = {"assets_ids": assets, "operation": operation}
        if operation == "subscribe":
            message["custom_feature_enabled"] = True
        try:
            await ws.send(json.dumps(message))
            self.marker("ticks", f"{operation.upper()} shard={shard.id} n={len(assets)}")
        except Exception as e:  # connection dropped; reconnect resyncs
            log(f"shard {shard.id}: {operation} failed ({type(e).__name__}); will resync on reconnect")

    # -- one market-channel connection
    async def run_shard(self, shard: Shard) -> None:
        backoff = 1.0
        while True:
            assets = list(shard.assets)
            if not assets:
                await asyncio.sleep(5)
                continue
            try:
                async with websockets.connect(MARKET_WS_URL, ping_interval=None,
                                              max_size=2 ** 25) as ws:
                    sent = set(assets)
                    await ws.send(json.dumps({"assets_ids": assets, "type": "market",
                                              "custom_feature_enabled": True}))
                    shard.ws = ws
                    missed = shard.assets - sent  # added while we were connecting
                    if missed:
                        await self._send(shard, "subscribe", list(missed))
                    self.marker("ticks", f"CONNECT shard={shard.id} assets={len(sent)}")
                    log(f"shard {shard.id} connected ({len(sent)} assets)")
                    backoff = 1.0
                    heartbeat = asyncio.create_task(self._heartbeat(ws))
                    odd = 0
                    try:
                        async for raw in ws:
                            if isinstance(raw, bytes):
                                raw = raw.decode("utf-8", "replace")
                            if raw == "PONG":
                                continue
                            if raw[:1] not in ("{", "[") and odd < 5:
                                odd += 1
                                log(f"shard {shard.id}: unexpected non-JSON message: {raw[:200]!r}")
                            self.stats["messages"] += 1
                            self.writer.put("ticks", time.time_ns(), raw)
                    finally:
                        heartbeat.cancel()
                        shard.ws = None
                        self.marker("ticks", f"DISCONNECT shard={shard.id}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                shard.ws = None
                self.stats["reconnects"] += 1
                log(f"shard {shard.id}: {type(e).__name__}: {e}; reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)

    @staticmethod
    async def _heartbeat(ws) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            await ws.send("PING")

    # -- sports score channel
    async def run_sports(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(SPORTS_WS_URL, ping_interval=None,
                                              max_size=2 ** 24) as ws:
                    self.marker("scores", "CONNECT")
                    log("sports score channel connected")
                    backoff = 1.0
                    try:
                        async for raw in ws:
                            if isinstance(raw, bytes):
                                raw = raw.decode("utf-8", "replace")
                            if raw.strip().lower() == "ping":
                                await ws.send("pong")  # must answer within 10s or it drops us
                                continue
                            self.stats["score_messages"] += 1
                            self.writer.put("scores", time.time_ns(), raw)
                    finally:
                        self.marker("scores", "DISCONNECT")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats["reconnects"] += 1
                log(f"sports channel: {type(e).__name__}: {e}; reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60.0)


# --- Disk budget, stats, report ---------------------------------------------

def data_files(out_dir: Path) -> list[Path]:
    return sorted(p for p in out_dir.iterdir() if FILE_RE.match(p.name))


def dir_bytes(out_dir: Path) -> int:
    return sum(p.stat().st_size for p in data_files(out_dir))


def enforce_budget(out_dir: Path, writer: TickWriter, max_gb: float, min_free_gb: float,
                   on_full: str) -> Optional[str]:
    """Return an error string if the budget is exceeded and can't be fixed.

    With on_full == "delete-oldest", old tick files (never the ones currently
    open) are removed until back under budget.
    """
    def over() -> Optional[str]:
        used = dir_bytes(out_dir)
        if used > max_gb * GB:
            return f"tick files use {used / GB:.1f} GB, over --max-gb {max_gb}"
        free = shutil.disk_usage(out_dir).free
        if free < min_free_gb * GB:
            return f"only {free / GB:.1f} GB free, under --min-free-gb {min_free_gb}"
        return None

    problem = over()
    if problem and on_full == "delete-oldest":
        keep = writer.open_paths()
        for path in [p for p in data_files(out_dir) if p.name.startswith("ticks_")]:
            if path in keep:
                continue
            path.unlink()
            log(f"budget: deleted oldest file {path.name}")
            problem = over()
            if not problem:
                break
    return problem


def report(out_dir: Path, max_gb: float) -> None:
    """Summarise what's on disk and project retention for the budget."""
    files = data_files(out_dir) if out_dir.exists() else []
    ticks = [p for p in files if p.name.startswith("ticks_")]
    print(f"\n--- {out_dir} ---")
    print(f"  files: {len(files)}   total: {sum(p.stat().st_size for p in files) / GB:.2f} GB")
    if len(ticks) < 2:
        print("  need at least 2 hourly tick files (one complete hour) to project retention")
        return
    complete = ticks[:-1]  # the newest hour is still being written
    gb_per_hour = sum(p.stat().st_size for p in complete) / GB / len(complete)
    print(f"  tick files: {len(ticks)}   first: {ticks[0].name}   last: {ticks[-1].name}")
    print(f"  average over {len(complete)} complete hours: {gb_per_hour:.3f} GB/hour"
          f" = {gb_per_hour * 24:.1f} GB/day")
    print(f"  at a {max_gb:g} GB budget: about {max_gb / (gb_per_hour * 24):.1f} days of ticks")
    biggest = max(complete, key=lambda p: p.stat().st_size)
    print(f"  busiest complete hour: {biggest.name} ({biggest.stat().st_size / GB:.3f} GB)")


async def monitor(collector: Collector, out_dir: Path, args, stop: asyncio.Event,
                  result: dict) -> None:
    start, start_bytes = time.time(), dir_bytes(out_dir)
    last_t, last_msgs = start, 0
    while not stop.is_set():
        await asyncio.sleep(STATS_SECONDS)
        now = time.time()
        msgs = collector.stats["messages"]
        used = dir_bytes(out_dir)
        rate = (msgs - last_msgs) / (now - last_t)
        grown = used - start_bytes
        elapsed = now - start
        line = (f"stats: {rate:,.0f} msg/s | {msgs:,} msgs | scores {collector.stats['score_messages']:,}"
                f" | shards {sum(1 for s in collector.shards if s.ws)}/{len(collector.shards)}"
                f" | assets {len(collector.known):,} | queue {collector.writer.q.qsize():,}"
                f" | dropped {collector.writer.dropped} | reconnects {collector.stats['reconnects']}"
                f" | disk {used / GB:.2f} GB")
        if elapsed > 300 and grown > 0:   # wait for gzip buffers to settle before projecting
            per_day = grown / elapsed * 86400 / GB
            line += f" | projected {per_day:.1f} GB/day"
            if per_day > 0:
                line += f" -> {max(args.max_gb - used / GB, 0) / per_day:.1f} days until budget"
        log(line)
        last_t, last_msgs = now, msgs

        problem = enforce_budget(out_dir, collector.writer, args.max_gb, args.min_free_gb, args.on_full)
        if problem:
            log(f"STOPPING: {problem}")
            result["error"] = problem
            stop.set()


async def refresh_universe(collector: Collector, args, tags: list[str], window,
                           stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.refresh_seconds)
            return
        except asyncio.TimeoutError:
            pass
        try:
            universe, _ = await asyncio.to_thread(
                fetch_universe, tags, args.both_tokens, window, args.keep_undated)
        except FetchError as e:
            log(f"universe refresh failed, keeping current subscriptions: {e}")
            continue
        added = [a for a in universe if a not in collector.known]
        gone = [a for a in collector.known if a not in universe]
        if added:
            collector.add_assets(universe, added)
        if gone:
            collector.remove_assets(gone)
        if added or gone:
            log(f"universe refresh: +{len(added)} / -{len(gone)} assets (now {len(collector.known):,})")


async def run(args) -> int:
    out_dir = Path(args.out_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log(f"ERROR: can't create {out_dir}: {e}")
        return 1
    if not os.access(out_dir, os.W_OK):
        log(f"ERROR: {out_dir} is not writable by this user")
        return 1

    tags = [t.strip() for t in args.tags.split(",") if t.strip()]
    window = ((timedelta(hours=args.window_before_hours), timedelta(hours=args.window_after_hours))
              if args.game_window else None)
    if args.refresh_seconds is None:
        args.refresh_seconds = 120 if window else 300
    log(f"discovering active markets for tags {tags}"
        + (f" (game window: -{args.window_before_hours:g}h / +{args.window_after_hours:g}h)" if window else "")
        + " ...")
    try:
        universe, seen = await asyncio.to_thread(
            fetch_universe, tags, args.both_tokens, window, args.keep_undated)
    except FetchError as e:
        log(f"ERROR: couldn't read the market universe, nothing started: {e}")
        return 1
    if not seen or (not universe and not window):
        log("ERROR: zero streamable markets found (wrong tags, or blocked?)")
        return 1
    log(f"{len(universe):,} tokens to stream now"
        + (" (nothing in the window yet; will pick up games as they approach)" if not universe else ""))

    writer = TickWriter(out_dir)
    writer.start()
    collector = Collector(writer, args.assets_per_conn, out_dir / "universe.jsonl")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass

    result: dict = {}
    collector.add_assets(universe, list(universe))
    background = [
        asyncio.create_task(collector.run_sports()),
        asyncio.create_task(monitor(collector, out_dir, args, stop, result)),
        asyncio.create_task(refresh_universe(collector, args, tags, window, stop)),
    ]
    await stop.wait()

    log("shutting down ...")
    for task in collector.tasks + background:
        task.cancel()
    await asyncio.gather(*collector.tasks, *background, return_exceptions=True)
    writer.stop()
    writer.join(timeout=60)
    if result.get("error"):
        log(f"ERROR: {result['error']}")
        return 1
    log("stopped cleanly")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                        help=f"where tick files go (default: {DEFAULT_OUT_DIR})")
    parser.add_argument("--tags", default="sports",
                        help="comma-separated Gamma tag slugs to stream (default: sports)")
    parser.add_argument("--both-tokens", action="store_true",
                        help="stream both outcome tokens per market (default: first only)")
    parser.add_argument("--assets-per-conn", type=int, default=500,
                        help="tokens per websocket connection (default: 500; lower it if the "
                             "server drops connections right after subscribing)")
    parser.add_argument("--refresh-seconds", type=int, default=None,
                        help="how often to look for new/resolved markets "
                             "(default: 300, or 120 with --game-window)")
    parser.add_argument("--game-window", action="store_true",
                        help="only stream each market from shortly before its gameStartTime "
                             "until some hours after it")
    parser.add_argument("--window-before-hours", type=float, default=1.0,
                        help="with --game-window: start streaming this long before the game (default: 1)")
    parser.add_argument("--window-after-hours", type=float, default=6.0,
                        help="with --game-window: keep streaming this long after the start (default: 6)")
    parser.add_argument("--keep-undated", action="store_true",
                        help="with --game-window: also stream markets that have no gameStartTime")
    parser.add_argument("--max-gb", type=float, default=30.0,
                        help="budget for tick files (default: 30)")
    parser.add_argument("--min-free-gb", type=float, default=3.0,
                        help="stop if free disk drops below this (default: 3)")
    parser.add_argument("--on-full", choices=("stop", "delete-oldest"), default="stop",
                        help="what to do at the budget (default: stop)")
    parser.add_argument("--report", action="store_true",
                        help="summarise files on disk and project retention, then exit")
    args = parser.parse_args()

    if args.report:
        report(Path(args.out_dir), args.max_gb)
        return 0
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
