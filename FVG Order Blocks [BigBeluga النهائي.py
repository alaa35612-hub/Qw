#!/usr/bin/env python3
"""FVG + Order-Block scanner with Pine-like execution order."""

from __future__ import annotations

import argparse
import concurrent.futures
import random
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover
    ccxt = None  # type: ignore

import numpy as np

ANSI_RESET = "\033[0m"
ANSI_BLUE = "\033[94m"
ANSI_GREEN = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_RED = "\033[91m"


DEFAULT_CONFIG: Dict[str, object] = {
    "timeframe": "1m",
    "lookback_candles": 2000,
    "atr_length": 200,
    "gap_filter_pct": 0.5,
    "box_amount": 6,
    "show_broken": False,
    "show_signal": False,
    "signal_age_bars": 0,
    "scan_interval_sec": 30,
    "exclude_last_unconfirmed": True,
    "rate_limit": True,
    "atr_warmup_extra": 500,
    # NEW: event filters/printing
    "event_age_bars": 3,
    "print_touched": True,
    "print_created": True,
    "print_broken": True,
    "print_near": True,
    "near_zone_pct": 0.20,
    # NEW: daily rise filter (symbol-level pre-filter)
    "min_daily_rise_pct": 0.0,
    # Performance/safety tuning
    "scan_workers": 4,
    "request_delay_ms": 500,
    "request_jitter_ms": 120,
    "scan_symbols_count": 500,
    "use_ticker_daily_prefilter": True,
    "ticker_prefilter_top_n": 220,
    # Zone-strength filter/reporting (Filter Gaps by %)
    "min_zone_gap_strength_pct": 0.5,
    "print_top_zone_strength": True,
}


@dataclass
class BoxLevel:
    kind: str
    top: float
    bottom: float
    created_index: int
    created_ts: int
    strength_pct: float
    touched_reported: bool = False
    near_reported: bool = False


@dataclass
class Signal:
    symbol: str
    kind: str
    ts: int
    price: float
    info: str


@dataclass
class ZoneEvent:
    symbol: str
    kind: str  # zone_created / zone_touched / zone_broken / zone_near
    side: str  # bull / bear
    ts: int
    price: float
    info: str
    strength_pct: float = 0.0


def wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    n = len(close)
    atr = np.full(n, np.nan, dtype=float)
    if n < length + 1:
        return atr

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))

    atr[length] = np.mean(tr[1 : length + 1])
    for i in range(length + 1, n):
        atr[i] = ((atr[i - 1] * (length - 1)) + tr[i]) / length
    return atr


def _safe_nanmax(values: np.ndarray) -> float:
    if values.size == 0:
        return np.nan
    mask = ~np.isnan(values)
    if not np.any(mask):
        return np.nan
    return float(np.max(values[mask]))


def _timeframe_to_minutes(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == "m":
        return val
    if unit == "h":
        return val * 60
    if unit == "d":
        return val * 1440
    return 0


def _daily_rise_pct_from_ohlcv(ohlcv: List[List[float]], timeframe: str) -> Optional[float]:
    mins = _timeframe_to_minutes(timeframe)
    if mins <= 0 or len(ohlcv) < 3:
        return None
    bars_per_day = max(1, int(1440 / mins))
    if len(ohlcv) <= bars_per_day:
        return None
    now_close = float(ohlcv[-1][4])
    old_close = float(ohlcv[-1 - bars_per_day][4])
    if old_close == 0:
        return None
    return (now_close - old_close) / old_close * 100.0


def _distance_to_zone_pct(price: float, bottom: float, top: float) -> float:
    if bottom <= price <= top:
        return 0.0
    if price < bottom:
        return ((bottom - price) / bottom) * 100.0 if bottom else float("inf")
    return ((price - top) / top) * 100.0 if top else float("inf")


def _event_color(kind: str) -> str:
    if kind == "zone_touched":
        return ANSI_BLUE
    if kind == "zone_created":
        return ANSI_GREEN
    if kind == "zone_broken":
        return ANSI_YELLOW
    if kind == "zone_near":
        return ANSI_RED
    return ANSI_RESET


def _process_bull_boxes_like_pine(
    levels: List[BoxLevel],
    i: int,
    high: np.ndarray,
    low: np.ndarray,
    is_bull_gap: bool,
    show_broken: bool,
    show_signal: bool,
    symbol: str,
    ts: np.ndarray,
    signals: List[Signal],
    events: List[ZoneEvent],
    near_zone_pct: float,
) -> None:
    idx = 0
    while idx < len(levels):
        box = levels[idx]
        deleted = False

        if (not box.touched_reported) and (high[i] >= box.bottom) and (low[i] <= box.top):
            box.touched_reported = True
            events.append(
                ZoneEvent(symbol, "zone_touched", "bull", int(ts[i]), float(low[i]), f"Bull zone touched [{box.bottom:.6f}, {box.top:.6f}] | gap={box.strength_pct:.3f}%", strength_pct=box.strength_pct)
            )

        close_i = float((high[i] + low[i]) / 2.0)
        near_pct = _distance_to_zone_pct(close_i, box.bottom, box.top)
        if (not box.touched_reported) and (not box.near_reported) and (near_pct > 0.0) and (near_pct <= near_zone_pct):
            box.near_reported = True
            events.append(
                ZoneEvent(symbol, "zone_near", "bull", int(ts[i]), close_i, f"Bull near zone ({near_pct:.3f}% <= {near_zone_pct:.3f}%) [{box.bottom:.6f}, {box.top:.6f}] | gap={box.strength_pct:.3f}%", strength_pct=box.strength_pct)
            )

        if high[i] < box.bottom:
            events.append(
                ZoneEvent(symbol, "zone_broken", "bull", int(ts[i]), float(high[i]), f"Bull zone broken below {box.bottom:.6f} | gap={box.strength_pct:.3f}%", strength_pct=box.strength_pct)
            )
            if not show_broken:
                levels.pop(idx)
                deleted = True

        if not deleted and show_signal and i >= 1:
            if (low[i] > box.top) and (low[i - 1] <= box.top) and (not is_bull_gap):
                signals.append(
                    Signal(symbol, "bull_break", int(ts[i - 1]), float(low[i - 1]), f"Break above Bull OB top={box.top:.6f} (gap={box.strength_pct:.3f}%)")
                )

        if not deleted:
            remove_current = False
            for other in levels:
                if (other.top < box.top) and (other.top > box.bottom):
                    remove_current = True
                    break
            if remove_current:
                levels.pop(idx)
                deleted = True

        if not deleted:
            idx += 1


def _process_bear_boxes_like_pine(
    levels: List[BoxLevel],
    i: int,
    high: np.ndarray,
    low: np.ndarray,
    is_bear_gap: bool,
    show_broken: bool,
    show_signal: bool,
    symbol: str,
    ts: np.ndarray,
    signals: List[Signal],
    events: List[ZoneEvent],
    near_zone_pct: float,
) -> None:
    idx = 0
    while idx < len(levels):
        box = levels[idx]
        deleted = False

        if (not box.touched_reported) and (high[i] >= box.bottom) and (low[i] <= box.top):
            box.touched_reported = True
            events.append(
                ZoneEvent(symbol, "zone_touched", "bear", int(ts[i]), float(high[i]), f"Bear zone touched [{box.bottom:.6f}, {box.top:.6f}] | gap={box.strength_pct:.3f}%", strength_pct=box.strength_pct)
            )

        close_i = float((high[i] + low[i]) / 2.0)
        near_pct = _distance_to_zone_pct(close_i, box.bottom, box.top)
        if (not box.touched_reported) and (not box.near_reported) and (near_pct > 0.0) and (near_pct <= near_zone_pct):
            box.near_reported = True
            events.append(
                ZoneEvent(symbol, "zone_near", "bear", int(ts[i]), close_i, f"Bear near zone ({near_pct:.3f}% <= {near_zone_pct:.3f}%) [{box.bottom:.6f}, {box.top:.6f}] | gap={box.strength_pct:.3f}%", strength_pct=box.strength_pct)
            )

        if low[i] > box.top:
            events.append(
                ZoneEvent(symbol, "zone_broken", "bear", int(ts[i]), float(low[i]), f"Bear zone broken above {box.top:.6f} | gap={box.strength_pct:.3f}%", strength_pct=box.strength_pct)
            )
            if not show_broken:
                levels.pop(idx)
                deleted = True

        if not deleted and show_signal and i >= 1:
            if (high[i] < box.bottom) and (high[i - 1] >= box.bottom) and (not is_bear_gap):
                signals.append(
                    Signal(symbol, "bear_break", int(ts[i - 1]), float(high[i - 1]), f"Break below Bear OB bottom={box.bottom:.6f} (gap={box.strength_pct:.3f}%)")
                )

        if not deleted:
            remove_current = False
            for other in levels:
                if (other.top < box.top) and (other.top > box.bottom):
                    remove_current = True
                    break
            if remove_current:
                levels.pop(idx)
                deleted = True

        if not deleted:
            idx += 1


def _filter_by_age_ts(events: List[ZoneEvent], ts: np.ndarray, age_bars: int) -> List[ZoneEvent]:
    if age_bars <= 0:
        return events
    n = len(ts)
    cutoff_index = max(0, n - 1 - age_bars)
    ts_to_index = {int(ts[idx]): idx for idx in range(n)}
    return [e for e in events if ts_to_index.get(e.ts, -1) >= cutoff_index]


def analyze_symbol(symbol: str, ohlcv: List[List[float]], cfg: Dict[str, object]) -> Tuple[List[Signal], List[ZoneEvent]]:
    arr = np.array(ohlcv, dtype=float)
    ts = arr[:, 0].astype(np.int64)
    high = arr[:, 2]
    low = arr[:, 3]
    close = arr[:, 4]

    if bool(cfg["exclude_last_unconfirmed"]) and len(ts) > 5:
        ts = ts[:-1]
        high = high[:-1]
        low = low[:-1]
        close = close[:-1]

    n = len(ts)
    atr_length = int(cfg["atr_length"])
    if n < max(atr_length + 5, 10):
        return [], []

    atr = wilder_atr(high, low, close, atr_length)

    filt_up = np.full(n, np.nan, dtype=float)
    filt_dn = np.full(n, np.nan, dtype=float)
    for i in range(2, n):
        if low[i] != 0:
            filt_up[i] = (low[i] - high[i - 2]) / low[i] * 100.0
        if low[i - 2] != 0:
            filt_dn[i] = (low[i - 2] - high[i]) / low[i - 2] * 100.0

    max_up_series = np.full(n, np.nan, dtype=float)
    max_dn_series = np.full(n, np.nan, dtype=float)
    win = min(200, n)
    for i in range(n):
        start = max(0, i - win + 1)
        max_up_series[i] = _safe_nanmax(filt_up[start : i + 1])
        max_dn_series[i] = _safe_nanmax(filt_dn[start : i + 1])

    boxes_bull: List[BoxLevel] = []
    boxes_bear: List[BoxLevel] = []
    signals: List[Signal] = []
    events: List[ZoneEvent] = []

    lookback = min(int(cfg["lookback_candles"]), n)
    start_i = max(0, n - lookback)
    gap_filter_pct = float(cfg["gap_filter_pct"])

    for i in range(start_i, n):
        if i < 2 or np.isnan(atr[i]):
            continue

        is_bull_gap = (
            (high[i - 2] < low[i])
            and (high[i - 2] < high[i - 1])
            and (low[i - 2] < low[i])
            and (filt_up[i] > gap_filter_pct)
        )
        is_bear_gap = (
            (low[i - 2] > high[i])
            and (low[i - 2] > low[i - 1])
            and (high[i - 2] > high[i])
            and (filt_dn[i] > gap_filter_pct)
        )

        if is_bull_gap:
            top = high[i - 2]
            bottom = top - atr[i]
            box = BoxLevel("bull", top, bottom, i, int(ts[i]), float(filt_up[i]))
            boxes_bull.append(box)
            events.append(ZoneEvent(symbol, "zone_created", "bull", int(ts[i]), float(top), f"New bull zone [{bottom:.6f}, {top:.6f}] | gap={float(filt_up[i]):.3f}%", strength_pct=float(filt_up[i])))

        if is_bear_gap:
            bottom = low[i - 2]
            top = bottom + atr[i]
            box = BoxLevel("bear", top, bottom, i, int(ts[i]), float(filt_dn[i]))
            boxes_bear.append(box)
            events.append(ZoneEvent(symbol, "zone_created", "bear", int(ts[i]), float(bottom), f"New bear zone [{bottom:.6f}, {top:.6f}] | gap={float(filt_dn[i]):.3f}%", strength_pct=float(filt_dn[i])))

        _process_bull_boxes_like_pine(
            levels=boxes_bull,
            i=i,
            high=high,
            low=low,
            is_bull_gap=is_bull_gap,
            show_broken=bool(cfg["show_broken"]),
            show_signal=bool(cfg["show_signal"]),
            symbol=symbol,
            ts=ts,
            signals=signals,
            events=events,
            near_zone_pct=float(cfg.get("near_zone_pct", 0.2)),
        )
        _process_bear_boxes_like_pine(
            levels=boxes_bear,
            i=i,
            high=high,
            low=low,
            is_bear_gap=is_bear_gap,
            show_broken=bool(cfg["show_broken"]),
            show_signal=bool(cfg["show_signal"]),
            symbol=symbol,
            ts=ts,
            signals=signals,
            events=events,
            near_zone_pct=float(cfg.get("near_zone_pct", 0.2)),
        )

        box_amount = int(cfg["box_amount"])
        if box_amount > 0:
            while len(boxes_bull) >= box_amount:
                boxes_bull.pop(0)
            while len(boxes_bear) >= box_amount:
                boxes_bear.pop(0)

    age_bars = int(cfg["signal_age_bars"])
    if age_bars > 0:
        cutoff_index = max(0, n - 1 - age_bars)
        ts_to_index = {int(ts[idx]): idx for idx in range(n)}
        signals = [s for s in signals if ts_to_index.get(s.ts, -1) >= cutoff_index]

    events = _filter_by_age_ts(events, ts, int(cfg.get("event_age_bars", 0)))
    return signals, events


_THREAD_LOCAL = threading.local()


def make_exchange(rate_limit: bool = True):
    if ccxt is None:
        raise RuntimeError("ccxt is required for live scanning. Install with: pip install ccxt")
    return ccxt.binance({"enableRateLimit": rate_limit, "options": {"defaultType": "future"}})


def get_usdt_perp_symbols(exchange) -> List[str]:
    markets = exchange.load_markets()
    symbols = [
        sym
        for sym, meta in markets.items()
        if meta.get("swap") and meta.get("quote") == "USDT" and meta.get("active", True)
    ]
    symbols.sort()
    return symbols


def fetch_ohlcv_safe(exchange, symbol: str, timeframe: str, limit: int) -> Optional[List[List[float]]]:
    try:
        return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception:
        return None


def fetch_tickers_safe(exchange) -> Dict[str, dict]:
    try:
        data = exchange.fetch_tickers()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _get_worker_exchange(rate_limit: bool):
    ex = getattr(_THREAD_LOCAL, "exchange", None)
    if ex is None:
        ex = make_exchange(rate_limit=rate_limit)
        _THREAD_LOCAL.exchange = ex
    return ex


def _worker_scan_symbol(symbol: str, config: Dict[str, object], fetch_limit: int) -> Tuple[List[Signal], List[ZoneEvent]]:
    ex = _get_worker_exchange(bool(config["rate_limit"]))
    delay_ms = float(config.get("request_delay_ms", 0))
    jitter_ms = float(config.get("request_jitter_ms", 0))
    if delay_ms > 0:
        sleep_ms = delay_ms + (random.random() * jitter_ms if jitter_ms > 0 else 0.0)
        time.sleep(max(0.0, sleep_ms) / 1000.0)

    ohlcv = fetch_ohlcv_safe(ex, symbol, str(config["timeframe"]), fetch_limit)
    if not ohlcv or len(ohlcv) < 10:
        return [], []

    daily_rise = _daily_rise_pct_from_ohlcv(ohlcv, str(config["timeframe"]))
    min_daily_rise = float(config.get("min_daily_rise_pct", 0.0))
    if daily_rise is not None and daily_rise < min_daily_rise:
        return [], []

    return analyze_symbol(symbol, ohlcv, config)


def fmt_ts(ms: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000))


def _print_event_line(event: ZoneEvent) -> None:
    color = _event_color(event.kind)
    print(f"{color}[EVT {fmt_ts(event.ts)}] {event.symbol:18s} {event.kind:12s} {event.side:4s} price={event.price:.6f} | {event.info}{ANSI_RESET}")


def _should_print_event(event: ZoneEvent, config: Dict[str, object]) -> bool:
    min_strength = float(config.get("min_zone_gap_strength_pct", 0.0))
    if event.strength_pct < min_strength:
        return False
    if event.kind == "zone_touched":
        return bool(config.get("print_touched", True))
    if event.kind == "zone_created":
        return bool(config.get("print_created", True))
    if event.kind == "zone_broken":
        return bool(config.get("print_broken", True))
    if event.kind == "zone_near":
        return bool(config.get("print_near", True))
    return True


def run_scan(config: Dict[str, object], once: bool = False) -> None:
    ex = make_exchange(rate_limit=bool(config["rate_limit"]))
    symbols = get_usdt_perp_symbols(ex)

    print(f"Loaded {len(symbols)} USDT-M perpetual symbols.")
    print(
        f"Timeframe={config['timeframe']}, lookback={config['lookback_candles']}, "
        f"filter={config['gap_filter_pct']}%, min_daily_rise={config['min_daily_rise_pct']}%, "
        f"workers={config['scan_workers']}, symbols={config['scan_symbols_count']}, "
        f"min_zone_gap={config['min_zone_gap_strength_pct']}%"
    )
    print("Scanning... (Ctrl+C to stop)\n")

    while True:
        all_signals: List[Signal] = []
        all_events: List[ZoneEvent] = []
        start_time = time.time()

        selected_symbols = list(symbols)

        if bool(config.get("use_ticker_daily_prefilter", True)):
            tickers = fetch_tickers_safe(ex)
            if tickers:
                min_daily_rise = float(config.get("min_daily_rise_pct", 0.0))
                ranked: List[Tuple[str, float]] = []
                for sym in selected_symbols:
                    t = tickers.get(sym)
                    if not t:
                        continue
                    pct = t.get("percentage")
                    if pct is None:
                        continue
                    try:
                        pct_f = float(pct)
                    except Exception:
                        continue
                    if pct_f >= min_daily_rise:
                        ranked.append((sym, pct_f))
                ranked.sort(key=lambda x: x[1], reverse=True)
                top_n = int(config.get("ticker_prefilter_top_n", 0))
                if top_n > 0:
                    ranked = ranked[:top_n]
                if ranked:
                    selected_symbols = [sym for sym, _ in ranked]

        max_symbols = int(config.get("scan_symbols_count", 0))
        if max_symbols > 0 and len(selected_symbols) > max_symbols:
            selected_symbols = selected_symbols[:max_symbols]

        fetch_limit = int(config["lookback_candles"]) + int(config.get("atr_warmup_extra", 0))
        workers = max(1, int(config.get("scan_workers", 1)))

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {
                pool.submit(_worker_scan_symbol, sym, config, fetch_limit): sym
                for sym in selected_symbols
            }
            for fut in concurrent.futures.as_completed(future_map):
                try:
                    sigs, events = fut.result()
                except Exception:
                    continue
                if sigs:
                    all_signals.extend(sigs)
                    for s in sigs:
                        print(f"[SIG {fmt_ts(s.ts)}] {s.symbol:18s} {s.kind:10s} price={s.price:.6f} | {s.info}")
                if events:
                    all_events.extend(events)
                    for e in events:
                        if _should_print_event(e, config):
                            _print_event_line(e)

        all_signals.sort(key=lambda s: s.ts, reverse=True)
        all_events.sort(key=lambda e: e.ts, reverse=True)

        print("=" * 100)
        print(
            f"Scan finished in {time.time() - start_time:.1f}s | scanned={len(selected_symbols)} | "
            f"signals={len(all_signals)} | events={len(all_events)} | {time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        if bool(config.get("print_top_zone_strength", True)):
            min_strength = float(config.get("min_zone_gap_strength_pct", 0.0))
            created = [e for e in all_events if e.kind == "zone_created" and e.strength_pct >= min_strength]
            top_bull = max((e for e in created if e.side == "bull"), key=lambda e: e.strength_pct, default=None)
            top_bear = max((e for e in created if e.side == "bear"), key=lambda e: e.strength_pct, default=None)
            if top_bull is not None:
                print(f"Top bull gap >= {min_strength:.2f}%: {top_bull.symbol} gap={top_bull.strength_pct:.3f}%")
            if top_bear is not None:
                print(f"Top bear gap >= {min_strength:.2f}%: {top_bear.symbol} gap={top_bear.strength_pct:.3f}%")

        if not all_signals:
            print("No signals.")
        if not all_events:
            print("No zone events.")
        print("=" * 100 + "\n")
        if once:
            break
        time.sleep(float(config["scan_interval_sec"]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FVG + OB signal scanner for Binance futures.")
    parser.add_argument("--timeframe", default=DEFAULT_CONFIG["timeframe"])
    parser.add_argument("--lookback", type=int, default=DEFAULT_CONFIG["lookback_candles"])
    parser.add_argument("--interval", type=float, default=DEFAULT_CONFIG["scan_interval_sec"])
    parser.add_argument("--symbols-count", type=int, default=DEFAULT_CONFIG["scan_symbols_count"])
    parser.add_argument("--min-zone-gap", type=float, default=DEFAULT_CONFIG["min_zone_gap_strength_pct"])
    parser.add_argument("--once", action="store_true", help="Run one scan iteration then exit.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = dict(DEFAULT_CONFIG)
    config["timeframe"] = args.timeframe
    config["lookback_candles"] = args.lookback
    config["scan_interval_sec"] = args.interval
    config["scan_symbols_count"] = args.symbols_count
    config["min_zone_gap_strength_pct"] = args.min_zone_gap
    run_scan(config, once=args.once)


if __name__ == "__main__":
    main()