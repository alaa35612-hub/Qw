"""Liquidity scanner replicating TradingView ICT Concepts [LuxAlgo] liquidity logic.

This module ports the buyside and sellside liquidity calculations from the Pine Script
indicator into Python and augments them with a CCXT-based Binance USDT-M futures scanner.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import ccxt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MARGIN: float = 4.0
VIS_LIQ: int = 2
ATR_PERIOD: int = 10
TIMEFRAME: str = "15m"
MAX_BARS_LOOKBACK: int = 1000
MAX_ALERT_AGE_BARS: int = 50
PRICE_TOLERANCE: float = 0.0001
PIVOT_LEFT: int = 5
PIVOT_RIGHT: int = 1  # The original script uses a right span of 1 bar.
MAX_ZIGZAG_SIZE: int = 50

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class OHLCVBar:
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class LiquidityBox:
    side: str  # "buyside" or "sellside"
    price_top: float
    price_bottom: float
    start_index: int
    start_time: int
    right_index: int
    line_start_index: int
    line_end_index: int
    reference_price: float
    broken_top: bool = False
    broken_bottom: bool = False
    broken: bool = False
    filled: bool = False
    line_active: bool = True
    last_updated_index: int = 0

    def as_dict(self) -> dict:
        """Return a serialisable representation of the box."""
        return {
            "side": self.side,
            "price_top": self.price_top,
            "price_bottom": self.price_bottom,
            "start_index": self.start_index,
            "start_time": self.start_time,
            "right_index": self.right_index,
            "line_start_index": self.line_start_index,
            "line_end_index": self.line_end_index,
            "reference_price": self.reference_price,
            "broken_top": self.broken_top,
            "broken_bottom": self.broken_bottom,
            "broken": self.broken,
            "filled": self.filled,
            "line_active": self.line_active,
            "last_updated_index": self.last_updated_index,
        }


class ZigZagState:
    """Mirror of the Pine Script `ZZ` user defined type."""

    __slots__ = ("max_size", "directions", "indices", "prices", "flags")

    def __init__(self, max_size: int = MAX_ZIGZAG_SIZE) -> None:
        self.max_size = max_size
        self.directions: List[int] = [0] * max_size
        self.indices: List[int] = [0] * max_size
        self.prices: List[float] = [math.nan] * max_size
        self.flags: List[bool] = [False] * max_size

    # Mimic the Pine Script method behaviour ---------------------------------
    def in_out(self, direction: int, x2: int, y2: float, flag: bool = True) -> None:
        self.directions.insert(0, direction)
        self.indices.insert(0, x2)
        self.prices.insert(0, y2)
        self.flags.insert(0, flag)
        del self.directions[self.max_size :]
        del self.indices[self.max_size :]
        del self.prices[self.max_size :]
        del self.flags[self.max_size :]

    def update_latest(self, x2: int, y2: float) -> None:
        self.indices[0] = x2
        self.prices[0] = y2

    def size(self) -> int:
        return len(self.directions)

    def get_direction(self, index: int) -> int:
        return self.directions[index]

    def get_price(self, index: int) -> float:
        return self.prices[index]

    def get_index(self, index: int) -> int:
        return self.indices[index]


# ---------------------------------------------------------------------------
# Helper calculations
# ---------------------------------------------------------------------------
def compute_atr(bars: Sequence[OHLCVBar], period: int = ATR_PERIOD) -> List[Optional[float]]:
    """Compute the Average True Range using a simple moving average, as in Pine."""
    atr_values: List[Optional[float]] = [None] * len(bars)
    tr_window: List[float] = []

    for i, bar in enumerate(bars):
        if i == 0:
            true_range = bar.high - bar.low
        else:
            prev_close = bars[i - 1].close
            true_range = max(
                bar.high - bar.low,
                abs(bar.high - prev_close),
                abs(bar.low - prev_close),
            )
        tr_window.append(true_range)
        if len(tr_window) > period:
            del tr_window[0]
        if len(tr_window) == period:
            atr_values[i] = sum(tr_window) / period
    return atr_values


def _pivot_extreme(
    values: Sequence[float],
    current_index: int,
    left: int,
    right: int,
    is_high: bool,
) -> Tuple[Optional[float], Optional[int]]:
    """Generic pivot helper replicating `ta.pivothigh/low` with right span of 1."""
    pivot_index = current_index - right
    if pivot_index < left:
        return None, None
    if current_index >= len(values):
        return None, None
    if pivot_index + right >= len(values):
        return None, None

    pivot_value = values[pivot_index]
    if math.isnan(pivot_value):
        return None, None

    start = pivot_index - left
    end = pivot_index + right
    for idx in range(start, end + 1):
        if idx == pivot_index:
            continue
        candidate = values[idx]
        if math.isnan(candidate):
            return None, None
        if is_high:
            if candidate >= pivot_value:
                return None, None
        else:
            if candidate <= pivot_value:
                return None, None
    return pivot_value, pivot_index


def pivot_high(values: Sequence[float], current_index: int, left: int, right: int) -> Tuple[Optional[float], Optional[int]]:
    return _pivot_extreme(values, current_index, left, right, is_high=True)


def pivot_low(values: Sequence[float], current_index: int, left: int, right: int) -> Tuple[Optional[float], Optional[int]]:
    return _pivot_extreme(values, current_index, left, right, is_high=False)


def _within_tolerance(value: float, lower: float, upper: float, tolerance: float) -> bool:
    return (lower - tolerance) <= value <= (upper + tolerance)


# ---------------------------------------------------------------------------
# Liquidity detection logic
# ---------------------------------------------------------------------------
def detect_liquidity_zones(
    bars: Sequence[OHLCVBar],
    margin: float = MARGIN,
    max_visible: int = VIS_LIQ,
    show_liquidity: bool = True,
    atr_period: int = ATR_PERIOD,
    pivot_left: int = PIVOT_LEFT,
    pivot_right: int = PIVOT_RIGHT,
) -> Tuple[List[LiquidityBox], List[LiquidityBox]]:
    """Replicate the Pine Script liquidity box creation and maintenance logic."""
    if not bars:
        return [], []

    zigzag = ZigZagState(max_size=MAX_ZIGZAG_SIZE)
    atr_values = compute_atr(bars, period=atr_period)
    buyside_boxes: List[LiquidityBox] = []
    sellside_boxes: List[LiquidityBox] = []

    a_value = 10.0 / margin

    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]

    for index, bar in enumerate(bars):
        atr_val = atr_values[index]
        span = atr_val / a_value if atr_val is not None else None

        # Pivot highs --------------------------------------------------------
        ph_value, ph_index = pivot_high(highs, index, pivot_left, pivot_right)
        if ph_value is not None and ph_index is not None:
            pivot_price = highs[ph_index]
            zigzag_direction = zigzag.get_direction(0)
            if zigzag_direction < 1:
                zigzag.in_out(1, ph_index, pivot_price)
            elif zigzag_direction == 1 and pivot_price > zigzag.get_price(0):
                zigzag.update_latest(ph_index, pivot_price)

            if show_liquidity and span is not None:
                count = 0
                start_index = None
                start_price = None
                min_price = 0.0
                max_price = 10e6
                size_limit = min(zigzag.size(), MAX_ZIGZAG_SIZE)
                for offset in range(size_limit):
                    if zigzag.get_direction(offset) != 1:
                        continue
                    price = zigzag.get_price(offset)
                    if math.isnan(price):
                        continue
                    if price > pivot_price + span:
                        break
                    if pivot_price - span < price < pivot_price + span:
                        count += 1
                        start_index = zigzag.get_index(offset)
                        start_price = price
                        if price > min_price:
                            min_price = price
                        if price < max_price:
                            max_price = price
                if count > 2 and start_index is not None and start_price is not None:
                    midpoint = (min_price + max_price) / 2.0
                    top = midpoint + span
                    bottom = midpoint - span
                    right_index = index + 10
                    if buyside_boxes and buyside_boxes[0].start_index == start_index:
                        box = buyside_boxes[0]
                        box.price_top = top
                        box.price_bottom = bottom
                        box.right_index = right_index
                        box.last_updated_index = index
                    else:
                        start_time = bars[start_index].time if 0 <= start_index < len(bars) else bars[index].time
                        new_box = LiquidityBox(
                            side="buyside",
                            price_top=top,
                            price_bottom=bottom,
                            start_index=start_index,
                            start_time=start_time,
                            right_index=right_index,
                            line_start_index=start_index,
                            line_end_index=index - 1,
                            reference_price=start_price,
                            last_updated_index=index,
                        )
                        buyside_boxes.insert(0, new_box)
                        if len(buyside_boxes) > max_visible:
                            buyside_boxes.pop()

        # Pivot lows ---------------------------------------------------------
        pl_value, pl_index = pivot_low(lows, index, pivot_left, pivot_right)
        if pl_value is not None and pl_index is not None:
            pivot_price = lows[pl_index]
            zigzag_direction = zigzag.get_direction(0)
            if zigzag_direction > -1:
                zigzag.in_out(-1, pl_index, pivot_price)
            elif zigzag_direction == -1 and pivot_price < zigzag.get_price(0):
                zigzag.update_latest(pl_index, pivot_price)

            if show_liquidity and span is not None:
                count = 0
                start_index = None
                start_price = None
                min_price = 0.0
                max_price = 10e6
                size_limit = min(zigzag.size(), MAX_ZIGZAG_SIZE)
                for offset in range(size_limit):
                    if zigzag.get_direction(offset) != -1:
                        continue
                    price = zigzag.get_price(offset)
                    if math.isnan(price):
                        continue
                    if price < pivot_price - span:
                        break
                    if pivot_price - span < price < pivot_price + span:
                        count += 1
                        start_index = zigzag.get_index(offset)
                        start_price = price
                        if price > min_price:
                            min_price = price
                        if price < max_price:
                            max_price = price
                if count > 2 and start_index is not None and start_price is not None:
                    midpoint = (min_price + max_price) / 2.0
                    top = midpoint + span
                    bottom = midpoint - span
                    right_index = index + 10
                    if sellside_boxes and sellside_boxes[0].start_index == start_index:
                        box = sellside_boxes[0]
                        box.price_top = top
                        box.price_bottom = bottom
                        box.right_index = right_index
                        box.last_updated_index = index
                    else:
                        start_time = bars[start_index].time if 0 <= start_index < len(bars) else bars[index].time
                        new_box = LiquidityBox(
                            side="sellside",
                            price_top=top,
                            price_bottom=bottom,
                            start_index=start_index,
                            start_time=start_time,
                            right_index=right_index,
                            line_start_index=start_index,
                            line_end_index=index - 1,
                            reference_price=start_price,
                            last_updated_index=index,
                        )
                        sellside_boxes.insert(0, new_box)
                        if len(sellside_boxes) > max_visible:
                            sellside_boxes.pop()

        # Update existing boxes each bar ------------------------------------
        for box in buyside_boxes:
            if box.broken:
                continue
            box.right_index = index + 3
            box.line_end_index = index + 3
            if not box.broken_top and bar.close > box.price_top:
                box.broken_top = True
            if not box.broken_bottom and bar.close > box.price_bottom:
                box.broken_bottom = True
            if box.broken_bottom and not box.filled:
                box.filled = True
                box.line_active = False
            if box.broken_top and box.broken_bottom:
                box.broken = True
                box.right_index = index
            box.last_updated_index = index

        for box in sellside_boxes:
            if box.broken:
                continue
            box.right_index = index + 3
            box.line_end_index = index + 3
            if not box.broken_top and bar.close < box.price_top:
                box.broken_top = True
            if not box.broken_bottom and bar.close < box.price_bottom:
                box.broken_bottom = True
            if box.broken_top and not box.filled:
                box.filled = True
                box.line_active = False
            if box.broken_top and box.broken_bottom:
                box.broken = True
                box.right_index = index
            box.last_updated_index = index

    return buyside_boxes, sellside_boxes

# ---------------------------------------------------------------------------
# Scanner logic
# ---------------------------------------------------------------------------
def fetch_ohlcv(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str = TIMEFRAME,
    limit: int = MAX_BARS_LOOKBACK,
) -> List[OHLCVBar]:
    """Fetch OHLCV data and convert it into OHLCVBar instances."""
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    bars = [
        OHLCVBar(
            time=int(item[0]),
            open=float(item[1]),
            high=float(item[2]),
            low=float(item[3]),
            close=float(item[4]),
            volume=float(item[5]),
        )
        for item in raw
    ]
    return bars


def find_liquidity_touches(
    bars: Sequence[OHLCVBar],
    buyside: Sequence[LiquidityBox],
    sellside: Sequence[LiquidityBox],
    tolerance: float = PRICE_TOLERANCE,
    max_age: int = MAX_ALERT_AGE_BARS,
) -> List[Tuple[LiquidityBox, float]]:
    """Return the liquidity boxes touched by the latest bar along with the touch price."""
    if not bars:
        return []
    last_index = len(bars) - 1
    latest_bar = bars[last_index]
    touch_prices = [latest_bar.high, latest_bar.low, latest_bar.close]

    touches: List[Tuple[LiquidityBox, float]] = []

    def _recent_enough(box: LiquidityBox) -> bool:
        return (last_index - box.start_index) <= max_age

    for box in list(buyside) + list(sellside):
        if box.broken:
            continue
        if not _recent_enough(box):
            continue
        lower = min(box.price_bottom, box.price_top)
        upper = max(box.price_bottom, box.price_top)
        for price in touch_prices:
            if _within_tolerance(price, lower, upper, tolerance):
                touches.append((box, price))
                break
    return touches


def run_scanner() -> None:
    """Main entry point: scan Binance USDT-M futures and emit alerts."""
    exchange = ccxt.binanceusdm()
    exchange.load_markets()
    markets = exchange.markets
    symbols = [
        symbol
        for symbol, info in markets.items()
        if info.get("quote") == "USDT" and info.get("contractType") == "PERPETUAL" and not info.get("darkpool", False)
    ]

    for symbol in symbols:
        market = markets[symbol]
        if not market.get("active", True):
            continue
        try:
            bars = fetch_ohlcv(exchange, symbol, timeframe=TIMEFRAME, limit=MAX_BARS_LOOKBACK)
        except Exception as exc:  # noqa: BLE001 - log and continue scanning
            print(f"[ERROR] Failed to fetch OHLCV for {symbol}: {exc}")
            time.sleep(exchange.rateLimit / 1000.0)
            continue

        buyside, sellside = detect_liquidity_zones(
            bars,
            margin=MARGIN,
            max_visible=VIS_LIQ,
            show_liquidity=True,
            atr_period=ATR_PERIOD,
            pivot_left=PIVOT_LEFT,
            pivot_right=PIVOT_RIGHT,
        )
        touches = find_liquidity_touches(bars, buyside, sellside)
        if not touches:
            continue

        last_time = bars[-1].time
        for box, touch_price in touches:
            side_label = "Buyside" if box.side == "buyside" else "Sellside"
            price_range = f"[{box.price_bottom:.6f}, {box.price_top:.6f}]"
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(last_time / 1000))
            print(
                f"[ALERT] SYMBOL={symbol}, SIDE={side_label}, TF={TIMEFRAME}, "
                f"TOUCH_PRICE={touch_price:.6f}, RANGE={price_range}, BAR_TIME={timestamp}"
            )
        time.sleep(exchange.rateLimit / 1000.0)


if __name__ == "__main__":
    run_scanner()
