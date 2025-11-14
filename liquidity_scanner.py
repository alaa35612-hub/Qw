"""Liquidity scanner replicating TradingView ICT Concepts [LuxAlgo] liquidity logic.

This module ports the buyside and sellside liquidity calculations together with the
order block logic from the Pine Script indicator into Python and augments them with a
CCXT-based Binance USDT-M futures scanner.
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
SHOW_ORDER_BLOCKS: bool = True
ORDER_BLOCK_LOOKBACK: int = 10
SHOW_LAST_BULLISH_OB: int = 1
SHOW_LAST_BEARISH_OB: int = 1
USE_BODY_FOR_OB: bool = True
PRESENT_LOOKBACK_BARS: int = 500
RUN_CONTINUOUSLY: bool = False
LOOP_DELAY_SECONDS: float = 60.0

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


@dataclass
class SwingPoint:
    price: float = math.nan
    index: int = -1
    crossed: bool = False

    def is_valid(self) -> bool:
        return not math.isnan(self.price) and self.index >= 0


@dataclass
class OrderBlock:
    side: str  # "bullish" or "bearish"
    price_top: float
    price_bottom: float
    origin_index: int
    origin_time: int
    created_index: int
    created_time: int
    breaker: bool = False
    break_index: Optional[int] = None
    break_time: Optional[int] = None

    def as_dict(self) -> dict:
        return {
            "side": self.side,
            "price_top": self.price_top,
            "price_bottom": self.price_bottom,
            "origin_index": self.origin_index,
            "origin_time": self.origin_time,
            "created_index": self.created_index,
            "created_time": self.created_time,
            "breaker": self.breaker,
            "break_index": self.break_index,
            "break_time": self.break_time,
        }


@dataclass
class OrderBlockEvent:
    event_type: str
    block: OrderBlock
    index: int
    time: int
    price: Optional[float] = None


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


class SwingState:
    """Mirror of the Pine Script swing detection helper used for order blocks."""

    __slots__ = ("length", "os", "top", "bottom")

    def __init__(self, length: int) -> None:
        self.length = length
        self.os = 0
        self.top = SwingPoint()
        self.bottom = SwingPoint()

    def update(
        self,
        highs: Sequence[float],
        lows: Sequence[float],
        closes: Sequence[float],
        index: int,
    ) -> Tuple[SwingPoint, SwingPoint, int]:
        start = max(0, index - self.length + 1)
        window_closes = closes[start : index + 1]
        if not window_closes:
            return self.top, self.bottom, self.os

        upper = max(window_closes)
        lower = min(window_closes)
        prev_os = self.os

        reference_index = index - self.length
        if reference_index >= 0:
            ref_high = highs[reference_index]
            ref_low = lows[reference_index]
            if ref_high > upper:
                self.os = 0
            elif ref_low < lower:
                self.os = 1

            if self.os == 0 and prev_os != 0:
                self.top = SwingPoint(price=ref_high, index=reference_index, crossed=False)
            if self.os == 1 and prev_os != 1:
                self.bottom = SwingPoint(price=ref_low, index=reference_index, crossed=False)

        return self.top, self.bottom, prev_os


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


def detect_order_blocks(
    bars: Sequence[OHLCVBar],
    length: int = ORDER_BLOCK_LOOKBACK,
    show_ob: bool = SHOW_ORDER_BLOCKS,
    show_bull: int = SHOW_LAST_BULLISH_OB,
    show_bear: int = SHOW_LAST_BEARISH_OB,
    use_body: bool = USE_BODY_FOR_OB,
    present_window: int = PRESENT_LOOKBACK_BARS,
) -> Tuple[List[OrderBlock], List[OrderBlock], List[OrderBlockEvent]]:
    """Replicate the Pine Script order block detection and maintenance logic."""

    bullish_blocks: List[OrderBlock] = []
    bearish_blocks: List[OrderBlock] = []
    events: List[OrderBlockEvent] = []

    if not bars or not show_ob:
        return bullish_blocks, bearish_blocks, events

    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    opens = [bar.open for bar in bars]
    closes = [bar.close for bar in bars]
    times = [bar.time for bar in bars]

    if use_body:
        max_series = [max(o, c) for o, c in zip(opens, closes)]
        min_series = [min(o, c) for o, c in zip(opens, closes)]
    else:
        max_series = highs[:]
        min_series = lows[:]

    swing_state = SwingState(length=length)
    total_bars = len(bars)

    for index, bar in enumerate(bars):
        top_swing, bottom_swing, _ = swing_state.update(highs, lows, closes, index)
        per = (total_bars - 1 - index) <= present_window

        bl_break_conf = 0
        br_break_conf = 0

        if show_ob and per:
            if top_swing.is_valid() and not top_swing.crossed and bar.close > top_swing.price:
                top_swing.crossed = True
                if index >= 1:
                    minima = max_series[index - 1]
                    maxima = min_series[index - 1]
                    loc_index = index - 1
                    if top_swing.index >= 0:
                        span = index - top_swing.index
                        for offset in range(1, span):
                            candidate_index = index - offset
                            if candidate_index < 0:
                                break
                            candidate_min = min_series[candidate_index]
                            new_min = min(minima, candidate_min)
                            if candidate_min <= minima:
                                minima = new_min
                                maxima = max_series[candidate_index]
                                loc_index = candidate_index
                            else:
                                minima = new_min
                    if loc_index >= 0:
                        block = OrderBlock(
                            side="bullish",
                            price_top=maxima,
                            price_bottom=minima,
                            origin_index=loc_index,
                            origin_time=times[loc_index],
                            created_index=index,
                            created_time=bar.time,
                        )
                        bullish_blocks.insert(0, block)
                        events.append(
                            OrderBlockEvent(
                                event_type="bullish_creation",
                                block=block,
                                index=index,
                                time=bar.time,
                                price=bar.close,
                            )
                        )

            if bullish_blocks:
                for blk_index in range(len(bullish_blocks) - 1, -1, -1):
                    block = bullish_blocks[blk_index]
                    if not block.breaker:
                        if min(bar.close, bar.open) < block.price_bottom:
                            block.breaker = True
                            block.break_index = index
                            block.break_time = bar.time
                            events.append(
                                OrderBlockEvent(
                                    event_type="bullish_break",
                                    block=block,
                                    index=index,
                                    time=bar.time,
                                    price=bar.close,
                                )
                            )
                    else:
                        if bar.close > block.price_top:
                            bullish_blocks.pop(blk_index)
                            continue
                        if (
                            blk_index < show_bull
                            and top_swing.is_valid()
                            and top_swing.price < block.price_top
                            and top_swing.price > block.price_bottom
                        ):
                            bl_break_conf = 1

        if show_ob and per:
            if bottom_swing.is_valid() and not bottom_swing.crossed and bar.close < bottom_swing.price:
                bottom_swing.crossed = True
                if index >= 1:
                    maxima = max_series[index - 1]
                    minima = min_series[index - 1]
                    loc_index = index - 1
                    if bottom_swing.index >= 0:
                        span = index - bottom_swing.index
                        for offset in range(1, span):
                            candidate_index = index - offset
                            if candidate_index < 0:
                                break
                            candidate_max = max_series[candidate_index]
                            new_max = max(maxima, candidate_max)
                            if candidate_max >= maxima:
                                maxima = new_max
                                minima = min_series[candidate_index]
                                loc_index = candidate_index
                            else:
                                maxima = new_max
                    if loc_index >= 0:
                        block = OrderBlock(
                            side="bearish",
                            price_top=maxima,
                            price_bottom=minima,
                            origin_index=loc_index,
                            origin_time=times[loc_index],
                            created_index=index,
                            created_time=bar.time,
                        )
                        bearish_blocks.insert(0, block)
                        events.append(
                            OrderBlockEvent(
                                event_type="bearish_creation",
                                block=block,
                                index=index,
                                time=bar.time,
                                price=bar.close,
                            )
                        )

            if bearish_blocks:
                for blk_index in range(len(bearish_blocks) - 1, -1, -1):
                    block = bearish_blocks[blk_index]
                    if not block.breaker:
                        if max(bar.close, bar.open) > block.price_top:
                            block.breaker = True
                            block.break_index = index
                            block.break_time = bar.time
                            events.append(
                                OrderBlockEvent(
                                    event_type="bearish_break",
                                    block=block,
                                    index=index,
                                    time=bar.time,
                                    price=bar.close,
                                )
                            )
                    else:
                        if bar.close < block.price_bottom:
                            bearish_blocks.pop(blk_index)
                            continue
                        if (
                            blk_index < show_bear
                            and bottom_swing.is_valid()
                            and bottom_swing.price > block.price_bottom
                            and bottom_swing.price < block.price_top
                        ):
                            br_break_conf = 1

    return bullish_blocks, bearish_blocks, events


# ---------------------------------------------------------------------------
# Order block touch detection
# ---------------------------------------------------------------------------
def find_order_block_touches(
    bars: Sequence[OHLCVBar],
    bullish_blocks: Sequence[OrderBlock],
    bearish_blocks: Sequence[OrderBlock],
    tolerance: float = PRICE_TOLERANCE,
    max_age: int = MAX_ALERT_AGE_BARS,
) -> List[OrderBlockEvent]:
    """Detect order block touches for the latest bar."""

    if not bars:
        return []

    last_index = len(bars) - 1
    latest_bar = bars[last_index]
    prices_to_check = [latest_bar.high, latest_bar.low, latest_bar.close]

    def _recent_enough(block: OrderBlock) -> bool:
        return (last_index - block.origin_index) <= max_age

    events: List[OrderBlockEvent] = []
    for block in list(bullish_blocks) + list(bearish_blocks):
        if not _recent_enough(block):
            continue
        lower = min(block.price_bottom, block.price_top)
        upper = max(block.price_bottom, block.price_top)
        for price in prices_to_check:
            if _within_tolerance(price, lower, upper, tolerance):
                events.append(
                    OrderBlockEvent(
                        event_type="order_block_touch",
                        block=block,
                        index=last_index,
                        time=latest_bar.time,
                        price=price,
                    )
                )
                break

    return events


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


def run_single_scan(exchange: ccxt.Exchange, symbols: Sequence[str]) -> None:
    """Scan the provided symbols once and print informational messages."""

    print(
        f"[INFO] Scanning {len(symbols)} Binance USDT-M futures symbols on timeframe {TIMEFRAME}"
    )

    for symbol in symbols:
        market = exchange.markets[symbol]
        if not market.get("active", True):
            print(f"[SKIP] SYMBOL={symbol} is inactive; skipping.")
            continue
        print(f"[INFO] Fetching OHLCV for {symbol}...")
        try:
            bars = fetch_ohlcv(exchange, symbol, timeframe=TIMEFRAME, limit=MAX_BARS_LOOKBACK)
        except Exception as exc:  # noqa: BLE001 - log and continue scanning
            print(f"[ERROR] Failed to fetch OHLCV for {symbol}: {exc}")
            time.sleep(exchange.rateLimit / 1000.0)
            continue

        if not bars:
            print(f"[WARN] SYMBOL={symbol} returned no candles; skipping.")
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
        bullish_obs, bearish_obs, ob_events = detect_order_blocks(
            bars,
            length=ORDER_BLOCK_LOOKBACK,
            show_ob=SHOW_ORDER_BLOCKS,
            show_bull=SHOW_LAST_BULLISH_OB,
            show_bear=SHOW_LAST_BEARISH_OB,
            use_body=USE_BODY_FOR_OB,
            present_window=PRESENT_LOOKBACK_BARS,
        )

        liquidity_touches = find_liquidity_touches(bars, buyside, sellside)
        ob_touch_events = find_order_block_touches(
            bars,
            bullish_obs,
            bearish_obs,
            tolerance=PRICE_TOLERANCE,
            max_age=MAX_ALERT_AGE_BARS,
        )

        latest_index = len(bars) - 1
        if latest_index < 0:
            print(f"[WARN] SYMBOL={symbol} has insufficient data; skipping.")
            continue

        last_time = bars[latest_index].time
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(last_time / 1000))
        alerts_emitted = False

        for box, touch_price in liquidity_touches:
            side_label = "Buyside" if box.side == "buyside" else "Sellside"
            price_range = f"[{box.price_bottom:.6f}, {box.price_top:.6f}]"
            print(
                f"[ALERT] SYMBOL={symbol}, EVENT=LiquidityTouch, SIDE={side_label}, TF={TIMEFRAME}, "
                f"TOUCH_PRICE={touch_price:.6f}, RANGE={price_range}, BAR_TIME={timestamp}"
            )
            alerts_emitted = True

        for event in ob_events:
            if event.index != latest_index:
                continue
            block = event.block
            price_range = f"[{block.price_bottom:.6f}, {block.price_top:.6f}]"
            side_label = "Bullish" if block.side == "bullish" else "Bearish"
            if event.event_type in {"bullish_creation", "bearish_creation"}:
                event_name = "OrderBlockCreated"
            elif event.event_type == "bullish_break":
                event_name = "BullishBreak"
            elif event.event_type == "bearish_break":
                event_name = "BearishBreak"
            else:
                event_name = event.event_type
            price_value = event.price if event.price is not None else bars[latest_index].close
            print(
                f"[ALERT] SYMBOL={symbol}, EVENT={event_name}, SIDE={side_label}, TF={TIMEFRAME}, "
                f"PRICE_RANGE={price_range}, EVENT_PRICE={price_value:.6f}, BAR_TIME={timestamp}"
            )
            alerts_emitted = True

        for event in ob_touch_events:
            block = event.block
            price_range = f"[{block.price_bottom:.6f}, {block.price_top:.6f}]"
            side_label = "Bullish" if block.side == "bullish" else "Bearish"
            touch_price = event.price if event.price is not None else bars[latest_index].close
            print(
                f"[ALERT] SYMBOL={symbol}, EVENT=OrderBlockTouch, SIDE={side_label}, TF={TIMEFRAME}, "
                f"TOUCH_PRICE={touch_price:.6f}, RANGE={price_range}, BAR_TIME={timestamp}"
            )
            alerts_emitted = True

        if not alerts_emitted:
            print(f"[INFO] SYMBOL={symbol} produced no alerts on the latest bar.")

        time.sleep(exchange.rateLimit / 1000.0)


def run_scanner() -> None:
    """Main entry point: optionally run the scanner continuously."""

    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    exchange.load_markets()
    markets = exchange.markets
    symbols = []
    for symbol, market in markets.items():
        if market.get("quote") != "USDT":
            continue
        contract_type = market.get("info", {}).get("contractType")
        if contract_type != "PERPETUAL":
            continue
        if market.get("darkpool", False):
            continue
        symbols.append(symbol)

    if not symbols:
        print("[WARN] No Binance USDT-M futures symbols found to scan.")
        return

    try:
        if RUN_CONTINUOUSLY:
            print("[INFO] Continuous scanning enabled. Press Ctrl+C to stop.")
            while True:
                run_single_scan(exchange, symbols)
                print(
                    f"[INFO] Sleeping for {LOOP_DELAY_SECONDS} seconds before the next scan cycle."
                )
                time.sleep(max(0.0, LOOP_DELAY_SECONDS))
        else:
            run_single_scan(exchange, symbols)
    except KeyboardInterrupt:
        print("[INFO] Scanner interrupted by user. Exiting cleanly.")


if __name__ == "__main__":
    run_scanner()
