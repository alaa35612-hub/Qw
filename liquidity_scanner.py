"""Liquidity scanner replicating TradingView ICT Concepts [LuxAlgo] liquidity logic.

This module ports the buyside and sellside liquidity calculations together with the
order block logic from the Pine Script indicator into Python and augments them with a
CCXT-based Binance USDT-M futures scanner.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import ccxt
try:
    import requests
except Exception:  # pragma: no cover - optional dependency for Telegram alerts
    requests = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MARGIN: float = 4.0
VIS_LIQ: int = 2
ATR_PERIOD: int = 10
TIMEFRAME: str = "15m"
BASE_TIMEFRAME: str = "15m"
ADAPT_TO_TIMEFRAME: bool = True
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
TELEGRAM_TOKEN: str = ""
TELEGRAM_CHAT_ID: str = ""
TELEGRAM_REQUEST_TIMEOUT: int = 10
LIQUIDITY_BUFFER: float = 10.0
ENABLE_HIGHER_TIMEFRAME_LIQUIDITY: bool = True
HIGHER_TIMEFRAME_LEVELS: Tuple[str, ...] = ("1d", "1w")
HIGHER_TIMEFRAME_LOOKBACK: int = 3

BASE_PIVOT_LEFT: int = PIVOT_LEFT
BASE_PIVOT_RIGHT: int = PIVOT_RIGHT
BASE_ORDER_BLOCK_LOOKBACK: int = ORDER_BLOCK_LOOKBACK
BASE_PRESENT_LOOKBACK_BARS: int = PRESENT_LOOKBACK_BARS
BASE_MAX_BARS_LOOKBACK: int = MAX_BARS_LOOKBACK

ANSI_RESET = "\033[0m"
EVENT_COLOR_MAP = {
    "BuysideLiquidity": "\033[96m",
    "SellsideLiquidity": "\033[94m",
    "BuysideLiquidityTouch": "\033[36m",
    "SellsideLiquidityTouch": "\033[34m",
    "Bullish OB": "\033[32m",
    "Bearish OB": "\033[31m",
    "Bullish Break": "\033[92m",
    "Bearish Break": "\033[95m",
    "Bullish Break Confirmed": "\033[92m",
    "Bearish Break Confirmed": "\033[95m",
    "BullishOBTouch": "\033[32m",
    "BearishOBTouch": "\033[31m",
}

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
    source_timeframe: Optional[str] = None
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
            "source_timeframe": self.source_timeframe,
            "broken_top": self.broken_top,
            "broken_bottom": self.broken_bottom,
            "broken": self.broken,
            "filled": self.filled,
            "line_active": self.line_active,
            "last_updated_index": self.last_updated_index,
        }


@dataclass
class LiquidityEvent:
    event_type: str
    box: LiquidityBox
    index: int
    time: int


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
    block_type: str = "breaker"
    breaker: bool = False
    break_index: Optional[int] = None
    break_time: Optional[int] = None
    breaker_confirmed: bool = False
    confirmation_index: Optional[int] = None
    confirmation_time: Optional[int] = None

    def as_dict(self) -> dict:
        return {
            "side": self.side,
            "price_top": self.price_top,
            "price_bottom": self.price_bottom,
            "origin_index": self.origin_index,
            "origin_time": self.origin_time,
            "created_index": self.created_index,
            "created_time": self.created_time,
            "block_type": self.block_type,
            "breaker": self.breaker,
            "break_index": self.break_index,
            "break_time": self.break_time,
            "breaker_confirmed": self.breaker_confirmed,
            "confirmation_index": self.confirmation_index,
            "confirmation_time": self.confirmation_time,
        }


@dataclass
class OrderBlockEvent:
    event_type: str
    block: OrderBlock
    index: int
    time: int
    price: Optional[float] = None


LIQUIDITY_CREATION_ALERTS: Dict[Tuple[str, str, int, Optional[str]], int] = {}
LIQUIDITY_TOUCH_ALERTS: Dict[Tuple[str, str, int, Optional[str]], int] = {}
ORDERBLOCK_CREATION_ALERTS: Dict[Tuple[str, str, int, int], int] = {}
ORDERBLOCK_BREAK_ALERTS: Dict[Tuple[str, str, int, int], int] = {}
ORDERBLOCK_TOUCH_ALERTS: Dict[Tuple[str, str, int, int], int] = {}
ORDERBLOCK_CONFIRM_ALERTS: Dict[Tuple[str, str, int, int], int] = {}
_telegram_warning_logged = False


def _colorize(message: str, event_label: str) -> str:
    color = EVENT_COLOR_MAP.get(event_label)
    if not color:
        return message
    return f"{color}{message}{ANSI_RESET}"


def send_telegram_alert(message: str) -> None:
    global _telegram_warning_logged
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if requests is None:
        if not _telegram_warning_logged:
            print("[WARN] requests library not available; Telegram alerts disabled.")
            _telegram_warning_logged = True
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        response = requests.post(url, data=payload, timeout=TELEGRAM_REQUEST_TIMEOUT)
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - best-effort notification
        if not _telegram_warning_logged:
            print(f"[WARN] Failed to send Telegram alert: {exc}")
            _telegram_warning_logged = True


def emit_alert(event_label: str, message: str) -> None:
    console_message = _colorize(message, event_label)
    print(console_message)
    send_telegram_alert(message)


def prune_history_for_symbol(
    history: Dict[Tuple, int], symbol: str, latest_index: int, max_age: int
) -> None:
    stale_keys = [
        key for key, idx in history.items() if key[0] == symbol and latest_index - idx > max_age
    ]
    for key in stale_keys:
        del history[key]


def should_emit(
    history: Dict[Tuple, int], key: Tuple, event_index: int, latest_index: int, max_age: int
) -> bool:
    if latest_index - event_index > max_age:
        return False
    previous = history.get(key)
    if previous is not None and previous == event_index:
        return False
    history[key] = event_index
    return True


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

    __slots__ = ("length", "os", "top", "bottom", "prev_top", "prev_bottom")

    def __init__(self, length: int) -> None:
        self.length = length
        self.os = 0
        self.top = SwingPoint()
        self.bottom = SwingPoint()
        self.prev_top = SwingPoint()
        self.prev_bottom = SwingPoint()

    def update(
        self,
        highs: Sequence[float],
        lows: Sequence[float],
        closes: Sequence[float],
        index: int,
    ) -> Tuple[SwingPoint, SwingPoint, int]:
        prev_os = self.os
        length = self.length
        if length <= 0:
            return self.top, self.bottom, prev_os

        reference_index = index - length
        if reference_index < 0 or index >= len(highs) or index >= len(lows):
            return self.top, self.bottom, prev_os

        window_start = reference_index + 1
        window_end = index + 1
        if window_start < 0:
            window_start = 0

        window_highs = highs[window_start:window_end]
        window_lows = lows[window_start:window_end]
        if not window_highs or not window_lows:
            return self.top, self.bottom, prev_os

        upper = max(window_highs)
        lower = min(window_lows)

        ref_high = highs[reference_index]
        ref_low = lows[reference_index]

        if not math.isnan(ref_high) and ref_high > upper:
            self.os = 0
        elif not math.isnan(ref_low) and ref_low < lower:
            self.os = 1

        if self.os == 0 and prev_os != 0 and not math.isnan(ref_high):
            self.prev_top = self.top
            self.top = SwingPoint(price=ref_high, index=reference_index, crossed=False)
        if self.os == 1 and prev_os != 1 and not math.isnan(ref_low):
            self.prev_bottom = self.bottom
            self.bottom = SwingPoint(price=ref_low, index=reference_index, crossed=False)

        return self.top, self.bottom, prev_os


# ---------------------------------------------------------------------------
# Helper calculations
# ---------------------------------------------------------------------------
def compute_atr(bars: Sequence[OHLCVBar], period: int = ATR_PERIOD) -> List[Optional[float]]:
    """Replicate Pine Script's ``ta.atr`` (RMA based) calculation."""

    atr_values: List[Optional[float]] = [None] * len(bars)
    prev_atr: Optional[float] = None

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

        if prev_atr is None:
            prev_atr = true_range
        else:
            prev_atr = ((period - 1) * prev_atr + true_range) / period

        if i >= period - 1:
            atr_values[i] = prev_atr

    return atr_values


def timeframe_to_minutes(timeframe: str) -> float:
    """Convert a timeframe string like '15m' or '1h' into minutes."""

    if not timeframe:
        return 0.0
    unit = timeframe[-1].lower()
    try:
        value = float(timeframe[:-1])
    except ValueError:
        return 0.0

    if unit == "m":
        return value
    if unit == "h":
        return value * 60.0
    if unit == "d":
        return value * 1440.0
    if unit == "w":
        return value * 10080.0
    return 0.0


def derive_timeframe_parameters(timeframe: str) -> Dict[str, int]:
    """Derive adaptive parameters that scale with the requested timeframe."""

    base_params = {
        "pivot_left": PIVOT_LEFT,
        "pivot_right": PIVOT_RIGHT,
        "order_block_lookback": ORDER_BLOCK_LOOKBACK,
        "present_window": PRESENT_LOOKBACK_BARS,
        "max_bars": MAX_BARS_LOOKBACK,
        "show_bull": SHOW_LAST_BULLISH_OB,
        "show_bear": SHOW_LAST_BEARISH_OB,
    }

    if not ADAPT_TO_TIMEFRAME:
        return base_params

    base_minutes = timeframe_to_minutes(BASE_TIMEFRAME) or timeframe_to_minutes(TIMEFRAME) or 15.0
    current_minutes = timeframe_to_minutes(timeframe) or base_minutes
    scale = current_minutes / base_minutes if base_minutes else 1.0
    scale = max(0.25, scale)
    sqrt_scale = math.sqrt(scale)

    pivot_left = max(2, int(round(BASE_PIVOT_LEFT * sqrt_scale)))
    pivot_right = max(1, int(round(BASE_PIVOT_RIGHT * sqrt_scale)))
    order_block_lookback = max(3, int(round(BASE_ORDER_BLOCK_LOOKBACK * sqrt_scale)))
    present_window = max(100, int(round(BASE_PRESENT_LOOKBACK_BARS * scale)))
    max_bars = max(200, int(round(BASE_MAX_BARS_LOOKBACK * scale)))

    return {
        "pivot_left": pivot_left,
        "pivot_right": pivot_right,
        "order_block_lookback": order_block_lookback,
        "present_window": present_window,
        "max_bars": max_bars,
        "show_bull": SHOW_LAST_BULLISH_OB,
        "show_bear": SHOW_LAST_BEARISH_OB,
    }


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
    present_window: int = PRESENT_LOOKBACK_BARS,
    liquidity_buffer: float = LIQUIDITY_BUFFER,
    current_timeframe: str = TIMEFRAME,
) -> Tuple[List[LiquidityBox], List[LiquidityBox], List[LiquidityEvent]]:
    """Replicate the Pine Script liquidity box creation and maintenance logic."""
    if not bars:
        return [], [], []

    zigzag = ZigZagState(max_size=MAX_ZIGZAG_SIZE)
    atr_values = compute_atr(bars, period=atr_period)
    buyside_boxes: List[LiquidityBox] = []
    sellside_boxes: List[LiquidityBox] = []
    liquidity_events: List[LiquidityEvent] = []

    a_value = 10.0 / margin

    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]

    last_index = len(bars) - 1

    for index, bar in enumerate(bars):
        atr_val = atr_values[index]
        span = atr_val / a_value if atr_val is not None else None
        per = (last_index - index) <= present_window

        # Pivot highs --------------------------------------------------------
        ph_value, ph_index = pivot_high(highs, index, pivot_left, pivot_right)
        if ph_value is not None and ph_index is not None:
            pivot_price = highs[ph_index]
            zigzag_direction = zigzag.get_direction(0)
            if zigzag_direction < 1:
                zigzag.in_out(1, ph_index, pivot_price)
            elif zigzag_direction == 1 and pivot_price > zigzag.get_price(0):
                zigzag.update_latest(ph_index, pivot_price)

            if show_liquidity and per and span is not None:
                cluster_prices: List[float] = []
                start_index = None
                size_limit = min(zigzag.size(), MAX_ZIGZAG_SIZE)
                for offset in range(size_limit):
                    if zigzag.get_direction(offset) != 1:
                        continue
                    price = zigzag.get_price(offset)
                    if math.isnan(price):
                        continue
                    if price > pivot_price + span:
                        break
                    if pivot_price - span <= price <= pivot_price + span:
                        cluster_prices.append(price)
                        start_index = zigzag.get_index(offset)
                if len(cluster_prices) >= 2 and start_index is not None:
                    cluster_high = max(cluster_prices)
                    cluster_low = min(cluster_prices)
                    buffer_value = max(liquidity_buffer, span) if liquidity_buffer > 0 else span
                    if buffer_value is None:
                        buffer_value = 0.0
                    price_bottom = cluster_low
                    price_top = cluster_high + (buffer_value or 0.0)
                    if price_top <= price_bottom:
                        price_top = price_bottom + (buffer_value or 0.0)
                    right_index = index + 10
                    if buyside_boxes and buyside_boxes[0].start_index == start_index:
                        box = buyside_boxes[0]
                        box.price_top = price_top
                        box.price_bottom = price_bottom
                        box.right_index = right_index
                        if box.line_active:
                            box.line_end_index = index
                        box.last_updated_index = index
                        box.source_timeframe = current_timeframe
                    else:
                        start_time = bars[start_index].time if 0 <= start_index < len(bars) else bars[index].time
                        new_box = LiquidityBox(
                            side="buyside",
                            price_top=price_top,
                            price_bottom=price_bottom,
                            start_index=start_index,
                            start_time=start_time,
                            right_index=right_index,
                            line_start_index=start_index,
                            line_end_index=index,
                            reference_price=pivot_price,
                            source_timeframe=current_timeframe,
                            last_updated_index=index,
                        )
                        buyside_boxes.insert(0, new_box)
                        liquidity_events.append(
                            LiquidityEvent(
                                event_type="buyside_creation",
                                box=new_box,
                                index=index,
                                time=bar.time,
                            )
                        )
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

            if show_liquidity and per and span is not None:
                cluster_prices = []
                start_index = None
                size_limit = min(zigzag.size(), MAX_ZIGZAG_SIZE)
                for offset in range(size_limit):
                    if zigzag.get_direction(offset) != -1:
                        continue
                    price = zigzag.get_price(offset)
                    if math.isnan(price):
                        continue
                    if price < pivot_price - span:
                        break
                    if pivot_price - span <= price <= pivot_price + span:
                        cluster_prices.append(price)
                        start_index = zigzag.get_index(offset)
                if len(cluster_prices) >= 2 and start_index is not None:
                    cluster_high = max(cluster_prices)
                    cluster_low = min(cluster_prices)
                    buffer_value = max(liquidity_buffer, span) if liquidity_buffer > 0 else span
                    if buffer_value is None:
                        buffer_value = 0.0
                    price_top = cluster_high
                    price_bottom = cluster_low - (buffer_value or 0.0)
                    if price_top <= price_bottom:
                        price_bottom = price_top - (buffer_value or 0.0)
                    right_index = index + 10
                    if sellside_boxes and sellside_boxes[0].start_index == start_index:
                        box = sellside_boxes[0]
                        box.price_top = price_top
                        box.price_bottom = price_bottom
                        box.right_index = right_index
                        if box.line_active:
                            box.line_end_index = index
                        box.last_updated_index = index
                        box.source_timeframe = current_timeframe
                    else:
                        start_time = bars[start_index].time if 0 <= start_index < len(bars) else bars[index].time
                        new_box = LiquidityBox(
                            side="sellside",
                            price_top=price_top,
                            price_bottom=price_bottom,
                            start_index=start_index,
                            start_time=start_time,
                            right_index=right_index,
                            line_start_index=start_index,
                            line_end_index=index,
                            reference_price=pivot_price,
                            source_timeframe=current_timeframe,
                            last_updated_index=index,
                        )
                        sellside_boxes.insert(0, new_box)
                        liquidity_events.append(
                            LiquidityEvent(
                                event_type="sellside_creation",
                                box=new_box,
                                index=index,
                                time=bar.time,
                            )
                        )
                        if len(sellside_boxes) > max_visible:
                            sellside_boxes.pop()

        # Update existing boxes each bar ------------------------------------
        for box in buyside_boxes:
            if box.broken:
                continue
            box.right_index = index + 3
            if box.line_active:
                box.line_end_index = index + 3
            if not box.filled and index > box.start_index and bar.high >= box.price_top:
                box.filled = True
                box.line_active = False
                box.line_end_index = index
            if not box.broken and bar.close > box.price_top:
                box.broken_top = True
                box.broken = True
                box.filled = True
                box.line_active = False
                box.right_index = index
                box.line_end_index = index
            box.last_updated_index = index

        for box in sellside_boxes:
            if box.broken:
                continue
            box.right_index = index + 3
            if box.line_active:
                box.line_end_index = index + 3
            if not box.filled and index > box.start_index and bar.low <= box.price_bottom:
                box.filled = True
                box.line_active = False
                box.line_end_index = index
            if not box.broken and bar.close < box.price_bottom:
                box.broken_bottom = True
                box.broken = True
                box.filled = True
                box.line_active = False
                box.right_index = index
                box.line_end_index = index
            box.last_updated_index = index

    return buyside_boxes, sellside_boxes, liquidity_events


def compute_higher_timeframe_liquidity(
    exchange: ccxt.Exchange,
    symbol: str,
    bars: Sequence[OHLCVBar],
    liquidity_buffer: float = LIQUIDITY_BUFFER,
) -> Tuple[List[LiquidityBox], List[LiquidityBox]]:
    """Derive higher timeframe liquidity pools such as previous day/week extremes."""

    if not ENABLE_HIGHER_TIMEFRAME_LIQUIDITY or not HIGHER_TIMEFRAME_LEVELS:
        return [], []

    latest_index = len(bars) - 1
    if latest_index < 0:
        return [], []

    buyside_levels: List[LiquidityBox] = []
    sellside_levels: List[LiquidityBox] = []

    for timeframe in HIGHER_TIMEFRAME_LEVELS:
        try:
            raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=HIGHER_TIMEFRAME_LOOKBACK)
        except Exception:
            continue
        if len(raw) < 2:
            continue
        ht_bars = [
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
        reference_bar = ht_bars[-2]
        buffer_value = max(0.0, liquidity_buffer)
        start_index = max(0, latest_index - 1)
        right_index = latest_index + 10

        if reference_bar.high > 0:
            price_bottom = reference_bar.high
            price_top = reference_bar.high + buffer_value
            if price_top <= price_bottom:
                price_top = price_bottom
            buyside_levels.append(
                LiquidityBox(
                    side="buyside",
                    price_top=price_top,
                    price_bottom=price_bottom,
                    start_index=start_index,
                    start_time=reference_bar.time,
                    right_index=right_index,
                    line_start_index=start_index,
                    line_end_index=latest_index,
                    reference_price=reference_bar.high,
                    source_timeframe=timeframe,
                    line_active=False,
                    last_updated_index=latest_index,
                )
            )

        if reference_bar.low > 0:
            price_top = reference_bar.low
            price_bottom = reference_bar.low - buffer_value
            if price_top <= price_bottom:
                price_bottom = price_top
            sellside_levels.append(
                LiquidityBox(
                    side="sellside",
                    price_top=price_top,
                    price_bottom=price_bottom,
                    start_index=start_index,
                    start_time=reference_bar.time,
                    right_index=right_index,
                    line_start_index=start_index,
                    line_end_index=latest_index,
                    reference_price=reference_bar.low,
                    source_timeframe=timeframe,
                    line_active=False,
                    last_updated_index=latest_index,
                )
            )

    return buyside_levels, sellside_levels


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

        if not (show_ob and per):
            continue

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
                        updated_min = min(minima, candidate_min)
                        if updated_min == candidate_min:
                            maxima = max_series[candidate_index]
                            loc_index = candidate_index
                        minima = updated_min
                block_type = "breaker"
                if swing_state.prev_top.is_valid() and top_swing.price <= swing_state.prev_top.price:
                    block_type = "mitigation"
                block = OrderBlock(
                    side="bullish",
                    price_top=maxima,
                    price_bottom=minima,
                    origin_index=loc_index,
                    origin_time=times[loc_index],
                    created_index=index,
                    created_time=bar.time,
                    block_type=block_type,
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
                if len(bullish_blocks) > show_bull:
                    del bullish_blocks[show_bull:]

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
                        and block.price_bottom < top_swing.price < block.price_top
                        and not block.breaker_confirmed
                    ):
                        block.breaker_confirmed = True
                        block.confirmation_index = index
                        block.confirmation_time = bar.time
                        events.append(
                            OrderBlockEvent(
                                event_type="bullish_break_confirmation",
                                block=block,
                                index=index,
                                time=bar.time,
                                price=bar.close,
                            )
                        )

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
                        updated_max = max(maxima, candidate_max)
                        if updated_max == candidate_max:
                            minima = min_series[candidate_index]
                            loc_index = candidate_index
                        maxima = updated_max
                block_type = "breaker"
                if swing_state.prev_bottom.is_valid() and bottom_swing.price >= swing_state.prev_bottom.price:
                    block_type = "mitigation"
                block = OrderBlock(
                    side="bearish",
                    price_top=maxima,
                    price_bottom=minima,
                    origin_index=loc_index,
                    origin_time=times[loc_index],
                    created_index=index,
                    created_time=bar.time,
                    block_type=block_type,
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
                if len(bearish_blocks) > show_bear:
                    del bearish_blocks[show_bear:]

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
                        and block.price_bottom < bottom_swing.price < block.price_top
                        and not block.breaker_confirmed
                    ):
                        block.breaker_confirmed = True
                        block.confirmation_index = index
                        block.confirmation_time = bar.time
                        events.append(
                            OrderBlockEvent(
                                event_type="bearish_break_confirmation",
                                block=block,
                                index=index,
                                time=bar.time,
                                price=bar.close,
                            )
                        )

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

    timeframe_params = derive_timeframe_parameters(TIMEFRAME)

    for symbol in symbols:
        market = exchange.markets[symbol]
        if not market.get("active", True):
            print(f"[SKIP] SYMBOL={symbol} is inactive; skipping.")
            continue
        print(f"[INFO] Fetching OHLCV for {symbol}...")
        try:
            bars = fetch_ohlcv(
                exchange,
                symbol,
                timeframe=TIMEFRAME,
                limit=timeframe_params["max_bars"],
            )
        except Exception as exc:  # noqa: BLE001 - log and continue scanning
            print(f"[ERROR] Failed to fetch OHLCV for {symbol}: {exc}")
            time.sleep(exchange.rateLimit / 1000.0)
            continue

        if not bars:
            print(f"[WARN] SYMBOL={symbol} returned no candles; skipping.")
            continue

        buyside, sellside, liquidity_events = detect_liquidity_zones(
            bars,
            margin=MARGIN,
            max_visible=VIS_LIQ,
            show_liquidity=True,
            atr_period=ATR_PERIOD,
            pivot_left=timeframe_params["pivot_left"],
            pivot_right=timeframe_params["pivot_right"],
            present_window=timeframe_params["present_window"],
            liquidity_buffer=LIQUIDITY_BUFFER,
            current_timeframe=TIMEFRAME,
        )
        htf_buyside, htf_sellside = compute_higher_timeframe_liquidity(
            exchange,
            symbol,
            bars,
            liquidity_buffer=LIQUIDITY_BUFFER,
        )
        if htf_buyside:
            buyside.extend(htf_buyside)
        if htf_sellside:
            sellside.extend(htf_sellside)
        bullish_obs, bearish_obs, ob_events = detect_order_blocks(
            bars,
            length=timeframe_params["order_block_lookback"],
            show_ob=SHOW_ORDER_BLOCKS,
            show_bull=timeframe_params["show_bull"],
            show_bear=timeframe_params["show_bear"],
            use_body=USE_BODY_FOR_OB,
            present_window=timeframe_params["present_window"],
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

        prune_history_for_symbol(
            LIQUIDITY_CREATION_ALERTS, symbol, latest_index, MAX_ALERT_AGE_BARS
        )
        prune_history_for_symbol(
            LIQUIDITY_TOUCH_ALERTS, symbol, latest_index, MAX_ALERT_AGE_BARS
        )
        prune_history_for_symbol(
            ORDERBLOCK_CREATION_ALERTS, symbol, latest_index, MAX_ALERT_AGE_BARS
        )
        prune_history_for_symbol(
            ORDERBLOCK_BREAK_ALERTS, symbol, latest_index, MAX_ALERT_AGE_BARS
        )
        prune_history_for_symbol(
            ORDERBLOCK_TOUCH_ALERTS, symbol, latest_index, MAX_ALERT_AGE_BARS
        )
        prune_history_for_symbol(
            ORDERBLOCK_CONFIRM_ALERTS, symbol, latest_index, MAX_ALERT_AGE_BARS
        )

        for event in liquidity_events:
            if event.index != latest_index:
                continue
            box = event.box
            event_label = "BuysideLiquidity" if box.side == "buyside" else "SellsideLiquidity"
            history_key = (
                symbol,
                event.event_type,
                box.start_index,
                box.source_timeframe,
            )
            if not should_emit(
                LIQUIDITY_CREATION_ALERTS,
                history_key,
                event.index,
                latest_index,
                MAX_ALERT_AGE_BARS,
            ):
                continue
            price_range = f"[{box.price_bottom:.6f}, {box.price_top:.6f}]"
            source_suffix = (
                f", SOURCE_TF={box.source_timeframe}"
                if box.source_timeframe and box.source_timeframe != TIMEFRAME
                else ""
            )
            message = (
                f"[ALERT] SYMBOL={symbol}, EVENT={event_label}, TF={TIMEFRAME}, "
                f"PRICE_RANGE={price_range}, BAR_TIME={timestamp}{source_suffix}"
            )
            emit_alert(event_label, message)
            alerts_emitted = True

        for box, touch_price in liquidity_touches:
            event_label = (
                "BuysideLiquidityTouch" if box.side == "buyside" else "SellsideLiquidityTouch"
            )
            history_key = (
                symbol,
                event_label,
                box.start_index,
                box.source_timeframe,
            )
            if not should_emit(
                LIQUIDITY_TOUCH_ALERTS,
                history_key,
                latest_index,
                latest_index,
                MAX_ALERT_AGE_BARS,
            ):
                continue
            price_range = f"[{box.price_bottom:.6f}, {box.price_top:.6f}]"
            source_suffix = (
                f", SOURCE_TF={box.source_timeframe}"
                if box.source_timeframe and box.source_timeframe != TIMEFRAME
                else ""
            )
            message = (
                f"[ALERT] SYMBOL={symbol}, EVENT={event_label}, TF={TIMEFRAME}, "
                f"TOUCH_PRICE={touch_price:.6f}, RANGE={price_range}, BAR_TIME={timestamp}{source_suffix}"
            )
            emit_alert(event_label, message)
            alerts_emitted = True

        for event in ob_events:
            if event.index != latest_index:
                continue
            block = event.block
            if event.event_type in {"bullish_creation", "bearish_creation"}:
                event_label = "Bullish OB" if block.side == "bullish" else "Bearish OB"
                history = ORDERBLOCK_CREATION_ALERTS
            elif event.event_type in {"bullish_break", "bearish_break"}:
                event_label = "Bullish Break" if block.side == "bullish" else "Bearish Break"
                history = ORDERBLOCK_BREAK_ALERTS
            elif event.event_type in {
                "bullish_break_confirmation",
                "bearish_break_confirmation",
            }:
                event_label = (
                    "Bullish Break Confirmed"
                    if block.side == "bullish"
                    else "Bearish Break Confirmed"
                )
                history = ORDERBLOCK_CONFIRM_ALERTS
            else:
                continue
            history_key = (symbol, event_label, block.origin_index, block.created_index)
            if not should_emit(
                history,
                history_key,
                event.index,
                latest_index,
                MAX_ALERT_AGE_BARS,
            ):
                continue
            price_range = f"[{block.price_bottom:.6f}, {block.price_top:.6f}]"
            price_value = event.price if event.price is not None else bars[latest_index].close
            block_type_suffix = (
                f", BLOCK_TYPE={block.block_type}"
                if event.event_type in {"bullish_creation", "bearish_creation"}
                else ""
            )
            message = (
                f"[ALERT] SYMBOL={symbol}, EVENT={event_label}, TF={TIMEFRAME}, "
                f"PRICE_RANGE={price_range}, EVENT_PRICE={price_value:.6f}, BAR_TIME={timestamp}{block_type_suffix}"
            )
            emit_alert(event_label, message)
            alerts_emitted = True

        for event in ob_touch_events:
            block = event.block
            event_label = "BullishOBTouch" if block.side == "bullish" else "BearishOBTouch"
            history_key = (symbol, event_label, block.origin_index, block.created_index)
            if not should_emit(
                ORDERBLOCK_TOUCH_ALERTS,
                history_key,
                event.index,
                latest_index,
                MAX_ALERT_AGE_BARS,
            ):
                continue
            price_range = f"[{block.price_bottom:.6f}, {block.price_top:.6f}]"
            touch_price = event.price if event.price is not None else bars[latest_index].close
            message = (
                f"[ALERT] SYMBOL={symbol}, EVENT={event_label}, TF={TIMEFRAME}, "
                f"TOUCH_PRICE={touch_price:.6f}, RANGE={price_range}, BAR_TIME={timestamp}"
            )
            emit_alert(event_label, message)
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
