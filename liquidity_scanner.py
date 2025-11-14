"""ICT Concepts [LuxAlgo] strategy backtester.

This module ports the liquidity and order block logic from the TradingView
"ICT Concepts [LuxAlgo]" indicator to Python and layers a rules-based
strategy with risk management and backtesting utilities on top of the
indicator state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Core data structures
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
class LiquidityZone:
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

    def clone(self) -> "LiquidityZone":
        return LiquidityZone(
            side=self.side,
            price_top=self.price_top,
            price_bottom=self.price_bottom,
            start_index=self.start_index,
            start_time=self.start_time,
            right_index=self.right_index,
            line_start_index=self.line_start_index,
            line_end_index=self.line_end_index,
            reference_price=self.reference_price,
            broken_top=self.broken_top,
            broken_bottom=self.broken_bottom,
            broken=self.broken,
            filled=self.filled,
            line_active=self.line_active,
            last_updated_index=self.last_updated_index,
        )


@dataclass
class LiquidityEvent:
    event_type: str
    zone: LiquidityZone
    index: int
    time: int


@dataclass
class LiquiditySnapshot:
    index: int
    buyside: List[LiquidityZone]
    sellside: List[LiquidityZone]


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
    breaker_confirmed: bool = False
    confirmation_index: Optional[int] = None
    confirmation_time: Optional[int] = None

    def clone(self) -> "OrderBlock":
        return OrderBlock(
            side=self.side,
            price_top=self.price_top,
            price_bottom=self.price_bottom,
            origin_index=self.origin_index,
            origin_time=self.origin_time,
            created_index=self.created_index,
            created_time=self.created_time,
            breaker=self.breaker,
            break_index=self.break_index,
            break_time=self.break_time,
            breaker_confirmed=self.breaker_confirmed,
            confirmation_index=self.confirmation_index,
            confirmation_time=self.confirmation_time,
        )


@dataclass
class OrderBlockEvent:
    event_type: str
    block: OrderBlock
    index: int
    time: int
    price: Optional[float] = None


@dataclass
class OrderBlockSnapshot:
    index: int
    bullish: List[OrderBlock]
    bearish: List[OrderBlock]


@dataclass
class Trade:
    entry_index: int
    entry_time: int
    entry_price: float
    side: str  # "long" or "short"
    size: float
    stop_loss: float
    take_profit: float
    exit_index: Optional[int] = None
    exit_time: Optional[int] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    exit_reason: Optional[str] = None


@dataclass
class StrategyConfig:
    # Indicator configuration (mirrors Pine inputs)
    margin: float = 4.0
    vis_liq: int = 2
    atr_period: int = 10
    pivot_left: int = 5
    pivot_right: int = 1
    order_block_length: int = 10
    show_order_blocks: bool = True
    show_bull: int = 1
    show_bear: int = 1
    use_body_for_ob: bool = True
    present_window: int = 500
    # Strategy configuration
    initial_equity: float = 100_000.0
    risk_per_trade: float = 0.01
    reward_risk: float = 2.0
    sweep_lookback: int = 20
    max_trades_per_day: int = 3
    max_daily_loss_pct: float = 0.02
    warmup_bars: int = 200


# ---------------------------------------------------------------------------
# Helper calculations mirroring Pine Script logic
# ---------------------------------------------------------------------------


def compute_atr(bars: Sequence[OHLCVBar], period: int) -> List[Optional[float]]:
    """Replicates ta.atr using Wilder's RMA as in Pine Script."""
    atr_values: List[Optional[float]] = [None] * len(bars)
    prev_atr: Optional[float] = None
    for i, bar in enumerate(bars):
        if i == 0:
            tr = bar.high - bar.low
        else:
            prev_close = bars[i - 1].close
            tr = max(
                bar.high - bar.low,
                abs(bar.high - prev_close),
                abs(bar.low - prev_close),
            )
        if prev_atr is None:
            prev_atr = tr
        else:
            prev_atr = ((period - 1) * prev_atr + tr) / period
        if i >= period - 1:
            atr_values[i] = prev_atr
    return atr_values


def _pivot_extreme(
    values: Sequence[float],
    current_index: int,
    left: int,
    right: int,
    is_high: bool,
) -> Tuple[Optional[float], Optional[int]]:
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
        if is_high and candidate >= pivot_value:
            return None, None
        if not is_high and candidate <= pivot_value:
            return None, None
    return pivot_value, pivot_index


def pivot_high(values: Sequence[float], current_index: int, left: int, right: int) -> Tuple[Optional[float], Optional[int]]:
    return _pivot_extreme(values, current_index, left, right, True)


def pivot_low(values: Sequence[float], current_index: int, left: int, right: int) -> Tuple[Optional[float], Optional[int]]:
    return _pivot_extreme(values, current_index, left, right, False)


class ZigZagState:
    """Port of the ICT Concepts ZigZag structure (type ZZ)."""

    __slots__ = ("max_size", "directions", "indices", "prices", "flags")

    def __init__(self, max_size: int = 50) -> None:
        self.max_size = max_size
        self.directions: List[int] = [0] * max_size
        self.indices: List[int] = [0] * max_size
        self.prices: List[float] = [math.nan] * max_size
        self.flags: List[bool] = [False] * max_size

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
    """Mirror of the Pine Script swings(len) helper."""

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
        prev_os = self.os
        length = self.length
        if length <= 0 or index < length or index >= len(highs) or index >= len(lows):
            return self.top, self.bottom, prev_os
        reference_index = index - length
        if reference_index < 0:
            return self.top, self.bottom, prev_os
        window_start = reference_index + 1
        window_end = index + 1
        next_highs = [h for h in highs[window_start:window_end] if not math.isnan(h)]
        next_lows = [l for l in lows[window_start:window_end] if not math.isnan(l)]
        if not next_highs or not next_lows:
            return self.top, self.bottom, prev_os
        upper = max(next_highs)
        lower = min(next_lows)
        ref_high = highs[reference_index]
        ref_low = lows[reference_index]
        if not math.isnan(ref_high) and ref_high > upper:
            self.os = 0
        elif not math.isnan(ref_low) and ref_low < lower:
            self.os = 1
        if self.os == 0 and prev_os != 0 and not math.isnan(ref_high):
            self.top = SwingPoint(price=ref_high, index=reference_index, crossed=False)
        if self.os == 1 and prev_os != 1 and not math.isnan(ref_low):
            self.bottom = SwingPoint(price=ref_low, index=reference_index, crossed=False)
        return self.top, self.bottom, prev_os


# ---------------------------------------------------------------------------
# Liquidity detection (ported from ICT Concepts [LuxAlgo])
# ---------------------------------------------------------------------------


@dataclass
class LiquidityDetectionResult:
    buyside_zones: List[LiquidityZone]
    sellside_zones: List[LiquidityZone]
    events: List[LiquidityEvent]
    history: List[LiquiditySnapshot]


def detect_liquidity_zones(
    bars: Sequence[OHLCVBar],
    config: StrategyConfig,
) -> LiquidityDetectionResult:
    if not bars:
        return LiquidityDetectionResult([], [], [], [])

    zigzag = ZigZagState(max_size=50)
    atr_values = compute_atr(bars, config.atr_period)
    buyside: List[LiquidityZone] = []
    sellside: List[LiquidityZone] = []
    events: List[LiquidityEvent] = []
    history: List[LiquiditySnapshot] = []
    a_value = 10.0 / config.margin
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    last_index = len(bars) - 1

    for index, bar in enumerate(bars):
        atr_val = atr_values[index]
        span = atr_val / a_value if atr_val is not None else None
        per = (last_index - index) <= config.present_window

        ph_value, ph_index = pivot_high(highs, index, config.pivot_left, config.pivot_right)
        if ph_value is not None and ph_index is not None:
            pivot_price = highs[ph_index]
            direction = zigzag.get_direction(0)
            if direction < 1:
                zigzag.in_out(1, ph_index, pivot_price)
            elif direction == 1 and pivot_price > zigzag.get_price(0):
                zigzag.update_latest(ph_index, pivot_price)
            if per and span is not None:
                count = 0
                start_index = None
                start_price = None
                min_price = 0.0
                max_price = 10e6
                size_limit = min(zigzag.size(), 50)
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
                    # Ported from ICT Concepts [LuxAlgo]: buyside liquidity box creation
                    midpoint = (min_price + max_price) / 2.0
                    top = midpoint + span
                    bottom = midpoint - span
                    right_index = index + 10
                    if buyside and buyside[0].start_index == start_index:
                        zone = buyside[0]
                        zone.price_top = top
                        zone.price_bottom = bottom
                        zone.right_index = right_index
                        zone.line_end_index = index - 1
                        zone.last_updated_index = index
                    else:
                        start_time = bars[start_index].time if 0 <= start_index < len(bars) else bar.time
                        new_zone = LiquidityZone(
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
                        buyside.insert(0, new_zone)
                        events.append(LiquidityEvent("buyside_creation", new_zone, index, bar.time))
                        if len(buyside) > config.vis_liq:
                            buyside.pop()

        pl_value, pl_index = pivot_low(lows, index, config.pivot_left, config.pivot_right)
        if pl_value is not None and pl_index is not None:
            pivot_price = lows[pl_index]
            direction = zigzag.get_direction(0)
            if direction > -1:
                zigzag.in_out(-1, pl_index, pivot_price)
            elif direction == -1 and pivot_price < zigzag.get_price(0):
                zigzag.update_latest(pl_index, pivot_price)
            if per and span is not None:
                count = 0
                start_index = None
                start_price = None
                min_price = 0.0
                max_price = 10e6
                size_limit = min(zigzag.size(), 50)
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
                    # Ported from ICT Concepts [LuxAlgo]: sellside liquidity box creation
                    midpoint = (min_price + max_price) / 2.0
                    top = midpoint + span
                    bottom = midpoint - span
                    right_index = index + 10
                    if sellside and sellside[0].start_index == start_index:
                        zone = sellside[0]
                        zone.price_top = top
                        zone.price_bottom = bottom
                        zone.right_index = right_index
                        zone.line_end_index = index - 1
                        zone.last_updated_index = index
                    else:
                        start_time = bars[start_index].time if 0 <= start_index < len(bars) else bar.time
                        new_zone = LiquidityZone(
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
                        sellside.insert(0, new_zone)
                        events.append(LiquidityEvent("sellside_creation", new_zone, index, bar.time))
                        if len(sellside) > config.vis_liq:
                            sellside.pop()

        for zone in buyside:
            if zone.broken:
                continue
            zone.right_index = index + 3
            zone.line_end_index = index + 3
            if not zone.broken_top and bar.close > zone.price_top:
                zone.broken_top = True
            if not zone.broken_bottom and bar.close > zone.price_bottom:
                zone.broken_bottom = True
            if zone.broken_bottom and not zone.filled:
                zone.filled = True
                zone.line_active = False
                zone.line_end_index = index
            if zone.broken_top and zone.broken_bottom:
                zone.broken = True
                zone.right_index = index
                zone.line_end_index = index
            zone.last_updated_index = index

        for zone in sellside:
            if zone.broken:
                continue
            zone.right_index = index + 3
            zone.line_end_index = index + 3
            if not zone.broken_top and bar.close < zone.price_top:
                zone.broken_top = True
            if not zone.broken_bottom and bar.close < zone.price_bottom:
                zone.broken_bottom = True
            if zone.broken_top and not zone.filled:
                zone.filled = True
                zone.line_active = False
                zone.line_end_index = index
            if zone.broken_top and zone.broken_bottom:
                zone.broken = True
                zone.right_index = index
                zone.line_end_index = index
            zone.last_updated_index = index

        history.append(
            LiquiditySnapshot(
                index=index,
                buyside=[zone.clone() for zone in buyside],
                sellside=[zone.clone() for zone in sellside],
            )
        )

    return LiquidityDetectionResult(buyside, sellside, events, history)


# ---------------------------------------------------------------------------
# Order block detection (ported from ICT Concepts [LuxAlgo])
# ---------------------------------------------------------------------------


@dataclass
class OrderBlockDetectionResult:
    bullish_blocks: List[OrderBlock]
    bearish_blocks: List[OrderBlock]
    events: List[OrderBlockEvent]
    history: List[OrderBlockSnapshot]


def detect_order_blocks(
    bars: Sequence[OHLCVBar],
    config: StrategyConfig,
) -> OrderBlockDetectionResult:
    bullish_blocks: List[OrderBlock] = []
    bearish_blocks: List[OrderBlock] = []
    events: List[OrderBlockEvent] = []
    history: List[OrderBlockSnapshot] = []

    if not bars or not config.show_order_blocks:
        return OrderBlockDetectionResult(bullish_blocks, bearish_blocks, events, history)

    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    opens = [bar.open for bar in bars]
    closes = [bar.close for bar in bars]
    times = [bar.time for bar in bars]

    if config.use_body_for_ob:
        max_series = [max(o, c) for o, c in zip(opens, closes)]
        min_series = [min(o, c) for o, c in zip(opens, closes)]
    else:
        max_series = highs[:]
        min_series = lows[:]

    swing_state = SwingState(length=config.order_block_length)
    total_bars = len(bars)

    for index, bar in enumerate(bars):
        top_swing, bottom_swing, _ = swing_state.update(highs, lows, closes, index)
        per = (total_bars - 1 - index) <= config.present_window
        if not per:
            history.append(
                OrderBlockSnapshot(
                    index=index,
                    bullish=[block.clone() for block in bullish_blocks],
                    bearish=[block.clone() for block in bearish_blocks],
                )
            )
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
                # Ported from ICT Concepts [LuxAlgo]: bullish order block creation
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
                events.append(OrderBlockEvent("bullish_creation", block, index, bar.time, bar.close))

        for blk_index in range(len(bullish_blocks) - 1, -1, -1):
            block = bullish_blocks[blk_index]
            if not block.breaker:
                if min(bar.close, bar.open) < block.price_bottom:
                    block.breaker = True
                    block.break_index = index
                    block.break_time = bar.time
                    events.append(OrderBlockEvent("bullish_break", block, index, bar.time, bar.close))
            else:
                if bar.close > block.price_top:
                    bullish_blocks.pop(blk_index)
                    continue
                if (
                    blk_index < config.show_bull
                    and top_swing.is_valid()
                    and block.price_bottom < top_swing.price < block.price_top
                    and not block.breaker_confirmed
                ):
                    block.breaker_confirmed = True
                    block.confirmation_index = index
                    block.confirmation_time = bar.time
                    events.append(
                        OrderBlockEvent("bullish_break_confirmation", block, index, bar.time, bar.close)
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
                # Ported from ICT Concepts [LuxAlgo]: bearish order block creation
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
                events.append(OrderBlockEvent("bearish_creation", block, index, bar.time, bar.close))

        for blk_index in range(len(bearish_blocks) - 1, -1, -1):
            block = bearish_blocks[blk_index]
            if not block.breaker:
                if max(bar.close, bar.open) > block.price_top:
                    block.breaker = True
                    block.break_index = index
                    block.break_time = bar.time
                    events.append(OrderBlockEvent("bearish_break", block, index, bar.time, bar.close))
            else:
                if bar.close < block.price_bottom:
                    bearish_blocks.pop(blk_index)
                    continue
                if (
                    blk_index < config.show_bear
                    and bottom_swing.is_valid()
                    and block.price_bottom < bottom_swing.price < block.price_top
                    and not block.breaker_confirmed
                ):
                    block.breaker_confirmed = True
                    block.confirmation_index = index
                    block.confirmation_time = bar.time
                    events.append(
                        OrderBlockEvent("bearish_break_confirmation", block, index, bar.time, bar.close)
                    )

        history.append(
            OrderBlockSnapshot(
                index=index,
                bullish=[block.clone() for block in bullish_blocks],
                bearish=[block.clone() for block in bearish_blocks],
            )
        )

    return OrderBlockDetectionResult(bullish_blocks, bearish_blocks, events, history)


# ---------------------------------------------------------------------------
# Strategy logic
# ---------------------------------------------------------------------------


@dataclass
class Signal:
    index: int
    time: int
    side: str  # "long" or "short"
    reason: str


def _find_nearest_zone_price(
    zones: Iterable[LiquidityZone],
    reference_price: float,
    side: str,
) -> Optional[float]:
    candidate_price: Optional[float] = None
    for zone in zones:
        if zone.broken:
            continue
        zone_mid = (zone.price_top + zone.price_bottom) / 2.0
        if side == "long" and zone_mid > reference_price:
            if candidate_price is None or zone_mid < candidate_price:
                candidate_price = zone_mid
        if side == "short" and zone_mid < reference_price:
            if candidate_price is None or zone_mid > candidate_price:
                candidate_price = zone_mid
    return candidate_price


def _has_liquidity_sweep(
    index: int,
    bars: Sequence[OHLCVBar],
    history: Dict[int, LiquiditySnapshot],
    side: str,
    lookback: int,
) -> bool:
    start = max(0, index - lookback)
    for idx in range(start, index + 1):
        bar = bars[idx]
        snapshot = history.get(idx)
        if snapshot is None:
            continue
        zones = snapshot.sellside if side == "long" else snapshot.buyside
        for zone in zones:
            if zone.broken:
                continue
            if side == "long":
                if bar.low < zone.price_bottom and bar.close > zone.price_bottom:
                    return True
            else:
                if bar.high > zone.price_top and bar.close < zone.price_top:
                    return True
    return False


def generate_signals(
    bars: Sequence[OHLCVBar],
    liquidity_result: LiquidityDetectionResult,
    order_block_result: OrderBlockDetectionResult,
    config: StrategyConfig,
) -> List[Signal]:
    liquidity_history = {snap.index: snap for snap in liquidity_result.history}
    order_block_history = {snap.index: snap for snap in order_block_result.history}
    signals: List[Signal] = []
    for index in range(config.warmup_bars, len(bars)):
        bar = bars[index]
        liq_snapshot = liquidity_history.get(index)
        ob_snapshot = order_block_history.get(index)
        if liq_snapshot is None or ob_snapshot is None:
            continue

        bullish_blocks = [block for block in ob_snapshot.bullish if not block.breaker]
        bearish_blocks = [block for block in ob_snapshot.bearish if not block.breaker]

        if bullish_blocks and _has_liquidity_sweep(index, bars, liquidity_history, "long", config.sweep_lookback):
            block = bullish_blocks[0]
            block_mid = (block.price_top + block.price_bottom) / 2.0
            if block.price_bottom <= bar.close <= block.price_top and bar.close >= block_mid:
                signals.append(Signal(index=index, time=bar.time, side="long", reason="bullish_ob_retest"))

        if bearish_blocks and _has_liquidity_sweep(index, bars, liquidity_history, "short", config.sweep_lookback):
            block = bearish_blocks[0]
            block_mid = (block.price_top + block.price_bottom) / 2.0
            if block.price_bottom <= bar.close <= block.price_top and bar.close <= block_mid:
                signals.append(Signal(index=index, time=bar.time, side="short", reason="bearish_ob_retest"))

    return signals


def run_backtest(bars: Sequence[OHLCVBar], config: StrategyConfig) -> List[Trade]:
    if len(bars) <= config.warmup_bars:
        return []

    liquidity_result = detect_liquidity_zones(bars, config)
    order_block_result = detect_order_blocks(bars, config)
    liquidity_history = {snap.index: snap for snap in liquidity_result.history}
    order_block_history = {snap.index: snap for snap in order_block_result.history}

    trades: List[Trade] = []
    open_trades: List[Trade] = []
    equity = config.initial_equity
    daily_trade_counts: Dict[int, int] = {}
    daily_pnl: Dict[int, float] = {}

    def current_day(timestamp: int) -> int:
        return timestamp // 86_400_000

    for index in range(config.warmup_bars, len(bars)):
        bar = bars[index]
        liq_snapshot = liquidity_history.get(index)
        ob_snapshot = order_block_history.get(index)
        if liq_snapshot is None or ob_snapshot is None:
            continue
        day = current_day(bar.time)
        daily_trade_counts.setdefault(day, 0)
        daily_pnl.setdefault(day, 0.0)

        for trade in list(open_trades):
            if trade.side == "long":
                if bar.low <= trade.stop_loss:
                    exit_price = trade.stop_loss
                    trade.exit_index = index
                    trade.exit_time = bar.time
                    trade.exit_price = exit_price
                    trade.pnl = (exit_price - trade.entry_price) * trade.size
                    trade.exit_reason = "stop_loss"
                    trades.append(trade)
                    open_trades.remove(trade)
                    equity += trade.pnl
                    daily_pnl[day] += trade.pnl
                    continue
                if bar.high >= trade.take_profit:
                    exit_price = trade.take_profit
                    trade.exit_index = index
                    trade.exit_time = bar.time
                    trade.exit_price = exit_price
                    trade.pnl = (exit_price - trade.entry_price) * trade.size
                    trade.exit_reason = "take_profit"
                    trades.append(trade)
                    open_trades.remove(trade)
                    equity += trade.pnl
                    daily_pnl[day] += trade.pnl
                    continue
            else:
                if bar.high >= trade.stop_loss:
                    exit_price = trade.stop_loss
                    trade.exit_index = index
                    trade.exit_time = bar.time
                    trade.exit_price = exit_price
                    trade.pnl = (trade.entry_price - exit_price) * trade.size
                    trade.exit_reason = "stop_loss"
                    trades.append(trade)
                    open_trades.remove(trade)
                    equity += trade.pnl
                    daily_pnl[day] += trade.pnl
                    continue
                if bar.low <= trade.take_profit:
                    exit_price = trade.take_profit
                    trade.exit_index = index
                    trade.exit_time = bar.time
                    trade.exit_price = exit_price
                    trade.pnl = (trade.entry_price - exit_price) * trade.size
                    trade.exit_reason = "take_profit"
                    trades.append(trade)
                    open_trades.remove(trade)
                    equity += trade.pnl
                    daily_pnl[day] += trade.pnl
                    continue

        if daily_trade_counts[day] >= config.max_trades_per_day:
            continue
        if abs(daily_pnl[day]) >= config.initial_equity * config.max_daily_loss_pct:
            continue

        bullish_blocks = [block for block in ob_snapshot.bullish if not block.breaker]
        bearish_blocks = [block for block in ob_snapshot.bearish if not block.breaker]

        risk_amount = equity * config.risk_per_trade

        if (
            bullish_blocks
            and _has_liquidity_sweep(index, bars, liquidity_history, "long", config.sweep_lookback)
        ):
            block = bullish_blocks[0]
            block_mid = (block.price_top + block.price_bottom) / 2.0
            if block.price_bottom <= bar.close <= block.price_top and bar.close >= block_mid:
                stop_loss = block.price_bottom
                nearest_zone = _find_nearest_zone_price(liq_snapshot.buyside, bar.close, "long")
                if nearest_zone is not None:
                    take_profit = nearest_zone
                    if take_profit <= bar.close:
                        take_profit = bar.close + (bar.close - stop_loss) * config.reward_risk
                else:
                    take_profit = bar.close + (bar.close - stop_loss) * config.reward_risk
                if stop_loss < bar.close:
                    position_size = risk_amount / (bar.close - stop_loss)
                    trade = Trade(
                        entry_index=index,
                        entry_time=bar.time,
                        entry_price=bar.close,
                        side="long",
                        size=position_size,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                    )
                    open_trades.append(trade)
                    daily_trade_counts[day] += 1

        if (
            bearish_blocks
            and _has_liquidity_sweep(index, bars, liquidity_history, "short", config.sweep_lookback)
        ):
            block = bearish_blocks[0]
            block_mid = (block.price_top + block.price_bottom) / 2.0
            if block.price_bottom <= bar.close <= block.price_top and bar.close <= block_mid:
                stop_loss = block.price_top
                nearest_zone = _find_nearest_zone_price(liq_snapshot.sellside, bar.close, "short")
                if nearest_zone is not None:
                    take_profit = nearest_zone
                    if take_profit >= bar.close:
                        take_profit = bar.close - (stop_loss - bar.close) * config.reward_risk
                else:
                    take_profit = bar.close - (stop_loss - bar.close) * config.reward_risk
                if stop_loss > bar.close:
                    position_size = risk_amount / (stop_loss - bar.close)
                    trade = Trade(
                        entry_index=index,
                        entry_time=bar.time,
                        entry_price=bar.close,
                        side="short",
                        size=position_size,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                    )
                    open_trades.append(trade)
                    daily_trade_counts[day] += 1

    for trade in open_trades:
        last_bar = bars[-1]
        trade.exit_index = len(bars) - 1
        trade.exit_time = last_bar.time
        trade.exit_price = last_bar.close
        if trade.side == "long":
            trade.pnl = (trade.exit_price - trade.entry_price) * trade.size
        else:
            trade.pnl = (trade.entry_price - trade.exit_price) * trade.size
        trade.exit_reason = "final_mark"
        trades.append(trade)

    return trades


__all__ = [
    "OHLCVBar",
    "LiquidityZone",
    "LiquidityEvent",
    "LiquidityDetectionResult",
    "LiquiditySnapshot",
    "OrderBlock",
    "OrderBlockEvent",
    "OrderBlockDetectionResult",
    "OrderBlockSnapshot",
    "Trade",
    "StrategyConfig",
    "Signal",
    "detect_liquidity_zones",
    "detect_order_blocks",
    "generate_signals",
    "run_backtest",
]
