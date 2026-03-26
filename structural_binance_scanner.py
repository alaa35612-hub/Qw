#!/usr/bin/env python3
"""
Structural Binance Futures scanner aligned to a strict causal research framework.
"""

from __future__ import annotations

import concurrent.futures
import math
import statistics
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

# ============================================================
# 1) CONFIG SECTION
# ============================================================
ANALYSIS_MODE = "multi"  # "single" | "multi"
PRIMARY_TIMEFRAME = "5m"
MULTI_TIMEFRAMES = {
    "context": "4h",      # preparation / regime
    "ignition": "15m",    # ignition window
    "acceptance": "5m",   # hold/retest/continuation validation
}

SYMBOL_LIMIT: Optional[int] = None
REQUEST_TIMEOUT = 8
RETRY_COUNT = 3
RETRY_BACKOFF_SECONDS = 0.8
MAX_WORKERS = 10

LOOKBACK_WINDOWS = {
    "context": 120,
    "ignition": 90,
    "acceptance": 80,
    "single": 120,
    "funding": 40,
    "ratio": 90,
    "oi_hist": 90,
}

ACCEPTANCE_LOOKBACK = 18
BREAKOUT_LEVEL_LOOKBACK = 24
MIN_REQUIRED_POINTS = 24

OUTPUT_MODE = "concise"  # "concise" | "json"
PRINT_MIXED_SIGNALS = False
CONTINUOUS_MODE = True
SCAN_INTERVAL_SECONDS = 60

BASE_FAPI = "https://fapi.binance.com"
BASE_SPOT = "https://api.binance.com"
ANSI_RESET = "\033[0m"
ANSI_GREEN = "\033[92m"
ANSI_BLUE = "\033[94m"
ANSI_YELLOW = "\033[93m"
ANSI_WHITE = "\033[97m"
ANSI_RED = "\033[91m"

# ============================================================
# 2) CONSTANTS AND ENUMS
# ============================================================
VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"
}


class LeadershipType(str, Enum):
    POSITION_LED = "Position-Led Expansion"
    ACCOUNT_LED = "Account-Led Expansion"
    CONSENSUS = "Consensus Bullish Expansion"
    CONTRADICTORY = "Contradictory Structure"
    INSUFFICIENT = "Insufficient Data"


class FlowState(str, Enum):
    SUPPORTIVE_SUSTAINED = "Sustained Buy-Side Flow"
    IGNITION_ONLY = "Ignition without Follow-through"
    WEAK = "Flow-Weak"
    DIVERGENT = "Flow/CVD Divergence"
    INSUFFICIENT = "Insufficient Flow Data"


class BreakoutAcceptanceState(str, Enum):
    ACCEPTED = "Accepted Breakout"
    FAILED = "Fake Breakout"
    NO_BREAKOUT = "No Qualified Breakout"
    INSUFFICIENT = "Insufficient Breakout Data"


class SqueezeState(str, Enum):
    REAL_SHORT_SQUEEZE = "Real Short Squeeze"
    SHORT_COVERING_ONLY = "Ordinary Short Covering"
    FAKE_DERIVATIVES_EXPLOSION = "Fake/Crowded Derivatives Explosion"
    NONE = "No Squeeze Structure"
    INSUFFICIENT = "Insufficient Squeeze Data"


class FinalClassification(str, Enum):
    GENUINE_EARLY_BULLISH = "Genuine Early Bullish Structure"
    POSITION_LED = "Position-Led Bullish Expansion"
    ACCOUNT_LED = "Account-Led Accumulation"
    CONSENSUS = "Consensus Bullish Expansion"
    REAL_SHORT_SQUEEZE = "Real Short Squeeze"
    SHORT_COVERING = "Ordinary Short Covering"
    ACCEPTED_BREAKOUT = "Accepted Breakout"
    FAKE_BREAKOUT = "Fake Breakout"
    FLOW_STRONG_WEAK_STRUCTURE = "Flow-Strong but Structurally Weak"
    MIXED = "Mixed / Contradictory Structure"
    INSUFFICIENT = "Insufficient Data"


POSITIVE_OR_SIGNIFICANT_CLASSES = {
    FinalClassification.GENUINE_EARLY_BULLISH,
    FinalClassification.POSITION_LED,
    FinalClassification.ACCOUNT_LED,
    FinalClassification.CONSENSUS,
    FinalClassification.REAL_SHORT_SQUEEZE,
    FinalClassification.SHORT_COVERING,
    FinalClassification.ACCEPTED_BREAKOUT,
    FinalClassification.FAKE_BREAKOUT,
    FinalClassification.FLOW_STRONG_WEAK_STRUCTURE,
}

CLASS_IMPORTANCE_RANK = {
    FinalClassification.CONSENSUS: 9,
    FinalClassification.POSITION_LED: 8,
    FinalClassification.GENUINE_EARLY_BULLISH: 7,
    FinalClassification.REAL_SHORT_SQUEEZE: 7,
    FinalClassification.ACCEPTED_BREAKOUT: 6,
    FinalClassification.ACCOUNT_LED: 5,
    FinalClassification.SHORT_COVERING: 4,
    FinalClassification.FAKE_BREAKOUT: 3,
    FinalClassification.FLOW_STRONG_WEAK_STRUCTURE: 2,
    FinalClassification.MIXED: 1,
    FinalClassification.INSUFFICIENT: 0,
}

AR_CLASS = {
    FinalClassification.GENUINE_EARLY_BULLISH.value: "بنية صاعدة مبكرة حقيقية",
    FinalClassification.POSITION_LED.value: "توسع صاعد بقيادة المراكز",
    FinalClassification.ACCOUNT_LED.value: "توسع بقيادة الحسابات",
    FinalClassification.CONSENSUS.value: "توسع صاعد توافقي",
    FinalClassification.REAL_SHORT_SQUEEZE.value: "عصر شورت حقيقي",
    FinalClassification.SHORT_COVERING.value: "تغطية شورت عادية",
    FinalClassification.ACCEPTED_BREAKOUT.value: "اختراق مقبول",
    FinalClassification.FAKE_BREAKOUT.value: "اختراق وهمي",
    FinalClassification.FLOW_STRONG_WEAK_STRUCTURE.value: "تدفق قوي لكن البنية ضعيفة",
    FinalClassification.MIXED.value: "بنية متضاربة",
    FinalClassification.INSUFFICIENT.value: "بيانات غير كافية",
}

AR_GENERIC = {
    LeadershipType.POSITION_LED.value: "قيادة بالمراكز",
    LeadershipType.ACCOUNT_LED.value: "قيادة بالحسابات",
    LeadershipType.CONSENSUS.value: "توافق صاعد",
    LeadershipType.CONTRADICTORY.value: "بنية متضاربة",
    LeadershipType.INSUFFICIENT.value: "بيانات غير كافية",
    FlowState.SUPPORTIVE_SUSTAINED.value: "تدفق شرائي مستدام",
    FlowState.IGNITION_ONLY.value: "اشتعال بدون متابعة",
    FlowState.WEAK.value: "تدفق ضعيف",
    FlowState.DIVERGENT.value: "تباين بين التدفق و CVD",
    FlowState.INSUFFICIENT.value: "بيانات تدفق غير كافية",
    BreakoutAcceptanceState.ACCEPTED.value: "اختراق مقبول",
    BreakoutAcceptanceState.FAILED.value: "اختراق فاشل",
    BreakoutAcceptanceState.NO_BREAKOUT.value: "لا يوجد اختراق مؤهل",
    BreakoutAcceptanceState.INSUFFICIENT.value: "بيانات قبول غير كافية",
    SqueezeState.REAL_SHORT_SQUEEZE.value: "عصر شورت حقيقي",
    SqueezeState.SHORT_COVERING_ONLY.value: "تغطية شورت فقط",
    SqueezeState.FAKE_DERIVATIVES_EXPLOSION.value: "انفجار مشتقات مزدحم/وهمي",
    SqueezeState.NONE.value: "لا توجد بنية عصر",
    SqueezeState.INSUFFICIENT.value: "بيانات العصر غير كافية",
}

# ============================================================
# 3) DATACLASSES / TYPED MODELS
# ============================================================
@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_buy_base: float
    taker_buy_quote: float


@dataclass
class RatioPoint:
    ts: int
    long_short_ratio: float
    long_account: float
    short_account: float


@dataclass
class OIHistPoint:
    ts: int
    sum_open_interest: float
    sum_open_interest_value: float


@dataclass
class SymbolTimeframeData:
    symbol: str
    timeframe: str
    futures_candles: List[Candle] = field(default_factory=list)
    spot_candles: List[Candle] = field(default_factory=list)
    oi_hist: List[OIHistPoint] = field(default_factory=list)
    top_position_ratio: List[RatioPoint] = field(default_factory=list)
    top_account_ratio: List[RatioPoint] = field(default_factory=list)
    global_account_ratio: List[RatioPoint] = field(default_factory=list)
    taker_ratio: List[RatioPoint] = field(default_factory=list)
    funding: List[float] = field(default_factory=list)


@dataclass
class PreparationStructure:
    status: str
    oi_gradual_expand: bool
    oi_value_confirm: bool
    positions_improving: bool
    accounts_crowding_ahead: bool
    flow_supportive: bool
    cvd_supportive: Optional[bool]
    funding_crowded: bool
    evidence: List[str]


@dataclass
class SqueezeAssessment:
    state: SqueezeState
    evidence: List[str]


@dataclass
class AcceptanceAssessment:
    state: BreakoutAcceptanceState
    evidence: List[str]


@dataclass
class StructuralResult:
    symbol: str
    timeframe_mode: str
    structure_phase: str
    leadership_type: str
    oi_state: str
    flow_state: str
    breakout_acceptance_state: str
    squeeze_state: str
    final_structural_classification: str
    confidence_score: float
    justifications: List[str]


@dataclass
class DisplayBucket:
    priority: int
    title_ar: str
    color: str


# ============================================================
# 4) BINANCE API CLIENT LAYER
# ============================================================
class BinanceClient:
    def __init__(self, timeout: int = REQUEST_TIMEOUT, retries: int = RETRY_COUNT):
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()

    def _get(self, base: str, path: str, params: Dict[str, Any]) -> Any:
        url = f"{base}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp.json()
                last_err = RuntimeError(f"HTTP {resp.status_code} {url}: {resp.text[:200]}")
            except Exception as exc:
                last_err = exc
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
        raise RuntimeError(f"Request failed for {url}: {last_err}")

    def get_usdt_perpetual_symbols(self) -> List[str]:
        data = self._get(BASE_FAPI, "/fapi/v1/exchangeInfo", {})
        symbols: List[str] = []
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
                symbols.append(s["symbol"])
        return sorted(symbols)

    def get_futures_klines(self, symbol: str, interval: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def get_spot_klines(self, symbol: str, interval: str, limit: int) -> List[Any]:
        return self._get(BASE_SPOT, "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def get_open_interest_hist(self, symbol: str, period: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit})

    def get_top_position_ratio(self, symbol: str, period: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/futures/data/topLongShortPositionRatio", {"symbol": symbol, "period": period, "limit": limit})

    def get_top_account_ratio(self, symbol: str, period: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/futures/data/topLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": limit})

    def get_global_account_ratio(self, symbol: str, period: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": limit})

    def get_taker_ratio(self, symbol: str, period: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/futures/data/takerlongshortRatio", {"symbol": symbol, "period": period, "limit": limit})

    def get_funding(self, symbol: str, limit: int) -> List[Any]:
        return self._get(BASE_FAPI, "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})


# ============================================================
# 5) NORMALIZATION / PARSING LAYER
# ============================================================
def _to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return math.nan


def parse_candles(raw: Sequence[Sequence[Any]]) -> List[Candle]:
    out: List[Candle] = []
    for r in raw:
        if len(r) < 11:
            continue
        out.append(
            Candle(
                open_time=int(r[0]),
                open=_to_float(r[1]),
                high=_to_float(r[2]),
                low=_to_float(r[3]),
                close=_to_float(r[4]),
                volume=_to_float(r[5]),
                quote_volume=_to_float(r[7]),
                taker_buy_base=_to_float(r[9]),
                taker_buy_quote=_to_float(r[10]),
            )
        )
    return out


def parse_oi_hist(raw: Sequence[Dict[str, Any]]) -> List[OIHistPoint]:
    out: List[OIHistPoint] = []
    for r in raw:
        out.append(
            OIHistPoint(
                ts=int(r.get("timestamp", 0)),
                sum_open_interest=_to_float(r.get("sumOpenInterest")),
                sum_open_interest_value=_to_float(r.get("sumOpenInterestValue")),
            )
        )
    return out


def parse_ratio_points(raw: Sequence[Dict[str, Any]]) -> List[RatioPoint]:
    out: List[RatioPoint] = []
    for r in raw:
        out.append(
            RatioPoint(
                ts=int(r.get("timestamp", 0)),
                long_short_ratio=_to_float(r.get("longShortRatio")),
                long_account=_to_float(r.get("longAccount", math.nan)),
                short_account=_to_float(r.get("shortAccount", math.nan)),
            )
        )
    return out


def parse_funding(raw: Sequence[Dict[str, Any]]) -> List[float]:
    return [_to_float(r.get("fundingRate")) for r in raw if "fundingRate" in r]


# ============================================================
# 6) METRIC BUILDERS
# ============================================================
def pct_change(start: float, end: float) -> float:
    if math.isnan(start) or math.isnan(end) or start == 0:
        return math.nan
    return (end - start) / abs(start)


def slope_like(values: Sequence[float]) -> float:
    clean = [v for v in values if not math.isnan(v)]
    if len(clean) < 2:
        return math.nan
    n = len(clean)
    x_mean = (n - 1) / 2
    y_mean = statistics.mean(clean)
    num = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(clean))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return math.nan
    return num / den


def rolling_delta_ratio(values: Sequence[float], split_ratio: float = 0.5) -> float:
    clean = [v for v in values if not math.isnan(v)]
    if len(clean) < 8:
        return math.nan
    split = int(len(clean) * split_ratio)
    left, right = clean[:split], clean[split:]
    if not left or not right:
        return math.nan
    return pct_change(statistics.mean(left), statistics.mean(right))


def compute_taker_buy_pressure(candles: Sequence[Candle]) -> float:
    total_taker_buy = sum(c.taker_buy_quote for c in candles if not math.isnan(c.taker_buy_quote))
    total_quote = sum(c.quote_volume for c in candles if not math.isnan(c.quote_volume))
    if total_quote == 0:
        return math.nan
    return total_taker_buy / total_quote


def derive_spot_cvd(spot_candles: Sequence[Candle]) -> Optional[List[float]]:
    if len(spot_candles) < MIN_REQUIRED_POINTS:
        return None
    cvd: List[float] = []
    acc = 0.0
    for c in spot_candles:
        if any(math.isnan(x) for x in (c.open, c.close, c.quote_volume)):
            continue
        signed = c.quote_volume if c.close >= c.open else -c.quote_volume
        acc += signed
        cvd.append(acc)
    return cvd if len(cvd) >= MIN_REQUIRED_POINTS else None


def is_funding_crowded(funding: Sequence[float]) -> Tuple[bool, str]:
    clean = [f for f in funding if not math.isnan(f)]
    if len(clean) < 8:
        return False, "funding unavailable/limited"
    recent = clean[-8:]
    med = statistics.median(recent)
    p90 = statistics.quantiles(clean, n=10)[8] if len(clean) >= 10 else max(clean)
    crowded = med > p90 and med > 0
    return crowded, f"recent median funding={med:.6f}"


def ratio_trend(points: Sequence[RatioPoint]) -> float:
    return slope_like([p.long_short_ratio for p in points])


def evaluate_oi_state(oi_hist: Sequence[OIHistPoint]) -> Tuple[bool, bool, str]:
    if len(oi_hist) < MIN_REQUIRED_POINTS:
        return False, False, "oi history insufficient"
    oi_vals = [p.sum_open_interest for p in oi_hist]
    oi_value_vals = [p.sum_open_interest_value for p in oi_hist]

    grad_expand = slope_like(oi_vals) > 0 and rolling_delta_ratio(oi_vals) > 0.01
    oi_value_conf = slope_like(oi_value_vals) > 0 if not all(math.isnan(x) for x in oi_value_vals) else False

    detail = (
        f"oi_slope={slope_like(oi_vals):.4f}, oi_delta={rolling_delta_ratio(oi_vals):.3f}, "
        f"oi_value_slope={slope_like(oi_value_vals):.4f}"
    )
    return grad_expand, oi_value_conf, detail


def evaluate_flow_state(futures_candles: Sequence[Candle], spot_candles: Sequence[Candle]) -> Tuple[FlowState, List[str]]:
    evidence: List[str] = []
    if len(futures_candles) < MIN_REQUIRED_POINTS:
        return FlowState.INSUFFICIENT, ["futures candles insufficient"]

    ignition = futures_candles[-12:-6]
    sustain = futures_candles[-6:]
    ign_bp = compute_taker_buy_pressure(ignition)
    sus_bp = compute_taker_buy_pressure(sustain)
    all_bp = compute_taker_buy_pressure(futures_candles[-24:])

    cvd = derive_spot_cvd(spot_candles)
    cvd_support: Optional[bool] = None
    if cvd is not None:
        cvd_support = slope_like(cvd[-24:]) > 0

    evidence.append(f"taker_buy_pressure ignition={ign_bp:.3f}, sustain={sus_bp:.3f}, all={all_bp:.3f}")
    if cvd_support is None:
        evidence.append("spot CVD unavailable")
    else:
        evidence.append(f"spot CVD {'supports' if cvd_support else 'diverges'}")

    ign_strong = not math.isnan(ign_bp) and ign_bp > 0.52
    sustain_strong = not math.isnan(sus_bp) and sus_bp > 0.52

    if ign_strong and sustain_strong and (cvd_support is None or cvd_support):
        return FlowState.SUPPORTIVE_SUSTAINED, evidence
    if ign_strong and not sustain_strong:
        return FlowState.IGNITION_ONLY, evidence
    if ign_strong and cvd_support is False:
        return FlowState.DIVERGENT, evidence
    return FlowState.WEAK, evidence


# ============================================================
# 7) BREAKOUT / ACCEPTANCE ANALYSIS HELPERS
# ============================================================
def detect_acceptance(candles: Sequence[Candle]) -> AcceptanceAssessment:
    if len(candles) < max(BREAKOUT_LEVEL_LOOKBACK, ACCEPTANCE_LOOKBACK) + 6:
        return AcceptanceAssessment(BreakoutAcceptanceState.INSUFFICIENT, ["not enough candles for breakout acceptance"])

    ref = candles[-(ACCEPTANCE_LOOKBACK + BREAKOUT_LEVEL_LOOKBACK):-ACCEPTANCE_LOOKBACK]
    action = candles[-ACCEPTANCE_LOOKBACK:]

    key_level = max(c.high for c in ref)
    closes = [c.close for c in action]
    highs = [c.high for c in action]
    lows = [c.low for c in action]

    breakout_idx = next((i for i, h in enumerate(highs) if h > key_level), None)
    if breakout_idx is None:
        return AcceptanceAssessment(BreakoutAcceptanceState.NO_BREAKOUT, [f"no break above level {key_level:.4f}"])

    post = action[breakout_idx:]
    if len(post) < 5:
        return AcceptanceAssessment(BreakoutAcceptanceState.INSUFFICIENT, ["breakout too recent for acceptance validation"])

    close_above = sum(1 for c in post[:4] if c.close > key_level) >= 2
    fast_reject = any(c.close < key_level for c in post[:3])
    retest_hold = any((c.low <= key_level <= c.close) for c in post[2:8])
    continuation = post[-1].close > key_level and post[-1].close >= max(c.close for c in post[: max(len(post) - 2, 1)])

    ev = [
        f"key_level={key_level:.4f}",
        f"close_above={close_above}",
        f"fast_reject={fast_reject}",
        f"retest_hold={retest_hold}",
        f"continuation={continuation}",
    ]

    if close_above and not fast_reject and (retest_hold or continuation):
        return AcceptanceAssessment(BreakoutAcceptanceState.ACCEPTED, ev)
    if breakout_idx is not None and (fast_reject or not continuation):
        return AcceptanceAssessment(BreakoutAcceptanceState.FAILED, ev)
    return AcceptanceAssessment(BreakoutAcceptanceState.NO_BREAKOUT, ev)


# ============================================================
# 8) SINGLE-TIMEFRAME ANALYZER + PREPARATION
# ============================================================
def detect_preparation_structure(data: SymbolTimeframeData) -> PreparationStructure:
    evidence: List[str] = []

    oi_expand, oi_value_conf, oi_detail = evaluate_oi_state(data.oi_hist)
    evidence.append(oi_detail)

    pos_tr = ratio_trend(data.top_position_ratio)
    acc_tr = ratio_trend(data.top_account_ratio)
    glob_tr = ratio_trend(data.global_account_ratio)

    positions_improving = not math.isnan(pos_tr) and pos_tr > 0
    accounts_crowding_ahead = (
        not math.isnan(acc_tr) and acc_tr > 0 and (math.isnan(pos_tr) or acc_tr > pos_tr * 1.4)
    )
    evidence.append(f"position_ratio_trend={pos_tr:.4f}, account_ratio_trend={acc_tr:.4f}, global_trend={glob_tr:.4f}")

    flow_state, flow_ev = evaluate_flow_state(data.futures_candles, data.spot_candles)
    evidence.extend(flow_ev)
    flow_supportive = flow_state == FlowState.SUPPORTIVE_SUSTAINED

    cvd = derive_spot_cvd(data.spot_candles)
    cvd_supportive: Optional[bool]
    if cvd is None:
        cvd_supportive = None
    else:
        cvd_supportive = slope_like(cvd[-24:]) >= 0

    funding_crowded, funding_note = is_funding_crowded(data.funding)
    evidence.append(funding_note)

    passed_count = sum(
        [
            oi_expand,
            oi_value_conf,
            positions_improving,
            not accounts_crowding_ahead,
            flow_supportive,
            (cvd_supportive is not False),
            not funding_crowded,
        ]
    )
    status = "qualified" if passed_count >= 5 else "not-qualified"

    return PreparationStructure(
        status=status,
        oi_gradual_expand=oi_expand,
        oi_value_confirm=oi_value_conf,
        positions_improving=positions_improving,
        accounts_crowding_ahead=accounts_crowding_ahead,
        flow_supportive=flow_supportive,
        cvd_supportive=cvd_supportive,
        funding_crowded=funding_crowded,
        evidence=evidence,
    )


def detect_squeeze_phase(data: SymbolTimeframeData) -> SqueezeAssessment:
    if len(data.futures_candles) < 36 or len(data.oi_hist) < 24:
        return SqueezeAssessment(SqueezeState.INSUFFICIENT, ["insufficient candles or OI for squeeze analysis"])

    closes = [c.close for c in data.futures_candles]
    oi = [p.sum_open_interest for p in data.oi_hist]
    funding_crowded, funding_note = is_funding_crowded(data.funding)

    p_early = pct_change(closes[-36], closes[-18])
    p_late = pct_change(closes[-18], closes[-1])
    oi_early = pct_change(oi[-24], oi[-12])
    oi_late = pct_change(oi[-12], oi[-1])

    pos_tr = ratio_trend(data.top_position_ratio[-24:]) if len(data.top_position_ratio) >= 24 else math.nan
    acc_tr = ratio_trend(data.top_account_ratio[-24:]) if len(data.top_account_ratio) >= 24 else math.nan
    glob_tr = ratio_trend(data.global_account_ratio[-24:]) if len(data.global_account_ratio) >= 24 else math.nan
    global_ratios = [p.long_short_ratio for p in data.global_account_ratio if not math.isnan(p.long_short_ratio)]
    lower_extreme_short_crowding = False
    if len(global_ratios) >= 24:
        recent_med = statistics.median(global_ratios[-8:])
        q1 = statistics.quantiles(global_ratios, n=4)[0]
        lower_extreme_short_crowding = recent_med <= q1

    ev = [
        f"price_early={p_early:.3f}, price_late={p_late:.3f}, oi_early={oi_early:.3f}, oi_late={oi_late:.3f}",
        f"position_tr={pos_tr:.4f}, account_tr={acc_tr:.4f}, global_tr={glob_tr:.4f}",
        f"lower_extreme_short_crowding={lower_extreme_short_crowding}",
        funding_note,
    ]

    if p_late > 0.01 and oi_late < -0.01:
        rebuild = oi_late > -0.005 or (not math.isnan(pos_tr) and pos_tr > 0)
        if rebuild and not funding_crowded:
            return SqueezeAssessment(SqueezeState.REAL_SHORT_SQUEEZE, ev + ["post-ignition structure rebuilt"])
        return SqueezeAssessment(SqueezeState.SHORT_COVERING_ONLY, ev + ["price rise with OI contraction; no clear rebuild"])

    crowded_explosion = p_late > 0.015 and oi_late > 0.015 and funding_crowded and (math.isnan(pos_tr) or pos_tr <= 0) and acc_tr > 0
    if crowded_explosion:
        return SqueezeAssessment(SqueezeState.FAKE_DERIVATIVES_EXPLOSION, ev + ["crowded expansion without position leadership"])

    return SqueezeAssessment(SqueezeState.NONE, ev)


def determine_leadership(data: SymbolTimeframeData, prep: PreparationStructure) -> LeadershipType:
    if len(data.oi_hist) < MIN_REQUIRED_POINTS:
        return LeadershipType.INSUFFICIENT

    pos_tr = ratio_trend(data.top_position_ratio)
    acc_tr = ratio_trend(data.top_account_ratio)
    glob_tr = ratio_trend(data.global_account_ratio)
    oi_expand, oi_value_conf, _ = evaluate_oi_state(data.oi_hist)

    if oi_expand and oi_value_conf and pos_tr > 0 and (math.isnan(acc_tr) or pos_tr >= acc_tr):
        return LeadershipType.POSITION_LED
    if oi_expand and acc_tr > 0 and (math.isnan(pos_tr) or pos_tr <= 0):
        return LeadershipType.ACCOUNT_LED
    if oi_expand and oi_value_conf and pos_tr > 0 and acc_tr > 0 and glob_tr > 0 and not prep.accounts_crowding_ahead:
        return LeadershipType.CONSENSUS
    return LeadershipType.CONTRADICTORY


# ============================================================
# 9) MULTI-TIMEFRAME ANALYZER
# ============================================================
def merge_multi_tf(
    symbol: str,
    context: SymbolTimeframeData,
    ignition: SymbolTimeframeData,
    acceptance: SymbolTimeframeData,
) -> StructuralResult:
    prep_context = detect_preparation_structure(context)
    prep_ignition = detect_preparation_structure(ignition)

    squeeze = detect_squeeze_phase(ignition)
    acceptance_assess = detect_acceptance(acceptance.futures_candles)
    lead = determine_leadership(context, prep_context)
    flow_state, flow_ev = evaluate_flow_state(acceptance.futures_candles, acceptance.spot_candles)

    final_class = classify_structure(prep_context, prep_ignition, lead, flow_state, acceptance_assess, squeeze)
    just = prep_context.evidence[-4:] + prep_ignition.evidence[-4:] + flow_ev[-2:] + acceptance_assess.evidence + squeeze.evidence[-2:]

    confidence = compute_confidence(prep_context, prep_ignition, lead, flow_state, acceptance_assess, squeeze)

    return StructuralResult(
        symbol=symbol,
        timeframe_mode="multi",
        structure_phase=f"context={prep_context.status}, ignition={prep_ignition.status}",
        leadership_type=lead.value,
        oi_state=f"context_oi_expand={prep_context.oi_gradual_expand}, ignition_oi_expand={prep_ignition.oi_gradual_expand}",
        flow_state=flow_state.value,
        breakout_acceptance_state=acceptance_assess.state.value,
        squeeze_state=squeeze.state.value,
        final_structural_classification=final_class.value,
        confidence_score=confidence,
        justifications=just,
    )


# ============================================================
# 10) STRUCTURAL CLASSIFICATION ENGINE
# ============================================================
def classify_structure(
    prep_main: PreparationStructure,
    prep_secondary: PreparationStructure,
    leadership: LeadershipType,
    flow_state: FlowState,
    acceptance: AcceptanceAssessment,
    squeeze: SqueezeAssessment,
) -> FinalClassification:
    prep_strong = prep_main.status == "qualified" and prep_secondary.status == "qualified"
    leadership_supportive = leadership in {LeadershipType.POSITION_LED, LeadershipType.CONSENSUS, LeadershipType.ACCOUNT_LED}
    flow_supportive = flow_state == FlowState.SUPPORTIVE_SUSTAINED
    flow_ignition_only = flow_state == FlowState.IGNITION_ONLY

    if prep_main.status == "not-qualified" and prep_secondary.status == "not-qualified" and flow_state == FlowState.INSUFFICIENT:
        return FinalClassification.INSUFFICIENT

    if squeeze.state == SqueezeState.REAL_SHORT_SQUEEZE:
        return FinalClassification.REAL_SHORT_SQUEEZE
    if squeeze.state == SqueezeState.SHORT_COVERING_ONLY:
        return FinalClassification.SHORT_COVERING

    if acceptance.state == BreakoutAcceptanceState.FAILED:
        if flow_state in (FlowState.SUPPORTIVE_SUSTAINED, FlowState.IGNITION_ONLY):
            return FinalClassification.FLOW_STRONG_WEAK_STRUCTURE
        return FinalClassification.FAKE_BREAKOUT

    if acceptance.state == BreakoutAcceptanceState.ACCEPTED:
        if leadership == LeadershipType.CONSENSUS and prep_strong and flow_supportive:
            return FinalClassification.CONSENSUS
        if leadership == LeadershipType.POSITION_LED and prep_strong and flow_supportive:
            return FinalClassification.POSITION_LED
        if leadership == LeadershipType.ACCOUNT_LED and prep_main.status == "qualified" and flow_supportive:
            return FinalClassification.ACCOUNT_LED
        if flow_ignition_only or flow_state in {FlowState.WEAK, FlowState.DIVERGENT}:
            return FinalClassification.FLOW_STRONG_WEAK_STRUCTURE
        if not leadership_supportive or not prep_main.status == "qualified":
            return FinalClassification.MIXED
        return FinalClassification.ACCEPTED_BREAKOUT

    if prep_main.status == "qualified":
        if leadership == LeadershipType.POSITION_LED:
            return FinalClassification.GENUINE_EARLY_BULLISH
        if leadership == LeadershipType.ACCOUNT_LED:
            return FinalClassification.ACCOUNT_LED
        if leadership == LeadershipType.CONSENSUS:
            return FinalClassification.CONSENSUS

    if flow_state == FlowState.IGNITION_ONLY:
        return FinalClassification.FLOW_STRONG_WEAK_STRUCTURE

    return FinalClassification.MIXED


def compute_confidence(
    prep_main: PreparationStructure,
    prep_secondary: PreparationStructure,
    leadership: LeadershipType,
    flow_state: FlowState,
    acceptance: AcceptanceAssessment,
    squeeze: SqueezeAssessment,
) -> float:
    checks = []
    checks.append(prep_main.status == "qualified")
    checks.append(prep_secondary.status == "qualified")
    checks.append(leadership in {LeadershipType.POSITION_LED, LeadershipType.CONSENSUS, LeadershipType.ACCOUNT_LED})
    checks.append(flow_state == FlowState.SUPPORTIVE_SUSTAINED)
    checks.append(acceptance.state == BreakoutAcceptanceState.ACCEPTED)
    checks.append(squeeze.state in {SqueezeState.REAL_SHORT_SQUEEZE, SqueezeState.SHORT_COVERING_ONLY})
    return round(sum(checks) / len(checks), 3)


# ============================================================
# 11) REPORTING / PRINTING LAYER
# ============================================================
def should_print(result: StructuralResult) -> bool:
    c = FinalClassification(result.final_structural_classification)
    if c in POSITIVE_OR_SIGNIFICANT_CLASSES:
        return True
    if PRINT_MIXED_SIGNALS and c == FinalClassification.MIXED:
        return True
    return False


def to_ar(text: str) -> str:
    if text in AR_CLASS:
        return AR_CLASS[text]
    if text in AR_GENERIC:
        return AR_GENERIC[text]
    return text


def bucket_for_result(result: StructuralResult) -> DisplayBucket:
    classification = FinalClassification(result.final_structural_classification)
    has_lower_extreme = any("lower_extreme_short_crowding=True" in j for j in result.justifications)
    accepted = result.breakout_acceptance_state == BreakoutAcceptanceState.ACCEPTED.value

    if classification == FinalClassification.POSITION_LED and accepted:
        return DisplayBucket(1, "الحالة 1 (الأقوى): توسع صاعد بقيادة المراكز + قبول اختراق واضح", ANSI_GREEN)

    if classification == FinalClassification.REAL_SHORT_SQUEEZE and has_lower_extreme:
        return DisplayBucket(2, "الحالة 2: عصر شورت من طرف سفلي يتحول إلى إعادة بناء حقيقية", ANSI_BLUE)

    if classification == FinalClassification.ACCEPTED_BREAKOUT and ("qualified" in result.structure_phase):
        return DisplayBucket(3, "الحالة 3: اختراق مقبول نظيف بعد تهيؤ بنيوي واضح", ANSI_YELLOW)

    leadership_ok = result.leadership_type in {
        LeadershipType.POSITION_LED.value,
        LeadershipType.CONSENSUS.value,
        LeadershipType.ACCOUNT_LED.value,
    }
    golden_rule_match = (
        result.breakout_acceptance_state == BreakoutAcceptanceState.ACCEPTED.value
        and result.flow_state == FlowState.SUPPORTIVE_SUSTAINED.value
        and leadership_ok
        and "qualified" in result.structure_phase
    )
    if golden_rule_match:
        return DisplayBucket(4, "القاعدة الذهبية: تهيؤ + قيادة + تدفق داعم + قبول (إشارة عالية)", ANSI_RED)

    if classification == FinalClassification.CONSENSUS:
        return DisplayBucket(5, "الحالة 4: توسع صاعد توافقي غير مزدحم", ANSI_WHITE)

    return DisplayBucket(9, "حالات هيكلية أخرى (أقل أولوية للدخول)", ANSI_WHITE)


def print_result_ar(result: StructuralResult, color: str) -> None:
    if OUTPUT_MODE == "json":
        print(asdict(result))
        return

    print(
        f"{color}{result.symbol} | التصنيف النهائي: {to_ar(result.final_structural_classification)} | "
        f"القيادة: {to_ar(result.leadership_type)} | التدفق: {to_ar(result.flow_state)} | "
        f"قبول الاختراق: {to_ar(result.breakout_acceptance_state)} | حالة العصر: {to_ar(result.squeeze_state)} | "
        f"الثقة: {result.confidence_score:.3f}{ANSI_RESET}"
    )
    print(f"{color}  ملخص بنيوي: {result.structure_phase} | حالة OI: {result.oi_state}{ANSI_RESET}")
    for j in result.justifications[:6]:
        print(f"{color}  - دليل: {j}{ANSI_RESET}")


# ============================================================
# 12) MAIN ENTRYPOINT
# ============================================================
def safe_fetch_symbol_tf(client: BinanceClient, symbol: str, timeframe: str, lookback: int) -> Optional[SymbolTimeframeData]:
    try:
        if timeframe not in VALID_INTERVALS:
            return None

        futures_raw = client.get_futures_klines(symbol, timeframe, lookback)
        futures_candles = parse_candles(futures_raw)

        try:
            spot_raw = client.get_spot_klines(symbol, timeframe, lookback)
            spot_candles = parse_candles(spot_raw)
        except Exception:
            spot_candles = []

        oi_raw = client.get_open_interest_hist(symbol, timeframe, LOOKBACK_WINDOWS["oi_hist"])
        top_pos_raw = client.get_top_position_ratio(symbol, timeframe, LOOKBACK_WINDOWS["ratio"])
        top_acc_raw = client.get_top_account_ratio(symbol, timeframe, LOOKBACK_WINDOWS["ratio"])
        glob_raw = client.get_global_account_ratio(symbol, timeframe, LOOKBACK_WINDOWS["ratio"])
        taker_raw = client.get_taker_ratio(symbol, timeframe, LOOKBACK_WINDOWS["ratio"])
        funding_raw = client.get_funding(symbol, LOOKBACK_WINDOWS["funding"])

        data = SymbolTimeframeData(
            symbol=symbol,
            timeframe=timeframe,
            futures_candles=futures_candles,
            spot_candles=spot_candles,
            oi_hist=parse_oi_hist(oi_raw),
            top_position_ratio=parse_ratio_points(top_pos_raw),
            top_account_ratio=parse_ratio_points(top_acc_raw),
            global_account_ratio=parse_ratio_points(glob_raw),
            taker_ratio=parse_ratio_points(taker_raw),
            funding=parse_funding(funding_raw),
        )
        return data
    except Exception:
        return None


def analyze_single(symbol: str, data: SymbolTimeframeData) -> StructuralResult:
    prep = detect_preparation_structure(data)
    squeeze = detect_squeeze_phase(data)
    acceptance = detect_acceptance(data.futures_candles)
    leadership = determine_leadership(data, prep)
    flow_state, flow_ev = evaluate_flow_state(data.futures_candles, data.spot_candles)

    final = classify_structure(prep, prep, leadership, flow_state, acceptance, squeeze)
    confidence = compute_confidence(prep, prep, leadership, flow_state, acceptance, squeeze)

    just = prep.evidence[-6:] + flow_ev[-2:] + acceptance.evidence + squeeze.evidence[-2:]

    return StructuralResult(
        symbol=symbol,
        timeframe_mode="single",
        structure_phase=prep.status,
        leadership_type=leadership.value,
        oi_state=f"oi_expand={prep.oi_gradual_expand}, oi_value_confirm={prep.oi_value_confirm}",
        flow_state=flow_state.value,
        breakout_acceptance_state=acceptance.state.value,
        squeeze_state=squeeze.state.value,
        final_structural_classification=final.value,
        confidence_score=confidence,
        justifications=just,
    )


def run_scanner() -> None:
    client = BinanceClient()
    symbols = client.get_usdt_perpetual_symbols()
    if SYMBOL_LIMIT:
        symbols = symbols[:SYMBOL_LIMIT]

    results: List[StructuralResult] = []

    if ANALYSIS_MODE == "single":
        tf = PRIMARY_TIMEFRAME
        lookback = LOOKBACK_WINDOWS["single"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fut_map = {ex.submit(safe_fetch_symbol_tf, client, s, tf, lookback): s for s in symbols}
            for fut in concurrent.futures.as_completed(fut_map):
                symbol = fut_map[fut]
                data = fut.result()
                if data is None:
                    continue
                result = analyze_single(symbol, data)
                if should_print(result):
                    results.append(result)

    else:
        ctx_tf = MULTI_TIMEFRAMES["context"]
        ign_tf = MULTI_TIMEFRAMES["ignition"]
        acc_tf = MULTI_TIMEFRAMES["acceptance"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            tasks: Dict[concurrent.futures.Future, Tuple[str, str]] = {}
            for symbol in symbols:
                tasks[ex.submit(safe_fetch_symbol_tf, client, symbol, ctx_tf, LOOKBACK_WINDOWS["context"])] = (symbol, "context")
                tasks[ex.submit(safe_fetch_symbol_tf, client, symbol, ign_tf, LOOKBACK_WINDOWS["ignition"])] = (symbol, "ignition")
                tasks[ex.submit(safe_fetch_symbol_tf, client, symbol, acc_tf, LOOKBACK_WINDOWS["acceptance"])] = (symbol, "acceptance")

            assembled: Dict[str, Dict[str, SymbolTimeframeData]] = {}
            for fut in concurrent.futures.as_completed(tasks):
                symbol, kind = tasks[fut]
                data = fut.result()
                if data is None:
                    continue
                assembled.setdefault(symbol, {})[kind] = data

        for symbol, parts in assembled.items():
            if {"context", "ignition", "acceptance"} - parts.keys():
                continue
            result = merge_multi_tf(symbol, parts["context"], parts["ignition"], parts["acceptance"])
            if should_print(result):
                results.append(result)

    ranked: List[Tuple[DisplayBucket, StructuralResult]] = [(bucket_for_result(r), r) for r in results]
    ranked.sort(
        key=lambda x: (
            x[0].priority,
            -CLASS_IMPORTANCE_RANK.get(FinalClassification(x[1].final_structural_classification), 0),
            -x[1].confidence_score,
            x[1].symbol,
        )
    )

    current_title = None
    for bucket, result in ranked:
        if bucket.title_ar != current_title:
            current_title = bucket.title_ar
            print(f"\n{bucket.color}{'=' * 90}\n{bucket.title_ar}\n{'=' * 90}{ANSI_RESET}")
        print_result_ar(result, bucket.color)


if __name__ == "__main__":
    if CONTINUOUS_MODE:
        cycle = 1
        while True:
            print(f"\n{ANSI_WHITE}{'=' * 90}\nبدء دورة المسح رقم: {cycle}\n{'=' * 90}{ANSI_RESET}")
            try:
                run_scanner()
            except Exception as exc:
                print(f"{ANSI_RED}خطأ في دورة المسح: {exc}{ANSI_RESET}")
            cycle += 1
            print(f"{ANSI_WHITE}انتظار {SCAN_INTERVAL_SECONDS} ثانية قبل الدورة التالية...{ANSI_RESET}")
            time.sleep(SCAN_INTERVAL_SECONDS)
    else:
        run_scanner()
