#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import math
import statistics
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

# ============================================================
# 1) CONFIG
# ============================================================
ANALYSIS_MODE = "multi"  # single | multi
PRIMARY_TIMEFRAME = "5m"
MULTI_TIMEFRAMES = {"context": "4h", "ignition": "15m", "acceptance": "5m"}

CONTINUOUS_MODE = True
SCAN_INTERVAL_SECONDS = 60

SYMBOL_LIMIT: Optional[int] = None
MAX_WORKERS = 10
REQUEST_TIMEOUT = 8
RETRY_COUNT = 3
RETRY_BACKOFF_SECONDS = 0.75

LOOKBACK_WINDOWS = {
    "single": 140,
    "context": 160,
    "ignition": 120,
    "acceptance": 100,
    "oi": 120,
    "ratio": 120,
    "funding": 60,
}

BREAKOUT_LEVEL_LOOKBACK = 36
ACCEPTANCE_LOOKBACK = 24
MIN_POINTS = 30
OUTPUT_MODE = "concise"  # concise | json

BASE_FAPI = "https://fapi.binance.com"
BASE_SPOT = "https://api.binance.com"

ANSI_RESET = "\033[0m"
ANSI_GREEN = "\033[92m"
ANSI_BLUE = "\033[94m"
ANSI_YELLOW = "\033[93m"
ANSI_RED = "\033[91m"
ANSI_WHITE = "\033[97m"


# ============================================================
# 2) ENUMS
# ============================================================
class OIState(str, Enum):
    GRADUAL_EXPANSION = "gradual_expansion"
    LATE_EXPANSION = "late_expansion"
    ISOLATED_SPIKE = "isolated_spike"
    CONTRACTION = "contraction"
    FLAT = "flat"
    CONTRADICTORY = "contradictory"


class FlowState(str, Enum):
    SUSTAINED_SUPPORTIVE = "sustained_supportive_flow"
    IGNITION_ONLY = "ignition_only"
    WEAK = "weak_flow"
    DIVERGENT = "divergent_flow"
    INSUFFICIENT = "insufficient_data"


class PreparationState(str, Enum):
    QUALIFIED_POSITION_LED_BUILD = "qualified_position_led_build"
    QUALIFIED_CONSENSUS_BUILD = "qualified_consensus_build"
    LATE_CROWDED_BUILD = "late_crowded_build"
    WEAK_BUILD = "weak_build"
    CONTRADICTORY_BUILD = "contradictory_build"
    INSUFFICIENT_DATA = "insufficient_data"


class LeadershipState(str, Enum):
    POSITION_LED_CLEAN = "POSITION_LED_CLEAN"
    POSITION_LED_WEAK = "POSITION_LED_WEAK"
    ACCOUNT_LED_EARLY = "ACCOUNT_LED_EARLY"
    ACCOUNT_LED_CROWDED = "ACCOUNT_LED_CROWDED"
    CONSENSUS_HEALTHY = "CONSENSUS_HEALTHY"
    CONSENSUS_CROWDED = "CONSENSUS_CROWDED"
    MIXED = "MIXED"
    INSUFFICIENT = "INSUFFICIENT"


class AcceptanceState(str, Enum):
    ACCEPTED_CLEAN = "accepted_clean"
    ACCEPTED_WEAK = "accepted_weak"
    BREAKOUT_NO_ACCEPTANCE = "breakout_no_acceptance"
    FAILED_RETEST = "failed_retest"
    WICK_ONLY_BREAKOUT = "wick_only_breakout"
    INSUFFICIENT_DATA = "insufficient_data"


class SqueezeState(str, Enum):
    REAL_SHORT_SQUEEZE = "real_short_squeeze"
    SHORT_COVERING_ONLY = "short_covering_only"
    FAKE_DERIVATIVES_EXPLOSION = "fake_derivatives_explosion"
    MIXED_SQUEEZE_STRUCTURE = "mixed_squeeze_structure"
    INSUFFICIENT_DATA = "insufficient_data"


class StructuralClass(str, Enum):
    GENUINE_EARLY_BULLISH = "Genuine Early Bullish Structure"
    POSITION_LED_EXPANSION = "Position-Led Expansion"
    CONSENSUS_BULLISH_EXPANSION = "Consensus Bullish Expansion"
    ACCEPTED_BREAKOUT = "Accepted Breakout"
    REAL_SHORT_SQUEEZE = "Real Short Squeeze"
    MIXED = "Mixed / Contradictory Structure"
    INSUFFICIENT = "Insufficient Data"


class Actionability(str, Enum):
    TRADABLE_BULLISH_CANDIDATE = "tradable_bullish_candidate"
    WATCHLIST_ONLY = "watchlist_only"
    REJECT = "reject"
    INSUFFICIENT_DATA = "insufficient_data"


class EvidenceQuality(str, Enum):
    HIGH = "high_evidence_quality"
    MEDIUM = "medium_evidence_quality"
    LOW = "low_evidence_quality"
    UNRELIABLE = "unreliable"


VALID_BULLISH_CLASSES = {
    StructuralClass.GENUINE_EARLY_BULLISH,
    StructuralClass.POSITION_LED_EXPANSION,
    StructuralClass.CONSENSUS_BULLISH_EXPANSION,
    StructuralClass.ACCEPTED_BREAKOUT,
    StructuralClass.REAL_SHORT_SQUEEZE,
}


# ============================================================
# 3) MODELS
# ============================================================
@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float
    taker_buy_quote: float


@dataclass
class OIHistPoint:
    ts: int
    oi: float
    oi_value: float


@dataclass
class RatioPoint:
    ts: int
    ratio: float


@dataclass
class SymbolTFData:
    symbol: str
    timeframe: str
    futures: List[Candle] = field(default_factory=list)
    spot: List[Candle] = field(default_factory=list)
    oi_hist: List[OIHistPoint] = field(default_factory=list)
    top_pos: List[RatioPoint] = field(default_factory=list)
    top_acc: List[RatioPoint] = field(default_factory=list)
    global_acc: List[RatioPoint] = field(default_factory=list)
    taker_ratio: List[RatioPoint] = field(default_factory=list)
    funding: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class PreparationResult:
    state: PreparationState
    oi_state: OIState
    leadership: LeadershipState
    evidence: List[str]


@dataclass
class AcceptanceResult:
    state: AcceptanceState
    evidence: List[str]


@dataclass
class SqueezeResult:
    state: SqueezeState
    evidence: List[str]


@dataclass
class SymbolResult:
    symbol: str
    timeframe_mode: str
    structure_phase: str
    leadership_type: str
    oi_state: str
    flow_state: str
    breakout_acceptance_state: str
    squeeze_state: str
    final_structural_classification: str
    confidence_score: str
    actionability: str
    structural_classification: str
    reasons: List[str]


# ============================================================
# 4) CLIENT
# ============================================================
class BinanceClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    def _get(self, base: str, path: str, params: Dict[str, Any]) -> Any:
        url = f"{base}{path}"
        err: Optional[Exception] = None
        for i in range(RETRY_COUNT):
            try:
                r = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return r.json()
                err = RuntimeError(f"http_{r.status_code}")
            except Exception as e:
                err = e
            time.sleep(RETRY_BACKOFF_SECONDS * (i + 1))
        raise RuntimeError(f"api_failure:{path}:{err}")

    def symbols(self) -> List[str]:
        data = self._get(BASE_FAPI, "/fapi/v1/exchangeInfo", {})
        out: List[str] = []
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
                out.append(s["symbol"])
        return sorted(out)

    def futures_klines(self, symbol: str, interval: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def spot_klines(self, symbol: str, interval: str, limit: int) -> Any:
        return self._get(BASE_SPOT, "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def oi_hist(self, symbol: str, period: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit})

    def top_pos(self, symbol: str, period: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/futures/data/topLongShortPositionRatio", {"symbol": symbol, "period": period, "limit": limit})

    def top_acc(self, symbol: str, period: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/futures/data/topLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": limit})

    def global_acc(self, symbol: str, period: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": limit})

    def taker_ratio(self, symbol: str, period: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/futures/data/takerlongshortRatio", {"symbol": symbol, "period": period, "limit": limit})

    def funding(self, symbol: str, limit: int) -> Any:
        return self._get(BASE_FAPI, "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})


# ============================================================
# 5) PARSING
# ============================================================
def f(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return math.nan


def parse_candles(raw: Sequence[Sequence[Any]]) -> List[Candle]:
    out: List[Candle] = []
    for r in raw:
        if len(r) < 11:
            continue
        out.append(Candle(int(r[0]), f(r[1]), f(r[2]), f(r[3]), f(r[4]), f(r[7]), f(r[10])))
    return out


def parse_oi(raw: Sequence[Dict[str, Any]]) -> List[OIHistPoint]:
    return [OIHistPoint(int(x.get("timestamp", 0)), f(x.get("sumOpenInterest")), f(x.get("sumOpenInterestValue"))) for x in raw]


def parse_ratio(raw: Sequence[Dict[str, Any]]) -> List[RatioPoint]:
    return [RatioPoint(int(x.get("timestamp", 0)), f(x.get("longShortRatio"))) for x in raw]


def parse_funding(raw: Sequence[Dict[str, Any]]) -> List[float]:
    return [f(x.get("fundingRate")) for x in raw if "fundingRate" in x]


# ============================================================
# 6) METRICS
# ============================================================
def slope(values: Sequence[float]) -> float:
    arr = [x for x in values if not math.isnan(x)]
    if len(arr) < 3:
        return math.nan
    n = len(arr)
    xm = (n - 1) / 2
    ym = statistics.mean(arr)
    num = sum((i - xm) * (y - ym) for i, y in enumerate(arr))
    den = sum((i - xm) ** 2 for i in range(n))
    return num / den if den else math.nan


def pct(a: float, b: float) -> float:
    if math.isnan(a) or math.isnan(b) or a == 0:
        return math.nan
    return (b - a) / abs(a)


def phase_split(vals: Sequence[float]) -> Tuple[List[float], List[float]]:
    m = max(len(vals) // 2, 1)
    return list(vals[:m]), list(vals[m:])


def proxy_spot_cvd(spot: Sequence[Candle]) -> Optional[List[float]]:
    if len(spot) < MIN_POINTS:
        return None
    out: List[float] = []
    acc = 0.0
    for c in spot:
        if any(math.isnan(x) for x in (c.open, c.close, c.quote_volume)):
            continue
        acc += c.quote_volume if c.close >= c.open else -c.quote_volume
        out.append(acc)
    return out if len(out) >= MIN_POINTS else None


def funding_context(funding: Sequence[float]) -> str:
    arr = [x for x in funding if not math.isnan(x)]
    if len(arr) < 8:
        return "insufficient_funding_data"
    recent = arr[-max(2, len(arr) // 4):]
    base = arr[:-len(recent)] if len(arr) > len(recent) else arr
    if not base:
        return "neutral_funding"
    if statistics.median(recent) > statistics.median(base):
        return "crowding_risk"
    if statistics.median(recent) < statistics.median(base):
        return "de_crowding"
    return "neutral_funding"


# ============================================================
# 7) ANALYSIS HELPERS
# ============================================================
def interpret_oi(oi_hist: Sequence[OIHistPoint]) -> Tuple[OIState, str]:
    vals = [x.oi for x in oi_hist if not math.isnan(x.oi)]
    if len(vals) < MIN_POINTS:
        return OIState.CONTRADICTORY, "missing_endpoint_data:oi"
    first, second = phase_split(vals)
    s1, s2 = slope(first), slope(second)
    deltas = [pct(vals[i - 1], vals[i]) for i in range(1, len(vals)) if not math.isnan(pct(vals[i - 1], vals[i]))]
    if len(deltas) < 6:
        return OIState.FLAT, "insufficient_klines_for_oi_deltas"
    q1, q3 = statistics.quantiles(deltas, n=4)[0], statistics.quantiles(deltas, n=4)[2]

    if s2 > s1 and statistics.median(deltas[-len(second):]) >= statistics.median(deltas[:len(first)]):
        return OIState.GRADUAL_EXPANSION, "oi_second_phase_strengthening"
    if s2 > 0 and s1 <= 0:
        return OIState.LATE_EXPANSION, "oi_late_phase_expansion"
    if max(deltas) > q3 and statistics.median(deltas[-len(second):]) < statistics.median(deltas[:len(first)]):
        return OIState.ISOLATED_SPIKE, "oi_spike_without_consistency"
    if statistics.median(deltas[-len(second):]) < q1:
        return OIState.CONTRACTION, "oi_contraction_phase"
    if abs(s2) < abs(s1):
        return OIState.FLAT, "oi_flattening"
    return OIState.CONTRADICTORY, "oi_contradictory"


def evaluate_leadership(data: SymbolTFData, oi_state: OIState) -> LeadershipState:
    if len(data.top_pos) < MIN_POINTS or len(data.top_acc) < MIN_POINTS or len(data.global_acc) < MIN_POINTS:
        return LeadershipState.INSUFFICIENT
    pos_vals = [x.ratio for x in data.top_pos if not math.isnan(x.ratio)]
    acc_vals = [x.ratio for x in data.top_acc if not math.isnan(x.ratio)]
    glob_vals = [x.ratio for x in data.global_acc if not math.isnan(x.ratio)]
    if min(len(pos_vals), len(acc_vals), len(glob_vals)) < MIN_POINTS:
        return LeadershipState.INSUFFICIENT

    ps1, ps2 = slope(phase_split(pos_vals)[0]), slope(phase_split(pos_vals)[1])
    as1, as2 = slope(phase_split(acc_vals)[0]), slope(phase_split(acc_vals)[1])
    gs2 = slope(phase_split(glob_vals)[1])

    if oi_state == OIState.GRADUAL_EXPANSION and ps2 > ps1 and ps2 > as2:
        return LeadershipState.POSITION_LED_CLEAN
    if ps2 > 0 and as2 <= ps2:
        return LeadershipState.POSITION_LED_WEAK
    if as2 > 0 and ps2 <= 0:
        return LeadershipState.ACCOUNT_LED_EARLY if gs2 <= as2 else LeadershipState.ACCOUNT_LED_CROWDED
    if ps2 > 0 and as2 > 0 and gs2 > 0:
        return LeadershipState.CONSENSUS_HEALTHY if as2 <= ps2 else LeadershipState.CONSENSUS_CROWDED
    return LeadershipState.MIXED


def evaluate_flow(data: SymbolTFData) -> Tuple[FlowState, List[str]]:
    ev: List[str] = []
    if len(data.futures) < MIN_POINTS or len(data.taker_ratio) < MIN_POINTS:
        return FlowState.INSUFFICIENT, ["unavailable_taker_data"]

    taker_api = [x.ratio for x in data.taker_ratio if not math.isnan(x.ratio)]
    if len(taker_api) < MIN_POINTS:
        return FlowState.INSUFFICIENT, ["unavailable_taker_data"]

    tbp = []
    for c in data.futures:
        if c.quote_volume > 0 and not math.isnan(c.taker_buy_quote):
            tbp.append(c.taker_buy_quote / c.quote_volume)
    if len(tbp) < MIN_POINTS:
        return FlowState.INSUFFICIENT, ["insufficient_klines"]

    tk1, tk2 = phase_split(taker_api)
    bp1, bp2 = phase_split(tbp)
    tk_s1, tk_s2 = slope(tk1), slope(tk2)
    bp_s1, bp_s2 = slope(bp1), slope(bp2)
    ev.append(f"taker_api_phase_slope={tk_s1:.6f}->{tk_s2:.6f}")
    ev.append(f"candle_taker_pressure_slope={bp_s1:.6f}->{bp_s2:.6f}")

    cvd = proxy_spot_cvd(data.spot)
    if cvd is None:
        ev.append("proxy_spot_cvd:unavailable_spot_data")
        cvd_support = None
    else:
        cvd_support = slope(phase_split(cvd)[1]) >= slope(phase_split(cvd)[0])
        ev.append(f"proxy_spot_cvd_support={cvd_support}")

    if tk_s2 > tk_s1 and bp_s2 >= bp_s1 and (cvd_support is not False):
        return FlowState.SUSTAINED_SUPPORTIVE, ev
    if tk_s2 > tk_s1 and bp_s2 < bp_s1:
        return FlowState.IGNITION_ONLY, ev
    if tk_s2 > tk_s1 and cvd_support is False:
        return FlowState.DIVERGENT, ev
    return FlowState.WEAK, ev


def detect_preparation_structure(data: SymbolTFData) -> PreparationResult:
    oi_state, oi_ev = interpret_oi(data.oi_hist)
    leader = evaluate_leadership(data, oi_state)
    flow_state, flow_ev = evaluate_flow(data)
    fctx = funding_context(data.funding)
    evidence = [oi_ev, f"leadership={leader.value}", f"funding_context={fctx}"] + flow_ev

    if oi_state == OIState.GRADUAL_EXPANSION and leader == LeadershipState.POSITION_LED_CLEAN and flow_state == FlowState.SUSTAINED_SUPPORTIVE and fctx != "crowding_risk":
        return PreparationResult(PreparationState.QUALIFIED_POSITION_LED_BUILD, oi_state, leader, evidence)
    if oi_state in {OIState.GRADUAL_EXPANSION, OIState.LATE_EXPANSION} and leader == LeadershipState.CONSENSUS_HEALTHY and flow_state in {FlowState.SUSTAINED_SUPPORTIVE, FlowState.IGNITION_ONLY} and fctx != "crowding_risk":
        return PreparationResult(PreparationState.QUALIFIED_CONSENSUS_BUILD, oi_state, leader, evidence)
    if leader in {LeadershipState.ACCOUNT_LED_CROWDED, LeadershipState.CONSENSUS_CROWDED} or fctx == "crowding_risk":
        return PreparationResult(PreparationState.LATE_CROWDED_BUILD, oi_state, leader, evidence)
    if flow_state in {FlowState.WEAK, FlowState.IGNITION_ONLY}:
        return PreparationResult(PreparationState.WEAK_BUILD, oi_state, leader, evidence)
    if leader == LeadershipState.INSUFFICIENT:
        return PreparationResult(PreparationState.INSUFFICIENT_DATA, oi_state, leader, evidence)
    return PreparationResult(PreparationState.CONTRADICTORY_BUILD, oi_state, leader, evidence)


def detect_acceptance(data: SymbolTFData, flow_state: FlowState) -> AcceptanceResult:
    if len(data.futures) < BREAKOUT_LEVEL_LOOKBACK + ACCEPTANCE_LOOKBACK:
        return AcceptanceResult(AcceptanceState.INSUFFICIENT_DATA, ["insufficient_klines"])

    ref = data.futures[-(BREAKOUT_LEVEL_LOOKBACK + ACCEPTANCE_LOOKBACK):-ACCEPTANCE_LOOKBACK]
    act = data.futures[-ACCEPTANCE_LOOKBACK:]
    level = max(c.high for c in ref)

    breakout_close_idx = next((i for i, c in enumerate(act) if c.close > level), None)
    breakout_wick_idx = next((i for i, c in enumerate(act) if c.high > level), None)

    if breakout_close_idx is None and breakout_wick_idx is not None:
        return AcceptanceResult(AcceptanceState.WICK_ONLY_BREAKOUT, [f"level={level:.8f}"])
    if breakout_close_idx is None:
        return AcceptanceResult(AcceptanceState.BREAKOUT_NO_ACCEPTANCE, [f"level={level:.8f}"])

    post = act[breakout_close_idx:]
    if len(post) < 5:
        return AcceptanceResult(AcceptanceState.INSUFFICIENT_DATA, ["post_breakout_too_short"])

    hold = all(c.close >= level for c in post[:3])
    retest = any(c.low <= level <= c.close for c in post[2:])
    failed_retest = any(c.low <= level and c.close < level for c in post[2:])

    oi_vals = [x.oi for x in data.oi_hist if not math.isnan(x.oi)]
    oi_ok = True
    if len(oi_vals) >= MIN_POINTS:
        a, b = phase_split(oi_vals)
        oi_ok = slope(b) >= slope(a) or statistics.median(b) >= statistics.median(a)

    funding_ok = funding_context(data.funding) != "crowding_risk"
    flow_ok = flow_state == FlowState.SUSTAINED_SUPPORTIVE

    ev = [f"breakout_level={level:.8f}", f"hold={hold}", f"retest={retest}", f"oi_ok={oi_ok}", f"flow_ok={flow_ok}", f"funding_ok={funding_ok}"]

    if hold and (retest or post[-1].close >= statistics.median([x.close for x in post])) and oi_ok and flow_ok and funding_ok:
        return AcceptanceResult(AcceptanceState.ACCEPTED_CLEAN, ev)
    if (hold or retest) and oi_ok and funding_ok:
        return AcceptanceResult(AcceptanceState.ACCEPTED_WEAK, ev)
    if failed_retest:
        return AcceptanceResult(AcceptanceState.FAILED_RETEST, ev)
    return AcceptanceResult(AcceptanceState.BREAKOUT_NO_ACCEPTANCE, ev)


def detect_squeeze_phase(data: SymbolTFData) -> SqueezeResult:
    if len(data.futures) < MIN_POINTS or len(data.oi_hist) < MIN_POINTS or len(data.global_acc) < MIN_POINTS:
        return SqueezeResult(SqueezeState.INSUFFICIENT_DATA, ["missing_endpoint_data"])

    closes = [x.close for x in data.futures if not math.isnan(x.close)]
    oi = [x.oi for x in data.oi_hist if not math.isnan(x.oi)]
    glob = [x.ratio for x in data.global_acc if not math.isnan(x.ratio)]
    pos = [x.ratio for x in data.top_pos if not math.isnan(x.ratio)]
    if min(len(closes), len(oi), len(glob), len(pos)) < MIN_POINTS:
        return SqueezeResult(SqueezeState.INSUFFICIENT_DATA, ["missing_endpoint_data"])

    cr = [pct(closes[i - 1], closes[i]) for i in range(1, len(closes)) if not math.isnan(pct(closes[i - 1], closes[i]))]
    orr = [pct(oi[i - 1], oi[i]) for i in range(1, len(oi)) if not math.isnan(pct(oi[i - 1], oi[i]))]
    g1, g2 = phase_split(glob)
    p1, p2 = phase_split(pos)

    pre_vulnerability = statistics.median(g2) <= statistics.median(g1)
    squeeze_ignition = slope(phase_split(closes)[1]) > slope(phase_split(closes)[0]) and statistics.median(phase_split(orr)[1]) <= statistics.median(phase_split(orr)[0])
    rebuild = slope(p2) >= slope(p1) and statistics.median(phase_split(orr)[1]) >= statistics.median(phase_split(orr)[0])

    ev = [f"pre_squeeze_vulnerability={pre_vulnerability}", f"squeeze_ignition={squeeze_ignition}", f"post_squeeze_rebuild={rebuild}"]

    if pre_vulnerability and squeeze_ignition and rebuild:
        return SqueezeResult(SqueezeState.REAL_SHORT_SQUEEZE, ev)
    if pre_vulnerability and squeeze_ignition and not rebuild:
        return SqueezeResult(SqueezeState.SHORT_COVERING_ONLY, ev)
    if squeeze_ignition and funding_context(data.funding) == "crowding_risk" and slope(p2) < 0:
        return SqueezeResult(SqueezeState.FAKE_DERIVATIVES_EXPLOSION, ev)
    return SqueezeResult(SqueezeState.MIXED_SQUEEZE_STRUCTURE, ev)


# ============================================================
# 8) CLASSIFICATION ENGINE
# ============================================================
def evidence_quality(prep: PreparationResult, flow: FlowState, acc: AcceptanceResult, sq: SqueezeResult, errors: Sequence[str]) -> EvidenceQuality:
    if errors:
        return EvidenceQuality.UNRELIABLE
    if prep.state in {PreparationState.QUALIFIED_POSITION_LED_BUILD, PreparationState.QUALIFIED_CONSENSUS_BUILD} and flow == FlowState.SUSTAINED_SUPPORTIVE and acc.state == AcceptanceState.ACCEPTED_CLEAN:
        return EvidenceQuality.HIGH
    if prep.state in {PreparationState.QUALIFIED_POSITION_LED_BUILD, PreparationState.QUALIFIED_CONSENSUS_BUILD, PreparationState.WEAK_BUILD} and acc.state in {AcceptanceState.ACCEPTED_CLEAN, AcceptanceState.ACCEPTED_WEAK}:
        return EvidenceQuality.MEDIUM
    if acc.state in {AcceptanceState.BREAKOUT_NO_ACCEPTANCE, AcceptanceState.FAILED_RETEST}:
        return EvidenceQuality.LOW
    return EvidenceQuality.UNRELIABLE


def classify_structure(prep: PreparationResult, flow: FlowState, acc: AcceptanceResult, sq: SqueezeResult) -> Tuple[StructuralClass, Actionability, str]:
    if prep.state == PreparationState.INSUFFICIENT_DATA or flow == FlowState.INSUFFICIENT or acc.state == AcceptanceState.INSUFFICIENT_DATA:
        return StructuralClass.INSUFFICIENT, Actionability.INSUFFICIENT_DATA, "insufficient_data"

    if sq.state == SqueezeState.REAL_SHORT_SQUEEZE and acc.state in {AcceptanceState.ACCEPTED_CLEAN, AcceptanceState.ACCEPTED_WEAK}:
        return StructuralClass.REAL_SHORT_SQUEEZE, Actionability.TRADABLE_BULLISH_CANDIDATE, "real_squeeze_with_rebuild"

    if prep.state == PreparationState.QUALIFIED_POSITION_LED_BUILD and acc.state == AcceptanceState.ACCEPTED_CLEAN and flow == FlowState.SUSTAINED_SUPPORTIVE:
        return StructuralClass.POSITION_LED_EXPANSION, Actionability.TRADABLE_BULLISH_CANDIDATE, "position_led_with_acceptance"

    if prep.state == PreparationState.QUALIFIED_CONSENSUS_BUILD and acc.state in {AcceptanceState.ACCEPTED_CLEAN, AcceptanceState.ACCEPTED_WEAK} and flow in {FlowState.SUSTAINED_SUPPORTIVE, FlowState.IGNITION_ONLY}:
        return StructuralClass.CONSENSUS_BULLISH_EXPANSION, Actionability.TRADABLE_BULLISH_CANDIDATE, "consensus_bullish_with_acceptance"

    if prep.state == PreparationState.QUALIFIED_POSITION_LED_BUILD and acc.state == AcceptanceState.ACCEPTED_WEAK:
        return StructuralClass.GENUINE_EARLY_BULLISH, Actionability.WATCHLIST_ONLY, "early_bullish_not_fully_confirmed"

    if acc.state in {AcceptanceState.ACCEPTED_CLEAN, AcceptanceState.ACCEPTED_WEAK} and flow == FlowState.SUSTAINED_SUPPORTIVE:
        return StructuralClass.ACCEPTED_BREAKOUT, Actionability.WATCHLIST_ONLY, "accepted_breakout_without_full_structure"

    return StructuralClass.MIXED, Actionability.REJECT, "structural_contradiction"


# ============================================================
# 9) FETCH + MODE ANALYSIS
# ============================================================
def fetch_tf(client: BinanceClient, symbol: str, tf: str, lookback: int) -> SymbolTFData:
    data = SymbolTFData(symbol=symbol, timeframe=tf)
    try:
        data.futures = parse_candles(client.futures_klines(symbol, tf, lookback))
    except Exception as e:
        data.errors.append(f"api_failure:futures_klines:{e}")

    try:
        data.spot = parse_candles(client.spot_klines(symbol, tf, lookback))
    except Exception:
        data.errors.append("unavailable_spot_data")

    try:
        data.oi_hist = parse_oi(client.oi_hist(symbol, tf, LOOKBACK_WINDOWS["oi"]))
    except Exception as e:
        data.errors.append(f"missing_endpoint_data:oi:{e}")

    try:
        data.top_pos = parse_ratio(client.top_pos(symbol, tf, LOOKBACK_WINDOWS["ratio"]))
    except Exception as e:
        data.errors.append(f"missing_endpoint_data:top_pos:{e}")

    try:
        data.top_acc = parse_ratio(client.top_acc(symbol, tf, LOOKBACK_WINDOWS["ratio"]))
    except Exception as e:
        data.errors.append(f"missing_endpoint_data:top_acc:{e}")

    try:
        data.global_acc = parse_ratio(client.global_acc(symbol, tf, LOOKBACK_WINDOWS["ratio"]))
    except Exception as e:
        data.errors.append(f"missing_endpoint_data:global_acc:{e}")

    try:
        data.taker_ratio = parse_ratio(client.taker_ratio(symbol, tf, LOOKBACK_WINDOWS["ratio"]))
    except Exception as e:
        data.errors.append(f"unavailable_taker_data:{e}")

    try:
        data.funding = parse_funding(client.funding(symbol, LOOKBACK_WINDOWS["funding"]))
    except Exception as e:
        data.errors.append(f"missing_endpoint_data:funding:{e}")

    return data


def analyze_single(data: SymbolTFData) -> SymbolResult:
    prep = detect_preparation_structure(data)
    flow, flow_ev = evaluate_flow(data)
    acc = detect_acceptance(data, flow)
    sq = detect_squeeze_phase(data)
    sc, action, reason = classify_structure(prep, flow, acc, sq)
    eq = evidence_quality(prep, flow, acc, sq, data.errors)

    reasons = data.errors + prep.evidence + flow_ev + acc.evidence + sq.evidence + [f"classification_reason={reason}"]

    return SymbolResult(
        symbol=data.symbol,
        timeframe_mode="single",
        structure_phase=prep.state.value,
        leadership_type=prep.leadership.value,
        oi_state=prep.oi_state.value,
        flow_state=flow.value,
        breakout_acceptance_state=acc.state.value,
        squeeze_state=sq.state.value,
        final_structural_classification=sc.value,
        confidence_score=eq.value,
        actionability=action.value,
        structural_classification=sc.value,
        reasons=reasons,
    )


def analyze_multi(ctx: SymbolTFData, ign: SymbolTFData, acc_tf: SymbolTFData) -> SymbolResult:
    prep_ctx = detect_preparation_structure(ctx)
    prep_ign = detect_preparation_structure(ign)
    flow, flow_ev = evaluate_flow(acc_tf)
    acc = detect_acceptance(acc_tf, flow)
    sq = detect_squeeze_phase(ign)

    merged_prep = prep_ctx if prep_ctx.state in {PreparationState.QUALIFIED_POSITION_LED_BUILD, PreparationState.QUALIFIED_CONSENSUS_BUILD} else prep_ign
    sc, action, reason = classify_structure(merged_prep, flow, acc, sq)
    merged_errors = ctx.errors + ign.errors + acc_tf.errors
    eq = evidence_quality(merged_prep, flow, acc, sq, merged_errors)

    reasons = merged_errors + prep_ctx.evidence + prep_ign.evidence + flow_ev + acc.evidence + sq.evidence + [f"classification_reason={reason}"]

    return SymbolResult(
        symbol=ctx.symbol,
        timeframe_mode="multi",
        structure_phase=f"context={prep_ctx.state.value}|ignition={prep_ign.state.value}",
        leadership_type=merged_prep.leadership.value,
        oi_state=merged_prep.oi_state.value,
        flow_state=flow.value,
        breakout_acceptance_state=acc.state.value,
        squeeze_state=sq.state.value,
        final_structural_classification=sc.value,
        confidence_score=eq.value,
        actionability=action.value,
        structural_classification=sc.value,
        reasons=reasons,
    )


# ============================================================
# 10) REPORTING
# ============================================================
def candidate_color(result: SymbolResult) -> str:
    cls = StructuralClass(result.final_structural_classification)
    if cls == StructuralClass.POSITION_LED_EXPANSION and result.breakout_acceptance_state in {AcceptanceState.ACCEPTED_CLEAN.value, AcceptanceState.ACCEPTED_WEAK.value}:
        return ANSI_GREEN
    if cls == StructuralClass.REAL_SHORT_SQUEEZE:
        return ANSI_BLUE
    if cls == StructuralClass.ACCEPTED_BREAKOUT:
        return ANSI_YELLOW
    return ANSI_RED


def is_main_candidate(result: SymbolResult) -> bool:
    cls = StructuralClass(result.final_structural_classification)
    action = Actionability(result.actionability)
    return cls in VALID_BULLISH_CLASSES and action == Actionability.TRADABLE_BULLISH_CANDIDATE


def print_result(r: SymbolResult) -> None:
    if OUTPUT_MODE == "json":
        print(asdict(r))
        return
    color = candidate_color(r)
    print(
        f"{color}{r.symbol} | structural_classification={r.structural_classification} | actionability={r.actionability} | "
        f"preparation={r.structure_phase} | leadership={r.leadership_type} | oi_state={r.oi_state} | "
        f"flow={r.flow_state} | acceptance={r.breakout_acceptance_state} | squeeze={r.squeeze_state} | "
        f"evidence_quality={r.confidence_score}{ANSI_RESET}"
    )
    for e in r.reasons[:8]:
        print(f"{color}  - {e}{ANSI_RESET}")


# ============================================================
# 11) RUNNER
# ============================================================
def run_once() -> None:
    client = BinanceClient()
    symbols = client.symbols()
    if SYMBOL_LIMIT:
        symbols = symbols[:SYMBOL_LIMIT]

    results: List[SymbolResult] = []

    if ANALYSIS_MODE == "single":
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(fetch_tf, client, s, PRIMARY_TIMEFRAME, LOOKBACK_WINDOWS["single"]): s for s in symbols}
            for fut in concurrent.futures.as_completed(futs):
                data = fut.result()
                res = analyze_single(data)
                if is_main_candidate(res):
                    results.append(res)
    else:
        ctx_tf, ign_tf, acc_tf = MULTI_TIMEFRAMES["context"], MULTI_TIMEFRAMES["ignition"], MULTI_TIMEFRAMES["acceptance"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            jobs: Dict[concurrent.futures.Future, Tuple[str, str]] = {}
            for s in symbols:
                jobs[ex.submit(fetch_tf, client, s, ctx_tf, LOOKBACK_WINDOWS["context"])] = (s, "ctx")
                jobs[ex.submit(fetch_tf, client, s, ign_tf, LOOKBACK_WINDOWS["ignition"])] = (s, "ign")
                jobs[ex.submit(fetch_tf, client, s, acc_tf, LOOKBACK_WINDOWS["acceptance"])] = (s, "acc")
            agg: Dict[str, Dict[str, SymbolTFData]] = {}
            for fut in concurrent.futures.as_completed(jobs):
                sym, k = jobs[fut]
                agg.setdefault(sym, {})[k] = fut.result()
        for sym, parts in agg.items():
            if {"ctx", "ign", "acc"} - parts.keys():
                continue
            res = analyze_multi(parts["ctx"], parts["ign"], parts["acc"])
            if is_main_candidate(res):
                results.append(res)

    def sort_key(x: SymbolResult) -> Tuple[int, int, str]:
        cls = StructuralClass(x.final_structural_classification)
        if cls == StructuralClass.POSITION_LED_EXPANSION:
            p = 0
        elif cls == StructuralClass.REAL_SHORT_SQUEEZE:
            p = 1
        elif cls == StructuralClass.ACCEPTED_BREAKOUT:
            p = 2
        elif cls == StructuralClass.CONSENSUS_BULLISH_EXPANSION:
            p = 3
        else:
            p = 4
        eq = [EvidenceQuality.HIGH.value, EvidenceQuality.MEDIUM.value, EvidenceQuality.LOW.value, EvidenceQuality.UNRELIABLE.value].index(x.confidence_score)
        return (p, eq, x.symbol)

    results.sort(key=sort_key)
    for r in results:
        print_result(r)


def main() -> None:
    if CONTINUOUS_MODE:
        cycle = 1
        while True:
            print(f"{ANSI_WHITE}=== scan_cycle={cycle} ==={ANSI_RESET}")
            try:
                run_once()
            except Exception as e:
                print(f"{ANSI_RED}run_failure={e}{ANSI_RESET}")
            cycle += 1
            time.sleep(SCAN_INTERVAL_SECONDS)
    else:
        run_once()


if __name__ == "__main__":
    main()
