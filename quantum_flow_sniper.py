import asyncio
import json
import time
import math
import csv
import sys
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Deque, Tuple, Optional

import aiohttp
import numpy as np
from scipy.stats import entropy as shannon_entropy

"""
محرّك "Quantum Flow Sniper" بنسخة مؤسسية:
- يعتمد على تحليل فيزيائي إحصائي (Hurst, Shannon Entropy, Kalman Filter).
- يستخدم تدفق Binance WebSocket الفوري (!ticker@arr) لأعلى سرعة ممكنة.
- كل الإشارات مبنية على نماذج كمية تلتقط التشوهات البنيوية قبل الضخ.
"""

# =====================[ ⚙️ إعدادات المحرك الكمي ]=====================
CONFIG = {
    # نوافذ التحليل (بالثواني/العينات)
    "WINDOW_SIZE": 180,                  # طول الذاكرة الزمنية للأسعار والحجوم
    "RETURNS_WINDOW": 120,               # نافذة حساب إرجاع اللوج و الإنتروبيا
    "IMBALANCE_WINDOW": 60,             # نافذة توازن العرض/الطلب

    # حواجز إحصائية
    "MIN_24H_VOL": 20_000_000,          # تجاهل الأصول ذات سيولة ضعيفة
    "HURST_MIN": 0.65,                  # الحد الأدنى لهيرست لاعتبار سلوك اتجاهي قوي
    "ENTROPY_DROP_RATIO": 0.18,         # مقدار الانخفاض النسبي المطلوب في الإنتروبيا
    "KALMAN_RESIDUAL_Z": 2.6,           # Z-Score لبقايا الكالمان لإعلان كسر هيكلي
    "IMBALANCE_ACCEL_THRESHOLD": 0.12,  # تسارع توازن الطلب/العرض المطلوب

    # بارامترات الكالمان (1D)
    "KALMAN_PROCESS_NOISE": 1e-3,
    "KALMAN_MEAS_NOISE": 2e-2,
    "KALMAN_STATE_SMOOTH": 10,

    # الأطر الزمنية المدعومة
    "TIMEFRAMES": {
        "tick": 0,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14_400,
        "1d": 86_400,
    },

    # عدد الأشرطة لكل إطار زمني للحفاظ على تاريخ كافٍ للمقاييس
    "TIMEFRAME_BARS": {
        "tick": 240,
        "1m": 360,
        "5m": 240,
        "15m": 180,
        "1h": 120,
        "4h": 90,
        "1d": 60,
    },

    # التفعيل/التعطيل لكل إشارة كمية
    "ENABLE_HURST_TREND": True,
    "ENABLE_ENTROPY_IMBALANCE": True,
    "ENABLE_KALMAN_BREAK": True,
    "ENABLE_IMBALANCE_ACCEL": True,
    "ENABLE_ICT_MODEL": True,

    # زمنية لحظية وحماية من التأخير
    "DROP_STALE_TICKS": True,          # تجاهل أي بيانات تتأخر عن الوقت الحقيقي
    "MAX_TICK_LATENCY_SEC": 1.5,       # الحد الأقصى المقبول لتأخر الحدث بالثواني

    # عرض المقاييس التراكمية
    "ENABLE_CUMULATIVE_RISE": True,
    "ENABLE_CUMULATIVE_DROP": True,
    "SHOW_ALERT_COUNTERS": True,

    # حماية السوق
    "BTC_PROTECTION": True,
    "BTC_DUMP_PERCENT": -0.4,

    # منطق ICT (سيولة + نزوح سعري)
    "ICT_SWEEP_LOOKBACK": 40,
    "ICT_MIN_DISPLACEMENT": 0.6,  # %
    "ICT_MIN_IMBALANCE": 0.04,
    "ICT_FVG_TOLERANCE": 0.12,    # % فجوة قيمة عادلة تقريبية بين آخر 3 نقاط

    "LOG_FILE": "quantum_signals.csv",

    # ترشيح الضوضاء وكبح التنبيهات السريعة
    "ALERT_COOLDOWN_SEC": 12,      # حد أدنى بين إشارتين لنفس الاستراتيجية/الرمز/الإطار
    "MIN_SIGNAL_CHANGE": 0.45,     # تجاهل التغيرات الصغيرة (٪ أو قيمة معيارية)
    "MIN_POSITIVE_MOMENTUM": 0.8,  # أقل زخم إيجابي مسموح به لإظهار أن الأصل يتحرك فعلاً لأعلى
    "MIN_CONFLUENCE_SCORE": 1.25,  # حد تماسك إشارات (زخم + إتزان + هيرست)
}

# =====================[ 🎨 واجهة التيرمينال الاحترافية ]=====================
class Term:
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"

    @staticmethod
    def print_banner():
        # تنظيف الشاشة بطريقة متوافقة مع جميع الأنظمة
        print("\033c", end="")
        print(
            f"""{Term.PURPLE}{Term.BOLD}
        ╔═══════════════════════════════════════════════════════════════╗
        ║      QUANTUM FLOW SNIPER v6.0 (Institutional Quant Edition)    ║
        ║ [ Hurst | Shannon Entropy | Kalman Structural Break | OFI d² ] ║
        ╚═══════════════════════════════════════════════════════════════╝
        {Term.END}"""
        )


# =====================[ 🧠 أدوات التحليل الرياضي ]=====================
@dataclass
class Kalman1D:
    process_noise: float
    measurement_noise: float
    state_estimate: float = 0.0
    covariance: float = 1.0
    initialized: bool = False

    def update(self, measurement: float) -> Tuple[float, float]:
        # تهيئة أولية لضبط البداية على أول قيمة سعر
        if not self.initialized:
            self.state_estimate = measurement
            self.covariance = 1.0
            self.initialized = True
            residual = 0.0
            return self.state_estimate, residual

        # مرحلة التنبؤ
        predicted_state = self.state_estimate
        predicted_cov = self.covariance + self.process_noise

        # مرحلة التصحيح باستخدام الملاحظة
        kalman_gain = predicted_cov / (predicted_cov + self.measurement_noise)
        residual = measurement - predicted_state
        self.state_estimate = predicted_state + kalman_gain * residual
        self.covariance = (1 - kalman_gain) * predicted_cov
        return self.state_estimate, residual


def hurst_exponent(series: np.ndarray) -> float:
    """حساب Hurst عبر تحليل النطاق المُعاد ضبطه (R/S)."""
    if series.size < 20:
        return 0.5
    lags = np.arange(2, min(40, series.size // 2))
    tau = [np.sqrt(np.std(np.subtract(series[lag:], series[:-lag]))) for lag in lags]
    with np.errstate(divide="ignore", invalid="ignore"):
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
    return poly[0] if not np.isnan(poly[0]) else 0.5


def shannon_entropy_drop(returns: np.ndarray, bins: int = 20) -> Tuple[float, float]:
    if returns.size < 30:
        return 0.0, 0.0
    hist, _ = np.histogram(returns, bins=bins, density=True)
    hist = hist[hist > 0]
    if hist.size == 0:
        return 0.0, 0.0
    ent = shannon_entropy(hist)
    rolling_mean = float(np.mean(hist))
    return float(ent), rolling_mean


@dataclass
class MarketState:
    symbol: str
    timeframe: str
    window_size: int
    returns_window: int
    imbalance_window: int
    prices: Deque[float] = field(init=False)
    returns: Deque[float] = field(init=False)
    imbalance: Deque[float] = field(init=False)
    kalman: Kalman1D = field(init=False)
    residuals: Deque[float] = field(init=False)
    last_timestamp: float = 0.0

    def __post_init__(self):
        self.prices = deque(maxlen=self.window_size)
        self.returns = deque(maxlen=self.returns_window)
        self.imbalance = deque(maxlen=self.imbalance_window)
        self.kalman = Kalman1D(CONFIG["KALMAN_PROCESS_NOISE"], CONFIG["KALMAN_MEAS_NOISE"])
        self.residuals = deque(maxlen=CONFIG["KALMAN_STATE_SMOOTH"])

    def update(self, price: float, bid_qty: float, ask_qty: float, timestamp: float) -> Dict[str, float]:
        """تحديث السلاسل الزمنية لكل إطار زمني وإرجاع القياسات المحدثة."""
        metrics = {}
        if timestamp <= self.last_timestamp:
            return metrics
        self.last_timestamp = timestamp
        self.prices.append(price)

        if len(self.prices) >= 2:
            ret = math.log(self.prices[-1] / self.prices[-2])
            self.returns.append(ret)

        total_depth = bid_qty + ask_qty
        imbalance_value = (bid_qty - ask_qty) / total_depth if total_depth > 0 else 0.0
        self.imbalance.append(imbalance_value)

        price_array = np.fromiter(self.prices, dtype=float)
        returns_array = np.fromiter(self.returns, dtype=float)
        imbalance_array = np.fromiter(self.imbalance, dtype=float)

        # Hurst
        metrics["hurst"] = hurst_exponent(price_array)

        # Entropy
        current_entropy, _ = shannon_entropy_drop(returns_array)
        metrics["entropy"] = current_entropy
        if len(returns_array) > 10:
            metrics["entropy_ema"] = float(pd_ema(returns_array, span=20)[-1])
        else:
            metrics["entropy_ema"] = current_entropy

        # Imbalance derivatives
        if imbalance_array.size >= 4:
            first_derivative = np.gradient(imbalance_array)[-1]
            second_derivative = np.gradient(np.gradient(imbalance_array))[-1]
        else:
            first_derivative = 0.0
            second_derivative = 0.0
        metrics["imbalance_d1"] = float(first_derivative)
        metrics["imbalance_d2"] = float(second_derivative)

        # Kalman residuals مع تنعيم
        _, residual = self.kalman.update(price)
        self.residuals.append(residual)
        metrics["kalman_residual"] = residual
        metrics["kalman_residual_mean"] = float(np.mean(self.residuals)) if self.residuals else 0.0

        # Momentum آخر 5 نقاط
        metrics["momentum"] = (
            ((self.prices[-1] - self.prices[-5]) / self.prices[-5]) * 100
            if len(self.prices) >= 5
            else 0.0
        )

        return metrics


@dataclass
class TimeframeBuffer:
    """تجميع الأسعار على أطر زمنية مختلفة دون ضغط الأداء."""

    name: str
    seconds: int
    bucket_start: float = 0.0
    last_price: float = 0.0
    last_bid: float = 0.0
    last_ask: float = 0.0

    def ingest(self, timestamp: float, price: float, bid_qty: float, ask_qty: float) -> Optional[Tuple[float, float, float]]:
        # إطار tick لا يحتاج لتجميع
        if self.seconds == 0:
            return None

        if self.bucket_start == 0:
            self.bucket_start = timestamp

        # تحديث آخر قيم لرسم الإغلاق عند نهاية الحاوية
        self.last_price = price
        self.last_bid = bid_qty
        self.last_ask = ask_qty

        if timestamp - self.bucket_start >= self.seconds:
            aggregated = (self.last_price, self.last_bid, self.last_ask)
            # تقدم الحاوية للأمام حتى لا نخسر الدقات المتأخرة
            while timestamp - self.bucket_start >= self.seconds:
                self.bucket_start += self.seconds
            return aggregated

        return None


# ===============[ 📈 مؤشرات مساندة عالية السرعة ]=====================
def pd_ema(series: np.ndarray, span: int) -> np.ndarray:
    """حساب EMA سريع باستخدام numpy (بدون pandas)."""
    alpha = 2 / (span + 1)
    ema = np.zeros_like(series)
    ema[0] = series[0]
    for i in range(1, len(series)):
        ema[i] = alpha * series[i] + (1 - alpha) * ema[i - 1]
    return ema


# =====================[ 🚀 الكور الرئيسي ]=====================
class QuantumSniper:
    def __init__(self):
        # يتم إنشاء طابور الرسائل داخل حلقة الحدث النشطة لضمان توافق الحلقة
        # (تفادي خطأ "Future attached to a different loop").
        self.msg_queue: Optional[asyncio.Queue] = None
        self.market_states: Dict[Tuple[str, str], MarketState] = {}
        self.timeframe_buffers: Dict[str, Dict[str, TimeframeBuffer]] = defaultdict(dict)
        self.alert_stats: Dict[Tuple[str, str], Dict[str, Dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: {"count": 0, "rise": 0.0, "drop": 0.0})
        )
        self.last_event_ts: Dict[str, float] = defaultdict(float)
        self.last_alert_time: Dict[Tuple[str, str, str], float] = defaultdict(float)
        self.paused = False
        self.btc_trend = 0.0

    @staticmethod
    def confluence_score(metrics: Dict[str, float]) -> float:
        """مؤشر بسيط لتماسك الإشارة يجمع الزخم مع تسارع الاتزان وهيرست."""
        mom_term = max(metrics.get("momentum", 0.0), 0.0) / 2
        imb_term = max(metrics.get("imbalance_d1", 0.0), 0.0) * 50
        acc_term = max(metrics.get("imbalance_d2", 0.0), 0.0) * 100
        hurst_term = max(metrics.get("hurst", 0.0) - CONFIG["HURST_MIN"], 0.0)
        return mom_term + imb_term + acc_term + hurst_term

    def should_emit(
        self,
        symbol: str,
        timeframe: str,
        strategy: str,
        change: float,
        metrics: Dict[str, float],
        enforce_trend: bool = False,
        confluence: Optional[float] = None,
    ) -> bool:
        key = (symbol, timeframe, strategy)
        now = time.time()

        if now - self.last_alert_time[key] < CONFIG["ALERT_COOLDOWN_SEC"]:
            return False

        if abs(change) < CONFIG["MIN_SIGNAL_CHANGE"]:
            return False

        if enforce_trend:
            if metrics.get("momentum", 0.0) < CONFIG["MIN_POSITIVE_MOMENTUM"]:
                return False
            if confluence is not None and confluence < CONFIG["MIN_CONFLUENCE_SCORE"]:
                return False

        return True

    def get_state(self, symbol: str, timeframe: str) -> MarketState:
        key = (symbol, timeframe)
        if key not in self.market_states:
            bars = CONFIG["TIMEFRAME_BARS"].get(timeframe, CONFIG["TIMEFRAME_BARS"]["tick"])
            self.market_states[key] = MarketState(
                symbol,
                timeframe,
                window_size=bars,
                returns_window=bars,
                imbalance_window=max(30, bars // 2),
            )
        return self.market_states[key]

    def get_buffer(self, symbol: str, timeframe: str) -> TimeframeBuffer:
        tf_map = self.timeframe_buffers[symbol]
        if timeframe not in tf_map:
            seconds = CONFIG["TIMEFRAMES"].get(timeframe, 0)
            tf_map[timeframe] = TimeframeBuffer(timeframe, seconds)
        return tf_map[timeframe]

    async def ws_listener(self):
        url = "wss://stream.binance.com:9443/ws/!ticker@arr"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=60) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        await self.msg_queue.put(data)

    async def market_analyzer(self):
        while True:
            tickers = await self.msg_queue.get()
            if not isinstance(tickers, list):
                continue
            for ticker in tickers:
                await self.process_ticker(ticker)

    def btc_guard(self, ticker):
        if ticker.get("s") != "BTCUSDT":
            return
        price = float(ticker["c"])
        open_price = float(ticker.get("o", price))
        self.btc_trend = ((price - open_price) / open_price) * 100
        if self.btc_trend < CONFIG["BTC_DUMP_PERCENT"]:
            if not self.paused:
                print(
                    f"\n{Term.RED}{Term.BOLD}⛔ BTC CRASH DETECTED ({self.btc_trend:.2f}%) - HALTING SNIPER{Term.END}"
                )
            self.paused = True
        elif self.paused:
            print(f"\n{Term.GREEN}✅ BTC STABILIZED - RESUMING{Term.END}")
            self.paused = False

    async def process_ticker(self, ticker):
        symbol = ticker.get("s")
        if not symbol or symbol.endswith("BUSD"):
            return

        self.btc_guard(ticker)
        if self.paused:
            return

        quote_vol = float(ticker.get("q", 0))
        if quote_vol < CONFIG["MIN_24H_VOL"]:
            return

        price = float(ticker["c"])
        bid_qty = float(ticker.get("B", 0.0))
        ask_qty = float(ticker.get("A", 0.0))

        timestamp = float(ticker.get("E", time.time() * 1000)) / 1000.0
        if CONFIG["DROP_STALE_TICKS"]:
            now = time.time()
            if now - timestamp > CONFIG["MAX_TICK_LATENCY_SEC"]:
                return
        if timestamp <= self.last_event_ts[symbol]:
            return
        self.last_event_ts[symbol] = timestamp

        # إطار tick الفوري
        tick_state = self.get_state(symbol, "tick")
        metrics = tick_state.update(price, bid_qty, ask_qty, timestamp)
        if metrics:
            await self.evaluate_signals(symbol, "tick", price, metrics, tick_state)

        # أطر زمنية مجمعة
        for tf_name, seconds in CONFIG["TIMEFRAMES"].items():
            if tf_name == "tick" or seconds <= 0:
                continue
            buffer = self.get_buffer(symbol, tf_name)
            aggregated = buffer.ingest(timestamp, price, bid_qty, ask_qty)
            if aggregated:
                agg_price, agg_bid, agg_ask = aggregated
                tf_state = self.get_state(symbol, tf_name)
                tf_metrics = tf_state.update(agg_price, agg_bid, agg_ask, timestamp)
                if tf_metrics:
                    await self.evaluate_signals(
                        symbol, tf_name, agg_price, tf_metrics, tf_state
                    )

    async def evaluate_signals(
        self, symbol: str, timeframe: str, price: float, m: Dict[str, float], state: MarketState
    ):
        entropy_buffer = list(state.returns)
        entropy_arr = np.fromiter(entropy_buffer, dtype=float)
        previous_entropy = float(pd_ema(entropy_arr, span=20)[-2]) if len(entropy_arr) > 2 else m["entropy"]
        confluence = self.confluence_score(m)

        # 1) اتجاهية قوية وفق Hurst + زخم إيجابي
        if (
            CONFIG["ENABLE_HURST_TREND"]
            and m["hurst"] > CONFIG["HURST_MIN"]
            and m["momentum"] > 0.25
        ):
            if self.should_emit(
                symbol,
                timeframe,
                "📐 HURST PERSISTENCE",
                change=m["momentum"],
                metrics=m,
                enforce_trend=True,
                confluence=confluence,
            ):
                await self.trigger_alert(
                    "📐 HURST PERSISTENCE",
                    symbol,
                    timeframe,
                    price,
                    extra={
                        "H": m["hurst"],
                        "mom": m["momentum"],
                    },
                    color=Term.CYAN,
                    change=m["momentum"],
                )

        # 2) انهيار إنتروبيا + تسارع توازن إيجابي (تزامن حيتان)
        entropy_drop = (
            (previous_entropy - m["entropy"]) / previous_entropy
            if previous_entropy > 0
            else 0.0
        )
        if (
            CONFIG["ENABLE_ENTROPY_IMBALANCE"]
            and entropy_drop >= CONFIG["ENTROPY_DROP_RATIO"]
            and m["imbalance_d2"] > 0
            and m["imbalance_d1"] > 0
        ):
            if self.should_emit(
                symbol,
                timeframe,
                "🧠 ENTROPY COLLAPSE",
                change=m["imbalance_d1"] * 100,
                metrics=m,
                enforce_trend=True,
                confluence=confluence,
            ):
                await self.trigger_alert(
                    "🧠 ENTROPY COLLAPSE",
                    symbol,
                    timeframe,
                    price,
                    extra={
                        "ΔH": entropy_drop,
                        "∂I": m["imbalance_d1"],
                        "∂²I": m["imbalance_d2"],
                    },
                    color=Term.PURPLE,
                    change=m["imbalance_d1"] * 100,
                )

        # 3) بقايا كالمان مرتفعة = كسر هيكلي مفاجئ
        if CONFIG["ENABLE_KALMAN_BREAK"]:
            residual_std = float(np.std(list(state.returns) or [0.0]))
            residual_std = residual_std if residual_std > 0 else 1e-6
            residual_z = m["kalman_residual"] / residual_std
            if abs(residual_z) >= CONFIG["KALMAN_RESIDUAL_Z"]:
                if self.should_emit(
                    symbol,
                    timeframe,
                    "🛰️ KALMAN STRUCTURAL BREAK",
                    change=residual_z,
                    metrics=m,
                ):
                    await self.trigger_alert(
                        "🛰️ KALMAN STRUCTURAL BREAK",
                        symbol,
                        timeframe,
                        price,
                        extra={
                            "z_res": residual_z,
                            "res": m["kalman_residual"],
                            "μ_res": m["kalman_residual_mean"],
                        },
                        color=Term.YELLOW,
                        change=residual_z,
                    )

        # 4) تسارع مشتق ثاني موجب لتوازن العرض/الطلب (تدفق أوامر حقيقي)
        if (
            CONFIG["ENABLE_IMBALANCE_ACCEL"]
            and m["imbalance_d2"] > CONFIG["IMBALANCE_ACCEL_THRESHOLD"]
            and m["imbalance_d1"] > 0
        ):
            if self.should_emit(
                symbol,
                timeframe,
                "⚡ ORDERFLOW ACCEL",
                change=m["imbalance_d2"] * 100,
                metrics=m,
                enforce_trend=True,
                confluence=confluence,
            ):
                await self.trigger_alert(
                    "⚡ ORDERFLOW ACCEL",
                    symbol,
                    timeframe,
                    price,
                    extra={
                        "∂I": m["imbalance_d1"],
                        "∂²I": m["imbalance_d2"],
                    },
                    color=Term.GREEN,
                    change=m["imbalance_d2"] * 100,
                )

        # 5) منطق ICT (سحب سيولة + نزوح سعري بفجوة قيمة عادلة)
        if CONFIG["ENABLE_ICT_MODEL"]:
            ict_ctx = self.ict_signal(state, price, m)
            if ict_ctx:
                if self.should_emit(
                    symbol,
                    timeframe,
                    "🎯 ICT LIQUIDITY SHIFT",
                    change=ict_ctx.get("disp", 0.0),
                    metrics=m,
                    enforce_trend=True,
                    confluence=confluence,
                ):
                    await self.trigger_alert(
                        "🎯 ICT LIQUIDITY SHIFT",
                        symbol,
                        timeframe,
                        price,
                        extra=ict_ctx,
                        color=Term.BLUE,
                        change=ict_ctx.get("disp", 0.0),
                    )

    def ict_signal(self, state: MarketState, price: float, m: Dict[str, float]) -> Optional[Dict[str, float]]:
        prices = np.fromiter(state.prices, dtype=float)
        if prices.size < 6:
            return None

        lookback = min(CONFIG["ICT_SWEEP_LOOKBACK"], prices.size)
        window = prices[-lookback:]
        prev_high = float(np.max(window[:-1])) if window.size > 1 else window[-1]
        prev_low = float(np.min(window[:-1])) if window.size > 1 else window[-1]

        sweep_up = price > prev_high
        sweep_down = price < prev_low

        displacement = ((price - float(np.mean(window))) / float(np.mean(window))) * 100
        last3 = prices[-3:]
        upper_gap = max(last3[0], last3[1])
        lower_gap = min(last3[1], last3[2])
        if lower_gap <= 0:
            fvg = 0.0
        else:
            fvg = ((upper_gap - lower_gap) / lower_gap) * 100 if upper_gap > lower_gap else 0.0

        imbalance_ok = m.get("imbalance_d1", 0.0) > CONFIG["ICT_MIN_IMBALANCE"]
        displacement_ok = abs(displacement) >= CONFIG["ICT_MIN_DISPLACEMENT"]
        fvg_ok = fvg >= CONFIG["ICT_FVG_TOLERANCE"]

        if sweep_up and displacement_ok and imbalance_ok:
            return {
                "sweep": 1.0,
                "disp": displacement,
                "imb": m.get("imbalance_d1", 0.0),
                "fvg": fvg,
                "dir": 1.0 if displacement > 0 else -1.0,
                "σH": m.get("hurst", 0.0),
            }

        if sweep_down and displacement_ok and imbalance_ok and m.get("momentum", 0.0) > 0:
            return {
                "sweep": -1.0,
                "disp": displacement,
                "imb": m.get("imbalance_d1", 0.0),
                "fvg": fvg,
                "dir": 1.0 if displacement > 0 else -1.0,
                "σH": m.get("hurst", 0.0),
            }

        if fvg_ok and imbalance_ok and displacement_ok:
            return {
                "sweep": 0.0,
                "disp": displacement,
                "imb": m.get("imbalance_d1", 0.0),
                "fvg": fvg,
                "dir": 1.0 if displacement > 0 else -1.0,
                "σH": m.get("hurst", 0.0),
            }

        return None

    def update_alert_stats(self, symbol: str, timeframe: str, strategy: str, change: float):
        stats = self.alert_stats[(symbol, timeframe)][strategy]
        stats["count"] += 1
        if CONFIG["ENABLE_CUMULATIVE_RISE"] and change > 0:
            stats["rise"] += change
        if CONFIG["ENABLE_CUMULATIVE_DROP"] and change < 0:
            stats["drop"] += abs(change)
        return stats

    async def trigger_alert(
        self,
        signal_type: str,
        symbol: str,
        timeframe: str,
        price: float,
        extra: Dict[str, float],
        color: str,
        change: float,
    ):
        timestamp = time.strftime("%H:%M:%S")
        stats = self.update_alert_stats(symbol, timeframe, signal_type, change)

        self.last_alert_time[(symbol, timeframe, signal_type)] = time.time()

        counter_info = f"#{int(stats['count'])}" if CONFIG["SHOW_ALERT_COUNTERS"] else ""
        rise_info = f"🔺{stats['rise']:.2f}%" if CONFIG["ENABLE_CUMULATIVE_RISE"] else ""
        drop_info = f"🔻{stats['drop']:.2f}%" if CONFIG["ENABLE_CUMULATIVE_DROP"] else ""
        stats_parts = [part for part in [rise_info, drop_info] if part]
        stats_text = " | ".join(stats_parts) if stats_parts else "N/A"
        symbol_display = f"{symbol}[{timeframe}] {counter_info} ({stats_text})".strip()

        # نص رياضي مختصر بالعربية
        extra_lines = " | ".join([f"{k}:{v:.3f}" for k, v in extra.items()]) if extra else ""

        print(f"{color}{Term.BOLD}╔══════════════════════════════════════════════════════════╗{Term.END}")
        print(f"{color}║ {signal_type:<28} | {symbol_display:<22} ⏰ {timestamp} ║{Term.END}")
        print(f"{color}╠══════════════════════════════════════════════════════════╣{Term.END}")
        print(f"{color}║ 💎 Price: {price:<12} | 📈 Change: {change:+.3f}             ║{Term.END}")
        print(f"{color}║ 📊 Metrics: {extra_lines:<44}║{Term.END}")
        print(f"{color}╚══════════════════════════════════════════════════════════╝{Term.END}")

        with open(CONFIG["LOG_FILE"], "a", newline="") as f:
            writer = csv.writer(f)
            row = [timestamp, f"{symbol}[{timeframe}]", signal_type, price, change] + [f"{k}:{v}" for k, v in extra.items()]
            writer.writerow(row)

    async def main(self):
        Term.print_banner()
        print(f"{Term.YELLOW}⏳ Initializing Statistical Engines...{Term.END}")
        # إنشاء الطابور بعد بدء الحلقة لتوحيد الحلقة بين جميع المهام
        self.msg_queue = asyncio.Queue()
        await asyncio.gather(self.ws_listener(), self.market_analyzer())


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        bot = QuantumSniper()
        asyncio.run(bot.main())
    except KeyboardInterrupt:
        print("\n🚫 System Shutdown.")
    except RuntimeError as e:
        if "Event loop is closed" not in str(e):
            print(f"Runtime Error: {e}")
