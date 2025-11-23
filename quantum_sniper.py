import asyncio
import json
import time
import math
import os
import csv
import sys
import logging
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Deque

import aiohttp

# =====================[ ⚙️ إعدادات المحرك الكمي ]=====================

CONFIG = {
    "WINDOW_SIZE": 90,               # نافذة التحليل بالثواني (لصنع المتوسطات)
    "MIN_24H_VOL": 25_000_000,       # تجاهل العملات الميتة (أقل من 25 مليون)
    "MAX_QUEUE_SIZE": 7_500,         # الحد الأقصى للطابور لحماية الذاكرة
    "RECONNECT_BACKOFF": 2,          # ثواني الانتظار قبل إعادة الاتصال
    "EMA_ALPHA": 0.24,               # معامل التنعيم للحجوم السعرية (0.2 = سلاسة أكبر)
    "FAST_ALPHA": 0.35,              # معامل أسرع لالتقاط اللحظات الحادة
    "VOLATILITY_SMOOTH": 0.18,       # تنعيم لتصنيف نظام التذبذب
    "VOL_REGIME_RANGE": 0.9,         # تقدير عنف السوق من نطاق السعر النسبي داخل النافذة
    "MULTI_WINDOWS": (15, 60, 180, 300),  # أطر زمنية متعددة للتقاطع

    # --- [ خوارزميات الحساسية ] ---
    "SIGMA_THRESHOLD": 1.4,          # (Z-Score) الحساسية للشذوذ الإحصائي (أقل = أكثر حساسية)
    "MAD_MULTIPLIER": 4.0,           # مضاعف حساس لـ MAD-Score للتأكيد المتقاطع
    "ACCELERATION_FACTOR": 1.15,     # معامل تسارع السيولة المطلوب
    "COOLDOWN_SECONDS": 18,          # تهدئة بين إشارات العملة الواحدة لمنع الإغراق
    "WARMUP_POINTS": 25,             # الحد الأدنى للعينات قبل تفعيل المنطق الخارق
    "SIGMA_ADAPT_FLOOR": 0.85,       # أقل معامل تخفيض للسقف الديناميكي
    "SIGMA_ADAPT_CEIL": 1.75,        # أعلى معامل تضخيم للسقف الديناميكي
    "WHL_SPIKE_MULT": 2.35,          # مضاعف حجم مفاجئ للحيتان
    "SILENT_SPREAD": 0.35,           # أقصى نطاق سعري % لتعريف التجميع/التصريف الهادئ
    "DISTRIBUTION_DRIFT": -0.25,     # ميل سعري سلبي بسيط لتعريف التصريف الهادئ

    # --- [ حماية السوق ] ---
    "BTC_PROTECTION": True,          # إيقاف الشراء إذا كان البيتكوين ينهار
    "BTC_DUMP_PERCENT": -0.35,       # نسبة هبوط البيتكوين في الدقيقة التي تفعل الحماية
    "BTC_RISK_AVERSION": -0.15,      # عطّل إشارات القفز إذا كان البيتكوين سلبيًا قليلًا

    "LOG_FILE": "quantum_signals.csv"
}

# =====================[ 🎨 واجهة التيرمينال الاحترافية ]=====================

class Term:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
    
    @staticmethod
    def print_banner():
        # تنظيف الشاشة بطريقة متوافقة مع جميع الأنظمة
        print("\033c", end="")
        print(f"""{Term.PURPLE}{Term.BOLD}
        ╔═══════════════════════════════════════════════════════════════╗
        ║           QUANTUM FLOW SNIPER v5.3 (MULTI-FACTOR)            ║
        ║  [ Z-Score | MAD | Dual Momentum | BTC Guard | Cooldowns ]   ║
        ╚═══════════════════════════════════════════════════════════════╝
        {Term.END}""")

# =====================[ 🧠 المحرك الإحصائي ]=====================

@dataclass
class MarketPulse:
    """يخزن نبض السوق لكل عملة لحساب الإحصائيات"""

    symbol: str
    prices: deque = field(default_factory=lambda: deque(maxlen=CONFIG["WINDOW_SIZE"]))
    volumes: deque = field(default_factory=lambda: deque(maxlen=CONFIG["WINDOW_SIZE"]))
    snapshots: Dict[int, Deque[Tuple[float, float, float]]] = field(default_factory=lambda: {
        window: deque() for window in CONFIG["MULTI_WINDOWS"]
    })
    last_accumulated_vol: float = 0.0
    ema_volume: Optional[float] = None
    ema_price: Optional[float] = None
    fast_ema_price: Optional[float] = None
    fast_ema_volume: Optional[float] = None
    on_balance_volume: float = 0.0
    last_price: Optional[float] = None
    regime_score: float = 1.0

    def add_snapshot(self, price: float, accumulated_vol: float, now: Optional[float] = None) -> float:
        # حساب حجم التدفق في هذه اللحظة (Delta)
        if self.last_accumulated_vol == 0:
            delta_vol = 0
        else:
            delta_vol = accumulated_vol - self.last_accumulated_vol
            # تصحيح في حالة إعادة تعيين اليوم
            if delta_vol < 0: delta_vol = 0
            
        self.last_accumulated_vol = accumulated_vol

        self.prices.append(price)
        self.volumes.append(delta_vol)

        # تحديث OBV لتقدير تدفق السيولة المحمية
        if self.last_price is not None:
            direction = 1 if price > self.last_price else -1 if price < self.last_price else 0
            self.on_balance_volume += direction * delta_vol
        self.last_price = price

        # تحديث المتوسط الأسي للحجم والسعر لتقليل الضوضاء ورفع حساسية الكشف
        alpha = CONFIG["EMA_ALPHA"]
        fast_alpha = CONFIG["FAST_ALPHA"]
        self.ema_volume = delta_vol if self.ema_volume is None else (alpha * delta_vol + (1 - alpha) * self.ema_volume)
        self.ema_price = price if self.ema_price is None else (alpha * price + (1 - alpha) * self.ema_price)
        self.fast_ema_volume = delta_vol if self.fast_ema_volume is None else (fast_alpha * delta_vol + (1 - fast_alpha) * self.fast_ema_volume)
        self.fast_ema_price = price if self.fast_ema_price is None else (fast_alpha * price + (1 - fast_alpha) * self.fast_ema_price)

        # الاحتفاظ بأطر زمنية متعددة مع الوقت الفعلي
        ts = now or time.time()
        for window, buf in self.snapshots.items():
            buf.append((ts, price, delta_vol))
            cutoff = ts - window
            while buf and buf[0][0] < cutoff:
                buf.popleft()

        return delta_vol

    @property
    def is_ready(self):
        # نحتاج بيانات كافية ليكون الانحراف المعياري دقيقاً
        return len(self.volumes) >= CONFIG["WARMUP_POINTS"]

    def calculate_statistics(self, current_vol_delta: float) -> Tuple[float, float]:
        """حساب الدرجة المعيارية (Z-Score) لاكتشاف الشذوذ"""
        if not self.volumes: return 0, 0
        
        vol_list = list(self.volumes)
        mean = sum(vol_list) / len(vol_list)
        
        if mean == 0: return 0, 0
        
        # حساب الانحراف المعياري (Standard Deviation)
        variance = sum((x - mean) ** 2 for x in vol_list) / len(vol_list)
        std_dev = math.sqrt(variance)
        
        if std_dev == 0: return 0, 0
        
        # معادلة Z-Score: (القيمة الحالية - المتوسط) / الانحراف
        z_score = (current_vol_delta - mean) / std_dev

        return z_score, mean

    def mad_score(self, current_vol_delta: float) -> float:
        """قياس الشذوذ باستخدام الانحراف المطلق الوسيط (أكثر ثباتًا ضد القمم)."""
        if not self.volumes:
            return 0.0
        vol_list = list(self.volumes)
        median = sorted(vol_list)[len(vol_list) // 2]
        deviations = [abs(v - median) for v in vol_list]
        if not deviations:
            return 0.0
        mad = sorted(deviations)[len(deviations) // 2]
        if mad == 0:
            return 0.0
        return 0.6745 * (current_vol_delta - median) / mad

    def get_price_momentum(self) -> float:
        if len(self.prices) < 5: return 0
        # نسبة التغير خلال آخر 5 ثواني
        start = self.prices[-5]
        end = self.prices[-1]
        return ((end - start) / start) * 100

    def volatility_regime(self) -> float:
        """تقدير ديناميكي لتصنيف التذبذب (هدوء/عاصف) لتعديل العتبات."""
        if len(self.prices) < 10:
            return 1.0
        returns = []
        for i in range(1, len(self.prices)):
            prev, curr = self.prices[i - 1], self.prices[i]
            if prev > 0:
                returns.append((curr - prev) / prev)
        if not returns:
            return 1.0
        std_dev = statistics.pstdev(returns)
        price_range = (max(self.prices) - min(self.prices)) / max(self.prices) if self.prices else 0
        regime = (std_dev + price_range * CONFIG["VOL_REGIME_RANGE"]) * 10
        # تنعيم لخفض الضوضاء الزمنية
        self.regime_score = (CONFIG["VOLATILITY_SMOOTH"] * regime) + ((1 - CONFIG["VOLATILITY_SMOOTH"]) * self.regime_score)
        return max(CONFIG["SIGMA_ADAPT_FLOOR"], min(self.regime_score, CONFIG["SIGMA_ADAPT_CEIL"]))

    def multi_frame_features(self, now: float) -> Dict[int, Dict[str, float]]:
        """حساب الزخم والحجم النسبي على عدة أطر زمنية."""
        features: Dict[int, Dict[str, float]] = {}
        for window, buf in self.snapshots.items():
            if len(buf) < 2:
                features[window] = {"momentum": 0.0, "vol_ratio": 0.0}
                continue
            start_ts, start_price, _ = buf[0]
            end_ts, end_price, _ = buf[-1]
            if start_price == 0:
                momentum = 0.0
            else:
                momentum = ((end_price - start_price) / start_price) * 100
            total_vol = sum(x[2] for x in buf)
            base_vol = statistics.fmean(self.volumes) if self.volumes else 1
            vol_ratio = (total_vol / (len(buf) or 1)) / base_vol
            features[window] = {
                "momentum": momentum,
                "vol_ratio": vol_ratio,
                "duration": end_ts - start_ts,
            }
        return features

    def get_smoothed_velocity(self) -> float:
        """حساب نسبة التغير اللحظية باستخدام المتوسط الأسي لزيادة دقة الاستباق."""
        if self.ema_price is None or len(self.prices) < 2:
            return 0
        last_price = self.prices[-1]
        if self.ema_price == 0:
            return 0
        return ((last_price - self.ema_price) / self.ema_price) * 100

    def get_fast_velocity(self) -> float:
        """تسارع أسرع مبني على EMA سريع لالتقاط الانطلاقة الأولى."""
        if self.fast_ema_price is None or len(self.prices) < 2:
            return 0
        last_price = self.prices[-1]
        if self.fast_ema_price == 0:
            return 0
        return ((last_price - self.fast_ema_price) / self.fast_ema_price) * 100

    def range_percent(self) -> float:
        if len(self.prices) < 2:
            return 0.0
        high, low = max(self.prices), min(self.prices)
        base = self.prices[0] if self.prices[0] != 0 else 1
        return ((high - low) / base) * 100

# =====================[ 🚀 الكور الرئيسي ]=====================

class QuantumSniper:
    def __init__(self):
        self.base_ws = "wss://fstream.binance.com/ws/!ticker@arr"
        self.coins: Dict[str, MarketPulse] = {}
        # هام: لا تقم بتهيئة Queue هنا لتجنب مشاكل Loop
        self.msg_queue = None
        self.session = None
        self.btc_trend = 0.0
        self.paused = False
        self.last_signal_time: Dict[str, float] = {}

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )
        self.logger = logging.getLogger("quantum-sniper")
        
        # إعداد ملف اللوج
        if not os.path.exists(CONFIG["LOG_FILE"]):
            with open(CONFIG["LOG_FILE"], 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Symbol", "Type", "Price", "Z-Score", "Volume($)", "Change%"])

    def is_on_cooldown(self, symbol: str) -> bool:
        """منع إغراق الإشعارات لنفس العملة مع السماح للانفجارات النووية بالمرور."""
        last_time = self.last_signal_time.get(symbol)
        if last_time is None:
            return False
        return (time.time() - last_time) < CONFIG["COOLDOWN_SECONDS"]

    def record_signal(self, symbol: str):
        self.last_signal_time[symbol] = time.time()

    def btc_relative_strength(self, price_change: float) -> float:
        """قياس قوة العملة مقابل اتجاه البيتكوين لرفض الإشارات المتعبة."""
        if 'BTCUSDT' not in self.coins or len(self.coins['BTCUSDT'].prices) < 2:
            return price_change
        btc_pulse = self.coins['BTCUSDT']
        btc_change = btc_pulse.get_price_momentum()
        return price_change - btc_change

    async def ws_listener(self):
        """مهمته الوحيدة شفط البيانات ورميها في الطابور بأقصى سرعة"""
        backoff = CONFIG["RECONNECT_BACKOFF"]
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.base_ws) as ws:
                        print(f"{Term.GREEN}✓ Connected to Binance Neural Network...{Term.END}")
                        backoff = CONFIG["RECONNECT_BACKOFF"]
                        async for msg in ws:
                            if self.msg_queue and not self.msg_queue.full():
                                await self.msg_queue.put(json.loads(msg.data))
                            elif self.msg_queue and self.msg_queue.full():
                                self.logger.warning("Dropping snapshot: queue is full")
            except Exception as e:
                print(f"{Term.RED}⚠️ Network Error: {e}{Term.END}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def market_analyzer(self):
        """العقل المدبر: يعالج البيانات رياضياً"""
        print(f"{Term.CYAN}⚡ Analyzer Engine Started (Waiting for buffer)...{Term.END}")
        
        while True:
            if not self.msg_queue:
                await asyncio.sleep(0.1)
                continue
                
            data = await self.msg_queue.get()
            
            try:
                # تحليل سريع للبيتكوين أولاً (Global Trend)
                btc_data = next((x for x in data if x['s'] == 'BTCUSDT'), None)
                if btc_data:
                    await self.update_btc_status(btc_data)

                # إذا كان السوق ينهار، توقف مؤقتاً
                if self.paused and CONFIG["BTC_PROTECTION"]:
                    continue

                # معالجة باقي العملات
                tasks = []
                for ticker in data:
                    symbol = ticker['s']
                    if not self.should_track(symbol, ticker):
                        continue

                    # تهيئة العملة إذا كانت جديدة
                    if symbol not in self.coins:
                        self.coins[symbol] = MarketPulse(symbol)

                    tasks.append(self.process_coin(self.coins[symbol], ticker))

                if tasks:
                    await asyncio.gather(*tasks)

            except Exception as e:
                self.logger.exception("Error in Analyzer: %s", e)
            finally:
                self.msg_queue.task_done()

    @staticmethod
    def should_track(symbol: str, ticker: Dict) -> bool:
        """فلترة صارمة لتجنب الرموز غير المستهدفة وتقليل الضوضاء"""
        if not symbol.endswith('USDT'):
            return False
        if 'BTC' in symbol and symbol != 'BTCUSDT':
            return False

        # فلتر الحجم اليومي
        try:
            if float(ticker['q']) < CONFIG["MIN_24H_VOL"]:
                return False
        except (KeyError, ValueError, TypeError):
            return False

        return True

    async def update_btc_status(self, ticker: Dict):
        """مراقبة اتجاه البيتكوين العام"""
        pulse = self.coins.get('BTCUSDT')
        if not pulse:
            self.coins['BTCUSDT'] = MarketPulse('BTCUSDT')
            return

        price = float(ticker['c'])
        vol = float(ticker['q'])
        pulse.add_snapshot(price, vol, now=time.time())

        if len(pulse.prices) > 10:
            start_price = pulse.prices[0]
            self.btc_trend = ((price - start_price) / start_price) * 100

            if self.btc_trend < CONFIG["BTC_DUMP_PERCENT"]:
                if not self.paused:
                    print(f"\n{Term.RED}{Term.BOLD}⛔ BTC CRASH DETECTED ({self.btc_trend:.2f}%) - HALTING SNIPER{Term.END}")
                self.paused = True
            else:
                if self.paused:
                    print(f"\n{Term.GREEN}✅ BTC STABILIZED - RESUMING{Term.END}")
                self.paused = False

    async def process_coin(self, pulse: MarketPulse, ticker: Dict):
        """تحليل العملة الواحدة"""
        now = time.time()
        current_price = float(ticker['c'])
        accumulated_vol = float(ticker['q'])

        # حساب الحجم اللحظي قبل التحديث
        prev_vol = pulse.last_accumulated_vol
        if prev_vol == 0:
            pulse.add_snapshot(current_price, accumulated_vol, now=now)
            return

        delta_vol = accumulated_vol - prev_vol
        if delta_vol < 0: delta_vol = 0 # Reset case

        # تحديث البيانات التاريخية
        pulse.add_snapshot(current_price, accumulated_vol, now=now)

        if not pulse.is_ready: return

        # --- [ المنطق الخارق: التحليل الإحصائي ] ---
        regime_factor = pulse.volatility_regime()
        adaptive_sigma = CONFIG["SIGMA_THRESHOLD"] * regime_factor

        z_score, mean_vol = pulse.calculate_statistics(delta_vol)
        mad_score = pulse.mad_score(delta_vol)
        price_momentum = pulse.get_price_momentum()
        smoothed_velocity = pulse.get_smoothed_velocity()
        fast_velocity = pulse.get_fast_velocity()
        relative_momentum = self.btc_relative_strength(price_momentum)
        range_pct = pulse.range_percent()
        multi_frames = pulse.multi_frame_features(now)

        # حواجز السوق العامة: خفّض الحساسية إذا كان البيتكوين متعبًا
        if CONFIG["BTC_PROTECTION"] and self.btc_trend < CONFIG["BTC_RISK_AVERSION"] and relative_momentum < 0.5:
            return

        vol_acceleration = delta_vol / mean_vol if mean_vol > 0 else 0
        ema_ratio = delta_vol / pulse.ema_volume if pulse.ema_volume else 0
        fast_ratio = delta_vol / pulse.fast_ema_volume if pulse.fast_ema_volume else 0
        liquidity_pressure = (vol_acceleration + ema_ratio + fast_ratio) / 3 if (vol_acceleration or ema_ratio or fast_ratio) else 0

        composite_score = (
            max(z_score, mad_score) * 0.4 +
            max(smoothed_velocity, fast_velocity) * 0.3 +
            liquidity_pressure * 0.3
        )

        short_frame = multi_frames.get(15, {"momentum": 0.0, "vol_ratio": 0.0})
        minute_frame = multi_frames.get(60, {"momentum": 0.0, "vol_ratio": 0.0})

        # 1. استراتيجية "الحدث النووي" (Sigma Event) مع تجاوز التهدئة
        if z_score > adaptive_sigma and price_momentum > 0.2:
            await self.trigger_alert(
                "☢️ STATISTICAL ANOMALY",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.RED,
                force=True
            )
            return

        # تطبيق التهدئة لمنع التكرار
        if self.is_on_cooldown(pulse.symbol):
            return

        # 2. استراتيجية "التجميع المخفي" (Silent Accumulation)
        if max(z_score, mad_score) > 2.8 and abs(price_momentum) <= 0.12 and liquidity_pressure > 1.1 and range_pct < CONFIG["SILENT_SPREAD"]:
            await self.trigger_alert(
                "🐳 SILENT ACCUMULATION",
                pulse.symbol, current_price, max(z_score, mad_score), delta_vol, price_momentum, Term.PURPLE
            )
            return

        # 3. استراتيجية "التصريف الهادئ" (Silent Distribution)
        if max(z_score, mad_score) > 1.8 and CONFIG["DISTRIBUTION_DRIFT"] <= price_momentum <= 0 and liquidity_pressure > 1.0 and pulse.on_balance_volume < 0 and range_pct < (CONFIG["SILENT_SPREAD"] * 1.3):
            await self.trigger_alert(
                "🥷 SILENT DISTRIBUTION",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.BLUE
            )
            return

        # 4. استراتيجية "حوت الحجم" (Volume Whale)
        if mean_vol > 0 and delta_vol > mean_vol * CONFIG["WHL_SPIKE_MULT"] and short_frame["vol_ratio"] > 1.25:
            await self.trigger_alert(
                "🐋 VOLUME SPIKE",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.YELLOW
            )
            return

        # 5. استراتيجية "كسر الزخم" (Velocity Breakout)
        if liquidity_pressure > CONFIG["ACCELERATION_FACTOR"] * 2 and price_momentum > 0.65 and fast_velocity > 0.25 and short_frame["momentum"] > minute_frame["momentum"]:
            await self.trigger_alert(
                "🚀 VELOCITY BREAKOUT",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.YELLOW
            )
            return

        # 6. استراتيجية "التسارع الأسي" (Exponential Thrust) مطعمة ب MAD
        if pulse.ema_volume and pulse.ema_volume > 0:
            if ema_ratio > (CONFIG["ACCELERATION_FACTOR"] * 1.35) and smoothed_velocity > 0.18 and mad_score > CONFIG["MAD_MULTIPLIER"]:
                await self.trigger_alert(
                    "🌌 EXPONENTIAL THRUST",
                    pulse.symbol, current_price, mad_score, delta_vol, smoothed_velocity, Term.CYAN
                )
                return

        # 7. رادار "الإشعال المبكر" متعدد الأطر
        if composite_score > 2.4 and relative_momentum > 0.2 and fast_ratio > 1.2 and short_frame["momentum"] > 0.4 and short_frame["vol_ratio"] > 1.1:
            await self.trigger_alert(
                "⚡ EARLY IGNITION",
                pulse.symbol, current_price, composite_score, delta_vol, fast_velocity, Term.GREEN
            )

    async def trigger_alert(self, signal_type, symbol, price, z, vol, change, color, force: bool = False):
        timestamp = time.strftime("%H:%M:%S")

        if not force and self.is_on_cooldown(symbol):
            return

        # تنسيق الحجم
        vol_str = f"${vol/1000:.1f}K" if vol < 1000000 else f"${vol/1000000:.2f}M"
        
        # طباعة التنبيه
        print(f"{color}{Term.BOLD}╔══════════════════════════════════════════════════════════╗{Term.END}")
        print(f"{color}║ {signal_type:<25} | {symbol:<10} ⏰ {timestamp}    ║{Term.END}")
        print(f"{color}╠══════════════════════════════════════════════════════════╣{Term.END}")
        print(f"{color}║ 📊 Z-Score: {z:.2f}σ (Rare!)     💎 Price: {price}       ║{Term.END}")
        print(f"{color}║ 🌊 Vol 1s:  {vol_str:<10}     📈 Change: {change:+.2f}%       ║{Term.END}")
        print(f"{color}╚══════════════════════════════════════════════════════════╝{Term.END}")

        self.record_signal(symbol)

        # حفظ في ملف CSV
        await asyncio.to_thread(
            self._write_csv,
            timestamp,
            symbol,
            signal_type,
            price,
            z,
            vol,
            change,
        )

    @staticmethod
    def _write_csv(timestamp: str, symbol: str, signal_type: str, price: float, z: float, vol: float, change: float):
        with open(CONFIG["LOG_FILE"], 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, symbol, signal_type, price, round(z, 2), round(vol, 2), round(change, 2)])

    async def main(self):
        Term.print_banner()
        print(f"{Term.YELLOW}⏳ Calibrating statistical models (Collecting History)...{Term.END}")
        
        # الحل الجذري للمشكلة: إنشاء الطابور داخل الحلقة النشطة هنا
        self.msg_queue = asyncio.Queue(maxsize=CONFIG["MAX_QUEUE_SIZE"])
        
        # تشغيل العمليات بشكل متوازي
        await asyncio.gather(
            self.ws_listener(),
            self.market_analyzer()
        )

if __name__ == "__main__":
    try:
        # إعدادات التوافقية لنظام Pydroid و Windows
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        bot = QuantumSniper()
        asyncio.run(bot.main())
    except KeyboardInterrupt:
        print("\n🚫 System Shutdown.")
    except RuntimeError as e:
        # تجاهل أخطاء إغلاق الحلقة المعروفة في Pydroid
        if "Event loop is closed" not in str(e):
            print(f"Runtime Error: {e}")
