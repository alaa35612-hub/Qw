import asyncio
import json
import time
import math
import os
import csv
import sys
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import aiohttp

# =====================[ ⚙️ إعدادات المحرك الكمي ]=====================

CONFIG = {
    "WINDOW_SIZE": 60,            # نافذة التحليل بالثواني (لصنع المتوسطات)
    "MIN_24H_VOL": 20_000_000,    # تجاهل العملات الميتة (أقل من 10 مليون)
    "MAX_QUEUE_SIZE": 5_000,      # الحد الأقصى للطابور لحماية الذاكرة
    "RECONNECT_BACKOFF": 2,       # ثواني الانتظار قبل إعادة الاتصال
    "EMA_ALPHA": 0.25,            # معامل التنعيم للحجوم السعرية (0.2 = سلاسة أكبر)
    
    # --- [ خوارزميات الحساسية ] ---
    "SIGMA_THRESHOLD": 1.5,       # (Z-Score) الحساسية للشذوذ الإحصائي (3.5 = حدث نادر جداً)
    "ACCELERATION_FACTOR": 1.0,   # معامل تسارع السيولة المطلوب
    
    # --- [ حماية السوق ] ---
    "BTC_PROTECTION": True,       # إيقاف الشراء إذا كان البيتكوين ينهار
    "BTC_DUMP_PERCENT": -0.4,     # نسبة هبوط البيتكوين في الدقيقة التي تفعل الحماية
    
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
        ║           QUANTUM FLOW SNIPER v5.1 (STATISTICAL)              ║
        ║       [ Z-Score Analysis | Momentum Velocity | BTC Guard ]    ║
        ╚═══════════════════════════════════════════════════════════════╝
        {Term.END}""")

# =====================[ 🧠 المحرك الإحصائي ]=====================

@dataclass
class MarketPulse:
    """يخزن نبض السوق لكل عملة لحساب الإحصائيات"""

    symbol: str
    prices: deque = field(default_factory=lambda: deque(maxlen=CONFIG["WINDOW_SIZE"]))
    volumes: deque = field(default_factory=lambda: deque(maxlen=CONFIG["WINDOW_SIZE"]))
    last_accumulated_vol: float = 0.0
    ema_volume: Optional[float] = None
    ema_price: Optional[float] = None
    
    def add_snapshot(self, price: float, accumulated_vol: float) -> float:
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

        # تحديث المتوسط الأسي للحجم والسعر لتقليل الضوضاء ورفع حساسية الكشف
        alpha = CONFIG["EMA_ALPHA"]
        self.ema_volume = delta_vol if self.ema_volume is None else (alpha * delta_vol + (1 - alpha) * self.ema_volume)
        self.ema_price = price if self.ema_price is None else (alpha * price + (1 - alpha) * self.ema_price)

        return delta_vol

    @property
    def is_ready(self):
        # نحتاج بيانات كافية ليكون الانحراف المعياري دقيقاً
        return len(self.volumes) >= 20

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

    def get_price_momentum(self) -> float:
        if len(self.prices) < 5: return 0
        # نسبة التغير خلال آخر 5 ثواني
        start = self.prices[-5]
        end = self.prices[-1]
        return ((end - start) / start) * 100

    def get_smoothed_velocity(self) -> float:
        """حساب نسبة التغير اللحظية باستخدام المتوسط الأسي لزيادة دقة الاستباق."""
        if self.ema_price is None or len(self.prices) < 2:
            return 0
        last_price = self.prices[-1]
        if self.ema_price == 0:
            return 0
        return ((last_price - self.ema_price) / self.ema_price) * 100

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
        pulse.add_snapshot(price, vol)
        
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
        current_price = float(ticker['c'])
        accumulated_vol = float(ticker['q'])

        # حساب الحجم اللحظي قبل التحديث
        prev_vol = pulse.last_accumulated_vol
        if prev_vol == 0:
            pulse.add_snapshot(current_price, accumulated_vol)
            return

        delta_vol = accumulated_vol - prev_vol
        if delta_vol < 0: delta_vol = 0 # Reset case

        # تحديث البيانات التاريخية
        pulse.add_snapshot(current_price, accumulated_vol)

        if not pulse.is_ready: return

        # --- [ المنطق الخارق: التحليل الإحصائي ] ---

        z_score, mean_vol = pulse.calculate_statistics(delta_vol)
        price_momentum = pulse.get_price_momentum()
        smoothed_velocity = pulse.get_smoothed_velocity()

        # 1. استراتيجية "الحدث النووي" (Sigma Event)
        if z_score > CONFIG["SIGMA_THRESHOLD"] and price_momentum > 0.2:
            await self.trigger_alert(
                "☢️ STATISTICAL ANOMALY",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.RED
            )

        # 2. استراتيجية "التجميع المخفي" (Silent Accumulation)
        elif z_score > 2.5 and -0.05 <= price_momentum <= 0.05:
            await self.trigger_alert(
                "🐳 SILENT ACCUMULATION",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.PURPLE
            )

        # 3. استراتيجية "كسر الزخم" (Velocity Breakout)
        vol_acceleration = delta_vol / mean_vol if mean_vol > 0 else 0
        if vol_acceleration > CONFIG["ACCELERATION_FACTOR"] * 2 and price_momentum > 0.5:
            await self.trigger_alert(
                "🚀 VELOCITY BREAKOUT",
                pulse.symbol, current_price, z_score, delta_vol, price_momentum, Term.YELLOW
            )

        # 4. استراتيجية "التسارع الأسي" (Exponential Thrust)
        if pulse.ema_volume and pulse.ema_volume > 0:
            # الاعتماد على التنعيم يقلل الإنذارات الكاذبة ويرصد التدفقات مبكراً
            ema_ratio = delta_vol / pulse.ema_volume if pulse.ema_volume else 0
            if ema_ratio > (CONFIG["ACCELERATION_FACTOR"] * 1.3) and smoothed_velocity > 0.15:
                await self.trigger_alert(
                    "🌌 EXPONENTIAL THRUST",
                    pulse.symbol, current_price, z_score, delta_vol, smoothed_velocity, Term.CYAN
                )

    async def trigger_alert(self, signal_type, symbol, price, z, vol, change, color):
        timestamp = time.strftime("%H:%M:%S")
        
        # تنسيق الحجم
        vol_str = f"${vol/1000:.1f}K" if vol < 1000000 else f"${vol/1000000:.2f}M"
        
        # طباعة التنبيه
        print(f"{color}{Term.BOLD}╔══════════════════════════════════════════════════════════╗{Term.END}")
        print(f"{color}║ {signal_type:<25} | {symbol:<10} ⏰ {timestamp}    ║{Term.END}")
        print(f"{color}╠══════════════════════════════════════════════════════════╣{Term.END}")
        print(f"{color}║ 📊 Z-Score: {z:.2f}σ (Rare!)     💎 Price: {price}       ║{Term.END}")
        print(f"{color}║ 🌊 Vol 1s:  {vol_str:<10}     📈 Change: {change:+.2f}%       ║{Term.END}")
        print(f"{color}╚══════════════════════════════════════════════════════════╝{Term.END}")
        
        # حفظ في ملف CSV
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