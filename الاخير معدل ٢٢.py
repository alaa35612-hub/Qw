#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
الماسح المتوازي المتقدم - الإصدار الاحترافي v5 مع طبقات اكتشاف متقدمة
================================================================================
نسخة v5 احترافية موجهة لـ Pydroid 3 لالتقاط سيناريوهات التدفق، ضغط الشورت، وتمدد الفائدة المفتوحة بدقة أعلى.
تم إضافة: فصل Position Ratio عن Account Ratio، تحليل زمني لانقلاب النسب،
ونمط PRE_EXPLOSION_SETUP، وتجميع الأنماط المتعددة لنفس الرمز بدل التوقف عند أول نمط.
كما تم تخفيف شرط APR لنمط PRE_CLOSE_SQUEEZE وخفض حد الإرسال المبكر،
وتحديث SMART_MONEY_DIVERGENCE ليعتمد على ضغط الشراء وتمدّد OI قصير المدى.
إضافة إلى سجل تشخيص JSONL، وتحمل أفضل لأخطاء SQLite، ووضع خفيف للأداء على الهاتف.
"""

import os
import json
import time
import threading
import sqlite3
import atexit
import random
import re
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import numpy as np
import pandas as pd
from scipy import stats
from binance.client import Client
from binance.exceptions import BinanceAPIException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =============================================================================
# أدوات ملفات ومسارات
# =============================================================================
def ensure_parent_dir(path: str) -> None:
    """إنشاء المجلد الأب للمسار إذا لزم الأمر. آمنة مع المسارات النسبية في Pydroid 3."""
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
    except Exception:
        pass


def safe_float(value, default=0.0):
    try:
        if value is None or value == '':
            return default
        return float(value)
    except Exception:
        return default


# محاولة استيراد hmmlearn (اختياري)
try:
    from hmmlearn import hmm
    HMM_AVAILABLE = True
except ImportError:
    HMM_AVAILABLE = False
    print("⚠️ hmmlearn غير مثبت. سيتم تعطيل تحليل HMM.")

# =============================================================================
# الإعدادات من ملف config.json
# =============================================================================
CONFIG_PATH = "config.json"
DEFAULT_CONFIG = {
    # Binance API
    "BINANCE_API_KEY": "YOUR_API_KEY",
    "BINANCE_API_SECRET": "YOUR_API_SECRET",
    "USE_TESTNET": False,

    # Telegram
    "TELEGRAM_TOKEN": "YOUR_BOT_TOKEN",
    "TELEGRAM_CHAT_ID": "YOUR_CHAT_ID",

    # الفلترة الأولية
    "MIN_VOLUME_24H": 500_000,
    "MIN_PRICE_CHANGE_24H": 2.0,
    "MIN_OI_USDT": 100_000,
    "VOLUME_TO_AVG_RATIO_MIN": 1.2,

    # عتبات عامة (z-score)
    "OI_CHANGE_ZSCORE": 1.5,
    "TOP_TRADER_ZSCORE": 1.5,
    "FUNDING_RATE_ZSCORE": 1.5,

    # عتبات دنيا مطلقة جديدة
    "MIN_ABS_OI_CHANGE": 2.0,           # أقل تغير مطلق لـ OI (%)
    "MIN_ABS_FUNDING_CHANGE": 0.5,      # أقل تغير مطلق للتمويل (%)
    "MIN_VOLUME_USD": 1_000_000,        # أقل حجم تداول يومي بالدولار

    # نمط بصمة الحيتان
    "WHALE_TOP_LONG_ZSCORE": 2.0,
    "WHALE_OI_CHANGE_ZSCORE": 2.0,
    "WHALE_FUNDING_MAX": 0.0,

    # نمط ما قبل الارتفاع
    "PRE_PUMP_FUNDING_MAX": 0.01,
    "PRE_PUMP_OI_CHANGE_MIN": 10.0,
    "PRE_PUMP_TOP_LONG_MIN": 60.0,
    "PRE_PUMP_HOURS": [2, 3, 4],

    # كشف الارتفاع المفاجئ
    "SKYROCKET_THRESHOLD": 8.0,
    "SKYROCKET_TIMEFRAMES": ["5m", "15m", "1h"],

    # عتبات الأنماط الخمسة
    "EXTREME_FUNDING_NEGATIVE": -0.1,
    "MODERATE_FUNDING_POSITIVE_MIN": 0.01,
    "MODERATE_FUNDING_POSITIVE_MAX": 0.1,
    "FUNDING_COUNTDOWN_HOURS": 1.0,
    "MIN_APR_PERCENT": 50.0,
    "LIQUIDITY_SWEEP_WICK_RATIO": 0.3,

    # عتبات الإضافات
    "BASIS_STRONG_NEGATIVE": -0.5,
    "BASIS_STRONG_POSITIVE": 1.0,
    "VOLUME_SPIKE_THRESHOLD": 2.0,
    "OI_ACCELERATION_THRESHOLD": 0.5,

    # إعدادات الإطار الزمني
    "PRIMARY_TIMEFRAME": "5m",
    "OI_HISTORY_TIMEFRAME": "15m",
    "PRICE_CHANGE_TIMEFRAME": "5m",
    "NUMBER_OF_CANDLES": 12,

    # إعدادات تنبيهات التحديث
    "ENABLE_SCORE_UPDATE_ALERTS": True,
    "MIN_SCORE_INCREASE_FOR_ALERT": 10,
    "UPDATE_ALERT_COOLDOWN_MINUTES": 30,

    # عتبات تصنيف القوة
    "STRONG_WHALE_RATIO": 75.0,
    "EXTREME_FUNDING_SHORT": -0.3,
    "OI_SURGE_THRESHOLD": 20.0,
    "VOLUME_EXPLOSION": 3.0,
    "BASIS_EXTREME_NEGATIVE": -1.0,
    "BASIS_EXTREME_POSITIVE": 2.0,

    # إعدادات الفريم اليومي
    "DAILY_TIMEFRAME": "1d",
    "DAILY_LOOKBACK_DAYS": 30,
    "DAILY_MIN_AVG_TOP_RATIO": 70.0,
    "DAILY_OI_TREND_DAYS": 7,
    "DAILY_FUNDING_FLIP_DAYS": 7,
    "DAILY_BASIS_THRESHOLD": 1.0,
    "DAILY_TAKER_VOLUME_RATIO": 1.2,

    # إعدادات تحسين تصنيف القوة
    "POWER_CRITICAL_THRESHOLD": 90,
    "POWER_IMPORTANT_THRESHOLD": 75,
    "POWER_ADD_DAILY_BOOST": 30,

    # إعدادات الأداء (تم تعديلها للسرعة القصوى)
    "MAX_WORKERS": 20,                    # زيادة عدد العمال
    "CHUNK_SIZE": 50,
    "SLEEP_BETWEEN_CHUNKS": 5,
    "SLEEP_BETWEEN_SYMBOLS": 0.0,         # إزالة التأخير بين الرموز
    "REQUEST_TIMEOUT": 20,
    "MAX_RETRIES": 3,
    "DEBUG": True,
    "CACHE_TTL_SECONDS": 30,               # للبيانات اللحظية
    "HISTORIC_CACHE_TTL": 300,             # للبيانات التاريخية (5 دقائق)
    "VOLUME_AVG_CACHE_TTL": 86400,         # لمتوسط الحجم (24 ساعة)
    "SCAN_INTERVAL_MINUTES": 1,

    # إعدادات HMM
    "ENABLE_HMM": True,
    "HMM_N_STATES": 4,
    "HMM_LOOKBACK_DAYS": 60,
    "HMM_UPDATE_INTERVAL": 3600,

    # إعدادات التعلم الذاتي
    "ENABLE_LEARNING": True,
    "LEARNING_INTERVAL_HOURS": 6,
    "MIN_SIGNALS_FOR_LEARNING": 5,
    "PERFORMANCE_THRESHOLD": 0.6,
    "WEIGHT_ADJUSTMENT_RATE": 0.1,

    # أوزان الأنماط
    "PATTERN_WEIGHTS": {
        "BEAR_SQUEEZE": 0.20,
        "WHALE_MOMENTUM": 0.15,
        "PRE_CLOSE_SQUEEZE": 0.25,
        "RETAIL_TRAP": 0.10,
        "PRE_PUMP": 0.30,
        "WHALE_SETUP": 0.25,
        "LONG_TERM_ACCUMULATION": 0.35,
        "STRONG_UPTREND": 0.40,
        "SKYROCKET": 0.50,
        "SMART_MONEY_DIVERGENCE": 0.35,
        "PRE_EXPLOSION_SETUP": 0.38
    },

    # إعدادات إدارة المخاطر
    "RISK_PER_TRADE": 2.0,
    "ATR_PERIOD": 14,
    "ATR_MULTIPLIER_STOP": 2.0,
    "ATR_MULTIPLIER_TAKE_PROFIT": 3.0,

    # إعدادات قاعدة البيانات
    "DB_PATH": "signals.db",
    "SAVE_SIGNALS": True,

    # إعدادات التشخيص والإخراج الملائمة لـ Pydroid 3
    "ENABLE_DIAGNOSTICS": True,
    "DIAGNOSTICS_PATH": "diagnostics.jsonl",
    "EVAL_HORIZONS_MINUTES": [5, 15, 30],
    "BACKTEST_LOOKBACK_HOURS": 72,
    "MAX_DIAGNOSTIC_RECORDS": 5000,
    "WRITE_REJECTION_LOGS": True,
    "PYDROID_MODE": True,
    "MAX_SYMBOLS_PER_CYCLE": 120,

    # عتبات إضافية
    "MIN_APR_PRE_CLOSE": 60,             # أقل APR لقبول نمط PRE_CLOSE_SQUEEZE بعد التخفيف
    "SIGNAL_MIN_SCORE": 65               # أقل درجة لقبول الإشارة المبكرة
}

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            user_config = json.load(f)
            config = DEFAULT_CONFIG.copy()
            config.update(user_config)
            return config
    else:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
        print(f"✅ تم إنشاء ملف {CONFIG_PATH} افتراضياً. يرجى تعديله حسب الحاجة.")
        return DEFAULT_CONFIG.copy()

CONFIG = load_config()

# استخراج الإعدادات
BINANCE_API_KEY = CONFIG["BINANCE_API_KEY"]
BINANCE_API_SECRET = CONFIG["BINANCE_API_SECRET"]
USE_TESTNET = CONFIG["USE_TESTNET"]
TELEGRAM_TOKEN = CONFIG["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = CONFIG["TELEGRAM_CHAT_ID"]
MIN_VOLUME_24H = CONFIG["MIN_VOLUME_24H"]
MIN_PRICE_CHANGE_24H = CONFIG["MIN_PRICE_CHANGE_24H"]
MIN_OI_USDT = CONFIG["MIN_OI_USDT"]
VOLUME_TO_AVG_RATIO_MIN = CONFIG["VOLUME_TO_AVG_RATIO_MIN"]
OI_CHANGE_ZSCORE = CONFIG["OI_CHANGE_ZSCORE"]
TOP_TRADER_ZSCORE = CONFIG["TOP_TRADER_ZSCORE"]
FUNDING_RATE_ZSCORE = CONFIG["FUNDING_RATE_ZSCORE"]
MIN_ABS_OI_CHANGE = CONFIG["MIN_ABS_OI_CHANGE"]
MIN_ABS_FUNDING_CHANGE = CONFIG["MIN_ABS_FUNDING_CHANGE"]
MIN_VOLUME_USD = CONFIG["MIN_VOLUME_USD"]
WHALE_TOP_LONG_ZSCORE = CONFIG["WHALE_TOP_LONG_ZSCORE"]
WHALE_OI_CHANGE_ZSCORE = CONFIG["WHALE_OI_CHANGE_ZSCORE"]
WHALE_FUNDING_MAX = CONFIG["WHALE_FUNDING_MAX"]
PRE_PUMP_FUNDING_MAX = CONFIG["PRE_PUMP_FUNDING_MAX"]
PRE_PUMP_OI_CHANGE_MIN = CONFIG["PRE_PUMP_OI_CHANGE_MIN"]
PRE_PUMP_TOP_LONG_MIN = CONFIG["PRE_PUMP_TOP_LONG_MIN"]
PRE_PUMP_HOURS = CONFIG["PRE_PUMP_HOURS"]
SKYROCKET_THRESHOLD = CONFIG["SKYROCKET_THRESHOLD"]
SKYROCKET_TIMEFRAMES = CONFIG["SKYROCKET_TIMEFRAMES"]
EXTREME_FUNDING_NEGATIVE = CONFIG["EXTREME_FUNDING_NEGATIVE"]
MODERATE_FUNDING_POSITIVE_MIN = CONFIG["MODERATE_FUNDING_POSITIVE_MIN"]
MODERATE_FUNDING_POSITIVE_MAX = CONFIG["MODERATE_FUNDING_POSITIVE_MAX"]
FUNDING_COUNTDOWN_HOURS = CONFIG["FUNDING_COUNTDOWN_HOURS"]
MIN_APR_PERCENT = CONFIG["MIN_APR_PERCENT"]
LIQUIDITY_SWEEP_WICK_RATIO = CONFIG["LIQUIDITY_SWEEP_WICK_RATIO"]
BASIS_STRONG_NEGATIVE = CONFIG["BASIS_STRONG_NEGATIVE"]
BASIS_STRONG_POSITIVE = CONFIG["BASIS_STRONG_POSITIVE"]
VOLUME_SPIKE_THRESHOLD = CONFIG["VOLUME_SPIKE_THRESHOLD"]
OI_ACCELERATION_THRESHOLD = CONFIG["OI_ACCELERATION_THRESHOLD"]
PRIMARY_TIMEFRAME = CONFIG["PRIMARY_TIMEFRAME"]
OI_HISTORY_TIMEFRAME = CONFIG["OI_HISTORY_TIMEFRAME"]
PRICE_CHANGE_TIMEFRAME = CONFIG["PRICE_CHANGE_TIMEFRAME"]
NUMBER_OF_CANDLES = CONFIG["NUMBER_OF_CANDLES"]
ENABLE_SCORE_UPDATE_ALERTS = CONFIG["ENABLE_SCORE_UPDATE_ALERTS"]
MIN_SCORE_INCREASE_FOR_ALERT = CONFIG["MIN_SCORE_INCREASE_FOR_ALERT"]
UPDATE_ALERT_COOLDOWN_MINUTES = CONFIG["UPDATE_ALERT_COOLDOWN_MINUTES"]
STRONG_WHALE_RATIO = CONFIG["STRONG_WHALE_RATIO"]
EXTREME_FUNDING_SHORT = CONFIG["EXTREME_FUNDING_SHORT"]
OI_SURGE_THRESHOLD = CONFIG["OI_SURGE_THRESHOLD"]
VOLUME_EXPLOSION = CONFIG["VOLUME_EXPLOSION"]
BASIS_EXTREME_NEGATIVE = CONFIG["BASIS_EXTREME_NEGATIVE"]
BASIS_EXTREME_POSITIVE = CONFIG["BASIS_EXTREME_POSITIVE"]
DAILY_TIMEFRAME = CONFIG["DAILY_TIMEFRAME"]
DAILY_LOOKBACK_DAYS = CONFIG["DAILY_LOOKBACK_DAYS"]
DAILY_MIN_AVG_TOP_RATIO = CONFIG["DAILY_MIN_AVG_TOP_RATIO"]
DAILY_OI_TREND_DAYS = CONFIG["DAILY_OI_TREND_DAYS"]
DAILY_FUNDING_FLIP_DAYS = CONFIG["DAILY_FUNDING_FLIP_DAYS"]
DAILY_BASIS_THRESHOLD = CONFIG["DAILY_BASIS_THRESHOLD"]
DAILY_TAKER_VOLUME_RATIO = CONFIG["DAILY_TAKER_VOLUME_RATIO"]
POWER_CRITICAL_THRESHOLD = CONFIG["POWER_CRITICAL_THRESHOLD"]
POWER_IMPORTANT_THRESHOLD = CONFIG["POWER_IMPORTANT_THRESHOLD"]
POWER_ADD_DAILY_BOOST = CONFIG["POWER_ADD_DAILY_BOOST"]
MAX_WORKERS = CONFIG["MAX_WORKERS"]
CHUNK_SIZE = CONFIG["CHUNK_SIZE"]
SLEEP_BETWEEN_CHUNKS = CONFIG["SLEEP_BETWEEN_CHUNKS"]
SLEEP_BETWEEN_SYMBOLS = CONFIG["SLEEP_BETWEEN_SYMBOLS"]
REQUEST_TIMEOUT = CONFIG["REQUEST_TIMEOUT"]
MAX_RETRIES = CONFIG["MAX_RETRIES"]
DEBUG = CONFIG["DEBUG"]
CACHE_TTL_SECONDS = CONFIG["CACHE_TTL_SECONDS"]
HISTORIC_CACHE_TTL = CONFIG.get("HISTORIC_CACHE_TTL", 300)
VOLUME_AVG_CACHE_TTL = CONFIG.get("VOLUME_AVG_CACHE_TTL", 86400)
SCAN_INTERVAL_MINUTES = CONFIG["SCAN_INTERVAL_MINUTES"]
ENABLE_HMM = CONFIG["ENABLE_HMM"] and HMM_AVAILABLE
HMM_N_STATES = CONFIG["HMM_N_STATES"]
HMM_LOOKBACK_DAYS = CONFIG["HMM_LOOKBACK_DAYS"]
HMM_UPDATE_INTERVAL = CONFIG["HMM_UPDATE_INTERVAL"]
ENABLE_LEARNING = CONFIG["ENABLE_LEARNING"]
LEARNING_INTERVAL_HOURS = CONFIG["LEARNING_INTERVAL_HOURS"]
MIN_SIGNALS_FOR_LEARNING = CONFIG["MIN_SIGNALS_FOR_LEARNING"]
PERFORMANCE_THRESHOLD = CONFIG["PERFORMANCE_THRESHOLD"]
WEIGHT_ADJUSTMENT_RATE = CONFIG["WEIGHT_ADJUSTMENT_RATE"]
PATTERN_WEIGHTS = CONFIG["PATTERN_WEIGHTS"]
RISK_PER_TRADE = CONFIG["RISK_PER_TRADE"]
ATR_PERIOD = CONFIG["ATR_PERIOD"]
ATR_MULTIPLIER_STOP = CONFIG["ATR_MULTIPLIER_STOP"]
ATR_MULTIPLIER_TAKE_PROFIT = CONFIG["ATR_MULTIPLIER_TAKE_PROFIT"]
DB_PATH = CONFIG["DB_PATH"]
SAVE_SIGNALS = CONFIG["SAVE_SIGNALS"]
ENABLE_DIAGNOSTICS = CONFIG.get("ENABLE_DIAGNOSTICS", True)
DIAGNOSTICS_PATH = CONFIG.get("DIAGNOSTICS_PATH", "diagnostics.jsonl")
EVAL_HORIZONS_MINUTES = CONFIG.get("EVAL_HORIZONS_MINUTES", [5, 15, 30])
BACKTEST_LOOKBACK_HOURS = CONFIG.get("BACKTEST_LOOKBACK_HOURS", 72)
MAX_DIAGNOSTIC_RECORDS = CONFIG.get("MAX_DIAGNOSTIC_RECORDS", 5000)
WRITE_REJECTION_LOGS = CONFIG.get("WRITE_REJECTION_LOGS", True)
PYDROID_MODE = CONFIG.get("PYDROID_MODE", True)
MAX_SYMBOLS_PER_CYCLE = CONFIG.get("MAX_SYMBOLS_PER_CYCLE", 120)
MIN_APR_PRE_CLOSE = CONFIG.get("MIN_APR_PRE_CLOSE", 60)
SIGNAL_MIN_SCORE = CONFIG.get("SIGNAL_MIN_SCORE", 65)

# إعدادات الطبقة المؤسسية الجديدة
FUNDING_Z_EXTREME = CONFIG.get("FUNDING_Z_EXTREME", 2.0)
OI_DELTA_Z_EXTREME = CONFIG.get("OI_DELTA_Z_EXTREME", 1.5)
OI_DELTA_MIN_PCT = CONFIG.get("OI_DELTA_MIN_PCT", 1.5)
COMPRESSION_ATR_RATIO_MAX = CONFIG.get("COMPRESSION_ATR_RATIO_MAX", 0.65)
COMPRESSION_BB_PERCENTILE_MAX = CONFIG.get("COMPRESSION_BB_PERCENTILE_MAX", 35)
OFI_STRONG_THRESHOLD = CONFIG.get("OFI_STRONG_THRESHOLD", 0.12)
CVD_SLOPE_THRESHOLD = CONFIG.get("CVD_SLOPE_THRESHOLD", 0.0)
DEPTH_RATIO_THIN = CONFIG.get("DEPTH_RATIO_THIN", 0.75)
DEPTH_RATIO_VERY_THIN = CONFIG.get("DEPTH_RATIO_VERY_THIN", 0.55)
BOOK_IMBALANCE_MIN = CONFIG.get("BOOK_IMBALANCE_MIN", 0.03)
FLOW_PRICE_CHASE_MAX = CONFIG.get("FLOW_PRICE_CHASE_MAX", 2.8)
FLOW_PRICE_CHASE_HARD_MAX = CONFIG.get("FLOW_PRICE_CHASE_HARD_MAX", 4.5)
OI_PERCENTILE_MIN = CONFIG.get("OI_PERCENTILE_MIN", 70.0)
MIN_ACCEPTANCE_QUALITY = CONFIG.get("MIN_ACCEPTANCE_QUALITY", 0.58)
PPS_WATCH_THRESHOLD = CONFIG.get("PPS_WATCH_THRESHOLD", 0.60)
CALIBRATION_PROFILE = CONFIG.get("CALIBRATION_PROFILE", "balanced")
CALIBRATION_WEIGHTS = {
    "conservative": {
        "oi": 0.24, "compression": 0.20, "flow": 0.18, "cvd": 0.11,
        "liquidity": 0.12, "funding": 0.07, "spot": 0.03, "top": 0.03, "basis": 0.02,
    },
    "balanced": {
        "oi": 0.22, "compression": 0.18, "flow": 0.18, "cvd": 0.10,
        "liquidity": 0.14, "funding": 0.08, "spot": 0.04, "top": 0.04, "basis": 0.02,
    },
    "aggressive": {
        "oi": 0.20, "compression": 0.15, "flow": 0.22, "cvd": 0.12,
        "liquidity": 0.13, "funding": 0.07, "spot": 0.05, "top": 0.04, "basis": 0.02,
    },
}
SPREAD_BPS_MAX = CONFIG.get("SPREAD_BPS_MAX", 18.0)
PPS_WATCH_THRESHOLD = CONFIG.get("PPS_WATCH_THRESHOLD", 0.55)
PPS_ALERT_THRESHOLD = CONFIG.get("PPS_ALERT_THRESHOLD", 0.68)
ENABLE_SPOT_CONFIRMATION = CONFIG.get("ENABLE_SPOT_CONFIRMATION", True)
ENABLE_ORDERBOOK_ANALYSIS = CONFIG.get("ENABLE_ORDERBOOK_ANALYSIS", True)
ORDERBOOK_DEPTH_LIMIT = CONFIG.get("ORDERBOOK_DEPTH_LIMIT", 50)
ORDERBOOK_DEPTH_BPS = CONFIG.get("ORDERBOOK_DEPTH_BPS", 15)
SPOT_BASE_URL = CONFIG.get("SPOT_BASE_URL", "https://api.binance.com")


# =============================================================================
# سجل التشخيص والقبول/الرفض
# =============================================================================
class DiagnosticsLogger:
    def __init__(self, path=DIAGNOSTICS_PATH, enabled=True, max_records=MAX_DIAGNOSTIC_RECORDS):
        self.path = path
        self.enabled = enabled
        self.max_records = max_records
        self.lock = threading.Lock()
        if self.enabled:
            ensure_parent_dir(self.path)

    def _append(self, payload):
        if not self.enabled:
            return
        try:
            payload = dict(payload)
            payload.setdefault('ts', datetime.now().isoformat())
            line = json.dumps(payload, ensure_ascii=False)
            with self.lock:
                with open(self.path, 'a', encoding='utf-8') as f:
                    f.write(line + '\n')
                self._trim_if_needed()
        except Exception:
            pass

    def _trim_if_needed(self):
        try:
            if self.max_records <= 0 or not os.path.exists(self.path):
                return
            with open(self.path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if len(lines) > self.max_records:
                with open(self.path, 'w', encoding='utf-8') as f:
                    f.writelines(lines[-self.max_records:])
        except Exception:
            pass

    def log_reject(self, symbol, pattern, reason, data=None):
        if not WRITE_REJECTION_LOGS:
            return None
        self._append({'type': 'reject', 'symbol': symbol, 'pattern': pattern, 'reason': reason, 'data': data or {}})
        return None

    def log_accept(self, symbol, pattern, score=None, price=None, reasons=None, data=None):
        self._append({'type': 'accept', 'symbol': symbol, 'pattern': pattern, 'score': score, 'price': price, 'reasons': reasons or [], 'data': data or {}})

    def log_error(self, symbol, stage, error):
        self._append({'type': 'error', 'symbol': symbol, 'stage': stage, 'error': str(error)})

diagnostics = DiagnosticsLogger()

def log_rejection(symbol, pattern, reason, **data):
    return diagnostics.log_reject(symbol, pattern, reason, data)

def log_acceptance(symbol, pattern, score=None, price=None, reasons=None, **data):
    diagnostics.log_accept(symbol, pattern, score=score, price=price, reasons=reasons, data=data)

# =============================================================================
# نظام التخزين المؤقت (Cache) مع دعم TTL متغير
# =============================================================================
class TTLCache:
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if expiry > time.time():
                    return value
                else:
                    del self.cache[key]
        return None

    def set(self, key, value, ttl=CACHE_TTL_SECONDS):
        with self.lock:
            expiry = time.time() + ttl
            self.cache[key] = (value, expiry)

    def clear(self):
        with self.lock:
            self.cache.clear()

cache = TTLCache()

# =============================================================================
# مدير الوزن (Weight Manager) لتتبع استهلاك نقاط API
# =============================================================================
class WeightManager:
    def __init__(self, max_weight_per_minute=6000):
        self.max_weight = max_weight_per_minute
        self.used_weight = 0
        self.reset_time = time.time() + 60
        self.lock = threading.Lock()

    def can_request(self, weight=1):
        """التحقق مما إذا كان يمكن إرسال طلب بهذا الوزن."""
        with self.lock:
            now = time.time()
            if now > self.reset_time:
                self.used_weight = 0
                self.reset_time = now + 60
            if self.used_weight + weight <= self.max_weight:
                self.used_weight += weight
                return True
            else:
                return False

    def wait_if_needed(self, weight=1):
        """الانتظار حتى يتوفر الوزن الكافي."""
        while not self.can_request(weight):
            # الانتظار حتى إعادة التعيين التالية
            sleep_time = self.reset_time - time.time()
            if sleep_time > 0:
                time.sleep(min(sleep_time, 1))
        return True

weight_manager = WeightManager()

# =============================================================================
# إدارة الجلسات (Session Pool) مع إعادة المحاولة
# =============================================================================
class SessionManager:
    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.sessions = []
        self.lock = threading.Lock()
        self._init_sessions()

    def _init_sessions(self):
        for i in range(self.max_workers):
            self.sessions.append(self._create_session())

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def get_session(self, index=None):
        if index is not None:
            return self.sessions[index % self.max_workers]
        return random.choice(self.sessions)

    def close_all(self):
        for s in self.sessions:
            s.close()

session_manager = SessionManager(MAX_WORKERS)
atexit.register(session_manager.close_all)

# =============================================================================
# إدارة Binance Client
# =============================================================================
def get_binance_client():
    if USE_TESTNET:
        return Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=True)
    else:
        return Client(BINANCE_API_KEY, BINANCE_API_SECRET)

client = get_binance_client()

# =============================================================================
# دوال جلب البيانات من Binance مع تحسينات الأداء والتخزين المؤقت
# =============================================================================
BASE_URL = "https://fapi.binance.com"

def fetch_json(url, params=None, use_cache=True, weight=1, cache_ttl=None):
    if cache_ttl is None:
        cache_ttl = CACHE_TTL_SECONDS
    cache_key = url + str(sorted(params.items())) if params else url
    if use_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    # الانتظار حتى يتوفر الوزن
    weight_manager.wait_if_needed(weight)

    session = session_manager.get_session()
    max_retries = MAX_RETRIES
    retry_delay = 1  # ثانية أولية
    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if use_cache:
                    cache.set(cache_key, data, ttl=cache_ttl)
                return data
            elif resp.status_code in (429, 418):
                # تجاوز حد الطلبات - الانتظار حسب Retry-After أو تأخير تصاعدي
                retry_after = int(resp.headers.get('Retry-After', retry_delay))
                print(f"⚠️ تجاوز حد الطلبات، انتظار {retry_after} ثانية...")
                time.sleep(retry_after)
                retry_delay *= 2  # exponential backoff
                continue
            else:
                if DEBUG:
                    print(f"⚠️ HTTP {resp.status_code} لـ {url}")
                return None
        except requests.exceptions.RequestException as e:
            if DEBUG:
                print(f"🔴 خطأ في {url} (محاولة {attempt+1}): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                return None
    return None

def get_all_usdt_perpetuals():
    data = fetch_json(f"{BASE_URL}/fapi/v1/exchangeInfo", use_cache=True, weight=10, cache_ttl=3600)  # ساعة كاملة
    if not data:
        return []
    return [s['symbol'] for s in data['symbols']
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']

def get_ticker_24hr():
    # وزن 40 لجلب الكل
    return fetch_json(f"{BASE_URL}/fapi/v1/ticker/24hr", use_cache=True, weight=40, cache_ttl=10)  # 10 ثوانٍ فقط

def get_ticker_24hr_one_symbol(symbol):
    # نستخدم cache لمدة ثانيتين فقط لأن السعر يتغير بسرعة
    data = fetch_json(f"{BASE_URL}/fapi/v1/ticker/24hr", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=2)
    return data

def get_open_interest(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/openInterest", {'symbol': symbol}, use_cache=False, weight=1)
    if data is None:
        # إذا فشل الطلب، نسجل الرمز كغير صالح لمنع المحاولات المتكررة
        mark_symbol_invalid(symbol)
        return None
    return float(data['openInterest']) if data else None

def get_funding_rate(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=5)  # 5 ثوانٍ
    if data:
        return float(data['lastFundingRate']) * 100, int(data['nextFundingTime'])
    return None, None

def get_top_long_short_ratio(symbol):
    """متوافق للخلفية القديمة: يعيد Position Ratio snapshot فقط."""
    snapshot = get_top_position_snapshot(symbol, period='5m')
    if snapshot:
        return snapshot['ratio'], snapshot['long_pct'], snapshot['short_pct']
    return None, None, None

def get_top_position_snapshot(symbol, period='5m'):
    """جلب لقطة Position Ratio الخاصة بكبار المتداولين."""
    data = fetch_json(
        f"{BASE_URL}/futures/data/topLongShortPositionRatio",
        {'symbol': symbol, 'period': period, 'limit': 1},
        use_cache=True, weight=1, cache_ttl=10
    )
    if data and len(data) > 0:
        row = data[0]
        return {
            'ratio': float(row['longShortRatio']),
            'long_pct': float(row['longAccount']),
            'short_pct': float(row['shortAccount']),
        }
    return None

def get_top_account_snapshot(symbol, period='5m'):
    """جلب لقطة Account Ratio الخاصة بكبار المتداولين."""
    data = fetch_json(
        f"{BASE_URL}/futures/data/topLongShortAccountRatio",
        {'symbol': symbol, 'period': period, 'limit': 1},
        use_cache=True, weight=1, cache_ttl=10
    )
    if data and len(data) > 0:
        row = data[0]
        return {
            'ratio': float(row['longShortRatio']),
            'long_pct': float(row['longAccount']),
            'short_pct': float(row['shortAccount']),
        }
    return None

def get_position_ratio_history(symbol, period='5m', limit=12):
    """جلب تاريخ نسبة مراكز كبار المتداولين (Position Ratio) مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"pos_ratio_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        ratios = [float(d['longShortRatio']) for d in data][::-1]
        cache.set(cache_key, ratios, ttl=HISTORIC_CACHE_TTL)
        return ratios
    return []

def get_account_ratio_history(symbol, period='5m', limit=12):
    """جلب تاريخ نسبة حسابات كبار المتداولين (Account Ratio) مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"acc_ratio_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortAccountRatio",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        ratios = [float(d['longShortRatio']) for d in data][::-1]
        cache.set(cache_key, ratios, ttl=HISTORIC_CACHE_TTL)
        return ratios
    return []

def get_klines(symbol, interval=None, limit=None):
    if interval is None:
        interval = PRIMARY_TIMEFRAME
    if limit is None:
        limit = NUMBER_OF_CANDLES
    # وزن klines = 1 لكل 100 شمعة (تقريباً)
    weight = max(1, limit // 100)
    cache_key = f"klines_{symbol}_{interval}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/fapi/v1/klines",
                      {'symbol': symbol, 'interval': interval, 'limit': limit}, use_cache=False, weight=weight)
    if data:
        result = {
            'open': [float(k[1]) for k in data],
            'high': [float(k[2]) for k in data],
            'low': [float(k[3]) for k in data],
            'close': [float(k[4]) for k in data],
            'volume': [float(k[5]) for k in data],
            'taker_buy_volume': [float(k[9]) for k in data]
        }
        cache.set(cache_key, result, ttl=30)  # 30 ثانية للشموع
        return result
    return None

def get_oi_history(symbol, period=None, limit=30):
    """جلب تاريخ الفائدة المفتوحة مع ترتيب زمني (الأقدم أولاً)."""
    if period is None:
        period = OI_HISTORY_TIMEFRAME
    cache_key = f"oi_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/openInterestHist",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        # البيانات تأتي من الأحدث إلى الأقدم، نعكسها
        oi = [float(d['sumOpenInterest']) for d in data][::-1]
        cache.set(cache_key, oi, ttl=HISTORIC_CACHE_TTL)
        return oi
    return []

def get_funding_rate_history(symbol, limit=30):
    """جلب تاريخ معدلات التمويل مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"funding_hist_{symbol}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/fapi/v1/fundingRate",
                      {'symbol': symbol, 'limit': limit}, use_cache=False, weight=1)
    if data:
        # البيانات تأتي من الأحدث إلى الأقدم، نعكسها
        rates = [float(d['fundingRate']) * 100 for d in data][::-1]
        cache.set(cache_key, rates, ttl=HISTORIC_CACHE_TTL)
        return rates
    return []

def get_basis(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=2)
    if data:
        mark = float(data['markPrice'])
        index = float(data['indexPrice'])
        return (mark - index) / index * 100
    return 0.0

def get_mark_price(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=2)
    if data:
        return float(data['markPrice'])
    return None

def pct_change(old, new):
    return ((new - old) / old) * 100 if old and old != 0 else 0


def get_oi_notional_history(symbol, period='5m', limit=12):
    """جلب تاريخ القيمة الاسمية للفائدة المفتوحة إن كانت متاحة."""
    cache_key = f"oi_notional_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/openInterestHist", {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        values = []
        for d in data[::-1]:
            v = d.get('sumOpenInterestValue')
            values.append(float(v) if v not in (None, '') else 0.0)
        cache.set(cache_key, values, ttl=HISTORIC_CACHE_TTL)
        return values
    return []

def rolling_mean(seq):
    return float(np.mean(seq)) if seq else 0.0

def compute_taker_buy_ratio(kl):
    ratios = []
    if not kl or 'volume' not in kl or 'taker_buy_volume' not in kl:
        return ratios
    for vol, tb in zip(kl['volume'], kl['taker_buy_volume']):
        ratios.append((tb / vol) if vol and vol > 0 else 0.0)
    return ratios

def classify_signal_stage(price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0, funding_rate=None, buy_ratio=0.0, score=0.0):
    funding_abs = abs(funding_rate) if funding_rate is not None else 0.0
    if price_5m >= 3.5 or price_15m >= 8.0 or score >= 90 or funding_abs >= 0.08:
        return 'LATE'
    if price_5m >= 2.0 or price_15m >= 4.0 or oi_15m >= 6.0 or score >= 80:
        return 'CONFIRMED'
    if buy_ratio >= 0.58 and (oi_5m >= 1.5 or oi_15m >= 2.5) and price_15m >= 0.8:
        return 'TRIGGERED'
    if buy_ratio >= 0.55 and (oi_5m >= 1.0 or oi_15m >= 2.0):
        return 'ARMED'
    return 'SETUP'

def attach_signal_stage(signal_data, price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0, funding_rate=None, buy_ratio=0.0):
    if not signal_data:
        return signal_data
    signal_data['signal_stage'] = classify_signal_stage(price_5m=price_5m, price_15m=price_15m, oi_5m=oi_5m, oi_15m=oi_15m, funding_rate=funding_rate, buy_ratio=buy_ratio, score=signal_data.get('score', 0))
    return signal_data

def calculate_apr(funding_rate_percent):
    return funding_rate_percent * 2190

# =============================================================================
# دوال إحصائية متقدمة
# =============================================================================
def mean(lst):
    return sum(lst) / len(lst) if lst else 0

def std(lst):
    if len(lst) < 2:
        return 0
    m = mean(lst)
    variance = sum((x - m) ** 2 for x in lst) / (len(lst) - 1)
    return variance ** 0.5

def zscore(series, value):
    if len(series) < 3:
        return 0
    m = mean(series)
    s = std(series)
    if s == 0:
        return 0
    return (value - m) / s

def percentile(series, value):
    if not series:
        return 50
    count_less = sum(1 for x in series if x < value)
    return (count_less / len(series)) * 100

def clamp(value, low=0.0, high=1.0):
    try:
        return max(low, min(high, float(value)))
    except Exception:
        return low


def scale_to_unit(value, low, high):
    if high <= low:
        return 0.0
    return clamp((value - low) / (high - low), 0.0, 1.0)


def robust_zscore(series, value):
    if not series or len(series) < 5:
        return 0.0
    arr = np.array(series, dtype=float)
    med = float(np.median(arr))
    mad = float(np.median(np.abs(arr - med)))
    if mad == 0:
        return zscore(series, value)
    return 0.6745 * (value - med) / mad


def linear_trend_slope(series):
    if not series or len(series) < 2:
        return 0.0
    x = np.arange(len(series), dtype=float)
    y = np.array(series, dtype=float)
    try:
        slope, _ = np.polyfit(x, y, 1)
        return float(slope)
    except Exception:
        return 0.0


def calc_true_range(highs, lows, closes):
    if len(highs) < 2 or len(lows) < 2 or len(closes) < 2:
        return []
    trs = []
    prev_close = closes[0]
    for h, l, c in zip(highs[1:], lows[1:], closes[1:]):
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
    return trs


def calc_atr(highs, lows, closes, period=14):
    trs = calc_true_range(highs, lows, closes)
    if len(trs) < max(2, period):
        return mean(trs) if trs else 0.0
    return mean(trs[-period:])


def bollinger_bandwidth_percentile(closes, window=20, lookback=60):
    if not closes or len(closes) < window + 5:
        return 50.0, 0.0
    series = pd.Series(closes)
    ma = series.rolling(window).mean()
    stdv = series.rolling(window).std(ddof=0)
    upper = ma + 2 * stdv
    lower = ma - 2 * stdv
    bw = ((upper - lower) / ma.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    valid = bw.dropna().tolist()
    if not valid:
        return 50.0, 0.0
    recent = float(valid[-1])
    hist = valid[-min(len(valid), lookback):]
    return percentile(hist, recent), recent


def safe_div(numerator, denominator, default=0.0):
    try:
        if denominator in (0, None):
            return default
        return numerator / denominator
    except Exception:
        return default


def ema(series, alpha=0.3):
    if not series:
        return 0
    result = series[0]
    for val in series[1:]:
        result = alpha * val + (1 - alpha) * result
    return result

def rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(-change)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)

def macd(prices):
    if len(prices) < 26:
        return 0
    ema12 = ema(prices[-12:], alpha=2/(12+1))
    ema26 = ema(prices[-26:], alpha=2/(26+1))
    return ema12 - ema26

def whale_retail_gap(tr, gr):
    if tr and gr and gr > 0:
        gap = tr / gr
        if gap > 1.2:
            return "bullish_gap", gap
        elif gap < 0.8:
            return "bearish_gap", gap
    return "neutral", 1.0

def detect_liquidity_sweep(highs, lows, closes, volumes, current_price):
    if len(highs) < 10:
        return False
    avg_volume = mean(volumes[-11:-1]) if len(volumes) >= 11 else mean(volumes)
    for i in range(1, 6):
        if i > len(highs):
            break
        high = highs[-i]
        low = lows[-i]
        close = closes[-i]
        volume = volumes[-i]
        candle_range = high - low
        if candle_range == 0:
            continue
        is_bullish = close > (high + low) / 2
        if is_bullish:
            wick_ratio = (high - close) / candle_range
        else:
            wick_ratio = (close - low) / candle_range
        volume_ratio = volume / avg_volume if avg_volume > 0 else 1
        if wick_ratio > LIQUIDITY_SWEEP_WICK_RATIO and volume_ratio > 1.5:
            if is_bullish and abs(current_price - high) / current_price < 0.01:
                return True
            if not is_bullish and abs(current_price - low) / current_price < 0.01:
                return True
    return False

# =============================================================================
# دوال جديدة للتحسينات (z-score, تاريخ يومي، إدارة مخاطر)
# =============================================================================

def get_historical_stats(symbol, feature, limit=30):
    """
    جلب إحصائيات تاريخية لمؤشر معين (مثل نسبة كبار، OI، تمويل)
    لحساب المتوسط والانحراف المعياري.
    """
    if feature == 'top_ratio':
        data = get_daily_top_ratio(symbol, limit)
        return data if data else []
    elif feature == 'oi':
        data = get_oi_history(symbol, period='4h', limit=limit*6)  # تقريب يومي
        if data:
            # نأخذ كل 6 قيم كقيمة يومية
            daily = [data[i] for i in range(0, len(data), 6)]
            return daily[-limit:]
        return []
    elif feature == 'funding':
        data = get_funding_rate_history(symbol, limit=limit*3)
        if data:
            # نأخذ كل 3 قيم (8 ساعات) كقيمة يومية
            daily = [sum(data[i:i+3])/len(data[i:i+3]) for i in range(0, len(data), 3)]
            return daily[-limit:]
        return []
    return []

def get_daily_top_ratio(symbol, limit=30):
    """نسبة كبار على فريم يومي (من 4h)"""
    cache_key = f"daily_top_{symbol}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                      {'symbol': symbol, 'period': '4h', 'limit': limit*6}, use_cache=False, weight=1)
    if not data:
        return []
    # البيانات من الأحدث إلى الأقدم، نعكس
    data = data[::-1]
    daily = []
    for i in range(0, len(data), 6):
        if i < len(data):
            daily.append(float(data[i]['longAccount']))
    result = daily[-limit:]
    cache.set(cache_key, result, ttl=HISTORIC_CACHE_TTL)
    return result

def get_historical_avg_volume(symbol, days=7):
    cache_key = f"avg_vol_{symbol}_{days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    klines = get_klines(symbol, '1d', limit=days)
    if not klines or len(klines['volume']) < days:
        return None
    volumes = klines['volume']
    avg = mean(volumes)
    cache.set(cache_key, avg, ttl=VOLUME_AVG_CACHE_TTL)
    return avg

def analyze_basis(symbol, current_reasons=None):
    basis = get_basis(symbol)
    if basis is None:
        return 0, []
    score = 0
    reasons = []
    reason_text = ""
    if basis < BASIS_STRONG_NEGATIVE:
        score += 15
        reason_text = f"📉 أساس سالب قوي {basis:+.2f}%"
    elif basis > BASIS_STRONG_POSITIVE:
        score += 5
        reason_text = f"⚠️ أساس موجب قوي {basis:+.2f}% (قد يكون تشبعاً)"
    if reason_text and (not current_reasons or reason_text not in current_reasons):
        reasons.append(reason_text)
    return score, reasons

def analyze_oi_acceleration(symbol, current_reasons=None):
    oi_hist = get_oi_history(symbol, period=OI_HISTORY_TIMEFRAME, limit=6)
    if len(oi_hist) < 6:
        return 0, []
    mom1 = oi_hist[-1] - oi_hist[-3]
    mom2 = oi_hist[-3] - oi_hist[-6]
    acceleration = mom1 - mom2
    if acceleration <= 0:
        return 0, []
    reason_text = ""
    if acceleration > OI_ACCELERATION_THRESHOLD * 1_000_000:
        score = 20
        reason_text = f"⚡ تسارع OI (قيمة {acceleration:.0f})"
    elif acceleration > OI_ACCELERATION_THRESHOLD * 500_000:
        score = 10
        reason_text = f"📈 تسارع OI معتدل"
    else:
        return 0, []
    reasons = []
    if reason_text and (not current_reasons or reason_text not in current_reasons):
        reasons.append(reason_text)
    return score, reasons

def analyze_volume_spike(symbol, current_reasons=None):
    ticker = get_ticker_24hr_one_symbol(symbol)
    if not ticker:
        return 0, []
    current_volume = float(ticker['volume'])
    avg_volume = get_historical_avg_volume(symbol, days=7)
    if not avg_volume or avg_volume == 0:
        return 0, []
    spike_ratio = current_volume / avg_volume
    reason_text = ""
    if spike_ratio >= VOLUME_SPIKE_THRESHOLD:
        score = 20
        reason_text = f"📊 حجم التداول {spike_ratio:.1f}x المتوسط"
    elif spike_ratio >= VOLUME_SPIKE_THRESHOLD * 0.75:
        score = 10
        reason_text = f"📈 حجم أعلى من المتوسط"
    else:
        return 0, []
    reasons = []
    if reason_text and (not current_reasons or reason_text not in current_reasons):
        reasons.append(reason_text)
    return score, reasons

def calculate_atr(symbol, period=14):
    """
    حساب Average True Range (ATR) على الفريم الرئيسي.
    """
    klines = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=period+1)
    if not klines or len(klines['close']) < period+1:
        return None
    tr_list = []
    for i in range(1, len(klines['close'])):
        high = klines['high'][i]
        low = klines['low'][i]
        prev_close = klines['close'][i-1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
    atr = sum(tr_list[-period:]) / period
    return atr

def linear_trend_slope(y):
    """
    حساب ميل الانحدار الخطي لسلسلة y.
    """
    if len(y) < 2:
        return 0
    x = list(range(len(y)))
    n = len(y)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi**2 for xi in x)
    denominator = n * sum_x2 - sum_x**2
    if denominator == 0:
        return 0
    slope = (n * sum_xy - sum_x * sum_y) / denominator
    return slope


def analyze_ratio_shift(symbol, period='5m', limit=12):
    """تحليل الانعكاس الزمني بين Position Ratio وAccount Ratio."""
    pos_hist = get_position_ratio_history(symbol, period=period, limit=limit)
    acc_hist = get_account_ratio_history(symbol, period=period, limit=limit)

    if len(pos_hist) < 6 or len(acc_hist) < 6:
        return None

    pos_now = pos_hist[-1]
    pos_prev = pos_hist[-4]
    acc_now = acc_hist[-1]
    acc_prev = acc_hist[-4]

    pos_delta = pos_now - pos_prev
    acc_delta = acc_now - acc_prev

    pos_slope = linear_trend_slope(pos_hist[-6:])
    acc_slope = linear_trend_slope(acc_hist[-6:])

    return {
        'pos_now': pos_now,
        'pos_prev': pos_prev,
        'acc_now': acc_now,
        'acc_prev': acc_prev,
        'pos_delta': pos_delta,
        'acc_delta': acc_delta,
        'pos_slope': pos_slope,
        'acc_slope': acc_slope,
        'divergence_now': acc_now - pos_now
    }

def check_with_zscore(symbol, feature, value, z_threshold):
    """
    التحقق مما إذا كانت قيمة معينة تتجاوز z-score معين باستخدام البيانات التاريخية.
    مع شرط إضافي: يجب أن تتجاوز القيمة الحد الأدنى المطلق.
    """
    # التحقق من الحد الأدنى المطلق أولاً
    if feature == 'oi' and abs(value) < MIN_ABS_OI_CHANGE:
        return False
    if feature == 'funding' and abs(value) < MIN_ABS_FUNDING_CHANGE:
        return False

    hist = get_historical_stats(symbol, feature, limit=30)
    if len(hist) < 10:
        return False  # بيانات غير كافية
    z = zscore(hist, value)
    return abs(z) >= z_threshold

# =============================================================================
# دوال جديدة لتطبيع معدلات التمويل وتحليل التدفق وعناقيد التصفية
# =============================================================================

# تخزين مؤقت لفترات التمويل لكل رمز
funding_intervals_cache = {}

def get_funding_interval(symbol):
    """إرجاع فترة التمويل بالدقائق للرمز."""
    if symbol in funding_intervals_cache:
        return funding_intervals_cache[symbol]
    # جلب معلومات الرمز من exchangeInfo (يمكن تخزينها مؤقتًا)
    data = fetch_json(f"{BASE_URL}/fapi/v1/exchangeInfo", use_cache=True, weight=10, cache_ttl=3600)
    if data:
        for s in data['symbols']:
            if s['symbol'] == symbol:
                # فترة التمويل موجودة في s['fundingInterval'] (بالساعات) أو s['fundingIntervalHours']
                interval_hours = s.get('fundingInterval', 8)  # الافتراضي 8 ساعات
                interval_minutes = interval_hours * 60
                funding_intervals_cache[symbol] = interval_minutes
                return interval_minutes
    return 8 * 60  # افتراضي 8 ساعات

def normalize_funding_rate(funding_rate_percent, interval_minutes):
    """تحويل معدل التمويل إلى APR سنوي."""
    # funding_rate_percent هو النسبة المئوية للفترة
    # APR = funding_rate_percent * (365 * 24 * 60 / interval_minutes)
    periods_per_year = (365 * 24 * 60) / interval_minutes
    return funding_rate_percent * periods_per_year

def analyze_intrabar_orderflow(symbol):
    """
    تحليل تدفق الأوامر قصير المدى اعتمادًا على شموع 1m/5m بدلاً من الاعتماد على ticker 24h فقط.
    يعيد score + reasons + meta قابلة للاستخدام في طبقة الـ score الموحدة.
    """
    kl_1m = get_klines(symbol, interval='1m', limit=12)
    kl_5m = get_klines(symbol, interval='5m', limit=12)
    if not kl_1m or len(kl_1m.get('close', [])) < 6:
        return 0, [], {}

    ratios = compute_taker_buy_ratio(kl_1m)
    deltas = []
    ofis = []
    for vol, tb in zip(kl_1m['volume'], kl_1m['taker_buy_volume']):
        sell = max(vol - tb, 0.0)
        total = tb + sell
        deltas.append(tb - sell)
        ofis.append(safe_div(tb - sell, total, default=0.0))

    recent_buy_ratio = mean(ratios[-3:]) if len(ratios) >= 3 else mean(ratios)
    prev_buy_ratio = mean(ratios[-6:-3]) if len(ratios) >= 6 else recent_buy_ratio
    buy_ratio_delta = recent_buy_ratio - prev_buy_ratio
    recent_ofi = mean(ofis[-3:]) if len(ofis) >= 3 else mean(ofis)
    peak_ofi = max(ofis[-6:]) if len(ofis) >= 6 else max(ofis)
    cvd = np.cumsum(deltas).tolist()
    cvd_slope = linear_trend_slope(cvd[-6:]) if len(cvd) >= 6 else 0.0

    volume_now = mean(kl_1m['volume'][-3:])
    volume_prev = mean(kl_1m['volume'][-9:-3]) if len(kl_1m['volume']) >= 9 else mean(kl_1m['volume'][:-3])
    volume_ratio = safe_div(volume_now, volume_prev, default=1.0)

    price_change_5m = 0.0
    if kl_5m and len(kl_5m.get('close', [])) >= 2:
        price_change_5m = pct_change(kl_5m['close'][-2], kl_5m['close'][-1])

    score = 0
    reasons = []
    if recent_buy_ratio >= 0.56 and buy_ratio_delta > 0.015:
        score += 18
        reasons.append(f"شراء عدواني قصير {recent_buy_ratio:.1%}")
    if recent_ofi >= 0.08 or peak_ofi >= 0.15:
        score += 16
        reasons.append(f"OFI موجب {recent_ofi:+.2f}")
    if cvd_slope > 0:
        score += 10
        reasons.append("CVD يميل للصعود")
    if volume_ratio >= 1.25:
        score += 8
        reasons.append(f"تسارع حجم {volume_ratio:.2f}x")
    if abs(price_change_5m) < 1.0 and volume_ratio >= 1.25 and recent_buy_ratio >= 0.55:
        score += 10
        reasons.append("امتصاص/تجميع تحت سعر شبه ثابت")

    meta = {
        'buy_ratio': recent_buy_ratio,
        'buy_ratio_delta': buy_ratio_delta,
        'ofi_recent': recent_ofi,
        'ofi_peak': peak_ofi,
        'cvd_slope': cvd_slope,
        'volume_ratio': volume_ratio,
        'price_change_5m': price_change_5m,
    }
    return min(score, 60), reasons[:4], meta


def estimate_liquidation_levels(symbol, price, lookback_hours=24):
    """
    تقدير مستويات التصفية بناءً على الرافعة المالية.
    تعيد قائمة بمستويات السعر التي قد تسبب تصفيات كبيرة.
    """
    klines = get_klines(symbol, '1h', limit=lookback_hours)
    if not klines or len(klines['high']) < 2:
        return []
    highs = klines['high']
    lows = klines['low']
    closes = klines['close']

    # أعلى وأدنى سعر خلال الفترة
    recent_high = max(highs)
    recent_low = min(lows)

    # الرافعات الشائعة: 10x, 20x, 50x (نسبة التصفية = 1/الرافعة)
    leverages = [10, 20, 50]
    liquidation_levels = []

    # للمراكز الطويلة: سعر التصفية = سعر الدخول / (1 + 1/رافعة) (تقريباً)
    # للمراكز القصيرة: سعر التصفية = سعر الدخول / (1 - 1/رافعة)
    # نفترض أن معظم المراكز دخلت عند القمة (للطويلة) أو القاع (للقصيرة)
    for lev in leverages:
        liq_short_above = recent_high * (1 + 1/lev)  # لمن هم في مراكز قصيرة عند القمة
        liq_long_below = recent_low / (1 + 1/lev)    # لمن هم في مراكز طويلة عند القاع
        liquidation_levels.append(('SHORT', liq_short_above, lev))
        liquidation_levels.append(('LONG', liq_long_below, lev))

    # ترتيب حسب القرب من السعر الحالي
    liquidation_levels.sort(key=lambda x: abs(x[1] - price))

    # إرجاع أقرب 5 مستويات
    return liquidation_levels[:5]

def check_liquidation_clusters(symbol, price):
    """
    تقييم ما إذا كان السعر الحالي قريباً من عنقود تصفية.
    """
    levels = estimate_liquidation_levels(symbol, price)
    if not levels:
        return 0, []

    score = 0
    reasons = []
    for direction, level, lev in levels:
        distance_pct = abs(level - price) / price * 100
        if distance_pct < 2.0:  # في نطاق 2%
            score += max(0, 30 - distance_pct * 5)  # كلما اقترب زادت الدرجة
            reasons.append(f"💥 عنقود تصفية {direction} على بعد {distance_pct:.2f}% (رافعة {lev}x)")

    return min(score, 50), reasons[:3]

# =============================================================================
# دوال الفريم اليومي
# =============================================================================

def get_daily_klines(symbol, limit=30):
    return get_klines(symbol, interval='1d', limit=limit)

def get_daily_oi_history(symbol, limit=30):
    oi_hist = get_oi_history(symbol, period='4h', limit=limit*6)
    if not oi_hist:
        return []
    daily_oi = [oi_hist[i] for i in range(0, len(oi_hist), 6)]
    return daily_oi[-limit:]

def get_daily_funding_history(symbol, limit=30):
    funding_hist = get_funding_rate_history(symbol, limit=limit*3)
    if not funding_hist:
        return []
    daily_funding = []
    for i in range(0, len(funding_hist), 3):
        chunk = funding_hist[i:i+3]
        if chunk:
            daily_funding.append(sum(chunk)/len(chunk))
    return daily_funding[-limit:]

def get_daily_taker_volume(symbol):
    ticker = get_ticker_24hr_one_symbol(symbol)
    if not ticker:
        return None, None
    buy_vol = float(ticker.get('takerBuyBaseVolume', 0))
    sell_vol = float(ticker.get('takerSellBaseVolume', 0))
    return buy_vol, sell_vol

def get_spot_klines(symbol, interval='5m', limit=24):
    if not ENABLE_SPOT_CONFIRMATION:
        return None
    cache_key = f"spot_klines_{symbol}_{interval}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{SPOT_BASE_URL}/api/v3/klines", {'symbol': symbol, 'interval': interval, 'limit': limit}, use_cache=False, weight=1)
    if not data:
        return None
    result = {
        'open': [float(k[1]) for k in data],
        'high': [float(k[2]) for k in data],
        'low': [float(k[3]) for k in data],
        'close': [float(k[4]) for k in data],
        'volume': [float(k[5]) for k in data],
        'taker_buy_volume': [float(k[9]) for k in data]
    }
    cache.set(cache_key, result, ttl=30)
    return result


def get_orderbook_snapshot(symbol, limit=50):
    if not ENABLE_ORDERBOOK_ANALYSIS:
        return None
    limit = max(20, min(int(limit), 1000))
    cache_key = f"depth_{symbol}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/fapi/v1/depth", {'symbol': symbol, 'limit': limit}, use_cache=False, weight=2, cache_ttl=3)
    if not data:
        return None
    try:
        result = {
            'bids': [(float(p), float(q)) for p, q in data.get('bids', [])],
            'asks': [(float(p), float(q)) for p, q in data.get('asks', [])],
            'lastUpdateId': data.get('lastUpdateId')
        }
        cache.set(cache_key, result, ttl=3)
        return result
    except Exception:
        return None


def compute_funding_regime(symbol, limit=30):
    hist = get_funding_rate_history(symbol, limit=limit)
    if len(hist) < 6:
        return {'current': None, 'z': 0.0, 'flip': 0, 'persistence': 0, 'trend': 0.0, 'series': hist}
    current = hist[-1]
    base = hist[:-1]
    prev = hist[-2]
    persistence = 0
    sign = 1 if current > 0 else -1 if current < 0 else 0
    for val in reversed(hist):
        val_sign = 1 if val > 0 else -1 if val < 0 else 0
        if val_sign == sign:
            persistence += 1
        else:
            break
    return {
        'current': current,
        'z': zscore(base, current),
        'robust_z': robust_zscore(base, current),
        'flip': 1 if (prev <= 0 < current or prev >= 0 > current) else 0,
        'persistence': persistence,
        'trend': linear_trend_slope(hist[-8:]),
        'series': hist,
    }


def compute_oi_regime(symbol):
    hist_5m = get_oi_history(symbol, period='5m', limit=24)
    hist_15m = get_oi_history(symbol, period='15m', limit=16)
    if len(hist_5m) < 8:
        return None
    deltas_5m = [pct_change(hist_5m[i-1], hist_5m[i]) for i in range(1, len(hist_5m))]
    current_delta_5m = deltas_5m[-1] if deltas_5m else 0.0
    delta_15m = pct_change(hist_15m[-2], hist_15m[-1]) if hist_15m and len(hist_15m) >= 2 else 0.0
    delta_30m = pct_change(hist_5m[-7], hist_5m[-1]) if len(hist_5m) >= 7 else 0.0
    return {
        'current_delta_5m': current_delta_5m,
        'delta_15m': delta_15m,
        'delta_30m': delta_30m,
        'z_5m': zscore(deltas_5m[:-1], current_delta_5m) if len(deltas_5m) >= 4 else 0.0,
        'robust_z_5m': robust_zscore(deltas_5m[:-1], current_delta_5m) if len(deltas_5m) >= 6 else 0.0,
        'percentile_5m': percentile(deltas_5m[:-1], current_delta_5m) if len(deltas_5m) >= 4 else 50.0,
        'series_5m': hist_5m,
        'series_15m': hist_15m,
    }


def compute_volatility_compression(symbol):
    kl = get_klines(symbol, interval='5m', limit=72)
    if not kl or len(kl['close']) < 30:
        return None
    atr_short = calc_atr(kl['high'][-15:], kl['low'][-15:], kl['close'][-15:], period=14)
    atr_long = calc_atr(kl['high'], kl['low'], kl['close'], period=28)
    atr_ratio = safe_div(atr_short, atr_long, default=1.0)
    bb_pct, bb_value = bollinger_bandwidth_percentile(kl['close'], window=20, lookback=60)
    last_6 = kl['close'][-6:]
    price_range_pct = safe_div(max(last_6) - min(last_6), mean(last_6), default=0.0) * 100
    return {
        'atr_ratio': atr_ratio,
        'bb_percentile': bb_pct,
        'bb_value': bb_value,
        'price_range_pct_30m': price_range_pct,
        'klines': kl,
    }


def compute_cvd_features_from_klines(kl):
    if not kl or len(kl.get('close', [])) < 8:
        return None
    deltas = []
    buy_ratios = []
    ofis = []
    for vol, tb in zip(kl['volume'], kl['taker_buy_volume']):
        sell = max(vol - tb, 0.0)
        delta = tb - sell
        total = tb + sell
        deltas.append(delta)
        buy_ratios.append(safe_div(tb, total, default=0.5))
        ofis.append(safe_div(tb - sell, total, default=0.0))
    cvd = np.cumsum(deltas).tolist()
    return {
        'delta_last': deltas[-1],
        'cvd_last': cvd[-1],
        'cvd_slope': linear_trend_slope(cvd[-6:]),
        'cvd_acceleration': (cvd[-1] - cvd[-4]) - (cvd[-4] - cvd[-7]) if len(cvd) >= 7 else 0.0,
        'buy_ratio_recent': mean(buy_ratios[-3:]),
        'buy_ratio_prev': mean(buy_ratios[-6:-3]) if len(buy_ratios) >= 6 else mean(buy_ratios[-3:]),
        'ofi_recent': mean(ofis[-3:]),
        'ofi_peak': max(ofis[-6:]),
        'delta_series': deltas,
        'cvd_series': cvd,
    }


def compute_flow_features(symbol):
    kl = get_klines(symbol, interval='5m', limit=24)
    if not kl or len(kl['close']) < 12:
        return None
    cvd = compute_cvd_features_from_klines(kl)
    if not cvd:
        return None
    price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
    price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
    price_change_30m = pct_change(kl['close'][-7], kl['close'][-1])
    intrabar_score, intrabar_reasons, intrabar_meta = analyze_intrabar_orderflow(symbol)
    cvd.update({
        'price_change_5m': price_change_5m,
        'price_change_15m': price_change_15m,
        'price_change_30m': price_change_30m,
        'intrabar_score': intrabar_score,
        'intrabar_reasons': intrabar_reasons,
        'intrabar_meta': intrabar_meta,
        'klines': kl,
    })
    return cvd


def compute_liquidity_features(symbol, reference_price=None):
    snapshot = get_orderbook_snapshot(symbol, limit=ORDERBOOK_DEPTH_LIMIT)
    if not snapshot or not snapshot.get('bids') or not snapshot.get('asks'):
        return {'spread_bps': 999.0, 'depth_ratio': 1.0, 'book_imbalance': 0.0, 'vacuum': 0, 'mid': reference_price}
    best_bid = snapshot['bids'][0][0]
    best_ask = snapshot['asks'][0][0]
    mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else reference_price
    spread_bps = safe_div(best_ask - best_bid, mid, default=0.0) * 10000
    band = ORDERBOOK_DEPTH_BPS / 10000.0
    lower = mid * (1 - band)
    upper = mid * (1 + band)
    bid_depth = sum(p * q for p, q in snapshot['bids'] if p >= lower)
    ask_depth = sum(p * q for p, q in snapshot['asks'] if p <= upper)
    total_depth = bid_depth + ask_depth
    all_depth = sum(p * q for p, q in snapshot['bids'][:20]) + sum(p * q for p, q in snapshot['asks'][:20])
    depth_ratio = safe_div(total_depth, all_depth, default=1.0)
    imbalance = safe_div(bid_depth - ask_depth, total_depth, default=0.0)
    vacuum = 1 if (depth_ratio < DEPTH_RATIO_THIN and spread_bps <= SPREAD_BPS_MAX) else 0
    return {
        'spread_bps': spread_bps,
        'depth_ratio': depth_ratio,
        'book_imbalance': imbalance,
        'vacuum': vacuum,
        'mid': mid,
        'bid_depth': bid_depth,
        'ask_depth': ask_depth,
    }


def compute_spot_perp_features(symbol):
    if not ENABLE_SPOT_CONFIRMATION:
        return {'available': False, 'spot_lead': 0.0, 'spot_volume_ratio': 1.0, 'spot_price_change_15m': 0.0, 'perp_price_change_15m': 0.0}
    spot = get_spot_klines(symbol, interval='5m', limit=12)
    perp = get_klines(symbol, interval='5m', limit=12)
    if not spot or not perp or len(spot['close']) < 4 or len(perp['close']) < 4:
        return {'available': False, 'spot_lead': 0.0, 'spot_volume_ratio': 1.0, 'spot_price_change_15m': 0.0, 'perp_price_change_15m': 0.0}
    spot_change_15m = pct_change(spot['close'][-4], spot['close'][-1])
    perp_change_15m = pct_change(perp['close'][-4], perp['close'][-1])
    spot_volume_ratio = safe_div(mean(spot['volume'][-3:]), mean(spot['volume'][-9:-3]) or 1.0, default=1.0)
    spot_lead = spot_change_15m - perp_change_15m
    return {
        'available': True,
        'spot_lead': spot_lead,
        'spot_volume_ratio': spot_volume_ratio,
        'spot_price_change_15m': spot_change_15m,
        'perp_price_change_15m': perp_change_15m,
    }


def compute_top_trader_context(symbol):
    ratio_shift = analyze_ratio_shift(symbol, period='5m', limit=12)
    if not ratio_shift:
        return {
            'available': False,
            'acc_delta': 0.0,
            'pos_delta': 0.0,
            'divergence_now': 0.0,
            'account_ratio_score': 0.0,
            'position_ratio_score': 0.0,
            'smart_money_divergence_score': 0.0,
        }

    acc_now = ratio_shift.get('acc_now', 1.0)
    pos_now = ratio_shift.get('pos_now', 1.0)
    divergence_now = ratio_shift.get('divergence_now', 0.0)

    if acc_now >= 3.0:
        account_ratio_score = 1.0
    elif acc_now >= 2.2:
        account_ratio_score = 0.8
    elif acc_now >= 1.6:
        account_ratio_score = 0.55
    elif acc_now >= 1.25:
        account_ratio_score = 0.30
    else:
        account_ratio_score = 0.0

    if pos_now >= 2.2:
        position_ratio_score = 0.45
    elif pos_now >= 1.6:
        position_ratio_score = 0.30
    elif pos_now >= 1.2:
        position_ratio_score = 0.15
    else:
        position_ratio_score = 0.0

    if divergence_now >= 0.18:
        smart_money_divergence_score = 1.0
    elif divergence_now >= 0.12:
        smart_money_divergence_score = 0.8
    elif divergence_now >= 0.08:
        smart_money_divergence_score = 0.6
    elif divergence_now >= 0.05:
        smart_money_divergence_score = 0.35
    else:
        smart_money_divergence_score = 0.0

    return {
        'available': True,
        **ratio_shift,
        'account_ratio_score': account_ratio_score,
        'position_ratio_score': position_ratio_score,
        'smart_money_divergence_score': smart_money_divergence_score,
    }


def _get_calibration_weights():
    profile = str(CALIBRATION_PROFILE).strip().lower()
    return CALIBRATION_WEIGHTS.get(profile, CALIBRATION_WEIGHTS["balanced"])


def compute_signal_quality(features, components, gates):
    flow = features['flow']
    oi = features['oi']
    compression = features['compression']
    liquidity = features['liquidity']
    funding = features['funding']
    spot = features['spot']

    data_quality = 1.0
    if liquidity.get('spread_bps', 999) >= SPREAD_BPS_MAX:
        data_quality -= 0.18
    if liquidity.get('spread_bps', 999) >= SPREAD_BPS_HARD_MAX:
        data_quality -= 0.12
    if flow.get('intrabar_meta', {}).get('price_change_24h') is None:
        data_quality -= 0.05
    if not spot.get('available'):
        data_quality -= 0.03

    non_chase = 1.0 - max(
        scale_to_unit(abs(flow.get('price_change_5m', 0.0)), FLOW_PRICE_CHASE_MAX, FLOW_PRICE_CHASE_HARD_MAX),
        scale_to_unit(abs(flow.get('price_change_15m', 0.0)), FLOW_PRICE_CHASE_MAX * 1.8, FLOW_PRICE_CHASE_HARD_MAX * 2.0),
    )

    compression_quality = max(
        1.0 - scale_to_unit(compression.get('price_range_pct_30m', 99.0), PRICE_COMPRESSION_RANGE_MAX, 3.5),
        1.0 - scale_to_unit(compression.get('bb_percentile', 100.0), COMPRESSION_BB_PERCENTILE_MAX, 70.0),
    )

    oi_quality = max(
        scale_to_unit(oi.get('percentile_5m', 50.0), OI_PERCENTILE_MIN, 95.0),
        scale_to_unit(oi.get('current_delta_5m', 0.0), OI_DELTA_MIN_PCT, OI_DELTA_MIN_PCT + 4.0),
    )

    funding_quality = 0.55
    if funding.get('current') is not None:
        funding_quality = max(
            scale_to_unit(-funding.get('z', 0.0), 0.8, FUNDING_Z_EXTREME + 1.0),
            scale_to_unit(funding.get('flip', 0), 0.5, 1.0),
            scale_to_unit(funding.get('persistence', 0), 2.0, 6.0) * 0.7,
        )

    liquidity_quality = max(
        1.0 - scale_to_unit(liquidity.get('depth_ratio', 1.0), DEPTH_RATIO_THIN, 1.3),
        scale_to_unit(abs(liquidity.get('book_imbalance', 0.0)), BOOK_IMBALANCE_MIN, 0.30),
    )

    gate_bonus = sum(1 for v in gates.values() if isinstance(v, bool) and v) / max(1, sum(1 for v in gates.values() if isinstance(v, bool)))

    top_accounts_quality = components.get('top_accounts', 0.0) if isinstance(components, dict) else 0.0
    top_divergence_quality = components.get('top_divergence', 0.0) if isinstance(components, dict) else 0.0

    quality = (
        0.22 * non_chase +
        0.16 * compression_quality +
        0.17 * oi_quality +
        0.13 * liquidity_quality +
        0.10 * funding_quality +
        0.10 * gate_bonus +
        0.07 * top_accounts_quality +
        0.05 * top_divergence_quality
    ) * max(0.0, min(1.0, data_quality))
    return max(0.0, min(1.0, quality))


def classify_confidence(pps, quality, classification, gates):
    base = 0.55 * pps + 0.45 * quality
    if classification == 'MANIPULATIVE_PUMP_DUMP':
        base -= 0.08
    if classification == 'SPOT_DRIVEN_ORGANIC':
        base += 0.03
    if gates.get('non_chase_gate'):
        base += 0.03
    if not gates.get('liquidity_gate', False):
        base -= 0.04
    if base >= 0.82:
        return 'HIGH', min(1.0, base)
    if base >= 0.68:
        return 'MEDIUM', min(1.0, base)
    return 'LOW', max(0.0, base)


def build_market_context_features(symbol):
    funding = compute_funding_regime(symbol)
    oi = compute_oi_regime(symbol)
    compression = compute_volatility_compression(symbol)
    flow = compute_flow_features(symbol)
    if not oi or not compression or not flow:
        return None
    price = flow['klines']['close'][-1]
    liquidity = compute_liquidity_features(symbol, reference_price=price)
    spot = compute_spot_perp_features(symbol)
    top = compute_top_trader_context(symbol)
    basis = get_basis(symbol)
    funding_current = funding.get('current') if funding else None
    return {
        'symbol': symbol,
        'price': price,
        'funding': funding,
        'oi': oi,
        'compression': compression,
        'flow': flow,
        'liquidity': liquidity,
        'spot': spot,
        'top': top,
        'basis': basis,
    }


def compute_pump_preparation_score(features):
    funding = features['funding']
    oi = features['oi']
    compression = features['compression']
    flow = features['flow']
    liquidity = features['liquidity']
    spot = features['spot']
    top = features['top']
    basis = features['basis']

    f_oi = max(
        scale_to_unit(abs(oi['z_5m']), OI_DELTA_Z_EXTREME, OI_DELTA_Z_EXTREME + 2.0),
        scale_to_unit(oi['current_delta_5m'], OI_DELTA_MIN_PCT, OI_DELTA_MIN_PCT + 3.0),
        scale_to_unit(oi['delta_15m'], OI_DELTA_MIN_PCT * 1.2, OI_DELTA_MIN_PCT * 1.2 + 4.0),
        scale_to_unit(oi.get('percentile_5m', 50.0), OI_PERCENTILE_MIN, 95.0),
    )
    f_compression = max(
        1.0 - scale_to_unit(compression['atr_ratio'], COMPRESSION_ATR_RATIO_MAX, 1.1),
        1.0 - scale_to_unit(compression['bb_percentile'], COMPRESSION_BB_PERCENTILE_MAX, 75),
        1.0 - scale_to_unit(compression['price_range_pct_30m'], PRICE_COMPRESSION_RANGE_MAX, 3.5),
    )

    buy_sell_ratio = safe_div(flow.get('buy_volume_recent', 0.0), flow.get('sell_volume_recent', 0.0), default=0.0)
    if buy_sell_ratio >= 4.0:
        taker_ratio_score = 1.0
    elif buy_sell_ratio >= 2.5:
        taker_ratio_score = 0.8
    elif buy_sell_ratio >= 1.8:
        taker_ratio_score = 0.6
    elif buy_sell_ratio >= 1.3:
        taker_ratio_score = 0.35
    else:
        taker_ratio_score = 0.0

    intrabar_norm = scale_to_unit(flow.get('intrabar_score', 0.0), 45.0, 90.0)
    flow_accel_norm = scale_to_unit(flow.get('delta_acceleration', 0.0), 0.0, max(1.0, abs(flow.get('delta_acceleration', 0.0)) * 1.2 + 1e-9))
    f_flow = max(
        taker_ratio_score,
        scale_to_unit(flow['buy_ratio_recent'], 0.54, 0.66),
        scale_to_unit(flow['ofi_recent'], OFI_STRONG_THRESHOLD * 0.5, 0.35),
        scale_to_unit(flow['ofi_peak'], OFI_STRONG_THRESHOLD, 0.45),
        intrabar_norm,
        flow_accel_norm,
    )
    f_cvd = max(
        scale_to_unit(flow['cvd_slope'], CVD_SLOPE_THRESHOLD, max(CVD_SLOPE_THRESHOLD + 1.0, abs(flow['cvd_slope']) * 1.2 + 1e-9)),
        scale_to_unit(flow['cvd_acceleration'], 0.0, max(1.0, abs(flow['cvd_acceleration']) * 1.2 + 1e-9)),
    )
    f_liquidity = max(
        1.0 - scale_to_unit(liquidity['depth_ratio'], DEPTH_RATIO_THIN, 1.2),
        scale_to_unit(abs(liquidity['book_imbalance']), BOOK_IMBALANCE_MIN, 0.35),
        scale_to_unit(1 if liquidity['vacuum'] else 0, 0.5, 1.0),
    )

    funding_current = funding.get('current')
    funding_trend = (funding.get('current', 0.0) or 0.0) - (funding.get('mean', 0.0) or 0.0)
    funding_regime_score = 0.0
    if funding_current is not None:
        if -0.03 <= funding_current <= 0.01:
            funding_regime_score += 0.45
        if funding_trend > 0:
            funding_regime_score += 0.35
        if funding.get('flip', 0) == 1:
            funding_regime_score += 0.20
        if funding_current > 0.03:
            funding_regime_score -= 0.20
    f_funding = max(0.0, min(1.0, funding_regime_score))

    f_spot = 0.0
    if spot.get('available'):
        f_spot = max(
            scale_to_unit(spot['spot_lead'], 0.0, 1.5),
            scale_to_unit(spot['spot_volume_ratio'], 1.0, 1.8),
        )

    account_score = top.get('account_ratio_score', 0.0) if top.get('available') else 0.0
    divergence_score = top.get('smart_money_divergence_score', 0.0) if top.get('available') else 0.0
    position_score = top.get('position_ratio_score', 0.0) if top.get('available') else 0.0

    basis_context = 0.0
    if -0.20 <= basis <= 0.20:
        basis_context = 0.65
    elif -0.45 <= basis <= 0.45:
        basis_context = 0.35
    elif basis > 0.80:
        basis_context = 0.10
    else:
        basis_context = 0.0

    raw_pps = (
        0.20 * f_oi +
        0.16 * f_compression +
        0.18 * f_flow +
        0.10 * f_cvd +
        0.12 * f_liquidity +
        0.12 * account_score +
        0.07 * divergence_score +
        0.03 * f_funding +
        0.02 * basis_context +
        0.00 * position_score
    )

    gates = {
        'compression_gate': (
            compression['atr_ratio'] <= COMPRESSION_ATR_RATIO_MAX or
            compression['bb_percentile'] <= COMPRESSION_BB_PERCENTILE_MAX or
            compression['price_range_pct_30m'] <= PRICE_COMPRESSION_RANGE_MAX
        ),
        'oi_gate': (
            oi['current_delta_5m'] >= OI_DELTA_MIN_PCT or
            oi['z_5m'] >= OI_DELTA_Z_EXTREME or
            oi.get('percentile_5m', 50.0) >= OI_PERCENTILE_MIN
        ),
        'flow_gate': (
            buy_sell_ratio >= 1.8 or
            flow['buy_ratio_recent'] >= BUY_RATIO_STRONG_THRESHOLD or
            flow['ofi_recent'] >= OFI_STRONG_THRESHOLD or
            flow['cvd_slope'] > CVD_SLOPE_THRESHOLD or
            flow.get('intrabar_score', 0.0) >= 55.0
        ),
        'liquidity_gate': (
            liquidity['vacuum'] == 1 or
            liquidity['depth_ratio'] <= DEPTH_RATIO_THIN or
            abs(liquidity['book_imbalance']) >= BOOK_IMBALANCE_MIN
        ) and liquidity['spread_bps'] <= SPREAD_BPS_MAX,
        'smart_money_gate': (
            account_score >= 0.55 or divergence_score >= 0.35
        ),
        'non_chase_gate': abs(flow['price_change_5m']) <= FLOW_PRICE_CHASE_MAX,
        'sanity_spread_gate': liquidity['spread_bps'] <= SPREAD_BPS_HARD_MAX,
    }
    gates['all_passed'] = (
        gates['compression_gate'] and gates['oi_gate'] and gates['flow_gate'] and
        gates['liquidity_gate'] and gates['smart_money_gate'] and gates['non_chase_gate'] and gates['sanity_spread_gate']
    )

    quality = compute_signal_quality(features, {
        'oi': f_oi, 'compression': f_compression, 'flow': f_flow, 'cvd': f_cvd,
        'liquidity': f_liquidity, 'funding': f_funding, 'spot': f_spot,
        'top_accounts': account_score, 'top_divergence': divergence_score, 'position': position_score,
        'basis': basis_context,
    }, gates)

    pps = raw_pps * (0.85 + 0.15 * quality)
    components = {
        'oi': round(f_oi, 4),
        'compression': round(f_compression, 4),
        'flow': round(f_flow, 4),
        'cvd': round(f_cvd, 4),
        'liquidity': round(f_liquidity, 4),
        'funding': round(f_funding, 4),
        'spot': round(f_spot, 4),
        'top_accounts': round(account_score, 4),
        'top_divergence': round(divergence_score, 4),
        'position_confirm': round(position_score, 4),
        'basis': round(basis_context, 4),
        'quality': round(quality, 4),
        'taker_buy_sell_ratio': round(buy_sell_ratio, 4),
    }
    return max(0.0, min(1.0, pps)), components, gates


def classify_pump_type(features, components=None):
    funding = features['funding']
    oi = features['oi']
    flow = features['flow']
    spot = features['spot']
    liquidity = features['liquidity']
    top = features['top']
    basis = features['basis']
    quality = (components or {}).get('quality', 0.5)
    buy_sell_ratio = (components or {}).get('taker_buy_sell_ratio', safe_div(flow.get('buy_volume_recent', 0.0), flow.get('sell_volume_recent', 0.0), default=0.0))

    if (
        top.get('available') and
        top.get('divergence_now', 0.0) >= 0.08 and
        top.get('account_ratio_score', 0.0) >= 0.55 and
        oi.get('percentile_5m', 50.0) >= 60 and
        (funding.get('current') is None or funding.get('current', 0.0) <= 0.01) and
        buy_sell_ratio >= 1.8
    ):
        return 'SHORT_SQUEEZE_BUILDUP'

    if (
        top.get('available') and
        top.get('account_ratio_score', 0.0) >= 0.55 and
        top.get('position_ratio_score', 0.0) >= 0.15 and
        oi.get('percentile_5m', 50.0) >= 60 and
        abs(basis) <= 0.45 and
        flow.get('cvd_slope', 0.0) > 0 and
        abs(flow.get('price_change_15m', 0.0)) < 3.0
    ):
        return 'INSTITUTIONAL_ACCUMULATION'

    if (
        liquidity['depth_ratio'] < DEPTH_RATIO_VERY_THIN and
        abs(liquidity.get('book_imbalance', 0.0)) > 0.10 and
        buy_sell_ratio >= 2.0 and
        flow.get('intrabar_score', 0.0) >= 60.0
    ):
        return 'LIQUIDITY_VACUUM_BREAKOUT'

    if spot.get('available') and spot['spot_lead'] > 0.35 and spot['spot_volume_ratio'] > 1.20 and oi['current_delta_5m'] < 2.2 and basis < 0.75:
        return 'SPOT_DRIVEN_ORGANIC'
    if (
        oi['current_delta_5m'] > 1.8 and
        (funding.get('current') is not None and (funding['current'] < 0 or funding['z'] < -0.8 or funding.get('flip', 0) == 1)) and
        flow['ofi_recent'] > 0.07 and
        quality >= 0.55
    ):
        return 'LEVERAGE_DRIVEN_SYNTHETIC'
    if (
        liquidity['depth_ratio'] < DEPTH_RATIO_VERY_THIN and
        abs(liquidity.get('book_imbalance', 0.0)) > 0.10 and
        flow['price_change_5m'] > 1.5 and
        oi['current_delta_5m'] < 1.2
    ):
        return 'MANIPULATIVE_PUMP_DUMP'
    return 'INSTITUTIONAL_ACCUMULATION'


def build_institutional_signal(symbol):
    features = build_market_context_features(symbol)
    if not features:
        return None
    pps, components, gates = compute_pump_preparation_score(features)
    quality = components.get('quality', 0.0)
    if not gates['all_passed']:
        return log_rejection(symbol, 'INSTITUTIONAL_PRE_PUMP', 'institutional_gates_not_passed', pps=pps, quality=quality, gates=gates, components=components, decisive_features=[k for k, v in sorted(components.items(), key=lambda kv: kv[1], reverse=True)[:4]], why_rejected=['One or more primary gates failed'], failed_gates=[k for k,v in gates.items() if isinstance(v, bool) and not v])
    if quality < MIN_ACCEPTANCE_QUALITY:
        return log_rejection(symbol, 'INSTITUTIONAL_PRE_PUMP', 'quality_below_threshold', pps=pps, quality=quality, gates=gates, components=components, decisive_features=[k for k, v in sorted(components.items(), key=lambda kv: kv[1], reverse=True)[:4]], why_rejected=[f'quality {quality:.2f} below threshold {MIN_ACCEPTANCE_QUALITY:.2f}'])
    if pps < PPS_ALERT_THRESHOLD:
        return log_rejection(symbol, 'INSTITUTIONAL_PRE_PUMP', 'pps_below_threshold', pps=pps, quality=quality, gates=gates, components=components, decisive_features=[k for k, v in sorted(components.items(), key=lambda kv: kv[1], reverse=True)[:4]], why_rejected=[f'pps {pps:.2f} below threshold {PPS_ALERT_THRESHOLD:.2f}'])

    classification = classify_pump_type(features, components)
    funding = features['funding']
    oi = features['oi']
    compression = features['compression']
    flow = features['flow']
    liquidity = features['liquidity']
    spot = features['spot']
    top = features['top']

    confidence_label, confidence_value = classify_confidence(pps, quality, classification, gates)
    score = int(round((0.7 * pps + 0.3 * confidence_value) * 100))
    reasons = [
        f"PPS {pps:.2f} / quality {quality:.2f} / confidence {confidence_label}",
        f"OI 5m {oi['current_delta_5m']:+.2f}% / 15m {oi['delta_15m']:+.2f}% / pct {oi.get('percentile_5m', 50.0):.0f}",
        f"ضغط شراء {flow['buy_ratio_recent']:.1%} و OFI {flow['ofi_recent']:+.2f} و intrabar {flow.get('intrabar_score', 0.0):.0f}",
        f"انضغاط ATR {compression['atr_ratio']:.2f} / BB pct {compression['bb_percentile']:.0f} / range30m {compression['price_range_pct_30m']:.2f}%",
        f"سيولة depth {liquidity['depth_ratio']:.2f} / imbalance {liquidity['book_imbalance']:+.2f} / spread {liquidity['spread_bps']:.1f}bps",
    ]
    if funding.get('current') is not None:
        reasons.append(f"Funding {funding['current']:+.4f}% (z={funding['z']:+.2f}, flip={funding.get('flip', 0)})")
    if spot.get('available'):
        reasons.append(f"Spot lead {spot['spot_lead']:+.2f}% / vol {spot['spot_volume_ratio']:.2f}x")
    if top.get('available'):
        reasons.append(f"Divergence كبار المتداولين {top.get('divergence_now', 0.0):.3f}")
        reasons.append(f"Top accounts score {top.get('account_ratio_score', 0.0):.2f} / position confirm {top.get('position_ratio_score', 0.0):.2f}")

    result = {
        'symbol': symbol,
        'score': min(score, 100),
        'pattern': 'INSTITUTIONAL_PRE_PUMP',
        'classification': classification,
        'direction': 'UP',
        'price': features['price'],
        'funding': funding.get('current'),
        'oi_change': oi['delta_15m'],
        'pps': pps,
        'quality': quality,
        'confidence': confidence_label,
        'confidence_value': confidence_value,
        'components': components,
        'gates': gates,
        'reasons': reasons[:8],
        'decisive_features': [k for k, v in sorted(components.items(), key=lambda kv: kv[1], reverse=True)[:4]],
        'diagnostics': {
            'accepted': True,
            'why_accepted': reasons[:5],
            'gates': gates,
            'components': components,
        },
        'timestamp': datetime.now().isoformat(),
    }
    attach_signal_stage(result, flow['price_change_5m'], flow['price_change_15m'], oi['current_delta_5m'], oi['delta_15m'], funding.get('current'), flow['buy_ratio_recent'])
    log_acceptance(symbol, 'INSTITUTIONAL_PRE_PUMP', score=result['score'], price=result['price'], reasons=result['reasons'], classification=classification, pps=pps, quality=quality, confidence=confidence_label, confidence_value=confidence_value, components=components, gates=gates, stage=result.get('signal_stage'))
    return result

def detect_long_term_accumulation(symbol):
    try:
        daily_ratios = get_daily_top_ratio(symbol, limit=DAILY_OI_TREND_DAYS)
        if len(daily_ratios) < DAILY_OI_TREND_DAYS:
            return None
        avg_ratio = mean(daily_ratios)
        if avg_ratio < DAILY_MIN_AVG_TOP_RATIO:
            return None

        daily_oi = get_daily_oi_history(symbol, limit=DAILY_OI_TREND_DAYS)
        if len(daily_oi) < DAILY_OI_TREND_DAYS:
            return None
        oi_slope = linear_trend_slope(daily_oi)
        if oi_slope <= 0:
            return None

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'LONG_TERM_ACCUMULATION', 'funding_unavailable')
        funding_hist = get_funding_rate_history(symbol, limit=DAILY_FUNDING_FLIP_DAYS*3)
        funding_flip = False
        if len(funding_hist) >= 3:
            pos_before = any(f > 0 for f in funding_hist[-DAILY_FUNDING_FLIP_DAYS*3:-3])
            neg_now = funding_rate < 0
            if pos_before and neg_now:
                funding_flip = True

        buy_vol, sell_vol = get_daily_taker_volume(symbol)
        if buy_vol is None or sell_vol is None:
            return None
        if buy_vol <= sell_vol * DAILY_TAKER_VOLUME_RATIO:
            return None

        basis = get_basis(symbol)
        if abs(basis) > DAILY_BASIS_THRESHOLD:
            return None

        score = 70
        reasons = [f"تراكم طويل (نسبة كبار {avg_ratio:.1f}%)"]

        if funding_flip:
            score += 15
            reasons.append("تحول التمويل إلى سلبي")
        if funding_rate < 0:
            score += 10
            reasons.append(f"تمويل سلبي {funding_rate:+.3f}%")
        if oi_slope > 0:
            score += 10
            reasons.append("اتجاه OI إيجابي")

        price = get_mark_price(symbol)
        result = {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'LONG_TERM_ACCUMULATION',
            'direction': 'UP',
            'price': price,
            'reasons': reasons[:5],
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_long_term_accumulation لـ {symbol}: {e}")
        return None

# =============================================================================
# تقييم القوة مع دمج HMM وإدارة المخاطر
# =============================================================================
def assess_signal_power(symbol, base_score, pattern_data):
    power_score = 0
    power_reasons = []
    power_level = "عادية"
    reasons_set = set()  # لتتبع الأسباب المضافة

    try:
        _, tr_long, _ = get_top_long_short_ratio(symbol)
        if tr_long and tr_long > STRONG_WHALE_RATIO:
            power_score += 20
            reason = f"🐋 هيمنة كبار {tr_long:.1f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate and funding_rate < EXTREME_FUNDING_SHORT:
            power_score += 15
            reason = f"🔥 تمويل سلبي شديد {funding_rate:+.3f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)

        oi_hist = get_oi_history(symbol, period='15m', limit=2)
        if len(oi_hist) >= 2:
            oi_change = pct_change(oi_hist[-2], oi_hist[-1])
            if oi_change > OI_SURGE_THRESHOLD:
                power_score += 15
                reason = f"📈 OI قفز {oi_change:+.1f}%"
                if reason not in reasons_set:
                    power_reasons.append(reason)
                    reasons_set.add(reason)

        ticker = get_ticker_24hr_one_symbol(symbol)
        if ticker:
            current_volume = float(ticker['volume'])
            avg_volume = get_historical_avg_volume(symbol, days=7)
            if avg_volume and avg_volume > 0:
                vol_ratio = current_volume / avg_volume
                if vol_ratio > VOLUME_EXPLOSION:
                    power_score += 15
                    reason = f"💥 حجم {vol_ratio:.1f}x المتوسط"
                    if reason not in reasons_set:
                        power_reasons.append(reason)
                        reasons_set.add(reason)

        basis = get_basis(symbol)
        if basis < BASIS_EXTREME_NEGATIVE:
            power_score += 10
            reason = f"📉 أساس سالب متطرف {basis:+.2f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)
        elif basis > BASIS_EXTREME_POSITIVE:
            power_score += 10
            reason = f"📈 أساس موجب متطرف {basis:+.2f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)

        # دمج مؤشرات الفريم اليومي
        daily_ratios = get_daily_top_ratio(symbol, limit=DAILY_OI_TREND_DAYS)
        if len(daily_ratios) >= DAILY_OI_TREND_DAYS:
            avg_daily_ratio = mean(daily_ratios)
            if avg_daily_ratio > DAILY_MIN_AVG_TOP_RATIO:
                power_score += POWER_ADD_DAILY_BOOST
                reason = f"📅 تراكم يومي {avg_daily_ratio:.1f}%"
                if reason not in reasons_set:
                    power_reasons.append(reason)
                    reasons_set.add(reason)

        # تطبيق مضاعف حالة السوق
        regime_multiplier = hmm_detector.get_regime_multiplier() if ENABLE_HMM else 1.0
        total_score = (base_score + power_score) * regime_multiplier

        if total_score >= POWER_CRITICAL_THRESHOLD:
            power_level = "حرجة 🔥"
        elif total_score >= POWER_IMPORTANT_THRESHOLD:
            power_level = "مهمة ⚡"
        else:
            power_level = "عادية"

    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في تقييم القوة لـ {symbol}: {e}")

    return power_score, power_level, power_reasons

def calculate_risk_management(symbol, price, direction):
    """
    حساب وقف الخسارة وأهداف الربح بناءً على ATR.
    تعيد أرقامًا صالحة (0 إذا فشلت) لتجنب أخطاء None.
    """
    atr = calculate_atr(symbol, ATR_PERIOD)
    if atr is None or price is None:
        return 0.0, 0.0, 0.0
    if direction == 'UP':
        stop_loss = price - atr * ATR_MULTIPLIER_STOP
        take_profit1 = price + atr * ATR_MULTIPLIER_TAKE_PROFIT
        take_profit2 = price + atr * ATR_MULTIPLIER_TAKE_PROFIT * 2
    else:
        stop_loss = price + atr * ATR_MULTIPLIER_STOP
        take_profit1 = price - atr * ATR_MULTIPLIER_TAKE_PROFIT
        take_profit2 = price - atr * ATR_MULTIPLIER_TAKE_PROFIT * 2
    # التأكد من عدم وجود None
    return stop_loss or 0.0, take_profit1 or 0.0, take_profit2 or 0.0

# =============================================================================
# محلل HMM
# =============================================================================
class HMMMarketRegimeDetector:
    def __init__(self):
        self.model = None
        self.scaler_mean = None
        self.scaler_std = None
        self.current_regime = -1
        self.regime_names = {
            0: "🟢 هادئ (Calm)",
            1: "📈 صعودي (Bullish Trending)",
            2: "📉 هبوطي (Bearish Trending)",
            3: "⚡ متقلب (High Volatility)"
        }
        self.last_fit_time = 0

    def _prepare_features(self):
        klines = get_klines('BTCUSDT', '1h', HMM_LOOKBACK_DAYS * 24)
        if not klines or len(klines['close']) < 100:
            return None
        prices = klines['close']
        returns = [pct_change(prices[i], prices[i+1]) for i in range(len(prices)-1)]
        volatilities = []
        for i in range(24, len(returns)):
            vol = np.std(returns[i-24:i])
            volatilities.append(vol)
        min_len = min(len(returns[24:]), len(volatilities))
        if min_len < 50:
            return None
        X = np.column_stack((returns[24:24+min_len], volatilities[:min_len]))
        return X

    def fit_model(self):
        if not ENABLE_HMM or not HMM_AVAILABLE:
            return False
        X = self._prepare_features()
        if X is None or len(X) < 100:
            return False
        self.scaler_mean = np.mean(X, axis=0)
        self.scaler_std = np.std(X, axis=0)
        X_scaled = (X - self.scaler_mean) / (self.scaler_std + 1e-9)

        self.model = hmm.GaussianHMM(
            n_components=HMM_N_STATES,
            covariance_type="full",
            n_iter=1000,
            random_state=42
        )
        self.model.fit(X_scaled)
        states = self.model.predict(X_scaled)
        self.current_regime = states[-1]
        return True

    def get_current_regime(self):
        now = time.time()
        if now - self.last_fit_time >= HMM_UPDATE_INTERVAL:
            self.fit_model()
            self.last_fit_time = now
        return {
            'regime_id': self.current_regime,
            'regime_name': self.regime_names.get(self.current_regime, "غير معروف"),
            'confidence': 0.8,
        }

    def get_regime_multiplier(self):
        if not ENABLE_HMM or self.model is None:
            return 1.0
        regime = self.get_current_regime()
        if regime['regime_id'] == 0:
            return 1.0
        elif regime['regime_id'] == 1:
            return 1.2
        elif regime['regime_id'] == 2:
            return 0.6
        elif regime['regime_id'] == 3:
            return 0.8
        else:
            return 1.0

hmm_detector = HMMMarketRegimeDetector()

# =============================================================================
# نظام التعلم الذاتي (مع تحسين تقييم الاتجاه الهابط)
# =============================================================================
class LearningSystem:
    def __init__(self, db):
        self.db = db
        self.last_learning_time = datetime.now()
        self.pattern_weights = PATTERN_WEIGHTS.copy()
        self.pattern_performance = defaultdict(lambda: {'total': 0, 'success': 0, 'score_sum': 0})

    def _evaluate_change_success(self, direction, change, horizon_minutes):
        thresholds = {5: 1.0, 15: 2.0, 30: 3.0}
        threshold = thresholds.get(horizon_minutes, 2.0)
        if direction == 'UP':
            return 1 if change >= threshold else 0
        return 1 if change <= -threshold else 0

    def evaluate_signals(self):
        total_evaluated = 0
        for horizon in EVAL_HORIZONS_MINUTES:
            pending = self.db.get_pending_eval_signals(horizon, hours_ago=BACKTEST_LOOKBACK_HOURS)
            if not pending:
                continue
            for sig in pending:
                sig_id, symbol, pattern, price, direction, ts = sig
                current_price = get_mark_price(symbol)
                if current_price is None or price in (None, 0):
                    continue
                change = (current_price - price) / price * 100
                success = self._evaluate_change_success(direction, change, int(horizon))
                self.db.update_signal_evaluation(sig_id, int(horizon), current_price, success)
                if int(horizon) == 15:
                    # نبقي التعلم الأساسي معتمدًا على أفق 15 دقيقة لأنه أكثر توازنًا على الهاتف
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute("UPDATE signals SET evaluated = 1, outcome = ?, price_after = ? WHERE id = ?",
                                     (success, current_price, sig_id))
                    self.pattern_performance[pattern]['total'] += 1
                    self.pattern_performance[pattern]['success'] += success
                    self.pattern_performance[pattern]['score_sum'] += abs(change)
                total_evaluated += 1
        return total_evaluated

    def adjust_weights(self):
        if not ENABLE_LEARNING:
            return

        self.evaluate_signals()

        total_weight = 0
        new_weights = {}
        for pattern, stats in self.pattern_performance.items():
            if stats['total'] < MIN_SIGNALS_FOR_LEARNING:
                continue
            accuracy = stats['success'] / stats['total']
            if accuracy > PERFORMANCE_THRESHOLD:
                old_weight = self.pattern_weights.get(pattern, 0.2)
                new_weight = old_weight * (1 + WEIGHT_ADJUSTMENT_RATE * (accuracy - PERFORMANCE_THRESHOLD))
                new_weight = min(new_weight, 1.0)
            else:
                old_weight = self.pattern_weights.get(pattern, 0.2)
                new_weight = old_weight * (1 - WEIGHT_ADJUSTMENT_RATE * (PERFORMANCE_THRESHOLD - accuracy))
                new_weight = max(new_weight, 0.05)
            new_weights[pattern] = new_weight
            total_weight += new_weight

        if total_weight > 0:
            for pattern in new_weights:
                new_weights[pattern] /= total_weight
            self.pattern_weights.update(new_weights)
            self.save_weights()

    def save_weights(self):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
            config['PATTERN_WEIGHTS'] = self.pattern_weights
            with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ خطأ في حفظ الأوزان: {e}")

# =============================================================================
# الفلترة الأولية (سريعة) مع تحسين السيولة والارتفاعات المفاجئة
# =============================================================================
def quick_filter():
    print("🔍 جلب بيانات السوق الأولية...")
    tickers = get_ticker_24hr()
    if not tickers:
        print("❌ فشل جلب tickers")
        return []

    candidates = []
    # تعبير منتظم للتحقق من الرموز اللاتينية (أحرف وأرقام و _ فقط)
    valid_symbol_pattern = re.compile(r'^[A-Z0-9_]+$')

    for t in tickers:
        symbol = t['symbol']

        # استبعاد الرموز التي تحتوي على أحرف غير لاتينية
        if not valid_symbol_pattern.match(symbol):
            if DEBUG:
                print(f"⚠️ استبعاد رمز غير لاتيني: {symbol}")
            continue

        volume = float(t['quoteVolume'])
        price_change_24h = float(t['priceChangePercent'])
        avg_vol = get_historical_avg_volume(symbol, days=7)
        vol_ok = (avg_vol is not None and volume / avg_vol >= VOLUME_TO_AVG_RATIO_MIN) if avg_vol else True

        # التحقق من الحد الأدنى لحجم التداول بالدولار
        if volume < MIN_VOLUME_USD:
            if DEBUG:
                print(f"⚠️ استبعاد {symbol} بسبب حجم منخفض: {volume:.0f} < {MIN_VOLUME_USD}")
            continue

        # إذا كان الارتفاع كبيرًا جدًا (أكثر من 8% في 24 ساعة)، نتجاوز شرط الحجم
        if price_change_24h >= SKYROCKET_THRESHOLD:
            candidates.append(symbol)
            continue

        if volume >= MIN_VOLUME_24H and price_change_24h >= MIN_PRICE_CHANGE_24H and vol_ok:
            candidates.append(symbol)

    print(f"✅ {len(candidates)} عملة اجتازت الفلترة الأولية")
    return candidates

# =============================================================================
# الأنماط الخمسة (محدثة باستخدام z-score) + الأنماط الجديدة للارتفاع
# =============================================================================

def bear_squeeze_pattern(symbol):
    try:
        oi_hist = get_oi_history(symbol, '4h', 2)
        if len(oi_hist) < 2:
            return None
        oi_change_4h = pct_change(oi_hist[-2], oi_hist[-1])

        if abs(oi_change_4h) < MIN_ABS_OI_CHANGE:
            return None

        funding_hist = get_funding_rate_history(symbol, 2)
        if len(funding_hist) < 2:
            return None
        funding_4h = funding_hist[-1]

        closes_4h = get_klines(symbol, '4h', 2)
        if not closes_4h or len(closes_4h['close']) < 2:
            return None
        price_change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])

        _, tr_long, _ = get_top_long_short_ratio(symbol)
        tr = tr_long / (100 - tr_long) if tr_long and (100 - tr_long) != 0 else None

        oi_hist_long = get_oi_history(symbol, '4h', 6)
        oi_accel = 0
        if len(oi_hist_long) >= 6:
            mom1 = oi_hist_long[-1] - oi_hist_long[-3]
            mom2 = oi_hist_long[-3] - oi_hist_long[-6]
            oi_accel = mom1 - mom2

        score = 0
        reasons = []
        reasons_set = set()  # لتتبع الأسباب المضافة

        if funding_4h < EXTREME_FUNDING_NEGATIVE:
            score += 40
            reason = f"🔥 تمويل متطرف ({funding_4h:+.3f}%)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif funding_4h < -0.05 or check_with_zscore(symbol, 'funding', funding_4h, FUNDING_RATE_ZSCORE):
            score += 25
            reason = f"⚠️ تمويل سلبي قوي"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if oi_change_4h > 15 and oi_accel > 0:
            score += 35
            reason = f"OI قفز +{oi_change_4h:.1f}% (متسارع)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif oi_change_4h > 10 or check_with_zscore(symbol, 'oi', oi_change_4h, OI_CHANGE_ZSCORE):
            score += 25
            reason = f"OI +{oi_change_4h:+.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if tr and tr < 1.1:
            score += 30
            reason = f"🐋 كبار يبيعون بصمت (نسبة {tr:.2f})"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if price_change_4h > 2:
            score += 20
            reason = f"سعر بدأ الصعود +{price_change_4h:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif price_change_4h > 0.5:
            score += 10

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'BEAR_SQUEEZE',
                'direction': 'UP',
                'price': closes_4h['close'][-1],
                'oi_change': oi_change_4h,
                'funding': funding_4h,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في bear_squeeze لـ {symbol}: {e}")
    return None

def whale_momentum_pattern(symbol):
    try:
        oi_hist = get_oi_history(symbol, '4h', 2)
        if len(oi_hist) < 2:
            return None
        oi_change_4h = pct_change(oi_hist[-2], oi_hist[-1])

        if abs(oi_change_4h) < MIN_ABS_OI_CHANGE:
            return None

        funding_hist = get_funding_rate_history(symbol, 2)
        if len(funding_hist) < 2:
            return None
        funding_4h = funding_hist[-1]

        closes_4h = get_klines(symbol, '4h', 2)
        if not closes_4h or len(closes_4h['close']) < 2:
            return None
        price_change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])

        tr_ratio, tr_long, _ = get_top_long_short_ratio(symbol)
        tr = tr_ratio

        score = 0
        reasons = []
        reasons_set = set()

        if MODERATE_FUNDING_POSITIVE_MIN <= funding_4h <= MODERATE_FUNDING_POSITIVE_MAX:
            score += 25
            reason = f"💰 تمويل إيجابي معتدل ({funding_4h:+.3f}%)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if tr and tr > 2.2:
            score += 40
            reason = f"🐋 كبار يشترون بكثافة (نسبة {tr:.2f})"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif tr and tr > 1.9:
            score += 30
            reason = f"كبار يشترون بقوة"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif tr and tr > 1.6 or check_with_zscore(symbol, 'top_ratio', tr_long, TOP_TRADER_ZSCORE):
            score += 20
            reason = f"كبار يميلون للشراء"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if tr:
            gap_type, gap_value = whale_retail_gap(tr, 1.0)
            if gap_type == "bullish_gap":
                score += 25
                reason = f"📊 فجوة صعودية (كبار {gap_value:.2f}x الصغار)"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        # سبب OI
        if oi_change_4h > 15:
            score += 25
            reason = f"OI قوي +{oi_change_4h:.1f}%"
        elif oi_change_4h > 8 or check_with_zscore(symbol, 'oi', oi_change_4h, OI_CHANGE_ZSCORE):
            score += 15
            reason = f"OI +{oi_change_4h:+.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        # سبب السعر
        if price_change_4h > 5:
            score += 20
            reason = f"سعر +{price_change_4h:.1f}%"
        elif price_change_4h > 2:
            score += 10
            reason = f"سعر +{price_change_4h:.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'WHALE_MOMENTUM',
                'direction': 'UP',
                'price': closes_4h['close'][-1],
                'oi_change': oi_change_4h,
                'funding': funding_4h,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في whale_momentum لـ {symbol}: {e}")
    return None

def pre_close_squeeze_pattern(symbol):
    try:
        funding_rate, next_time = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'PRE_CLOSE_SQUEEZE', 'funding_unavailable')

        now = int(time.time() * 1000)
        time_left = (next_time - now) / (1000 * 3600) if next_time else 999

        funding_hist = get_funding_rate_history(symbol, 4)
        if len(funding_hist) < 4:
            return None

        oi_hist = get_oi_history(symbol, '1h', 2)
        if len(oi_hist) < 2:
            return None
        oi_change_1h = pct_change(oi_hist[-2], oi_hist[-1])

        if abs(oi_change_1h) < MIN_ABS_OI_CHANGE:
            return None

        klines = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=20)
        liquidity_sweep = False
        if klines:
            liquidity_sweep = detect_liquidity_sweep(
                klines['high'], klines['low'],
                klines['close'], klines['volume'],
                klines['close'][-1]
            )

        price = get_mark_price(symbol)

        # حساب APR
        apr = calculate_apr(funding_rate)
        if apr < MIN_APR_PRE_CLOSE and funding_rate < 0.015:
            return None

        score = 0
        reasons = []
        reasons_set = set()

        if 0 < time_left <= FUNDING_COUNTDOWN_HOURS:
            score += 30
            reason = f"⏰ أقل من ساعة ({time_left:.2f}h)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
            if time_left < 0.25:
                score += 10
                reason = f"⚡ لحظة حرجة!"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        if apr > 500:
            score += 40
            reason = f"💰 APR هائل {apr:.0f}%"
        elif apr > MIN_APR_PRE_CLOSE:
            score += 30
            reason = f"💰 APR {apr:.0f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if len(funding_hist) >= 3:
            funding_trend = funding_hist[-1] - funding_hist[-3]
            if funding_trend > 0.02:
                score += 25
                reason = f"📈 تمويل يتحسن بـ {funding_trend:+.3f}%"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        if oi_change_1h > 10:
            score += 20
            reason = f"OI قفز +{oi_change_1h:.1f}%"
        elif oi_change_1h > 5 or check_with_zscore(symbol, 'oi', oi_change_1h, OI_CHANGE_ZSCORE):
            score += 10
            reason = f"OI +{oi_change_1h:+.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if liquidity_sweep:
            score += 15
            reason = f"🌊 مسح سيولة"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'PRE_CLOSE_SQUEEZE',
                'direction': 'UP',
                'price': price,
                'oi_change': oi_change_1h,
                'funding': funding_rate,
                'apr': apr,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في pre_close_squeeze لـ {symbol}: {e}")
    return None

def retail_trap_pattern(symbol):
    try:
        funding_hist = get_funding_rate_history(symbol, 2)
        if len(funding_hist) < 2:
            return None
        funding_4h = funding_hist[-1]

        closes_4h = get_klines(symbol, '4h', 2)
        if not closes_4h or len(closes_4h['close']) < 2:
            return None
        price_change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])

        closes_1d = get_klines(symbol, '1d', 2)
        price_change_24h = 0
        if closes_1d and len(closes_1d['close']) >= 2:
            price_change_24h = pct_change(closes_1d['close'][-2], closes_1d['close'][-1])

        tr_ratio, tr_long, _ = get_top_long_short_ratio(symbol)
        tr = tr_ratio

        score = 0
        reasons = []
        reasons_set = set()

        if price_change_24h < -20:
            score += 50
            reason = f"💥 انهيار {price_change_24h:.1f}% في 24 ساعة"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif price_change_4h < -5:
            score += 30
            reason = f"سعر ينهار {price_change_4h:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif price_change_4h < -2:
            score += 15
            reason = f"سعر ينهار {price_change_4h:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        # سبب نسبة كبار
        if tr and tr > 2.5:
            score += 50
            reason = f"تجمهر خطير (نسبة {tr:.2f})"
        elif tr and tr > 2.0:
            score += 40
            reason = f"تجمهر شديد"
        elif tr and tr > 1.5:
            score += 30
            reason = f"نسبة شراء مرتفعة"
        elif tr and tr > 1.3 or check_with_zscore(symbol, 'top_ratio', tr_long, TOP_TRADER_ZSCORE):
            score += 15
            reason = f"شراء زائد"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if tr:
            gap_type, gap_value = whale_retail_gap(tr, 1.0)
            if gap_type == "bearish_gap":
                score += 35
                reason = f"🐋 كبار يبيعون بينما الصغار يشترون (فجوة {gap_value:.2f})"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        if funding_4h > 0.01 and price_change_4h < -2:
            score += 25
            reason = f"💰 تمويل إيجابي مع هبوط حاد"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        if basis_score > 0 and basis_reasons:
            for r in basis_reasons:
                if r not in reasons_set:
                    reasons.append(r)
                    reasons_set.add(r)
                    score += basis_score  # يتم إضافة الـ score مرة واحدة فقط

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'RETAIL_TRAP',
                'direction': 'DOWN',
                'price': closes_4h['close'][-1],
                'funding': funding_4h,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في retail_trap لـ {symbol}: {e}")
    return None

def detect_pre_pump(symbol):
    try:
        funding_hist = get_funding_rate_history(symbol, limit=4)
        if len(funding_hist) < 4:
            return None
        low_funding = all(r < PRE_PUMP_FUNDING_MAX for r in funding_hist)
        if not low_funding:
            return None

        oi_current = get_open_interest(symbol)
        oi_hist = get_oi_history(symbol, period=OI_HISTORY_TIMEFRAME, limit=2)
        if not oi_current or len(oi_hist) < 2:
            return None
        oi_change = pct_change(oi_hist[-2], oi_current)

        if abs(oi_change) < MIN_ABS_OI_CHANGE:
            return None

        _, tr_long, _ = get_top_long_short_ratio(symbol)
        if not tr_long or tr_long < PRE_PUMP_TOP_LONG_MIN:
            return None

        current_hour = datetime.utcnow().hour
        if current_hour not in PRE_PUMP_HOURS:
            return None

        price = get_mark_price(symbol)

        score = 85
        reasons = [
            f"تمويل منخفض (<{PRE_PUMP_FUNDING_MAX}%) لفترة",
            f"OI +{oi_change:.1f}%",
            f"كبار {tr_long:.1f}%",
            f"وقت انخفاض السيولة ({current_hour}:00)"
        ]
        reasons_set = set(reasons)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        return {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'PRE_PUMP',
            'direction': 'UP',
            'price': price,
            'oi_change_15': oi_change,
            'funding': funding_hist[-1],
            'top_long': tr_long,
            'reasons': reasons[:5],
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_pre_pump لـ {symbol}: {e}")
    return None

def detect_whale_setup(symbol):
    try:
        oi_current = get_open_interest(symbol)
        oi_hist = get_oi_history(symbol, period=OI_HISTORY_TIMEFRAME, limit=4)
        if not oi_current or len(oi_hist) < 4:
            return None

        oi_change_15 = pct_change(oi_hist[-2], oi_current) if len(oi_hist) >= 2 else 0
        oi_change_30 = pct_change(oi_hist[-3], oi_current) if len(oi_hist) >= 3 else 0

        # رفض إذا كان التغير أقل من الحد الأدنى المطلق
        if abs(oi_change_15) < MIN_ABS_OI_CHANGE:
            return None

        closes = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=NUMBER_OF_CANDLES)
        if not closes or len(closes['close']) < NUMBER_OF_CANDLES:
            return None
        price = closes['close'][-1]
        price_1h_ago = closes['close'][0]
        price_change_1h = pct_change(price_1h_ago, price)

        _, tr_long, _ = get_top_long_short_ratio(symbol)
        funding_rate, _ = get_funding_rate(symbol)
        funding_hist = get_funding_rate_history(symbol, 2)
        funding_prev = funding_hist[-2] if len(funding_hist) >= 2 else None

        score = 0
        reasons = []
        reasons_set = set()

        if check_with_zscore(symbol, 'top_ratio', tr_long, WHALE_TOP_LONG_ZSCORE):
            score += 40
            reason = f"🐋 كبار {tr_long:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif tr_long and tr_long > 60:
            score += 20
            reason = f"كبار {tr_long:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if check_with_zscore(symbol, 'oi', oi_change_15, WHALE_OI_CHANGE_ZSCORE):
            score += 35
            reason = f"📈 OI +{oi_change_15:+.1f}%"
        elif oi_change_15 > 7:
            score += 15
            reason = f"OI +{oi_change_15:+.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if funding_rate is not None and funding_rate < WHALE_FUNDING_MAX:
            score += 30
            reason = f"💰 تمويل {funding_rate:+.3f}%"
        elif funding_rate is not None and funding_rate < 0:
            score += 10
            reason = f"💰 تمويل {funding_rate:+.3f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if funding_prev and funding_rate and funding_prev > 0 and funding_rate < 0:
            score += 25
            reason = "🔄 تمويل تحول إلى سلبي"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if oi_change_15 > oi_change_30 * 1.5 and oi_change_30 > 0:
            score += 20
            reason = "⚡ تسارع OI"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if price_change_1h < 5:
            score += 10
            # لا نضيف سبباً هنا

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        # إضافة تحليل التدفق وعناقيد التصفية
        flow_score, flow_reasons, _ = analyze_intrabar_orderflow(symbol)
        score += flow_score
        for r in flow_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        liq_score, liq_reasons = check_liquidation_clusters(symbol, price)
        score += liq_score
        for r in liq_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'WHALE_SETUP',
                'direction': 'UP',
                'price': price,
                'change_1h': price_change_1h,
                'oi_change_15': oi_change_15,
                'funding': funding_rate,
                'top_long': tr_long,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_whale_setup لـ {symbol}: {e}")
    return None

# =============================================================================
# أنماط جديدة للكشف عن الارتفاعات
# =============================================================================

def detect_strong_uptrend(symbol):
    """
    نمط يكشف عن ارتفاع قوي خلال آخر ساعة أو 4 ساعات.
    """
    try:
        # فحص آخر ساعة
        closes_1h = get_klines(symbol, '1h', 2)
        if closes_1h and len(closes_1h['close']) >= 2:
            change_1h = pct_change(closes_1h['close'][-2], closes_1h['close'][-1])
            if change_1h >= SKYROCKET_THRESHOLD:
                price = closes_1h['close'][-1]
                score = 80
                reasons = [f"ارتفاع قوي {change_1h:+.1f}% خلال آخر ساعة"]
                return {
                    'symbol': symbol,
                    'score': min(score, 100),
                    'pattern': 'STRONG_UPTREND',
                    'direction': 'UP',
                    'price': price,
                    'reasons': reasons,
                    'timestamp': datetime.now().isoformat()
                }

        # فحص آخر 4 ساعات
        closes_4h = get_klines(symbol, '4h', 2)
        if closes_4h and len(closes_4h['close']) >= 2:
            change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])
            if change_4h >= SKYROCKET_THRESHOLD * 1.5:  # عتبة أعلى قليلاً لأربع ساعات
                price = closes_4h['close'][-1]
                score = 85
                reasons = [f"ارتفاع قوي {change_4h:+.1f}% خلال آخر 4 ساعات"]
                return {
                    'symbol': symbol,
                    'score': min(score, 100),
                    'pattern': 'STRONG_UPTREND',
                    'direction': 'UP',
                    'price': price,
                    'reasons': reasons,
                    'timestamp': datetime.now().isoformat()
                }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_strong_uptrend لـ {symbol}: {e}")
    return None

def detect_skyrocket_enhanced(symbol):
    """
    كشف الارتفاع المفاجئ عبر فريمات متعددة.
    """
    try:
        for tf in SKYROCKET_TIMEFRAMES:
            closes = get_klines(symbol, interval=tf, limit=2)
            if closes and len(closes['close']) >= 2:
                change = pct_change(closes['close'][-2], closes['close'][-1])
                if change >= SKYROCKET_THRESHOLD:
                    price = closes['close'][-1]
                    reasons = [f'ارتفاع مفاجئ +{change:.1f}% خلال {tf}']
                    return {
                        'symbol': symbol,
                        'score': 100,
                        'pattern': 'SKYROCKET',
                        'direction': 'UP',
                        'price': price,
                        'reasons': reasons,
                        'timestamp': datetime.now().isoformat()
                    }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_skyrocket_enhanced لـ {symbol}: {e}")
    return None

# =============================================================================
# نمط جديد: تباين الأموال الذكية (Smart Money Divergence) - الإصدار المحسن v2
# =============================================================================
def detect_smart_money_divergence(symbol):
    """
    يلتقط مرحلة التحضير قبل الانفجار:
    - OI يرتفع على 5m/15m
    - السعر يبدأ بالصعود لكن ليس انفجارًا مكتملًا بعد
    - top account ratio لا يؤكد الحركة أو يسوء
    - ضغط شراء قصير المدى قوي
    """
    try:
        ratio_shift = analyze_ratio_shift(symbol, period='5m', limit=12)
        if not ratio_shift:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'ratio_shift_unavailable')

        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'insufficient_klines')

        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'insufficient_buy_ratio_points')

        recent_buy_ratio = mean(buy_ratios[-3:])
        prev_buy_ratio = mean(buy_ratios[-6:-3])
        buy_pressure_delta = recent_buy_ratio - prev_buy_ratio

        oi_5m = get_oi_history(symbol, period='5m', limit=6)
        oi_15m = get_oi_history(symbol, period='15m', limit=4)
        if len(oi_5m) < 6 or len(oi_15m) < 4:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'insufficient_oi_history')

        oi_change_5m = pct_change(oi_5m[-3], oi_5m[-1])
        oi_change_15m = pct_change(oi_15m[-2], oi_15m[-1])
        if oi_change_5m < 1.5 and oi_change_15m < 2.5:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'oi_not_expanding', oi_change_5m=oi_change_5m, oi_change_15m=oi_change_15m)

        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        if price_change_15m < 0.8:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'price_move_too_small', price_change_15m=price_change_15m)
        if price_change_15m > 7:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'price_move_too_late', price_change_15m=price_change_15m)

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'funding_unavailable')

        crowded_long = funding_rate > 0.04
        acc_not_confirming = ratio_shift['acc_delta'] <= 0
        pos_holding = ratio_shift['pos_delta'] >= -0.03
        divergence_expanding = ratio_shift['divergence_now'] >= 0.08
        strong_buy_pressure = recent_buy_ratio >= 0.58 and buy_pressure_delta > 0.03
        oi_expanding = oi_change_5m >= 1.5 or oi_change_15m >= 3.0

        if not ((acc_not_confirming and pos_holding) or divergence_expanding):
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'no_positioning_divergence', acc_delta=ratio_shift['acc_delta'], pos_delta=ratio_shift['pos_delta'], divergence_now=ratio_shift['divergence_now'])
        if not (strong_buy_pressure and oi_expanding):
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'buy_pressure_or_oi_weak', recent_buy_ratio=recent_buy_ratio, buy_pressure_delta=buy_pressure_delta, oi_change_5m=oi_change_5m, oi_change_15m=oi_change_15m)

        score = 55
        reasons = []
        if acc_not_confirming:
            score += 12
            reasons.append(f"حسابات الكبار لا تؤكد الحركة ({ratio_shift['acc_delta']:+.3f})")
        if divergence_expanding:
            score += 10
            reasons.append(f"اتساع divergence بين position/account ({ratio_shift['divergence_now']:.3f})")
        if oi_change_5m >= 2:
            score += 10
            reasons.append(f"OI 5m +{oi_change_5m:.1f}%")
        if oi_change_15m >= 3:
            score += 8
            reasons.append(f"OI 15m +{oi_change_15m:.1f}%")
        if strong_buy_pressure:
            score += 12
            reasons.append(f"ضغط شراء قصير قوي ({recent_buy_ratio:.1%})")
        if 0.8 <= price_change_15m <= 4.5:
            score += 8
            reasons.append(f"بداية اندفاع سعري +{price_change_15m:.1f}%/15m")
        if 0 < price_change_5m <= 2.5:
            score += 4
            reasons.append(f"زخم 5m +{price_change_5m:.1f}%")
        if not crowded_long:
            score += 5
            reasons.append(f"التمويل غير مزدحم {funding_rate:+.3f}%")

        if score < 65:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'score_below_threshold', score=score)

        result = {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'SMART_MONEY_DIVERGENCE',
            'direction': 'UP',
            'price': kl['close'][-1],
            'oi_change': oi_change_15m,
            'funding': funding_rate,
            'reasons': reasons[:6],
            'timestamp': datetime.now().isoformat()
        }
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy_ratio)
        log_acceptance(symbol, 'SMART_MONEY_DIVERGENCE', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'))
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'SMART_MONEY_DIVERGENCE', str(e))
        if DEBUG:
            print(f"⚠️ خطأ في detect_smart_money_divergence لـ {symbol}: {e}")
        return None


def detect_pre_explosion_setup(symbol):
    """التقاط الإعداد المبكر قبل الانفجار السعري."""
    try:
        kl = get_klines(symbol, '5m', 12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'insufficient_klines')

        oi_hist = get_oi_history(symbol, period='5m', limit=6)
        if len(oi_hist) < 6:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'insufficient_oi_history')

        price_15m = pct_change(kl['close'][-4], kl['close'][-1])
        price_30m = pct_change(kl['close'][-7], kl['close'][-1])
        oi_15m = pct_change(oi_hist[-3], oi_hist[-1])

        buy_ratios = []
        for i in range(len(kl['volume'])):
            vol = kl['volume'][i]
            tb = kl['taker_buy_volume'][i]
            buy_ratios.append(tb / vol if vol > 0 else 0)

        recent_buy = mean(buy_ratios[-3:])
        prev_buy = mean(buy_ratios[-6:-3])

        if price_15m < 0.8 or price_15m > 5.5:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'price_window_rejected', price_15m=price_15m)
        if oi_15m < 2.0:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'oi_too_low', oi_15m=oi_15m)
        if recent_buy < 0.56 or recent_buy <= prev_buy:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'buy_pressure_too_weak', recent_buy=recent_buy, prev_buy=prev_buy)

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'funding_unavailable')

        score = 58
        reasons = [
            f"بداية تحرك سعري +{price_15m:.1f}%/15m",
            f"OI +{oi_15m:.1f}%/15m",
            f"ضغط شراء 5m {recent_buy:.1%}",
        ]

        if funding_rate <= 0.03:
            score += 8
            reasons.append(f"التمويل غير مزدحم {funding_rate:+.3f}%")

        ratio_shift = analyze_ratio_shift(symbol, '5m', 12)
        if ratio_shift and ratio_shift['acc_delta'] <= 0:
            score += 10
            reasons.append("حسابات الكبار لا تؤكد الصعود")

        if price_30m <= 6:
            score += 6
            reasons.append(f"الحركة ما زالت مبكرة +{price_30m:.1f}%/30m")

        if score < 65:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'score_below_threshold', score=score)

        result = {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'PRE_EXPLOSION_SETUP',
            'direction': 'UP',
            'price': kl['close'][-1],
            'reasons': reasons[:6],
            'timestamp': datetime.now().isoformat()
        }
        attach_signal_stage(result, price_15m=price_15m, oi_15m=oi_15m, funding_rate=funding_rate, buy_ratio=recent_buy)
        log_acceptance(symbol, 'PRE_EXPLOSION_SETUP', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'))
        return result

    except Exception as e:
        diagnostics.log_error(symbol, 'PRE_EXPLOSION_SETUP', str(e))
        if DEBUG:
            print(f"⚠️ خطأ في detect_pre_explosion_setup لـ {symbol}: {e}")
        return None



def detect_pre_squeeze_up(symbol):
    try:
        ratio_shift = analyze_ratio_shift(symbol, period='5m', limit=12)
        if not ratio_shift:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'ratio_shift_unavailable')
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'insufficient_klines')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'insufficient_buy_ratio_points')
        recent_buy = rolling_mean(buy_ratios[-3:])
        prev_buy = rolling_mean(buy_ratios[-6:-3])
        buy_accel = recent_buy - prev_buy
        oi_5m_hist = get_oi_history(symbol, period='5m', limit=6)
        oi_15m_hist = get_oi_history(symbol, period='15m', limit=4)
        if len(oi_5m_hist) < 6 or len(oi_15m_hist) < 4:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'insufficient_oi_history')
        oi_change_5m = pct_change(oi_5m_hist[-3], oi_5m_hist[-1])
        oi_change_15m = pct_change(oi_15m_hist[-2], oi_15m_hist[-1])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        position_snapshot = get_top_position_snapshot(symbol, period='5m')
        account_snapshot = get_top_account_snapshot(symbol, period='5m')
        if not position_snapshot or not account_snapshot:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'snapshot_unavailable')
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'funding_unavailable')
        if price_change_15m < 1.0:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'price_move_too_small', price_change_15m=price_change_15m)
        if oi_change_15m < 2.5 and oi_change_5m < 1.5:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'oi_not_expanding', oi_change_5m=oi_change_5m, oi_change_15m=oi_change_15m)
        if position_snapshot['ratio'] < 1.8:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'position_ratio_too_low', position_ratio=position_snapshot['ratio'])
        if not (account_snapshot['ratio'] <= 1.10 or ratio_shift['acc_delta'] <= 0):
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'account_ratio_confirming_too_much', account_ratio=account_snapshot['ratio'], acc_delta=ratio_shift['acc_delta'])
        if recent_buy < 0.58 or buy_accel < 0.04:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'buy_pressure_too_weak', recent_buy=recent_buy, buy_accel=buy_accel)
        if funding_rate > 0.02:
            return log_rejection(symbol, 'PRE_SQUEEZE_UP', 'funding_too_crowded', funding_rate=funding_rate)
        score = 60
        reasons = [f"ضغط شراء قوي {recent_buy:.1%}", f"تسارع شراء +{buy_accel:.1%}", f"OI 15m +{oi_change_15m:.1f}%", f"Position Ratio {position_snapshot['ratio']:.2f}", f"Account Ratio {account_snapshot['ratio']:.2f}", f"تمويل غير مزدحم {funding_rate:+.3f}%"]
        if ratio_shift['divergence_now'] >= 0.20:
            score += 10
            reasons.append(f"تباين مراكز/حسابات {ratio_shift['divergence_now']:.2f}")
        if price_change_5m >= 1.2:
            score += 8
            reasons.append(f"اندفاع 5m +{price_change_5m:.1f}%")
        if funding_rate < -0.05:
            score += 8
            reasons.append(f"وقود شورت {funding_rate:+.3f}%")
        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'PRE_SQUEEZE_UP', 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:6], 'timestamp': datetime.now().isoformat()}
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        log_acceptance(symbol, 'PRE_SQUEEZE_UP', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'))
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'PRE_SQUEEZE_UP', str(e))
        return None

def detect_flow_breakout(symbol):
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'insufficient_klines')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'insufficient_buy_ratio_points')
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        recent_high = max(kl['high'][-7:-1])
        local_high_break = kl['close'][-1] > recent_high
        open_, high_, low_, close_ = kl['open'][-1], kl['high'][-1], kl['low'][-1], kl['close'][-1]
        candle_range = max(high_ - low_, 1e-12)
        candle_body_ratio = abs(close_ - open_) / candle_range
        recent_buy = rolling_mean(buy_ratios[-3:])
        trade_recent = rolling_mean(kl['trades'][-3:])
        trade_prev = rolling_mean(kl['trades'][-6:-3])
        trade_ratio = (trade_recent / trade_prev) if trade_prev > 0 else 0
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'funding_unavailable')
        if price_change_5m < 1.2 and price_change_15m < 1.8:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'price_impulse_too_small', price_change_5m=price_change_5m, price_change_15m=price_change_15m)
        if not local_high_break:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'local_high_not_broken', recent_high=recent_high, close=kl['close'][-1])
        if candle_body_ratio < 0.65:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'candle_body_ratio_too_low', candle_body_ratio=candle_body_ratio)
        if recent_buy < 0.60:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'buy_ratio_too_low', recent_buy=recent_buy)
        if trade_ratio < 1.8:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'trade_count_not_accelerating', trade_ratio=trade_ratio)
        if abs(funding_rate) > 0.02:
            return log_rejection(symbol, 'FLOW_BREAKOUT', 'funding_not_neutral', funding_rate=funding_rate)
        score = 62
        reasons = [f"اختراق قمة محلية {recent_high:.6f}", f"شمعة تنفيذ قوية ({candle_body_ratio:.2f})", f"ضغط شراء {recent_buy:.1%}", f"تسارع صفقات {trade_ratio:.2f}x", f"سعر 5m +{price_change_5m:.1f}%", f"تمويل محايد {funding_rate:+.3f}%"]
        if price_change_15m >= 3.0:
            score += 8
            reasons.append(f"اندفاع 15m +{price_change_15m:.1f}%")
        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'FLOW_BREAKOUT', 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:6], 'timestamp': datetime.now().isoformat()}
        attach_signal_stage(result, price_change_5m, price_change_15m, 0.0, 0.0, funding_rate, recent_buy)
        log_acceptance(symbol, 'FLOW_BREAKOUT', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'))
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'FLOW_BREAKOUT', str(e))
        return None

def detect_oi_expansion_pump(symbol):
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'insufficient_klines')
        oi_hist_5m = get_oi_history(symbol, period='5m', limit=6)
        oi_hist_15m = get_oi_history(symbol, period='15m', limit=4)
        oi_notional_hist = get_oi_notional_history(symbol, period='5m', limit=6)
        if len(oi_hist_5m) < 6 or len(oi_hist_15m) < 4:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'insufficient_oi_history')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'insufficient_buy_ratio_points')
        recent_buy = rolling_mean(buy_ratios[-3:])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        oi_change_5m = pct_change(oi_hist_5m[-3], oi_hist_5m[-1])
        oi_change_15m = pct_change(oi_hist_15m[-2], oi_hist_15m[-1])
        oi_notional_change = pct_change(oi_notional_hist[-2], oi_notional_hist[-1]) if len(oi_notional_hist) >= 2 and oi_notional_hist[-2] > 0 else 0.0
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'funding_unavailable')
        if price_change_15m < 2.0:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'price_move_too_small', price_change_15m=price_change_15m)
        if oi_change_15m < 4.0 and oi_change_5m < 2.5:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'oi_change_too_small', oi_change_5m=oi_change_5m, oi_change_15m=oi_change_15m)
        if recent_buy < 0.56:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'buy_ratio_too_low', recent_buy=recent_buy)
        if abs(funding_rate) > 0.03:
            return log_rejection(symbol, 'OI_EXPANSION_PUMP', 'funding_too_extreme', funding_rate=funding_rate)
        score = 64
        reasons = [f"OI 15m +{oi_change_15m:.1f}%", f"OI 5m +{oi_change_5m:.1f}%", f"ضغط شراء {recent_buy:.1%}", f"سعر 15m +{price_change_15m:.1f}%", f"التمويل متوازن {funding_rate:+.3f}%"]
        if oi_notional_change > 3.0:
            score += 10
            reasons.append(f"القيمة الاسمية للفائدة +{oi_notional_change:.1f}%")
        if price_change_5m >= 1.5:
            score += 6
            reasons.append(f"اندفاع 5m +{price_change_5m:.1f}%")
        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'OI_EXPANSION_PUMP', 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:6], 'timestamp': datetime.now().isoformat()}
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        log_acceptance(symbol, 'OI_EXPANSION_PUMP', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'))
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'OI_EXPANSION_PUMP', str(e))
        return None

# =============================================================================
# قاموس الترجمة العربية (محدث)
# =============================================================================
PATTERN_ARABIC = {
    'LONG_TERM_ACCUMULATION': {
        'name': 'تراكم طويل الأجل',
        'default_direction': 'شراء',
        'emoji': '📅'
    },
    'PRE_PUMP': {
        'name': 'ما قبل الارتفاع',
        'default_direction': 'شراء',
        'emoji': '🕒'
    },
    'WHALE_SETUP': {
        'name': 'بصمة الحيتان',
        'default_direction': 'شراء',
        'emoji': '🐋'
    },
    'BEAR_SQUEEZE': {
        'name': 'عصر الدببة',
        'default_direction': 'شراء',
        'emoji': '🐻'
    },
    'WHALE_MOMENTUM': {
        'name': 'زخم الحيتان',
        'default_direction': 'شراء',
        'emoji': '🐋'
    },
    'PRE_CLOSE_SQUEEZE': {
        'name': 'ما قبل الإغلاق',
        'default_direction': 'شراء',
        'emoji': '⏰'
    },
    'RETAIL_TRAP': {
        'name': 'فخ التجزئة',
        'default_direction': 'بيع',
        'emoji': '🔻'
    },
    'SKYROCKET': {
        'name': 'ارتفاع مفاجئ',
        'default_direction': 'شراء',
        'emoji': '💥'
    },
    'STRONG_UPTREND': {
        'name': 'اتجاه صاعد قوي',
        'default_direction': 'شراء',
        'emoji': '📈'
    },
    'SMART_MONEY_DIVERGENCE': {
        'name': 'تباين الأموال الذكية',
        'default_direction': 'شراء',
        'emoji': '🧠'
    },
    'PRE_EXPLOSION_SETUP': {
        'name': 'إعداد ما قبل الانفجار',
        'default_direction': 'شراء',
        'emoji': '🚀'
    },
    'PRE_SQUEEZE_UP': {
        'name': 'ضغط صاعد مبكر',
        'default_direction': 'شراء',
        'emoji': '🧲'
    },
    'FLOW_BREAKOUT': {
        'name': 'اختراق تدفقي',
        'default_direction': 'شراء',
        'emoji': '⚡'
    },
    'OI_EXPANSION_PUMP': {
        'name': 'ضخ بتمدد الفائدة المفتوحة',
        'default_direction': 'شراء',
        'emoji': '🟡'
    },
    'INSTITUTIONAL_PRE_PUMP': {
        'name': 'إعداد مؤسسي قبل الارتفاع',
        'default_direction': 'شراء',
        'emoji': '🏛️'
    }
}

# =============================================================================
# قاعدة بيانات الإشارات
# =============================================================================
class SignalDatabase:
    REQUIRED_SIGNAL_COLUMNS = {
        "timestamp": "TEXT",
        "symbol": "TEXT",
        "pattern": "TEXT",
        "score": "REAL",
        "price": "REAL",
        "direction": "TEXT DEFAULT 'UP'",
        "reasons": "TEXT",
        "power_level": "TEXT",
        "stop_loss": "REAL",
        "take_profit1": "REAL",
        "take_profit2": "REAL",
        "signal_stage": "TEXT DEFAULT 'SETUP'",
        "evaluated": "INTEGER DEFAULT 0",
        "outcome": "INTEGER DEFAULT 0",
        "price_after": "REAL",
        "price_after_5m": "REAL",
        "price_after_15m": "REAL",
        "price_after_30m": "REAL",
        "outcome_5m": "INTEGER DEFAULT -1",
        "outcome_15m": "INTEGER DEFAULT -1",
        "outcome_30m": "INTEGER DEFAULT -1",
        "evaluated_5m": "INTEGER DEFAULT 0",
        "evaluated_15m": "INTEGER DEFAULT 0",
        "evaluated_30m": "INTEGER DEFAULT 0"
    }

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        ensure_parent_dir(self.db_path)
        self._init_db()

    def _configure_connection(self, conn):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.OperationalError:
            pass

    def _get_existing_columns(self, conn):
        rows = conn.execute("PRAGMA table_info(signals)").fetchall()
        return {row[1] for row in rows}

    def _ensure_signal_schema(self, conn):
        self._configure_connection(conn)
        existing_columns = self._get_existing_columns(conn)
        for col_name, col_def in self.REQUIRED_SIGNAL_COLUMNS.items():
            if col_name not in existing_columns:
                try:
                    conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
                except sqlite3.OperationalError as e:
                    # SQLite لا يدعم IF NOT EXISTS في ALTER TABLE ADD COLUMN،
                    # لذا نتجاهل الخطأ فقط إذا كان العمود موجودًا بالفعل.
                    if "duplicate column name" not in str(e).lower():
                        raise
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON signals(symbol, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_evaluated ON signals(evaluated)")

    def _init_db(self):
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    pattern TEXT,
                    score REAL,
                    price REAL,
                    direction TEXT,
                    reasons TEXT,
                    power_level TEXT,
                    stop_loss REAL,
                    take_profit1 REAL,
                    take_profit2 REAL,
                    signal_stage TEXT DEFAULT 'SETUP',
                    evaluated INTEGER DEFAULT 0,
                    outcome INTEGER DEFAULT 0,
                    price_after REAL
                )
            """)
            self._ensure_signal_schema(conn)

    def save_signal(self, signal_data):
        if not SAVE_SIGNALS:
            return
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._ensure_signal_schema(conn)
            conn.execute("""
                INSERT INTO signals (timestamp, symbol, pattern, score, price, direction, reasons, power_level, stop_loss, take_profit1, take_profit2, signal_stage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data.get('timestamp', datetime.now().isoformat()),
                signal_data.get('symbol', ''),
                signal_data.get('pattern', 'UNKNOWN'),
                float(signal_data.get('score', 0) or 0),
                float(signal_data.get('price', 0) or 0),
                signal_data.get('direction', 'UP'),
                json.dumps(signal_data.get('reasons', []), ensure_ascii=False),
                signal_data.get('power_level', 'عادية'),
                float(signal_data.get('stop_loss', 0) or 0),
                float(signal_data.get('take_profit1', 0) or 0),
                float(signal_data.get('take_profit2', 0) or 0),
                signal_data.get('signal_stage', 'SETUP')
            ))

    def update_signal_evaluation(self, sig_id, horizon_minutes, current_price, success):
        horizon_minutes = int(horizon_minutes)
        if horizon_minutes not in (5, 15, 30):
            return
        price_col = f"price_after_{horizon_minutes}m"
        outcome_col = f"outcome_{horizon_minutes}m"
        eval_col = f"evaluated_{horizon_minutes}m"
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            conn.execute(
                f"UPDATE signals SET {price_col} = ?, {outcome_col} = ?, {eval_col} = 1, price_after = COALESCE(price_after, ?) WHERE id = ?",
                (current_price, success, current_price, sig_id)
            )

    def get_pending_eval_signals(self, horizon_minutes, hours_ago=72):
        horizon_minutes = int(horizon_minutes)
        eval_col = f"evaluated_{horizon_minutes}m"
        cutoff = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        max_age = (datetime.now() - timedelta(minutes=horizon_minutes)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute(
                f"SELECT id, symbol, pattern, price, direction, timestamp FROM signals WHERE {eval_col} = 0 AND timestamp >= ? AND timestamp <= ?",
                (cutoff, max_age)
            )
            return cur.fetchall()

    def get_backtest_summary(self, lookback_hours=72):
        cutoff = (datetime.now() - timedelta(hours=lookback_hours)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("""
                SELECT pattern,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome_5m = 1 THEN 1 ELSE 0 END) AS win_5m,
                       SUM(CASE WHEN outcome_15m = 1 THEN 1 ELSE 0 END) AS win_15m,
                       SUM(CASE WHEN outcome_30m = 1 THEN 1 ELSE 0 END) AS win_30m
                FROM signals
                WHERE timestamp >= ?
                GROUP BY pattern
                ORDER BY total DESC
            """, (cutoff,))
            return cur.fetchall()

    def get_un_evaluated_signals(self, hours_ago=24):
        cutoff = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("SELECT id, symbol, pattern, price, direction, timestamp FROM signals WHERE evaluated = 0 AND timestamp < ?", (cutoff,))
            return cur.fetchall()

db = SignalDatabase()

# =============================================================================
# نظام تتبع تحديثات الإشارات
# =============================================================================
class SignalUpdateTracker:
    def __init__(self):
        self.last_signals = {}
        self.lock = threading.Lock()

    def should_alert(self, symbol, new_score, new_pattern):
        with self.lock:
            now = time.time()
            if symbol not in self.last_signals:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'pattern': new_pattern,
                    'timestamp': now
                }
                return True

            last = self.last_signals[symbol]
            if last['pattern'] != new_pattern:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'pattern': new_pattern,
                    'timestamp': now
                }
                return True

            score_increase = new_score - last['score']
            time_since_last = now - last['timestamp']
            cooldown_seconds = UPDATE_ALERT_COOLDOWN_MINUTES * 60

            if score_increase >= MIN_SCORE_INCREASE_FOR_ALERT and time_since_last >= cooldown_seconds:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'pattern': new_pattern,
                    'timestamp': now
                }
                return True
            else:
                return False

signal_tracker = SignalUpdateTracker()

# =============================================================================
# ذاكرة مؤقتة للرموز غير الصالحة (تجنب إعادة المحاولة في نفس الدورة)
# =============================================================================
invalid_symbols = set()
invalid_symbols_lock = threading.Lock()

def mark_symbol_invalid(symbol):
    with invalid_symbols_lock:
        invalid_symbols.add(symbol)

def is_symbol_invalid(symbol):
    with invalid_symbols_lock:
        return symbol in invalid_symbols

def clear_invalid_symbols():
    with invalid_symbols_lock:
        invalid_symbols.clear()

def evaluate_symbol_patterns(sym):
    """تجميع كل الأنماط الممكنة للرمز بدل التوقف عند أول نمط."""
    results = []
    pattern_functions = [
        build_institutional_signal,
        detect_pre_squeeze_up,
        detect_flow_breakout,
        detect_oi_expansion_pump,
        bear_squeeze_pattern,
        whale_momentum_pattern,
        pre_close_squeeze_pattern,
        retail_trap_pattern,
        detect_strong_uptrend,
        detect_skyrocket_enhanced,
        detect_smart_money_divergence,
        detect_pre_explosion_setup,
    ]

    for fn in pattern_functions:
        try:
            res = fn(sym)
            if res:
                power_score, power_level, power_reasons = assess_signal_power(
                    res['symbol'], res['score'], {}
                )
                res['power_level'] = power_level
                for reason in power_reasons:
                    if reason not in res['reasons']:
                        res['reasons'].append(reason)
                stop_loss, tp1, tp2 = calculate_risk_management(
                    res['symbol'], res['price'], res['direction']
                )
                res['stop_loss'] = stop_loss
                res['take_profit1'] = tp1
                res['take_profit2'] = tp2
                results.append(res)
        except Exception as e:
            diagnostics.log_error(sym, fn.__name__, str(e))
            if DEBUG:
                print(f"⚠️ خطأ في {fn.__name__} لـ {sym}: {e}")
    return results

# =============================================================================
# دالة إرسال إشارة فردية
# =============================================================================
def send_individual_signal(signal_data):
    """إرسال تنبيه فوري عند اكتشاف إشارة جديدة أو محدثة."""
    if signal_data['score'] < SIGNAL_MIN_SCORE:
        diagnostics.log_reject(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('pattern', 'UNKNOWN'), 'signal_below_send_threshold', {'score': signal_data.get('score')})
        return  # تجاهل الإشارات الضعيفة

    pattern_key = signal_data['pattern']
    pattern_info = PATTERN_ARABIC.get(pattern_key, {
        'name': pattern_key,
        'default_direction': 'غير معروف',
        'emoji': '🔹'
    })
    emoji = pattern_info['emoji']
    pattern_name_ar = pattern_info['name']
    direction = signal_data.get('direction', 'UP')
    if direction == 'UP':
        direction_ar = '🟢 شراء'
    elif direction == 'DOWN':
        direction_ar = '🔴 بيع'
    else:
        direction_ar = pattern_info['default_direction']

    reasons = "، ".join(signal_data.get('reasons', []))
    classification = signal_data.get('classification')
    stop_loss = signal_data.get('stop_loss') or 0
    tp1 = signal_data.get('take_profit1') or 0
    tp2 = signal_data.get('take_profit2') or 0

    # إضافة إشارة إذا كان هناك أنماط متعددة (سيتم إضافتها في deep_scan)
    multi_pattern_hint = signal_data.get('multi_pattern_hint', '')
    classification_line = f"   التصنيف: {classification}\n" if classification else ''

    message = (
        f"🔔 *إشارة جديدة: {signal_data['symbol']}* {multi_pattern_hint}\n"
        f"{emoji} النمط: {pattern_name_ar} [{signal_data.get('power_level', 'عادية')}]\n"
        f"   الدرجة: {signal_data['score']}\n"
        f"   الاتجاه: {direction_ar}\n"
        f"{classification_line}"
        f"   السعر: {signal_data['price']:.6f}\n"
        f"   وقف الخسارة: {stop_loss:.6f} | الهدف1: {tp1:.6f} | الهدف2: {tp2:.6f}\n"
        f"   أسباب: {reasons}"
    )
    send_telegram(message)

# =============================================================================
# المسح العميق (معدل لإرسال التنبيهات الفورية)
# =============================================================================
def deep_scan(candidates, learning_system):
    all_results = []  # جميع الإشارات
    symbol_signals = defaultdict(list)  # لتجميع الإشارات لكل رمز
    print(f"🔍 تحليل عميق لـ {len(candidates)} عملة...")

    clear_invalid_symbols()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_long = {executor.submit(detect_long_term_accumulation, sym): sym for sym in candidates if not is_symbol_invalid(sym)}
        long_results = []
        remaining = []
        for future in as_completed(future_long):
            sym = future_long[future]
            try:
                res = future.result(timeout=30)
                if res:
                    print(f"   📅 {sym}: {res['score']} LONG_TERM_ACCUMULATION")
                    # تقييم القوة وإدارة المخاطر
                    power_score, power_level, power_reasons = assess_signal_power(res['symbol'], res['score'], {})
                    res['power_level'] = power_level
                    res['reasons'].extend(power_reasons)
                    stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
                    res['stop_loss'] = stop_loss
                    res['take_profit1'] = tp1
                    res['take_profit2'] = tp2
                    long_results.append(res)
                    symbol_signals[sym].append(res)
                else:
                    remaining.append(sym)
                # لا تأخير بين الرموز
            except Exception as e:
                print(f"   ❌ {sym}: {str(e)[:50]}")
                if "400" in str(e) or "404" in str(e):
                    mark_symbol_invalid(sym)
                remaining.append(sym)

        if remaining:
            future_pre = {executor.submit(detect_pre_pump, sym): sym for sym in remaining if not is_symbol_invalid(sym)}
            pre_results = []
            still_remaining = []
            for future in as_completed(future_pre):
                sym = future_pre[future]
                try:
                    res = future.result(timeout=30)
                    if res:
                        print(f"   🕒 {sym}: {res['score']} PRE_PUMP")
                        power_score, power_level, power_reasons = assess_signal_power(res['symbol'], res['score'], {})
                        res['power_level'] = power_level
                        res['reasons'].extend(power_reasons)
                        stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
                        res['stop_loss'] = stop_loss
                        res['take_profit1'] = tp1
                        res['take_profit2'] = tp2
                        pre_results.append(res)
                        symbol_signals[sym].append(res)
                    else:
                        still_remaining.append(sym)
                except Exception as e:
                    print(f"   ❌ {sym}: {str(e)[:50]}")
                    if "400" in str(e) or "404" in str(e):
                        mark_symbol_invalid(sym)
                    still_remaining.append(sym)

            if still_remaining:
                future_whale = {executor.submit(detect_whale_setup, sym): sym for sym in still_remaining if not is_symbol_invalid(sym)}
                whale_results = []
                still_remaining2 = []
                for future in as_completed(future_whale):
                    sym = future_whale[future]
                    try:
                        res = future.result(timeout=30)
                        if res:
                            print(f"   🐋 {sym}: {res['score']} WHALE_SETUP")
                            power_score, power_level, power_reasons = assess_signal_power(res['symbol'], res['score'], {})
                            res['power_level'] = power_level
                            res['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
                            res['stop_loss'] = stop_loss
                            res['take_profit1'] = tp1
                            res['take_profit2'] = tp2
                            whale_results.append(res)
                            symbol_signals[sym].append(res)
                        else:
                            still_remaining2.append(sym)
                    except Exception as e:
                        print(f"   ❌ {sym}: {str(e)[:50]}")
                        if "400" in str(e) or "404" in str(e):
                            mark_symbol_invalid(sym)
                        still_remaining2.append(sym)

                if still_remaining2:
                    individual_results = []
                    for sym in still_remaining2:
                        if is_symbol_invalid(sym):
                            continue
                        symbol_results = evaluate_symbol_patterns(sym)
                        for res in symbol_results:
                            print(f"   {PATTERN_ARABIC.get(res['pattern'], {}).get('emoji', '🔹')} {sym}: {res['score']} {res['pattern']}")
                            individual_results.append(res)
                            symbol_signals[sym].append(res)
                    # جمع كل النتائج
                    all_results = (long_results + pre_results + whale_results + individual_results)
                else:
                    all_results = (long_results + pre_results + whale_results)
            else:
                all_results = (long_results + pre_results)
        else:
            all_results = long_results

    # بعد جمع كل الإشارات، نضيف تعليق "فرصة دخول عالية" إذا تعددت الأنماط
    final_results = []
    for sym, sigs in symbol_signals.items():
        if len(sigs) > 1:
            # هناك أكثر من نمط لنفس الرمز
            for sig in sigs:
                sig['multi_pattern_hint'] = "🔥 (فرصة دخول عالية)"
                final_results.append(sig)
        else:
            final_results.extend(sigs)

    return final_results

# =============================================================================
# التنبيهات عبر تليجرام (تقرير نهائي)
# =============================================================================
def send_telegram(message):
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        print("\n" + message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}, timeout=10)
    except Exception as e:
        print(f"تليجرام خطأ: {e}")


HIERARCHICAL_STATES = {
    'EARLY_ACCUMULATION': 'مرشح مبكر',
    'INSTITUTIONAL_BUILDUP': 'إشارة مؤسسية',
    'PRE_BREAKOUT': 'انفجار وشيك',
    'IMMINENT_EXPANSION': 'انفجار وشيك جدًا'
}


def timeframe_label(tf: str) -> str:
    return {'1d': '1D', '4h': '4H', '15m': '15m', '5m': '5m'}.get(tf, tf)


def describe_daily_bias(symbol: str):
    kl = get_klines(symbol, '1d', 45)
    if not kl or len(kl['close']) < 25:
        return {'status': 'UNKNOWN', 'score': 0, 'text': 'بيانات يومية غير كافية'}

    closes = kl['close']
    ema20 = ema(closes[-20:], alpha=2/(20+1))
    ema40 = ema(closes[-40:], alpha=2/(40+1))
    slope = linear_trend_slope(closes[-12:])
    price_vs_ema20 = pct_change(ema20, closes[-1])

    if closes[-1] > ema20 > ema40 and slope > 0:
        return {'status': 'BULLISH', 'score': 3, 'text': f"اتجاه صاعد: السعر فوق EMA20/EMA40 وميل يومي موجب ({price_vs_ema20:+.2f}% فوق EMA20)"}
    if closes[-1] < ema20 < ema40 and slope < 0:
        return {'status': 'BEARISH', 'score': -3, 'text': f"اتجاه هابط: السعر أسفل EMA20/EMA40 وميل يومي سالب ({price_vs_ema20:+.2f}% مقابل EMA20)"}
    if closes[-1] >= ema20 and slope >= 0:
        return {'status': 'BULLISH_BIAS', 'score': 1, 'text': "انحياز صاعد غير مكتمل: السعر أعلى EMA20 لكن الاتجاه ليس قويًا"}
    if closes[-1] <= ema20 and slope <= 0:
        return {'status': 'BEARISH_BIAS', 'score': -1, 'text': "انحياز هابط غير مكتمل: السعر أسفل EMA20 لكن الاتجاه ليس قويًا"}
    return {'status': 'NEUTRAL', 'score': 0, 'text': "سوق يومي محايد/متذبذب"}


def describe_4h_confirmation(symbol: str, daily_status: str):
    kl = get_klines(symbol, '4h', 60)
    if not kl or len(kl['close']) < 30:
        return {'status': 'UNKNOWN', 'score': 0, 'text': 'بيانات 4H غير كافية', 'alignment': 'unknown'}

    closes = kl['close']
    ema21 = ema(closes[-21:], alpha=2/(21+1))
    ema55 = ema(closes[-55:], alpha=2/(55+1))
    slope = linear_trend_slope(closes[-10:])

    if closes[-1] > ema21 > ema55 and slope > 0:
        status = 'BULLISH'
        score = 2
    elif closes[-1] < ema21 < ema55 and slope < 0:
        status = 'BEARISH'
        score = -2
    else:
        status = 'MIXED'
        score = 0

    daily_bull = daily_status in ('BULLISH', 'BULLISH_BIAS')
    daily_bear = daily_status in ('BEARISH', 'BEARISH_BIAS')
    if (daily_bull and status == 'BULLISH') or (daily_bear and status == 'BEARISH'):
        alignment = 'aligned'
    elif (daily_bull and status == 'BEARISH') or (daily_bear and status == 'BULLISH'):
        alignment = 'conflict'
    else:
        alignment = 'partial'

    return {
        'status': status,
        'score': score,
        'alignment': alignment,
        'text': f"{status} | EMA21/EMA55 + ميل 4H ({'متوافق' if alignment == 'aligned' else 'متعارض' if alignment == 'conflict' else 'جزئي'})"
    }


def describe_15m_interest(symbol: str):
    kl = get_klines(symbol, '15m', 64)
    if not kl or len(kl['close']) < 25:
        return {'status': 'UNKNOWN', 'score': 0, 'zone': 'none', 'text': 'بيانات 15m غير كافية'}

    closes = kl['close']
    highs = kl['high']
    lows = kl['low']
    vols = kl['volume']
    change_3 = pct_change(closes[-4], closes[-1])
    change_6 = pct_change(closes[-7], closes[-1])
    bb_pct, _ = bollinger_bandwidth_percentile(closes, window=20, lookback=50)
    avg_vol = mean(vols[-20:-5]) if len(vols) >= 25 else mean(vols)
    vol_ratio = safe_div(vols[-1], avg_vol, default=1.0)
    recent_high = max(highs[-12:-1])
    recent_low = min(lows[-12:-1])

    if bb_pct < 25 and vol_ratio >= 1.15:
        return {'status': 'BUILDUP', 'score': 2, 'zone': 'squeeze', 'text': f"بداية squeeze/build-up | BB%={bb_pct:.0f} | حجم={vol_ratio:.2f}x"}
    if closes[-1] > recent_high and change_3 > 0.6:
        return {'status': 'BULLISH_INTEREST', 'score': 3, 'zone': 'breakout_zone', 'text': f"منطقة اهتمام صاعدة (اختراق 15m +{change_3:.2f}%)"}
    if closes[-1] < recent_low and change_3 < -0.6:
        return {'status': 'BEARISH_INTEREST', 'score': -3, 'zone': 'failure_zone', 'text': f"منطقة ضعف/فشل هابطة (كسر 15m {change_3:.2f}%)"}
    if change_6 > 0.9:
        return {'status': 'BULLISH_INTEREST', 'score': 1, 'zone': 'monitor_up', 'text': f"تجميع صاعد تدريجي على 15m ({change_6:+.2f}%)"}
    if change_6 < -0.9:
        return {'status': 'BEARISH_INTEREST', 'score': -1, 'zone': 'monitor_down', 'text': f"ضغط هابط تدريجي على 15m ({change_6:+.2f}%)"}
    return {'status': 'NEUTRAL', 'score': 0, 'zone': 'none', 'text': 'لا توجد منطقة اهتمام واضحة على 15m'}


def describe_5m_trigger(symbol: str, side_hint: str = 'UP'):
    kl = get_klines(symbol, '5m', 36)
    oi5 = get_oi_history(symbol, '5m', 18)
    if not kl or len(kl['close']) < 12:
        return {'status': 'UNKNOWN', 'score': 0, 'timing': 'unknown', 'text': 'بيانات 5m غير كافية'}

    closes = kl['close']
    vols = kl['volume']
    change_1 = pct_change(closes[-2], closes[-1])
    change_3 = pct_change(closes[-4], closes[-1])
    avg_vol = mean(vols[-12:-1]) if len(vols) >= 13 else mean(vols)
    vol_ratio = safe_div(vols[-1], avg_vol, default=1.0)
    oi_delta = pct_change(oi5[-2], oi5[-1]) if oi5 and len(oi5) >= 2 else 0.0

    if side_hint == 'UP':
        if change_1 > 0.2 and change_3 < 1.5 and vol_ratio >= 1.1:
            return {'status': 'TRIGGER_UP', 'score': 2, 'timing': 'early', 'text': f"trigger صاعد مبكر | 5m={change_1:+.2f}% | حجم={vol_ratio:.2f}x | OI={oi_delta:+.2f}%"}
        if change_3 >= 2.0 or vol_ratio >= 2.2:
            return {'status': 'TRIGGER_UP', 'score': 1, 'timing': 'late', 'text': f"الدخول متأخر نسبيًا بعد اندفاع 5m ({change_3:+.2f}%)"}
    else:
        if change_1 < -0.2 and change_3 > -1.5 and vol_ratio >= 1.1:
            return {'status': 'TRIGGER_DOWN', 'score': -2, 'timing': 'early', 'text': f"trigger هابط مبكر | 5m={change_1:+.2f}% | حجم={vol_ratio:.2f}x | OI={oi_delta:+.2f}%"}
        if change_3 <= -2.0 or vol_ratio >= 2.2:
            return {'status': 'TRIGGER_DOWN', 'score': -1, 'timing': 'late', 'text': f"الدخول متأخر نسبيًا بعد هبوط 5m ({change_3:+.2f}%)"}

    return {'status': 'NO_TRIGGER', 'score': 0, 'timing': 'waiting', 'text': 'لا يوجد trigger تنفيذي واضح على 5m'}


def detect_hierarchical_states(direction: str, tf15: dict, tf5: dict, flow_data: dict):
    states = []
    if direction == 'UP':
        if tf15['status'] in ('BULLISH_INTEREST', 'BUILDUP') and tf5['timing'] == 'early':
            states.append('EARLY_ACCUMULATION')
        if flow_data.get('buy_ratio', 0.0) >= 0.57 and flow_data.get('recent_trade_ratio', 1.0) >= 1.15:
            states.append('INSTITUTIONAL_BUILDUP')
        if tf15.get('zone') in ('breakout_zone', 'squeeze'):
            states.append('PRE_BREAKOUT')
        if flow_data.get('oi_15m', 0.0) >= 3.0 and flow_data.get('price_15m', 0.0) >= 1.4:
            states.append('IMMINENT_EXPANSION')
    else:
        if tf15['status'] == 'BEARISH_INTEREST' and tf5['timing'] == 'early':
            states.append('EARLY_ACCUMULATION')
        if flow_data.get('buy_ratio', 0.0) <= 0.44 and flow_data.get('recent_trade_ratio', 1.0) <= 0.9:
            states.append('INSTITUTIONAL_BUILDUP')
        if tf15.get('zone') == 'failure_zone':
            states.append('PRE_BREAKOUT')
        if flow_data.get('oi_15m', 0.0) >= 2.8 and flow_data.get('price_15m', 0.0) <= -1.3:
            states.append('IMMINENT_EXPANSION')
    return states


def build_hierarchical_decision(signal_data: dict):
    symbol = signal_data['symbol']
    direction = signal_data.get('direction', 'UP')

    flow_data = analyze_orderflow_short(symbol)
    oi_profile = get_oi_accel_profile(symbol)
    flow_features = {
        'buy_ratio': flow_data.get('recent_buy_ratio', 0.5),
        'recent_trade_ratio': flow_data.get('recent_trade_ratio', 1.0),
        'price_15m': flow_data.get('price_change_15m', 0.0),
        'oi_15m': oi_profile.get('delta_15m', 0.0),
    }

    tf1d = describe_daily_bias(symbol)
    tf4h = describe_4h_confirmation(symbol, tf1d['status'])
    tf15 = describe_15m_interest(symbol)
    tf5 = describe_5m_trigger(symbol, side_hint=direction)
    states = detect_hierarchical_states(direction, tf15, tf5, flow_features)

    direction_gate = True
    if direction == 'UP':
        direction_gate = tf1d['score'] >= -1 and tf4h['status'] in ('BULLISH', 'MIXED') and tf15['score'] >= 1 and tf5['status'] == 'TRIGGER_UP'
    elif direction == 'DOWN':
        direction_gate = tf1d['score'] <= 1 and tf4h['status'] in ('BEARISH', 'MIXED') and tf15['score'] <= -1 and tf5['status'] == 'TRIGGER_DOWN'

    quality = max(0, signal_data.get('score', 0))
    quality += 8 if tf4h['alignment'] == 'aligned' else -6 if tf4h['alignment'] == 'conflict' else 0
    quality += 5 * len(states)
    quality += 5 if tf5['timing'] == 'early' else -8 if tf5['timing'] == 'late' else 0
    quality += int(scale_to_unit(abs(flow_features['oi_15m']), 0, 6) * 6)
    quality = max(0, min(100, quality))

    decisive_factor = 'توافق 1D و4H مع منطقة اهتمام 15m وتفعيل 5m'
    if tf4h['alignment'] == 'conflict':
        decisive_factor = 'تعارض 4H مع 1D خفّض الجودة'
    elif len(states) >= 2:
        decisive_factor = 'تعدد الحالات الداعمة على نفس العملة'
    elif tf5['timing'] == 'late':
        decisive_factor = 'الإشارة تأخرت على 5m رغم تحقق الشروط'

    return {
        'eligible': direction_gate and len(states) >= 1,
        'symbol': symbol,
        'direction': direction,
        'base_classification': signal_data.get('classification') or signal_data.get('pattern'),
        'quality': quality,
        'states': states,
        'timing': tf5['timing'],
        'tf1d': tf1d,
        'tf4h': tf4h,
        'tf15': tf15,
        'tf5': tf5,
        'decisive_factor': decisive_factor,
        'why': signal_data.get('reasons', []),
        'not_random': [
            'تم فرض تسلسل 1D -> 4H -> 15m -> 5m',
            'تم رفض أي إشارة لا تملك trigger على 5m مدعومًا من الفريمات الأكبر',
            f"حالات داعمة: {', '.join(HIERARCHICAL_STATES.get(s, s) for s in states)}"
        ],
    }


def format_hierarchical_block(item: dict):
    states_ar = [HIERARCHICAL_STATES.get(s, s) for s in item['states']]
    return (
        f"- الرمز: {item['symbol']}\n"
        f"- الاتجاه المتوقع: {'صعود' if item['direction'] == 'UP' else 'هبوط'}\n"
        f"- التصنيف الأساسي: {item['base_classification']}\n"
        f"- جودة الإشارة: {item['quality']}/100\n"
        f"- الحالة الأساسية: {states_ar[0] if states_ar else 'غير محددة'}\n"
        f"- الحالات المتوافقة: {', '.join(states_ar) if states_ar else 'لا يوجد'}\n"
        f"- 1D: {item['tf1d']['text']}\n"
        f"- 4H: {item['tf4h']['text']}\n"
        f"- 15m: {item['tf15']['text']}\n"
        f"- 5m: {item['tf5']['text']}\n"
        f"- العامل الحاسم: {item['decisive_factor']}\n"
        f"- لماذا تم ترشيحها: {'، '.join(item['why']) if item['why'] else 'توافق هرمي + تدفق + OI'}\n"
        f"- لماذا ليست عشوائية: {' | '.join(item['not_random'])}\n"
        f"- هل الدخول مبكر أم متأخر: {item['timing']}\n"
    )


def generate_hierarchical_report(results):
    if not results:
        return []

    decisions = []
    for r in results:
        try:
            decisions.append(build_hierarchical_decision(r))
        except Exception as e:
            diagnostics.log_error(r.get('symbol', 'UNKNOWN'), 'build_hierarchical_decision', str(e))

    bullish = [d for d in decisions if d['eligible'] and d['direction'] == 'UP']
    bearish = [d for d in decisions if d['eligible'] and d['direction'] == 'DOWN']

    bullish.sort(key=lambda x: x['quality'], reverse=True)
    bearish.sort(key=lambda x: x['quality'], reverse=True)

    lines = ["🚦 *تقرير الترشيح الهرمي متعدد الفريمات*", ""]
    lines.append("📈 *أفضل مرشحي الصعود*")
    if bullish:
        for b in bullish[:8]:
            lines.append(format_hierarchical_block(b))
    else:
        lines.append("- لا توجد عملات تستوفي الشروط الصارمة للصعود حالياً.")

    lines.append("📉 *أفضل مرشحي الهبوط*")
    if bearish:
        for b in bearish[:8]:
            lines.append(format_hierarchical_block(b))
    else:
        lines.append("- لا توجد عملات تستوفي الشروط الصارمة للهبوط حالياً.")

    return lines

def generate_report(results):
    if not results:
        print("⚠️ لا توجد إشارات.")
        return
    results.sort(key=lambda x: x['score'], reverse=True)
    lines = ["🚀 *تقرير الماسح المتكامل*\n"]
    lines.extend(generate_hierarchical_report(results))
    lines.append("\n🔎 *الإشارات الخام (مرجعية)*\n")
    for r in results[:10]:
        pattern_key = r['pattern']
        pattern_info = PATTERN_ARABIC.get(pattern_key, {
            'name': pattern_key,
            'default_direction': 'غير معروف',
            'emoji': '🔹'
        })
        emoji = pattern_info['emoji']
        pattern_name_ar = pattern_info['name']
        direction = r.get('direction', 'UP')
        if direction == 'UP':
            direction_ar = '🟢 شراء'
        elif direction == 'DOWN':
            direction_ar = '🔴 بيع'
        else:
            direction_ar = pattern_info['default_direction']

        reasons = "، ".join(r.get('reasons', []))
        stop_loss = r.get('stop_loss') or 0
        tp1 = r.get('take_profit1') or 0
        tp2 = r.get('take_profit2') or 0
        multi_hint = r.get('multi_pattern_hint', '')
        signal_stage = r.get('signal_stage', 'SETUP')

        lines.append(
            f"{emoji} *{r['symbol']}*: {r['score']} ({pattern_name_ar}) {multi_hint} [{r.get('power_level', 'عادية')}]\n"
            f"   الاتجاه: {direction_ar} | المرحلة: {signal_stage}\n"
            f"   السعر: {r['price']:.6f}\n"
            f"   وقف الخسارة: {stop_loss:.6f} | الهدف1: {tp1:.6f} | الهدف2: {tp2:.6f}\n"
            f"   أسباب: {reasons}\n"
        )
        try:
            db.save_signal(r)
        except Exception as e:
            diagnostics.log_error(r.get('symbol', 'UNKNOWN'), 'save_signal', str(e))
            if DEBUG:
                print(f"⚠️ فشل حفظ الإشارة لـ {r.get('symbol', 'UNKNOWN')}: {e}")

    full = "\n".join(lines)
    print(full)
    send_telegram(full)

def generate_backtest_report(lookback_hours=BACKTEST_LOOKBACK_HOURS):
    rows = db.get_backtest_summary(lookback_hours=lookback_hours)
    if not rows:
        print("📈 لا توجد بيانات كافية للباك تست بعد.")
        return
    print("\n📈 ملخص الباك تست المحلي:")
    for pattern, total, win5, win15, win30 in rows[:10]:
        total = total or 0
        r5 = (win5 / total * 100) if total else 0
        r15 = (win15 / total * 100) if total else 0
        r30 = (win30 / total * 100) if total else 0
        print(f"   {pattern}: total={total} | 5m={r5:.1f}% | 15m={r15:.1f}% | 30m={r30:.1f}%")

# =============================================================================
# الحلقة الرئيسية
# =============================================================================
def main():
    print("="*70)
    print("الماسح المتوازي المتقدم - الإصدار v5 الاحترافي لـ Pydroid 3".center(70))
    print("="*70)
    if ENABLE_DIAGNOSTICS:
        print(f"🧪 سجل التشخيص: {DIAGNOSTICS_PATH}")

    all_symbols = get_all_usdt_perpetuals()
    print(f"✅ إجمالي العقود: {len(all_symbols)}")

    if ENABLE_HMM:
        print("🔄 تدريب نموذج HMM...")
        hmm_detector.fit_model()
        regime = hmm_detector.get_current_regime()
        print(f"📊 حالة السوق الحالية: {regime['regime_name']}")

    learning_system = LearningSystem(db)

    while True:
        start_time = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] بدء الدورة...")

        candidates = quick_filter()
        if not candidates:
            print("⚠️ لا توجد مرشحات بعد الفلترة.")
        else:
            results = deep_scan(candidates, learning_system)
            generate_report(results)

        if ENABLE_LEARNING:
            evaluated_count = learning_system.evaluate_signals()
            if evaluated_count:
                print(f"🧠 تم تقييم {evaluated_count} نتيجة عبر آفاق 5m/15m/30m")
            hours_since_last = (datetime.now() - learning_system.last_learning_time).total_seconds() / 3600
            if hours_since_last >= LEARNING_INTERVAL_HOURS:
                print("🔄 جاري تحديث الأوزان وعرض ملخص الباك تست...")
                learning_system.adjust_weights()
                generate_backtest_report()
                learning_system.last_learning_time = datetime.now()

        elapsed = time.time() - start_time
        print(f"⏱️ استغرق المسح: {elapsed:.2f} ثانية")
        print(f"⏳ انتظار {SCAN_INTERVAL_MINUTES} دقيقة قبل الدورة التالية...")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ تم إيقاف البرنامج.")
        session_manager.close_all()