#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
الماسح المرجعي المنظف - Four-Family Reference Engine
================================================================================
نسخة منقحة تنظف واجهة القرار النهائية بحيث تعتمد فقط على أربع عائلات مرجعية:
1) POSITION_LED_SQUEEZE_BUILDUP
2) ACCOUNT_LED_ACCUMULATION
3) CONSENSUS_BULLISH_EXPANSION
4) FLOW_LIQUIDITY_VACUUM_BREAKOUT

التمويل هنا طبقة سياقية فقط، ولا يعمل كـ trigger مستقل.
تم عزل الواجهات legacy من التقرير النهائي والحفظ النهائي، مع الإبقاء فقط على
طبقات البيانات الخام المفيدة للعائلات الأربع.
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
import math


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

    # فلتر الحركة الكبيرة 24H (تسمية محايدة غير legacy)
    "FAST_MOVE_24H_THRESHOLD": 8.0,

    # عتبات عامة داعمة للـ market context
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

    # إعدادات تقييم القوة العامة

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

    # أوزان العائلات الأربع فقط
    "FAMILY_WEIGHTS": {
        "POSITION_LED_SQUEEZE_BUILDUP": 0.25,
        "ACCOUNT_LED_ACCUMULATION": 0.25,
        "CONSENSUS_BULLISH_EXPANSION": 0.25,
        "FLOW_LIQUIDITY_VACUUM_BREAKOUT": 0.25
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
    "MAX_SYMBOLS_PER_CYCLE": 140,

    # ثبات الكون والمرحلة التنفيذية
    "UNIVERSE_MIN_CYCLES_TO_KEEP": 2,
    "PERSISTENCE_MIN_CYCLES": 2,
    "PERSISTENCE_SHORT_SQUEEZE_BYPASS": 1,
    "SETUP_PRINT_MIN_SCORE": 84,
    "ARMED_PRINT_MIN_SCORE": 80,
    "TRIGGER_PRINT_MIN_SCORE": 76,
    "MIN_BUY_RATIO_FLOW_STRONG": 0.58,
    "MIN_BUY_RATIO_FLOW_ACTIONABLE": 0.54,
    "MIN_OFI_FLOW_STRONG": 0.12,
    "MIN_OFI_FLOW_ACTIONABLE": 0.08,
    "HYBRID_ALIGNMENT_MIN": 0.50,
    "HYBRID_POS_ACC_MIN": 1.10,
    "HYBRID_ONE_CYCLE_PRINT_SCORE": 78,
    "EXPLOSIVE_ONE_CYCLE_PRICE5M_MIN": 0.90,
    "EXPLOSIVE_ONE_CYCLE_PRICE15M_MIN": 1.20,
    "FAST_MOVER_INTRADAY_COUNT_MIN": 1200,
    "FAST_MOVER_INTRADAY_VOL_RATIO_MIN": 1.35,
    "DYNAMIC_SAFETY_MIN_VOLUME_USD": 150000,
    "DYNAMIC_SAFETY_MIN_24H_TRADES": 120,
    "DYNAMIC_BASE_LOOKBACK_BARS": 6,
    "DYNAMIC_MIN_RECENT_QUOTEVOL": 60000,
    "DYNAMIC_MIN_RECENT_TRADES": 45,
    "DYNAMIC_MIN_VOL_RATIO": 1.18,
    "DYNAMIC_MIN_TRADE_RATIO": 1.15,
    "DYNAMIC_MIN_PRICE_5M": 0.22,
    "DYNAMIC_MIN_PRICE_15M": 0.45,
    "DYNAMIC_MIN_OI_5M": 0.18,
    "DYNAMIC_MIN_OI_15M": 0.35,
    "DYNAMIC_MIN_OINV_5M": 0.20,
    "DYNAMIC_MIN_OINV_15M": 0.45,
    "DYNAMIC_BREAKOUT_BUFFER_PCT": 0.0015,
    "DYNAMIC_KEEP_HOT_CYCLES": 3,
    "PRETRIGGER_MAX_PRICE_5M": 1.35,
    "PRETRIGGER_MAX_PRICE_15M": 4.8,
    "HTTP_400_BAN_CYCLES": 6,
    "UNIVERSE_STABILITY_BUFFER": 25,
    "EARLY_BASELINE_COUNT_MIN": 800,
    "EARLY_BASELINE_VOLUME_MULT": 1.20,
    "SHORT_CROWD_RATIO_MAX": 0.92,
    "EXTREME_SHORT_CROWD_RATIO_MAX": 0.78,
    "SHORT_SQUEEZE_MIN_PRICE_5M": 0.35,
    "SHORT_SQUEEZE_MIN_PRICE_15M": 0.80,
    "SHORT_SQUEEZE_MIN_OI_5M": 0.12,
    "SHORT_SQUEEZE_MIN_OI_15M": 0.25,
    "FLOW_BREAKOUT_LOOKBACK": 7,
    "ENABLE_RISK_SYSTEM": False,

    # عتبات إضافية
    "SIGNAL_MIN_SCORE": 65,              # أقل درجة لقبول الإشارة المبكرة

    # طبقة الذاكرة البشرية الاصطناعية
    "ENABLE_HUMAN_MEMORY": True,
    "MEMORY_HISTORY_LIMIT": 8,
    "MEMORY_OUTCOME_LOOKBACK": 18,
    "MEMORY_OUTCOME_CACHE_TTL": 600,
    "MEMORY_SUCCESS_BONUS_MAX": 10.0,
    "MEMORY_FALSESTART_PENALTY_MAX": 8.0
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
FAST_MOVE_24H_THRESHOLD = CONFIG.get("FAST_MOVE_24H_THRESHOLD", CONFIG.get("SKYROCKET_THRESHOLD", 8.0))
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
FAMILY_WEIGHTS = CONFIG.get("FAMILY_WEIGHTS", CONFIG.get("PATTERN_WEIGHTS", DEFAULT_CONFIG["FAMILY_WEIGHTS"]))
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
SIGNAL_MIN_SCORE = CONFIG.get("SIGNAL_MIN_SCORE", 65)
UNIVERSE_MIN_CYCLES_TO_KEEP = CONFIG.get("UNIVERSE_MIN_CYCLES_TO_KEEP", 2)
PERSISTENCE_MIN_CYCLES = CONFIG.get("PERSISTENCE_MIN_CYCLES", 2)
PERSISTENCE_SHORT_SQUEEZE_BYPASS = CONFIG.get("PERSISTENCE_SHORT_SQUEEZE_BYPASS", 1)
SETUP_PRINT_MIN_SCORE = CONFIG.get("SETUP_PRINT_MIN_SCORE", 84)
ARMED_PRINT_MIN_SCORE = CONFIG.get("ARMED_PRINT_MIN_SCORE", 84)
TRIGGER_PRINT_MIN_SCORE = CONFIG.get("TRIGGER_PRINT_MIN_SCORE", 80)
MIN_BUY_RATIO_FLOW_STRONG = CONFIG.get("MIN_BUY_RATIO_FLOW_STRONG", 0.58)
MIN_BUY_RATIO_FLOW_ACTIONABLE = CONFIG.get("MIN_BUY_RATIO_FLOW_ACTIONABLE", 0.54)
MIN_OFI_FLOW_STRONG = CONFIG.get("MIN_OFI_FLOW_STRONG", 0.12)
MIN_OFI_FLOW_ACTIONABLE = CONFIG.get("MIN_OFI_FLOW_ACTIONABLE", 0.08)
HYBRID_ALIGNMENT_MIN = CONFIG.get("HYBRID_ALIGNMENT_MIN", 0.50)
HYBRID_POS_ACC_MIN = CONFIG.get("HYBRID_POS_ACC_MIN", 1.10)
HYBRID_ONE_CYCLE_PRINT_SCORE = CONFIG.get("HYBRID_ONE_CYCLE_PRINT_SCORE", 78)
EXPLOSIVE_ONE_CYCLE_PRICE5M_MIN = CONFIG.get("EXPLOSIVE_ONE_CYCLE_PRICE5M_MIN", 0.90)
EXPLOSIVE_ONE_CYCLE_PRICE15M_MIN = CONFIG.get("EXPLOSIVE_ONE_CYCLE_PRICE15M_MIN", 1.20)
FAST_MOVER_INTRADAY_COUNT_MIN = CONFIG.get("FAST_MOVER_INTRADAY_COUNT_MIN", 1200)
FAST_MOVER_INTRADAY_VOL_RATIO_MIN = CONFIG.get("FAST_MOVER_INTRADAY_VOL_RATIO_MIN", 1.35)
DYNAMIC_SAFETY_MIN_VOLUME_USD = CONFIG.get("DYNAMIC_SAFETY_MIN_VOLUME_USD", 150000)
DYNAMIC_SAFETY_MIN_24H_TRADES = CONFIG.get("DYNAMIC_SAFETY_MIN_24H_TRADES", 120)
DYNAMIC_BASE_LOOKBACK_BARS = CONFIG.get("DYNAMIC_BASE_LOOKBACK_BARS", 6)
DYNAMIC_MIN_RECENT_QUOTEVOL = CONFIG.get("DYNAMIC_MIN_RECENT_QUOTEVOL", 60000)
DYNAMIC_MIN_RECENT_TRADES = CONFIG.get("DYNAMIC_MIN_RECENT_TRADES", 45)
DYNAMIC_MIN_VOL_RATIO = CONFIG.get("DYNAMIC_MIN_VOL_RATIO", 1.18)
DYNAMIC_MIN_TRADE_RATIO = CONFIG.get("DYNAMIC_MIN_TRADE_RATIO", 1.15)
DYNAMIC_MIN_PRICE_5M = CONFIG.get("DYNAMIC_MIN_PRICE_5M", 0.22)
DYNAMIC_MIN_PRICE_15M = CONFIG.get("DYNAMIC_MIN_PRICE_15M", 0.45)
DYNAMIC_MIN_OI_5M = CONFIG.get("DYNAMIC_MIN_OI_5M", 0.18)
DYNAMIC_MIN_OI_15M = CONFIG.get("DYNAMIC_MIN_OI_15M", 0.35)
DYNAMIC_MIN_OINV_5M = CONFIG.get("DYNAMIC_MIN_OINV_5M", 0.20)
DYNAMIC_MIN_OINV_15M = CONFIG.get("DYNAMIC_MIN_OINV_15M", 0.45)
DYNAMIC_BREAKOUT_BUFFER_PCT = CONFIG.get("DYNAMIC_BREAKOUT_BUFFER_PCT", 0.0015)
DYNAMIC_KEEP_HOT_CYCLES = CONFIG.get("DYNAMIC_KEEP_HOT_CYCLES", 3)
PRETRIGGER_MAX_PRICE_5M = CONFIG.get("PRETRIGGER_MAX_PRICE_5M", 1.35)
PRETRIGGER_MAX_PRICE_15M = CONFIG.get("PRETRIGGER_MAX_PRICE_15M", 3.8)
HTTP_400_BAN_CYCLES = CONFIG.get("HTTP_400_BAN_CYCLES", 6)
UNIVERSE_STABILITY_BUFFER = CONFIG.get("UNIVERSE_STABILITY_BUFFER", 25)
EARLY_BASELINE_COUNT_MIN = CONFIG.get("EARLY_BASELINE_COUNT_MIN", 800)
EARLY_BASELINE_VOLUME_MULT = CONFIG.get("EARLY_BASELINE_VOLUME_MULT", 1.20)
SHORT_CROWD_RATIO_MAX = CONFIG.get("SHORT_CROWD_RATIO_MAX", 0.92)
EXTREME_SHORT_CROWD_RATIO_MAX = CONFIG.get("EXTREME_SHORT_CROWD_RATIO_MAX", 0.78)
SHORT_SQUEEZE_MIN_PRICE_5M = CONFIG.get("SHORT_SQUEEZE_MIN_PRICE_5M", 0.35)
SHORT_SQUEEZE_MIN_PRICE_15M = CONFIG.get("SHORT_SQUEEZE_MIN_PRICE_15M", 0.80)
SHORT_SQUEEZE_MIN_OI_5M = CONFIG.get("SHORT_SQUEEZE_MIN_OI_5M", 0.12)
SHORT_SQUEEZE_MIN_OI_15M = CONFIG.get("SHORT_SQUEEZE_MIN_OI_15M", 0.25)
FLOW_BREAKOUT_LOOKBACK = CONFIG.get("FLOW_BREAKOUT_LOOKBACK", 7)
ENABLE_RISK_SYSTEM = CONFIG.get("ENABLE_RISK_SYSTEM", False)

PRE_IGNITION_MEMORY_BARS = CONFIG.get("PRE_IGNITION_MEMORY_BARS", 8)
PRE_IGNITION_MIN_SCORE = CONFIG.get("PRE_IGNITION_MIN_SCORE", 0.56)
PREPARED_CONTINUATION_MIN_SCORE = CONFIG.get("PREPARED_CONTINUATION_MIN_SCORE", 0.60)
SHORT_SQUEEZE_OVERRIDE_MIN_SCORE = CONFIG.get("SHORT_SQUEEZE_OVERRIDE_MIN_SCORE", 0.62)
BURST_REENTRY_TTL = CONFIG.get("BURST_REENTRY_TTL", 4)
BURST_REENTRY_MIN_RANK = CONFIG.get("BURST_REENTRY_MIN_RANK", 5.6)
FAST_REENTRY_PRICE_5M = CONFIG.get("FAST_REENTRY_PRICE_5M", 0.55)
FAST_REENTRY_PRICE_15M = CONFIG.get("FAST_REENTRY_PRICE_15M", 0.95)
FLAT_OI_CONTEXT_ALLOW_SCORE = CONFIG.get("FLAT_OI_CONTEXT_ALLOW_SCORE", 0.58)
ENABLE_HUMAN_MEMORY = CONFIG.get("ENABLE_HUMAN_MEMORY", True)
MEMORY_HISTORY_LIMIT = int(CONFIG.get("MEMORY_HISTORY_LIMIT", 8) or 8)
MEMORY_OUTCOME_LOOKBACK = int(CONFIG.get("MEMORY_OUTCOME_LOOKBACK", 18) or 18)
MEMORY_OUTCOME_CACHE_TTL = int(CONFIG.get("MEMORY_OUTCOME_CACHE_TTL", 600) or 600)
MEMORY_SUCCESS_BONUS_MAX = float(CONFIG.get("MEMORY_SUCCESS_BONUS_MAX", 10.0) or 10.0)
GRACE_PERSISTENCE_CYCLES = int(CONFIG.get("GRACE_PERSISTENCE_CYCLES", 2) or 2)
GRACE_EXECUTION_FLOOR = float(CONFIG.get("GRACE_EXECUTION_FLOOR", 170.0) or 170.0)
CONSENSUS_PRINT_MIN_OINV = float(CONFIG.get("CONSENSUS_PRINT_MIN_OINV", 0.10) or 0.10)
CONSENSUS_MIN_DELTA_SUM = float(CONFIG.get("CONSENSUS_MIN_DELTA_SUM", 0.006) or 0.006)
FLOW_LOW_TRADE_RATIO_MIN = float(CONFIG.get("FLOW_LOW_TRADE_RATIO_MIN", 0.95) or 0.95)
FLOW_MICRO_CONFIRM_BUY = float(CONFIG.get("FLOW_MICRO_CONFIRM_BUY", 0.56) or 0.56)
FLOW_MICRO_CONFIRM_OFI = float(CONFIG.get("FLOW_MICRO_CONFIRM_OFI", 0.10) or 0.10)
MICRO_IGNITION_ENABLED = bool(CONFIG.get("MICRO_IGNITION_ENABLED", True))
MICRO_IGNITION_INTERVAL = CONFIG.get("MICRO_IGNITION_INTERVAL", "1m")
MICRO_IGNITION_LOOKBACK = int(CONFIG.get("MICRO_IGNITION_LOOKBACK", 18) or 18)
MICRO_IGNITION_BREAKOUT_BARS = int(CONFIG.get("MICRO_IGNITION_BREAKOUT_BARS", 6) or 6)
MICRO_IGNITION_VOL_RATIO_MIN = float(CONFIG.get("MICRO_IGNITION_VOL_RATIO_MIN", 1.8) or 1.8)
MICRO_IGNITION_PRICE_MIN = float(CONFIG.get("MICRO_IGNITION_PRICE_MIN", 0.22) or 0.22)
MICRO_IGNITION_PRICE_BURST = float(CONFIG.get("MICRO_IGNITION_PRICE_BURST", 0.35) or 0.35)
MICRO_IGNITION_TRADE_BONUS_MIN = float(CONFIG.get("MICRO_IGNITION_TRADE_BONUS_MIN", 1.15) or 1.15)
MEMORY_FALSESTART_PENALTY_MAX = float(CONFIG.get("MEMORY_FALSESTART_PENALTY_MAX", 8.0) or 8.0)

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

PRIMARY_FAMILIES = [
    "POSITION_LED_SQUEEZE_BUILDUP",
    "ACCOUNT_LED_ACCUMULATION",
    "CONSENSUS_BULLISH_EXPANSION",
    "FLOW_LIQUIDITY_VACUUM_BREAKOUT",
]

ALLOWED_FUNDING_CONTEXTS = [
    "FUNDING_QUIET_REGIME",
    "FUNDING_NEGATIVE_IMPROVING",
    "FUNDING_NON_CROWDED",
    "FUNDING_UNINFORMATIVE",
]

ALLOWED_SECONDARY_CONTEXTS = [
    "CROWDED_SHORT_SQUEEZE",
    "BASIS_NEUTRAL_SUPPORT",
    "SMART_ACCUMULATION_SUPPORT",
]

FUNDING_CONTEXT_PRIORITY = {
    "FUNDING_QUIET_REGIME": 4,
    "FUNDING_NEGATIVE_IMPROVING": 3,
    "FUNDING_NON_CROWDED": 2,
    "FUNDING_UNINFORMATIVE": 1,
}

PRIMARY_FAMILY_ARABIC = {
    "POSITION_LED_SQUEEZE_BUILDUP": {"name": "قيادة المراكز / squeeze buildup", "emoji": "🧲"},
    "ACCOUNT_LED_ACCUMULATION": {"name": "قيادة الحسابات / accumulation", "emoji": "🧠"},
    "CONSENSUS_BULLISH_EXPANSION": {"name": "توسع صاعد توافقي", "emoji": "📈"},
    "FLOW_LIQUIDITY_VACUUM_BREAKOUT": {"name": "اختراق تدفق / فراغ سيولة", "emoji": "⚡"},
}

STAGE_ARABIC = {
    "DISCOVERY_ONLY": "اكتشاف فقط",
    "WATCH": "مراقبة",
    "PREPARE": "تهيؤ",
    "ARMED": "جاهز",
    "TRIGGERED": "انطلاق",
    "FAILED_CONTINUATION": "فشل الاستمرار",
    "LATE_CHASE": "مطاردة متأخرة",
    "REJECTED": "مرفوض",
}

QUALITY_ARABIC = {
    "HIGH": "عالية",
    "MEDIUM": "متوسطة",
    "LOW": "منخفضة",
}

FUNDING_CONTEXT_ARABIC = {
    "FUNDING_QUIET_REGIME": "تمويل هادئ",
    "FUNDING_NEGATIVE_IMPROVING": "تمويل سلبي يتحسن",
    "FUNDING_NON_CROWDED": "تمويل غير مزدحم",
    "FUNDING_UNINFORMATIVE": "تمويل غير حاسم",
}

OI_STATE_ARABIC = {
    "BUILDUP_EXPANSION": "بناء/تمدد",
    "PREMOVE_BUILDUP": "بناء قبل الحركة",
    "SHORT_COVERING": "تغطية شورت",
    "LONG_UNWIND": "تفكيك لونغ",
    "NEUTRAL": "محايد",
    "UNKNOWN": "غير معروف",
}


def family_name_ar(family):
    return PRIMARY_FAMILY_ARABIC.get(family, {"name": family, "emoji": "🔹"}).get("name", family)


def stage_name_ar(stage):
    return STAGE_ARABIC.get(stage, stage)


def quality_name_ar(q):
    return QUALITY_ARABIC.get(q, q)


def funding_name_ar(ctx):
    return FUNDING_CONTEXT_ARABIC.get(ctx, ctx)


def oi_state_name_ar(state):
    return OI_STATE_ARABIC.get(state, state)


def persistence_name_ar(cycles):
    cycles = int(cycles or 1)
    if cycles <= 1:
        return "جديد"
    if cycles == 2:
        return "متكرر"
    if cycles == 3:
        return "مستقر أوليًا"
    if cycles <= 5:
        return "ثابت"
    return "راسخ"


def stage_transition_text_ar(prev_stage, current_stage):
    if not prev_stage or prev_stage == current_stage:
        return "ثبات مرحلي"
    mapping = {
        ('WATCH', 'PREPARE'): 'انتقال مراقبة ← تهيؤ',
        ('PREPARE', 'ARMED'): 'ترقية تهيؤ ← جاهز',
        ('ARMED', 'TRIGGERED'): 'ترقية جاهز ← انطلاق',
        ('WATCH', 'ARMED'): 'قفزة مبكرة إلى الجاهزية',
        ('PREPARE', 'TRIGGERED'): 'قفزة مباشرة إلى الانطلاق',
        ('WATCH', 'DISCOVERY_ONLY'): 'تحول إلى اكتشاف فقط',
        ('PREPARE', 'FAILED_CONTINUATION'): 'فشل استمرار بعد التهيؤ',
        ('ARMED', 'FAILED_CONTINUATION'): 'فشل استمرار بعد الجاهزية',
        ('TRIGGERED', 'LATE_CHASE'): 'الإشارة أصبحت مطاردة متأخرة',
    }
    return mapping.get((prev_stage, current_stage), f"تحول مرحلي: {stage_name_ar(prev_stage)} ←→ {stage_name_ar(current_stage)}")


def family_transition_text_ar(prev_family, current_family):
    if not prev_family or prev_family == current_family:
        return "ثبات عائلي"
    return f"تحول عائلي: {family_name_ar(prev_family)} ← {family_name_ar(current_family)}"


def _compress_history(seq):
    out = []
    for item in seq or []:
        if not item:
            continue
        if not out or out[-1] != item:
            out.append(item)
    return out


_STAGE_ORDER = {
    'REJECTED': -3,
    'FAILED_CONTINUATION': -2,
    'LATE_CHASE': -1,
    'DISCOVERY_ONLY': 0,
    'WATCH': 1,
    'PREPARE': 2,
    'ARMED': 3,
    'TRIGGERED': 4,
}


def _stage_order(stage):
    return _STAGE_ORDER.get(stage, 0)



def _supportive_stage_inputs(signal_data):
    ref = signal_data.get('reference_features') or {}
    flow = ref.get('flow') or {}
    micro_ignition = signal_data.get('micro_ignition') or ref.get('micro_ignition') or {}
    buy_ratio = safe_float(flow.get('buy_ratio_recent', ref.get('buy_ratio_recent', 0.0)), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent', ref.get('ofi_recent', 0.0)), 0.0)
    trade_expansion = bool(ref.get('trade_activity_expansion')) or bool(micro_ignition.get('active'))
    breakout = bool(ref.get('local_breakout_clear') or (ref.get('tf15') or {}).get('zone') == 'breakout_zone' or micro_ignition.get('breakout'))
    oi_state = signal_data.get('oi_state', 'NEUTRAL')
    funding_ctx = signal_data.get('funding_context', 'FUNDING_UNINFORMATIVE')
    supportive = {
        'oi_ready': oi_state in ('PREMOVE_BUILDUP', 'BUILDUP_EXPANSION'),
        'oi_strong': oi_state == 'BUILDUP_EXPANSION',
        'flow_ready': buy_ratio >= MIN_BUY_RATIO_FLOW_ACTIONABLE or ofi_recent >= MIN_OFI_FLOW_ACTIONABLE,
        'flow_strong': buy_ratio >= MIN_BUY_RATIO_FLOW_STRONG or ofi_recent >= MIN_OFI_FLOW_STRONG,
        'trade_expansion': trade_expansion,
        'breakout': breakout,
        'funding_ok': funding_ctx in ('FUNDING_QUIET_REGIME', 'FUNDING_NEGATIVE_IMPROVING', 'FUNDING_NON_CROWDED'),
        'flat_oi': oi_state == 'PRICE_UP_OI_FLAT',
    }
    return supportive


def _path_based_stage_override(signal_data, prev_stage=None):
    stage = signal_data.get('signal_stage', 'WATCH')
    ref = signal_data.get('reference_features') or {}
    continuation = signal_data.get('continuation_state') or ref.get('continuation_state') or {}
    exhaustion = signal_data.get('exhaustion_risk') or ref.get('exhaustion_risk') or {}
    acceptance = signal_data.get('acceptance_state') or ref.get('acceptance_state') or {}
    memory = signal_data.get('behavioral_memory') or ref.get('behavioral_memory') or {}

    # التاريخ لا يرقّي المرحلة وحده: فقط يخفض عند فساد البصمة الحالية
    if continuation.get('failed'):
        return 'FAILED_CONTINUATION', 'memory_continuation_failed'
    if exhaustion.get('late'):
        return 'LATE_CHASE', 'memory_late_chase'
    if memory.get('veto'):
        return 'DISCOVERY_ONLY', 'memory_behavioral_veto'
    if acceptance.get('state') == 'REJECTED':
        return 'DISCOVERY_ONLY', 'memory_acceptance_rejected'
    return stage, None


def _stage_upgrade_text_ar(reason, old_stage, new_stage):
    mapping = {
        'memory_continuation_failed': 'المرحلة خُفّضت بسبب فشل الاستمرار الفعلي',
        'memory_late_chase': 'المرحلة تحولت إلى مطاردة متأخرة بسبب البصمة الحالية',
        'memory_behavioral_veto': 'الذاكرة السلوكية منعت التنفيذ الحالي',
        'memory_acceptance_rejected': 'رفض قبول البصمة الحالية أعاد الحالة إلى اكتشاف فقط',
    }
    return mapping.get(reason, f'تعديل مرحلي من {stage_name_ar(old_stage)} إلى {stage_name_ar(new_stage)}')

def evaluate_signal_path_quality(stage_history, current_stage=None):
    history = list(_compress_history(stage_history or []))
    if not history and current_stage:
        history = [current_stage]
    if not history:
        return {
            'path_score': 0,
            'path_quality': 'flat',
            'path_label_ar': 'مسار غير متاح',
            'path_bonus': 0,
            'good_upgrades': 0,
            'bad_regressions': 0,
            'whipsaws': 0,
            'prepare_retests': 0,
            'watch_prepare_cycles': 0,
        }

    upgrades = 0
    regressions = 0
    whipsaws = 0
    hard_resets = 0
    repeated_prepare = 0
    repeated_watch = 0
    prepare_retests = 0
    watch_prepare_cycles = 0

    for i in range(1, len(history)):
        prev_s = history[i - 1]
        cur_s = history[i]
        delta = _stage_order(cur_s) - _stage_order(prev_s)
        if delta > 0:
            upgrades += 1
        elif delta < 0:
            regressions += 1
            if cur_s == 'WATCH':
                hard_resets += 1
        if i >= 2:
            a, b, c = history[i - 2], history[i - 1], history[i]
            if a == c and a != b:
                whipsaws += 1
            if a == 'PREPARE' and b == 'WATCH' and c == 'PREPARE':
                repeated_prepare += 1
                prepare_retests += 1
            if a == 'WATCH' and b == 'PREPARE' and c == 'WATCH':
                repeated_watch += 1
            if set((a, b, c)).issubset({'WATCH', 'PREPARE'}) and ('PREPARE' in (a, b, c)):
                watch_prepare_cycles += 1

    last_stage = history[-1]
    last_rank = _stage_order(last_stage)
    monotonic = regressions == 0 and upgrades > 0
    improving_tail = len(history) >= 2 and _stage_order(history[-1]) >= _stage_order(history[-2])
    human_probe = prepare_retests >= 1 and last_stage in ('PREPARE', 'ARMED', 'TRIGGERED', 'CONFIRMED')

    score = (upgrades * 10) + (last_rank * 6) - (regressions * 12) - (whipsaws * 7) - (hard_resets * 8)
    score += 5 if monotonic else 0
    score += 4 if improving_tail else -2
    score += min(8, prepare_retests * 4)
    score += min(6, watch_prepare_cycles * 2)
    score -= repeated_watch * 2

    if last_stage in ('TRIGGERED', 'CONFIRMED') and regressions == 0:
        quality = 'excellent'
        label = 'مسار متصاعد نظيف'
        bonus = 14
    elif human_probe and last_stage in ('PREPARE', 'ARMED', 'TRIGGERED'):
        quality = 'strong'
        label = 'مسار ضغط بشري تراكمي'
        bonus = 11
    elif monotonic and last_stage in ('ARMED', 'TRIGGERED'):
        quality = 'strong'
        label = 'مسار متصاعد قوي'
        bonus = 10
    elif upgrades >= 1 and regressions == 0:
        quality = 'good'
        label = 'مسار متحسن'
        bonus = 6
    elif human_probe:
        quality = 'good'
        label = 'إعادة اختبار بناءة قبل الانطلاق'
        bonus = 5
    elif regressions >= 1 and upgrades >= 1:
        quality = 'mixed'
        label = 'مسار متذبذب'
        bonus = -4
    elif whipsaws >= 1 or hard_resets >= 1:
        quality = 'weak'
        label = 'مسار متخبط'
        bonus = -10
    else:
        quality = 'flat'
        label = 'مسار مسطح'
        bonus = 0

    if prepare_retests >= 1:
        label += ' | إعادة PREPARE صحية'
    if repeated_watch >= 1 and not human_probe:
        label += ' | WATCH متكرر'

    return {
        'path_score': score,
        'path_quality': quality,
        'path_label_ar': label,
        'path_bonus': bonus,
        'good_upgrades': upgrades,
        'bad_regressions': regressions,
        'whipsaws': whipsaws,
        'prepare_retests': prepare_retests,
        'watch_prepare_cycles': watch_prepare_cycles,
    }


def signal_path_summary_ar(signal_data):
    stage_history = _compress_history(signal_data.get('stage_history') or signal_data.get('stages') or [])
    family_history = _compress_history(signal_data.get('family_history') or signal_data.get('families') or [])

    if not stage_history:
        cur_stage = signal_data.get('signal_stage')
        stage_history = [cur_stage] if cur_stage else []
    if not family_history:
        cur_family = signal_data.get('primary_family')
        family_history = [cur_family] if cur_family else []

    stage_text = ' → '.join(stage_name_ar(s) for s in stage_history if s)
    family_text = ' → '.join(family_name_ar(f) for f in family_history if f)
    path_eval = evaluate_signal_path_quality(stage_history, signal_data.get('signal_stage'))
    path_label = path_eval.get('path_label_ar')

    parts = []
    if stage_text:
        parts.append(stage_text)
    if family_text:
        parts.append(family_text)
    if path_label:
        parts.append(path_label)
    return ' | '.join(parts) if parts else 'غير متاح'


def signal_evolution_summary_ar(signal_data):
    family_change = signal_data.get('family_transition_text', 'ثبات عائلي')
    stage_change = signal_data.get('stage_transition_text', 'ثبات مرحلي')
    execution_trend = signal_data.get('execution_trend', 'stable')
    trend_text = {
        'improving': 'القابلية التنفيذية تتحسن',
        'stable': 'القابلية التنفيذية مستقرة',
        'weakening': 'القابلية التنفيذية تتراجع',
    }.get(execution_trend, 'القابلية التنفيذية مستقرة')
    return f"{family_change} | {stage_change} | {trend_text}"


def execution_judgment_ar(signal_data):
    stage = signal_data.get('signal_stage', 'WATCH')
    acceptance = signal_data.get('acceptance_state') or {}
    continuation = signal_data.get('continuation_state') or {}
    exhaustion = signal_data.get('exhaustion_risk') or {}
    mem = signal_data.get('behavioral_memory') or {}
    micro = signal_data.get('micro_ignition') or {}
    if stage == 'TRIGGERED':
        return 'تنفيذي: TRIGGERED'
    if stage == 'ARMED':
        return 'تنفيذي: ARMED'
    if stage == 'PREPARE':
        return 'تهيئة تنفيذ: PREPARE'
    if stage == 'LATE_CHASE' or exhaustion.get('late'):
        return 'مرفوض تنفيذيًا: late chase'
    if stage == 'FAILED_CONTINUATION' or continuation.get('failed'):
        return 'مرفوض: failure continuation'
    if mem.get('veto'):
        return 'مرفوض: behavioral memory veto'
    if acceptance.get('state') == 'REJECTED':
        return 'مرفوض: acceptance rejected'
    if micro.get('active'):
        return 'اكتشاف فقط مع micro ignition قيد المتابعة'
    return 'اكتشاف فقط'


def execution_priority_score(signal_data):
    """معزول للتشخيص فقط؛ لا يستخدم في القرار التنفيذي."""
    stage = signal_data.get('signal_stage', 'WATCH')
    return {'TRIGGERED': 4, 'ARMED': 3, 'PREPARE': 2, 'WATCH': 1}.get(stage, 0)

TRADING_SYMBOLS_SET = set()
TRADING_SYMBOLS_SET = set()
SPOT_TRADING_SYMBOLS_SET = None
SIGNAL_STABILITY = {}
SIGNAL_STABILITY_LOCK = threading.Lock()
CYCLE_DYNAMIC_CACHE = {}
CYCLE_CONTEXT_CACHE = {}
CYCLE_CACHE_LOCK = threading.Lock()
FAMILY_LOCK_SCORE_MARGIN = 4.0
FAMILY_LOCK_MAX_STAGE_DOWNGRADE = 1
SYMBOL_INTELLIGENCE = {}
SYMBOL_INTELLIGENCE_LOCK = threading.Lock()
OUTCOME_PROFILE_CACHE = {}
OUTCOME_PROFILE_LOCK = threading.Lock()


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _ranked_top_key(counter_dict, default='UNKNOWN'):
    if not isinstance(counter_dict, dict) or not counter_dict:
        return default
    try:
        return sorted(counter_dict.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    except Exception:
        return default


def load_symbol_outcome_profile(symbol, lookback=MEMORY_OUTCOME_LOOKBACK):
    if not ENABLE_HUMAN_MEMORY or not symbol:
        return {
            'success_rate': 0.0, 'avg_move': 0.0, 'family_success': {}, 'family_fail': {},
            'preferred_family': None, 'false_starts': 0, 'evaluated_count': 0,
        }
    now = time.time()
    with OUTCOME_PROFILE_LOCK:
        cached = OUTCOME_PROFILE_CACHE.get(symbol)
        if cached and (now - cached.get('ts', 0)) <= MEMORY_OUTCOME_CACHE_TTL:
            return dict(cached.get('data', {}))
    data = {
        'success_rate': 0.0, 'avg_move': 0.0, 'family_success': {}, 'family_fail': {},
        'preferred_family': None, 'false_starts': 0, 'evaluated_count': 0,
    }
    try:
        if not os.path.exists(DB_PATH):
            return data
        with sqlite3.connect(DB_PATH, timeout=5) as conn:
            rows = conn.execute(
                """
                SELECT primary_family,
                       COALESCE(outcome_15m, outcome_30m, outcome, 0) AS success,
                       COALESCE(price_after_15m, price_after_30m, price_after, 0) AS price_after,
                       price, signal_stage
                FROM signals
                WHERE symbol = ?
                  AND (evaluated_15m = 1 OR evaluated_30m = 1 OR evaluated = 1)
                ORDER BY id DESC
                LIMIT ?
                """,
                (symbol, max(6, lookback))
            ).fetchall()
        if not rows:
            return data
        fam_success = defaultdict(int)
        fam_fail = defaultdict(int)
        moves = []
        success_total = 0
        false_starts = 0
        for family, success, price_after, price, stage in rows:
            family = family or 'UNKNOWN'
            success = _safe_int(success, 0)
            if success:
                fam_success[family] += 1
                success_total += 1
            else:
                fam_fail[family] += 1
                if stage in ('ARMED', 'TRIGGERED', 'CONFIRMED', 'PREPARE'):
                    false_starts += 1
            p0 = safe_float(price, 0.0)
            p1 = safe_float(price_after, 0.0)
            if p0 > 0 and p1 > 0:
                moves.append(((p1 - p0) / p0) * 100.0)
        total = len(rows)
        data = {
            'success_rate': (success_total / total) if total else 0.0,
            'avg_move': (sum(moves) / len(moves)) if moves else 0.0,
            'family_success': dict(fam_success),
            'family_fail': dict(fam_fail),
            'preferred_family': _ranked_top_key(dict(fam_success), None),
            'false_starts': false_starts,
            'evaluated_count': total,
        }
    except Exception:
        pass
    with OUTCOME_PROFILE_LOCK:
        OUTCOME_PROFILE_CACHE[symbol] = {'ts': now, 'data': dict(data)}
    return data


def human_memory_score(signal_data):
    symbol = signal_data.get('symbol')
    family = signal_data.get('primary_family', 'UNKNOWN')
    cycles = int(signal_data.get('persistence_cycles', 1) or 1)
    with SYMBOL_INTELLIGENCE_LOCK:
        memory = dict(SYMBOL_INTELLIGENCE.get(symbol, {}))
    outcome = signal_data.get('outcome_profile') or {}
    score = 0.0
    if cycles >= 2:
        score += min(8.0, cycles * 1.5)
    family_counts = memory.get('family_counts', {}) or {}
    preferred_live = _ranked_top_key(family_counts, None)
    if preferred_live and preferred_live == family:
        score += 3.5
    preferred_outcome = outcome.get('preferred_family')
    if preferred_outcome and preferred_outcome == family:
        score += min(MEMORY_SUCCESS_BONUS_MAX, 4.0 + 1.2 * safe_float(outcome.get('success_rate'), 0.0) * 10.0)
    false_starts = _safe_int(outcome.get('false_starts', 0), 0)
    if false_starts > 0:
        score -= min(MEMORY_FALSESTART_PENALTY_MAX, false_starts * 0.9)
    avg_move = safe_float(outcome.get('avg_move'), 0.0)
    if avg_move > 0:
        score += min(4.5, avg_move / 1.5)
    return round(score, 2)


def memory_summary_ar(signal_data):
    outcome = signal_data.get('outcome_profile') or {}
    success_rate = safe_float(outcome.get('success_rate'), 0.0) * 100.0
    preferred_family = outcome.get('preferred_family')
    false_starts = _safe_int(outcome.get('false_starts', 0), 0)
    total = _safe_int(outcome.get('evaluated_count', 0), 0)
    parts = []
    if preferred_family:
        parts.append(f"أفضلية تاريخية: {family_name_ar(preferred_family)}")
    if total > 0:
        parts.append(f"نجاح تاريخي: {success_rate:.0f}% من {total}")
    if false_starts > 0:
        parts.append(f"بدايات كاذبة: {false_starts}")
    return ' | '.join(parts) if parts else 'ذاكرة قيد البناء'


def _memory_recent_frames(symbol, limit=PRE_IGNITION_MEMORY_BARS):
    with SYMBOL_INTELLIGENCE_LOCK:
        mem = SYMBOL_INTELLIGENCE.get(symbol, {}) if symbol else {}
        frames = list(mem.get('recent_frames', []) or [])
    return frames[-max(2, limit):]


def compute_pre_ignition_pressure(symbol, history=None):
    frames = list(history or _memory_recent_frames(symbol))
    if not frames:
        return {
            'score': 0.0, 'pressure_building': False, 'failed_attempts': 0,
            'watch_prepare_bias': 0, 'stage_bias': 'none', 'reason': 'no_history'
        }
    seq = frames[-PRE_IGNITION_MEMORY_BARS:]
    stage_hist = [str(x.get('stage', 'WATCH')) for x in seq if x]
    price5 = [safe_float(x.get('price_5m'), 0.0) for x in seq]
    oi15 = [safe_float(x.get('oi_15m'), 0.0) for x in seq]
    oinv15 = [safe_float(x.get('oi_nv_15m'), 0.0) for x in seq]
    buy = [safe_float(x.get('buy_ratio'), 0.0) for x in seq]
    trade = [safe_float(x.get('trade_ratio'), 0.0) for x in seq]
    breakouts = sum(1 for x in seq if x.get('breakout'))
    vacuums = sum(1 for x in seq if x.get('vacuum'))
    micro_hits = sum(1 for x in seq if x.get('micro_active'))
    failed_attempts = 0
    watch_prepare_bias = 0
    for i in range(1, len(stage_hist)):
        if stage_hist[i-1] in ('PREPARE', 'ARMED') and stage_hist[i] == 'WATCH':
            failed_attempts += 1
        if stage_hist[i-1] == 'WATCH' and stage_hist[i] == 'PREPARE':
            watch_prepare_bias += 1
    improving_tail = 1 if len(stage_hist) >= 2 and _stage_order(stage_hist[-1]) >= _stage_order(stage_hist[-2]) else 0
    avg_price = mean(price5[-4:]) if price5 else 0.0
    avg_oi = mean(oi15[-4:]) if oi15 else 0.0
    avg_oinv = mean(oinv15[-4:]) if oinv15 else 0.0
    avg_buy = mean(buy[-4:]) if buy else 0.0
    avg_trade = mean(trade[-4:]) if trade else 0.0
    score = 0.0
    score += min(0.18, max(0.0, avg_price) / 6.0)
    score += min(0.18, max(0.0, avg_oi) / 7.0)
    score += min(0.16, max(0.0, avg_oinv) / 8.0)
    score += min(0.14, max(0.0, avg_buy - 0.5) * 0.9)
    score += min(0.12, max(0.0, avg_trade - 1.0) / 4.0)
    score += min(0.10, breakouts * 0.03)
    score += min(0.06, vacuums * 0.02)
    score += min(0.10, micro_hits * 0.03)
    score += min(0.10, watch_prepare_bias * 0.03)
    score += 0.04 if improving_tail else 0.0
    score -= min(0.10, max(0, failed_attempts - 1) * 0.03)
    stage_bias = 'build' if stage_hist and stage_hist[-1] in ('PREPARE', 'ARMED', 'TRIGGERED') else 'watch'
    return {
        'score': round(max(0.0, min(1.0, score)), 4),
        'pressure_building': score >= PRE_IGNITION_MIN_SCORE,
        'failed_attempts': failed_attempts,
        'watch_prepare_bias': watch_prepare_bias,
        'stage_bias': stage_bias,
        'reason': f"avg_price={avg_price:.2f}, avg_oi={avg_oi:.2f}, avg_oinv={avg_oinv:.2f}, watch_prepare={watch_prepare_bias}, micro_hits={micro_hits}"
    }


def classify_price_up_oi_flat_context(ref):
    flow = ref.get('flow') or {}
    liquidity = ref.get('liquidity') or {}
    price_5m = safe_float(flow.get('price_change_5m'), 0.0)
    price_15m = safe_float(flow.get('price_change_15m'), 0.0)
    buy_ratio = safe_float(flow.get('buy_ratio_recent'), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent'), 0.0)
    trade_ratio = safe_float(flow.get('trade_ratio'), 0.0)
    crowded_short = ref.get('ratio_conflict_state') == 'SHORT_SIDE_CROWDING' or bool(ref.get('short_crowd_extreme'))
    breakout = bool(ref.get('local_breakout_clear') or ((ref.get('tf15') or {}).get('zone') == 'breakout_zone'))
    trade_expansion = bool(ref.get('trade_activity_expansion'))
    vacuum = bool(liquidity.get('vacuum'))
    supportive = 0.0
    supportive += 0.18 if crowded_short else 0.0
    supportive += 0.18 if breakout else 0.0
    supportive += 0.16 if trade_expansion else 0.0
    supportive += min(0.16, max(0.0, buy_ratio - 0.5) * 0.9)
    supportive += min(0.14, max(0.0, ofi_recent) * 0.6)
    supportive += min(0.10, max(0.0, trade_ratio - 1.0) / 3.0)
    supportive += 0.08 if vacuum else 0.0
    supportive -= 0.12 if price_5m > FLOW_PRICE_CHASE_HARD_MAX or price_15m > 6.0 else 0.0
    label = 'salvageable' if supportive >= FLAT_OI_CONTEXT_ALLOW_SCORE else 'weak_flat'
    return {
        'label': label,
        'supportive_score': round(max(0.0, min(1.0, supportive)), 4),
        'crowded_short': crowded_short,
        'breakout': breakout,
        'trade_expansion': trade_expansion,
        'vacuum': vacuum,
    }


def detect_short_squeeze_ignition_override(ref, memory=None):
    flow = ref.get('flow') or {}
    top = ref.get('top') or {}
    oi = ref.get('oi') or {}
    buy_ratio = safe_float(flow.get('buy_ratio_recent'), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent'), 0.0)
    trade_ratio = safe_float(flow.get('trade_ratio'), 0.0)
    price_5m = safe_float(flow.get('price_change_5m'), 0.0)
    price_15m = safe_float(flow.get('price_change_15m'), 0.0)
    pos_now = safe_float(top.get('pos_now'), 1.0)
    acc_now = safe_float(top.get('acc_now'), 1.0)
    pos_delta = safe_float(top.get('pos_delta'), 0.0)
    acc_delta = safe_float(top.get('acc_delta'), 0.0)
    oi_5m = safe_float(oi.get('current_delta_5m'), 0.0)
    oi_15m = safe_float(oi.get('delta_15m'), 0.0)
    crowded_short = ref.get('ratio_conflict_state') == 'SHORT_SIDE_CROWDING' or bool(ref.get('short_crowd_extreme'))
    breakout = bool(ref.get('local_breakout_clear') or ((ref.get('tf15') or {}).get('zone') == 'breakout_zone'))
    vacuum = bool((ref.get('liquidity') or {}).get('vacuum'))
    score = 0.0
    score += 0.20 if crowded_short else 0.0
    score += 0.14 if breakout else 0.0
    score += 0.10 if vacuum else 0.0
    score += min(0.12, max(0.0, SHORT_CROWD_RATIO_MAX - safe_float(ref.get('global_ls_ratio_now'), 1.0)) * 1.2)
    score += min(0.12, max(0.0, pos_now - 0.75) / 2.5)
    score += min(0.08, max(0.0, acc_now - 0.70) / 3.0)
    score += min(0.08, max(0.0, pos_delta + 0.01) * 6.0)
    score += min(0.06, max(0.0, acc_delta + 0.01) * 4.0)
    score += min(0.10, max(0.0, oi_15m) / 8.0)
    score += min(0.08, max(0.0, oi_5m) / 4.0)
    score += min(0.10, max(0.0, buy_ratio - 0.5) * 0.8)
    score += min(0.08, max(0.0, ofi_recent) * 0.5)
    score += min(0.08, max(0.0, trade_ratio - 1.0) / 3.0)
    score -= 0.10 if price_5m > FLOW_PRICE_CHASE_HARD_MAX or price_15m > 6.5 else 0.0
    if memory and memory.get('pressure_building'):
        score += 0.05
    return {
        'active': score >= SHORT_SQUEEZE_OVERRIDE_MIN_SCORE and crowded_short and (breakout or vacuum or trade_ratio >= 1.2),
        'score': round(max(0.0, min(1.0, score)), 4),
        'reason': f"crowded={crowded_short}, breakout={breakout}, vacuum={vacuum}, pos_now={pos_now:.2f}, acc_now={acc_now:.2f}"
    }


def detect_prepared_continuation(ref, memory=None):
    flow = ref.get('flow') or {}
    oi = ref.get('oi') or {}
    buy_ratio = safe_float(flow.get('buy_ratio_recent'), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent'), 0.0)
    trade_ratio = safe_float(flow.get('trade_ratio'), 0.0)
    price_5m = safe_float(flow.get('price_change_5m'), 0.0)
    price_15m = safe_float(flow.get('price_change_15m'), 0.0)
    oi_5m = safe_float(oi.get('current_delta_5m'), 0.0)
    oi_15m = safe_float(oi.get('delta_15m'), 0.0)
    breakout = bool(ref.get('local_breakout_clear') or ((ref.get('tf15') or {}).get('zone') == 'breakout_zone'))
    score = 0.0
    score += 0.18 if breakout else 0.0
    score += min(0.15, max(0.0, price_5m) / 6.0)
    score += min(0.15, max(0.0, price_15m) / 8.0)
    score += min(0.12, max(0.0, oi_5m) / 5.0)
    score += min(0.12, max(0.0, oi_15m) / 8.0)
    score += min(0.10, max(0.0, buy_ratio - 0.5) * 0.8)
    score += min(0.08, max(0.0, ofi_recent) * 0.5)
    score += min(0.10, max(0.0, trade_ratio - 1.0) / 4.0)
    score += 0.08 if ref.get('oi_nv_confirmed_expansion') else 0.0
    score += 0.06 if ref.get('oi_confirmed_expansion') else 0.0
    if memory and memory.get('pressure_building'):
        score += 0.06
    if memory and memory.get('watch_prepare_bias', 0) >= 1:
        score += 0.03
    score -= 0.10 if price_5m > FLOW_PRICE_CHASE_HARD_MAX or price_15m > 6.5 else 0.0
    return {
        'active': score >= PREPARED_CONTINUATION_MIN_SCORE and breakout and (buy_ratio >= 0.53 or trade_ratio >= 1.15 or oi_5m > 0),
        'score': round(max(0.0, min(1.0, score)), 4),
        'reason': f"breakout={breakout}, buy_ratio={buy_ratio:.2f}, trade_ratio={trade_ratio:.2f}, oi_15m={oi_15m:.2f}"
    }


def should_force_reenter_universe(symbol, memory=None, burst_metrics=None):
    memory = memory or compute_pre_ignition_pressure(symbol)
    burst_metrics = burst_metrics or {}
    price_5m = safe_float(burst_metrics.get('price_5m'), 0.0)
    price_15m = safe_float(burst_metrics.get('price_15m'), 0.0)
    oi_15m = safe_float(burst_metrics.get('oi_15m'), 0.0)
    trade_ratio = safe_float(burst_metrics.get('trade_ratio'), 0.0)
    vol_ratio = safe_float(burst_metrics.get('vol_ratio'), 0.0)
    rank = 0.0
    rank += 2.4 if memory.get('pressure_building') else 0.0
    rank += min(1.4, memory.get('watch_prepare_bias', 0) * 0.45)
    rank += min(1.0, price_5m * 0.9)
    rank += min(1.0, price_15m * 0.55)
    rank += min(0.8, max(0.0, oi_15m) * 0.18)
    rank += min(0.8, max(0.0, trade_ratio - 1.0) * 0.55)
    rank += min(0.6, max(0.0, vol_ratio - 1.0) * 0.8)
    active = rank >= BURST_REENTRY_MIN_RANK or (memory.get('pressure_building') and (price_5m >= FAST_REENTRY_PRICE_5M or price_15m >= FAST_REENTRY_PRICE_15M))
    return active, round(rank, 3)


def enrich_human_memory(signal_data):
    if not signal_data or not ENABLE_HUMAN_MEMORY:
        return signal_data
    symbol = signal_data.get('symbol')
    outcome = load_symbol_outcome_profile(symbol)
    signal_data['outcome_profile'] = outcome
    signal_data['memory_summary'] = memory_summary_ar(signal_data)
    signal_data['human_memory_score'] = human_memory_score(signal_data)
    return signal_data


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
SPOT_BASE_URL = "https://api.binance.com"
SUPPORTED_FUTURES_SYMBOLS = None
UNIVERSE_STABILITY = {}
UNIVERSE_STABILITY_LOCK = threading.Lock()
HOT_SYMBOL_MEMORY = {}
HOT_SYMBOL_MEMORY_LOCK = threading.Lock()
HTTP_ERROR_COUNTS = defaultdict(int)
HTTP_ERROR_LOCK = threading.Lock()
CYCLE_SNAPSHOT_TS = None

def fetch_json(url, params=None, use_cache=True, weight=1, cache_ttl=None):
    if cache_ttl is None:
        cache_ttl = CACHE_TTL_SECONDS
    cache_key = url + str(sorted(params.items())) if params else url
    if use_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    weight_manager.wait_if_needed(weight)
    session = session_manager.get_session()
    max_retries = MAX_RETRIES
    retry_delay = 1
    symbol = str((params or {}).get('symbol', '')).upper().strip()
    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if use_cache:
                    cache.set(cache_key, data, ttl=cache_ttl)
                if symbol:
                    with HTTP_ERROR_LOCK:
                        HTTP_ERROR_COUNTS.pop(symbol, None)
                return data
            elif resp.status_code in (429, 418):
                retry_after = int(resp.headers.get('Retry-After', retry_delay))
                print(f"⚠️ تجاوز حد الطلبات، انتظار {retry_after} ثانية...")
                time.sleep(retry_after)
                retry_delay *= 2
                continue
            elif resp.status_code == 400:
                if DEBUG:
                    print(f"⚠️ HTTP 400 لـ {url} | symbol={symbol or 'N/A'}")
                if symbol:
                    with HTTP_ERROR_LOCK:
                        HTTP_ERROR_COUNTS[symbol] += 1
                        if HTTP_ERROR_COUNTS[symbol] >= HTTP_400_BAN_CYCLES:
                            mark_symbol_invalid(symbol)
                return None
            else:
                if DEBUG:
                    print(f"⚠️ HTTP {resp.status_code} لـ {url} | symbol={symbol or 'N/A'}")
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


def get_supported_futures_symbols():
    global SUPPORTED_FUTURES_SYMBOLS
    if SUPPORTED_FUTURES_SYMBOLS is not None:
        return SUPPORTED_FUTURES_SYMBOLS
    data = fetch_json(f"{BASE_URL}/fapi/v1/exchangeInfo", use_cache=True, weight=10, cache_ttl=3600)
    symbols = set()
    if data and isinstance(data, dict):
        for s in data.get('symbols', []):
            try:
                if s.get('status') == 'TRADING':
                    symbols.add(str(s.get('symbol', '')).upper())
            except Exception:
                continue
    SUPPORTED_FUTURES_SYMBOLS = symbols
    return SUPPORTED_FUTURES_SYMBOLS


def resolve_symbol_market_type(symbol):
    symbol = str(symbol).upper().strip()
    if not symbol:
        return 'unsupported'
    if symbol in get_supported_futures_symbols():
        if '_' in symbol:
            return 'delivery'
        return 'usd_m_futures'
    spot_symbols = get_spot_trading_symbols() if ENABLE_SPOT_CONFIRMATION else set()
    if spot_symbols and symbol in spot_symbols:
        return 'spot'
    return 'unsupported'


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


def get_global_ls_ratio_snapshot(symbol, period='5m'):
    """جلب Global Long/Short Account Ratio كـ L.S Ratio مرجعي مستقل."""
    data = fetch_json(
        f"{BASE_URL}/futures/data/globalLongShortAccountRatio",
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


def get_global_ls_ratio_history(symbol, period='5m', limit=12):
    cache_key = f"global_ls_ratio_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(
        f"{BASE_URL}/futures/data/globalLongShortAccountRatio",
        {'symbol': symbol, 'period': period, 'limit': limit},
        use_cache=False, weight=1
    )
    if data:
        ratios = [float(d['longShortRatio']) for d in data][::-1]
        cache.set(cache_key, ratios, ttl=HISTORIC_CACHE_TTL)
        return ratios
    return []


def get_klines(symbol, interval=None, limit=None):
    symbol = str(symbol).upper().strip()
    if interval is None:
        interval = PRIMARY_TIMEFRAME
    if limit is None:
        limit = NUMBER_OF_CANDLES
    if not symbol or is_symbol_invalid(symbol):
        return None

    market_type = resolve_symbol_market_type(symbol)
    if market_type == 'unsupported':
        mark_symbol_invalid(symbol)
        return None

    weight = max(1, limit // 100)
    cache_key = f"klines_{market_type}_{symbol}_{interval}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    if market_type in ('usd_m_futures', 'delivery'):
        endpoint = f"{BASE_URL}/fapi/v1/klines"
    elif market_type == 'spot':
        endpoint = f"{SPOT_BASE_URL}/api/v3/klines"
    else:
        return None

    data = fetch_json(endpoint, {'symbol': symbol, 'interval': interval, 'limit': limit}, use_cache=False, weight=weight)
    if data:
        result = {
            'open': [float(k[1]) for k in data],
            'high': [float(k[2]) for k in data],
            'low': [float(k[3]) for k in data],
            'close': [float(k[4]) for k in data],
            'volume': [float(k[5]) for k in data],
            'quote_volume': [float(k[7]) for k in data],
            'trades': [float(k[8]) for k in data],
            'taker_buy_volume': [float(k[9]) for k in data]
        }
        cache.set(cache_key, result, ttl=30)
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

def classify_oi_price_state(price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0):
    """تصنيف حالة OI بالنسبة للسعر: الفصل الحاسم بين build-up وcovering،
    مع السماح بحالات squeeze المبكرة عندما يبدأ OI بالانعطاف من قاعدة قصيرة مزدحمة."""
    price_push = max(price_5m, price_15m)
    oi_push = max(oi_5m, oi_15m)
    oi_drop = min(oi_5m, oi_15m)

    if price_push >= 0.45 and oi_drop <= -0.35 and oi_push < SHORT_SQUEEZE_MIN_OI_5M:
        return 'COVERING'
    if price_push >= 1.20 and abs(oi_15m) <= 0.35 and oi_push < SHORT_SQUEEZE_MIN_OI_5M:
        return 'PRICE_UP_OI_FLAT'
    if abs(price_15m) <= 0.65 and oi_push >= 1.00:
        return 'PREMOVE_BUILDUP'
    if price_push >= 0.35 and oi_push >= 0.80:
        return 'BUILDUP_EXPANSION'
    if price_push >= SHORT_SQUEEZE_MIN_PRICE_15M and (oi_5m >= SHORT_SQUEEZE_MIN_OI_5M or oi_15m >= SHORT_SQUEEZE_MIN_OI_15M):
        return 'BUILDUP_EXPANSION'
    if oi_push >= 0.60 and abs(price_15m) <= 0.45:
        return 'PREMOVE_BUILDUP'
    return 'NEUTRAL'


def classify_signal_stage(price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0, funding_rate=None, buy_ratio=0.0, score=0.0):
    """Fallback stage classifier for legacy callers.
    التصنيف التنفيذي الحقيقي يجب أن يُبنى لاحقًا من البصمة الكاملة داخل classify_signal_stage_from_reference.
    """
    oi_state = classify_oi_price_state(price_5m=price_5m, price_15m=price_15m, oi_5m=oi_5m, oi_15m=oi_15m)
    funding_abs = abs(funding_rate) if funding_rate is not None else 0.0
    if oi_state == 'COVERING' or price_5m >= 3.8 or price_15m >= 8.5 or funding_abs >= 0.08:
        return 'LATE_CHASE'
    if oi_state == 'BUILDUP_EXPANSION' and buy_ratio >= 0.58 and 0.4 <= price_15m <= 3.5:
        return 'TRIGGERED'
    if oi_state == 'PREMOVE_BUILDUP' and buy_ratio >= 0.54:
        return 'ARMED'
    if oi_state == 'PREMOVE_BUILDUP':
        return 'PREPARE'
    return 'DISCOVERY_ONLY'


def _stage_rank(stage):
    order = {'REJECTED': -3, 'FAILED_CONTINUATION': -2, 'LATE_CHASE': -1, 'DISCOVERY_ONLY': 0, 'WATCH': 1, 'PREPARE': 2, 'ARMED': 3, 'TRIGGERED': 4}
    return order.get(stage, 0)


def _apply_family_hysteresis(symbol, candidates, best_candidate):
    """Family lock لا يتغلب على فساد البصمة الحالية."""
    if not symbol or not candidates or not best_candidate:
        return best_candidate
    with SIGNAL_STABILITY_LOCK:
        prev = SIGNAL_STABILITY.get(symbol)
    if not prev:
        return best_candidate
    prev_family = prev.get('family')
    if not prev_family or prev_family == best_candidate.get('family'):
        return best_candidate
    prev_candidate = next((c for c in candidates if c.get('family') == prev_family), None)
    if not prev_candidate:
        return best_candidate

    prev_ref = prev_candidate.get('_reference_ref', {})
    prev_stage, _ = classify_signal_stage_from_reference(prev_ref, family_hint=prev_family)
    best_ref = best_candidate.get('_reference_ref', {})
    best_stage, _ = classify_signal_stage_from_reference(best_ref, family_hint=best_candidate.get('family'))

    prev_failure = (prev_ref.get('continuation_state') or {}).get('failed') or (prev_ref.get('exhaustion_risk') or {}).get('risky')
    if prev_failure:
        return best_candidate
    if _stage_rank(best_stage) > _stage_rank(prev_stage):
        return best_candidate

    locked = dict(prev_candidate)
    locked['why_selected'] = (locked.get('why_selected', '') + ' | family lock حافظ على العائلة بدون تجاوز فشل البصمة الحالية').strip(' |')
    return locked


def detect_acceptance_state(ref, oi_state=None, family_hint=None):
    flow = ref.get('flow') or {}
    oi = ref.get('oi') or {}
    oi_state = oi_state or classify_oi_price_state(
        price_5m=safe_float(flow.get('price_change_5m'), 0.0),
        price_15m=safe_float(flow.get('price_change_15m'), 0.0),
        oi_5m=safe_float(oi.get('current_delta_5m'), 0.0),
        oi_15m=safe_float(oi.get('delta_15m'), 0.0),
    )
    micro_state = detect_micro_ignition_state(ref)
    squeeze_override = ref.get('short_squeeze_override') or detect_short_squeeze_ignition_override(ref, ref.get('pre_ignition_pressure'))
    trade_expansion = bool(ref.get('trade_activity_expansion'))
    breakout = bool(ref.get('local_breakout_clear') or ((ref.get('tf15') or {}).get('zone') == 'breakout_zone') or micro_state.get('breakout'))
    buy_ratio = safe_float(flow.get('buy_ratio_recent'), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent'), 0.0)

    if oi_state == 'PRICE_UP_OI_FLAT':
        if micro_state.get('active') or squeeze_override.get('active'):
            return {'state': 'ACCEPTED_EXCEPTION', 'confidence': 'narrow', 'reason': 'flat_oi_exception_micro_or_squeeze'}
        return {'state': 'REJECTED', 'confidence': 'hard', 'reason': 'flat_oi_default_reject'}

    if oi_state == 'COVERING':
        return {'state': 'REJECTED', 'confidence': 'hard', 'reason': 'covering_state'}

    if oi_state in ('PREMOVE_BUILDUP', 'BUILDUP_EXPANSION') and (trade_expansion or breakout or buy_ratio >= 0.53 or ofi_recent >= 0.08):
        return {'state': 'CONFIRMED', 'confidence': 'strong', 'reason': 'oi_flow_accept'}

    if micro_state.get('active') and breakout:
        return {'state': 'CONFIRMED', 'confidence': 'strong', 'reason': 'micro_ignition_accept'}

    return {'state': 'WEAK', 'confidence': 'weak', 'reason': 'insufficient_acceptance'}


def detect_micro_ignition_state(ref):
    micro = ref.get('micro_ignition') or detect_micro_ignition(ref.get('symbol'), ref)
    return {
        'active': bool(micro.get('active')),
        'pre_break': bool(micro.get('pre_break')),
        'breakout': bool(micro.get('breakout')),
        'reason': micro.get('reason', ''),
    }


def detect_failure_continuation(ref, family_hint=None, oi_state=None):
    flow = ref.get('flow') or {}
    oi_state = oi_state or ref.get('oi_state', 'NEUTRAL')
    buy_ratio = safe_float(flow.get('buy_ratio_recent'), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent'), 0.0)
    trade_ratio = safe_float(flow.get('trade_ratio'), 1.0)
    trade_expansion = bool(ref.get('trade_activity_expansion'))
    breakout = bool(ref.get('local_breakout_clear') or ((ref.get('tf15') or {}).get('zone') == 'breakout_zone'))
    reasons = []
    failed = False

    if oi_state == 'COVERING':
        failed = True
        reasons.append('oi_covering')
    if buy_ratio < 0.50 and ofi_recent < 0.03 and trade_ratio < 1.0:
        failed = True
        reasons.append('flow_decay')
    if family_hint == 'FLOW_LIQUIDITY_VACUUM_BREAKOUT' and not (trade_expansion and breakout):
        failed = True
        reasons.append('flow_breakout_lost')
    if family_hint in ('CONSENSUS_BULLISH_EXPANSION', 'ACCOUNT_LED_ACCUMULATION') and not ref.get('oi_nv_confirmed_expansion'):
        reasons.append('oi_nv_weak')

    return {'failed': failed, 'reason': '|'.join(reasons) if reasons else 'continuation_ok'}


def detect_exhaustion_risk(ref, oi_state=None):
    flow = ref.get('flow') or {}
    oi_state = oi_state or ref.get('oi_state', 'NEUTRAL')
    price_5m = safe_float(flow.get('price_change_5m'), 0.0)
    price_15m = safe_float(flow.get('price_change_15m'), 0.0)
    buy_ratio = safe_float(flow.get('buy_ratio_recent'), 0.0)
    ofi_recent = safe_float(flow.get('ofi_recent'), 0.0)
    late = bool(ref.get('late_or_chased')) or price_5m > FLOW_PRICE_CHASE_HARD_MAX or price_15m > 6.0
    overheat = price_15m > PRETRIGGER_MAX_PRICE_15M and buy_ratio < 0.54 and ofi_recent < 0.08
    risky = late or overheat or oi_state == 'PRICE_UP_OI_FLAT'
    if oi_state == 'PRICE_UP_OI_FLAT' and (ref.get('micro_ignition') or {}).get('active'):
        risky = False
    return {'risky': bool(risky), 'late': bool(late), 'reason': 'late_or_overheat' if risky else 'risk_ok'}


def behavioral_memory_veto(symbol, ref, stage=None, family=None):
    if not ENABLE_HUMAN_MEMORY or not symbol:
        return {'veto': False, 'reason': 'memory_disabled'}
    profile = load_symbol_outcome_profile(symbol)
    false_starts = int(profile.get('false_starts', 0) or 0)
    success_rate = safe_float(profile.get('success_rate'), 0.0)
    evaluated = int(profile.get('evaluated_count', 0) or 0)
    if evaluated >= 4 and false_starts >= 3 and success_rate < 0.35 and stage in ('PREPARE', 'ARMED'):
        return {'veto': True, 'reason': 'historical_false_starts'}
    return {'veto': False, 'reason': 'memory_ok'}


def fingerprint_quality_rank(ref, family, stage):
    flow = ref.get('flow') or {}
    micro = ref.get('micro_ignition') or {}
    acceptance = ref.get('acceptance_state') or {}
    continuation = ref.get('continuation_state') or {}
    exhaustion = ref.get('exhaustion_risk') or {}
    return (
        1 if stage == 'TRIGGERED' else 0,
        1 if stage == 'ARMED' else 0,
        1 if stage == 'PREPARE' else 0,
        1 if acceptance.get('state') in ('CONFIRMED', 'ACCEPTED_EXCEPTION') else 0,
        0 if continuation.get('failed') else 1,
        0 if exhaustion.get('risky') else 1,
        1 if micro.get('active') else 0,
        1 if ref.get('oi_nv_confirmed_expansion') else 0,
        1 if ref.get('oi_confirmed_expansion') else 0,
        1 if ref.get('trade_activity_expansion') else 0,
        1 if ref.get('local_breakout_clear') else 0,
        round(safe_float(flow.get('buy_ratio_recent'), 0.0), 4),
        round(safe_float(flow.get('ofi_recent'), 0.0), 4),
        round(safe_float(flow.get('trade_ratio'), 1.0), 4),
    )


def classify_signal_stage_from_reference(ref, family_hint=None):
    """تصنيف مرحلي مبني فقط على البصمة الحالية (Discovery vs Actionability)."""
    flow = ref.get('flow') or {}
    oi = ref.get('oi') or {}
    symbol = ref.get('symbol')

    ref['micro_ignition'] = ref.get('micro_ignition') or detect_micro_ignition(symbol, ref) or {}
    ref['pre_ignition_pressure'] = ref.get('pre_ignition_pressure') or compute_pre_ignition_pressure(symbol)
    ref['short_squeeze_override'] = ref.get('short_squeeze_override') or detect_short_squeeze_ignition_override(ref, ref.get('pre_ignition_pressure'))
    ref['prepared_continuation'] = ref.get('prepared_continuation') or detect_prepared_continuation(ref, ref.get('pre_ignition_pressure'))
    ref['flat_oi_context'] = classify_price_up_oi_flat_context(ref)

    oi_state = classify_oi_price_state(
        price_5m=safe_float(flow.get('price_change_5m'), 0.0),
        price_15m=safe_float(flow.get('price_change_15m'), 0.0),
        oi_5m=safe_float(oi.get('current_delta_5m'), 0.0),
        oi_15m=safe_float(oi.get('delta_15m'), 0.0),
    )
    ref['oi_state'] = oi_state

    acceptance = detect_acceptance_state(ref, oi_state=oi_state, family_hint=family_hint)
    continuation = detect_failure_continuation(ref, family_hint=family_hint, oi_state=oi_state)
    exhaustion = detect_exhaustion_risk(ref, oi_state=oi_state)
    micro_state = detect_micro_ignition_state(ref)
    memory_veto = behavioral_memory_veto(symbol, ref, family=family_hint)
    ref['acceptance_state'] = acceptance
    ref['continuation_state'] = continuation
    ref['exhaustion_risk'] = exhaustion
    ref['micro_ignition_state'] = micro_state
    ref['behavioral_memory'] = memory_veto

    breakout = bool(ref.get('local_breakout_clear') or ((ref.get('tf15') or {}).get('zone') == 'breakout_zone') or micro_state.get('breakout'))
    trade_expansion = bool(ref.get('trade_activity_expansion'))
    flow_ready = safe_float(flow.get('buy_ratio_recent'), 0.0) >= MIN_BUY_RATIO_FLOW_ACTIONABLE or safe_float(flow.get('ofi_recent'), 0.0) >= MIN_OFI_FLOW_ACTIONABLE

    if continuation.get('failed'):
        return 'FAILED_CONTINUATION', oi_state
    if acceptance.get('state') == 'REJECTED':
        return 'DISCOVERY_ONLY', oi_state
    if exhaustion.get('late'):
        return 'LATE_CHASE', oi_state
    if memory_veto.get('veto'):
        return 'DISCOVERY_ONLY', oi_state
    if acceptance.get('state') in ('CONFIRMED', 'ACCEPTED_EXCEPTION') and breakout and (micro_state.get('active') or (trade_expansion and flow_ready)):
        return 'TRIGGERED', oi_state
    if acceptance.get('state') in ('CONFIRMED', 'ACCEPTED_EXCEPTION') and (trade_expansion or breakout or micro_state.get('pre_break')):
        return 'ARMED', oi_state
    if acceptance.get('state') in ('CONFIRMED', 'WEAK') and (flow_ready or ref.get('prepared_continuation', {}).get('active')):
        return 'PREPARE', oi_state
    return 'WATCH', oi_state

def attach_signal_stage(signal_data, price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0, funding_rate=None, buy_ratio=0.0):
    if not signal_data:
        return signal_data
    signal_data['signal_stage'] = classify_signal_stage(price_5m=price_5m, price_15m=price_15m, oi_5m=oi_5m, oi_15m=oi_15m, funding_rate=funding_rate, buy_ratio=buy_ratio, score=signal_data.get('score', 0))
    signal_data['oi_state'] = classify_oi_price_state(price_5m=price_5m, price_15m=price_15m, oi_5m=oi_5m, oi_15m=oi_15m)
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


def get_spot_trading_symbols():
    global SPOT_TRADING_SYMBOLS_SET
    if SPOT_TRADING_SYMBOLS_SET is not None:
        return SPOT_TRADING_SYMBOLS_SET
    data = fetch_json(f"{SPOT_BASE_URL}/api/v3/exchangeInfo", use_cache=True, weight=10, cache_ttl=3600)
    symbols = set()
    if data and isinstance(data, dict):
        for item in data.get('symbols', []):
            try:
                if item.get('status') == 'TRADING':
                    symbols.add(str(item.get('symbol', '')).upper())
            except Exception:
                continue
    SPOT_TRADING_SYMBOLS_SET = symbols
    return SPOT_TRADING_SYMBOLS_SET


def get_spot_klines(symbol, interval='5m', limit=24):
    if not ENABLE_SPOT_CONFIRMATION:
        return None
    symbol = str(symbol).upper().strip()
    if '_' in symbol:
        return None
    spot_symbols = get_spot_trading_symbols()
    if not spot_symbols or symbol not in spot_symbols:
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


# [REMOVED_LEGACY] _get_calibration_weights removed from executable decision/report path.


# [REMOVED_LEGACY] compute_signal_quality removed from executable decision/report path.


# [REMOVED_LEGACY] classify_confidence removed from executable decision/report path.


def build_market_context_features(symbol):
    cache_key = (CYCLE_SNAPSHOT_TS, symbol)
    with CYCLE_CACHE_LOCK:
        cached = CYCLE_CONTEXT_CACHE.get(cache_key)
    if cached is not None:
        return cached

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
    result = {
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
    with CYCLE_CACHE_LOCK:
        CYCLE_CONTEXT_CACHE[cache_key] = result
    return result


# [REMOVED_LEGACY] compute_pump_preparation_score removed from executable decision/report path.


# [REMOVED_LEGACY] classify_pump_type removed from executable decision/report path.


# [REMOVED_LEGACY] build_institutional_signal removed from executable decision/report path.

# [REMOVED_LEGACY] detect_long_term_accumulation removed from executable decision/report path.

# =============================================================================
# تقييم القوة مع دمج HMM وإدارة المخاطر
# =============================================================================
# [REMOVED_LEGACY] assess_signal_power removed from executable decision/report path.

def calculate_risk_management(symbol, price, direction):
    """
    حساب وقف الخسارة وأهداف الربح بناءً على ATR.
    يمكن تعطيله بالكامل عند تشغيل النظام ككاشف مبكر فقط.
    """
    if not ENABLE_RISK_SYSTEM:
        return 0.0, 0.0, 0.0
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
# نظام التعلم الذاتي للعائلات الأربع فقط
# =============================================================================
class LearningSystem:
    def __init__(self, db):
        self.db = db
        self.last_learning_time = datetime.now()
        self.family_weights = FAMILY_WEIGHTS.copy()
        self.family_performance = defaultdict(lambda: {'total': 0, 'success': 0, 'score_sum': 0})

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
                sig_id, symbol, family, price, direction, ts = sig
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
                    self.family_performance[family]['total'] += 1
                    self.family_performance[family]['success'] += success
                    self.family_performance[family]['score_sum'] += abs(change)
                total_evaluated += 1
        return total_evaluated

    def adjust_weights(self):
        if not ENABLE_LEARNING:
            return

        self.evaluate_signals()

        total_weight = 0
        new_weights = {}
        for family, stats in self.family_performance.items():
            if stats['total'] < MIN_SIGNALS_FOR_LEARNING:
                continue
            accuracy = stats['success'] / stats['total']
            if accuracy > PERFORMANCE_THRESHOLD:
                old_weight = self.family_weights.get(family, 0.2)
                new_weight = old_weight * (1 + WEIGHT_ADJUSTMENT_RATE * (accuracy - PERFORMANCE_THRESHOLD))
                new_weight = min(new_weight, 1.0)
            else:
                old_weight = self.family_weights.get(family, 0.2)
                new_weight = old_weight * (1 - WEIGHT_ADJUSTMENT_RATE * (PERFORMANCE_THRESHOLD - accuracy))
                new_weight = max(new_weight, 0.05)
            new_weights[family] = new_weight
            total_weight += new_weight

        if total_weight > 0:
            for family in new_weights:
                new_weights[family] /= total_weight
            self.family_weights.update(new_weights)
            self.save_weights()

    def save_weights(self):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
            config['FAMILY_WEIGHTS'] = self.family_weights
            config['PATTERN_WEIGHTS'] = self.family_weights  # mirror للتوافق مع ملفات config الأقدم
            with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ خطأ في حفظ الأوزان: {e}")

# =============================================================================
# الفلترة الأولية (سريعة) مع تحسين السيولة والارتفاعات المفاجئة
# =============================================================================
def compute_dynamic_opportunity_metrics(symbol):
    """مقاييس اكتشاف دورية قصيرة الأجل بدل الاعتماد على فلتر يومي جامد."""
    cache_key = (CYCLE_SNAPSHOT_TS, symbol)
    with CYCLE_CACHE_LOCK:
        cached = CYCLE_DYNAMIC_CACHE.get(cache_key)
    if cached is not None:
        return cached

    kl = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=max(DYNAMIC_BASE_LOOKBACK_BARS + 4, 10))
    if not kl or len(kl.get("close", [])) < max(DYNAMIC_BASE_LOOKBACK_BARS + 2, 8):
        return None

    closes = kl.get('close', [])
    highs = kl.get('high', [])
    quote_vols = kl.get('quote_volume', [])
    trades = kl.get('trades', [])
    base_n = min(DYNAMIC_BASE_LOOKBACK_BARS, len(closes) - 2)
    if base_n < 3:
        return None

    current_close = safe_float(closes[-1], 0.0)
    prev_close = safe_float(closes[-2], current_close)
    base_close = safe_float(closes[-1 - base_n], prev_close)
    price_5m = pct_change(prev_close, current_close)
    price_base = pct_change(base_close, current_close)

    recent_quote = safe_float(quote_vols[-1], 0.0)
    base_quote_slice = [safe_float(x, 0.0) for x in quote_vols[-1 - base_n:-1]]
    base_quote_mean = rolling_mean(base_quote_slice)
    vol_ratio = (recent_quote / base_quote_mean) if base_quote_mean > 0 else (2.0 if recent_quote > 0 else 0.0)

    recent_trades = safe_float(trades[-1], 0.0)
    base_trades_slice = [safe_float(x, 0.0) for x in trades[-1 - base_n:-1]]
    base_trades_mean = rolling_mean(base_trades_slice)
    trade_ratio = (recent_trades / base_trades_mean) if base_trades_mean > 0 else (2.0 if recent_trades > 0 else 0.0)

    prev_highs = [safe_float(x, 0.0) for x in highs[-1 - base_n:-1]]
    breakout = bool(prev_highs) and current_close >= (max(prev_highs) * (1.0 + DYNAMIC_BREAKOUT_BUFFER_PCT))

    oi_hist = get_oi_history(symbol, period='5m', limit=max(DYNAMIC_BASE_LOOKBACK_BARS + 4, 10))
    oi_5m = pct_change(oi_hist[-2], oi_hist[-1]) if len(oi_hist) >= 2 else 0.0
    oi_15m = pct_change(oi_hist[-4], oi_hist[-1]) if len(oi_hist) >= 4 else oi_5m

    oi_nv_hist = get_oi_notional_history(symbol, period='5m', limit=max(DYNAMIC_BASE_LOOKBACK_BARS + 4, 10))
    oi_nv_5m = pct_change(oi_nv_hist[-2], oi_nv_hist[-1]) if len(oi_nv_hist) >= 2 else 0.0
    oi_nv_15m = pct_change(oi_nv_hist[-4], oi_nv_hist[-1]) if len(oi_nv_hist) >= 4 else oi_nv_5m

    momentum_ok = (
        (price_5m >= DYNAMIC_MIN_PRICE_5M and vol_ratio >= DYNAMIC_MIN_VOL_RATIO) or
        (price_base >= DYNAMIC_MIN_PRICE_15M and (vol_ratio >= 1.0 or trade_ratio >= DYNAMIC_MIN_TRADE_RATIO))
    )
    oi_drive_ok = (
        oi_5m >= DYNAMIC_MIN_OI_5M or
        oi_15m >= DYNAMIC_MIN_OI_15M or
        oi_nv_5m >= DYNAMIC_MIN_OINV_5M or
        oi_nv_15m >= DYNAMIC_MIN_OINV_15M
    )
    flow_burst_ok = (
        recent_quote >= DYNAMIC_MIN_RECENT_QUOTEVOL and
        recent_trades >= DYNAMIC_MIN_RECENT_TRADES and
        (vol_ratio >= DYNAMIC_MIN_VOL_RATIO or trade_ratio >= DYNAMIC_MIN_TRADE_RATIO)
    )
    dynamic_ok = (breakout and (flow_burst_ok or oi_drive_ok)) or (momentum_ok and oi_drive_ok) or (flow_burst_ok and price_base >= 0.18)

    result = {
        'price_5m': price_5m,
        'price_base': price_base,
        'recent_quote': recent_quote,
        'recent_trades': recent_trades,
        'vol_ratio': vol_ratio,
        'trade_ratio': trade_ratio,
        'oi_5m': oi_5m,
        'oi_15m': oi_15m,
        'oi_nv_5m': oi_nv_5m,
        'oi_nv_15m': oi_nv_15m,
        'breakout': breakout,
        'momentum_ok': momentum_ok,
        'oi_drive_ok': oi_drive_ok,
        'flow_burst_ok': flow_burst_ok,
        'dynamic_ok': dynamic_ok,
    }
    with CYCLE_CACHE_LOCK:
        CYCLE_DYNAMIC_CACHE[cache_key] = result
    return result


def quick_filter():
    print("🔍 جلب بيانات السوق الأولية...")
    tickers = get_ticker_24hr()
    if not tickers:
        print("❌ فشل جلب tickers")
        return []

    valid_symbol_pattern = re.compile(r'^[A-Z0-9_]+$')
    current_seen = set()
    lightweight_pool = []

    for t in tickers:
        symbol = str(t.get('symbol', '')).upper().strip()
        if not symbol:
            continue
        if not valid_symbol_pattern.match(symbol):
            if DEBUG:
                print(f"⚠️ استبعاد رمز غير لاتيني: {symbol}")
            continue
        if TRADING_SYMBOLS_SET and symbol not in TRADING_SYMBOLS_SET:
            continue
        if is_symbol_invalid(symbol):
            continue
        if resolve_symbol_market_type(symbol) not in ('usd_m_futures', 'delivery'):
            continue

        current_seen.add(symbol)
        volume = safe_float(t.get('quoteVolume', 0.0), 0.0)
        price_change_24h = safe_float(t.get('priceChangePercent', 0.0), 0.0)
        count = int(safe_float(t.get('count', 0), 0))
        last_price = safe_float(t.get('lastPrice', 0.0), 0.0)

        if volume < DYNAMIC_SAFETY_MIN_VOLUME_USD or count < DYNAMIC_SAFETY_MIN_24H_TRADES:
            if DEBUG:
                print(f"⚠️ استبعاد {symbol} بسبب سلامة دنيا ضعيفة: volume={volume:.0f}, trades={count}")
            continue

        with HOT_SYMBOL_MEMORY_LOCK:
            hot_ttl = int(HOT_SYMBOL_MEMORY.get(symbol, {'ttl': 0}).get('ttl', 0))
        with UNIVERSE_STABILITY_LOCK:
            prev_hits = int(UNIVERSE_STABILITY.get(symbol, {'hits': 0}).get('hits', 0))

        memory = compute_pre_ignition_pressure(symbol)
        qv_score = math.log10(max(volume, 1.0))
        micro_score = min(4.0, count / 1200.0)
        fast_score = max(0.0, price_change_24h)
        memory_score = safe_float(memory.get('score'), 0.0) * 3.0
        early_ok = (
            (volume >= MIN_VOLUME_USD * 0.85 and count >= max(350, EARLY_BASELINE_COUNT_MIN // 2)) or
            (price_change_24h >= max(0.7, MIN_PRICE_CHANGE_24H * 0.45) and count >= 250) or
            hot_ttl > 0 or
            prev_hits >= 1 or
            memory.get('pressure_building')
        )
        if not early_ok:
            continue

        pre_rank = (
            qv_score * 1.7 +
            micro_score * 1.2 +
            fast_score * 1.4 +
            memory_score +
            (2.5 if hot_ttl > 0 else 0.0) +
            min(2.5, prev_hits * 0.6) +
            (0.8 if last_price > 0 else 0.0)
        )
        lightweight_pool.append((symbol, volume, price_change_24h, count, prev_hits, hot_ttl, pre_rank, memory))

    lightweight_pool.sort(key=lambda x: (x[6], x[4], x[2], x[1], x[3]), reverse=True)
    shortlist_limit = max(int(MAX_SYMBOLS_PER_CYCLE * 1.45), MAX_SYMBOLS_PER_CYCLE + int(UNIVERSE_STABILITY_BUFFER) + 18)
    shortlist = lightweight_pool[:shortlist_limit]

    candidates = []

    def _enrich_candidate(row):
        symbol, volume, price_change_24h, count, prev_hits, hot_ttl, pre_rank, memory = row
        dynamic_metrics = compute_dynamic_opportunity_metrics(symbol)
        if not dynamic_metrics:
            return None

        avg_vol = get_historical_avg_volume(symbol, days=7)
        vol_ratio = (volume / avg_vol) if (avg_vol is not None and avg_vol > 0) else max(1.0, safe_float(dynamic_metrics.get('vol_ratio'), 1.0))
        vol_ok = vol_ratio >= VOLUME_TO_AVG_RATIO_MIN if avg_vol else True

        mover_ok = price_change_24h >= MIN_PRICE_CHANGE_24H
        fast_mover = price_change_24h >= FAST_MOVE_24H_THRESHOLD
        microstructure_ok = count >= 500 or vol_ratio >= 1.08 or price_change_24h >= 0.8
        early_baseline = (
            volume >= (MIN_VOLUME_USD * EARLY_BASELINE_VOLUME_MULT) and
            count >= EARLY_BASELINE_COUNT_MIN
        )
        intraday_fast_mover = (
            count >= FAST_MOVER_INTRADAY_COUNT_MIN and
            vol_ratio >= FAST_MOVER_INTRADAY_VOL_RATIO_MIN
        )
        dynamic_hot = bool(dynamic_metrics.get('dynamic_ok'))
        force_reenter, reentry_rank = should_force_reenter_universe(symbol, memory=memory, burst_metrics=dynamic_metrics)
        warm_candidate = (
            dynamic_hot or hot_ttl > 0 or force_reenter or fast_mover or intraday_fast_mover or
            (vol_ok and mover_ok and microstructure_ok) or
            (vol_ratio >= 1.18 and count >= 350) or
            early_baseline or memory.get('pressure_building')
        )
        if not warm_candidate:
            return None

        with UNIVERSE_STABILITY_LOCK:
            rec = UNIVERSE_STABILITY.get(symbol, {'hits': 0, 'misses': 0, 'last_rank': 0})
            rec['hits'] = int(rec.get('hits', 0)) + 1
            rec['misses'] = 0
            rec['last_rank'] = pre_rank
            UNIVERSE_STABILITY[symbol] = rec
            hits = rec['hits']

        if dynamic_hot or force_reenter or memory.get('pressure_building'):
            with HOT_SYMBOL_MEMORY_LOCK:
                HOT_SYMBOL_MEMORY[symbol] = {'ttl': max(DYNAMIC_KEEP_HOT_CYCLES, BURST_REENTRY_TTL), 'ts': time.time()}

        dynamic_rank = (
            pre_rank +
            safe_float(dynamic_metrics.get('price_base'), 0.0) * 2.2 +
            safe_float(dynamic_metrics.get('price_5m'), 0.0) * 2.8 +
            safe_float(dynamic_metrics.get('oi_15m'), 0.0) * 1.6 +
            safe_float(dynamic_metrics.get('oi_nv_15m'), 0.0) * 1.8 +
            min(4.0, safe_float(dynamic_metrics.get('vol_ratio'), 0.0)) +
            min(3.0, safe_float(dynamic_metrics.get('trade_ratio'), 0.0)) +
            (2.5 if dynamic_metrics.get('breakout') else 0.0) +
            (1.5 if dynamic_metrics.get('flow_burst_ok') else 0.0) +
            (1.5 if dynamic_metrics.get('oi_drive_ok') else 0.0) +
            (memory.get('score', 0.0) * 4.0) +
            reentry_rank
        )
        stability_bonus = min(3.0, hits * 0.5)
        return (symbol, volume, price_change_24h, count, vol_ratio, hits, stability_bonus, dynamic_rank, dynamic_metrics, memory, force_reenter)

    enrich_workers = max(6, min(14, MAX_WORKERS if MAX_WORKERS > 1 else 6))
    with ThreadPoolExecutor(max_workers=enrich_workers) as executor:
        futures = {executor.submit(_enrich_candidate, row): row[0] for row in shortlist}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                item = future.result(timeout=18)
                if item:
                    candidates.append(item)
            except Exception as e:
                diagnostics.log_error(sym, 'quick_filter_enrich', str(e))

    with UNIVERSE_STABILITY_LOCK:
        for sym, rec in list(UNIVERSE_STABILITY.items()):
            if sym not in current_seen:
                rec['misses'] = int(rec.get('misses', 0)) + 1
                if rec['misses'] >= UNIVERSE_MIN_CYCLES_TO_KEEP + 1:
                    UNIVERSE_STABILITY.pop(sym, None)
                else:
                    UNIVERSE_STABILITY[sym] = rec

    candidate_symbols = {c[0] for c in candidates}
    with HOT_SYMBOL_MEMORY_LOCK:
        for sym, rec in list(HOT_SYMBOL_MEMORY.items()):
            ttl = int(rec.get('ttl', 0)) - 1
            if sym in current_seen and sym in candidate_symbols:
                ttl = max(ttl, DYNAMIC_KEEP_HOT_CYCLES - 1)
            if ttl <= 0:
                HOT_SYMBOL_MEMORY.pop(sym, None)
            else:
                rec['ttl'] = ttl
                HOT_SYMBOL_MEMORY[sym] = rec

    candidates.sort(key=lambda x: (x[5] >= UNIVERSE_MIN_CYCLES_TO_KEEP, x[10], x[7], x[9].get('pressure_building', False), x[5], x[2] + x[6], x[4], x[1], x[3]), reverse=True)
    buffered_limit = MAX_SYMBOLS_PER_CYCLE + max(0, int(UNIVERSE_STABILITY_BUFFER))
    buffered = candidates[:buffered_limit]
    stable_first = [row[0] for row in buffered if row[5] >= UNIVERSE_MIN_CYCLES_TO_KEEP]
    hot_dynamic = [row[0] for row in buffered if row[7] >= 6.0 or row[10]]
    memory_hot = [row[0] for row in buffered if row[9].get('pressure_building')]
    fresh_extreme = [row[0] for row in buffered if row[5] < UNIVERSE_MIN_CYCLES_TO_KEEP and (row[2] >= FAST_MOVE_24H_THRESHOLD or row[7] >= 7.5)]
    ordered = []
    seen = set()
    for bucket in (stable_first, hot_dynamic, memory_hot, fresh_extreme, [row[0] for row in buffered]):
        for sym in bucket:
            if sym not in seen:
                seen.add(sym)
                ordered.append(sym)
            if len(ordered) >= MAX_SYMBOLS_PER_CYCLE:
                break
        if len(ordered) >= MAX_SYMBOLS_PER_CYCLE:
            break

    final_symbols = ordered
    print(f"✅ {len(final_symbols)} عملة اجتازت الفلترة الأولية")
    return final_symbols

# =============================================================================
# طبقة العائلات المرجعية الأربع + funding context layer
# =============================================================================
# =============================================================================
# طبقة العائلات المرجعية الأربع + funding context layer
# =============================================================================

def _snapshot_ratio_value(snapshot):
    if not snapshot:
        return 1.0
    return safe_float(snapshot.get('ratio'), 1.0)


def classify_funding_context(funding):
    if not funding or funding.get('current') is None:
        return 'FUNDING_UNINFORMATIVE'

    current = safe_float(funding.get('current'), 0.0)
    z = safe_float(funding.get('z'), 0.0)
    trend = safe_float(funding.get('trend'), 0.0)
    flip = int(funding.get('flip', 0) or 0)
    persistence = int(funding.get('persistence', 0) or 0)
    series = funding.get('series') or []
    recent_floor = min(series[-4:]) if len(series) >= 4 else current

    if current <= 0.0 and (trend > -0.002 or flip == 1 or current > recent_floor):
        return 'FUNDING_NEGATIVE_IMPROVING'
    if abs(current) <= 0.012 and abs(z) <= 1.15 and abs(trend) <= 0.01 and persistence >= 2:
        return 'FUNDING_QUIET_REGIME'
    if current <= 0.02 and z < 1.35:
        return 'FUNDING_NON_CROWDED'
    return 'FUNDING_UNINFORMATIVE'


def build_reference_market_features(symbol):
    base = build_market_context_features(symbol)
    if not base:
        return None

    top = base['top'] or {}
    flow = base['flow'] or {}
    oi = base['oi'] or {}
    funding = base['funding'] or {}
    basis = safe_float(base.get('basis'), 0.0)
    price = safe_float(base.get('price'), 0.0)

    global_snapshot = get_global_ls_ratio_snapshot(symbol, period='5m') or {'ratio': 1.0, 'long_pct': 50.0, 'short_pct': 50.0}
    global_hist = get_global_ls_ratio_history(symbol, period='5m', limit=12)
    global_prev = global_hist[-4] if len(global_hist) >= 4 else global_snapshot['ratio']
    global_delta = global_snapshot['ratio'] - global_prev
    global_slope = linear_trend_slope(global_hist[-6:]) if len(global_hist) >= 6 else 0.0

    pos_now = safe_float(top.get('pos_now'), _snapshot_ratio_value(get_top_position_snapshot(symbol)))
    acc_now = safe_float(top.get('acc_now'), _snapshot_ratio_value(get_top_account_snapshot(symbol)))
    pos_delta = safe_float(top.get('pos_delta'), 0.0)
    acc_delta = safe_float(top.get('acc_delta'), 0.0)
    ls_ratio_now = safe_float(global_snapshot.get('ratio'), 1.0)

    short_crowd_extreme = (
        ls_ratio_now <= EXTREME_SHORT_CROWD_RATIO_MAX or
        (ls_ratio_now <= SHORT_CROWD_RATIO_MAX and pos_now < 0.95 and acc_now < 0.95)
    )

    position_led_divergence = max(0.0, (pos_now - acc_now)) + max(0.0, (pos_delta - acc_delta)) * 0.85
    account_led_divergence = max(0.0, (acc_now - pos_now)) + max(0.0, (acc_delta - pos_delta)) * 0.85

    pos_structure_supportive = pos_now >= 1.03 or (short_crowd_extreme and pos_now >= 0.72)
    acc_structure_supportive = acc_now >= 1.03 or (short_crowd_extreme and acc_now >= 0.70)
    pos_delta_supportive = pos_delta >= 0.002 or (pos_now >= 1.10 and pos_delta >= -0.003) or (short_crowd_extreme and pos_delta >= -0.0015)
    acc_delta_supportive = acc_delta >= 0.002 or (acc_now >= 1.10 and acc_delta >= -0.003) or (short_crowd_extreme and acc_delta >= -0.0015)

    position_leads = (
        position_led_divergence >= (0.05 if short_crowd_extreme else 0.08) and
        pos_structure_supportive and
        pos_delta_supportive and
        (pos_delta >= acc_delta + (0.002 if short_crowd_extreme else 0.005) or (short_crowd_extreme and pos_now > acc_now))
    )
    account_leads = (
        account_led_divergence >= (0.05 if short_crowd_extreme else 0.08) and
        acc_structure_supportive and
        acc_delta_supportive and
        (acc_delta >= pos_delta + (0.002 if short_crowd_extreme else 0.005) or (short_crowd_extreme and acc_now > pos_now))
    )

    both_ratios_supportive = (
        ((pos_now >= 1.02 and acc_now >= 1.02) or short_crowd_extreme) and
        pos_delta > -0.04 and acc_delta > -0.04
    )
    ls_ratio_supportive = 0.90 <= ls_ratio_now <= 1.75 and global_delta > -0.05
    consensus_alignment = round(
        (
            (1.0 if both_ratios_supportive else 0.0) +
            (1.0 if ls_ratio_supportive else 0.0) +
            (1.0 if (pos_delta > 0 and acc_delta > 0) else 0.0) +
            (1.0 if (safe_float(flow.get('price_change_15m'), 0.0) >= 0.4) else 0.0)
        ) / 4.0,
        4
    )

    if ls_ratio_now < SHORT_CROWD_RATIO_MAX and (position_leads or account_leads or global_delta > 0 or pos_delta > 0 or acc_delta > 0):
        ratio_conflict_state = 'SHORT_SIDE_CROWDING'
    elif position_leads and not account_leads:
        ratio_conflict_state = 'POSITION_LED'
    elif account_leads and not position_leads:
        ratio_conflict_state = 'ACCOUNT_LED'
    elif consensus_alignment >= 0.75:
        ratio_conflict_state = 'CONSENSUS'
    else:
        ratio_conflict_state = 'MIXED'

    oi_nv_hist_5m = get_oi_notional_history(symbol, period='5m', limit=8)
    oi_nv_hist_15m = get_oi_notional_history(symbol, period='15m', limit=6)
    oi_nv_delta_5m = pct_change(oi_nv_hist_5m[-3], oi_nv_hist_5m[-1]) if len(oi_nv_hist_5m) >= 3 and oi_nv_hist_5m[-3] > 0 else 0.0
    oi_nv_delta_15m = pct_change(oi_nv_hist_15m[-2], oi_nv_hist_15m[-1]) if len(oi_nv_hist_15m) >= 2 and oi_nv_hist_15m[-2] > 0 else 0.0

    oi_confirmed_expansion = (
        safe_float(oi.get('current_delta_5m'), 0.0) >= 1.0 or
        safe_float(oi.get('delta_15m'), 0.0) >= 2.0 or
        safe_float(oi.get('delta_30m'), 0.0) >= 3.0 or
        safe_float(oi.get('percentile_5m'), 50.0) >= 62.0
    )
    oi_nv_confirmed_expansion = oi_nv_delta_5m >= 1.2 or oi_nv_delta_15m >= 2.0
    hybrid_transition_ready = (
        pos_now >= HYBRID_POS_ACC_MIN and
        acc_now >= HYBRID_POS_ACC_MIN and
        ls_ratio_now >= 1.05 and
        (oi_confirmed_expansion or oi_nv_confirmed_expansion) and
        safe_float(flow.get('price_change_15m'), 0.0) >= 0.20
    )
    hybrid_transition_strength = round(
        max(0.0, ((max(0.0, pos_now - 1.0) + max(0.0, acc_now - 1.0) + max(0.0, ls_ratio_now - 1.0)) / 3.0)),
        4
    )

    kl = flow.get('klines') or {}
    closes = kl.get('close', [])
    highs = kl.get('high', [])
    trades = kl.get('trades', [])
    recent_high = max(highs[-FLOW_BREAKOUT_LOOKBACK:-1]) if len(highs) >= FLOW_BREAKOUT_LOOKBACK else (max(highs[:-1]) if len(highs) >= 2 else price)
    local_breakout_clear = bool(closes and closes[-1] >= recent_high and len(highs) >= max(4, FLOW_BREAKOUT_LOOKBACK))
    recent_trade_ratio = (
        safe_div(mean(trades[-3:]), mean(trades[-9:-3]) or 1.0, default=1.0)
        if len(trades) >= 9 else 1.0
    )
    trade_activity_expansion = recent_trade_ratio >= 1.25 or safe_float(flow.get('intrabar_score'), 0.0) >= 58.0

    funding_context = classify_funding_context(funding)
    if funding_context not in ALLOWED_FUNDING_CONTEXTS:
        funding_context = 'FUNDING_UNINFORMATIVE'
    tf1d = describe_daily_bias(symbol)
    tf4h = describe_4h_confirmation(symbol, tf1d.get('status', 'UNKNOWN'))
    tf15 = describe_15m_interest(symbol)
    tf5 = describe_5m_trigger(symbol, side_hint='UP')

    secondary_contexts = []
    if ls_ratio_now < SHORT_CROWD_RATIO_MAX and (position_leads or account_leads or global_delta > 0 or pos_delta > 0 or acc_delta > 0) and safe_float(flow.get('price_change_15m'), 0.0) >= 0.2:
        secondary_contexts.append('CROWDED_SHORT_SQUEEZE')
    if abs(basis) <= 0.60:
        secondary_contexts.append('BASIS_NEUTRAL_SUPPORT')
    if account_leads or consensus_alignment >= 0.75 or safe_float(base.get('spot', {}).get('spot_lead'), 0.0) > 0:
        secondary_contexts.append('SMART_ACCUMULATION_SUPPORT')
    secondary_contexts = [ctx for ctx in secondary_contexts if ctx in ALLOWED_SECONDARY_CONTEXTS]

    hard_bearish_conflict = tf1d.get('status') == 'BEARISH' and tf4h.get('status') == 'BEARISH'

    pre_ignition_pressure = compute_pre_ignition_pressure(symbol)
    micro_ignition = detect_micro_ignition(symbol, base)
    stage_ref = {**base,
        'symbol': symbol,
        'funding_context': funding_context,
        'top': top,
        'global_ls_ratio_now': ls_ratio_now,
        'oi_confirmed_expansion': oi_confirmed_expansion,
        'oi_nv_confirmed_expansion': oi_nv_confirmed_expansion,
        'trade_activity_expansion': trade_activity_expansion,
        'local_breakout_clear': local_breakout_clear,
        'tf15': tf15,
        'tf4h': tf4h,
        'tf1d': tf1d,
        'basis_supportive': abs(basis) <= 0.75,
        'short_crowd_extreme': short_crowd_extreme,
        'short_squeeze_inflexion': short_crowd_extreme and (global_delta > 0.01 or pos_delta > -0.001 or acc_delta > -0.001 or safe_float(flow.get('price_change_5m'), 0.0) >= SHORT_SQUEEZE_MIN_PRICE_5M),
        'position_leads': position_leads,
        'account_leads': account_leads,
        'consensus_alignment': consensus_alignment,
        'hard_bearish_conflict': hard_bearish_conflict,
        'late_or_chased': safe_float(flow.get('price_change_5m'), 0.0) > FLOW_PRICE_CHASE_HARD_MAX,
        'liquidity': base.get('liquidity') or {},
        'pre_ignition_pressure': pre_ignition_pressure,
        'micro_ignition': micro_ignition,
    }
    signal_stage, oi_state = classify_signal_stage_from_reference(stage_ref)
    late_or_chased = signal_stage == 'LATE' or safe_float(flow.get('price_change_5m'), 0.0) > FLOW_PRICE_CHASE_HARD_MAX

    return {
        **base,
        'top': top,
        'global_ls_snapshot': global_snapshot,
        'global_ls_history': global_hist,
        'global_ls_ratio_now': ls_ratio_now,
        'global_ls_ratio_delta': global_delta,
        'global_ls_ratio_slope': global_slope,
        'position_led_divergence': round(position_led_divergence, 4),
        'account_led_divergence': round(account_led_divergence, 4),
        'position_leads': position_leads,
        'account_leads': account_leads,
        'consensus_alignment': consensus_alignment,
        'hybrid_transition_ready': hybrid_transition_ready,
        'hybrid_transition_strength': hybrid_transition_strength,
        'ratio_conflict_state': ratio_conflict_state,
        'oi_confirmed_expansion': oi_confirmed_expansion,
        'oi_nv_confirmed_expansion': oi_nv_confirmed_expansion,
        'oi_nv_delta_5m': round(oi_nv_delta_5m, 4),
        'oi_nv_delta_15m': round(oi_nv_delta_15m, 4),
        'funding_context': funding_context,
        'secondary_contexts': secondary_contexts,
        'recent_trade_ratio': recent_trade_ratio,
        'trade_activity_expansion': trade_activity_expansion,
        'local_breakout_clear': local_breakout_clear,
        'recent_high': recent_high,
        'tf1d': tf1d,
        'tf4h': tf4h,
        'tf15': tf15,
        'tf5': tf5,
        'signal_stage': signal_stage,
        'oi_state': oi_state,
        'basis_supportive': abs(basis) <= 0.75,
        'hard_bearish_conflict': hard_bearish_conflict,
        'late_or_chased': late_or_chased,
        'micro_ignition': micro_ignition,
        'pre_ignition_pressure': stage_ref.get('pre_ignition_pressure', pre_ignition_pressure),
        'flat_oi_context': stage_ref.get('flat_oi_context', {}),
        'short_squeeze_override': stage_ref.get('short_squeeze_override', {}),
        'prepared_continuation': stage_ref.get('prepared_continuation', {}),
    }


def _funding_is_supportive(ref):
    return ref['funding_context'] in ('FUNDING_QUIET_REGIME', 'FUNDING_NEGATIVE_IMPROVING', 'FUNDING_NON_CROWDED')


def _base_uptrend_not_hostile(ref):
    return not ref['hard_bearish_conflict'] and ref['tf15'].get('status') != 'BEARISH_INTEREST'


def _quality_tier_from_score(score):
    if score >= 84:
        return 'HIGH'
    if score >= 72:
        return 'MEDIUM'
    return 'LOW'


def _build_family_result(family, eligible, strength, decisive_feature, why_selected, why_not_selected, extra=None):
    payload = {
        'family': family,
        'eligible': bool(eligible),
        'strength': max(0.0, min(100.0, strength)),
        'decisive_feature': decisive_feature,
        'why_selected': why_selected,
        'why_not_selected': why_not_selected,
    }
    if extra:
        payload.update(extra)
    return payload


def detect_position_led_squeeze_buildup(ref):
    flow = ref['flow']
    top = ref['top']
    unmet = []
    strength = 0.0

    if ref['position_leads']:
        strength += 24
    elif ref.get('short_squeeze_inflexion') and ref.get('ratio_conflict_state') == 'SHORT_SIDE_CROWDING':
        strength += 18
        unmet.append('قيادة Posit غير textbook لكن squeeze مبكر قائم')
    else:
        unmet.append('L.S Posit لا يقود بوضوح')
    if safe_float(top.get('acc_delta'), 0.0) <= safe_float(top.get('pos_delta'), 0.0) + 0.05:
        strength += 11
    else:
        unmet.append('L.S Acco يؤكد بسرعة أعلى من المطلوب')
    if ref['global_ls_ratio_now'] <= 1.55 or ref.get('ratio_conflict_state') == 'SHORT_SIDE_CROWDING':
        strength += 9
    else:
        unmet.append('L.S Ratio مزدحم نسبيًا')
    if ref['oi_confirmed_expansion']:
        strength += 15
    elif ref.get('short_squeeze_inflexion'):
        strength += 10
        unmet.append('OI ما زال في بداية الانعطاف')
    else:
        unmet.append('OI لا يؤكد التمدد')
    if ref['oi_nv_confirmed_expansion']:
        strength += 8
    if _funding_is_supportive(ref):
        strength += 11
    else:
        unmet.append('التمويل ليس سياقًا داعمًا')
    if safe_float(flow.get('buy_ratio_recent'), 0.0) >= 0.53:
        strength += 8
    else:
        unmet.append('ضغط الشراء لم يتأكد')
    if ref['signal_stage'] != 'LATE' and safe_float(flow.get('price_change_15m'), 0.0) <= 4.5:
        strength += 7
    else:
        unmet.append('الحركة أصبحت late')
    if _base_uptrend_not_hostile(ref):
        strength += 7
    else:
        unmet.append('الفريمات الأكبر تعارض الصعود')

    decisive_feature = f"Position-led divergence={ref['position_led_divergence']:.3f} مع PosNow={safe_float(top.get('pos_now'), 0.0):.2f} وPosΔ={safe_float(top.get('pos_delta'), 0.0):+.2f} مقابل AccΔ={safe_float(top.get('acc_delta'), 0.0):+.2f}"
    why_selected = (
        f"L.S Posit يقود بوضوح بينما L.S Acco متأخر نسبيًا، "
        f"مع OI={'مؤكد' if ref['oi_confirmed_expansion'] else 'غير مؤكد'} "
        f"وFunding={ref['funding_context']}"
    )
    eligible = (ref['position_leads'] or ref.get('short_squeeze_inflexion')) and safe_float(top.get('pos_delta'), 0.0) >= -0.004 and (ref['oi_confirmed_expansion'] or ref.get('short_squeeze_inflexion')) and _funding_is_supportive(ref) and _base_uptrend_not_hostile(ref)
    why_not_selected = ' | '.join(unmet) if unmet else 'الشروط الجوهرية مكتملة'
    return _build_family_result('POSITION_LED_SQUEEZE_BUILDUP', eligible, strength, decisive_feature, why_selected, why_not_selected)


def detect_account_led_accumulation(ref):
    flow = ref['flow']
    top = ref['top']
    unmet = []
    strength = 0.0

    if ref['account_leads']:
        strength += 24
    elif ref.get('short_squeeze_inflexion') and ref.get('ratio_conflict_state') == 'SHORT_SIDE_CROWDING':
        strength += 16
        unmet.append('قيادة Acco غير textbook لكن هناك انقلاب حسابات من قاعدة short crowded')
    else:
        unmet.append('L.S Acco لا يقود بوضوح')
    if safe_float(top.get('pos_delta'), 0.0) <= safe_float(top.get('acc_delta'), 0.0) + 0.06:
        strength += 10
    else:
        unmet.append('L.S Posit متقدم أكثر من منطق هذه العائلة')
    if ref['oi_confirmed_expansion']:
        strength += 14
    elif ref.get('short_squeeze_inflexion'):
        strength += 9
        unmet.append('OI غير مكتمل لكنه بدأ ينعطف من short crowding')
    else:
        unmet.append('OI غير داعم')
    if safe_float(flow.get('price_change_15m'), 0.0) >= 0.25 and safe_float(flow.get('price_change_15m'), 0.0) <= 6.0:
        strength += 10
    else:
        unmet.append('السعر إما لم يبدأ أو أصبح ممتدًا أكثر من اللازم')
    if _funding_is_supportive(ref):
        strength += 10
    else:
        unmet.append('التمويل ليس non-crowded')
    if ref['basis_supportive']:
        strength += 9
    else:
        unmet.append('basis عدائي')
    if 'SMART_ACCUMULATION_SUPPORT' in ref['secondary_contexts']:
        strength += 7
    if ref['signal_stage'] != 'LATE':
        strength += 6
    else:
        unmet.append('الإشارة late')
    if _base_uptrend_not_hostile(ref):
        strength += 6
    else:
        unmet.append('اتجاه الفريمات غير مناسب')

    decisive_feature = f"Account-led divergence={ref['account_led_divergence']:.3f} مع AccNow={safe_float(top.get('acc_now'), 0.0):.2f} وAccΔ={safe_float(top.get('acc_delta'), 0.0):+.2f} مقابل PosΔ={safe_float(top.get('pos_delta'), 0.0):+.2f}"
    why_selected = (
        f"L.S Acco يقود بينما L.S Posit أبطأ، والسعر بدأ قبل اكتمال توسع المراكز، "
        f"مع Basis={'neutral' if ref['basis_supportive'] else 'hostile'} وFunding={ref['funding_context']}"
    )
    eligible = (ref['account_leads'] or ref.get('short_squeeze_inflexion')) and safe_float(top.get('acc_delta'), 0.0) >= -0.004 and (ref['oi_confirmed_expansion'] or ref.get('short_squeeze_inflexion')) and ref['basis_supportive'] and _funding_is_supportive(ref) and _base_uptrend_not_hostile(ref)
    why_not_selected = ' | '.join(unmet) if unmet else 'الشروط الجوهرية مكتملة'
    return _build_family_result('ACCOUNT_LED_ACCUMULATION', eligible, strength, decisive_feature, why_selected, why_not_selected)


def detect_consensus_bullish_expansion(ref):
    flow = ref['flow']
    top = ref['top']
    unmet = []
    strength = 0.0

    if ref['consensus_alignment'] >= 0.75:
        strength += 23
    elif ref.get('hybrid_transition_ready') and ref.get('hybrid_transition_strength', 0.0) >= 0.15:
        strength += 18
        unmet.append('توافق هجين مبكر قبل اكتمال consensus textbook')
    else:
        unmet.append('لا يوجد توافق كافٍ بين Posit وAcco وL.S Ratio')
    if safe_float(top.get('pos_delta'), 0.0) > 0 and safe_float(top.get('acc_delta'), 0.0) > 0:
        strength += 12
    elif ref.get('hybrid_transition_ready'):
        strength += 8
        unmet.append('الانتقال ما زال هجينًا ولم يكتمل delta alignment بعد')
    else:
        unmet.append('أحد ratios غير داعم')
    if 0.90 <= ref['global_ls_ratio_now'] <= 2.40:
        strength += 10
    else:
        unmet.append('L.S Ratio غير منظم')
    if ref['oi_confirmed_expansion'] and ref['oi_nv_confirmed_expansion']:
        strength += 16
    elif ref['oi_confirmed_expansion']:
        strength += 10
        unmet.append('OI NV لم يؤكد كاملًا')
    else:
        unmet.append('OI لا يتمدد')
    if 0.45 <= safe_float(flow.get('price_change_15m'), 0.0) <= 4.8 and safe_float(flow.get('price_change_5m'), 0.0) <= 2.8:
        strength += 11
    else:
        unmet.append('التوسع السعري غير منظم')
    if _funding_is_supportive(ref):
        strength += 10
    else:
        unmet.append('التمويل مزدحم أو غير واضح')
    if ref['tf4h'].get('status') in ('BULLISH', 'MIXED') and ref['tf15'].get('status') in ('BUILDUP', 'BULLISH_INTEREST'):
        strength += 8
    else:
        unmet.append('4H/15m لا يقدمان تأكيدًا كافيًا')
    if ref['signal_stage'] != 'LATE':
        strength += 6
    else:
        unmet.append('الإشارة late')

    decisive_feature = f"Consensus alignment={ref['consensus_alignment']:.2f} مع PosΔ={safe_float(top.get('pos_delta'), 0.0):+.2f}, AccΔ={safe_float(top.get('acc_delta'), 0.0):+.2f}, OI_NV={ref['oi_nv_delta_15m']:+.2f}%"
    why_selected = (
        f"Posit وAcco وL.S Ratio يتحركون معًا في توسع منظم، "
        f"والـ OI/OI NV يؤكدان التمدد، وFunding={ref['funding_context']}"
    )
    eligible = (ref['consensus_alignment'] >= 0.75 or ref.get('hybrid_transition_ready')) and ref['oi_confirmed_expansion'] and _funding_is_supportive(ref) and _base_uptrend_not_hostile(ref)
    why_not_selected = ' | '.join(unmet) if unmet else 'الشروط الجوهرية مكتملة'
    return _build_family_result('CONSENSUS_BULLISH_EXPANSION', eligible, strength, decisive_feature, why_selected, why_not_selected)


def detect_flow_liquidity_vacuum_breakout(ref):
    flow = ref['flow']
    unmet = []
    strength = 0.0

    taker_imbalance = max(
        safe_float(flow.get('ofi_recent'), 0.0),
        safe_float(flow.get('buy_ratio_recent'), 0.0) - 0.5
    )
    if taker_imbalance >= 0.07 or safe_float(flow.get('buy_ratio_recent'), 0.0) >= 0.58:
        strength += 20
    else:
        unmet.append('taker imbalance غير كافٍ')
    if ref['trade_activity_expansion']:
        strength += 14
    else:
        unmet.append('trade activity expansion غير واضح')
    if ref['local_breakout_clear'] or ref['tf15'].get('zone') == 'breakout_zone':
        strength += 14
    else:
        unmet.append('لا يوجد local/range breakout واضح')
    if ref['liquidity'].get('vacuum') == 1 or ref['liquidity'].get('depth_ratio', 1.0) <= DEPTH_RATIO_THIN:
        strength += 11
    else:
        unmet.append('لا يوجد فراغ سيولة كافٍ')
    if _funding_is_supportive(ref) or ref['funding_context'] == 'FUNDING_UNINFORMATIVE':
        strength += 8
    else:
        unmet.append('التمويل عدائي')
    if ref['ratio_conflict_state'] not in ('SHORT_SIDE_CROWDING',) and ref['global_ls_ratio_now'] <= 2.80:
        strength += 8
    else:
        unmet.append('الـ ratios معادية أو crowded جدًا')
    if safe_float(flow.get('price_change_5m'), 0.0) <= FLOW_PRICE_CHASE_MAX and ref['signal_stage'] != 'LATE':
        strength += 8
    else:
        unmet.append('السعر chased/late')
    if _base_uptrend_not_hostile(ref):
        strength += 7
    else:
        unmet.append('الفريمات الأعلى تعارض الاختراق')

    decisive_feature = f"Taker/flow decisive: buy_ratio={safe_float(flow.get('buy_ratio_recent'), 0.0):.2f}, OFI={safe_float(flow.get('ofi_recent'), 0.0):+.2f}, trade_ratio={ref['recent_trade_ratio']:.2f}x, vacuum={ref['liquidity'].get('vacuum', 0)}"
    why_selected = (
        f"التدفق هو العامل الحاسم: imbalance قوي + trade activity expansion + breakout واضح "
        f"مع تمويل غير عدائي ورفض الاعتماد على textbook candle"
    )
    eligible = (taker_imbalance >= 0.07 or safe_float(flow.get('buy_ratio_recent'), 0.0) >= 0.58) and ref['trade_activity_expansion'] and (ref['local_breakout_clear'] or ref['tf15'].get('zone') == 'breakout_zone') and _base_uptrend_not_hostile(ref)
    why_not_selected = ' | '.join(unmet) if unmet else 'الشروط الجوهرية مكتملة'
    return _build_family_result('FLOW_LIQUIDITY_VACUUM_BREAKOUT', eligible, strength, decisive_feature, why_selected, why_not_selected)


def _resolver_priority_score(candidate, ref):
    family = candidate['family']
    stage = ref.get('signal_stage', 'WATCH')
    return fingerprint_quality_rank(ref, family, stage)


def _resolver_sort_key(candidate):
    return candidate.get('resolver_score')


def update_signal_stability(signal_data):
    if not signal_data:
        return signal_data
    symbol = signal_data.get('symbol')
    family = signal_data.get('primary_family')
    original_stage = signal_data.get('signal_stage', 'WATCH')
    oi_state = signal_data.get('oi_state', 'NEUTRAL')
    now = time.time()
    with SIGNAL_STABILITY_LOCK:
        prev = SIGNAL_STABILITY.get(symbol)
        stable_family = bool(prev and prev.get('family') == family)
        stable_oi = bool(prev and prev.get('oi_state') == oi_state)
        stable_window = max(240, SCAN_INTERVAL_MINUTES * 180)
        if prev and stable_family and stable_oi and (now - prev.get('ts', 0)) <= stable_window:
            cycles = int(prev.get('cycles', 1)) + 1
        else:
            cycles = 1
        stage_history = list(prev.get('stages', [])) if prev else []
        stage_history.append(original_stage)
        stage_history = stage_history[-6:]
        family_history = list(prev.get('families', [])) if prev else []
        family_history.append(family)
        family_history = family_history[-6:]
        prev_family = prev.get('family') if prev else None
        prev_stage = prev.get('stage') if prev else None

    signal_data['persistence_cycles'] = cycles
    signal_data['stage_persistence'] = stage_history.count(original_stage)
    signal_data['family_persistence'] = family_history.count(family)
    signal_data['stage_history'] = stage_history
    signal_data['family_history'] = family_history
    signal_data['previous_family'] = prev_family
    signal_data['previous_stage'] = prev_stage

    adjusted_stage, stage_memory_reason = _path_based_stage_override(signal_data, prev_stage=prev_stage)
    if adjusted_stage != original_stage:
        stage_history[-1] = adjusted_stage
        signal_data['stage_history'] = stage_history
        signal_data['stage_memory_reason'] = stage_memory_reason
        signal_data['signal_stage'] = adjusted_stage
    else:
        signal_data['signal_stage'] = original_stage

    stage = signal_data.get('signal_stage', original_stage)
    signal_data['stage_persistence'] = stage_history.count(stage)
    current_exec = execution_priority_score(signal_data)
    signal_data['execution_priority_score'] = current_exec
    signal_data['execution_priority'] = current_exec
    prev_exec = float(prev.get('execution_priority', current_exec)) if prev else current_exec
    exec_delta = current_exec - prev_exec
    execution_trend = 'stable'
    family_transition = bool(prev_family and prev_family != family)
    stage_transition_upgrade = _stage_rank(stage) > _stage_rank(prev_stage) if prev_stage else False

    with SIGNAL_STABILITY_LOCK:
        SIGNAL_STABILITY[symbol] = {
            'family': family,
            'stage': stage,
            'cycles': cycles,
            'ts': now,
            'oi_state': oi_state,
            'stages': stage_history,
            'families': family_history,
            'resolver_score': float(signal_data.get('reference_features', {}).get('resolver_score', signal_data.get('score', 0)) or signal_data.get('score', 0)),
            'execution_priority': current_exec,
        }

    signal_data['family_transition'] = family_transition
    signal_data['family_transition_text'] = family_transition_text_ar(prev_family, family)
    signal_data['stage_transition_upgrade'] = bool(stage_transition_upgrade)
    signal_data['stage_transition_text'] = stage_transition_text_ar(prev_stage, stage)
    if adjusted_stage != original_stage and stage_memory_reason:
        signal_data['stage_transition_text'] += f" | {_stage_upgrade_text_ar(stage_memory_reason, original_stage, adjusted_stage)}"
    signal_data['execution_trend'] = execution_trend
    signal_data['execution_delta'] = exec_delta
    signal_data['signal_path_eval'] = evaluate_signal_path_quality(stage_history, stage)
    signal_data['signal_path_quality'] = signal_data['signal_path_eval'].get('path_quality', 'flat')
    signal_data['signal_path_label'] = signal_data['signal_path_eval'].get('path_label_ar', 'مسار غير متاح')
    signal_data['signal_path_bonus'] = signal_data['signal_path_eval'].get('path_bonus', 0)
    signal_data['signal_evolution_summary'] = signal_evolution_summary_ar(signal_data)
    signal_data['signal_path_summary'] = signal_path_summary_ar(signal_data)
    signal_data['pre_ignition_pressure'] = signal_data.get('reference_features', {}).get('pre_ignition_pressure', signal_data.get('pre_ignition_pressure', {}))
    signal_data['short_squeeze_override'] = signal_data.get('reference_features', {}).get('short_squeeze_override', signal_data.get('short_squeeze_override', {}))
    signal_data['prepared_continuation'] = signal_data.get('reference_features', {}).get('prepared_continuation', signal_data.get('prepared_continuation', {}))
    signal_data['flat_oi_context'] = signal_data.get('reference_features', {}).get('flat_oi_context', signal_data.get('flat_oi_context', {}))
    signal_data['micro_ignition'] = signal_data.get('reference_features', {}).get('micro_ignition', signal_data.get('micro_ignition', {}))

    if ENABLE_HUMAN_MEMORY and symbol:
        with SYMBOL_INTELLIGENCE_LOCK:
            mem = SYMBOL_INTELLIGENCE.get(symbol, {})
            family_counts = dict(mem.get('family_counts', {}))
            family_counts[family] = family_counts.get(family, 0) + 1
            stage_counts = dict(mem.get('stage_counts', {}))
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
            seen = int(mem.get('seen_count', 0) or 0) + 1
            first_seen_ts = mem.get('first_seen_ts') or signal_data.get('cycle_snapshot_ts') or datetime.now().isoformat()
            recent_frames = list(mem.get('recent_frames', []) or [])
            recent_frames.append({
                'ts': signal_data.get('cycle_snapshot_ts') or datetime.now().isoformat(),
                'family': family,
                'stage': stage,
                'price_5m': safe_float((signal_data.get('reference_features') or {}).get('flow', {}).get('price_change_5m'), 0.0),
                'price_15m': safe_float((signal_data.get('reference_features') or {}).get('flow', {}).get('price_change_15m'), 0.0),
                'oi_5m': safe_float((signal_data.get('reference_features') or {}).get('oi', {}).get('current_delta_5m'), 0.0),
                'oi_15m': safe_float((signal_data.get('reference_features') or {}).get('oi', {}).get('delta_15m'), 0.0),
                'oi_nv_15m': safe_float((signal_data.get('reference_features') or {}).get('oi_nv_delta_15m', 0.0), 0.0),
                'buy_ratio': safe_float((signal_data.get('reference_features') or {}).get('buy_ratio_recent', 0.0), 0.0),
                'trade_ratio': safe_float(((signal_data.get('reference_features') or {}).get('flow') or {}).get('trade_ratio', 0.0), 0.0),
                'breakout': bool((signal_data.get('reference_features') or {}).get('local_breakout_clear')),
                'vacuum': bool(((signal_data.get('reference_features') or {}).get('liquidity') or {}).get('vacuum')),
                'execution_rank_diag': current_exec,
                'micro_active': bool((signal_data.get('reference_features') or {}).get('micro_ignition', {}).get('active')),
            })
            recent_frames = recent_frames[-PRE_IGNITION_MEMORY_BARS:]
            SYMBOL_INTELLIGENCE[symbol] = {
                'seen_count': seen,
                'first_seen_ts': first_seen_ts,
                'last_seen_ts': signal_data.get('cycle_snapshot_ts') or datetime.now().isoformat(),
                'family_counts': family_counts,
                'stage_counts': stage_counts,
                'last_family': family,
                'last_stage': stage,
                'last_path': signal_data.get('signal_path_summary', ''),
                'recent_frames': recent_frames,
            }
        signal_data['memory_seen_count'] = seen
        signal_data['memory_first_seen_ts'] = first_seen_ts

    signal_data = enrich_human_memory(signal_data)
    return signal_data


def is_signal_printable(signal_data):
    if not signal_data:
        return False, 'empty_signal'
    stage = signal_data.get('signal_stage', 'WATCH')
    ref = signal_data.get('reference_features') or {}
    acceptance = signal_data.get('acceptance_state') or ref.get('acceptance_state') or {}
    continuation = signal_data.get('continuation_state') or ref.get('continuation_state') or {}
    exhaustion = signal_data.get('exhaustion_risk') or ref.get('exhaustion_risk') or {}
    memory = signal_data.get('behavioral_memory') or ref.get('behavioral_memory') or {}
    micro = signal_data.get('micro_ignition') or ref.get('micro_ignition') or {}
    oi_state = signal_data.get('oi_state', 'NEUTRAL')

    if stage in ('FAILED_CONTINUATION', 'REJECTED') or continuation.get('failed'):
        signal_data['execution_status'] = 'BLOCKED'
        return False, 'failed_continuation'
    if stage == 'LATE_CHASE' or exhaustion.get('late'):
        signal_data['execution_status'] = 'LATE_CHASE'
        signal_data['discovery_status'] = 'DETECTED_LATE'
        return False, 'late_chase'
    if memory.get('veto'):
        signal_data['execution_status'] = 'BLOCKED'
        return False, 'behavioral_memory_veto'
    if acceptance.get('state') == 'REJECTED':
        signal_data['execution_status'] = 'BLOCKED'
        return False, f"acceptance_rejected:{acceptance.get('reason','unknown')}"

    if oi_state == 'PRICE_UP_OI_FLAT':
        squeeze = signal_data.get('short_squeeze_override') or ref.get('short_squeeze_override') or {}
        if not (micro.get('active') or squeeze.get('active')):
            signal_data['execution_status'] = 'BLOCKED'
            return False, 'flat_oi_default_block'

    if stage == 'TRIGGERED':
        signal_data['execution_status'] = 'ACTIONABLE'
        return True, 'triggered'
    if stage == 'ARMED':
        signal_data['execution_status'] = 'ACTIONABLE'
        return True, 'armed'
    if stage == 'PREPARE':
        signal_data['execution_status'] = 'ACTIONABLE'
        return True, 'prepare'

    signal_data['execution_status'] = 'DISCOVERY_ONLY'
    return False, 'discovery_only'


def resolve_primary_family_signal(symbol):
    ref = build_reference_market_features(symbol)
    if not ref:
        return None

    detector_results = [
        detect_position_led_squeeze_buildup(ref),
        detect_account_led_accumulation(ref),
        detect_consensus_bullish_expansion(ref),
        detect_flow_liquidity_vacuum_breakout(ref),
    ]

    families_why_not = {
        item['family']: item['why_not_selected']
        for item in detector_results
    }
    candidates = [item for item in detector_results if item['eligible']]

    if ref['hard_bearish_conflict']:
        return log_rejection(symbol, 'FOUR_FAMILY_RESOLVER', 'hard_bearish_conflict', tf1d=ref['tf1d'], tf4h=ref['tf4h'])
    if ref['late_or_chased']:
        diagnostics.log_rejection(symbol, 'FOUR_FAMILY_RESOLVER', 'late_or_chased_discovery_only', signal_stage=ref['signal_stage'], price_5m=ref['flow'].get('price_change_5m'), price_15m=ref['flow'].get('price_change_15m'))

    if not candidates:
        return log_rejection(symbol, 'FOUR_FAMILY_RESOLVER', 'no_primary_family_qualified', families=families_why_not, funding_context=ref['funding_context'], ratio_conflict_state=ref['ratio_conflict_state'])

    for item in candidates:
        item['_reference_ref'] = ref
        item['resolver_score'] = _resolver_priority_score(item, ref)
    candidates.sort(key=lambda item: _resolver_sort_key(item), reverse=True)
    best = _apply_family_hysteresis(symbol, candidates, candidates[0])

    # Resolver صارم: لا نعتمد على أول detector نجح، بل على الأعلى قوة بعد التحقق من عدم late/contradiction.
    other_family_notes = {}
    for family in PRIMARY_FAMILIES:
        if family == best['family']:
            continue
        note = families_why_not.get(family, 'لم يتم جمع بيانات كافية')
        if any(c['family'] == family for c in candidates):
            family_strength = next(c['strength'] for c in candidates if c['family'] == family)
            family_candidate = next(c for c in candidates if c['family'] == family)
            family_resolver = family_candidate.get('resolver_score', family_strength)
            best_resolver = best.get('resolver_score', best['strength'])
            if abs(family_resolver - best_resolver) < 0.15:
                note = f"مؤهل وقريب جدًا من {best['family']} لكن خسر tie-break المرجعي ({family_resolver:.1f} ≈ {best_resolver:.1f})"
            else:
                note = f"مؤهل لكنه أضعف من {best['family']} (resolver={family_resolver:.1f} < {best_resolver:.1f})"
        other_family_notes[family] = note

    price = ref['price']
    score = int(round(best['strength']))
    result = {
        'symbol': symbol,
        'pattern': best['family'],  # mirror داخلي فقط؛ الواجهة النهائية تعتمد primary_family
        'primary_family': best['family'],
        'secondary_contexts': list(dict.fromkeys(ref['secondary_contexts'])),
        'funding_context': ref['funding_context'],
        'decisive_feature': best['decisive_feature'],
        'signal_quality_tier': 'N/A',
        'signal_stage': ref['signal_stage'],
        'oi_state': ref.get('oi_state', 'NEUTRAL'),
        'why_selected': best['why_selected'],
        'why_not_other_families': other_family_notes,
        'direction': 'UP',
        'price': price,
        'score': score,
        'reasons': [
            f"primary_family={best['family']}",
            f"funding_context={ref['funding_context']}",
            f"secondary_contexts={', '.join(ref['secondary_contexts']) if ref['secondary_contexts'] else 'NONE'}",
            f"resolver_fingerprint_rank={best.get('resolver_score')}",
            best['decisive_feature'],
            best['why_selected'],
        ],
        'reference_features': {
            'position_led_divergence': ref['position_led_divergence'],
            'account_led_divergence': ref['account_led_divergence'],
            'position_leads': ref['position_leads'],
            'account_leads': ref['account_leads'],
            'consensus_alignment': ref['consensus_alignment'],
            'ratio_conflict_state': ref['ratio_conflict_state'],
            'oi_confirmed_expansion': ref['oi_confirmed_expansion'],
            'oi_nv_confirmed_expansion': ref['oi_nv_confirmed_expansion'],
            'funding_context': ref['funding_context'],
            'secondary_contexts': ref['secondary_contexts'],
            'global_ls_ratio_now': ref['global_ls_ratio_now'],
            'global_ls_ratio_delta': ref['global_ls_ratio_delta'],
            'hybrid_transition_ready': ref.get('hybrid_transition_ready', False),
            'hybrid_transition_strength': ref.get('hybrid_transition_strength', 0.0),
            'signal_stage': ref['signal_stage'],
            'oi_state': ref.get('oi_state', 'NEUTRAL'),
            'hybrid_transition_ready': ref.get('hybrid_transition_ready', False),
            'hybrid_transition_strength': ref.get('hybrid_transition_strength', 0.0),
            'buy_ratio_recent': safe_float(ref['flow'].get('buy_ratio_recent'), 0.0),
            'ofi_recent': safe_float(ref['flow'].get('ofi_recent'), 0.0),
            'trade_activity_expansion': ref.get('trade_activity_expansion', False),
            'local_breakout_clear': ref.get('local_breakout_clear', False),
            'tf1d': ref['tf1d'],
            'tf4h': ref['tf4h'],
            'tf15': ref['tf15'],
            'tf5': ref['tf5'],
            'resolver_score': best.get('resolver_score', best['strength']),
            'flow': ref.get('flow') or {},
            'oi': ref.get('oi') or {},
            'liquidity': ref.get('liquidity') or {},
            'pre_ignition_pressure': ref.get('pre_ignition_pressure', {}),
            'short_squeeze_override': ref.get('short_squeeze_override', {}),
            'prepared_continuation': ref.get('prepared_continuation', {}),
            'flat_oi_context': ref.get('flat_oi_context', {}),
            'micro_ignition': ref.get('micro_ignition', {}),
            'recent_trade_ratio': ref.get('recent_trade_ratio', 1.0),
            'late_or_chased': ref.get('late_or_chased', False),
        },
        'timestamp': datetime.now().isoformat(),
        'cycle_snapshot_ts': CYCLE_SNAPSHOT_TS,
    }

    stage_with_family, oi_state = classify_signal_stage_from_reference(ref, family_hint=best['family'])
    result['signal_stage'] = stage_with_family
    result['oi_state'] = oi_state
    result['discovery_status'] = 'DETECTED'
    ref['symbol'] = symbol
    acceptance_state = detect_acceptance_state(ref, oi_state=oi_state, family_hint=best['family'])
    continuation_state = detect_failure_continuation(ref, family_hint=best['family'], oi_state=oi_state)
    exhaustion_risk = detect_exhaustion_risk(ref, oi_state=oi_state)
    micro_state = detect_micro_ignition_state(ref)
    memory_state = behavioral_memory_veto(symbol, ref, stage=stage_with_family, family=best['family'])
    result['acceptance_state'] = acceptance_state
    result['continuation_state'] = continuation_state
    result['exhaustion_risk'] = exhaustion_risk
    result['behavioral_memory'] = memory_state
    result['micro_ignition_state'] = micro_state
    result['execution_allowed'] = False
    result['execution_block_reasons'] = []
    result['execution_status'] = 'DISCOVERY_ONLY'
    result['pre_ignition_pressure'] = ref.get('pre_ignition_pressure', {})
    result['short_squeeze_override'] = ref.get('short_squeeze_override', {})
    result['prepared_continuation'] = ref.get('prepared_continuation', {})
    result['flat_oi_context'] = ref.get('flat_oi_context', {})
    result['micro_ignition'] = ref.get('micro_ignition', {}) or {'active': False, 'pre_break': False, 'breakout': False}

    stop_loss, tp1, tp2 = calculate_risk_management(symbol, price, 'UP')
    result['stop_loss'] = stop_loss
    result['take_profit1'] = tp1
    result['take_profit2'] = tp2
    result['reference_features']['acceptance_state'] = result.get('acceptance_state', {})
    result['reference_features']['continuation_state'] = result.get('continuation_state', {})
    result['reference_features']['exhaustion_risk'] = result.get('exhaustion_risk', {})
    result['reference_features']['behavioral_memory'] = result.get('behavioral_memory', {})
    result['reference_features']['micro_ignition_state'] = result.get('micro_ignition_state', {})
    result = update_signal_stability(result)
    printable, print_reason = is_signal_printable(result)
    result['print_ready'] = printable
    result['print_reason'] = print_reason
    result['execution_allowed'] = bool(printable)
    if not printable:
        result['execution_block_reasons'] = [print_reason]
    if not printable:
        return log_rejection(symbol, 'FOUR_FAMILY_RESOLVER', print_reason, primary_family=result.get('primary_family'), signal_stage=result.get('signal_stage'), oi_state=result.get('oi_state'))
    diagnostics.log_accept(symbol, best['family'], reasons=[f"discovered={result.get('discovery_status')}", f"execution_allowed={result.get('execution_allowed')}", f"acceptance={result.get('acceptance_state',{}).get('state')}", f"continuation_failed={result.get('continuation_state',{}).get('failed')}", f"exhaustion={result.get('exhaustion_risk',{}).get('risky')}", f"behavioral_veto={result.get('behavioral_memory',{}).get('veto')}", f"stage={result.get('signal_stage')}", f"print_reason={result.get('print_reason')}"])
    log_acceptance(
        symbol,
        best['family'],
        score=result['score'],
        price=result['price'],
        reasons=result['reasons'],
        funding_context=result['funding_context'],
        secondary_contexts=result['secondary_contexts'],
        decisive_feature=result['decisive_feature'],
        why_selected=result['why_selected'],
        why_not_other_families=result['why_not_other_families'],
    )
    return result

# =============================================================================
# قاموس العرض النهائي - العائلات الأربع فقط
# =============================================================================
PATTERN_ARABIC = PRIMARY_FAMILY_ARABIC.copy()

# =============================================================================
# قاعدة بيانات الإشارات
# =============================================================================
class SignalDatabase:
    REQUIRED_SIGNAL_COLUMNS = {
        "timestamp": "TEXT",
        "symbol": "TEXT",
        "pattern": "TEXT",
        "primary_family": "TEXT",
        "score": "REAL",
        "price": "REAL",
        "direction": "TEXT DEFAULT 'UP'",
        "reasons": "TEXT",
        "secondary_contexts": "TEXT",
        "funding_context": "TEXT",
        "decisive_feature": "TEXT",
        "signal_quality_tier": "TEXT",
        "signal_stage": "TEXT DEFAULT 'WATCH'",
        "why_selected": "TEXT",
        "why_not_other_families": "TEXT",
        "stop_loss": "REAL",
        "take_profit1": "REAL",
        "take_profit2": "REAL",
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
                    if "duplicate column name" not in str(e).lower():
                        raise
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON signals(symbol, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_eval15 ON signals(evaluated_15m)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_family ON signals(primary_family)")

    def _init_db(self):
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    pattern TEXT,
                    primary_family TEXT,
                    score REAL,
                    price REAL,
                    direction TEXT,
                    reasons TEXT,
                    secondary_contexts TEXT,
                    funding_context TEXT,
                    decisive_feature TEXT,
                    signal_quality_tier TEXT,
                    signal_stage TEXT DEFAULT 'WATCH',
                    why_selected TEXT,
                    why_not_other_families TEXT,
                    stop_loss REAL,
                    take_profit1 REAL,
                    take_profit2 REAL,
                    evaluated INTEGER DEFAULT 0,
                    outcome INTEGER DEFAULT 0,
                    price_after REAL
                )
            """)
            self._ensure_signal_schema(conn)

    def save_signal(self, signal_data):
        if not SAVE_SIGNALS:
            return
        primary_family = signal_data.get('primary_family') or signal_data.get('pattern') or 'UNCLASSIFIED'
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._ensure_signal_schema(conn)
            conn.execute("""
                INSERT INTO signals (
                    timestamp, symbol, pattern, primary_family, score, price, direction,
                    reasons, secondary_contexts, funding_context, decisive_feature,
                    signal_quality_tier, signal_stage, why_selected, why_not_other_families,
                    stop_loss, take_profit1, take_profit2
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data.get('timestamp', datetime.now().isoformat()),
                signal_data.get('symbol', ''),
                primary_family,
                primary_family,
                float(signal_data.get('score', 0) or 0),
                float(signal_data.get('price', 0) or 0),
                signal_data.get('direction', 'UP'),
                json.dumps(signal_data.get('reasons', []), ensure_ascii=False),
                json.dumps(signal_data.get('secondary_contexts', []), ensure_ascii=False),
                signal_data.get('funding_context', 'FUNDING_UNINFORMATIVE'),
                signal_data.get('decisive_feature', ''),
                signal_data.get('signal_quality_tier', 'LOW'),
                signal_data.get('signal_stage', 'WATCH'),
                signal_data.get('why_selected', ''),
                json.dumps(signal_data.get('why_not_other_families', {}), ensure_ascii=False),
                float(signal_data.get('stop_loss', 0) or 0),
                float(signal_data.get('take_profit1', 0) or 0),
                float(signal_data.get('take_profit2', 0) or 0),
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
                f"SELECT id, symbol, COALESCE(primary_family, pattern) AS family, price, direction, timestamp FROM signals WHERE {eval_col} = 0 AND timestamp >= ? AND timestamp <= ?",
                (cutoff, max_age)
            )
            return cur.fetchall()

    def get_backtest_summary(self, lookback_hours=72):
        cutoff = (datetime.now() - timedelta(hours=lookback_hours)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("""
                SELECT COALESCE(primary_family, pattern) AS family,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome_5m = 1 THEN 1 ELSE 0 END) AS win_5m,
                       SUM(CASE WHEN outcome_15m = 1 THEN 1 ELSE 0 END) AS win_15m,
                       SUM(CASE WHEN outcome_30m = 1 THEN 1 ELSE 0 END) AS win_30m
                FROM signals
                WHERE timestamp >= ?
                GROUP BY COALESCE(primary_family, pattern)
                ORDER BY total DESC
            """, (cutoff,))
            return cur.fetchall()

    def get_un_evaluated_signals(self, hours_ago=24):
        cutoff = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("SELECT id, symbol, COALESCE(primary_family, pattern), price, direction, timestamp FROM signals WHERE evaluated = 0 AND timestamp < ?", (cutoff,))
            return cur.fetchall()

db = SignalDatabase()

# =============================================================================
# نظام تتبع تحديثات الإشارات
# =============================================================================
class SignalUpdateTracker:
    def __init__(self):
        self.last_signals = {}
        self.lock = threading.Lock()

    def should_alert(self, symbol, new_score, new_primary_family):
        with self.lock:
            now = time.time()
            if symbol not in self.last_signals:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'primary_family': new_primary_family,
                    'timestamp': now
                }
                return True

            last = self.last_signals[symbol]
            if last['primary_family'] != new_primary_family:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'primary_family': new_primary_family,
                    'timestamp': now
                }
                return True

            score_increase = new_score - last['score']
            time_since_last = now - last['timestamp']
            cooldown_seconds = UPDATE_ALERT_COOLDOWN_MINUTES * 60

            if score_increase >= MIN_SCORE_INCREASE_FOR_ALERT and time_since_last >= cooldown_seconds:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'primary_family': new_primary_family,
                    'timestamp': now
                }
                return True
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
    result = []
    try:
        resolved = resolve_primary_family_signal(sym)
        if resolved:
            result.append(resolved)
    except Exception as e:
        diagnostics.log_error(sym, 'resolve_primary_family_signal', str(e))
        if DEBUG:
            print(f"⚠️ خطأ في resolve_primary_family_signal لـ {sym}: {e}")
    return result

# =============================================================================
# دالة إرسال إشارة فردية
# =============================================================================
def send_individual_signal(signal_data):
    """إرسال تنبيه فوري مبني فقط على العائلة الرئيسية الجديدة."""
    if not signal_data.get('execution_allowed', False):
        diagnostics.log_reject(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('primary_family', 'UNKNOWN'), 'signal_not_actionable', {'reason': signal_data.get('print_reason')})
        return

    family = signal_data.get('primary_family', 'UNKNOWN')
    family_info = PRIMARY_FAMILY_ARABIC.get(family, {'name': family, 'emoji': '🔹'})
    direction_ar = '🟢 شراء' if signal_data.get('direction', 'UP') == 'UP' else '🔴 بيع'
    contexts = "، ".join(signal_data.get('secondary_contexts', [])) if signal_data.get('secondary_contexts') else 'لا يوجد'
    why_not = signal_data.get('why_not_other_families', {})
    if isinstance(why_not, dict):
        why_not_text = " | ".join([f"{family_name_ar(k)}: {v}" for k, v in why_not.items()])
    else:
        why_not_text = str(why_not)

    message = (
        f"🔔 *إشارة مرجعية جديدة: {signal_data['symbol']}*\n"
        f"{family_info['emoji']} العائلة الرئيسية: {family_info['name']}\n"
        f"   المرحلة: {stage_name_ar(signal_data.get('signal_stage', 'WATCH'))} | القبول: {signal_data.get('acceptance_state',{}).get('state','N/A')}\n"
        f"   الاتجاه: {direction_ar}\n"
        f"   السعر: {signal_data['price']:.6f}\n"
        f"   التمويل: {funding_name_ar(signal_data.get('funding_context', 'FUNDING_UNINFORMATIVE'))}\n"
        f"   السياقات الثانوية: {contexts}\n"
        f"   العامل الحاسم: {signal_data.get('decisive_feature', '')}\n"
        f"   لماذا اختيرت: {signal_data.get('why_selected', '')}\n"
        f"   لماذا لم تُختر العائلات الأخرى: {why_not_text}\n"
    )
    send_telegram(message)

# =============================================================================
# المسح العميق (معدل لإرسال التنبيهات الفورية)
# =============================================================================
def deep_scan(candidates, learning_system):
    print(f"🔍 تحليل مرجعي صارم لـ {len(candidates)} عملة...")
    clear_invalid_symbols()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(resolve_primary_family_signal, sym): sym for sym in candidates if not is_symbol_invalid(sym)}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                res = future.result(timeout=35)
                if res:
                    family = res.get('primary_family', 'UNKNOWN')
                    print(f"   ✅ {sym}: {family} | stage={res.get('signal_stage')} | execution_allowed={res.get('execution_allowed')}")
                    results.append(res)
                else:
                    if DEBUG:
                        print(f"   … {sym}: لا توجد عائلة رئيسية مؤهلة")
            except Exception as e:
                diagnostics.log_error(sym, 'deep_scan_resolver', str(e))
                print(f"   ❌ {sym}: {str(e)[:80]}")
                if "400" in str(e) or "404" in str(e):
                    mark_symbol_invalid(sym)

    stage_bonus = {'TRIGGERED': 3, 'ARMED': 2, 'PREPARE': 1}
    results.sort(key=lambda x: (stage_bonus.get(x.get('signal_stage', 'WATCH'), 0), fingerprint_quality_rank(x.get('reference_features') or {}, x.get('primary_family'), x.get('signal_stage'))), reverse=True)
    return results

# =============================================================================
# التنبيهات عبر تليجرام (تقرير نهائي)
# =============================================================================
def send_telegram(message):
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}, timeout=10)
    except Exception as e:
        print(f"تليجرام خطأ: {e}")


# [REMOVED_LEGACY] HIERARCHICAL_STATES removed from executable decision/report path.


# [REMOVED_LEGACY] timeframe_label removed from executable decision/report path.


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


def detect_micro_ignition(symbol: str, ref=None):
    """طبقة اشتعال دقيقة 1m/3m لتأكيد الانفجار المبكر دون تحويلها إلى family مستقلة."""
    if not MICRO_IGNITION_ENABLED:
        return {
            'active': False, 'pre_break': False, 'price_1': 0.0, 'price_3': 0.0,
            'vol_ratio': 1.0, 'buy_ratio': 0.5, 'ofi': 0.0, 'trade_ratio': 1.0,
            'breakout': False, 'reason': 'disabled'
        }
    kl = get_klines(symbol, MICRO_IGNITION_INTERVAL, MICRO_IGNITION_LOOKBACK)
    if not kl or len(kl.get('close', [])) < max(8, MICRO_IGNITION_BREAKOUT_BARS + 2):
        return {
            'active': False, 'pre_break': False, 'price_1': 0.0, 'price_3': 0.0,
            'vol_ratio': 1.0, 'buy_ratio': 0.5, 'ofi': 0.0, 'trade_ratio': 1.0,
            'breakout': False, 'reason': 'insufficient_klines'
        }
    closes = kl['close']
    highs = kl['high']
    vols = kl['volume']
    trades = kl.get('trades') or []
    cvd = compute_cvd_features_from_klines(kl) or {}
    price_1 = pct_change(closes[-2], closes[-1]) if len(closes) >= 2 else 0.0
    price_3 = pct_change(closes[-4], closes[-1]) if len(closes) >= 4 else price_1
    base_vol = mean(vols[-(MICRO_IGNITION_BREAKOUT_BARS + 4):-1]) if len(vols) >= (MICRO_IGNITION_BREAKOUT_BARS + 4) else mean(vols[:-1])
    vol_ratio = safe_div(vols[-1], base_vol or 1.0, default=1.0)
    recent_high = max(highs[-(MICRO_IGNITION_BREAKOUT_BARS + 1):-1]) if len(highs) >= (MICRO_IGNITION_BREAKOUT_BARS + 1) else max(highs[:-1])
    breakout = bool(closes[-1] >= recent_high)
    buy_ratio = safe_float(cvd.get('buy_ratio_recent'), 0.5)
    ofi = safe_float(cvd.get('ofi_recent'), 0.0)
    trade_ratio = 1.0
    if len(trades) >= 8:
        trade_ratio = safe_div(mean(trades[-3:]), mean(trades[-8:-3]) or 1.0, default=1.0)
    pre_break = (
        (price_1 >= MICRO_IGNITION_PRICE_MIN or price_3 >= MICRO_IGNITION_PRICE_BURST) and
        (vol_ratio >= max(1.35, MICRO_IGNITION_VOL_RATIO_MIN * 0.82) or trade_ratio >= MICRO_IGNITION_TRADE_BONUS_MIN) and
        (buy_ratio >= 0.53 or ofi >= 0.06)
    )
    active = (
        breakout and
        (price_1 >= MICRO_IGNITION_PRICE_MIN or price_3 >= MICRO_IGNITION_PRICE_BURST) and
        (vol_ratio >= MICRO_IGNITION_VOL_RATIO_MIN or trade_ratio >= MICRO_IGNITION_TRADE_BONUS_MIN) and
        (buy_ratio >= FLOW_MICRO_CONFIRM_BUY or ofi >= FLOW_MICRO_CONFIRM_OFI)
    )
    return {
        'active': active,
        'pre_break': pre_break and not active,
        'price_1': round(price_1, 4),
        'price_3': round(price_3, 4),
        'vol_ratio': round(vol_ratio, 4),
        'buy_ratio': round(buy_ratio, 4),
        'ofi': round(ofi, 4),
        'trade_ratio': round(trade_ratio, 4),
        'breakout': breakout,
        'reason': f"p1={price_1:+.2f}% p3={price_3:+.2f}% vol={vol_ratio:.2f}x buy={buy_ratio:.2f} ofi={ofi:+.2f}"
    }


# [REMOVED_LEGACY] detect_hierarchical_states removed from executable decision/report path.


# [REMOVED_LEGACY] build_hierarchical_decision removed from executable decision/report path.


# [REMOVED_LEGACY] format_hierarchical_block removed from executable decision/report path.


# [REMOVED_LEGACY] generate_hierarchical_report removed from executable decision/report path.

def generate_report(results):
    if not results:
        print("⚠️ لا توجد إشارات.")
        return

    actionable = [r for r in results if r.get('execution_allowed')]
    if not actionable:
        research_results = sorted(
            results,
            key=lambda x: ({'TRIGGERED': 3, 'ARMED': 2, 'PREPARE': 1}.get(x.get('signal_stage', 'WATCH'), 0), fingerprint_quality_rank(x.get('reference_features') or {}, x.get('primary_family'), x.get('signal_stage'))),
            reverse=True
        )[:5]
        print("⚠️ لا توجد إشارات تنفيذية حاليًا. المتاح أدناه discovery only.")
        for r in research_results:
            print(f"   - {r.get('symbol')} | {family_name_ar(r.get('primary_family','UNKNOWN'))} | stage={stage_name_ar(r.get('signal_stage','WATCH'))} | سبب الحجب={r.get('print_reason','N/A')}")
        return

    actionable.sort(
        key=lambda x: ({'TRIGGERED': 3, 'ARMED': 2, 'PREPARE': 1}.get(x.get('signal_stage', 'WATCH'), 0), fingerprint_quality_rank(x.get('reference_features') or {}, x.get('primary_family'), x.get('signal_stage'))),
        reverse=True,
    )
    lines = ["🚀 *التقرير التنفيذي العربي - العائلات المرجعية الأربع*", "", "القرار النهائي يعتمد على fingerprints فقط بدون score.", ""]
    console_lines = ["🚀 التقرير التنفيذي العربي - العائلات المرجعية الأربع", "", "القرار النهائي يعتمد على fingerprints فقط بدون score.", ""]
    green = "\033[92m"
    reset = "\033[0m"

    for r in actionable[:10]:
        family = r.get('primary_family', 'UNKNOWN')
        family_info = PRIMARY_FAMILY_ARABIC.get(family, {'name': family, 'emoji': '🔹'})
        contexts = "، ".join(r.get('secondary_contexts', [])) if r.get('secondary_contexts') else 'لا يوجد'
        why_not = r.get('why_not_other_families', {})
        why_not_text = " | ".join([f"{family_name_ar(k)}: {v}" for k, v in why_not.items()]) if isinstance(why_not, dict) else str(why_not)
        judgment = execution_judgment_ar(r)
        persistence_text = persistence_name_ar(r.get('persistence_cycles', 1))
        acceptance = r.get('acceptance_state', {})
        continuation = r.get('continuation_state', {})
        exhaustion = r.get('exhaustion_risk', {})
        memory = r.get('behavioral_memory', {})

        lines.append(
            f"{family_info['emoji']} *{r['symbol']}*\n"
            f"   العائلة الرئيسية: {family_info['name']}\n"
            f"   الحكم التنفيذي: {judgment}\n"
            f"   المرحلة: {stage_name_ar(r.get('signal_stage', 'WATCH'))} | execution_allowed={r.get('execution_allowed')}\n"
            f"   acceptance: {acceptance.get('state','N/A')} ({acceptance.get('reason','')})\n"
            f"   continuation: {'FAILED' if continuation.get('failed') else 'OK'} ({continuation.get('reason','')})\n"
            f"   exhaustion: {'RISK' if exhaustion.get('risky') else 'OK'} ({exhaustion.get('reason','')})\n"
            f"   behavioral memory: {'VETO' if memory.get('veto') else 'OK'} ({memory.get('reason','')})\n"
            f"   حالة OI: {oi_state_name_ar(r.get('oi_state', 'NEUTRAL'))} | الثبات: {persistence_text} ({r.get('persistence_cycles', 1)})\n"
            f"   السعر: {r['price']:.6f}\n"
            f"   سياق التمويل: {funding_name_ar(r.get('funding_context', 'FUNDING_UNINFORMATIVE'))}\n"
            f"   السياقات الثانوية: {contexts}\n"
            f"   العامل الحاسم: {r.get('decisive_feature', '')}\n"
            f"   سبب الاختيار: {r.get('why_selected', '')}\n"
            f"   أسباب استبعاد العائلات الأخرى: {why_not_text}\n"
        )
        console_lines.append(
            f"{family_info['emoji']} {r['symbol']}\n"
            f"   العائلة الرئيسية: {family_info['name']}\n"
            f"   {green}الحكم التنفيذي: {judgment}{reset}\n"
            f"   المرحلة: {stage_name_ar(r.get('signal_stage', 'WATCH'))} | execution_allowed={r.get('execution_allowed')}\n"
            f"   acceptance={acceptance.get('state','N/A')} | continuation={'FAILED' if continuation.get('failed') else 'OK'} | exhaustion={'RISK' if exhaustion.get('risky') else 'OK'} | memory={'VETO' if memory.get('veto') else 'OK'}\n"
            f"   حالة OI: {oi_state_name_ar(r.get('oi_state', 'NEUTRAL'))} | الثبات: {persistence_text} ({r.get('persistence_cycles', 1)})\n"
            f"   السعر: {r['price']:.6f}\n"
            f"   سياق التمويل: {funding_name_ar(r.get('funding_context', 'FUNDING_UNINFORMATIVE'))}\n"
            f"   السياقات الثانوية: {contexts}\n"
            f"   العامل الحاسم: {r.get('decisive_feature', '')}\n"
            f"   سبب الاختيار: {r.get('why_selected', '')}\n"
            f"   أسباب استبعاد العائلات الأخرى: {why_not_text}\n"
        )
        try:
            db.save_signal(r)
        except Exception as e:
            diagnostics.log_error(r.get('symbol', 'UNKNOWN'), 'save_signal', str(e))
            if DEBUG:
                print(f"⚠️ فشل حفظ الإشارة لـ {r.get('symbol', 'UNKNOWN')}: {e}")

    full = "\n".join(lines)
    console_full = "\n".join(console_lines)
    print(console_full)
    send_telegram(full)

def generate_backtest_report(lookback_hours=BACKTEST_LOOKBACK_HOURS):
    rows = db.get_backtest_summary(lookback_hours=lookback_hours)
    if not rows:
        print("📈 لا توجد بيانات كافية للباك تست بعد.")
        return
    print("\n📈 ملخص الباك تست المحلي حسب primary_family:")
    for family, total, win5, win15, win30 in rows[:10]:
        total = total or 0
        r5 = (win5 / total * 100) if total else 0
        r15 = (win15 / total * 100) if total else 0
        r30 = (win30 / total * 100) if total else 0
        print(f"   {family}: total={total} | 5m={r5:.1f}% | 15m={r15:.1f}% | 30m={r30:.1f}%")

# =============================================================================
# الحلقة الرئيسية
# =============================================================================
def main():
    print("="*70)
    print("الماسح المرجعي المنظف - Four-Family Reference Engine V3".center(70))
    print("="*70)
    if ENABLE_DIAGNOSTICS:
        print(f"🧪 سجل التشخيص: {DIAGNOSTICS_PATH}")

    all_symbols = get_all_usdt_perpetuals()
    global TRADING_SYMBOLS_SET
    TRADING_SYMBOLS_SET = set(all_symbols)
    print(f"✅ إجمالي العقود: {len(all_symbols)}")

    if ENABLE_HMM:
        print("🔄 تدريب نموذج HMM...")
        hmm_detector.fit_model()
        regime = hmm_detector.get_current_regime()
        print(f"📊 حالة السوق الحالية: {regime['regime_name']}")

    learning_system = LearningSystem(db)

    while True:
        start_time = time.time()
        global CYCLE_SNAPSHOT_TS
        CYCLE_SNAPSHOT_TS = datetime.now().replace(microsecond=0).isoformat()
        with CYCLE_CACHE_LOCK:
            CYCLE_DYNAMIC_CACHE.clear()
            CYCLE_CONTEXT_CACHE.clear()
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
