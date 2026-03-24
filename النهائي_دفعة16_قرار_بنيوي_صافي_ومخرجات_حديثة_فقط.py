#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ماسح فرص لعقود Binance Futures USDT-M
- يلتزم فقط بالعائلات الأربع المحددة
- يلتزم فقط بالمراحل الخمس المحددة
- يفصل بوضوح بين الاكتشاف Discovery وقابلية التنفيذ Actionability
- مبني ليعمل دوريًا في بيئة محدودة الموارد مثل Pydroid 3

ملخص التعديلات الرئيسية في هذه النسخة:
- إضافة CVD تقريبي من شموع 5m لقياس استدامة التدفق بدل الاكتفاء بالشمعة الحالية.
- توسيع تحليل OI إلى OI dynamics متسلسل مع حالات squeeze_buildup / squeeze_covering / squeeze_post_covering.
- إضافة Z-score ديناميكي للمؤشرات الرئيسية مع fallback آمن عندما لا تتوفر عينة كافية.
- تعميق تحليل العلاقة بين Positions و Accounts باستخدام القيم المطلقة واتجاه الفجوة بينهما.
- دمج 15m و 4h داخل build_reference_market_features و stage classification.
- إعادة بناء القرار النهائي وفق التسلسل: تهيؤ ← اشتعال ← قبول ← استمرار ← قرار تنفيذي.
- الإبقاء على الهيكل العام للملف والوحدات الأصلية مع توسيعها بدل استبدالها.
- تعديل أولوية القرار بعد حالة ANKRUSDT: OI dynamics و Position-Account gap أصبحا أهم من CVD،
  مع استخدام CVD كعامل تعزيز لا كشرط حاسم للقبول أو الفشل.
- تلوين الحالات القابلة للدخول باللون الأخضر عند الطباعة الطرفية لتسهيل الالتقاط البصري.
- تمت إضافة ذاكرة محلية كاملة لكل أصل، وذاكرة حالات مرجعية، وانتقالات حالة، وطبع سلوكي تاريخي يدخل في التفسير والترجيح.
- إضافة حماية type-safety لنتائج الـ API عبر safe_list_from_api و safe_dict_from_api لمنع أخطاء .get على قيم غير متوقعة.

ملاحظة مهمة:
هذا الملف يستخدم فقط REST العامة الخاصة بـ Binance Futures، ويُبقي المنطق دفاعيًا
ضد نقص البيانات أو تعطل بعض الطلبات أو ظهور رموز غير مستقرة.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, median, pstdev
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 1) الإعدادات الثابتة
# ============================================================

STATIC_SETTINGS: Dict[str, Any] = {
    "BINANCE_FAPI_BASE": "https://fapi.binance.com",
    "HTTP_TIMEOUT_SEC": 8,
    "HTTP_CONNECT_TIMEOUT_SEC": 4,
    "USER_AGENT": "Mozilla/5.0 Binance-Futures-Opportunity-Scanner/1.0",
    "REQUEST_RETRIES_TOTAL": 3,
    "REQUEST_BACKOFF_FACTOR": 0.35,
    "STATUS_FORCELIST": [429, 500, 502, 503, 504],
    "PRINT_HORIZONTAL_LINE": "=" * 90,
}


# ============================================================
# 2) الإعدادات الديناميكية
# ============================================================

DYNAMIC_SETTINGS: Dict[str, Any] = {
    "SCAN_LOOP_ENABLED": False,
    "SCAN_LOOP_INTERVAL_SEC": 120,
    "FULL_UNIVERSE_SCAN_ENABLED": False,
    "MAX_CANDIDATES_AFTER_FILTER": 18,
    "TOP_N_BY_24H_VOLUME": 60,
    "MIN_24H_QUOTE_VOLUME": 5_000_000.0,
    "MIN_24H_TRADE_COUNT": 3_000,
    "MIN_24H_PRICE_CHANGE_PCT": 0.35,
    "MAX_24H_PRICE_CHANGE_PCT": 18.0,
    "MIN_MARK_PRICE": 0.00001,
    "KLINE_LIMIT_5M": 120,
    "KLINE_LIMIT_15M": 72,
    "KLINE_LIMIT_1H": 96,
    "KLINE_LIMIT_4H": 36,
    "OI_HIST_LIMIT": 100,
    "OI_HIST_LIMIT_15M": 60,
    "OI_HIST_LIMIT_1H": 48,
    "OI_HIST_LIMIT_4H": 36,
    "RATIO_LIMIT": 18,
    "RATIO_LIMIT_15M": 18,
    "RATIO_LIMIT_1H": 18,
    "RATIO_LIMIT_4H": 18,
    "FUNDING_LIMIT": 6,
    "BASIS_LIMIT": 18,
    "MAX_WORKERS": 6,
    "CYCLE_CACHE_TTL_SEC": 60,
    "LONGER_CACHE_TTL_SEC": 600,
    "DEBUG_HTTP_ERRORS": False,
    # مفاتيح التوافق مع البيئات المحدودة
    "ENABLE_CVD": True,
    "ENABLE_ZSCORE": True,
    "ENABLE_MULTI_TF": True,
    "ENABLE_SQUEEZE_LOGIC": True,
    "ZSCORE_LOOKBACK": 100,
    "CVD_WINDOW_5M": 24,
    "OI_SEQUENCE_WINDOW": 12,
    "HOLD_CONFIRM_BARS": 2,
    "ENABLE_ASSET_LOCAL_MEMORY": True,
    "ASSET_MEMORY_DIR": "asset_memory_store",
    "ASSET_MEMORY_MAX_POINTS": 320,
    "ASSET_MEMORY_MIN_POINTS_FOR_PROFILE": 24,
    "ASSET_MEMORY_RECENT_WINDOW": 12,
    "ASSET_CASE_MEMORY_MAX_CASES": 180,
    "ASSET_CASE_SIMILARITY_TOP_K": 3,
    "ASSET_TRANSITION_MEMORY_MAX": 120,
    "ASSET_BEHAVIOR_MIN_CASES": 18,
}


# ============================================================
# 3) قواعد العائلات
# ============================================================

FAMILY_RULES: Dict[str, Any] = {
    "POSITION_LED_SQUEEZE_BUILDUP": {
        "position_lead_min": 0.03,
        "position_over_account_gap": 0.02,
        "allow_with_short_crowding": True,
    },
    "ACCOUNT_LED_ACCUMULATION": {
        "account_lead_min": 0.03,
        "account_over_position_gap": 0.02,
        "global_delta_min": 0.01,
        "prefer_early_price": True,
    },
    "CONSENSUS_BULLISH_EXPANSION": {
        "consensus_min_move": 0.02,
        "max_internal_disagreement": 0.09,
    },
    "FLOW_LIQUIDITY_VACUUM_BREAKOUT": {
        "min_taker_buy_ratio": 0.56,
        "min_trade_expansion": 1.35,
        "min_volume_expansion": 1.35,
    },
    "ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION": {
        "account_lead_min": 0.03,
        "global_delta_min": 0.01,
        "max_internal_disagreement": 0.09,
        "allow_quiet_positive_funding": True,
    },
}


# ============================================================
# 4) قواعد التنفيذ
# ============================================================

EXECUTION_RULES: Dict[str, Any] = {
    # --- OI ---
    "oi_buildup_pct": 1.0,
    "oi_expansion_pct": 2.0,
    "oi_flat_abs_pct": 0.6,
    "oi_collapse_pct": -1.0,
    "premove_price_flat_pct": 1.0,
    "oi_buildup_z_threshold": 1.5,
    "buy_ratio_z_threshold": 1.5,
    "volume_expansion_z_threshold": 1.5,
    "trade_expansion_z_threshold": 1.5,
    "cvd_slope_z_threshold": 1.5,
    "cvd_ratio_z_threshold": 1.5,

    # --- Price / Breakout / Acceptance ---
    "breakout_buffer_pct": 0.12,
    "hold_buffer_pct": 0.06,
    "ignition_return_5m_pct": 0.40,
    "strong_bar_return_5m_pct": 0.80,
    "max_clean_extension_atr": 1.80,
    "max_clean_extension_pct": 3.20,
    "acceptance_cvd_ratio_min": 0.10,
    "acceptance_cvd_persistence_min": 0.50,
    "min_position_lead_for_support": 0.05,
    "min_oi_increase_for_support": 1.0,

    # --- Flow / Activity ---
    "strong_taker_buy_ratio": 0.56,
    "persistent_taker_buy_ratio": 0.53,
    "min_trade_expansion": 1.35,
    "min_volume_expansion": 1.35,
    "min_trade_activity_hold": 1.10,
    "min_cvd_positive_for_support": 0.0,

    # --- Crowding / Funding / Basis ---
    "crowded_short_global_ratio": 0.95,
    "crowded_long_global_ratio": 1.20,
    "funding_quiet_abs": 0.0005,
    "funding_non_crowded_abs": 0.0010,
    "funding_hostile_positive": 0.0018,
    "funding_deep_negative": -0.0018,
    "funding_squeeze_negative_pct": -0.00005,
    "basis_neutral_abs": 0.0008,
    "basis_supportive_abs": 0.0020,

}


SPECIAL_PATTERN_RULES: Dict[str, Any] = {
    "COUNTERFLOW_SHORT_SQUEEZE_EXPANSION": {
        "account_long_ratio_min": 0.58,
        "account_lead_min": 0.03,
        "position_long_ratio_max": 0.49,
        "position_ratio_max": 0.98,
        "oi_delta_1_min": 2.0,
        "oi_delta_3_min": 6.0,
        "oi_up_ratio_min": 0.60,
        "trade_expansion_min": 1.20,
        "volume_expansion_min": 1.20,
        "min_pattern_score": 5,
    },
}


POST_FLUSH_PATTERN_RULES: Dict[str, Any] = {
    "flush_drop_pct": -6.0,
    "violent_range_pct": 10.0,
    "relief_rebound_pct": 6.0,
    "rebuild_attempt_pct": 5.0,
    "oi_strong_contraction_1": -2.0,
    "oi_strong_contraction_3": -3.0,
    "oi_rebuild_1": 1.0,
    "oi_rebuild_3": 2.0,
    "oi_up_ratio_min": 0.60,
    "failed_rejection_from_high_pct": 3.0,
    "relief_min_score": 7,
    "bullish_min_score": 9,
    "failed_min_score": 8,
}


EXTREME_ENGINE_RULES: Dict[str, Any] = {
    "upper_range_pos": 0.82,
    "lower_range_pos": 0.18,
    "extreme_range_pos_strict": 0.90,
    "relative_extreme_buffer": 0.015,
    "absolute_upper_ratio": 1.15,
    "absolute_lower_ratio": 0.90,
    "component_absolute_guides": {
        "position": {"upper": 1.15, "lower": 0.90},
        "account": {"upper": 1.12, "lower": 0.88},
        "global": {"upper": 1.08, "lower": 0.92},
        "taker": {"upper": 1.03, "lower": 0.97},
    },
    "inflection_delta_eps": 0.01,
    "inflection_trend_up": 0.60,
    "inflection_trend_down": 0.40,
    "higher_tf_min_agree": 2,
    "lower_squeeze_min_score": 5,
    "upper_bullish_min_score": 5,
    "upper_failure_min_score": 5,
    "long_rollover_min_score": 5,
    "oi_counter_min_score": 4,
    "rollover_position_floor": 1.18,
    "rollover_global_floor": 1.10,
    "rollover_account_ceiling": 1.20,
}

# ============================================================
# 5) إعدادات الأداء
# ============================================================

PERFORMANCE_SETTINGS: Dict[str, Any] = {
    "CACHE_EXCHANGE_INFO_TTL": 900,
    "CACHE_MARK_PRICE_TTL": 15,
    "CACHE_24H_TICKER_TTL": 15,
    "CACHE_BASIS_TTL": 20,
    "CACHE_FUNDING_TTL": 30,
    "FREEZE_CYCLE_SNAPSHOT": True,
    "ENABLE_THREADPOOL": True,
}

OUTPUT_SETTINGS: Dict[str, Any] = {
    "PRINT_IMPORTANT_ONLY": False,
    "IMPORTANT_STRUCTURAL_STATES": [
        "actionable_now",
        "discovered_not_actionable",
        "watch_for_acceptance",
        "watch_for_rebuild",
        "no_trade_flow_trap",
        "no_trade_structural_conflict",
        "late",
        "failed",
    ],
    "PRINT_MAGMA_PATTERN_ONLY": False,
    "MAGMA_PATTERN_NAME": "COUNTERFLOW_SHORT_SQUEEZE_EXPANSION",
    "HIGHLIGHT_MAGMA_IN_RED": True,
    "SORT_BY_IMPORTANCE_ASC": True,
    "MAX_PRINT_RESULTS": None,
    "SHOW_LEGACY_CLASSIFIERS": False,
    "SHOW_LEGACY_SUMMARY": False,
}

STRUCTURAL_EXECUTION_ORDER: Dict[str, int] = {
    "actionable_now": 0,
    "watch_for_acceptance": 1,
    "watch_for_rebuild": 2,
    "discovered_not_actionable": 3,
    "no_trade_flow_trap": 4,
    "no_trade_structural_conflict": 5,
    "late": 6,
    "failed": 7,
}


# ============================================================
# 5.1) الدستور البحثي والطبقة البنيوية العليا
# ============================================================

BINANCE_DEFINITION_LAYER: Dict[str, str] = {
    "oi": "Open Interest يقيس اتساع الانكشاف المفتوح، وليس الاتجاه وحده",
    "oi_value": "Open Interest Value يقيس القيمة الاسمية للانكشاف المفتوح",
    "positions_ratio": "Top Trader Positions Ratio يقيس تموضع الأحجام لدى أعلى 20% من المستخدمين بحسب رصيد الهامش",
    "accounts_ratio": "Top Trader Accounts Ratio يقيس اتساع الانحياز بين الحسابات لدى أعلى 20% من المستخدمين بحسب رصيد الهامش",
    "global_ratio": "Global Long/Short Ratio يقيس مشاركة عموم السوق على مستوى الحسابات لا الأحجام",
    "taker_flow": "Taker Buy/Sell يقيس الضغط التنفيذي المنفذ داخل الفترة",
    "funding": "التمويل طبقة ازدحام وسياق، لا محرك اتجاهي مستقل",
}

RESEARCH_CONSTITUTION: Dict[str, str] = {
    "core_rule": "تُقرأ Acco وPosit وRatio كسلسلة بنيوية لا كقيم جامدة",
    "sequence_rule": "الموضع داخل النطاق ← الانعطاف ← علاقة OI ← حكم السعر ← جودة التدفق ← القبول أو الفشل ← النتيجة البنيوية",
    "oi_rule": "OI وقود للحركة وليس اتجاهًا بذاته",
    "extreme_rule": "التطرف حافة نظام لا انعكاسًا تلقائيًا",
    "trigger_rule": "الزناد الحقيقي ينتج من الانعطاف مع OI وحكم السعر",
    "leadership_rule": "قيادة المراكز أرجح من قيادة الحسابات في كشف البناء الحجمي المبكر",
    "acceptance_rule": "الاختراق ليس حكمًا نهائيًا؛ القبول بعد الاختراق هو الحكم",
    "flow_rule": "التدفق القوي غير كافٍ من دون قبول وبناء بنيوي",
    "htf_rule": "الفريم الأعلى يحدد النظام البنيوي، والأدنى يحدد الزناد",
    "memory_rule": "يجب قراءة الأصل مقارنة بدوراته السابقة: هل يتحسن أم يتدهور أم ينتقل بين الأنظمة",
}

STRUCTURAL_EXECUTION_STATES = {
    "actionable_now",
    "discovered_not_actionable",
    "watch_for_acceptance",
    "watch_for_rebuild",
    "no_trade_flow_trap",
    "no_trade_crowded_late",
    "no_trade_structural_conflict",
    "late",
    "failed",
}



STRUCTURAL_BUCKET_MAP: Dict[str, str] = {
    "actionable_now": "Actionable now",
    "watch_for_acceptance": "Discovered but not actionable",
    "watch_for_rebuild": "Discovered but not actionable",
    "discovered_not_actionable": "Discovered but not actionable",
    "no_trade_structural_conflict": "Discovered but not actionable",
    "no_trade_flow_trap": "Failed",
    "no_trade_crowded_late": "Late",
    "late": "Late",
    "failed": "Failed",
}


def apply_structural_center(result: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(result)
    execution_verdict = safe_dict_from_api(result.get("execution_verdict"))
    structural_case = safe_dict_from_api(result.get("structural_case"))
    structural_thesis = safe_dict_from_api(result.get("structural_thesis"))
    adversarial_review = safe_dict_from_api(result.get("adversarial_review"))

    if not execution_verdict:
        return result

    result["legacy_decision_snapshot"] = {
        "execution_state": result.get("execution_state"),
        "final_bucket": result.get("final_bucket"),
        "actionable_now": result.get("actionable_now"),
        "not_actionable_reason": result.get("not_actionable_reason"),
        "decision_reason": result.get("decision_reason"),
    }

    structural_state = _safe_state_text(execution_verdict.get("execution_state"), "discovered_not_actionable")
    structural_state_ar = _safe_state_text(execution_verdict.get("execution_state_ar"), "مرصودة لكنها غير قابلة للتنفيذ بعد")
    structural_reason = _safe_state_text(execution_verdict.get("reason_ar"), "البنية موجودة لكن التنفيذ غير مكتمل بعد")
    mapped_bucket = STRUCTURAL_BUCKET_MAP.get(structural_state, "Discovered but not actionable")

    result["decision_center"] = "structural"
    result["structural_execution_state"] = structural_state
    result["structural_execution_state_ar"] = structural_state_ar
    result["structural_execution_reason_ar"] = structural_reason

    result["execution_state"] = structural_state_ar
    result["final_bucket"] = mapped_bucket
    result["actionable_now"] = structural_state == "actionable_now"
    result["not_actionable_reason"] = structural_reason if structural_state != "actionable_now" else ""

    if structural_case:
        result["regime_pattern"] = _safe_state_text(structural_case.get("regime_pattern"), result.get("regime_pattern"))
        result["execution_pattern"] = _safe_state_text(structural_case.get("execution_state"), result.get("execution_pattern"))
        result["importance_score"] = max(
            safe_float(result.get("importance_score"), 0.0),
            safe_float(structural_case.get("importance_score"), 0.0),
        )
        if structural_case.get("primary_hypothesis"):
            result["primary_hypothesis"] = structural_case.get("primary_hypothesis")
        if structural_case.get("alternative_hypotheses"):
            result["alternative_hypotheses"] = structural_case.get("alternative_hypotheses")

    if structural_thesis.get("summary_ar"):
        result["decision_reason"] = structural_thesis.get("summary_ar")
    elif structural_reason:
        result["decision_reason"] = structural_reason

    if adversarial_review.get("flip_condition_ar"):
        result["invalidation_reason_ar"] = adversarial_review.get("flip_condition_ar")

    return result


@dataclass
class StructuralCase:
    symbol: str
    regime_pattern: str = ""
    regime_pattern_ar: str = ""
    regime_confidence: float = 0.0

    leader_type: str = ""
    leader_type_ar: str = ""
    leader_evidence: List[str] = field(default_factory=list)

    oi_role: str = ""
    oi_role_ar: str = ""
    oi_supportive: bool = False

    flow_quality: str = ""
    flow_quality_ar: str = ""
    flow_persistent: bool = False

    breakout_state: str = ""
    acceptance_state: str = ""
    price_verdict: str = ""
    price_verdict_ar: str = ""

    primary_hypothesis: str = ""
    primary_hypothesis_ar: str = ""
    alternative_hypotheses: List[str] = field(default_factory=list)
    alternative_hypotheses_ar: List[str] = field(default_factory=list)

    invalidation_signals: List[str] = field(default_factory=list)
    next_failure_mode: str = ""

    execution_state: str = ""
    execution_state_ar: str = ""
    no_trade_reason: str = ""

    causal_chain: List[str] = field(default_factory=list)

    confidence_score: float = 0.0
    uncertainty_score: float = 0.0
    conflict_score: float = 0.0
    importance_score: float = 0.0

    thesis_quality: str = ""
    thesis_quality_ar: str = ""
    thesis_confidence: float = 0.0

    adversarial_warning: str = ""
    adversarial_warning_ar: str = ""
    adversarial_trap: str = ""
    adversarial_trap_ar: str = ""
    flip_condition: str = ""


FAMILIES = (
    "POSITION_LED_SQUEEZE_BUILDUP",
    "ACCOUNT_LED_ACCUMULATION",
    "CONSENSUS_BULLISH_EXPANSION",
    "FLOW_LIQUIDITY_VACUUM_BREAKOUT",
    "ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION",
)

STAGES = ("WATCH", "PREPARE", "ARMED", "TRIGGERED", "LATE")
OI_STATES = (
    "premove buildup",
    "buildup expansion",
    "neutral",
    "covering",
    "price up OI flat",
    "squeeze_buildup",
    "squeeze_covering",
    "squeeze_post_covering",
)
FINAL_BUCKETS_ORDER = {
    "Actionable now": 0,
    "Discovered but not actionable": 1,
    "Late": 2,
    "Failed": 3,
}


def compute_importance_score(result: Dict[str, Any]) -> float:
    """درجة أهمية بنيوية للطباعة والترتيب فقط، ولا تحكم القرار التنفيذي."""
    features = safe_dict_from_api(result.get("market_features"))
    structural_state = _safe_state_text(
        result.get("structural_execution_state")
        or safe_dict_from_api(result.get("execution_verdict")).get("execution_state"),
        "discovered_not_actionable",
    )
    structural_thesis = safe_dict_from_api(result.get("structural_thesis"))
    structural_case = safe_dict_from_api(result.get("structural_case"))
    acceptance_lifecycle = safe_dict_from_api(result.get("acceptance_lifecycle"))
    flow_mismatch = safe_dict_from_api(result.get("flow_quality_mismatch"))
    adversarial_review = safe_dict_from_api(result.get("adversarial_review"))
    ratio_alignment = safe_dict_from_api(result.get("ratio_alignment") or features.get("ratio_alignment"))
    extreme_engine = safe_dict_from_api(result.get("extreme_engine") or features.get("extreme_engine"))
    ratio_conflict = safe_dict_from_api(result.get("ratio_conflict") or features.get("ratio_conflict"))

    state_weight = {
        "actionable_now": 420.0,
        "watch_for_acceptance": 320.0,
        "watch_for_rebuild": 295.0,
        "discovered_not_actionable": 250.0,
        "no_trade_structural_conflict": 120.0,
        "no_trade_flow_trap": 90.0,
        "no_trade_crowded_late": 70.0,
        "late": 60.0,
        "failed": 0.0,
    }.get(structural_state, 0.0)

    thesis_conf = safe_float(structural_thesis.get("top_score"), safe_float(structural_case.get("thesis_confidence"), 0.0))
    thesis_quality = _safe_state_text(structural_thesis.get("thesis_quality"), _safe_state_text(structural_case.get("thesis_quality"), "fragile"))
    thesis_weight = {"coherent": 44.0, "usable": 28.0, "contested": 14.0, "fragile": 0.0}.get(thesis_quality, 0.0)
    confidence_component = thesis_conf * 60.0

    acceptance_quality = _safe_state_text(acceptance_lifecycle.get("acceptance_quality"), "unaccepted")
    acceptance_weight = {
        "clean_repricing": 36.0,
        "rebuild_reacceptance": 28.0,
        "early_reacceptance": 18.0,
        "fragile_acceptance": 14.0,
        "failed_acceptance": -24.0,
        "unaccepted": 0.0,
    }.get(acceptance_quality, 0.0)

    oi_supportive = bool(structural_case.get("oi_supportive", result.get("oi_supportive", False)))
    oi_weight = 16.0 if oi_supportive else 0.0

    flow_persistent = bool(structural_case.get("flow_persistent", False))
    flow_weight = 14.0 if flow_persistent else 0.0
    if flow_mismatch.get("mismatch", False):
        flow_weight -= 22.0

    not_late_weight = 8.0 if not features.get("price_late", False) else -18.0

    vol_component = min(max(safe_float(features.get("vol_expansion_last"), 0.0), 0.0), 3.0) * 4.0
    trade_component = min(max(safe_float(features.get("trade_expansion_last"), 0.0), 0.0), 3.0) * 4.0
    oi_component = min(max(safe_float(features.get("oi_delta_3"), 0.0), 0.0), 12.0) * 1.5
    gap_component = max(safe_float((result.get("position_account_gap") or {}).get("position_lead"), 0.0), 0.0) * 120.0

    alignment_bonus = 0.0
    if ratio_alignment.get("overall_supportive", False):
        alignment_bonus += 16.0
    if ratio_alignment.get("higher_tf_supportive", False):
        alignment_bonus += 10.0
    if ratio_alignment.get("overall_conflict", False):
        alignment_bonus -= 18.0
    if ratio_alignment.get("higher_tf_conflict", False):
        alignment_bonus -= 10.0
    conflict_penalty = -10.0 * safe_float(ratio_conflict.get("penalty"), 0.0)
    conflict_penalty -= safe_float(adversarial_review.get("skepticism_score"), 0.0) * 24.0

    patterns = safe_dict_from_api(extreme_engine.get("patterns"))
    squeeze_probability = safe_float(extreme_engine.get("squeeze_probability"), 0.0)
    flush_probability = safe_float(extreme_engine.get("flush_probability"), 0.0)
    extreme_bonus = 0.0
    if safe_dict_from_api(patterns.get("lower_extreme_squeeze")).get("active", False):
        extreme_bonus += 22.0 + (squeeze_probability * 0.20)
    if safe_dict_from_api(patterns.get("upper_extreme_bullish_expansion")).get("active", False):
        extreme_bonus += 16.0 + (squeeze_probability * 0.14)
    if safe_dict_from_api(patterns.get("upper_extreme_failure")).get("active", False):
        extreme_bonus -= 24.0 + (flush_probability * 0.18)
    if safe_dict_from_api(patterns.get("long_crowding_rollover")).get("active", False):
        extreme_bonus -= 20.0 + (flush_probability * 0.15)
    if safe_dict_from_api(patterns.get("oi_expansion_without_bullish_confirmation")).get("active", False):
        extreme_bonus -= 14.0 + (flush_probability * 0.10)

    post_flush = safe_dict_from_api(result.get("post_flush_structures"))
    post_flush_bonus = 0.0
    if post_flush.get("pattern") == "bullish_rebuild_after_flush":
        post_flush_bonus += 56.0 + safe_float(post_flush.get("confidence"), 0.0) * 28.0
    elif post_flush.get("pattern") == "relief_bounce_after_flush":
        post_flush_bonus -= 24.0 + safe_float(post_flush.get("confidence"), 0.0) * 18.0
    elif post_flush.get("pattern") == "failed_rebuild_after_flush":
        post_flush_bonus -= 60.0 + safe_float(post_flush.get("confidence"), 0.0) * 24.0

    behavior_context = safe_dict_from_api(result.get("behavior_profile_context"))
    behavior_bonus = 0.0
    if behavior_context.get("available", False):
        behavior_bonus += safe_float(behavior_context.get("decision_bias_bonus"), 0.0) * 70.0
        behavior_bonus -= safe_float(behavior_context.get("decision_bias_penalty"), 0.0) * 70.0

    return round(
        state_weight
        + thesis_weight
        + confidence_component
        + acceptance_weight
        + oi_weight
        + flow_weight
        + not_late_weight
        + vol_component
        + trade_component
        + oi_component
        + gap_component
        + alignment_bonus
        + conflict_penalty
        + extreme_bonus
        + post_flush_bonus
        + behavior_bonus,
        2,
    )

def filter_and_sort_results_for_output(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered = list(results)
    if OUTPUT_SETTINGS.get("PRINT_IMPORTANT_ONLY", False):
        allowed = set(
            OUTPUT_SETTINGS.get(
                "IMPORTANT_STRUCTURAL_STATES",
                [
                    "actionable_now",
                    "discovered_not_actionable",
                    "watch_for_acceptance",
                    "watch_for_rebuild",
                    "no_trade_flow_trap",
                    "no_trade_structural_conflict",
                    "late",
                    "failed",
                ],
            )
        )
        filtered = [
            row for row in filtered
            if _safe_state_text(row.get("structural_execution_state"), "unknown") in allowed
        ]

    if OUTPUT_SETTINGS.get("PRINT_MAGMA_PATTERN_ONLY", False):
        filtered = [
            row for row in filtered
            if ((row.get("market_features") or {}).get("counterflow_pattern") or {}).get("active", False)
        ]

    sort_asc = OUTPUT_SETTINGS.get("SORT_BY_IMPORTANCE_ASC", True)
    filtered.sort(
        key=lambda r: (
            STRUCTURAL_EXECUTION_ORDER.get(_safe_state_text(r.get("structural_execution_state"), "failed"), 99),
            safe_float(r.get("importance_score"), 0.0),
            r.get("symbol", ""),
        ),
        reverse=False if sort_asc else False,
    )

    max_results = OUTPUT_SETTINGS.get("MAX_PRINT_RESULTS")
    if isinstance(max_results, int) and max_results > 0:
        filtered = filtered[:max_results]
    return filtered


# ============================================================
# Helpers عامة
# ============================================================


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(value)
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        return int(float(value))
    except Exception:
        return default


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if abs(b) < 1e-12:
        return default
    return a / b

def safe_list_from_api(data: Any) -> List[Any]:
    """تأكد من أن البيانات القادمة من الـ API هي قائمة، وإلا أرجع قائمة فارغة."""
    return data if isinstance(data, list) else []


def safe_dict_from_api(data: Any) -> Dict[str, Any]:
    """تأكد من أن البيانات القادمة من الـ API هي قاموس، وإلا أرجع قاموسًا فارغًا."""
    return data if isinstance(data, dict) else {}


def pct_change(new_value: float, old_value: float, default: float = 0.0) -> float:
    if abs(old_value) < 1e-12:
        return default
    return ((new_value - old_value) / old_value) * 100.0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def avg(values: List[float], default: float = 0.0) -> float:
    clean = [v for v in values if v is not None]
    return mean(clean) if clean else default


def med(values: List[float], default: float = 0.0) -> float:
    clean = [v for v in values if v is not None]
    return median(clean) if clean else default


def std(values: List[float], default: float = 0.0) -> float:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return default
    return pstdev(clean)


def tail(values: List[Any], count: int) -> List[Any]:
    return values[-count:] if len(values) >= count else values[:]


def rolling_max(values: List[float], count: int) -> float:
    if not values:
        return 0.0
    subset = tail(values, count)
    return max(subset) if subset else 0.0


def format_pct(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}%"


def format_num(value: Optional[float], digits: int = 4) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def bool_to_ar(value: Optional[bool], partial: bool = False) -> str:
    if partial:
        return "جزئي"
    if value is True:
        return "نعم"
    if value is False:
        return "لا"
    return "N/A"



def supports_ansi_colors() -> bool:
    try:
        return sys.stdout.isatty() and os.environ.get("TERM", "") not in {"", "dumb"}
    except Exception:
        return False


def colorize_text(text: str, color: str) -> str:
    colors = {
        "green": "\033[92m",
        "yellow": "\033[93m",
        "red": "\033[91m",
        "reset": "\033[0m",
    }
    if not supports_ansi_colors():
        return text
    prefix = colors.get(color)
    if not prefix:
        return text
    return f"{prefix}{text}{colors['reset']}"

def ensure_dir_for_file(filepath: str) -> None:
    directory = os.path.dirname(os.path.abspath(filepath))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def load_json_file(filepath: str, default: Any) -> Any:
    try:
        if not os.path.exists(filepath):
            return default
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_atomic(filepath: str, payload: Any) -> None:
    ensure_dir_for_file(filepath)
    tmp_path = f"{filepath}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, filepath)


def _sanitize_symbol_for_filename(symbol: str) -> str:
    allowed = []
    for ch in str(symbol or ""):
        if ch.isalnum() or ch in {'_', '-'}:
            allowed.append(ch)
        else:
            allowed.append('_')
    return ''.join(allowed) or 'unknown_symbol'


def get_asset_memory_path(symbol: str) -> str:
    base_dir = DYNAMIC_SETTINGS.get("ASSET_MEMORY_DIR", "asset_memory_store")
    filename = f"{_sanitize_symbol_for_filename(symbol)}.json"
    return os.path.join(base_dir, filename)


def _trim_memory_series(values: List[Any], max_points: int) -> List[Any]:
    if max_points <= 0:
        return values
    return values[-max_points:]


def _build_distribution_summary(values: List[float]) -> Dict[str, Any]:
    clean = [safe_float(v) for v in values if v is not None]
    count = len(clean)
    if count == 0:
        return {
            "count": 0,
            "min": 0.0,
            "max": 0.0,
            "q10": 0.0,
            "q25": 0.0,
            "q50": 0.0,
            "q75": 0.0,
            "q90": 0.0,
            "mean": 0.0,
            "recent_mean": 0.0,
        }
    recent_window = safe_int(DYNAMIC_SETTINGS.get("ASSET_MEMORY_RECENT_WINDOW", 12), 12)
    return {
        "count": count,
        "min": min(clean),
        "max": max(clean),
        "q10": _compute_quantile(clean, 0.10, clean[0]),
        "q25": _compute_quantile(clean, 0.25, clean[0]),
        "q50": _compute_quantile(clean, 0.50, clean[0]),
        "q75": _compute_quantile(clean, 0.75, clean[-1]),
        "q90": _compute_quantile(clean, 0.90, clean[-1]),
        "mean": avg(clean, 0.0),
        "recent_mean": avg(tail(clean, recent_window), 0.0),
    }


def load_asset_local_memory(symbol: str) -> Dict[str, Any]:
    if not DYNAMIC_SETTINGS.get("ENABLE_ASSET_LOCAL_MEMORY", True):
        return {"symbol": symbol, "timeframes": {}, "global": {}, "last_snapshot": {}}
    filepath = get_asset_memory_path(symbol)
    data = load_json_file(filepath, {"symbol": symbol, "timeframes": {}, "global": {}, "last_snapshot": {}})
    if not isinstance(data, dict):
        return {"symbol": symbol, "timeframes": {}, "global": {}, "last_snapshot": {}}
    data.setdefault("symbol", symbol)
    data.setdefault("timeframes", {})
    data.setdefault("global", {})
    data.setdefault("last_snapshot", {})
    return data


def build_asset_local_memory_profile(symbol: str, memory_data: Dict[str, Any]) -> Dict[str, Any]:
    profile = {
        "symbol": symbol,
        "available": False,
        "timeframes": {},
        "global": {},
        "last_snapshot": safe_dict_from_api(memory_data.get("last_snapshot")),
    }
    min_points = safe_int(DYNAMIC_SETTINGS.get("ASSET_MEMORY_MIN_POINTS_FOR_PROFILE", 24), 24)
    for tf_name, tf_payload in safe_dict_from_api(memory_data.get("timeframes")).items():
        tf_payload = safe_dict_from_api(tf_payload)
        tf_profile: Dict[str, Any] = {}
        enough_any = False
        for key, values in tf_payload.items():
            if not isinstance(values, list):
                continue
            dist = _build_distribution_summary(values)
            dist["enough"] = dist["count"] >= min_points
            tf_profile[key] = dist
            enough_any = enough_any or dist["enough"]
        tf_profile["available"] = enough_any
        profile["timeframes"][tf_name] = tf_profile
        profile["available"] = profile["available"] or enough_any
    for key, values in safe_dict_from_api(memory_data.get("global")).items():
        if not isinstance(values, list):
            continue
        dist = _build_distribution_summary(values)
        dist["enough"] = dist["count"] >= min_points
        profile["global"][key] = dist
        profile["available"] = profile["available"] or dist["enough"]
    return profile


def _describe_relative_location(current_value: float, dist: Dict[str, Any]) -> str:
    if not dist or safe_int(dist.get("count"), 0) <= 0:
        return "غير متاح تاريخيًا"
    q10 = safe_float(dist.get("q10"), current_value)
    q25 = safe_float(dist.get("q25"), current_value)
    q50 = safe_float(dist.get("q50"), current_value)
    q75 = safe_float(dist.get("q75"), current_value)
    q90 = safe_float(dist.get("q90"), current_value)
    if current_value <= q10:
        return "عند تطرف سفلي تاريخيًا"
    if current_value <= q25:
        return "في الربع السفلي تاريخيًا"
    if current_value >= q90:
        return "عند تطرف علوي تاريخيًا"
    if current_value >= q75:
        return "في الربع العلوي تاريخيًا"
    if current_value >= q50:
        return "فوق الوسيط التاريخي"
    return "دون الوسيط التاريخي"


def summarize_asset_memory_context(symbol: str, ratio_multi_tf: Dict[str, Dict[str, Any]], multi_tf: Dict[str, Dict[str, Any]], memory_profile: Dict[str, Any]) -> Dict[str, Any]:
    if not (memory_profile or {}).get("available", False):
        return {
            "available": False,
            "summary": "لا توجد ذاكرة محلية كافية لهذا الأصل بعد.",
            "per_tf": {},
            "change_state": "memory_building",
            "change_state_ar": "الذاكرة المحلية لهذا الأصل ما زالت قيد البناء.",
        }

    per_tf: Dict[str, Any] = {}
    improving = 0
    deteriorating = 0
    diagnostics: List[str] = []
    for tf_name in ("5m", "15m", "1h", "4h"):
        tf_ratio = safe_dict_from_api(ratio_multi_tf.get(tf_name))
        tf_mem = safe_dict_from_api((memory_profile.get("timeframes") or {}).get(tf_name))
        pos_last = safe_float(safe_dict_from_api(tf_ratio.get("position")).get("last"), 0.0)
        acc_last = safe_float(safe_dict_from_api(tf_ratio.get("account")).get("last"), 0.0)
        glob_last = safe_float(safe_dict_from_api(tf_ratio.get("global")).get("last"), 0.0)
        oi_delta_3 = safe_float(safe_dict_from_api(multi_tf.get(tf_name)).get("oi_delta_3"), 0.0)
        pos_desc = _describe_relative_location(pos_last, safe_dict_from_api(tf_mem.get("position_last")))
        acc_desc = _describe_relative_location(acc_last, safe_dict_from_api(tf_mem.get("account_last")))
        glob_desc = _describe_relative_location(glob_last, safe_dict_from_api(tf_mem.get("global_last")))
        oi_desc = _describe_relative_location(oi_delta_3, safe_dict_from_api(tf_mem.get("oi_delta_3")))

        pos_recent = safe_float(safe_dict_from_api(tf_mem.get("position_last")).get("recent_mean"), pos_last)
        acc_recent = safe_float(safe_dict_from_api(tf_mem.get("account_last")).get("recent_mean"), acc_last)
        oi_recent = safe_float(safe_dict_from_api(tf_mem.get("oi_delta_3")).get("recent_mean"), oi_delta_3)

        tf_change = []
        if pos_last > pos_recent and acc_last > acc_recent and oi_delta_3 >= oi_recent:
            improving += 1
            tf_change.append("يتحسن بنيويًا مقارنة بمتوسطه المحلي القريب")
        elif pos_last < pos_recent and acc_last < acc_recent and oi_delta_3 < oi_recent:
            deteriorating += 1
            tf_change.append("يتدهور بنيويًا مقارنة بمتوسطه المحلي القريب")
        else:
            tf_change.append("يتحرك بصورة مختلطة مقارنة بتاريخه القريب")

        text = f"على فريم {tf_name}: بوزيشن {pos_desc}، وأكاونت {acc_desc}، والنسبة العامة {glob_desc}، وOIΔ3 {oi_desc}، والحالة الحالية {tf_change[0]}."
        per_tf[tf_name] = {
            "position_desc": pos_desc,
            "account_desc": acc_desc,
            "global_desc": glob_desc,
            "oi_desc": oi_desc,
            "change_desc": tf_change[0],
            "text": text,
        }
        diagnostics.append(text)

    change_state = "mixed"
    change_state_ar = "الأصل يتحرك بصورة مختلطة عبر الفريمات مقارنة بذاكرته المحلية."
    if improving >= 2 and deteriorating == 0:
        change_state = "improving"
        change_state_ar = "الأصل يتحسن بنيويًا مقارنة بذاكرته المحلية عبر أكثر من فريم."
    elif deteriorating >= 2 and improving == 0:
        change_state = "deteriorating"
        change_state_ar = "الأصل يتدهور بنيويًا مقارنة بذاكرته المحلية عبر أكثر من فريم."

    summary = " ".join([per_tf[tf]["text"] for tf in ("4h", "1h", "15m", "5m") if tf in per_tf])
    return {
        "available": True,
        "summary": summary,
        "per_tf": per_tf,
        "change_state": change_state,
        "change_state_ar": change_state_ar,
        "diagnostics": diagnostics,
    }


def _safe_state_text(value: Any, default: str = "غير معروف") -> str:
    if value is None or value == "":
        return default
    return str(value)


def build_asset_case_vector(features: Dict[str, Any], result: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    ratio_multi_tf = safe_dict_from_api(features.get("ratio_multi_tf"))
    multi_tf = safe_dict_from_api(features.get("multi_tf"))

    def _ratio(tf: str, bucket: str, key: str = "last") -> float:
        return safe_float(safe_dict_from_api(safe_dict_from_api(ratio_multi_tf.get(tf)).get(bucket)).get(key), 0.0)

    def _tf(tf: str, key: str) -> float:
        return safe_float(safe_dict_from_api(multi_tf.get(tf)).get(key), 0.0)

    vector = {
        "pos_5m": _ratio("5m", "position"),
        "acc_5m": _ratio("5m", "account"),
        "glob_5m": _ratio("5m", "global"),
        "pos_1h": _ratio("1h", "position"),
        "acc_1h": _ratio("1h", "account"),
        "glob_1h": _ratio("1h", "global"),
        "pos_4h": _ratio("4h", "position"),
        "acc_4h": _ratio("4h", "account"),
        "glob_4h": _ratio("4h", "global"),
        "gap_pos_5m": safe_float(safe_dict_from_api(safe_dict_from_api(ratio_multi_tf.get("5m")).get("gap")).get("position_lead"), 0.0),
        "gap_acc_5m": safe_float(safe_dict_from_api(safe_dict_from_api(ratio_multi_tf.get("5m")).get("gap")).get("account_lead"), 0.0),
        "oi_5m": safe_float(features.get("oi_delta_3"), 0.0),
        "oi_1h": _tf("1h", "oi_delta_3"),
        "oi_4h": _tf("4h", "oi_delta_3"),
        "ret_5m": safe_float(features.get("ret_3"), 0.0),
        "ret_1h": _tf("1h", "ret_3"),
        "ret_4h": _tf("4h", "ret_3"),
        "buy_ratio": safe_float(features.get("recent_buy_ratio"), 0.0),
        "vol_exp": safe_float(features.get("vol_expansion_last"), 0.0),
        "trade_exp": safe_float(features.get("trade_expansion_last"), 0.0),
    }
    if result is not None:
        vector["importance_score"] = safe_float(result.get("importance_score"), 0.0)
        vector["confidence_score"] = safe_float(result.get("confidence_score"), 0.0)
    return vector


def _compute_case_similarity_score(current_vector: Dict[str, float], historical_vector: Dict[str, Any]) -> float:
    shared_keys = [k for k in current_vector.keys() if k in historical_vector]
    if not shared_keys:
        return 0.0
    scores: List[float] = []
    for key in shared_keys:
        current_val = safe_float(current_vector.get(key), 0.0)
        hist_val = safe_float(historical_vector.get(key), 0.0)
        scale = max(abs(current_val), abs(hist_val), 1.0)
        diff = abs(current_val - hist_val) / scale
        scores.append(max(0.0, 1.0 - min(diff, 1.0)))
    return round(avg(scores, 0.0), 3)


def build_asset_state_transition_context(memory_data: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    last_snapshot = safe_dict_from_api(memory_data.get("last_snapshot"))
    current_regime = _safe_state_text(result.get("regime_pattern"), "NEUTRAL_REGIME")
    current_trigger = _safe_state_text(result.get("trigger_pattern"), "NO_TRIGGER")
    current_execution = _safe_state_text(result.get("execution_pattern"), "OBSERVE_ONLY")
    current_bucket = _safe_state_text(result.get("final_bucket"), "Failed")
    current_hypothesis = _safe_state_text(result.get("primary_hypothesis"), "unknown")

    if not last_snapshot or not last_snapshot.get("regime_pattern"):
        return {
            "available": False,
            "transition_key": "initial_observation",
            "transition_ar": "هذه أول ملاحظة محفوظة لهذا الأصل داخل الذاكرة المحلية.",
            "previous_snapshot": {},
            "current_snapshot": {
                "regime_pattern": current_regime,
                "trigger_pattern": current_trigger,
                "execution_pattern": current_execution,
                "final_bucket": current_bucket,
                "primary_hypothesis": current_hypothesis,
            },
        }

    prev_regime = _safe_state_text(last_snapshot.get("regime_pattern"), "NEUTRAL_REGIME")
    prev_trigger = _safe_state_text(last_snapshot.get("trigger_pattern"), "NO_TRIGGER")
    prev_execution = _safe_state_text(last_snapshot.get("execution_pattern"), "OBSERVE_ONLY")
    prev_bucket = _safe_state_text(last_snapshot.get("final_bucket"), "Failed")
    prev_hypothesis = _safe_state_text(last_snapshot.get("primary_hypothesis"), "unknown")

    transition_key = "state_shift"
    if prev_bucket != "Actionable now" and current_bucket == "Actionable now":
        transition_key = "promotion_to_actionable"
        transition_ar = "انتقل الأصل من حالة غير قابلة للتنفيذ إلى حالة قابلة للتنفيذ الآن."
    elif prev_bucket in {"Actionable now", "Discovered but not actionable", "Late"} and current_bucket == "Failed":
        transition_key = "deterioration_to_failed"
        transition_ar = "تدهورت الحالة من وضع قابل للمراقبة أو التنفيذ إلى حالة فشل واضحة."
    elif prev_regime != current_regime and current_regime in {"crowded_short_regime", "crowded_long_regime", "counterflow_regime", "mixed_regime"}:
        transition_key = "regime_transition"
        transition_ar = f"انتقل الأصل من نظام {prev_regime} إلى نظام {current_regime}."
    elif prev_trigger != current_trigger and current_trigger != "NO_TRIGGER":
        transition_key = "trigger_transition"
        transition_ar = f"انتقل الأصل من زناد {prev_trigger} إلى زناد {current_trigger}."
    elif prev_hypothesis != current_hypothesis:
        transition_key = "hypothesis_rotation"
        transition_ar = f"تغيرت الفرضية الأقوى من {prev_hypothesis} إلى {current_hypothesis}."
    elif prev_execution != current_execution:
        transition_key = "execution_shift"
        transition_ar = f"تغير الوضع التنفيذي من {prev_execution} إلى {current_execution}."
    else:
        transition_key = "state_stable"
        transition_ar = "بقي الأصل ضمن نفس البنية العامة دون انتقال نوعي حاسم مقارنة بآخر دورة محفوظة."

    return {
        "available": True,
        "transition_key": transition_key,
        "transition_ar": transition_ar,
        "previous_snapshot": {
            "regime_pattern": prev_regime,
            "trigger_pattern": prev_trigger,
            "execution_pattern": prev_execution,
            "final_bucket": prev_bucket,
            "primary_hypothesis": prev_hypothesis,
        },
        "current_snapshot": {
            "regime_pattern": current_regime,
            "trigger_pattern": current_trigger,
            "execution_pattern": current_execution,
            "final_bucket": current_bucket,
            "primary_hypothesis": current_hypothesis,
        },
    }


def build_asset_case_similarity_context(memory_data: Dict[str, Any], features: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    cases = safe_list_from_api(memory_data.get("cases"))
    if not cases:
        return {
            "available": False,
            "summary_ar": "لا توجد حالات مرجعية مشابهة محفوظة لهذا الأصل بعد.",
            "top_cases": [],
        }

    current_vector = build_asset_case_vector(features, result)
    scored_cases = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        sim = _compute_case_similarity_score(current_vector, safe_dict_from_api(case.get("vector")))
        enriched = dict(case)
        enriched["similarity"] = sim
        scored_cases.append(enriched)

    scored_cases.sort(key=lambda x: (safe_float(x.get("similarity"), 0.0), safe_float(x.get("importance_score"), 0.0)), reverse=True)
    top_k = safe_int(DYNAMIC_SETTINGS.get("ASSET_CASE_SIMILARITY_TOP_K", 3), 3)
    top_cases = scored_cases[:top_k]
    if not top_cases:
        return {
            "available": False,
            "summary_ar": "لا توجد حالات مرجعية مشابهة محفوظة لهذا الأصل بعد.",
            "top_cases": [],
        }

    parts = []
    for case in top_cases:
        hypothesis = _safe_state_text(case.get("primary_hypothesis_ar") or case.get("primary_hypothesis"), "غير معروف")
        bucket = _safe_state_text(case.get("final_bucket_ar") or case.get("final_bucket"), "غير معروف")
        similarity = safe_float(case.get("similarity"), 0.0)
        parts.append(f"حالة مشابهة بدرجة {similarity:.2f}: فرضيتها كانت {hypothesis} وانتهت إلى {bucket}")
    return {
        "available": True,
        "summary_ar": " | ".join(parts),
        "top_cases": top_cases,
    }




def _categorize_hypothesis_behavior(hypothesis: Any) -> str:
    h = _safe_state_text(hypothesis, "unknown")
    if h in {"lower_extreme_short_squeeze_up", "counterflow_short_squeeze_expansion"}:
        return "squeeze_bias"
    if h in {"position_led_buildup", "account_led_accumulation", "consensus_bullish_expansion", "upper_extreme_bullish_expansion"}:
        return "bullish_build_bias"
    if h in {"bullish_rebuild_after_flush"}:
        return "rebuild_bias"
    if h in {"relief_bounce_after_flush"}:
        return "relief_bias"
    if h in {"failed_rebuild_after_flush", "upper_extreme_failed_acceptance", "long_crowding_rollover", "oi_expansion_without_bullish_confirmation", "late_blowoff"}:
        return "flush_failure_bias"
    return "mixed_bias"


def _behavior_category_ar(category: str) -> str:
    mapping = {
        "squeeze_bias": "يميل إلى حركات العصر الصاعد والانفجارات الناتجة عن ضغط التموضع",
        "bullish_build_bias": "يميل إلى البناءات الصاعدة التدريجية أو التوسعات التوافقية",
        "rebuild_bias": "يميل إلى إعادة البناء الصاعد بعد التفريغ أكثر من الارتداد الهش",
        "relief_bias": "يميل إلى الارتدادات التقنية بعد التفريغ أكثر من إعادة البناء الكاملة",
        "flush_failure_bias": "يميل إلى فشل القبول والانقلابات الهابطة أو التفريغات الحادة",
        "mixed_bias": "يميل إلى سلوك مختلط وغير ثابت النمط",
    }
    return mapping.get(category, "يميل إلى سلوك مختلط وغير ثابت النمط")


def _behavior_fit_text(current_category: str, dominant_category: str, final_bucket: str) -> str:
    if current_category == dominant_category and dominant_category != "mixed_bias":
        return "الحالة الحالية منسجمة مع الطبع التاريخي الغالب لهذا الأصل."
    if dominant_category == "flush_failure_bias" and final_bucket in {"Actionable now", "Discovered but not actionable"}:
        return "الحالة الحالية تخالف ميل الأصل التاريخي للفشل؛ لذا يلزم حذر إضافي قبل رفع الثقة."
    if dominant_category in {"squeeze_bias", "rebuild_bias", "bullish_build_bias"} and final_bucket == "Failed":
        return "الحالة الحالية أضعف من السلوك الإيجابي المعتاد لهذا الأصل وتوحي بانحراف سلبي عن طبعه التاريخي."
    return "الحالة الحالية لا تتطابق بالكامل مع الطبع التاريخي لهذا الأصل، لكنها لا تناقضه بصورة حاسمة أيضًا."


def build_asset_behavior_profile_context(memory_data: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    cases = safe_list_from_api(memory_data.get("cases"))
    transitions = safe_list_from_api(memory_data.get("transitions"))
    min_cases = safe_int(DYNAMIC_SETTINGS.get("ASSET_BEHAVIOR_MIN_CASES", 18), 18)
    if len(cases) < min_cases:
        return {
            "available": False,
            "summary_ar": "لا توجد بعد ذاكرة سلوكية محلية كافية لبناء طبع واضح لهذا الأصل.",
            "fit_ar": "طبع الأصل ما زال قيد البناء مع تراكم الحالات المحلية.",
            "fit_state": "building",
            "decision_bias_bonus": 0.0,
            "decision_bias_penalty": 0.0,
        }

    bucket_counts: Dict[str, int] = defaultdict(int)
    leader_counts: Dict[str, int] = defaultdict(int)
    behavior_counts: Dict[str, int] = defaultdict(int)
    confidence_values: List[float] = []
    importance_values: List[float] = []

    for case in cases:
        if not isinstance(case, dict):
            continue
        bucket = _safe_state_text(case.get("final_bucket"), "unknown")
        leader = _safe_state_text(case.get("leader_type"), "undetermined")
        hypothesis = _safe_state_text(case.get("primary_hypothesis"), "unknown")
        category = _categorize_hypothesis_behavior(hypothesis)
        bucket_counts[bucket] += 1
        leader_counts[leader] += 1
        behavior_counts[category] += 1
        confidence_values.append(safe_float(case.get("confidence_score"), 0.0))
        importance_values.append(safe_float(case.get("importance_score"), 0.0))

    total = max(len(cases), 1)
    actionable_rate = safe_div(bucket_counts.get("Actionable now", 0), total, 0.0)
    discovered_rate = safe_div(bucket_counts.get("Discovered but not actionable", 0), total, 0.0)
    failed_rate = safe_div(bucket_counts.get("Failed", 0), total, 0.0)
    late_rate = safe_div(bucket_counts.get("Late", 0), total, 0.0)

    dominant_behavior = max(behavior_counts.items(), key=lambda x: x[1])[0] if behavior_counts else "mixed_bias"
    dominant_leader = max(leader_counts.items(), key=lambda x: x[1])[0] if leader_counts else "undetermined"

    unstable_keys = {"promotion_to_actionable", "deterioration_to_failed", "regime_transition", "trigger_transition", "hypothesis_rotation", "execution_shift"}
    unstable_count = sum(1 for t in transitions if _safe_state_text(safe_dict_from_api(t).get("transition_key"), "") in unstable_keys)
    instability_rate = safe_div(unstable_count, max(len(transitions), 1), 0.0) if transitions else 0.0

    avg_conf = avg(confidence_values, 0.0)
    avg_importance = avg(importance_values, 0.0)
    global_memory = safe_dict_from_api(memory_data.get("global"))
    avg_conflict = avg([safe_float(v, 0.0) for v in safe_list_from_api(global_memory.get("conflict_score"))], 0.0)
    avg_uncertainty = avg([safe_float(v, 0.0) for v in safe_list_from_api(global_memory.get("uncertainty_score"))], 0.0)

    if avg_conflict <= 0.22 and avg_uncertainty <= 0.28 and instability_rate <= 0.35:
        structural_style_ar = "هذا الأصل يميل إلى بنية أنظف نسبيًا وأكثر قابلية للقراءة."
        structural_style = "clean"
    elif avg_conflict >= 0.45 or avg_uncertainty >= 0.45 or instability_rate >= 0.60:
        structural_style_ar = "هذا الأصل يميل إلى بنية ضوضائية ومتقلبة، ويحتاج حذرًا أعلى عند التنفيذ."
        structural_style = "noisy"
    else:
        structural_style_ar = "هذا الأصل يقع في منطقة وسط بين البنية النظيفة والبنية الضوضائية."
        structural_style = "mixed"

    leader_mapping = {
        "counterflow_led": "قيادة عكسية مع إعادة تسعير ضد التموضع الظاهر",
        "position_led": "القيادة للمراكز",
        "account_led": "القيادة للحسابات",
        "consensus_led": "قيادة توافقية بين المراكز والحسابات",
        "flow_led": "القيادة للتدفق والتنفيذ",
        "mixed_led": "قيادة مختلطة ومتعارضة",
        "undetermined": "قيادة غير محسومة بعد",
    }
    dominant_leader_ar = leader_mapping.get(dominant_leader, "قيادة غير محسومة بعد")

    current_category = _categorize_hypothesis_behavior(result.get("primary_hypothesis"))
    fit_ar = _behavior_fit_text(current_category, dominant_behavior, _safe_state_text(result.get("final_bucket"), "Failed"))

    fit_state = "neutral"
    decision_bias_bonus = 0.0
    decision_bias_penalty = 0.0
    if current_category == dominant_behavior and dominant_behavior != "mixed_bias":
        fit_state = "aligned"
        decision_bias_bonus += 0.10 if structural_style == "clean" else 0.06
    elif current_category != "mixed_bias" and dominant_behavior != "mixed_bias":
        fit_state = "misaligned"
        decision_bias_penalty += 0.08 if structural_style == "clean" else 0.05
    if _safe_state_text(result.get("final_bucket"), "Failed") == "Actionable now" and actionable_rate >= 0.35:
        decision_bias_bonus += 0.04
    if _safe_state_text(result.get("final_bucket"), "Failed") == "Actionable now" and failed_rate > actionable_rate and structural_style == "noisy":
        decision_bias_penalty += 0.06

    summary_ar = (
        f"الطبع التاريخي الغالب لهذا الأصل: {_behavior_category_ar(dominant_behavior)}. "
        f"معدل الوصول إلى حالة قابلة للتنفيذ يبلغ {actionable_rate:.2f}، "
        f"ومعدل الحالات المكتشفة غير الجاهزة {discovered_rate:.2f}، "
        f"ومعدل الفشل {failed_rate:.2f}، "
        f"ومعدل التأخر {late_rate:.2f}. "
        f"القيادة الأكثر شيوعًا تاريخيًا: {dominant_leader_ar}. "
        f"متوسط الثقة التاريخي {avg_conf:.2f} ومتوسط الأهمية {avg_importance:.2f}. "
        f"{structural_style_ar}"
    )

    return {
        "available": True,
        "dominant_behavior": dominant_behavior,
        "dominant_behavior_ar": _behavior_category_ar(dominant_behavior),
        "dominant_leader": dominant_leader,
        "dominant_leader_ar": dominant_leader_ar,
        "actionable_rate": round(actionable_rate, 2),
        "discovered_rate": round(discovered_rate, 2),
        "failed_rate": round(failed_rate, 2),
        "late_rate": round(late_rate, 2),
        "avg_confidence": round(avg_conf, 2),
        "avg_importance": round(avg_importance, 2),
        "avg_conflict": round(avg_conflict, 2),
        "avg_uncertainty": round(avg_uncertainty, 2),
        "instability_rate": round(instability_rate, 2),
        "structural_style": structural_style,
        "structural_style_ar": structural_style_ar,
        "summary_ar": summary_ar,
        "fit_ar": fit_ar,
        "fit_state": fit_state,
        "decision_bias_bonus": round(decision_bias_bonus, 3),
        "decision_bias_penalty": round(decision_bias_penalty, 3),
    }


def apply_behavior_profile_influence(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    إدخال طبع الأصل داخل الترجيح النهائي:
    لا ينسف القرار الحالي، لكنه يضيف/يخصم من الثقة والأهمية إذا كانت
    الحالة الحالية منسجمة جدًا أو متعارضة جدًا مع السلوك التاريخي للأصل.
    """
    behavior = safe_dict_from_api(result.get("behavior_profile_context"))
    if not behavior.get("available", False):
        return result

    bonus = safe_float(behavior.get("decision_bias_bonus"), 0.0)
    penalty = safe_float(behavior.get("decision_bias_penalty"), 0.0)
    fit_state = _safe_state_text(behavior.get("fit_state"), "neutral")
    current_conf = safe_float(result.get("confidence_score"), 0.0)
    current_unc = safe_float(result.get("uncertainty_score"), 0.0)
    current_imp = safe_float(result.get("importance_score"), 0.0)

    adjusted_conf = clamp(current_conf + bonus - penalty, 0.05, 0.99)
    adjusted_unc = clamp(current_unc + (penalty * 0.55) - (bonus * 0.30), 0.02, 0.99)
    adjusted_imp = max(0.0, current_imp + (bonus * 70.0) - (penalty * 70.0))

    result["confidence_score"] = round(adjusted_conf, 2)
    result["uncertainty_score"] = round(adjusted_unc, 2)
    result["importance_score"] = round(adjusted_imp, 2)
    result["behavior_fit_state"] = fit_state
    result["behavior_bias_bonus"] = round(bonus, 3)
    result["behavior_bias_penalty"] = round(penalty, 3)
    return result


def update_asset_local_memory(symbol: str, features: Dict[str, Any], result: Optional[Dict[str, Any]] = None) -> None:
    if not DYNAMIC_SETTINGS.get("ENABLE_ASSET_LOCAL_MEMORY", True):
        return
    memory_data = load_asset_local_memory(symbol)
    memory_data["symbol"] = symbol
    memory_data.setdefault("timeframes", {})
    memory_data.setdefault("global", {})
    memory_data.setdefault("cases", [])
    memory_data.setdefault("transitions", [])
    max_points = safe_int(DYNAMIC_SETTINGS.get("ASSET_MEMORY_MAX_POINTS", 320), 320)

    ratio_multi_tf = safe_dict_from_api(features.get("ratio_multi_tf"))
    multi_tf = safe_dict_from_api(features.get("multi_tf"))
    for tf_name in ("5m", "15m", "1h", "4h"):
        tf_store = safe_dict_from_api(memory_data["timeframes"].get(tf_name))
        tf_ratio = safe_dict_from_api(ratio_multi_tf.get(tf_name))
        tf_market = safe_dict_from_api(multi_tf.get(tf_name))
        updates = {
            "position_last": safe_float(safe_dict_from_api(tf_ratio.get("position")).get("last"), 0.0),
            "account_last": safe_float(safe_dict_from_api(tf_ratio.get("account")).get("last"), 0.0),
            "global_last": safe_float(safe_dict_from_api(tf_ratio.get("global")).get("last"), 0.0),
            "taker_last": safe_float(safe_dict_from_api(tf_ratio.get("taker")).get("last"), 0.0),
            "position_lead": safe_float(safe_dict_from_api(tf_ratio.get("gap")).get("position_lead"), 0.0),
            "account_lead": safe_float(safe_dict_from_api(tf_ratio.get("gap")).get("account_lead"), 0.0),
            "oi_delta_1": safe_float(tf_market.get("oi_delta_1"), 0.0),
            "oi_delta_3": safe_float(tf_market.get("oi_delta_3"), 0.0),
            "ret_3": safe_float(tf_market.get("ret_3"), 0.0),
            "ret_12": safe_float(tf_market.get("ret_12"), 0.0),
        }
        for key, value in updates.items():
            series = tf_store.get(key, [])
            if not isinstance(series, list):
                series = []
            series.append(value)
            tf_store[key] = _trim_memory_series(series, max_points)
        memory_data["timeframes"][tf_name] = tf_store

    global_store = safe_dict_from_api(memory_data.get("global"))
    global_updates = {
        "vol_expansion_last": safe_float(features.get("vol_expansion_last"), 0.0),
        "trade_expansion_last": safe_float(features.get("trade_expansion_last"), 0.0),
        "recent_buy_ratio": safe_float(features.get("recent_buy_ratio"), 0.0),
        "oi_delta_3_5m": safe_float(features.get("oi_delta_3"), 0.0),
        "funding_current": safe_float(features.get("funding_current"), 0.0),
    }
    if result is not None:
        global_updates.update({
            "importance_score": safe_float(result.get("importance_score"), 0.0),
            "confidence_score": safe_float(result.get("confidence_score"), 0.0),
            "conflict_score": safe_float(result.get("conflict_score"), 0.0),
            "uncertainty_score": safe_float(result.get("uncertainty_score"), 0.0),
        })
    for key, value in global_updates.items():
        series = global_store.get(key, [])
        if not isinstance(series, list):
            series = []
        series.append(value)
        global_store[key] = _trim_memory_series(series, max_points)
    memory_data["global"] = global_store

    if result is not None:
        transition_context = build_asset_state_transition_context(memory_data, result)
        transition_entry = {
            "updated_at": utc_now_iso(),
            "transition_key": transition_context.get("transition_key"),
            "transition_ar": transition_context.get("transition_ar"),
            "previous_snapshot": transition_context.get("previous_snapshot", {}),
            "current_snapshot": transition_context.get("current_snapshot", {}),
        }
        transitions = memory_data.get("transitions", [])
        if not isinstance(transitions, list):
            transitions = []
        transitions.append(transition_entry)
        memory_data["transitions"] = _trim_memory_series(transitions, safe_int(DYNAMIC_SETTINGS.get("ASSET_TRANSITION_MEMORY_MAX", 120), 120))

        case_entry = {
            "timestamp": utc_now_iso(),
            "regime_pattern": result.get("regime_pattern"),
            "trigger_pattern": result.get("trigger_pattern"),
            "execution_pattern": result.get("execution_pattern"),
            "final_bucket": result.get("final_bucket"),
            "final_bucket_ar": result.get("final_bucket_ar"),
            "primary_hypothesis": result.get("primary_hypothesis"),
            "primary_hypothesis_ar": result.get("primary_hypothesis_ar"),
            "leader_type": result.get("leader_type"),
            "importance_score": safe_float(result.get("importance_score"), 0.0),
            "confidence_score": safe_float(result.get("confidence_score"), 0.0),
            "vector": build_asset_case_vector(features, result),
        }
        cases = memory_data.get("cases", [])
        if not isinstance(cases, list):
            cases = []
        cases.append(case_entry)
        memory_data["cases"] = _trim_memory_series(cases, safe_int(DYNAMIC_SETTINGS.get("ASSET_CASE_MEMORY_MAX_CASES", 180), 180))

    memory_data["last_snapshot"] = {
        "updated_at": utc_now_iso(),
        "final_bucket": (result or {}).get("final_bucket"),
        "primary_hypothesis": (result or {}).get("primary_hypothesis"),
        "regime_pattern": (result or {}).get("regime_pattern"),
        "trigger_pattern": (result or {}).get("trigger_pattern"),
        "execution_pattern": (result or {}).get("execution_pattern"),
        "importance_score": safe_float((result or {}).get("importance_score"), 0.0),
    }
    save_json_atomic(get_asset_memory_path(symbol), memory_data)


def load_asset_memory(symbol: str) -> Dict[str, Any]:
    """واجهة V2 مطلوبة: تعيد ذاكرة الأصل المحلية."""
    return load_asset_local_memory(symbol)


def build_asset_behavior_profile(symbol: str, features: Dict[str, Any], memory: Dict[str, Any]) -> Dict[str, Any]:
    """واجهة V2 مطلوبة: ملف سلوكي مركّز لكل أصل."""
    profile = build_asset_local_memory_profile(symbol, memory)
    behavior_context = build_asset_behavior_profile_context(memory, {"primary_hypothesis": None, "final_bucket": "Discovered but not actionable"})
    return {
        "symbol": symbol,
        "available": profile.get("available", False),
        "timeframes": profile.get("timeframes", {}),
        "global": profile.get("global", {}),
        "behavior_context": behavior_context,
        "squeeze_prone": behavior_context.get("dominant_behavior") == "squeeze_bias",
        "flush_prone": behavior_context.get("dominant_behavior") == "flush_failure_bias",
        "fakeout_prone": safe_float(features.get("noise_metric"), 0.0) >= 2.0,
        "counterflow_prone": behavior_context.get("dominant_leader") == "counterflow_led",
    }


def update_asset_memory(symbol: str, features: Dict[str, Any], result: Dict[str, Any]) -> None:
    """واجهة V2 مطلوبة: تحديث ذاكرة الأصل."""
    update_asset_local_memory(symbol, features, result)


def classify_relative_extreme(value: float, distribution: Dict[str, Any]) -> Dict[str, Any]:
    q10 = safe_float(distribution.get("q10"), value)
    q25 = safe_float(distribution.get("q25"), value)
    q75 = safe_float(distribution.get("q75"), value)
    q90 = safe_float(distribution.get("q90"), value)
    if value >= q90:
        state = "upper_extreme"
    elif value >= q75:
        state = "upper_band"
    elif value <= q10:
        state = "lower_extreme"
    elif value <= q25:
        state = "lower_band"
    else:
        state = "neutral"
    return {"state": state, "value": value, "distribution": distribution}


def build_ratio_extreme_profile(features: Dict[str, Any], asset_memory: Dict[str, Any]) -> Dict[str, Any]:
    ratio_multi_tf = safe_dict_from_api(features.get("ratio_multi_tf"))
    memory_profile = safe_dict_from_api(asset_memory.get("timeframes"))
    profile: Dict[str, Any] = {"components": {}, "summary": {}}
    for tf in ("5m", "15m", "1h", "4h"):
        tf_ratios = safe_dict_from_api(ratio_multi_tf.get(tf))
        tf_mem = safe_dict_from_api(memory_profile.get(tf))
        for component in ("position", "account", "global"):
            key = f"{tf}_{component}"
            value = safe_float(safe_dict_from_api(tf_ratios.get(component)).get("last"), 0.0)
            dist = safe_dict_from_api(tf_mem.get(f"{component}_last"))
            profile["components"][key] = classify_relative_extreme(value, dist)
    profile["summary"]["higher_regime"] = safe_dict_from_api(features.get("extreme_engine")).get("higher_regime", "neutral")
    return profile


def detect_extreme_inflection(extreme_profile: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
    components = safe_dict_from_api(extreme_profile.get("components"))
    inflection: Dict[str, Any] = {}
    for key, payload in components.items():
        state = _safe_state_text(safe_dict_from_api(payload).get("state"), "neutral")
        if state == "upper_extreme":
            inflection[key] = "upper_extreme_rolling_over" if safe_float(features.get("ret_1"), 0.0) < 0 else "plateau_extreme"
        elif state == "lower_extreme":
            inflection[key] = "lower_extreme_rebounding" if safe_float(features.get("ret_1"), 0.0) > 0 else "plateau_extreme"
        else:
            inflection[key] = "neutral"
    return {"states": inflection}


def score_position_led_structure(features: Dict[str, Any]) -> float:
    gap = safe_dict_from_api(features.get("position_account_gap"))
    ratio_alignment = safe_dict_from_api(features.get("ratio_alignment"))
    score = 0.0
    score += clamp(safe_float(gap.get("position_lead"), 0.0) * 8.0, 0.0, 0.45)
    if ratio_alignment.get("overall_supportive", False):
        score += 0.20
    if safe_float(features.get("oi_delta_3"), 0.0) > 0:
        score += 0.15
    if bool(features.get("flow_supported", False)):
        score += 0.12
    return round(clamp(score, 0.0, 1.0), 3)


def score_account_led_structure(features: Dict[str, Any]) -> float:
    gap = safe_dict_from_api(features.get("position_account_gap"))
    ratio_alignment = safe_dict_from_api(features.get("ratio_alignment"))
    score = 0.0
    score += clamp(safe_float(gap.get("account_lead"), 0.0) * 8.0, 0.0, 0.45)
    if ratio_alignment.get("higher_tf_supportive", False):
        score += 0.20
    if safe_float(features.get("oi_delta_1"), 0.0) > 0:
        score += 0.10
    if safe_float(features.get("recent_buy_ratio"), 0.0) >= EXECUTION_RULES["persistent_taker_buy_ratio"]:
        score += 0.12
    return round(clamp(score, 0.0, 1.0), 3)


def score_consensus_structure(features: Dict[str, Any]) -> float:
    ratio_alignment = safe_dict_from_api(features.get("ratio_alignment"))
    score = 0.0
    if ratio_alignment.get("overall_supportive", False):
        score += 0.35
    if ratio_alignment.get("higher_tf_supportive", False):
        score += 0.25
    if not ratio_alignment.get("overall_conflict", False):
        score += 0.15
    if safe_float(features.get("oi_delta_3"), 0.0) > 0 and bool(features.get("flow_supported", False)):
        score += 0.15
    return round(clamp(score, 0.0, 1.0), 3)


def score_counterflow_structure(features: Dict[str, Any]) -> float:
    pattern = safe_dict_from_api(features.get("counterflow_pattern"))
    score = 0.0
    if pattern.get("active", False):
        score += 0.45
    if pattern.get("explosive_oi", False):
        score += 0.20
    if pattern.get("activity_expanding", False):
        score += 0.15
    if bool(features.get("flow_supported", False)):
        score += 0.10
    return round(clamp(score, 0.0, 1.0), 3)


def determine_leader_type(features: Dict[str, Any]) -> Dict[str, Any]:
    scores = {
        "position_led": score_position_led_structure(features),
        "account_led": score_account_led_structure(features),
        "consensus_led": score_consensus_structure(features),
        "counterflow_led": score_counterflow_structure(features),
    }
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_name, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if abs(best_score - second_score) <= 0.07:
        leader_type = "mixed"
    else:
        leader_type = best_name
    return {"leader_type": leader_type, "leader_scores": scores, "strength": round(best_score, 3)}


def build_hypothesis_scores(
    features_or_result: Dict[str, Any],
    regime_info: Optional[Dict[str, Any]] = None,
    leader_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """واجهة V2: تدعم النداء القديم (result) والنداء الجديد (features+regime+leader)."""
    if regime_info is None and leader_info is None and ("market_features" in features_or_result or "family" in features_or_result):
        return _build_hypothesis_scores_legacy(features_or_result)
    synthetic_result = {
        "market_features": features_or_result,
        "regime_pattern": safe_dict_from_api(regime_info).get("regime_pattern"),
        "leader_type": safe_dict_from_api(leader_info).get("leader_type"),
    }
    return _build_hypothesis_scores_legacy(synthetic_result)


def compute_confidence_score(features: Dict[str, Any], result: Dict[str, Any]) -> float:
    return derive_confidence_score({"market_features": features, **result})


def compute_uncertainty_score(features: Dict[str, Any], result: Dict[str, Any]) -> float:
    return derive_uncertainty_score({"market_features": features, **result})


def compute_conflict_score(features: Dict[str, Any], result: Dict[str, Any]) -> float:
    return derive_conflict_score({"market_features": features, **result})


def load_previous_symbol_state(symbol: str) -> Dict[str, Any]:
    memory = load_asset_local_memory(symbol)
    return safe_dict_from_api(memory.get("last_snapshot"))


def detect_state_transition(previous: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    context = build_asset_state_transition_context({"last_snapshot": previous}, current)
    return {"state_transition": context.get("transition_key"), "reason": context.get("transition_ar", "")}


def save_current_symbol_state(symbol: str, current: Dict[str, Any]) -> None:
    memory = load_asset_local_memory(symbol)
    memory["last_snapshot"] = {
        "updated_at": utc_now_iso(),
        "final_bucket": current.get("final_bucket"),
        "primary_hypothesis": current.get("primary_hypothesis"),
        "regime_pattern": current.get("regime_pattern"),
        "trigger_pattern": current.get("trigger_pattern"),
        "execution_pattern": current.get("execution_pattern"),
        "importance_score": safe_float(current.get("importance_score"), 0.0),
    }
    save_json_atomic(get_asset_memory_path(symbol), memory)


def load_case_library() -> List[Dict[str, Any]]:
    data = load_global_case_library()
    return safe_list_from_api(data.get("cases"))


def build_case_vector(features: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, float]:
    return build_asset_case_vector(features, result)


def match_closest_case(case_vector: Dict[str, float], case_library: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not case_library:
        return {"case_name": None, "score": 0.0}
    scored = []
    for case in case_library:
        if not isinstance(case, dict):
            continue
        sim = _compute_case_similarity_score(case_vector, safe_dict_from_api(case.get("vector")))
        scored.append((sim, case))
    if not scored:
        return {"case_name": None, "score": 0.0}
    scored.sort(key=lambda x: x[0], reverse=True)
    top_score, top_case = scored[0]
    return {
        "case_name": top_case.get("primary_hypothesis") or top_case.get("regime_pattern"),
        "score": round(top_score, 3),
        "case_reason": top_case.get("final_bucket"),
    }


def build_causal_chain(features: Dict[str, Any], result: Dict[str, Any]) -> List[str]:
    chain: List[str] = []
    tf_4h = safe_dict_from_api(safe_dict_from_api(features.get("multi_tf")).get("4h"))
    tf_1h = safe_dict_from_api(safe_dict_from_api(features.get("multi_tf")).get("1h"))
    if tf_4h.get("accumulation", False):
        chain.append("4h accumulation regime detected")
    if safe_float(tf_1h.get("oi_delta_3"), 0.0) > 0:
        chain.append("1h OI remained supportive under pressure")
    if bool(features.get("higher_low_bias", False)):
        chain.append("5m downside rejection appeared")
    if safe_float(safe_dict_from_api(features.get("position_account_gap")).get("position_lead"), 0.0) > 0:
        chain.append("position/account inflection turned upward")
    if bool(features.get("flow_supported", False)):
        chain.append("taker flow confirmed breakout attempt")
    if _safe_state_text(result.get("acceptance_state"), "no") in {"yes", "partial"}:
        chain.append("early acceptance started before classical hold fully matured")
    return chain


def build_invalidation_reason(features: Dict[str, Any], result: Dict[str, Any]) -> str:
    reasons: List[str] = []
    if not bool(features.get("hold_above_breakout_3bars", True)):
        reasons.append("loss of reclaimed level")
    if safe_float(features.get("oi_delta_1"), 0.0) <= EXECUTION_RULES["oi_collapse_pct"]:
        reasons.append("oi contraction")
    ratio_conflict = safe_dict_from_api(result.get("ratio_conflict"))
    if ratio_conflict.get("state") == "conflicted":
        reasons.append("ratio relapse / timeframe conflict")
    return " + ".join(reasons) if reasons else derive_invalidation_reason_ar(result)


def build_next_failure_mode(features: Dict[str, Any], result: Dict[str, Any]) -> str:
    if safe_float(features.get("ret_1"), 0.0) > 0 and safe_float(features.get("oi_delta_1"), 0.0) < 0:
        return "oi_expansion_without_bullish_confirmation"
    if bool(features.get("one_bar_spike", False)):
        return "one_bar_spike_rejection"
    if safe_dict_from_api(result.get("failure")).get("early_failure", False):
        return "early_failure_continuation"
    return derive_next_failure_mode_ar(result)


def apply_v2_decision_gate(result: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
    """Gate نهائي للقرار وفق منطق V2 (فرضية/ثقة/عدم يقين/تأخر/إبطال قريب)."""
    result = dict(result)
    primary = _safe_state_text(result.get("primary_hypothesis"), "undetermined")
    confidence = safe_float(result.get("confidence_score"), 0.0)
    uncertainty = safe_float(result.get("uncertainty_score"), 1.0)
    acceptance_state = _safe_state_text(result.get("acceptance_state"), "no")
    price_late = bool(features.get("price_late", False))
    next_failure = _safe_state_text(result.get("next_failure_mode"), "")

    strong_hypothesis = confidence >= 0.60 and primary not in {"undetermined", "late_blowoff", "failed_rebuild_after_flush"}
    failure_near = next_failure in {"early_failure_continuation", "oi_expansion_without_bullish_confirmation", "one_bar_spike_rejection"}
    transition_key = _safe_state_text(result.get("state_transition"), "")

    if strong_hypothesis and uncertainty <= 0.45 and acceptance_state == "yes" and not price_late and not failure_near:
        result["final_bucket"] = "Actionable now"
        result["actionable_now"] = True
    elif result.get("final_bucket") == "Actionable now" and (uncertainty > 0.62 or price_late or failure_near):
        result["final_bucket"] = "Discovered but not actionable"
        result["actionable_now"] = False
        result["not_actionable_reason"] = "V2 gate downgraded: uncertainty/late/failure risk"
    elif transition_key in {"initial_observation", "state_shift"} and result.get("final_bucket") == "Actionable now":
        result["final_bucket"] = "Discovered but not actionable"
        result["actionable_now"] = False
        result["not_actionable_reason"] = "V2 gate: transition still early"
    return result


def compute_true_range(current_bar: Dict[str, float], prev_close: float) -> float:
    high = current_bar["high"]
    low = current_bar["low"]
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def compute_atr_pct(bars: List[Dict[str, float]], period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(bars)):
        trs.append(compute_true_range(bars[i], bars[i - 1]["close"]))
    recent = tail(trs, period)
    atr = avg(recent)
    last_close = bars[-1]["close"]
    return pct_change(atr + last_close, last_close, default=0.0)


def parse_klines(raw_klines: List[List[Any]]) -> List[Dict[str, float]]:
    parsed: List[Dict[str, float]] = []
    for row in raw_klines:
        if not isinstance(row, list) or len(row) < 11:
            continue
        quote_volume = safe_float(row[7])
        taker_buy_quote = safe_float(row[10])
        buy_ratio = safe_div(taker_buy_quote, quote_volume, 0.0)
        parsed.append(
            {
                "open_time": safe_int(row[0]),
                "open": safe_float(row[1]),
                "high": safe_float(row[2]),
                "low": safe_float(row[3]),
                "close": safe_float(row[4]),
                "volume": safe_float(row[5]),
                "close_time": safe_int(row[6]),
                "quote_volume": quote_volume,
                "trades": safe_int(row[8]),
                "taker_buy_base": safe_float(row[9]),
                "taker_buy_quote": taker_buy_quote,
                "taker_buy_ratio": buy_ratio,
            }
        )
    return parsed


class TTLCache:
    """Cache بسيط جدًا بمدة صلاحية محددة، مناسب للهواتف والبيئات المحدودة."""

    def __init__(self) -> None:
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Any:
        item = self._data.get(key)
        if not item:
            return None
        expires_at, value = item
        if expires_at < time.time():
            self._data.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_sec: int) -> None:
        self._data[key] = (time.time() + ttl_sec, value)

    def clear(self) -> None:
        self._data.clear()


# ============================================================
# عميل Binance العام
# ============================================================


class BinanceFuturesPublicClient:
    def __init__(self) -> None:
        self.base_url = STATIC_SETTINGS["BINANCE_FAPI_BASE"]
        self.timeout = (
            STATIC_SETTINGS["HTTP_CONNECT_TIMEOUT_SEC"],
            STATIC_SETTINGS["HTTP_TIMEOUT_SEC"],
        )
        self.session = self._build_session()
        self.cache = TTLCache()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=STATIC_SETTINGS["REQUEST_RETRIES_TOTAL"],
            backoff_factor=STATIC_SETTINGS["REQUEST_BACKOFF_FACTOR"],
            status_forcelist=STATIC_SETTINGS["STATUS_FORCELIST"],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "User-Agent": STATIC_SETTINGS["USER_AGENT"],
                "Accept": "application/json",
            }
        )
        return session

    def _cache_key(self, path: str, params: Optional[Dict[str, Any]]) -> str:
        params = params or {}
        items = sorted((str(k), str(v)) for k, v in params.items() if v is not None)
        return f"{path}|{items}"

    def _get_json(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        cache_ttl: int = 0,
    ) -> Any:
        key = self._cache_key(path, params)
        if cache_ttl > 0:
            cached = self.cache.get(key)
            if cached is not None:
                return cached

        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(url, params=params or {}, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if cache_ttl > 0:
                self.cache.set(key, data, cache_ttl)
            return data
        except Exception as exc:
            if DYNAMIC_SETTINGS.get("DEBUG_HTTP_ERRORS"):
                print(f"[HTTP-ERROR] {path} params={params} exc={exc}")
            return None

    # ---------- Endpoints عامة ----------
    def get_exchange_info(self) -> Optional[Dict[str, Any]]:
        return self._get_json("/fapi/v1/exchangeInfo", cache_ttl=PERFORMANCE_SETTINGS["CACHE_EXCHANGE_INFO_TTL"])

    def get_all_tickers_24h(self) -> Optional[List[Dict[str, Any]]]:
        return self._get_json("/fapi/v1/ticker/24hr", cache_ttl=PERFORMANCE_SETTINGS["CACHE_24H_TICKER_TTL"])

    def get_all_mark_prices(self) -> Optional[List[Dict[str, Any]]]:
        return self._get_json("/fapi/v1/premiumIndex", cache_ttl=PERFORMANCE_SETTINGS["CACHE_MARK_PRICE_TTL"])

    def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[List[List[Any]]]:
        return self._get_json(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_open_interest(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._get_json(
            "/fapi/v1/openInterest",
            params={"symbol": symbol},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_open_interest_hist(self, symbol: str, period: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/futures/data/openInterestHist",
            params={"symbol": symbol, "period": period, "limit": limit},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_top_position_ratio(self, symbol: str, period: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/futures/data/topLongShortPositionRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_top_account_ratio(self, symbol: str, period: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/futures/data/topLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_global_long_short_ratio(self, symbol: str, period: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_taker_long_short_ratio(self, symbol: str, period: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
            cache_ttl=DYNAMIC_SETTINGS["CYCLE_CACHE_TTL_SEC"],
        )

    def get_basis(self, pair: str, period: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/futures/data/basis",
            params={"pair": pair, "contractType": "PERPETUAL", "period": period, "limit": limit},
            cache_ttl=PERFORMANCE_SETTINGS["CACHE_BASIS_TTL"],
        )

    def get_funding_history(self, symbol: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        return self._get_json(
            "/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": limit},
            cache_ttl=PERFORMANCE_SETTINGS["CACHE_FUNDING_TTL"],
        )


# ============================================================
# مرحلة التصفية السريعة
# ============================================================



def build_symbol_universe(exchange_info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    symbols: Dict[str, Dict[str, Any]] = {}
    for item in exchange_info.get("symbols", []):
        symbol = item.get("symbol")
        if not symbol:
            continue
        symbols[symbol] = item
    return symbols


def quick_filter(symbol: str, symbol_meta: Dict[str, Any], ticker_24h: Dict[str, Any], mark_info: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if symbol_meta.get("status") != "TRADING":
        reasons.append("العقد غير متداول حاليًا")
        return False, reasons

    if symbol_meta.get("quoteAsset") != "USDT":
        reasons.append("ليس عقد USDT-M")
        return False, reasons

    if symbol_meta.get("contractType") != "PERPETUAL":
        reasons.append("ليس عقدًا دائمًا")
        return False, reasons

    quote_volume = safe_float(ticker_24h.get("quoteVolume"))
    trades = safe_int(ticker_24h.get("count"))
    price_change_pct = safe_float(ticker_24h.get("priceChangePercent"))
    mark_price = safe_float(mark_info.get("markPrice"))

    if mark_price < DYNAMIC_SETTINGS["MIN_MARK_PRICE"]:
        reasons.append("السعر الحالي غير صالح")
        return False, reasons

    if quote_volume < DYNAMIC_SETTINGS["MIN_24H_QUOTE_VOLUME"]:
        reasons.append("سيولة 24h أقل من الحد المطلوب")
        return False, reasons

    if trades < DYNAMIC_SETTINGS["MIN_24H_TRADE_COUNT"]:
        reasons.append("نشاط الصفقات 24h ضعيف")
        return False, reasons

    if price_change_pct < DYNAMIC_SETTINGS["MIN_24H_PRICE_CHANGE_PCT"]:
        reasons.append("الحركة اليومية ما زالت خاملة")
        return False, reasons

    if price_change_pct > DYNAMIC_SETTINGS["MAX_24H_PRICE_CHANGE_PCT"]:
        reasons.append("الحركة اليومية متأخرة بصريًا جدًا")
        return False, reasons

    reasons.append("اجتاز التصفية السريعة")
    return True, reasons


def universe_filter_for_full_scan(symbol: str, symbol_meta: Dict[str, Any], mark_info: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    فلتر خفيف جدًا عند تفعيل الفحص الشامل:
    نُبقي فقط العقود الدائمة USDT-M المتداولة ذات mark price صالح.
    لا نُقصي الرمز بسبب السيولة أو الحركة اليومية لأن المطلوب هنا فحص كل العملات.
    """
    reasons: List[str] = []

    if symbol_meta.get("status") != "TRADING":
        reasons.append("العقد غير متداول حاليًا")
        return False, reasons

    if symbol_meta.get("quoteAsset") != "USDT":
        reasons.append("ليس عقد USDT-M")
        return False, reasons

    if symbol_meta.get("contractType") != "PERPETUAL":
        reasons.append("ليس عقدًا دائمًا")
        return False, reasons

    mark_price = safe_float(mark_info.get("markPrice"))
    if mark_price < DYNAMIC_SETTINGS["MIN_MARK_PRICE"]:
        reasons.append("السعر الحالي غير صالح")
        return False, reasons

    reasons.append("أُدرج ضمن الفحص الشامل لجميع عقود USDT-M النشطة")
    return True, reasons


# ============================================================
# بناء الخصائص المرجعية للسوق
# ============================================================


def extract_ratio_features(raw_rows: Optional[List[Dict[str, Any]]], ratio_field: str) -> Dict[str, Any]:
    """
    استخراج خصائص ratios مع الحفاظ على الشكل القديم وإضافة القيم المطلقة للحسابات/المراكز.
    نستخدم longAccount/shortAccount لأن Binance تعيد هذه الحقول في واجهات top position/account ratio.
    """
    default_payload = {
        "last": 0.0,
        "prev": 0.0,
        "baseline": 0.0,
        "delta_last": 0.0,
        "delta_baseline": 0.0,
        "trend_up": 0.0,
        "long_ratio_last": 0.0,
        "short_ratio_last": 0.0,
        "long_ratio_prev": 0.0,
        "short_ratio_prev": 0.0,
        "long_ratio_baseline": 0.0,
        "short_ratio_baseline": 0.0,
        "long_ratio_delta": 0.0,
        "short_ratio_delta": 0.0,
        "ratio_series": [],
        "long_ratio_series": [],
        "short_ratio_series": [],
    }
    if not raw_rows:
        return default_payload

    ratio_values: List[float] = []
    long_values: List[float] = []
    short_values: List[float] = []

    for row in raw_rows:
        ratio_values.append(safe_float(row.get(ratio_field)))
        long_values.append(
            safe_float(
                row.get("longAccount", row.get("longPosition", row.get("longRatio", row.get("buyVol"))))
            )
        )
        short_values.append(
            safe_float(
                row.get("shortAccount", row.get("shortPosition", row.get("shortRatio", row.get("sellVol"))))
            )
        )

    if not ratio_values:
        return default_payload

    last_val = ratio_values[-1]
    prev_val = ratio_values[-2] if len(ratio_values) >= 2 else last_val
    baseline_slice = ratio_values[:-1][-6:]
    baseline = avg(baseline_slice, default=prev_val)
    deltas = [ratio_values[i] - ratio_values[i - 1] for i in range(1, len(ratio_values))]
    trend_up = safe_div(sum(1 for d in tail(deltas, 5) if d > 0), max(len(tail(deltas, 5)), 1), 0.0)

    long_last = long_values[-1] if long_values else 0.0
    short_last = short_values[-1] if short_values else 0.0
    long_prev = long_values[-2] if len(long_values) >= 2 else long_last
    short_prev = short_values[-2] if len(short_values) >= 2 else short_last
    long_baseline = avg(long_values[:-1][-6:], default=long_prev)
    short_baseline = avg(short_values[:-1][-6:], default=short_prev)

    payload = dict(default_payload)
    payload.update(
        {
            "last": last_val,
            "prev": prev_val,
            "baseline": baseline,
            "delta_last": last_val - prev_val,
            "delta_baseline": last_val - baseline,
            "trend_up": trend_up,
            "long_ratio_last": long_last,
            "short_ratio_last": short_last,
            "long_ratio_prev": long_prev,
            "short_ratio_prev": short_prev,
            "long_ratio_baseline": long_baseline,
            "short_ratio_baseline": short_baseline,
            "long_ratio_delta": long_last - long_prev,
            "short_ratio_delta": short_last - short_prev,
            "ratio_series": ratio_values,
            "long_ratio_series": long_values,
            "short_ratio_series": short_values,
        }
    )
    return payload


def compute_last_zscore(current_value: float, history_values: List[float], min_obs: int = 10) -> float:
    clean = [safe_float(v) for v in history_values if v is not None]
    if len(clean) < min_obs:
        return 0.0
    sigma = std(clean, 0.0)
    if sigma <= 1e-12:
        return 0.0
    return (current_value - avg(clean, 0.0)) / sigma


def build_rolling_ratio_series(values: List[float], baseline_window: int = 12) -> List[float]:
    out: List[float] = []
    for i in range(len(values)):
        if i == 0:
            out.append(1.0)
            continue
        baseline = avg(values[max(0, i - baseline_window):i], default=values[i - 1])
        out.append(safe_div(values[i], baseline, 0.0))
    return out


def compute_cvd_from_klines(bars_5m: List[Dict[str, float]]) -> Dict[str, Any]:
    """
    CVD تقريبي من بيانات klines المتاحة:
    delta = taker_buy_quote - (quote_volume - taker_buy_quote)
    """
    try:
        window = safe_int(DYNAMIC_SETTINGS.get("CVD_WINDOW_5M", 24), 24)
        bars = tail(bars_5m, window)
        cvd_series: List[float] = []
        delta_series: List[float] = []
        cumulative = 0.0
        for bar in bars:
            quote_volume = safe_float(bar.get("quote_volume"))
            taker_buy_quote = safe_float(bar.get("taker_buy_quote"))
            taker_sell_quote = max(quote_volume - taker_buy_quote, 0.0)
            delta = taker_buy_quote - taker_sell_quote
            cumulative += delta
            delta_series.append(delta)
            cvd_series.append(cumulative)
        return {
            "cvd_series": cvd_series,
            "delta_series": delta_series,
            "cvd_current": cvd_series[-1] if cvd_series else 0.0,
            "cvd_delta_last": delta_series[-1] if delta_series else 0.0,
        }
    except Exception:
        return {
            "cvd_series": [],
            "delta_series": [],
            "cvd_current": 0.0,
            "cvd_delta_last": 0.0,
        }


def evaluate_cvd_sustainability(cvd_values: List[float], volumes: List[float], delta_series: Optional[List[float]] = None) -> Dict[str, Any]:
    """
    حساب الميل والاستدامة:
    - cvd_slope: ميل آخر 4 نقاط
    - cvd_volume_ratio: |delta آخر 5m| / حجم آخر 5m
    - cvd_positive_persistence: نسبة الشموع ذات delta موجب في آخر 6 شموع
    """
    try:
        delta_series = delta_series or []
        slope_window = tail(cvd_values, 4)
        if len(slope_window) >= 2:
            cvd_slope = safe_div(slope_window[-1] - slope_window[0], max(len(slope_window) - 1, 1), 0.0)
        else:
            cvd_slope = 0.0

        last_delta = delta_series[-1] if delta_series else 0.0
        last_volume = tail(volumes, 1)[0] if volumes else 0.0
        cvd_volume_ratio = safe_div(abs(last_delta), last_volume, 0.0)
        recent_deltas = tail(delta_series, 6)
        cvd_positive_persistence = safe_div(sum(1 for d in recent_deltas if d > 0), max(len(recent_deltas), 1), 0.0)
        cvd_is_positive = (cvd_values[-1] if cvd_values else 0.0) > 0
        return {
            "cvd_slope": cvd_slope,
            "cvd_volume_ratio": cvd_volume_ratio,
            "cvd_positive_persistence": cvd_positive_persistence,
            "cvd_is_positive": cvd_is_positive,
            "cvd_delta_last": last_delta,
            "cvd_current": cvd_values[-1] if cvd_values else 0.0,
        }
    except Exception:
        return {
            "cvd_slope": 0.0,
            "cvd_volume_ratio": 0.0,
            "cvd_positive_persistence": 0.0,
            "cvd_is_positive": False,
            "cvd_delta_last": 0.0,
            "cvd_current": 0.0,
        }


def compute_position_account_gap(pos_features: Dict[str, Any], acc_features: Dict[str, Any]) -> Dict[str, Any]:
    long_position_ratio = safe_float(pos_features.get("long_ratio_last"))
    short_position_ratio = safe_float(pos_features.get("short_ratio_last"))
    long_account_ratio = safe_float(acc_features.get("long_ratio_last"))
    short_account_ratio = safe_float(acc_features.get("short_ratio_last"))

    position_lead = long_position_ratio - long_account_ratio
    account_lead = long_account_ratio - long_position_ratio
    short_gap = short_account_ratio - short_position_ratio

    return {
        "long_position_ratio": long_position_ratio,
        "short_position_ratio": short_position_ratio,
        "long_account_ratio": long_account_ratio,
        "short_account_ratio": short_account_ratio,
        "position_lead": position_lead,
        "account_lead": account_lead,
        "short_gap": short_gap,
    }


def track_gap_evolution(pos_features: Dict[str, Any], acc_features: Dict[str, Any], window: int = 12) -> Dict[str, Any]:
    try:
        pos_series = pos_features.get("long_ratio_series", [])
        acc_series = acc_features.get("long_ratio_series", [])
        n = min(len(pos_series), len(acc_series))
        if n == 0:
            return {
                "gap_sequence": [],
                "gap_last": 0.0,
                "gap_baseline": 0.0,
                "gap_delta": 0.0,
                "direction": "stable",
                "trend_up": 0.0,
            }
        gaps = [safe_float(pos_series[i]) - safe_float(acc_series[i]) for i in range(n)]
        gaps = tail(gaps, window)
        gap_last = gaps[-1]
        gap_baseline = avg(gaps[:-1][-4:], default=gap_last)
        gap_delta = gap_last - gap_baseline
        direction = "stable"
        if gap_delta >= 0.01:
            direction = "widening_position"
        elif gap_delta <= -0.01:
            direction = "widening_account"
        changes = [gaps[i] - gaps[i - 1] for i in range(1, len(gaps))]
        trend_up = safe_div(sum(1 for c in changes if c > 0), max(len(changes), 1), 0.0)
        return {
            "gap_sequence": gaps,
            "gap_last": gap_last,
            "gap_baseline": gap_baseline,
            "gap_delta": gap_delta,
            "direction": direction,
            "trend_up": trend_up,
        }
    except Exception:
        return {
            "gap_sequence": [],
            "gap_last": 0.0,
            "gap_baseline": 0.0,
            "gap_delta": 0.0,
            "direction": "stable",
            "trend_up": 0.0,
        }


def build_ratio_tf_snapshot(name: str, raw_pos: Optional[List[Dict[str, Any]]], raw_acc: Optional[List[Dict[str, Any]]], raw_global: Optional[List[Dict[str, Any]]], raw_taker: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    pos_features = extract_ratio_features(raw_pos, "longShortRatio")
    acc_features = extract_ratio_features(raw_acc, "longShortRatio")
    global_features = extract_ratio_features(raw_global, "longShortRatio")
    taker_features = extract_ratio_features(raw_taker, "buySellRatio")
    gap = compute_position_account_gap(pos_features, acc_features)
    gap_evolution = track_gap_evolution(pos_features, acc_features, window=safe_int(DYNAMIC_SETTINGS.get("OI_SEQUENCE_WINDOW", 12), 12))
    return {
        "name": name,
        "position": pos_features,
        "account": acc_features,
        "global": global_features,
        "taker": taker_features,
        "gap": gap,
        "gap_evolution": gap_evolution,
    }


def build_ratio_multi_tf(raw_ratio_inputs: Dict[str, Dict[str, Optional[List[Dict[str, Any]]]]]) -> Dict[str, Dict[str, Any]]:
    ratio_multi_tf: Dict[str, Dict[str, Any]] = {}
    for tf_name, payload in raw_ratio_inputs.items():
        ratio_multi_tf[tf_name] = build_ratio_tf_snapshot(
            tf_name,
            payload.get("position"),
            payload.get("account"),
            payload.get("global"),
            payload.get("taker"),
        )
    return ratio_multi_tf


def _weighted_alignment_state(signals: Dict[str, int], weights: Dict[str, int]) -> Dict[str, Any]:
    active = {tf: sig for tf, sig in signals.items() if sig in (-1, 0, 1) and tf in weights}
    active_weight = sum(weights[tf] for tf in active) or 1
    score = sum(weights[tf] * active[tf] for tf in active)
    normalized = safe_div(score, active_weight, 0.0)
    positive_frames = [tf for tf, sig in active.items() if sig > 0]
    negative_frames = [tf for tf, sig in active.items() if sig < 0]
    if normalized >= 0.45:
        state = "aligned"
    elif normalized <= -0.45:
        state = "conflicted"
    elif positive_frames and negative_frames:
        state = "mixed"
    elif positive_frames:
        state = "supportive"
    elif negative_frames:
        state = "conflicted"
    else:
        state = "neutral"
    return {
        "score": score,
        "normalized": round(normalized, 3),
        "state": state,
        "positive_frames": positive_frames,
        "negative_frames": negative_frames,
        "higher_tf_positive": any(tf in {"15m", "1h", "4h"} for tf in positive_frames),
        "higher_tf_negative": any(tf in {"15m", "1h", "4h"} for tf in negative_frames),
    }


def compute_ratio_alignment(ratio_multi_tf: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    weights = {"5m": 1, "15m": 2, "1h": 3, "4h": 4}
    position_signals: Dict[str, int] = {}
    account_signals: Dict[str, int] = {}
    consensus_signals: Dict[str, int] = {}
    flow_signals: Dict[str, int] = {}
    diagnostics: List[str] = []

    for tf_name, snapshot in ratio_multi_tf.items():
        gap = snapshot.get("gap", {})
        global_features = snapshot.get("global", {})
        taker_features = snapshot.get("taker", {})

        position_lead = safe_float(gap.get("position_lead"), 0.0)
        account_lead = safe_float(gap.get("account_lead"), 0.0)
        long_position_ratio = safe_float(gap.get("long_position_ratio"), 0.0)
        long_account_ratio = safe_float(gap.get("long_account_ratio"), 0.0)
        global_delta = safe_float(global_features.get("delta_baseline"), 0.0)
        taker_last = safe_float(taker_features.get("last"), 0.0)
        taker_delta = safe_float(taker_features.get("delta_baseline"), 0.0)

        if position_lead >= FAMILY_RULES["POSITION_LED_SQUEEZE_BUILDUP"]["position_lead_min"]:
            position_signals[tf_name] = 1
        elif account_lead >= FAMILY_RULES["ACCOUNT_LED_ACCUMULATION"]["account_lead_min"]:
            position_signals[tf_name] = -1
        else:
            position_signals[tf_name] = 0

        if account_lead >= FAMILY_RULES["ACCOUNT_LED_ACCUMULATION"]["account_lead_min"]:
            account_signals[tf_name] = 1
        elif position_lead >= FAMILY_RULES["POSITION_LED_SQUEEZE_BUILDUP"]["position_lead_min"]:
            account_signals[tf_name] = -1
        else:
            account_signals[tf_name] = 0

        disagreement = abs(long_position_ratio - long_account_ratio)
        if disagreement <= FAMILY_RULES["CONSENSUS_BULLISH_EXPANSION"]["max_internal_disagreement"] and global_delta > 0:
            consensus_signals[tf_name] = 1
        elif global_delta < 0:
            consensus_signals[tf_name] = -1
        else:
            consensus_signals[tf_name] = 0

        if taker_last > 1.0 or taker_delta > 0:
            flow_signals[tf_name] = 1
        elif taker_last < 1.0 and taker_delta < 0:
            flow_signals[tf_name] = -1
        else:
            flow_signals[tf_name] = 0

    position_alignment = _weighted_alignment_state(position_signals, weights)
    account_alignment = _weighted_alignment_state(account_signals, weights)
    consensus_alignment = _weighted_alignment_state(consensus_signals, weights)
    flow_alignment = _weighted_alignment_state(flow_signals, weights)

    positive_cores = sum(1 for block in (position_alignment, account_alignment, consensus_alignment, flow_alignment) if block.get("state") in {"aligned", "supportive"})
    negative_cores = sum(1 for block in (position_alignment, account_alignment, consensus_alignment, flow_alignment) if block.get("state") == "conflicted")
    higher_tf_supportive = any(
        block.get("higher_tf_positive", False)
        for block in (position_alignment, account_alignment, consensus_alignment, flow_alignment)
    )
    higher_tf_conflict = any(
        block.get("higher_tf_negative", False)
        for block in (position_alignment, account_alignment, consensus_alignment, flow_alignment)
    )
    overall_supportive = positive_cores >= 2 and negative_cores == 0
    overall_conflict = negative_cores >= 2

    diagnostics.append(
        f"ratio_alignment position={position_alignment['state']} account={account_alignment['state']} consensus={consensus_alignment['state']} flow={flow_alignment['state']}"
    )

    return {
        "position": position_alignment,
        "account": account_alignment,
        "consensus": consensus_alignment,
        "flow": flow_alignment,
        "overall_supportive": overall_supportive,
        "overall_conflict": overall_conflict,
        "higher_tf_supportive": higher_tf_supportive,
        "higher_tf_conflict": higher_tf_conflict,
        "diagnostics": diagnostics,
    }



def compute_ratio_conflict(ratio_alignment: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    penalty = 0
    if ratio_alignment.get("overall_conflict", False):
        penalty += 2
        diagnostics.append("يوجد conflict عام بين ratios متعدد الفريمات")
    if ratio_alignment.get("higher_tf_conflict", False):
        penalty += 2
        diagnostics.append("الفريمات الأعلى تعاكس الإشارة الحالية")
    for key in ("position", "account", "consensus", "flow"):
        block = ratio_alignment.get(key, {})
        if block.get("state") == "conflicted":
            penalty += 1
            diagnostics.append(f"{key} في حالة conflicted")
        elif block.get("state") == "mixed":
            penalty += 0.5
    if penalty >= 4:
        state = "conflicted"
    elif penalty >= 1:
        state = "mixed"
    else:
        state = "clean"
    return {
        "penalty": penalty,
        "state": state,
        "diagnostics": diagnostics,
    }



def compute_crowding_regime(ratio_multi_tf: Dict[str, Dict[str, Any]], asset_memory_profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Triad crowding regime engine:
    يقرأ Position / Account / Global كمنظومة واحدة، ويعتمد أساسًا على
    Relative Extremes داخل كل مكوّن ثم inflection/bias، لا على threshold موحّدة فقط.
    """
    diagnostics: List[str] = []
    frames = ("5m", "15m", "1h", "4h")
    component_signals: Dict[str, Dict[str, str]] = {}
    component_profiles: Dict[str, Dict[str, Any]] = {}
    tf_states: Dict[str, str] = {}
    values: Dict[str, Dict[str, float]] = {}
    edge_flags = {
        "short_squeeze_edge_frames": [],
        "long_rollover_edge_frames": [],
        "counterflow_edge_frames": [],
    }

    def classify_component(profile: Dict[str, Any]) -> str:
        bias = profile.get("bias", "neutral")
        upper_extreme = bool(profile.get("upper_extreme", False))
        lower_extreme = bool(profile.get("lower_extreme", False))
        inflection_up = bool(profile.get("inflection_up", False))
        inflection_down = bool(profile.get("inflection_down", False))
        upper_strength = safe_float(profile.get("upper_extreme_strength"), 0.0)
        lower_strength = safe_float(profile.get("lower_extreme_strength"), 0.0)

        if lower_extreme and bias == "short":
            if inflection_up:
                return "short_edge"
            return "short_crowded"
        if upper_extreme and bias == "long":
            if inflection_down:
                return "long_edge"
            return "long_crowded"

        if lower_extreme and bias == "long":
            return "lower_extreme_counterflow"
        if upper_extreme and bias == "short":
            return "upper_extreme_counterflow"

        if lower_strength >= 1.0 and bias == "short":
            return "short_lean"
        if upper_strength >= 1.0 and bias == "long":
            return "long_lean"
        if inflection_up and bias == "long":
            return "long_inflecting"
        if inflection_down and bias == "short":
            return "short_inflecting"
        return "neutral"

    for tf_name in frames:
        snapshot = ratio_multi_tf.get(tf_name, {})
        tf_memory = safe_dict_from_api((asset_memory_profile or {}).get("timeframes", {}).get(tf_name, {}))
        profiles = {
            "position": _build_component_extreme_profile("position", snapshot.get("position", {}), tf_memory.get("position_last")),
            "account": _build_component_extreme_profile("account", snapshot.get("account", {}), tf_memory.get("account_last")),
            "global": _build_component_extreme_profile("global", snapshot.get("global", {}), tf_memory.get("global_last")),
        }
        component_profiles[tf_name] = profiles
        values[tf_name] = {
            name: safe_float(profile.get("last"), 0.0)
            for name, profile in profiles.items()
        }
        component_signals[tf_name] = {
            name: classify_component(profile)
            for name, profile in profiles.items()
        }

        short_score = 0.0
        long_score = 0.0
        counterflow_score = 0.0
        mixed_components: List[str] = []

        for name, signal in component_signals[tf_name].items():
            if signal == "short_crowded":
                short_score += 2.0
            elif signal == "short_edge":
                short_score += 1.5
                edge_flags["short_squeeze_edge_frames"].append(tf_name)
            elif signal == "long_crowded":
                long_score += 2.0
            elif signal == "long_edge":
                long_score += 1.5
                edge_flags["long_rollover_edge_frames"].append(tf_name)
            elif signal == "short_lean":
                short_score += 1.0
            elif signal == "long_lean":
                long_score += 1.0
            elif signal in {"lower_extreme_counterflow", "upper_extreme_counterflow"}:
                counterflow_score += 1.5
                edge_flags["counterflow_edge_frames"].append(tf_name)
                mixed_components.append(name)
            elif signal in {"long_inflecting", "short_inflecting"}:
                mixed_components.append(name)

        tf_state = "neutral"
        if short_score >= 3.0 and long_score == 0 and counterflow_score == 0:
            tf_state = "crowded_short"
        elif long_score >= 3.0 and short_score == 0 and counterflow_score == 0:
            tf_state = "crowded_long"
        elif counterflow_score >= 1.5 or (short_score >= 1.5 and long_score >= 1.5):
            tf_state = "counterflow"
        elif short_score >= 1.5 or long_score >= 1.5 or mixed_components:
            tf_state = "mixed"
        tf_states[tf_name] = tf_state

    higher_frames = ("15m", "1h", "4h")
    higher_short_frames = [tf for tf in higher_frames if tf_states.get(tf) == "crowded_short"]
    higher_long_frames = [tf for tf in higher_frames if tf_states.get(tf) == "crowded_long"]
    higher_counterflow_frames = [tf for tf in higher_frames if tf_states.get(tf) == "counterflow"]
    higher_mixed_frames = [tf for tf in higher_frames if tf_states.get(tf) == "mixed"]
    micro_short_frames = [tf for tf in ("5m", "15m") if tf_states.get(tf) == "crowded_short"]
    micro_long_frames = [tf for tf in ("5m", "15m") if tf_states.get(tf) == "crowded_long"]

    higher_tf_short_crowded = len(higher_short_frames) >= 2 or {"1h", "4h"}.issubset(set(higher_short_frames))
    higher_tf_long_crowded = len(higher_long_frames) >= 2 or {"1h", "4h"}.issubset(set(higher_long_frames))

    state = "neutral_regime"
    if higher_counterflow_frames or (higher_short_frames and higher_long_frames):
        state = "counterflow_regime"
    elif higher_tf_short_crowded:
        state = "crowded_short_regime"
    elif higher_tf_long_crowded:
        state = "crowded_long_regime"
    elif higher_mixed_frames or (micro_short_frames and micro_long_frames):
        state = "mixed_regime"
    elif micro_short_frames and not micro_long_frames:
        state = "micro_crowded_short_regime"
    elif micro_long_frames and not micro_short_frames:
        state = "micro_crowded_long_regime"

    short_crowding = state in {"crowded_short_regime", "micro_crowded_short_regime"}
    long_crowding = state in {"crowded_long_regime", "micro_crowded_long_regime"}
    overall_conflicted = state in {"counterflow_regime", "mixed_regime"}

    diagnostics.append(
        f"crowding_regime state={state} tf_states={tf_states} short_edges={edge_flags['short_squeeze_edge_frames']} long_edges={edge_flags['long_rollover_edge_frames']} counterflow_edges={edge_flags['counterflow_edge_frames']}"
    )

    return {
        "state": state,
        "tf_states": tf_states,
        "component_signals": component_signals,
        "component_profiles": component_profiles,
        "values": values,
        "edge_flags": edge_flags,
        "higher_short_frames": higher_short_frames,
        "higher_long_frames": higher_long_frames,
        "higher_counterflow_frames": higher_counterflow_frames,
        "higher_mixed_frames": higher_mixed_frames,
        "micro_short_frames": micro_short_frames,
        "micro_long_frames": micro_long_frames,
        "higher_tf_short_crowded": higher_tf_short_crowded,
        "higher_tf_long_crowded": higher_tf_long_crowded,
        "short_crowding": short_crowding,
        "long_crowding": long_crowding,
        "overall_conflicted": overall_conflicted,
        "diagnostics": diagnostics,
    }



def _compute_quantile(values: List[float], q: float, default: float = 0.0) -> float:
    clean = sorted(safe_float(v) for v in values if v is not None)
    if not clean:
        return default
    if len(clean) == 1:
        return clean[0]
    q = clamp(q, 0.0, 1.0)
    pos = (len(clean) - 1) * q
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return clean[low]
    frac = pos - low
    return clean[low] + (clean[high] - clean[low]) * frac



def _build_component_extreme_profile(component_name: str, ratio_features: Dict[str, Any], memory_distribution: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    محرك extreme أقرب لروح البحث النهائي:
    - quantile-first عندما توجد ذاكرة محلية كافية.
    - absolute guides تصبح fallback مساعدًا لا الحاكم الرئيسي.
    - يخرج شدة التطرف وموقعه النسبي داخل تاريخ الأصل نفسه.
    """
    rules = EXTREME_ENGINE_RULES
    ratio_series = [safe_float(v) for v in ratio_features.get("ratio_series", []) if safe_float(v) > 0]
    last = safe_float(ratio_features.get("last"), 0.0)
    prev = safe_float(ratio_features.get("prev"), last)
    delta_last = safe_float(ratio_features.get("delta_last"), 0.0)
    delta_baseline = safe_float(ratio_features.get("delta_baseline"), 0.0)
    trend_up = safe_float(ratio_features.get("trend_up"), 0.0)
    long_last = safe_float(ratio_features.get("long_ratio_last"), 0.0)
    short_last = safe_float(ratio_features.get("short_ratio_last"), 0.0)

    memory_distribution = safe_dict_from_api(memory_distribution)
    memory_count = safe_int(memory_distribution.get("count"), 0)
    min_points = safe_int(DYNAMIC_SETTINGS.get("ASSET_MEMORY_MIN_POINTS_FOR_PROFILE", 24), 24)
    use_memory_distribution = bool(memory_distribution) and memory_count >= min_points

    if use_memory_distribution:
        min_val = safe_float(memory_distribution.get("min"), last)
        max_val = safe_float(memory_distribution.get("max"), last)
        q_low = safe_float(memory_distribution.get("q10"), last)
        q_mid = safe_float(memory_distribution.get("q50"), last)
        q_high = safe_float(memory_distribution.get("q90"), last)
        q_low_ext = safe_float(memory_distribution.get("q05"), q_low)
        q_high_ext = safe_float(memory_distribution.get("q95"), q_high)
    else:
        min_val = min(ratio_series) if ratio_series else last
        max_val = max(ratio_series) if ratio_series else last
        q_low_ext = _compute_quantile(ratio_series, 0.05, default=last)
        q_low = _compute_quantile(ratio_series, rules["lower_range_pos"], default=last)
        q_mid = _compute_quantile(ratio_series, 0.50, default=last)
        q_high = _compute_quantile(ratio_series, rules["upper_range_pos"], default=last)
        q_high_ext = _compute_quantile(ratio_series, 0.95, default=last)

    spread = max(max_val - min_val, 1e-9)
    range_pos = safe_div(last - min_val, spread, 0.5)
    upper_span = max(q_high_ext - q_mid, 1e-9)
    lower_span = max(q_mid - q_low_ext, 1e-9)
    upper_quantile_pos = safe_div(last - q_mid, upper_span, 0.0)
    lower_quantile_pos = safe_div(q_mid - last, lower_span, 0.0)

    guides = (rules.get("component_absolute_guides") or {}).get(component_name, {})
    guide_upper = safe_float(guides.get("upper"), rules["absolute_upper_ratio"])
    guide_lower = safe_float(guides.get("lower"), rules["absolute_lower_ratio"])

    absolute_upper_support = last >= guide_upper
    absolute_lower_support = last <= guide_lower

    relative_upper_extreme = (
        last >= (q_high - rules["relative_extreme_buffer"])
        or range_pos >= rules["upper_range_pos"]
        or upper_quantile_pos >= 0.85
    )
    relative_lower_extreme = (
        last <= (q_low + rules["relative_extreme_buffer"])
        or range_pos <= rules["lower_range_pos"]
        or lower_quantile_pos >= 0.85
    )

    if use_memory_distribution:
        upper_extreme = relative_upper_extreme and (upper_quantile_pos >= 0.80 or range_pos >= 0.86)
        lower_extreme = relative_lower_extreme and (lower_quantile_pos >= 0.80 or range_pos <= 0.14)
    else:
        upper_extreme = relative_upper_extreme and (range_pos >= rules["extreme_range_pos_strict"] or absolute_upper_support or spread <= 0.08)
        lower_extreme = relative_lower_extreme and (range_pos <= (1.0 - rules["extreme_range_pos_strict"]) or absolute_lower_support or spread <= 0.08)

    bias = "neutral"
    if last > 1.0 or long_last > short_last:
        bias = "long"
    elif last < 1.0 or short_last > long_last:
        bias = "short"

    inflection_up = delta_last > rules["inflection_delta_eps"] and (delta_baseline > 0 or trend_up >= rules["inflection_trend_up"])
    inflection_down = delta_last < -rules["inflection_delta_eps"] and (delta_baseline < 0 or trend_up <= rules["inflection_trend_down"])

    zone = "neutral"
    if upper_extreme:
        zone = "upper_extreme"
    elif lower_extreme:
        zone = "lower_extreme"

    upper_strength = 0.0
    lower_strength = 0.0
    if relative_upper_extreme:
        upper_strength += 1.00
    if upper_quantile_pos >= 0.60:
        upper_strength += clamp(upper_quantile_pos * 0.90, 0.0, 1.00)
    if not use_memory_distribution and absolute_upper_support:
        upper_strength += 0.25
    if bias == "long":
        upper_strength += 0.20
    if inflection_down and upper_extreme:
        upper_strength += 0.15

    if relative_lower_extreme:
        lower_strength += 1.00
    if lower_quantile_pos >= 0.60:
        lower_strength += clamp(lower_quantile_pos * 0.90, 0.0, 1.00)
    if not use_memory_distribution and absolute_lower_support:
        lower_strength += 0.25
    if bias == "short":
        lower_strength += 0.20
    if inflection_up and lower_extreme:
        lower_strength += 0.15

    return {
        "component": component_name,
        "last": last,
        "prev": prev,
        "delta_last": delta_last,
        "delta_baseline": delta_baseline,
        "trend_up": trend_up,
        "long_ratio_last": long_last,
        "short_ratio_last": short_last,
        "range_pos": round(range_pos, 4),
        "q_low": q_low,
        "q_mid": q_mid,
        "q_high": q_high,
        "q_low_ext": q_low_ext,
        "q_high_ext": q_high_ext,
        "upper_quantile_pos": round(upper_quantile_pos, 4),
        "lower_quantile_pos": round(lower_quantile_pos, 4),
        "relative_upper_extreme": relative_upper_extreme,
        "relative_lower_extreme": relative_lower_extreme,
        "absolute_upper_support": absolute_upper_support,
        "absolute_lower_support": absolute_lower_support,
        "upper_extreme_strength": round(upper_strength, 3),
        "lower_extreme_strength": round(lower_strength, 3),
        "upper_extreme": upper_extreme,
        "lower_extreme": lower_extreme,
        "zone": zone,
        "bias": bias,
        "inflection_up": inflection_up,
        "inflection_down": inflection_down,
        "memory_backed": use_memory_distribution,
        "memory_count": memory_count,
    }

def build_extreme_quantile_inflection_engine(
    ratio_multi_tf: Dict[str, Dict[str, Any]],
    market_context: Dict[str, Any],
    asset_memory_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rules = EXTREME_ENGINE_RULES
    diagnostics: List[str] = []
    snapshots: Dict[str, Dict[str, Any]] = {}

    for tf_name, snapshot in ratio_multi_tf.items():
        tf_memory = safe_dict_from_api((asset_memory_profile or {}).get("timeframes", {}).get(tf_name, {}))
        components = {
            "position": _build_component_extreme_profile("position", snapshot.get("position", {}), tf_memory.get("position_last")),
            "account": _build_component_extreme_profile("account", snapshot.get("account", {}), tf_memory.get("account_last")),
            "global": _build_component_extreme_profile("global", snapshot.get("global", {}), tf_memory.get("global_last")),
            "taker": _build_component_extreme_profile("taker", snapshot.get("taker", {}), tf_memory.get("taker_last")),
        }
        core = [components["position"], components["account"], components["global"]]
        upper_count = sum(1 for comp in core if comp["upper_extreme"])
        lower_count = sum(1 for comp in core if comp["lower_extreme"])
        long_bias_count = sum(1 for comp in core if comp["bias"] == "long")
        short_bias_count = sum(1 for comp in core if comp["bias"] == "short")
        inflection_up_count = sum(1 for comp in core if comp["inflection_up"])
        inflection_down_count = sum(1 for comp in core if comp["inflection_down"])
        state = "neutral"
        if upper_count >= 2 and long_bias_count >= 2:
            state = "upper_extreme"
        elif lower_count >= 2 and short_bias_count >= 2:
            state = "lower_extreme"
        elif upper_count >= 1 and lower_count >= 1:
            state = "mixed_extremes"
        snapshots[tf_name] = {
            "components": components,
            "upper_count": upper_count,
            "lower_count": lower_count,
            "long_bias_count": long_bias_count,
            "short_bias_count": short_bias_count,
            "inflection_up_count": inflection_up_count,
            "inflection_down_count": inflection_down_count,
            "state": state,
        }

    higher_frames = [tf for tf in ("15m", "1h", "4h") if tf in snapshots]
    higher_upper = [tf for tf in higher_frames if snapshots[tf]["state"] == "upper_extreme"]
    higher_lower = [tf for tf in higher_frames if snapshots[tf]["state"] == "lower_extreme"]
    higher_regime = "neutral"
    if len(higher_upper) >= rules["higher_tf_min_agree"] or {"1h", "4h"}.issubset(set(higher_upper)):
        higher_regime = "upper_extreme"
    elif len(higher_lower) >= rules["higher_tf_min_agree"] or {"1h", "4h"}.issubset(set(higher_lower)):
        higher_regime = "lower_extreme"
    elif higher_upper and higher_lower:
        higher_regime = "mixed"

    lower_frames = [tf for tf in ("5m", "15m") if tf in snapshots]
    inflection_up = any(snapshots[tf]["inflection_up_count"] >= 1 for tf in lower_frames)
    inflection_down = any(snapshots[tf]["inflection_down_count"] >= 1 for tf in lower_frames)

    pos_features = market_context.get("pos_features", {})
    acc_features = market_context.get("acc_features", {})
    global_features = market_context.get("global_features", {})

    price_breakout = bool(market_context.get("breakout_touched", False) or market_context.get("close_above_breakout", False))
    price_acceptance = bool(market_context.get("hold_above_breakout_3bars", False) or market_context.get("close_above_breakout", False) or market_context.get("follow_through_bias", False))
    price_failure = bool(
        market_context.get("fakeout_like", False)
        or market_context.get("one_bar_spike", False)
        or (market_context.get("breakout_touched", False) and not market_context.get("hold_above_breakout_3bars", False))
        or (safe_float(market_context.get("ret_1"), 0.0) < 0 and not market_context.get("higher_low_bias", False))
    )
    oi_expand = safe_float(market_context.get("oi_delta_1"), 0.0) > 0 or safe_float(market_context.get("oi_delta_3"), 0.0) >= EXECUTION_RULES["oi_buildup_pct"]
    oi_strong_expand = safe_float(market_context.get("oi_delta_3"), 0.0) >= EXECUTION_RULES["oi_expansion_pct"] or safe_float(market_context.get("oi_up_ratio"), 0.0) >= POST_FLUSH_PATTERN_RULES["oi_up_ratio_min"]
    oi_flat_or_down = safe_float(market_context.get("oi_delta_1"), 0.0) <= EXECUTION_RULES["oi_flat_abs_pct"] or safe_float(market_context.get("oi_delta_3"), 0.0) <= 0

    ratio_rollover_count = sum(
        1
        for value in (
            safe_float(pos_features.get("delta_baseline"), 0.0),
            safe_float(acc_features.get("delta_baseline"), 0.0),
            safe_float(global_features.get("delta_baseline"), 0.0),
        )
        if value < 0
    )
    ratio_rebuild_count = sum(
        1
        for value in (
            safe_float(pos_features.get("delta_baseline"), 0.0),
            safe_float(acc_features.get("delta_baseline"), 0.0),
            safe_float(global_features.get("delta_baseline"), 0.0),
        )
        if value > 0
    )

    account_weakening_first = (
        safe_float(acc_features.get("delta_baseline"), 0.0) < 0
        and safe_float(acc_features.get("last"), 0.0) <= max(
            rules["rollover_account_ceiling"],
            safe_float(pos_features.get("last"), 0.0) - 0.05,
        )
        and safe_float(pos_features.get("last"), 0.0) >= rules["rollover_position_floor"]
        and safe_float(global_features.get("last"), 0.0) >= rules["rollover_global_floor"]
    )

    short_crowding = bool(market_context.get("short_crowding", False) or higher_regime == "lower_extreme")
    long_crowding = bool(market_context.get("long_crowding", False) or higher_regime == "upper_extreme")
    ratio_supportive = bool(market_context.get("ratio_supportive", False))
    counterflow_active = bool(market_context.get("counterflow_active", False))

    lower_squeeze_score = 0
    if higher_regime == "lower_extreme":
        lower_squeeze_score += 2
    if short_crowding:
        lower_squeeze_score += 1
    if inflection_up:
        lower_squeeze_score += 1
    if price_breakout or market_context.get("higher_low_bias", False):
        lower_squeeze_score += 1
    if oi_expand or oi_strong_expand:
        lower_squeeze_score += 1

    upper_bullish_score = 0
    if higher_regime == "upper_extreme":
        upper_bullish_score += 2
    if long_crowding:
        upper_bullish_score += 1
    if price_breakout:
        upper_bullish_score += 1
    if price_acceptance:
        upper_bullish_score += 1
    if oi_expand:
        upper_bullish_score += 1
    if ratio_supportive:
        upper_bullish_score += 1

    upper_failure_score = 0
    if higher_regime == "upper_extreme" or snapshots.get("15m", {}).get("state") == "upper_extreme":
        upper_failure_score += 2
    if inflection_down:
        upper_failure_score += 1
    if price_failure:
        upper_failure_score += 1
    if ratio_rollover_count >= 2:
        upper_failure_score += 1
    if oi_flat_or_down or (oi_strong_expand and ratio_rollover_count >= 1):
        upper_failure_score += 1

    long_rollover_score = 0
    if account_weakening_first:
        long_rollover_score += 2
    if long_crowding:
        long_rollover_score += 1
    if price_failure or market_context.get("price_late", False):
        long_rollover_score += 1
    if oi_expand or oi_flat_or_down:
        long_rollover_score += 1
    if safe_float(pos_features.get("last"), 0.0) > safe_float(acc_features.get("last"), 0.0):
        long_rollover_score += 1

    oi_counter_score = 0
    if oi_strong_expand:
        oi_counter_score += 2
    if ratio_rollover_count >= 2:
        oi_counter_score += 1
    if price_failure:
        oi_counter_score += 1
    if higher_regime == "upper_extreme":
        oi_counter_score += 1

    squeeze_probability = clamp(
        lower_squeeze_score * 14
        + upper_bullish_score * 8
        + (10 if higher_regime == "lower_extreme" and price_acceptance else 0)
        + (8 if counterflow_active else 0),
        0,
        100,
    )
    flush_probability = clamp(
        upper_failure_score * 14
        + long_rollover_score * 12
        + oi_counter_score * 10,
        0,
        100,
    )

    patterns = {
        "lower_extreme_squeeze": {
            "active": lower_squeeze_score >= rules["lower_squeeze_min_score"],
            "score": lower_squeeze_score,
            "regime": higher_regime,
        },
        "upper_extreme_bullish_expansion": {
            "active": upper_bullish_score >= rules["upper_bullish_min_score"] and upper_failure_score < rules["upper_failure_min_score"],
            "score": upper_bullish_score,
            "regime": higher_regime,
        },
        "upper_extreme_failure": {
            "active": upper_failure_score >= rules["upper_failure_min_score"],
            "score": upper_failure_score,
            "regime": higher_regime,
        },
        "long_crowding_rollover": {
            "active": long_rollover_score >= rules["long_rollover_min_score"],
            "score": long_rollover_score,
            "account_weakening_first": account_weakening_first,
        },
        "oi_expansion_without_bullish_confirmation": {
            "active": oi_counter_score >= rules["oi_counter_min_score"],
            "score": oi_counter_score,
        },
    }

    active_patterns = [(name, payload.get("score", 0)) for name, payload in patterns.items() if payload.get("active")]
    active_patterns.sort(key=lambda item: item[1], reverse=True)
    dominant_pattern_name = active_patterns[0][0] if active_patterns else None

    diagnostics.append(
        f"extreme_regime={higher_regime} inflection_up={inflection_up} inflection_down={inflection_down} squeeze_prob={squeeze_probability:.1f} flush_prob={flush_probability:.1f}"
    )
    if patterns["lower_extreme_squeeze"]["active"]:
        diagnostics.append("Lower-Extreme squeeze context detected")
    if patterns["upper_extreme_bullish_expansion"]["active"]:
        diagnostics.append("Upper-Extreme bullish expansion context detected")
    if patterns["upper_extreme_failure"]["active"]:
        diagnostics.append("Upper-Extreme failure / flush risk detected")
    if patterns["long_crowding_rollover"]["active"]:
        diagnostics.append("Long crowding rollover detected")
    if patterns["oi_expansion_without_bullish_confirmation"]["active"]:
        diagnostics.append("OI expansion without bullish ratio confirmation detected")

    return {
        "snapshots": snapshots,
        "higher_regime": higher_regime,
        "higher_upper_frames": higher_upper,
        "higher_lower_frames": higher_lower,
        "inflection_up": inflection_up,
        "inflection_down": inflection_down,
        "price_breakout": price_breakout,
        "price_acceptance": price_acceptance,
        "price_failure": price_failure,
        "oi_expand": oi_expand,
        "oi_strong_expand": oi_strong_expand,
        "oi_flat_or_down": oi_flat_or_down,
        "ratio_rollover_count": ratio_rollover_count,
        "ratio_rebuild_count": ratio_rebuild_count,
        "patterns": patterns,
        "dominant_pattern_name": dominant_pattern_name,
        "squeeze_probability": squeeze_probability,
        "flush_probability": flush_probability,
        "diagnostics": diagnostics,
    }

def build_breakout_state(bars: List[Dict[str, float]], lookback: int = 20, hold_bars: int = 3) -> Dict[str, Any]:
    if len(bars) < max(lookback, hold_bars + 2):
        last_close = bars[-1]["close"] if bars else 0.0
        return {
            "breakout_level": last_close,
            "breakout_touched": False,
            "close_above_breakout": False,
            "hold_above_breakout": False,
            "hold_above_breakout_3bars": False,
            "ret_3": 0.0,
            "ret_12": 0.0,
        }

    breakout_buffer = EXECUTION_RULES["breakout_buffer_pct"] / 100.0
    hold_buffer = EXECUTION_RULES["hold_buffer_pct"] / 100.0
    ref_highs = [b["high"] for b in bars[-lookback-2:-2]] if len(bars) >= lookback + 2 else [b["high"] for b in bars[:-2]]
    breakout_level = max(ref_highs) if ref_highs else bars[-2]["high"]
    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    last_close = closes[-1]
    breakout_touched = max(highs[-2:]) > breakout_level * (1.0 + breakout_buffer)
    close_above_breakout = last_close > breakout_level * (1.0 + breakout_buffer)
    recent_closes = closes[-hold_bars:]
    hold_above_breakout_3bars = bool(recent_closes) and all(c > breakout_level * (1.0 + hold_buffer) for c in recent_closes)
    hold_above_breakout = hold_above_breakout_3bars or (
        close_above_breakout and min(lows[-hold_bars:]) > breakout_level * (1.0 - hold_buffer)
    )
    prev3 = bars[-4]["close"] if len(bars) >= 4 else closes[-2]
    prev12 = bars[-13]["close"] if len(bars) >= 13 else prev3
    return {
        "breakout_level": breakout_level,
        "breakout_touched": breakout_touched,
        "close_above_breakout": close_above_breakout,
        "hold_above_breakout": hold_above_breakout,
        "hold_above_breakout_3bars": hold_above_breakout_3bars,
        "ret_3": pct_change(last_close, prev3),
        "ret_12": pct_change(last_close, prev12),
    }


def build_timeframe_snapshot(bars: List[Dict[str, float]], oi_hist_values: List[float], name: str) -> Dict[str, Any]:
    try:
        breakout = build_breakout_state(bars, lookback=min(20, max(len(bars) - 2, 2)), hold_bars=3)
        oi_delta_1 = pct_change(oi_hist_values[-1], oi_hist_values[-2]) if len(oi_hist_values) >= 2 else 0.0
        oi_delta_3 = pct_change(oi_hist_values[-1], oi_hist_values[-4]) if len(oi_hist_values) >= 4 else oi_delta_1
        accumulation = oi_delta_3 >= EXECUTION_RULES["oi_buildup_pct"] and abs(breakout["ret_12"]) <= 3.5
        ignition = breakout["breakout_touched"] or breakout["ret_3"] >= EXECUTION_RULES["ignition_return_5m_pct"]
        return {
            "name": name,
            "ret_3": breakout["ret_3"],
            "ret_12": breakout["ret_12"],
            "oi_delta_1": oi_delta_1,
            "oi_delta_3": oi_delta_3,
            "breakout_level": breakout["breakout_level"],
            "breakout_touched": breakout["breakout_touched"],
            "close_above_breakout": breakout["close_above_breakout"],
            "hold_above_breakout": breakout["hold_above_breakout"],
            "accumulation": accumulation,
            "ignition": ignition,
        }
    except Exception:
        return {
            "name": name,
            "ret_3": 0.0,
            "ret_12": 0.0,
            "oi_delta_1": 0.0,
            "oi_delta_3": 0.0,
            "breakout_level": 0.0,
            "breakout_touched": False,
            "close_above_breakout": False,
            "hold_above_breakout": False,
            "accumulation": False,
            "ignition": False,
        }


def compute_dynamic_threshold_signal(raw_value: float, z_value: float, raw_threshold: float, z_threshold: float) -> bool:
    if DYNAMIC_SETTINGS.get("ENABLE_ZSCORE", True):
        return z_value >= z_threshold or raw_value >= raw_threshold
    return raw_value >= raw_threshold


def build_context_labels(features: Dict[str, Any]) -> Dict[str, Any]:
    funding_current = features.get("funding_current", 0.0)
    funding_hist = features.get("funding_history_values", [])
    basis_current = features.get("basis_current_rate")
    mark_basis_rate = features.get("mark_basis_rate")

    # -------- Funding context --------
    funding_context = "unknown"
    funding_notes: List[str] = []
    quiet_abs = EXECUTION_RULES["funding_quiet_abs"]
    non_crowded_abs = EXECUTION_RULES["funding_non_crowded_abs"]
    hostile_positive = EXECUTION_RULES["funding_hostile_positive"]

    if funding_hist and len(funding_hist) >= 2:
        prev_funding = funding_hist[-2]
    else:
        prev_funding = funding_current

    if abs(funding_current) <= quiet_abs and abs(avg(funding_hist, 0.0)) <= quiet_abs:
        funding_context = "funding quiet"
        funding_notes.append("التمويل هادئ وغير مزدحم")
    elif funding_current < 0 and funding_current > prev_funding:
        funding_context = "funding negative improving"
        funding_notes.append("التمويل ما زال سالبًا لكنه يتحسن نحو الصفر")
    elif abs(funding_current) <= non_crowded_abs:
        funding_context = "non-crowded"
        funding_notes.append("التمويل غير مزدحم نسبيًا")
    elif funding_current >= hostile_positive:
        funding_context = "crowded long side"
        funding_notes.append("التمويل موجب ومزدحم على جهة الـ long")
    elif funding_current <= EXECUTION_RULES["funding_deep_negative"]:
        funding_context = "funding hostile"
        funding_notes.append("التمويل سالب بحدة ويحتاج حذرًا")
    else:
        funding_context = "non-crowded"
        funding_notes.append("التمويل ليس مثاليًا لكنه ليس مزدحمًا بوضوح")

    # -------- Basis context --------
    basis_context = "missing"
    basis_notes: List[str] = []
    basis_val = basis_current if basis_current is not None else mark_basis_rate
    if basis_val is None:
        basis_context = "missing"
        basis_notes.append("لا توجد بيانات Basis كافية")
    else:
        abs_basis = abs(basis_val)
        if abs_basis <= EXECUTION_RULES["basis_neutral_abs"]:
            basis_context = "neutral"
            basis_notes.append("الـ Basis قريب من الحياد")
        elif abs_basis <= EXECUTION_RULES["basis_supportive_abs"]:
            basis_context = "supportive"
            basis_notes.append("الـ Basis داعم بشكل معتدل")
        else:
            basis_context = "adverse"
            basis_notes.append("الـ Basis ممتد ويرفع الحذر")

    return {
        "funding_context": funding_context,
        "funding_notes": funding_notes,
        "basis_context": basis_context,
        "basis_notes": basis_notes,
    }




def detect_counterflow_short_squeeze_pattern(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    بصمة شبيهة بحالة MAGMA:
    - الحسابات تميل بوضوح إلى long
    - بينما جانب المراكز/التموضع يبقى قصيرًا أو أقل bullish من الحسابات
    - مع انفجار واضح في OI وتوسع نشاط/حجم واختراق سعري
    """
    diagnostics: List[str] = []
    rules = SPECIAL_PATTERN_RULES["COUNTERFLOW_SHORT_SQUEEZE_EXPANSION"]

    gap = features.get("position_account_gap", {})
    pos_features = features.get("pos_features", {})
    acc_features = features.get("acc_features", {})
    ratio_multi_tf = features.get("ratio_multi_tf", {})

    account_long_ratio = safe_float(gap.get("long_account_ratio"), safe_float(acc_features.get("long_ratio_last"), 0.0))
    position_long_ratio = safe_float(gap.get("long_position_ratio"), safe_float(pos_features.get("long_ratio_last"), 0.0))
    position_ratio_last = safe_float(pos_features.get("last"), 0.0)
    account_lead = safe_float(gap.get("account_lead"), 0.0)

    higher_tf_account_bias = False
    higher_tf_position_short_tilt = False
    for tf_name in ("15m", "1h", "4h"):
        tf_snapshot = ratio_multi_tf.get(tf_name, {})
        tf_gap = tf_snapshot.get("gap", {})
        tf_pos = tf_snapshot.get("position", {})
        tf_acc = tf_snapshot.get("account", {})
        if safe_float(tf_gap.get("long_account_ratio"), safe_float(tf_acc.get("long_ratio_last"), 0.0)) >= rules["account_long_ratio_min"]:
            higher_tf_account_bias = True
        tf_position_long = safe_float(tf_gap.get("long_position_ratio"), safe_float(tf_pos.get("long_ratio_last"), 0.0))
        tf_position_ratio = safe_float(tf_pos.get("last"), 0.0)
        if tf_position_long <= rules["position_long_ratio_max"] or tf_position_ratio <= rules["position_ratio_max"] or safe_float(tf_gap.get("position_lead"), 0.0) < 0:
            higher_tf_position_short_tilt = True

    account_bias = account_long_ratio >= rules["account_long_ratio_min"] or higher_tf_account_bias
    account_lead_ok = account_lead >= rules["account_lead_min"]
    position_short_tilt = (
        position_long_ratio <= rules["position_long_ratio_max"]
        or position_ratio_last <= rules["position_ratio_max"]
        or safe_float(gap.get("position_lead"), 0.0) < 0
    )
    if higher_tf_account_bias:
        diagnostics.append("التحيز الشرائي للحسابات مؤكد أيضًا على فريمات أعلى")
    if higher_tf_position_short_tilt:
        diagnostics.append("التموضع القصير/الأقل bullish ظاهر أيضًا على فريمات أعلى")

    explosive_oi = (
        features.get("oi_delta_1", 0.0) >= rules["oi_delta_1_min"]
        or features.get("oi_delta_3", 0.0) >= rules["oi_delta_3_min"]
        or features.get("oi_up_ratio", 0.0) >= rules["oi_up_ratio_min"]
    )
    breakout_ready = (
        features.get("breakout_touched", False)
        or features.get("close_above_breakout", False)
        or features.get("ret_3", 0.0) >= EXECUTION_RULES["ignition_return_5m_pct"]
    )
    activity_expanding = (
        features.get("trade_expansion_last", 0.0) >= rules["trade_expansion_min"]
        and features.get("vol_expansion_last", 0.0) >= rules["volume_expansion_min"]
    )
    early_enough = not features.get("price_late", False)

    score = 0
    if account_bias:
        score += 1
        diagnostics.append("الحسابات تميل بوضوح إلى long")
    if account_lead_ok:
        score += 1
        diagnostics.append("account_lead أعلى من الحد البنيوي")
    if position_short_tilt:
        score += 1
        diagnostics.append("جانب المراكز/التموضع ما زال قصيرًا أو أقل bullish من الحسابات")
    if explosive_oi:
        score += 2
        diagnostics.append("OI يتوسع بقوة على نحو غير اعتيادي")
    if breakout_ready:
        score += 1
        diagnostics.append("هناك اشتعال سعري/اختراق فعلي أو بداية اختراق")
    if activity_expanding:
        score += 1
        diagnostics.append("الحجم والصفقات يتوسعان مع الحركة")
    if early_enough:
        score += 1
        diagnostics.append("الحركة ليست متأخرة بالكامل بعد")

    active = score >= rules["min_pattern_score"] and account_bias and position_short_tilt and explosive_oi
    armed_ready = active and breakout_ready and activity_expanding and early_enough

    if active:
        diagnostics.insert(0, "تم رصد بصمة COUNTERFLOW_SHORT_SQUEEZE_EXPANSION الشبيهة بحالة MAGMA")

    return {
        "name": "COUNTERFLOW_SHORT_SQUEEZE_EXPANSION",
        "active": active,
        "armed_ready": armed_ready,
        "score": score,
        "account_bias": account_bias,
        "account_lead_ok": account_lead_ok,
        "position_short_tilt": position_short_tilt,
        "explosive_oi": explosive_oi,
        "breakout_ready": breakout_ready,
        "activity_expanding": activity_expanding,
        "diagnostics": diagnostics,
    }

def build_reference_market_features(
    client: BinanceFuturesPublicClient,
    symbol: str,
    symbol_meta: Dict[str, Any],
    ticker_24h: Dict[str, Any],
    mark_info: Dict[str, Any],
) -> Dict[str, Any]:
    diagnostics: List[str] = []

    raw_5m = safe_list_from_api(client.get_klines(symbol, "5m", DYNAMIC_SETTINGS["KLINE_LIMIT_5M"]))
    raw_1h = safe_list_from_api(client.get_klines(symbol, "1h", DYNAMIC_SETTINGS["KLINE_LIMIT_1H"]))
    raw_15m = safe_list_from_api(client.get_klines(symbol, "15m", DYNAMIC_SETTINGS["KLINE_LIMIT_15M"])) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_4h = safe_list_from_api(client.get_klines(symbol, "4h", DYNAMIC_SETTINGS["KLINE_LIMIT_4H"])) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_oi = safe_dict_from_api(client.get_open_interest(symbol))
    raw_oi_hist = safe_list_from_api(client.get_open_interest_hist(symbol, "5m", DYNAMIC_SETTINGS["OI_HIST_LIMIT"]))
    raw_oi_hist_15m = safe_list_from_api(client.get_open_interest_hist(symbol, "15m", DYNAMIC_SETTINGS["OI_HIST_LIMIT_15M"])) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_oi_hist_1h = safe_list_from_api(client.get_open_interest_hist(symbol, "1h", DYNAMIC_SETTINGS["OI_HIST_LIMIT_1H"]))
    raw_oi_hist_4h = safe_list_from_api(client.get_open_interest_hist(symbol, "4h", DYNAMIC_SETTINGS["OI_HIST_LIMIT_4H"])) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_pos_ratio = safe_list_from_api(client.get_top_position_ratio(symbol, "5m", DYNAMIC_SETTINGS["RATIO_LIMIT"]))
    raw_acc_ratio = safe_list_from_api(client.get_top_account_ratio(symbol, "5m", DYNAMIC_SETTINGS["RATIO_LIMIT"]))
    raw_global_ratio = safe_list_from_api(client.get_global_long_short_ratio(symbol, "5m", DYNAMIC_SETTINGS["RATIO_LIMIT"]))
    raw_taker_ratio = safe_list_from_api(client.get_taker_long_short_ratio(symbol, "5m", DYNAMIC_SETTINGS["RATIO_LIMIT"]))
    raw_pos_ratio_15m = safe_list_from_api(client.get_top_position_ratio(symbol, "15m", DYNAMIC_SETTINGS.get("RATIO_LIMIT_15M", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_acc_ratio_15m = safe_list_from_api(client.get_top_account_ratio(symbol, "15m", DYNAMIC_SETTINGS.get("RATIO_LIMIT_15M", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_global_ratio_15m = safe_list_from_api(client.get_global_long_short_ratio(symbol, "15m", DYNAMIC_SETTINGS.get("RATIO_LIMIT_15M", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_taker_ratio_15m = safe_list_from_api(client.get_taker_long_short_ratio(symbol, "15m", DYNAMIC_SETTINGS.get("RATIO_LIMIT_15M", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_pos_ratio_1h = safe_list_from_api(client.get_top_position_ratio(symbol, "1h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_1H", DYNAMIC_SETTINGS["RATIO_LIMIT"])))
    raw_acc_ratio_1h = safe_list_from_api(client.get_top_account_ratio(symbol, "1h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_1H", DYNAMIC_SETTINGS["RATIO_LIMIT"])))
    raw_global_ratio_1h = safe_list_from_api(client.get_global_long_short_ratio(symbol, "1h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_1H", DYNAMIC_SETTINGS["RATIO_LIMIT"])))
    raw_taker_ratio_1h = safe_list_from_api(client.get_taker_long_short_ratio(symbol, "1h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_1H", DYNAMIC_SETTINGS["RATIO_LIMIT"])))
    raw_pos_ratio_4h = safe_list_from_api(client.get_top_position_ratio(symbol, "4h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_4H", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_acc_ratio_4h = safe_list_from_api(client.get_top_account_ratio(symbol, "4h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_4H", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_global_ratio_4h = safe_list_from_api(client.get_global_long_short_ratio(symbol, "4h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_4H", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_taker_ratio_4h = safe_list_from_api(client.get_taker_long_short_ratio(symbol, "4h", DYNAMIC_SETTINGS.get("RATIO_LIMIT_4H", DYNAMIC_SETTINGS["RATIO_LIMIT"]))) if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else []
    raw_basis = safe_list_from_api(client.get_basis(symbol, "5m", DYNAMIC_SETTINGS["BASIS_LIMIT"]))
    raw_funding_hist = safe_list_from_api(client.get_funding_history(symbol, DYNAMIC_SETTINGS["FUNDING_LIMIT"]))

    bars_5m = parse_klines(raw_5m)
    bars_1h = parse_klines(raw_1h)
    bars_15m = parse_klines(raw_15m)
    bars_4h = parse_klines(raw_4h)

    if len(bars_5m) < 24:
        return {
            "ok": False,
            "symbol": symbol,
            "diagnostics": ["بيانات 5m غير كافية لبناء الخصائص المرجعية"],
            "error": "insufficient_5m_bars",
        }

    last = bars_5m[-1]
    prev = bars_5m[-2]
    prev3 = bars_5m[-4] if len(bars_5m) >= 4 else prev
    prev6 = bars_5m[-7] if len(bars_5m) >= 7 else prev3
    prev12 = bars_5m[-13] if len(bars_5m) >= 13 else prev6

    breakout_state_5m = build_breakout_state(bars_5m, lookback=20, hold_bars=safe_int(DYNAMIC_SETTINGS.get("HOLD_CONFIRM_BARS", 3), 3))
    breakout_level = breakout_state_5m["breakout_level"]
    atr_pct = compute_atr_pct(bars_5m, 14)

    last_close = last["close"]
    prev_close = prev["close"]
    ret_1 = pct_change(last_close, prev_close)
    ret_3 = pct_change(last_close, prev3["close"])
    ret_6 = pct_change(last_close, prev6["close"])
    ret_12 = pct_change(last_close, prev12["close"])
    h1_ret_3 = pct_change(bars_1h[-1]["close"], bars_1h[-4]["close"]) if len(bars_1h) >= 4 else 0.0

    quote_volumes = [b["quote_volume"] for b in bars_5m]
    trades = [float(b["trades"]) for b in bars_5m]
    buy_ratios = [b["taker_buy_ratio"] for b in bars_5m]

    last_quote_vol = last["quote_volume"]
    avg_quote_vol_prev12 = avg(quote_volumes[-13:-1], default=last_quote_vol)
    vol_expansion_last = safe_div(last_quote_vol, avg_quote_vol_prev12, 0.0)
    vol_expansion_3 = safe_div(avg(quote_volumes[-3:], last_quote_vol), avg(quote_volumes[-15:-3], avg_quote_vol_prev12), 0.0)

    last_trades = float(last["trades"])
    avg_trades_prev12 = avg(trades[-13:-1], default=last_trades)
    trade_expansion_last = safe_div(last_trades, avg_trades_prev12, 0.0)
    trade_expansion_3 = safe_div(avg(trades[-3:], last_trades), avg(trades[-15:-3], avg_trades_prev12), 0.0)

    last_buy_ratio = buy_ratios[-1]
    recent_buy_ratio = avg(buy_ratios[-3:], default=last_buy_ratio)
    baseline_buy_ratio = avg(buy_ratios[-12:-3], default=recent_buy_ratio)
    flow_ratio_delta = recent_buy_ratio - baseline_buy_ratio
    ofi_proxy = (2.0 * recent_buy_ratio) - 1.0

    extension_pct_from_breakout = pct_change(last_close, breakout_level) if breakout_level > 0 else 0.0
    extension_atr = safe_div(extension_pct_from_breakout, atr_pct if atr_pct > 0 else 1.0, 0.0)
    price_late = (
        extension_atr >= EXECUTION_RULES["max_clean_extension_atr"]
        or extension_pct_from_breakout >= EXECUTION_RULES["max_clean_extension_pct"]
    )

    ignition_seed = breakout_state_5m["breakout_touched"] or (
        ret_1 >= EXECUTION_RULES["ignition_return_5m_pct"]
        and recent_buy_ratio >= EXECUTION_RULES["strong_taker_buy_ratio"]
        and trade_expansion_last >= EXECUTION_RULES["min_trade_expansion"]
    )

    one_bar_spike = (
        ret_1 >= EXECUTION_RULES["strong_bar_return_5m_pct"]
        and ret_3 < ret_1 * 0.70
        and not breakout_state_5m["hold_above_breakout_3bars"]
    )

    # -------- OI --------
    oi_contracts_now = safe_float(raw_oi.get("openInterest"), default=0.0)
    oi_hist_values = [safe_float(row.get("sumOpenInterestValue")) for row in raw_oi_hist if isinstance(row, dict)]
    oi_hist_contracts = [safe_float(row.get("sumOpenInterest")) for row in raw_oi_hist if isinstance(row, dict)]
    oi_value_last = oi_hist_values[-1] if oi_hist_values else None
    oi_delta_1 = pct_change(oi_hist_values[-1], oi_hist_values[-2]) if len(oi_hist_values) >= 2 else 0.0
    oi_delta_3 = pct_change(oi_hist_values[-1], oi_hist_values[-4]) if len(oi_hist_values) >= 4 else oi_delta_1
    oi_delta_6 = pct_change(oi_hist_values[-1], oi_hist_values[-7]) if len(oi_hist_values) >= 7 else oi_delta_3
    oi_up_ratio = safe_div(sum(1 for i in range(1, len(tail(oi_hist_values, 6))) if tail(oi_hist_values, 6)[i] > tail(oi_hist_values, 6)[i - 1]), max(len(tail(oi_hist_values, 6)) - 1, 1), 0.0)

    oi_hist_values_15m = [safe_float(row.get("sumOpenInterestValue")) for row in raw_oi_hist_15m if isinstance(row, dict)]
    oi_hist_values_1h = [safe_float(row.get("sumOpenInterestValue")) for row in raw_oi_hist_1h if isinstance(row, dict)]
    oi_hist_values_4h = [safe_float(row.get("sumOpenInterestValue")) for row in raw_oi_hist_4h if isinstance(row, dict)]

    # -------- Ratios --------
    ratio_multi_tf = build_ratio_multi_tf({
        "5m": {"position": raw_pos_ratio, "account": raw_acc_ratio, "global": raw_global_ratio, "taker": raw_taker_ratio},
        "15m": {"position": raw_pos_ratio_15m, "account": raw_acc_ratio_15m, "global": raw_global_ratio_15m, "taker": raw_taker_ratio_15m},
        "1h": {"position": raw_pos_ratio_1h, "account": raw_acc_ratio_1h, "global": raw_global_ratio_1h, "taker": raw_taker_ratio_1h},
        "4h": {"position": raw_pos_ratio_4h, "account": raw_acc_ratio_4h, "global": raw_global_ratio_4h, "taker": raw_taker_ratio_4h},
    })
    asset_local_memory = load_asset_local_memory(symbol)
    asset_memory_profile = build_asset_local_memory_profile(symbol, asset_local_memory)
    ratio_alignment = compute_ratio_alignment(ratio_multi_tf)
    ratio_conflict = compute_ratio_conflict(ratio_alignment)

    pos_features = ratio_multi_tf["5m"]["position"]
    acc_features = ratio_multi_tf["5m"]["account"]
    global_features = ratio_multi_tf["5m"]["global"]
    taker_features = ratio_multi_tf["5m"]["taker"]

    position_account_gap = ratio_multi_tf["5m"]["gap"]
    gap_evolution = ratio_multi_tf["5m"]["gap_evolution"]
    position_lead_strength = position_account_gap["position_lead"]
    account_lead_strength = position_account_gap["account_lead"]
    global_ratio_last = global_features["last"]

    crowding_regime = compute_crowding_regime(ratio_multi_tf, asset_memory_profile)
    short_crowding = crowding_regime["short_crowding"]
    long_crowding = crowding_regime["long_crowding"]

    # -------- Funding / Basis / Mark --------
    funding_current = safe_float(mark_info.get("lastFundingRate"), default=0.0)
    index_price = safe_float(mark_info.get("indexPrice"), default=0.0)
    mark_price = safe_float(mark_info.get("markPrice"), default=0.0)
    mark_basis_rate = safe_div(mark_price - index_price, index_price, None) if index_price > 0 else None

    funding_history_values = [safe_float(row.get("fundingRate")) for row in raw_funding_hist if isinstance(row, dict)]
    basis_rates = [safe_float(row.get("basisRate")) for row in raw_basis if isinstance(row, dict) and row.get("basisRate") is not None]
    basis_current_rate = basis_rates[-1] if basis_rates else mark_basis_rate

    context_labels = build_context_labels(
        {
            "funding_current": funding_current,
            "funding_history_values": funding_history_values,
            "basis_current_rate": basis_current_rate,
            "mark_basis_rate": mark_basis_rate,
        }
    )

    # -------- CVD / Flow --------
    cvd_pack = compute_cvd_from_klines(bars_5m) if DYNAMIC_SETTINGS.get("ENABLE_CVD") else {
        "cvd_series": [], "delta_series": [], "cvd_current": 0.0, "cvd_delta_last": 0.0
    }
    cvd_metrics = evaluate_cvd_sustainability(cvd_pack.get("cvd_series", []), tail(quote_volumes, DYNAMIC_SETTINGS.get("CVD_WINDOW_5M", 24)), cvd_pack.get("delta_series", []))

    # -------- Dynamic Z-score --------
    volume_expansion_series = build_rolling_ratio_series(quote_volumes, baseline_window=12)
    trade_expansion_series = build_rolling_ratio_series(trades, baseline_window=12)
    buy_ratio_series = buy_ratios[:]
    oi_delta_series = [pct_change(oi_hist_values[i], oi_hist_values[i - 1]) for i in range(1, len(oi_hist_values))] if len(oi_hist_values) >= 2 else []
    cvd_slope_series: List[float] = []
    cvd_ratio_series: List[float] = []
    cvd_series_full = cvd_pack.get("cvd_series", [])
    delta_series_full = cvd_pack.get("delta_series", [])
    for i in range(len(cvd_series_full)):
        window_vals = cvd_series_full[max(0, i - 3): i + 1]
        if len(window_vals) >= 2:
            cvd_slope_series.append(safe_div(window_vals[-1] - window_vals[0], max(len(window_vals) - 1, 1), 0.0))
        else:
            cvd_slope_series.append(0.0)
        vol = quote_volumes[max(0, len(quote_volumes) - len(delta_series_full) + i)] if delta_series_full else 0.0
        delta_val = delta_series_full[i] if i < len(delta_series_full) else 0.0
        cvd_ratio_series.append(safe_div(abs(delta_val), vol, 0.0))

    zscores = {
        "oi_delta_z": compute_last_zscore(oi_delta_1, oi_delta_series[-DYNAMIC_SETTINGS.get("ZSCORE_LOOKBACK", 100): -1]),
        "buy_ratio_z": compute_last_zscore(last_buy_ratio, buy_ratio_series[-DYNAMIC_SETTINGS.get("ZSCORE_LOOKBACK", 100): -1]),
        "volume_expansion_z": compute_last_zscore(vol_expansion_last, volume_expansion_series[-DYNAMIC_SETTINGS.get("ZSCORE_LOOKBACK", 100): -1]),
        "trade_expansion_z": compute_last_zscore(trade_expansion_last, trade_expansion_series[-DYNAMIC_SETTINGS.get("ZSCORE_LOOKBACK", 100): -1]),
        "cvd_slope_z": compute_last_zscore(cvd_metrics.get("cvd_slope", 0.0), cvd_slope_series[-DYNAMIC_SETTINGS.get("ZSCORE_LOOKBACK", 100): -1]),
        "cvd_ratio_z": compute_last_zscore(cvd_metrics.get("cvd_volume_ratio", 0.0), cvd_ratio_series[-DYNAMIC_SETTINGS.get("ZSCORE_LOOKBACK", 100): -1]),
    }

    # -------- Multi-timeframe --------
    tf_5m = {
        "name": "5m",
        "ret_3": ret_3,
        "ret_12": ret_12,
        "oi_delta_1": oi_delta_1,
        "oi_delta_3": oi_delta_3,
        "breakout_level": breakout_state_5m["breakout_level"],
        "breakout_touched": breakout_state_5m["breakout_touched"],
        "close_above_breakout": breakout_state_5m["close_above_breakout"],
        "hold_above_breakout": breakout_state_5m["hold_above_breakout"],
        "accumulation": oi_delta_3 >= EXECUTION_RULES["oi_buildup_pct"] and abs(ret_12) <= 3.5,
        "ignition": ignition_seed,
    }
    tf_15m = build_timeframe_snapshot(bars_15m, oi_hist_values_15m, "15m") if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else {}
    tf_1h = build_timeframe_snapshot(bars_1h, oi_hist_values_1h, "1h")
    tf_4h = build_timeframe_snapshot(bars_4h, oi_hist_values_4h, "4h") if DYNAMIC_SETTINGS.get("ENABLE_MULTI_TF") else {}
    multi_tf = {"5m": tf_5m, "15m": tf_15m, "1h": tf_1h, "4h": tf_4h}

    # -------- Noise / structure --------
    bar_returns = [pct_change(bars_5m[i]["close"], bars_5m[i - 1]["close"]) for i in range(1, len(bars_5m))]
    noise_metric = safe_div(std(tail(bar_returns, 12), 0.0), abs(avg(tail(bar_returns, 12), 0.0)) + 0.01, 0.0)
    higher_low_bias = last["low"] >= min(prev["low"], prev3["low"])
    follow_through_bias = ret_3 > EXECUTION_RULES["ignition_return_5m_pct"] and last_close >= prev_close

    oi_buildup_supported = compute_dynamic_threshold_signal(
        raw_value=oi_delta_3,
        z_value=zscores["oi_delta_z"],
        raw_threshold=EXECUTION_RULES["oi_buildup_pct"],
        z_threshold=EXECUTION_RULES["oi_buildup_z_threshold"],
    )
    flow_supported = (
        compute_dynamic_threshold_signal(last_buy_ratio, zscores["buy_ratio_z"], EXECUTION_RULES["strong_taker_buy_ratio"], EXECUTION_RULES["buy_ratio_z_threshold"])
        and compute_dynamic_threshold_signal(vol_expansion_last, zscores["volume_expansion_z"], EXECUTION_RULES["min_volume_expansion"], EXECUTION_RULES["volume_expansion_z_threshold"])
        and compute_dynamic_threshold_signal(trade_expansion_last, zscores["trade_expansion_z"], EXECUTION_RULES["min_trade_expansion"], EXECUTION_RULES["trade_expansion_z_threshold"])
    )

    build_up_seed = (
        oi_buildup_supported
        or position_lead_strength >= FAMILY_RULES["POSITION_LED_SQUEEZE_BUILDUP"]["position_lead_min"]
        or account_lead_strength >= FAMILY_RULES["ACCOUNT_LED_ACCUMULATION"]["account_lead_min"]
        or vol_expansion_3 >= 1.15
        or tf_4h.get("accumulation", False)
    ) and ret_6 > -1.5

    fakeout_like = breakout_state_5m["breakout_touched"] and not breakout_state_5m["hold_above_breakout_3bars"] and (flow_ratio_delta < 0 or trade_expansion_last < 1.0)

    counterflow_pattern = detect_counterflow_short_squeeze_pattern({
        "position_account_gap": position_account_gap,
        "pos_features": pos_features,
        "acc_features": acc_features,
        "ratio_multi_tf": ratio_multi_tf,
        "oi_delta_1": oi_delta_1,
        "oi_delta_3": oi_delta_3,
        "oi_up_ratio": oi_up_ratio,
        "breakout_touched": breakout_state_5m["breakout_touched"],
        "close_above_breakout": breakout_state_5m["close_above_breakout"],
        "hold_above_breakout_3bars": breakout_state_5m["hold_above_breakout_3bars"],
        "ret_3": ret_3,
        "trade_expansion_last": trade_expansion_last,
        "vol_expansion_last": vol_expansion_last,
        "price_late": price_late,
    })

    extreme_engine = build_extreme_quantile_inflection_engine(
        ratio_multi_tf,
        {
            "pos_features": pos_features,
            "acc_features": acc_features,
            "global_features": global_features,
            "breakout_touched": breakout_state_5m["breakout_touched"],
            "close_above_breakout": breakout_state_5m["close_above_breakout"],
            "hold_above_breakout_3bars": breakout_state_5m["hold_above_breakout_3bars"],
            "higher_low_bias": higher_low_bias,
            "follow_through_bias": follow_through_bias,
            "fakeout_like": fakeout_like,
            "one_bar_spike": one_bar_spike,
            "price_late": price_late,
            "ret_1": ret_1,
            "ret_3": ret_3,
            "oi_delta_1": oi_delta_1,
            "oi_delta_3": oi_delta_3,
            "oi_up_ratio": oi_up_ratio,
            "short_crowding": short_crowding,
            "long_crowding": long_crowding,
            "ratio_supportive": ratio_alignment.get("overall_supportive", False) or ratio_alignment.get("higher_tf_supportive", False),
            "counterflow_active": counterflow_pattern.get("active", False),
        },
        asset_memory_profile,
    )

    asset_memory_context = summarize_asset_memory_context(symbol, ratio_multi_tf, multi_tf, asset_memory_profile)

    diagnostics.extend(
        [
            f"ret_1={format_pct(ret_1)} ret_3={format_pct(ret_3)} ret_6={format_pct(ret_6)}",
            f"vol_expansion_last={format_num(vol_expansion_last, 2)} trade_expansion_last={format_num(trade_expansion_last, 2)} buy_ratio_last={format_num(last_buy_ratio, 3)}",
            f"oi_delta_1={format_pct(oi_delta_1)} oi_delta_3={format_pct(oi_delta_3)} oi_delta_6={format_pct(oi_delta_6)} oi_z={format_num(zscores['oi_delta_z'], 2)}",
            f"CVD slope={format_num(cvd_metrics.get('cvd_slope'), 2)} CVD ratio={format_num(cvd_metrics.get('cvd_volume_ratio'), 3)} persistence={format_num(cvd_metrics.get('cvd_positive_persistence'), 2)}",
            f"position_lead={format_num(position_lead_strength, 4)} account_lead={format_num(account_lead_strength, 4)} gap_dir={gap_evolution.get('direction')}",
            f"ratio_mtf position={ratio_alignment.get('position', {}).get('state')} account={ratio_alignment.get('account', {}).get('state')} consensus={ratio_alignment.get('consensus', {}).get('state')}",
            f"crowding_regime={crowding_regime.get('state')} short={short_crowding} long={long_crowding}",
            f"ratio_conflict={ratio_conflict.get('state')} penalty={format_num(ratio_conflict.get('penalty'), 2)} extreme_regime={extreme_engine.get('higher_regime')}",
            f"extreme squeeze_prob={format_num(extreme_engine.get('squeeze_probability'), 1)} flush_prob={format_num(extreme_engine.get('flush_probability'), 1)} dominant={extreme_engine.get('dominant_pattern_name')}",
            f"funding={format_num(funding_current, 6)} basis={format_num(basis_current_rate, 6) if basis_current_rate is not None else 'N/A'} 4h_acc={tf_4h.get('accumulation', False)} 1h_ignition={tf_1h.get('ignition', False)}",
            f"counterflow_pattern={counterflow_pattern.get('active', False)} score={counterflow_pattern.get('score', 0)}",
            f"asset_memory={asset_memory_context.get('change_state')} available={asset_memory_context.get('available', False)}",
        ]
    )

    return {
        "ok": True,
        "symbol": symbol,
        "symbol_meta": symbol_meta,
        "ticker_24h": ticker_24h,
        "mark_info": mark_info,
        "diagnostics": diagnostics,
        # أسعار وحركة
        "last_close": last_close,
        "mark_price": mark_price,
        "index_price": index_price,
        "ret_1": ret_1,
        "ret_3": ret_3,
        "ret_6": ret_6,
        "ret_12": ret_12,
        "h1_ret_3": h1_ret_3,
        "breakout_level": breakout_level,
        "breakout_touched": breakout_state_5m["breakout_touched"],
        "close_above_breakout": breakout_state_5m["close_above_breakout"],
        "hold_above_breakout": breakout_state_5m["hold_above_breakout"],
        "hold_above_breakout_3bars": breakout_state_5m["hold_above_breakout_3bars"],
        "extension_pct_from_breakout": extension_pct_from_breakout,
        "extension_atr": extension_atr,
        "price_late": price_late,
        "higher_low_bias": higher_low_bias,
        "follow_through_bias": follow_through_bias,
        "bars_5m": bars_5m,
        "bars_1h": bars_1h,
        "bars_15m": bars_15m,
        "bars_4h": bars_4h,
        # Volume / flow
        "last_quote_vol": last_quote_vol,
        "vol_expansion_last": vol_expansion_last,
        "vol_expansion_3": vol_expansion_3,
        "last_trades": last_trades,
        "trade_expansion_last": trade_expansion_last,
        "trade_expansion_3": trade_expansion_3,
        "last_buy_ratio": last_buy_ratio,
        "recent_buy_ratio": recent_buy_ratio,
        "baseline_buy_ratio": baseline_buy_ratio,
        "flow_ratio_delta": flow_ratio_delta,
        "ofi_proxy": ofi_proxy,
        "taker_ratio_last": taker_features["last"],
        "taker_ratio_delta": taker_features["delta_baseline"],
        # OI
        "oi_contracts_now": oi_contracts_now,
        "oi_value_last": oi_value_last,
        "oi_hist_values": oi_hist_values,
        "oi_hist_contracts": oi_hist_contracts,
        "oi_delta_1": oi_delta_1,
        "oi_delta_3": oi_delta_3,
        "oi_delta_6": oi_delta_6,
        "oi_up_ratio": oi_up_ratio,
        # Ratios
        "pos_features": pos_features,
        "acc_features": acc_features,
        "global_features": global_features,
        "taker_features": taker_features,
        "position_lead_strength": position_lead_strength,
        "account_lead_strength": account_lead_strength,
        "global_ratio_last": global_ratio_last,
        "short_crowding": short_crowding,
        "long_crowding": long_crowding,
        "crowding_regime": crowding_regime,
        "position_account_gap": position_account_gap,
        "gap_evolution": gap_evolution,
        # Context
        "funding_current": funding_current,
        "funding_history_values": funding_history_values,
        "basis_current_rate": basis_current_rate,
        "mark_basis_rate": mark_basis_rate,
        "funding_context": context_labels["funding_context"],
        "funding_notes": context_labels["funding_notes"],
        "basis_context": context_labels["basis_context"],
        "basis_notes": context_labels["basis_notes"],
        # structure / timing
        "atr_pct": atr_pct,
        "ignition_seed": ignition_seed,
        "build_up_seed": build_up_seed,
        "one_bar_spike": one_bar_spike,
        "fakeout_like": fakeout_like,
        "noise_metric": noise_metric,
        # الجديد
        "cvd_pack": cvd_pack,
        "cvd_metrics": cvd_metrics,
        "zscores": zscores,
        "volume_expansion_series": volume_expansion_series,
        "trade_expansion_series": trade_expansion_series,
        "buy_ratio_series": buy_ratio_series,
        "oi_delta_series": oi_delta_series,
        "ratio_multi_tf": ratio_multi_tf,
        "ratio_alignment": ratio_alignment,
        "ratio_conflict": ratio_conflict,
        "extreme_engine": extreme_engine,
        "ratio_extreme_profile": safe_dict_from_api(extreme_engine.get("profiles")),
        "ratio_inflection_profile": safe_dict_from_api(extreme_engine.get("inflection")),
        "asset_local_memory": asset_local_memory,
        "asset_memory_profile": asset_memory_profile,
        "asset_memory_context": asset_memory_context,
        "leader_evidence": {},
        "acceptance_evidence": {},
        "failure_evidence": {},
        "flow_quality_profile": {},
        "oi_context_profile": {},
        "hypothesis_inputs": {},
        "multi_tf": multi_tf,
        "tf_5m": tf_5m,
        "tf_15m": tf_15m,
        "tf_1h": tf_1h,
        "tf_4h": tf_4h,
        "oi_buildup_supported": oi_buildup_supported,
        "flow_supported": flow_supported,
        "counterflow_pattern": counterflow_pattern,
    }


# ============================================================
# تحديد العائلة الرئيسية
# ============================================================


def resolve_primary_family_signal(features: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    family: Optional[str] = None
    decisive_factor = "لا يوجد عامل حاسم بعد"

    gap = features.get("position_account_gap", {})
    gap_evolution = features.get("gap_evolution", {})
    counterflow_pattern = features.get("counterflow_pattern", {})
    ratio_alignment = features.get("ratio_alignment", {})
    ratio_conflict = features.get("ratio_conflict", {})
    extreme_engine = features.get("extreme_engine", {})
    extreme_patterns = extreme_engine.get("patterns", {})
    crowding_regime = features.get("crowding_regime", {})

    position_lead = safe_float(gap.get("position_lead"), 0.0)
    account_lead = safe_float(gap.get("account_lead"), 0.0)
    gap_direction = gap_evolution.get("direction", "stable")

    long_position_ratio = safe_float(gap.get("long_position_ratio"), 0.0)
    long_account_ratio = safe_float(gap.get("long_account_ratio"), 0.0)

    position_alignment = ratio_alignment.get("position", {})
    account_alignment = ratio_alignment.get("account", {})
    consensus_alignment = ratio_alignment.get("consensus", {})
    flow_alignment = ratio_alignment.get("flow", {})

    pos_score = safe_float(position_alignment.get("score"), 0.0)
    acc_score = safe_float(account_alignment.get("score"), 0.0)
    cons_score = safe_float(consensus_alignment.get("score"), 0.0)
    flow_score = safe_float(flow_alignment.get("score"), 0.0)
    pos_norm = safe_float(position_alignment.get("normalized"), 0.0)
    acc_norm = safe_float(account_alignment.get("normalized"), 0.0)
    cons_norm = safe_float(consensus_alignment.get("normalized"), 0.0)
    flow_norm = safe_float(flow_alignment.get("normalized"), 0.0)

    higher_tf_ratio_supportive = ratio_alignment.get("higher_tf_supportive", False)
    overall_ratio_conflict = ratio_alignment.get("overall_conflict", False)

    lower_extreme_squeeze = extreme_patterns.get("lower_extreme_squeeze", {})
    upper_extreme_bullish = extreme_patterns.get("upper_extreme_bullish_expansion", {})
    upper_extreme_failure = extreme_patterns.get("upper_extreme_failure", {})
    long_crowding_rollover = extreme_patterns.get("long_crowding_rollover", {})

    flow_strong = (features.get("flow_supported", False) or flow_alignment.get("state") in {"aligned", "supportive"}) or (
        features["recent_buy_ratio"] >= EXECUTION_RULES["strong_taker_buy_ratio"]
        and features["trade_expansion_last"] >= EXECUTION_RULES["min_trade_expansion"]
        and features["vol_expansion_last"] >= EXECUTION_RULES["min_volume_expansion"]
    )
    price_early_enough = not features["price_late"]
    price_not_rejected = features["close_above_breakout"] or features["higher_low_bias"] or features["ret_3"] > 0
    oi_supportive = features.get("oi_buildup_supported", False) or features["oi_delta_1"] > 0
    global_delta = safe_float(features.get("global_features", {}).get("delta_baseline"), 0.0)

    consensus_like = (
        long_position_ratio > 0
        and long_account_ratio > 0
        and abs(long_position_ratio - long_account_ratio)
        <= FAMILY_RULES["CONSENSUS_BULLISH_EXPANSION"]["max_internal_disagreement"]
        and global_delta > 0
        and oi_supportive
        and price_not_rejected
    )

    severe_bearish_context = (
        upper_extreme_failure.get("active", False)
        or long_crowding_rollover.get("active", False)
        or ratio_conflict.get("state") == "conflicted"
    ) and not counterflow_pattern.get("active", False)

    family_scores: Dict[str, float] = {name: 0.0 for name in FAMILIES}
    family_reasons: Dict[str, List[str]] = {name: [] for name in FAMILIES}

    def add_score(name: str, points: float, reason: str) -> None:
        family_scores[name] += points
        if reason:
            family_reasons[name].append(reason)

    def add_base_support(name: str) -> None:
        if oi_supportive:
            add_score(name, 0.8, "OI داعم")
        if price_not_rejected:
            add_score(name, 0.8, "السعر لم يرفض الحركة")
        if price_early_enough:
            add_score(name, 0.6, "السعر ما زال مبكرًا")
        if higher_tf_ratio_supportive:
            add_score(name, 0.8, "الفريمات الأعلى تدعم")

    # POSITION_LED_SQUEEZE_BUILDUP
    add_base_support("POSITION_LED_SQUEEZE_BUILDUP")
    if position_lead >= FAMILY_RULES["POSITION_LED_SQUEEZE_BUILDUP"]["position_lead_min"]:
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 2.0, "position_lead 5m موجب")
    if gap_direction in {"widening_position", "stable"}:
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 0.8, "gap position مستقر/يتسع")
    if pos_score > 0:
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 1.2 + max(pos_norm, 0.0) * 2.0, "position alignment MTF داعم")
    if position_alignment.get("higher_tf_positive", False):
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 1.6, "position lead مدعوم على 1h/4h")
    if crowding_regime.get("higher_tf_short_crowded", False):
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 1.2, "crowded short regime على الفريمات الأعلى")
    if lower_extreme_squeeze.get("active", False):
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 3.0, "lower-extreme squeeze active")
    if acc_score < 0 and pos_score > 0:
        add_score("POSITION_LED_SQUEEZE_BUILDUP", 0.8, "المراكز تقود أكثر من الحسابات")

    # ACCOUNT_LED_ACCUMULATION
    add_base_support("ACCOUNT_LED_ACCUMULATION")
    if account_lead >= FAMILY_RULES["ACCOUNT_LED_ACCUMULATION"]["account_lead_min"]:
        add_score("ACCOUNT_LED_ACCUMULATION", 2.0, "account_lead 5m موجب")
    if global_delta > FAMILY_RULES["ACCOUNT_LED_ACCUMULATION"]["global_delta_min"]:
        add_score("ACCOUNT_LED_ACCUMULATION", 1.0, "global ratio يتحسن")
    if acc_score > 0:
        add_score("ACCOUNT_LED_ACCUMULATION", 1.2 + max(acc_norm, 0.0) * 2.0, "account alignment MTF داعم")
    if account_alignment.get("higher_tf_positive", False):
        add_score("ACCOUNT_LED_ACCUMULATION", 1.6, "account lead مدعوم على 1h/4h")
    if pos_score <= 0 and acc_score > 0:
        add_score("ACCOUNT_LED_ACCUMULATION", 0.7, "الحسابات تقود قبل المراكز")

    # CONSENSUS_BULLISH_EXPANSION
    add_base_support("CONSENSUS_BULLISH_EXPANSION")
    if consensus_like:
        add_score("CONSENSUS_BULLISH_EXPANSION", 2.0, "consensus-like baseline")
    if cons_score > 0:
        add_score("CONSENSUS_BULLISH_EXPANSION", 1.2 + max(cons_norm, 0.0) * 2.0, "consensus alignment MTF داعم")
    if flow_strong:
        add_score("CONSENSUS_BULLISH_EXPANSION", 0.9, "flow قوي")
    if upper_extreme_bullish.get("active", False):
        add_score("CONSENSUS_BULLISH_EXPANSION", 2.6, "upper-extreme bullish expansion active")
    if crowding_regime.get("higher_tf_long_crowded", False) and not upper_extreme_failure.get("active", False):
        add_score("CONSENSUS_BULLISH_EXPANSION", 0.8, "crowded long regime لم يفشل بعد")

    # ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION
    add_base_support("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION")
    if account_lead >= FAMILY_RULES["ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION"]["account_lead_min"]:
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", 1.6, "account lead موجب")
    if global_delta >= FAMILY_RULES["ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION"]["global_delta_min"]:
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", 1.0, "global delta داعم")
    if acc_score > 0:
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", 1.0 + max(acc_norm, 0.0) * 1.6, "account alignment MTF داعم")
    if cons_score > 0:
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", 0.8 + max(cons_norm, 0.0) * 1.4, "consensus alignment MTF داعم")
    if upper_extreme_bullish.get("active", False):
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", 2.0, "upper-extreme bullish expansion")
    if counterflow_pattern.get("active", False) and oi_supportive and price_not_rejected:
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", 4.5, "counterflow/MAGMA-like regime")

    # FLOW_LIQUIDITY_VACUUM_BREAKOUT
    add_base_support("FLOW_LIQUIDITY_VACUUM_BREAKOUT")
    if flow_strong:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 2.2, "flow execution قوي")
    if features["breakout_touched"]:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 1.2, "breakout touched")
    if features["close_above_breakout"] or features["hold_above_breakout_3bars"]:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 1.8, "قبول مبدئي فوق الاختراق")
    if features["trade_expansion_last"] >= EXECUTION_RULES["min_trade_expansion"]:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 0.9, "trade expansion")
    if features["vol_expansion_last"] >= EXECUTION_RULES["min_volume_expansion"]:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 0.9, "volume expansion")
    if flow_score > 0:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 1.0 + max(flow_norm, 0.0) * 1.4, "flow alignment MTF داعم")
    if lower_extreme_squeeze.get("active", False) and flow_strong:
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", 0.8, "flow + lower-extreme squeeze")

    # Bonuses مشتركة حسب البيئة
    if severe_bearish_context:
        add_score("POSITION_LED_SQUEEZE_BUILDUP", -2.2, "severe bearish context")
        add_score("ACCOUNT_LED_ACCUMULATION", -2.2, "severe bearish context")
        add_score("CONSENSUS_BULLISH_EXPANSION", -2.6, "severe bearish context")
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", -2.0, "severe bearish context")
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", -1.4, "severe bearish context")
    if upper_extreme_failure.get("active", False):
        add_score("CONSENSUS_BULLISH_EXPANSION", -2.0, "upper-extreme failure active")
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", -1.6, "upper-extreme failure active")
        add_score("FLOW_LIQUIDITY_VACUUM_BREAKOUT", -0.8, "upper-extreme failure active")
    if long_crowding_rollover.get("active", False):
        add_score("CONSENSUS_BULLISH_EXPANSION", -1.8, "long crowding rollover")
        add_score("ACCOUNT_LED_ACCUMULATION", -1.0, "long crowding rollover")
        add_score("ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION", -1.2, "long crowding rollover")
    if not price_not_rejected:
        for name in family_scores:
            add_score(name, -1.6, "price rejection")
    if not oi_supportive:
        for name in family_scores:
            add_score(name, -1.2, "OI غير داعم")
    if overall_ratio_conflict:
        for name in family_scores:
            add_score(name, -0.9, "ratio conflict")
    if not price_early_enough:
        for name in family_scores:
            add_score(name, -0.7, "price late")

    ranked = sorted(family_scores.items(), key=lambda item: item[1], reverse=True)
    best_family, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0

    if best_score >= 4.2 and (best_score - second_score >= 0.4 or best_score >= 5.0):
        family = best_family
    else:
        family = None

    if family == "POSITION_LED_SQUEEZE_BUILDUP":
        decisive_factor = "العائلة حُسمت عبر score متعدد الفريمات يميل لقيادة المراكز مع دعم higher TF"
    elif family == "ACCOUNT_LED_ACCUMULATION":
        decisive_factor = "العائلة حُسمت عبر score متعدد الفريمات يميل لقيادة الحسابات قبل اتساع الإجماع"
    elif family == "CONSENSUS_BULLISH_EXPANSION":
        decisive_factor = "العائلة حُسمت عبر score متعدد الفريمات يبين توافق accounts/positions/global مع OI"
    elif family == "ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION":
        decisive_factor = "العائلة حُسمت عبر score متعدد الفريمات يلتقط قيادة الحسابات/الإجماع أو بصمة counterflow"
    elif family == "FLOW_LIQUIDITY_VACUUM_BREAKOUT":
        decisive_factor = "العائلة حُسمت عبر score متعدد الفريمات يفضل flow + breakout + expansion"

    score_line = ", ".join(f"{name}={family_scores[name]:.2f}" for name in FAMILIES)
    diagnostics.append(f"family_scores: {score_line}")

    if family is not None:
        top_reasons = family_reasons.get(family, [])[:4]
        if top_reasons:
            diagnostics.append(f"family_pick={family} because: " + " | ".join(top_reasons))
    else:
        diagnostics.append("لم يتم حسم العائلة لأن score العائلات متقارب أو دون العتبة")

    if upper_extreme_failure.get("active", False):
        diagnostics.append("تحذير: يوجد Upper-Extreme failure / flush risk فوق العائلة المختارة")
    if long_crowding_rollover.get("active", False):
        diagnostics.append("تحذير: يوجد Long Crowding Rollover وقد يضعف صلاحية العائلة للتنفيذ")

    diagnostics.extend(ratio_alignment.get("diagnostics", []))
    diagnostics.extend((features.get("extreme_engine") or {}).get("diagnostics", [])[:3])

    return {
        "family": family,
        "decisive_factor": decisive_factor,
        "family_scores": family_scores,
        "family_reasons": family_reasons,
        "diagnostics": diagnostics,
    }


def classify_signal_stage_from_reference(features: Dict[str, Any], family_info: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    multi_tf = features.get("multi_tf", {})
    tf_15m = multi_tf.get("15m", {})
    tf_1h = multi_tf.get("1h", {})
    tf_4h = multi_tf.get("4h", {})
    counterflow_pattern = features.get("counterflow_pattern", {})
    ratio_alignment = features.get("ratio_alignment", {})
    extreme_engine = features.get("extreme_engine", {})
    patterns = extreme_engine.get("patterns", {})

    higher_tf_buildup = tf_4h.get("accumulation", False) or tf_4h.get("oi_delta_3", 0.0) >= EXECUTION_RULES["oi_buildup_pct"]
    mid_tf_ignition = tf_1h.get("ignition", False) or tf_15m.get("ignition", False)
    low_tf_ignition = features["breakout_touched"] or features["ret_3"] >= EXECUTION_RULES["ignition_return_5m_pct"]
    contradictory = tf_4h.get("ret_12", 0.0) < -4.0 and not higher_tf_buildup
    counterflow_ready = counterflow_pattern.get("armed_ready", False) and features.get("oi_delta_3", 0.0) >= EXECUTION_RULES["oi_expansion_pct"]
    ratio_supportive = ratio_alignment.get("higher_tf_supportive", False) or ratio_alignment.get("overall_supportive", False)
    severe_flush_risk = (
        (patterns.get("upper_extreme_failure") or {}).get("active", False)
        or (patterns.get("long_crowding_rollover") or {}).get("active", False)
    ) and not counterflow_pattern.get("active", False)
    lower_extreme_ready = (patterns.get("lower_extreme_squeeze") or {}).get("active", False)
    upper_bull_ready = (patterns.get("upper_extreme_bullish_expansion") or {}).get("active", False)

    stage = "WATCH"
    stage_reason = "لا يوجد اشتعال أو تراكم كافٍ"

    if features["price_late"] and (low_tf_ignition or features["hold_above_breakout_3bars"]):
        stage = "LATE"
        stage_reason = "الحركة ممتدة بعد ignition/acceptance"
        diagnostics.append(stage_reason)
        return {"stage": stage, "stage_reason": stage_reason, "diagnostics": diagnostics}

    if severe_flush_risk and not features.get("hold_above_breakout_3bars", False):
        stage = "WATCH"
        stage_reason = "هناك upper-extreme failure / long crowding rollover يضغط ضد البناء الحالي"
        diagnostics.append(stage_reason)
        return {"stage": stage, "stage_reason": stage_reason, "diagnostics": diagnostics}

    if low_tf_ignition and (mid_tf_ignition or higher_tf_buildup or counterflow_ready or ratio_supportive or lower_extreme_ready or upper_bull_ready):
        if features["hold_above_breakout_3bars"]:
            stage = "TRIGGERED"
            stage_reason = "4h/1h أو extreme-regime يدعمان و 5m بدأ hold فعلي"
            if counterflow_ready:
                stage_reason += " مع بصمة counterflow squeeze"
            diagnostics.append(stage_reason)
        else:
            stage = "ARMED"
            stage_reason = "التراكم الأعلى زمنيًا التقى مع ignition لكن hold غير مكتمل"
            if lower_extreme_ready:
                stage_reason += " ضمن lower-extreme squeeze regime"
            if upper_bull_ready:
                stage_reason += " ضمن upper-extreme bullish expansion"
            if counterflow_ready:
                stage_reason += " وبصمة MAGMA-like رفعت الجاهزية"
            diagnostics.append(stage_reason)
        return {"stage": stage, "stage_reason": stage_reason, "diagnostics": diagnostics}

    if family_info.get("family") is not None and (higher_tf_buildup or features.get("build_up_seed") or counterflow_pattern.get("active", False) or ratio_supportive or lower_extreme_ready or upper_bull_ready):
        stage = "PREPARE"
        stage_reason = "4h/1h أو extreme-regime يظهران تراكمًا دون قبول كامل"
        if counterflow_pattern.get("active", False):
            stage_reason += " مع وجود انقسام حسابات/مراكز صالح للضغط"
        if ratio_supportive:
            stage_reason += " مع دعم ratios متعدد الفريمات"
        if lower_extreme_ready:
            stage_reason += " ومع lower-extreme regime"
        if upper_bull_ready:
            stage_reason += " ومع upper-extreme bullish regime"
        if contradictory:
            stage_reason += " (تناقضات زمنية)"
        diagnostics.append(stage_reason)
        return {"stage": stage, "stage_reason": stage_reason, "diagnostics": diagnostics}

    stage_reason = "البذور موجودة لكن التسلسل الزمني لم يكتمل"
    diagnostics.append(stage_reason)
    return {"stage": stage, "stage_reason": stage_reason, "diagnostics": diagnostics}


def detect_squeeze_setup(features: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    try:
        oi_values = tail(features.get("oi_hist_values", []), safe_int(DYNAMIC_SETTINGS.get("OI_SEQUENCE_WINDOW", 12), 12))
        closes = [b["close"] for b in tail(features.get("bars_5m", []), safe_int(DYNAMIC_SETTINGS.get("OI_SEQUENCE_WINDOW", 12), 12))]
        if len(oi_values) < 6 or len(closes) < 6:
            return {"active": False, "diagnostics": ["بيانات squeeze setup غير كافية"]}

        oi_rise = pct_change(oi_values[-1], oi_values[0])
        price_change = pct_change(closes[-1], closes[0])
        funding_tail = tail(features.get("funding_history_values", []), 2)
        deep_negative = all(v <= EXECUTION_RULES["funding_squeeze_negative_pct"] for v in funding_tail) if funding_tail else False
        active = (
            oi_rise >= EXECUTION_RULES["oi_buildup_pct"]
            and price_change <= EXECUTION_RULES["premove_price_flat_pct"]
            and features.get("short_crowding", False)
            and deep_negative
        )
        if active:
            diagnostics.append("squeeze setup: OI يبني مع سعر ثابت/هابط وshort crowding وتمويل سلبي")
        return {
            "active": active,
            "oi_rise": oi_rise,
            "price_change": price_change,
            "deep_negative_funding": deep_negative,
            "diagnostics": diagnostics,
        }
    except Exception:
        return {"active": False, "diagnostics": ["تعذر تقييم squeeze setup"]}


def detect_squeeze_ignition(features: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    try:
        oi_values = tail(features.get("oi_hist_values", []), 4)
        bars = tail(features.get("bars_5m", []), 4)
        if len(oi_values) < 4 or len(bars) < 4:
            return {"active": False, "diagnostics": ["بيانات squeeze ignition غير كافية"]}
        oi_drop = pct_change(oi_values[-1], oi_values[0])
        price_rise = pct_change(bars[-1]["close"], bars[0]["close"])
        vol_rise = safe_float(features.get("vol_expansion_last"), 0.0)
        active = (
            oi_drop <= EXECUTION_RULES["oi_collapse_pct"]
            and price_rise >= EXECUTION_RULES["ignition_return_5m_pct"]
            and vol_rise >= EXECUTION_RULES["min_volume_expansion"]
        )
        if active:
            diagnostics.append("squeeze ignition: OI ينخفض مع صعود السعر وتوسع الحجم")
        return {
            "active": active,
            "oi_drop": oi_drop,
            "price_rise": price_rise,
            "diagnostics": diagnostics,
        }
    except Exception:
        return {"active": False, "diagnostics": ["تعذر تقييم squeeze ignition"]}


def detect_squeeze_continuation(features: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    try:
        oi_values = tail(features.get("oi_hist_values", []), 6)
        if len(oi_values) < 4:
            return {"active": False, "diagnostics": ["بيانات squeeze continuation غير كافية"]}
        prior_drop = pct_change(oi_values[-3], oi_values[0]) <= EXECUTION_RULES["oi_collapse_pct"]
        recent_rebuild = pct_change(oi_values[-1], oi_values[-3]) >= 0.30
        price_holds = features.get("hold_above_breakout_3bars", False) or features.get("close_above_breakout", False)
        active = prior_drop and recent_rebuild and price_holds
        if active:
            diagnostics.append("squeeze continuation: OI عاد للارتفاع بعد covering مع بقاء السعر مرتفعًا")
        return {
            "active": active,
            "prior_drop": prior_drop,
            "recent_rebuild": recent_rebuild,
            "diagnostics": diagnostics,
        }
    except Exception:
        return {"active": False, "diagnostics": ["تعذر تقييم squeeze continuation"]}

def classify_oi_dynamics(features: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    seq_window = safe_int(DYNAMIC_SETTINGS.get("OI_SEQUENCE_WINDOW", 12), 12)
    oi_values = tail(features.get("oi_hist_values", []), seq_window + 1)
    price_closes = [b["close"] for b in tail(features.get("bars_5m", []), seq_window + 1)]
    oi_state_sequence: List[str] = []

    try:
        n = min(len(oi_values), len(price_closes))
        oi_values = oi_values[-n:]
        price_closes = price_closes[-n:]
        for i in range(1, n):
            oi_pct = pct_change(oi_values[i], oi_values[i - 1])
            px_pct = pct_change(price_closes[i], price_closes[i - 1])
            if oi_pct >= EXECUTION_RULES["oi_buildup_pct"] and px_pct <= EXECUTION_RULES["premove_price_flat_pct"] and px_pct > -1.5:
                oi_state_sequence.append("premove buildup")
            elif oi_pct >= EXECUTION_RULES["oi_expansion_pct"] and px_pct > 0.15:
                oi_state_sequence.append("buildup expansion")
            elif oi_pct <= EXECUTION_RULES["oi_collapse_pct"] and px_pct > 0.15:
                oi_state_sequence.append("covering")
            elif abs(oi_pct) <= EXECUTION_RULES["oi_flat_abs_pct"] and px_pct > 0.15:
                oi_state_sequence.append("price up OI flat")
            else:
                oi_state_sequence.append("neutral")

        squeeze_setup = detect_squeeze_setup(features) if DYNAMIC_SETTINGS.get("ENABLE_SQUEEZE_LOGIC") else {"active": False, "diagnostics": []}
        squeeze_ignition = detect_squeeze_ignition(features) if DYNAMIC_SETTINGS.get("ENABLE_SQUEEZE_LOGIC") else {"active": False, "diagnostics": []}
        squeeze_continuation = detect_squeeze_continuation(features) if DYNAMIC_SETTINGS.get("ENABLE_SQUEEZE_LOGIC") else {"active": False, "diagnostics": []}

        final_state = "neutral"
        if squeeze_setup.get("active") and not squeeze_ignition.get("active"):
            final_state = "squeeze_buildup"
        elif squeeze_ignition.get("active") and not squeeze_continuation.get("active"):
            final_state = "squeeze_covering"
        elif squeeze_ignition.get("active") and squeeze_continuation.get("active"):
            final_state = "squeeze_post_covering"
        else:
            if oi_state_sequence:
                counts = defaultdict(int)
                for state in oi_state_sequence:
                    counts[state] += 1
                final_state = max(counts.items(), key=lambda x: x[1])[0]

        diagnostics.append(f"oi_state_sequence={oi_state_sequence[-6:]}")
        diagnostics.extend(squeeze_setup.get("diagnostics", []))
        diagnostics.extend(squeeze_ignition.get("diagnostics", []))
        diagnostics.extend(squeeze_continuation.get("diagnostics", []))
        diagnostics.append(f"OI dynamics final state = {final_state}")

        return {
            "oi_state": final_state,
            "oi_state_sequence": oi_state_sequence,
            "squeeze_setup": squeeze_setup,
            "squeeze_ignition": squeeze_ignition,
            "squeeze_continuation": squeeze_continuation,
            "diagnostics": diagnostics,
        }
    except Exception as exc:
        return {
            "oi_state": "neutral",
            "oi_state_sequence": oi_state_sequence,
            "squeeze_setup": {"active": False},
            "squeeze_ignition": {"active": False},
            "squeeze_continuation": {"active": False},
            "diagnostics": [f"تعذر تصنيف OI dynamics: {exc}"],
        }


def classify_oi_state(features: Dict[str, Any]) -> Dict[str, Any]:
    return classify_oi_dynamics(features)


# ============================================================
# فحص القبول السعري
# ============================================================



def detect_acceptance_state(features: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics: List[str] = []
    accepted = False
    partial = False
    acceptance_reason = ""

    counterflow_pattern = features.get("counterflow_pattern", {}) or {}
    ratio_alignment = features.get("ratio_alignment", {}) or {}
    crowding_regime = features.get("crowding_regime", {}) or {}
    crowding_edges = crowding_regime.get("edge_flags", {}) or {}
    extreme_engine = features.get("extreme_engine", {}) or {}
    extreme_patterns = extreme_engine.get("patterns", {}) or {}
    precomputed_post_flush = features.get("precomputed_post_flush_patterns", {}) or {}
    bullish_rebuild = precomputed_post_flush.get("bullish_rebuild", {}) or {}
    relief_bounce = precomputed_post_flush.get("relief_bounce", {}) or {}
    failed_rebuild = precomputed_post_flush.get("failed_rebuild", {}) or {}

    oi_not_collapsing = features["oi_delta_1"] > EXECUTION_RULES["oi_collapse_pct"]
    buy_pressure_persistent = (
        features["recent_buy_ratio"] >= EXECUTION_RULES["persistent_taker_buy_ratio"]
        and features["flow_ratio_delta"] > -0.01
    )
    trade_activity_alive = features["trade_expansion_last"] >= EXECUTION_RULES["min_trade_activity_hold"]
    flow_supported = bool(features.get("flow_supported", False))
    follow_through = features["follow_through_bias"]
    hold_confirmed = features.get("hold_above_breakout_3bars", False)
    close_above = features.get("close_above_breakout", False)
    breakout_touched = features.get("breakout_touched", False)
    higher_low_bias = features.get("higher_low_bias", False)
    ret_1 = safe_float(features.get("ret_1", 0.0), 0.0)
    ret_3 = safe_float(features.get("ret_3", 0.0), 0.0)
    breakout_level = safe_float(features.get("breakout_level", 0.0), 0.0)
    last_close = safe_float(features.get("last_close", 0.0), 0.0)
    price_late = bool(features.get("price_late", False))

    ignition_like = breakout_touched or close_above or higher_low_bias or ret_3 >= EXECUTION_RULES["ignition_return_5m_pct"]
    price_reaccept_proxy = (
        close_above
        or hold_confirmed
        or higher_low_bias
        or (breakout_level > 0 and last_close >= breakout_level * 0.995)
        or (ret_1 > 0 and ret_3 > 0 and breakout_touched)
    )

    oi_supportive = bool(features.get("oi_buildup_supported", False)) or features.get("oi_delta_3", 0.0) >= EXECUTION_RULES["min_oi_increase_for_support"]
    gap_supportive = (features.get("position_account_gap", {}) or {}).get("position_lead", 0.0) >= EXECUTION_RULES["min_position_lead_for_support"]
    ratio_supportive = bool(ratio_alignment.get("overall_supportive", False) or ratio_alignment.get("higher_tf_supportive", False))
    ratio_conflicted = bool(ratio_alignment.get("overall_conflict", False) or ratio_alignment.get("higher_tf_conflict", False))

    lower_extreme_squeeze_active = bool((extreme_patterns.get("lower_extreme_squeeze") or {}).get("active", False))
    upper_bullish_expansion_active = bool((extreme_patterns.get("upper_extreme_bullish_expansion") or {}).get("active", False))
    long_rollover_active = bool((extreme_patterns.get("long_crowding_rollover") or {}).get("active", False))
    upper_failure_active = bool((extreme_patterns.get("upper_extreme_failure") or {}).get("active", False))
    short_squeeze_edge = bool(crowding_edges.get("short_squeeze_edge_frames"))
    long_rollover_edge = bool(crowding_edges.get("long_rollover_edge_frames"))
    counterflow_active = bool(counterflow_pattern.get("active", False))
    counterflow_explosive = bool(counterflow_pattern.get("explosive_oi", False))
    squeeze_probability = safe_float(extreme_engine.get("squeeze_probability"), 0.0)
    bullish_rebuild_oi = bool(bullish_rebuild.get("oi_rebuilding", False))
    bullish_rebuild_strong = bool(bullish_rebuild.get("oi_strong_rebuild", False))

    strict_acceptance = (
        close_above
        and hold_confirmed
        and oi_not_collapsing
        and trade_activity_alive
        and (oi_supportive or gap_supportive or ratio_supportive)
        and (flow_supported or buy_pressure_persistent)
        and follow_through
        and not ratio_conflicted
        and not long_rollover_active
        and not upper_failure_active
        and not failed_rebuild.get("active", False)
    )

    lower_extreme_regime_acceptance = (
        (lower_extreme_squeeze_active or short_squeeze_edge)
        and ignition_like
        and not price_late
        and oi_not_collapsing
        and (oi_supportive or counterflow_explosive or squeeze_probability >= 55)
        and (flow_supported or buy_pressure_persistent or trade_activity_alive or ratio_supportive)
        and (price_reaccept_proxy or higher_low_bias or ret_1 > 0.0 or ret_3 > 0.20)
        and not upper_failure_active
        and not long_rollover_edge
    )

    bullish_rebuild_regime_acceptance = (
        bullish_rebuild.get("active", False)
        and not relief_bounce.get("active", False)
        and not failed_rebuild.get("active", False)
        and not price_late
        and oi_not_collapsing
        and (oi_supportive or bullish_rebuild_oi or bullish_rebuild_strong)
        and (
            bullish_rebuild.get("rebuilding_count", 0) >= 2
            or (bullish_rebuild.get("rebuilding_count", 0) >= 1 and bullish_rebuild_strong)
        )
        and (
            bullish_rebuild.get("price_reaccepted", False)
            or price_reaccept_proxy
            or higher_low_bias
        )
    )

    counterflow_regime_acceptance = (
        counterflow_active
        and ignition_like
        and not price_late
        and oi_not_collapsing
        and (counterflow_explosive or oi_supportive or squeeze_probability >= 58)
        and (price_reaccept_proxy or higher_low_bias or ret_1 > 0.12 or ret_3 > 0.35)
        and (flow_supported or trade_activity_alive or ratio_supportive)
        and not ratio_conflicted
    )

    upper_expansion_regime_acceptance = (
        upper_bullish_expansion_active
        and not upper_failure_active
        and not long_rollover_active
        and ignition_like
        and not price_late
        and oi_not_collapsing
        and ratio_supportive
        and (flow_supported or buy_pressure_persistent or trade_activity_alive)
        and (price_reaccept_proxy or hold_confirmed or ret_3 > 0.35)
    )

    regime_anchor_strength = sum([
        lower_extreme_regime_acceptance,
        bullish_rebuild_regime_acceptance,
        counterflow_regime_acceptance,
        upper_expansion_regime_acceptance,
    ])

    early_regime_partial = (
        regime_anchor_strength >= 1
        and ignition_like
        and not price_late
        and oi_not_collapsing
        and (oi_supportive or gap_supportive or ratio_supportive or bullish_rebuild_oi)
        and (flow_supported or trade_activity_alive or buy_pressure_persistent)
        and not upper_failure_active
        and not long_rollover_active
    )

    structural_acceptance = (
        not price_late
        and (price_reaccept_proxy or higher_low_bias or (breakout_touched and ret_1 > -0.05))
        and oi_not_collapsing
        and (oi_supportive or ratio_supportive or trade_activity_alive)
        and not upper_failure_active
        and not long_rollover_active
        and not features.get("one_bar_spike", False)
    )

    if strict_acceptance:
        accepted = True
        acceptance_reason = "قبول صارم: إغلاق واختراق مثبت مع دعم OI والتدفق والنشاط"
        diagnostics.append(acceptance_reason)
    elif lower_extreme_regime_acceptance:
        accepted = True
        acceptance_reason = "قبول نظامي مبكر: عصر صاعد من تطرف سفلي مع OI داعم ورفض واضح للهبوط"
        diagnostics.append(acceptance_reason)
    elif bullish_rebuild_regime_acceptance:
        accepted = True
        acceptance_reason = "قبول نظامي: إعادة بناء صاعدة بعد التفريغ مع عودة OI واستعادة المستوى"
        diagnostics.append(acceptance_reason)
    elif counterflow_regime_acceptance:
        accepted = True
        acceptance_reason = "قبول نظامي: إعادة تسعير عكسية قوية قبل اكتمال hold الكلاسيكي"
        diagnostics.append(acceptance_reason)
    elif upper_expansion_regime_acceptance:
        accepted = True
        acceptance_reason = "قبول نظامي: توسع صاعد من تطرف علوي مع قبول سعري ودعم من النسب"
        diagnostics.append(acceptance_reason)
    elif early_regime_partial:
        partial = True
        acceptance_reason = "قبول جزئي نظامي: النظام البنيوي داعم لكن القبول السعري ما زال في طور الاكتمال"
        diagnostics.append(acceptance_reason)
    elif structural_acceptance:
        accepted = True
        acceptance_reason = "قبول بنيوي بصري: السعر حافظ على منطقة الاستعادة مع قيعان متحسنة دون إشارات فخ بارزة"
        diagnostics.append(acceptance_reason)
    elif (
        ignition_like
        and not price_late
        and oi_not_collapsing
        and (price_reaccept_proxy or breakout_touched)
        and (oi_supportive or gap_supportive or ratio_supportive)
        and (flow_supported or buy_pressure_persistent or trade_activity_alive)
        and not upper_failure_active
        and not long_rollover_active
    ):
        partial = True
        acceptance_reason = "قبول جزئي: البنية بدأت تتشكل لكن التأكيد السعري لم يكتمل بعد"
        diagnostics.append(acceptance_reason)
    else:
        diagnostics.append("لا يوجد قبول سعري/بنيوي مقنع بعد")

    if ratio_conflicted and accepted and not (counterflow_active or bullish_rebuild.get("active", False) or lower_extreme_regime_acceptance):
        accepted = False
        partial = True
        acceptance_reason = "القبول موجود جزئيًا لكن تعارض الفريمات يمنع اعتباره قبولًا كاملًا"
        diagnostics.append(acceptance_reason)

    return {
        "accepted": accepted,
        "partial": partial,
        "state": "yes" if accepted else ("partial" if partial else "no"),
        "acceptance_reason": acceptance_reason,
        "strict_acceptance": strict_acceptance,
        "regime_acceptance": regime_anchor_strength >= 1,
        "structural_acceptance": structural_acceptance,
        "diagnostics": diagnostics,
    }

def detect_failure_continuation(features: Dict[str, Any], acceptance: Dict[str, Any], oi_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    CVD مهمل تماماً. الفشل يُحدد بـ flow collapse / fake breakout / OI unsupported / late pop.
    يتم التساهل قليلًا مع بصمة MAGMA-like لأن الصعود قد يبدأ ضد التموضع الظاهر قبل اكتمال الـ hold.
    """
    diagnostics: List[str] = []
    failure_flags: List[str] = []
    continuation_state = "weak"
    early_failure = False
    failure_reason = ""
    counterflow_pattern = features.get("counterflow_pattern", {})

    squeeze_cont = oi_info.get("squeeze_continuation", {}).get("active", False)
    oi_increasing = features.get("oi_delta_3", 0.0) >= EXECUTION_RULES["min_oi_increase_for_support"]
    gap_positive = (
        features.get("position_account_gap", {}).get("position_lead", 0.0)
        >= EXECUTION_RULES["min_position_lead_for_support"]
    )
    magma_like = counterflow_pattern.get("active", False)

    flow_collapsing = (
        features["recent_buy_ratio"] < EXECUTION_RULES["persistent_taker_buy_ratio"]
        and features["flow_ratio_delta"] < -0.02
    )
    breakout_without_hold = features["breakout_touched"] and not features.get("hold_above_breakout_3bars", False)
    oi_unsupported = oi_info["oi_state"] in {"neutral", "price up OI flat"} and not magma_like
    late_pop_without_hold = features["price_late"] and not features.get("hold_above_breakout_3bars", False)
    oi_squeeze_failure = oi_info["oi_state"] == "squeeze_covering" and not squeeze_cont

    if acceptance["accepted"]:
        if not flow_collapsing and not late_pop_without_hold and not oi_squeeze_failure:
            continuation_state = "yes"
            diagnostics.append("الاستمرار جيد لأن OI/gap يدعمان")
        else:
            continuation_state = "no"
            if flow_collapsing:
                failure_reason = "flow collapse بعد الإشعال"
            elif late_pop_without_hold:
                failure_reason = "late pop بلا تثبيت"
            elif oi_squeeze_failure:
                failure_reason = "oi squeeze failure"
            else:
                failure_reason = "ضعف عام بعد القبول"
            diagnostics.append(failure_reason)
    elif acceptance["partial"] and not flow_collapsing and (oi_increasing or gap_positive or magma_like):
        continuation_state = "weak"
        diagnostics.append("الاستمرار ما زال ضعيفًا لكن OI/gap أو بصمة MAGMA-like يدعمان")
    else:
        continuation_state = "no"
        if not acceptance["accepted"]:
            failure_reason = "لا قبول سعري"
        elif flow_collapsing:
            failure_reason = "flow collapse"
        elif oi_unsupported:
            failure_reason = "OI غير داعم"
        else:
            failure_reason = "لا يوجد استمرار موثوق"
        diagnostics.append(failure_reason)

    if breakout_without_hold:
        failure_flags.append("fake breakout / breakout بلا hold")
    if features["one_bar_spike"]:
        failure_flags.append("one-bar spike without acceptance")
    if flow_collapsing:
        failure_flags.append("flow collapse بعد الإشعال")
    if oi_unsupported and features["ret_3"] > 0 and not oi_increasing:
        failure_flags.append("السعر صعد لكن OI غير داعم")
    if late_pop_without_hold:
        failure_flags.append("late pop بلا تثبيت")
    if features["fakeout_like"]:
        failure_flags.append("بصمة fakeout متكررة داخل الدورة الحالية")
    if oi_squeeze_failure:
        failure_flags.append("oi squeeze failure: انخفاض OI بلا عودة خلال 3 فترات")

    if failure_flags and (continuation_state == "no" or features["one_bar_spike"] or late_pop_without_hold):
        early_failure = True
        if not failure_reason:
            failure_reason = " | ".join(failure_flags[:2])

    risk_level = "منخفض"
    if len(failure_flags) >= 3:
        risk_level = "مرتفع"
    elif len(failure_flags) >= 1:
        risk_level = "متوسط"

    return {
        "continuation_state": continuation_state,
        "early_failure": early_failure,
        "failure_flags": failure_flags,
        "risk_level": risk_level,
        "failure_reason": failure_reason,
        "diagnostics": diagnostics,
    }

def detect_relief_bounce_after_flush(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Relief Bounce After Flush
    -------------------------
    يكتشف الارتداد الناتج عن تفريغ/إغلاق مراكز بعد flush، وليس عن بناء صاعد جديد
    مدعوم بتحسن داخلي في Acco/Posit/Ratio.
    """
    diagnostics: List[str] = []
    score = 0
    rules = POST_FLUSH_PATTERN_RULES
    bars = features.get("bars_5m", [])

    if len(bars) < 8:
        return {
            "active": False,
            "score": 0,
            "state": "insufficient_data",
            "diagnostics": ["بيانات 5m غير كافية لرصد Relief Bounce After Flush"],
        }

    pos_features = features.get("pos_features", {})
    acc_features = features.get("acc_features", {})
    global_features = features.get("global_features", {})

    recent_bars = bars[-6:]
    recent_ranges_pct: List[float] = []
    recent_returns_pct: List[float] = []
    for i in range(1, len(recent_bars)):
        prev_close = recent_bars[i - 1]["close"]
        cur = recent_bars[i]
        bar_return = pct_change(cur["close"], prev_close) if prev_close else 0.0
        bar_range = pct_change(cur["high"], cur["low"]) if cur["low"] else 0.0
        recent_returns_pct.append(bar_return)
        recent_ranges_pct.append(bar_range)

    flush_bar_exists = any(r <= rules["flush_drop_pct"] for r in recent_returns_pct)
    violent_range_exists = any(rg >= rules["violent_range_pct"] for rg in recent_ranges_pct)

    if flush_bar_exists:
        score += 2
        diagnostics.append("يوجد flush هابط حديث (شمعة/شموع هبوطية قوية)")
    if violent_range_exists:
        score += 1
        diagnostics.append("المدى السعري في الشموع الأخيرة مرتفع جدًا")

    recent_low = min(b["low"] for b in recent_bars[:-1])
    last_close = recent_bars[-1]["close"]
    rebound_from_low_pct = pct_change(last_close, recent_low) if recent_low else 0.0
    rebound_active = rebound_from_low_pct >= rules["relief_rebound_pct"]

    if rebound_active:
        score += 2
        diagnostics.append(f"السعر ارتد من قاع flush بنسبة {rebound_from_low_pct:.2f}%")

    oi_delta_1 = safe_float(features.get("oi_delta_1", 0.0))
    oi_delta_3 = safe_float(features.get("oi_delta_3", 0.0))
    oi_contracting = oi_delta_1 < 0 or oi_delta_3 < 0
    oi_strong_contraction = (
        oi_delta_1 <= rules["oi_strong_contraction_1"]
        or oi_delta_3 <= rules["oi_strong_contraction_3"]
    )

    if oi_contracting:
        score += 2
        diagnostics.append("OI ينكمش أثناء الارتداد")
    if oi_strong_contraction:
        score += 1
        diagnostics.append("انكماش OI واضح/قوي، ما يرجح أن الارتداد ناتج عن إغلاق مراكز")

    posit_delta_last = safe_float(pos_features.get("delta_last", 0.0))
    posit_delta_base = safe_float(pos_features.get("delta_baseline", 0.0))
    acco_delta_last = safe_float(acc_features.get("delta_last", 0.0))
    acco_delta_base = safe_float(acc_features.get("delta_baseline", 0.0))
    ratio_delta_last = safe_float(global_features.get("delta_last", 0.0))
    ratio_delta_base = safe_float(global_features.get("delta_baseline", 0.0))

    posit_not_rebuilding = posit_delta_last <= 0 or posit_delta_base <= 0
    acco_not_rebuilding = acco_delta_last <= 0 or acco_delta_base <= 0
    ratio_not_rebuilding = ratio_delta_last <= 0 or ratio_delta_base <= 0
    not_rebuilding_count = sum([posit_not_rebuilding, acco_not_rebuilding, ratio_not_rebuilding])

    if posit_not_rebuilding:
        diagnostics.append("Posit لا تعيد البناء أثناء الارتداد")
    if acco_not_rebuilding:
        diagnostics.append("Acco لا تعيد البناء أثناء الارتداد")
    if ratio_not_rebuilding:
        diagnostics.append("L.S Ratio لا تعيد البناء أثناء الارتداد")

    if not_rebuilding_count >= 2:
        score += 2
    elif not_rebuilding_count == 1:
        score += 1

    breakout_level = safe_float(features.get("breakout_level", 0.0))
    hold_above_breakout = bool(features.get("hold_above_breakout_3bars", False))
    close_above_breakout = bool(features.get("close_above_breakout", False))
    not_clean_reclaim = (
        (breakout_level > 0 and last_close < breakout_level * 1.01)
        or (close_above_breakout and not hold_above_breakout)
        or (not close_above_breakout)
    )

    if not_clean_reclaim:
        score += 1
        diagnostics.append("الارتداد لم يتحول بعد إلى reclaim clean مع hold واضح")

    active = (
        flush_bar_exists
        and rebound_active
        and oi_contracting
        and not_rebuilding_count >= 1
    )
    state = "no"
    if active and score >= rules["relief_min_score"]:
        state = "strong_relief_bounce_after_flush"
    elif active:
        state = "relief_bounce_after_flush"

    return {
        "active": active,
        "state": state,
        "score": score,
        "rebound_from_low_pct": rebound_from_low_pct,
        "oi_contracting": oi_contracting,
        "oi_strong_contraction": oi_strong_contraction,
        "not_rebuilding_count": not_rebuilding_count,
        "posit_not_rebuilding": posit_not_rebuilding,
        "acco_not_rebuilding": acco_not_rebuilding,
        "ratio_not_rebuilding": ratio_not_rebuilding,
        "not_clean_reclaim": not_clean_reclaim,
        "diagnostics": diagnostics,
    }


def detect_bullish_rebuild_after_flush(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bullish Rebuild After Flush
    ---------------------------
    يكتشف الحالات التي حصل فيها flush هابط أولاً، ثم تحول الارتداد إلى إعادة بناء
    صاعدة حقيقية، وليس مجرد relief bounce مؤقت.
    """
    diagnostics: List[str] = []
    score = 0
    rules = POST_FLUSH_PATTERN_RULES
    bars = features.get("bars_5m", [])

    if len(bars) < 8:
        return {
            "active": False,
            "score": 0,
            "state": "insufficient_data",
            "diagnostics": ["بيانات 5m غير كافية لرصد Bullish Rebuild After Flush"],
        }

    pos_features = features.get("pos_features", {})
    acc_features = features.get("acc_features", {})
    global_features = features.get("global_features", {})

    recent_bars = bars[-6:]
    recent_returns_pct: List[float] = []
    recent_ranges_pct: List[float] = []
    for i in range(1, len(recent_bars)):
        prev_close = recent_bars[i - 1]["close"]
        cur = recent_bars[i]
        bar_return = pct_change(cur["close"], prev_close) if prev_close else 0.0
        bar_range = pct_change(cur["high"], cur["low"]) if cur["low"] else 0.0
        recent_returns_pct.append(bar_return)
        recent_ranges_pct.append(bar_range)

    flush_bar_exists = any(r <= rules["flush_drop_pct"] for r in recent_returns_pct)
    violent_range_exists = any(rg >= rules["violent_range_pct"] for rg in recent_ranges_pct)
    if flush_bar_exists:
        score += 1
        diagnostics.append("يوجد flush هابط حديث")
    if violent_range_exists:
        score += 1
        diagnostics.append("المدى السعري يشير إلى تفريغ/ضغط عنيف")

    recent_low = min(b["low"] for b in recent_bars[:-1])
    last_close = recent_bars[-1]["close"]
    rebound_from_low_pct = pct_change(last_close, recent_low) if recent_low else 0.0
    rebound_active = rebound_from_low_pct >= rules["relief_rebound_pct"]
    if rebound_active:
        score += 2
        diagnostics.append(f"السعر ارتد من القاع بنسبة {rebound_from_low_pct:.2f}%")

    oi_delta_1 = safe_float(features.get("oi_delta_1", 0.0))
    oi_delta_3 = safe_float(features.get("oi_delta_3", 0.0))
    oi_up_ratio = safe_float(features.get("oi_up_ratio", 0.0))
    oi_rebuilding = oi_delta_1 > 0 or oi_delta_3 > 0
    oi_strong_rebuild = (
        oi_delta_1 >= rules["oi_rebuild_1"]
        or oi_delta_3 >= rules["oi_rebuild_3"]
        or oi_up_ratio >= rules["oi_up_ratio_min"]
    )

    if oi_rebuilding:
        score += 2
        diagnostics.append("OI عاد يدعم الحركة أثناء الارتداد")
    if oi_strong_rebuild:
        score += 1
        diagnostics.append("إعادة بناء OI قوية نسبيًا")

    posit_delta_last = safe_float(pos_features.get("delta_last", 0.0))
    posit_delta_base = safe_float(pos_features.get("delta_baseline", 0.0))
    acco_delta_last = safe_float(acc_features.get("delta_last", 0.0))
    acco_delta_base = safe_float(acc_features.get("delta_baseline", 0.0))
    ratio_delta_last = safe_float(global_features.get("delta_last", 0.0))
    ratio_delta_base = safe_float(global_features.get("delta_baseline", 0.0))

    posit_rebuilding = posit_delta_last > 0 or posit_delta_base > 0
    acco_rebuilding = acco_delta_last > 0 or acco_delta_base > 0
    ratio_rebuilding = ratio_delta_last > 0 or ratio_delta_base > 0
    rebuilding_count = sum([posit_rebuilding, acco_rebuilding, ratio_rebuilding])

    if posit_rebuilding:
        diagnostics.append("Posit تعيد البناء")
    if acco_rebuilding:
        diagnostics.append("Acco تعيد البناء")
    if ratio_rebuilding:
        diagnostics.append("L.S Ratio تعيد البناء")

    if rebuilding_count >= 2:
        score += 2
    elif rebuilding_count == 1:
        score += 1

    breakout_level = safe_float(features.get("breakout_level", 0.0))
    close_above_breakout = bool(features.get("close_above_breakout", False))
    hold_above_breakout = bool(features.get("hold_above_breakout_3bars", False))
    follow_through_bias = bool(features.get("follow_through_bias", False))
    price_late = bool(features.get("price_late", False))
    price_reaccepted = (
        close_above_breakout
        or hold_above_breakout
        or (breakout_level > 0 and last_close >= breakout_level * 0.995)
    )

    if price_reaccepted:
        score += 2
        diagnostics.append("السعر استعاد مستوى مهمًا / أعاد القبول")
    if hold_above_breakout:
        score += 1
        diagnostics.append("يوجد hold فوق المستوى المرجعي")
    if follow_through_bias:
        score += 1
        diagnostics.append("يوجد follow-through بعد الارتداد")
    if price_late:
        score -= 1
        diagnostics.append("السعر قد يكون متأخرًا نسبيًا رغم إعادة البناء")

    active = (
        flush_bar_exists
        and rebound_active
        and oi_rebuilding
        and rebuilding_count >= 2
        and price_reaccepted
    )
    state = "no"
    if active and score >= rules["bullish_min_score"]:
        state = "strong_bullish_rebuild_after_flush"
    elif active:
        state = "bullish_rebuild_after_flush"

    return {
        "active": active,
        "state": state,
        "score": score,
        "rebound_from_low_pct": rebound_from_low_pct,
        "oi_rebuilding": oi_rebuilding,
        "oi_strong_rebuild": oi_strong_rebuild,
        "rebuilding_count": rebuilding_count,
        "posit_rebuilding": posit_rebuilding,
        "acco_rebuilding": acco_rebuilding,
        "ratio_rebuilding": ratio_rebuilding,
        "price_reaccepted": price_reaccepted,
        "hold_above_breakout": hold_above_breakout,
        "diagnostics": diagnostics,
    }


def detect_failed_rebuild_after_flush(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Failed Rebuild After Flush
    -------------------------
    يكتشف الحالات التي حصل فيها flush أولاً، ثم محاولة إعادة بناء تبدو إيجابية
    مؤقتًا، لكنها فشلت وتحولت إلى bull trap / failed reclaim.
    """
    diagnostics: List[str] = []
    score = 0
    rules = POST_FLUSH_PATTERN_RULES
    bars = features.get("bars_5m", [])

    if len(bars) < 10:
        return {
            "active": False,
            "score": 0,
            "state": "insufficient_data",
            "diagnostics": ["بيانات 5m غير كافية لرصد Failed Rebuild After Flush"],
        }

    pos_features = features.get("pos_features", {})
    acc_features = features.get("acc_features", {})
    global_features = features.get("global_features", {})

    recent_bars = bars[-8:]
    closes = [b["close"] for b in recent_bars]
    highs = [b["high"] for b in recent_bars]
    lows = [b["low"] for b in recent_bars]

    recent_returns_pct: List[float] = []
    recent_ranges_pct: List[float] = []
    for i in range(1, len(recent_bars)):
        prev_close = recent_bars[i - 1]["close"]
        cur = recent_bars[i]
        bar_return = pct_change(cur["close"], prev_close) if prev_close else 0.0
        bar_range = pct_change(cur["high"], cur["low"]) if cur["low"] else 0.0
        recent_returns_pct.append(bar_return)
        recent_ranges_pct.append(bar_range)

    flush_bar_exists = any(r <= rules["flush_drop_pct"] for r in recent_returns_pct)
    violent_range_exists = any(rg >= rules["violent_range_pct"] for rg in recent_ranges_pct)
    if flush_bar_exists:
        score += 1
        diagnostics.append("يوجد flush هابط سابق")
    if violent_range_exists:
        score += 1
        diagnostics.append("المدى السعري الكبير يؤكد وجود shakeout / flush")

    recent_low = min(lows[:-2]) if len(lows) >= 3 else min(lows)
    local_rebound_pct = pct_change(max(highs[-4:]), recent_low) if recent_low else 0.0
    rebuild_attempt = local_rebound_pct >= rules["rebuild_attempt_pct"]
    if rebuild_attempt:
        score += 2
        diagnostics.append(f"ظهرت محاولة rebuild بعد flush بنسبة {local_rebound_pct:.2f}%")

    oi_delta_1 = safe_float(features.get("oi_delta_1", 0.0))
    oi_delta_3 = safe_float(features.get("oi_delta_3", 0.0))
    posit_delta_last = safe_float(pos_features.get("delta_last", 0.0))
    acco_delta_last = safe_float(acc_features.get("delta_last", 0.0))
    ratio_delta_last = safe_float(global_features.get("delta_last", 0.0))

    partial_internal_rebuild = any([
        oi_delta_1 > 0,
        oi_delta_3 > 0,
        posit_delta_last > 0,
        acco_delta_last > 0,
        ratio_delta_last > 0,
    ])
    if partial_internal_rebuild:
        score += 1
        diagnostics.append("ظهر تحسن داخلي جزئي يوحي بإعادة بناء مؤقتة")

    breakout_level = safe_float(features.get("breakout_level", 0.0))
    close_above_breakout = bool(features.get("close_above_breakout", False))
    hold_above_breakout = bool(features.get("hold_above_breakout_3bars", False))
    last_close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else last_close
    recent_high = max(highs[-4:]) if highs else last_close
    rejection_from_high_pct = pct_change(recent_high, last_close) if recent_high else 0.0
    close_failed = last_close < prev_close
    failed_hold = close_above_breakout and not hold_above_breakout
    below_reclaim = breakout_level > 0 and last_close < breakout_level
    failed_acceptance = failed_hold or below_reclaim or rejection_from_high_pct >= rules["failed_rejection_from_high_pct"] or close_failed

    if failed_acceptance:
        score += 2
        diagnostics.append("محاولة إعادة البناء فشلت سعريًا (rejection / failed hold / below reclaim)")

    posit_weak_again = posit_delta_last <= 0
    acco_weak_again = acco_delta_last <= 0
    ratio_weak_again = ratio_delta_last <= 0
    oi_weak_again = oi_delta_1 <= 0
    weakness_count = sum([posit_weak_again, acco_weak_again, ratio_weak_again, oi_weak_again])

    if weakness_count >= 2:
        score += 2
        diagnostics.append("الضعف الداخلي عاد بعد محاولة rebuild")
    elif weakness_count == 1:
        score += 1
        diagnostics.append("يوجد ضعف داخلي واحد على الأقل بعد المحاولة")

    last_two_red = len(closes) >= 3 and closes[-1] < closes[-2] < closes[-3]
    if last_two_red:
        score += 1
        diagnostics.append("السعر يعود للانزلاق بعد محاولة rebuild")

    active = (
        flush_bar_exists
        and rebuild_attempt
        and partial_internal_rebuild
        and failed_acceptance
        and weakness_count >= 2
    )
    state = "no"
    if active and score >= rules["failed_min_score"]:
        state = "strong_failed_rebuild_after_flush"
    elif active:
        state = "failed_rebuild_after_flush"

    return {
        "active": active,
        "state": state,
        "score": score,
        "rebuild_attempt": rebuild_attempt,
        "partial_internal_rebuild": partial_internal_rebuild,
        "failed_acceptance": failed_acceptance,
        "weakness_count": weakness_count,
        "rejection_from_high_pct": rejection_from_high_pct,
        "diagnostics": diagnostics,
    }


def derive_regime_trigger_execution_patterns(
    features: Dict[str, Any],
    family_info: Dict[str, Any],
    stage_info: Dict[str, Any],
    acceptance: Dict[str, Any],
    failure: Dict[str, Any],
    decision_ctx: Dict[str, Any],
) -> Dict[str, str]:
    crowding_regime = features.get("crowding_regime", {}) or {}
    extreme_engine = features.get("extreme_engine", {}) or {}
    extreme_patterns = extreme_engine.get("patterns", {}) or {}
    counterflow_pattern = features.get("counterflow_pattern", {}) or {}
    post_flush_name = decision_ctx.get("post_flush_pattern_name")
    final_bucket = decision_ctx.get("final_bucket", "Discovered but not actionable")
    stage = decision_ctx.get("stage", stage_info.get("stage", "WATCH"))
    actionable_now = decision_ctx.get("actionable_now", False)

    regime_pattern = "NEUTRAL_REGIME"
    if (extreme_patterns.get("lower_extreme_squeeze") or {}).get("active", False):
        regime_pattern = "LOWER_EXTREME_SHORT_SQUEEZE_REGIME"
    elif (extreme_patterns.get("upper_extreme_bullish_expansion") or {}).get("active", False):
        regime_pattern = "UPPER_EXTREME_BULLISH_EXPANSION_REGIME"
    elif (extreme_patterns.get("upper_extreme_failure") or {}).get("active", False):
        regime_pattern = "UPPER_EXTREME_FAILURE_REGIME"
    elif (extreme_patterns.get("long_crowding_rollover") or {}).get("active", False):
        regime_pattern = "LONG_CROWDING_ROLLOVER_REGIME"
    elif counterflow_pattern.get("active", False):
        regime_pattern = "COUNTERFLOW_REPRICING_REGIME"
    else:
        state = crowding_regime.get("state", "neutral_regime")
        mapping = {
            "crowded_short_regime": "CROWDED_SHORT_REGIME",
            "crowded_long_regime": "CROWDED_LONG_REGIME",
            "counterflow_regime": "COUNTERFLOW_REGIME",
            "mixed_regime": "MIXED_REGIME",
            "micro_crowded_short_regime": "MICRO_CROWDED_SHORT_REGIME",
            "micro_crowded_long_regime": "MICRO_CROWDED_LONG_REGIME",
        }
        regime_pattern = mapping.get(state, "NEUTRAL_REGIME")

    trigger_pattern = "NO_TRIGGER"
    if post_flush_name == "BULLISH_REBUILD_AFTER_FLUSH":
        trigger_pattern = "POST_FLUSH_BULLISH_REBUILD_TRIGGER"
    elif post_flush_name == "FAILED_REBUILD_AFTER_FLUSH":
        trigger_pattern = "POST_FLUSH_FAILED_REBUILD_TRIGGER"
    elif post_flush_name == "RELIEF_BOUNCE_AFTER_FLUSH":
        trigger_pattern = "POST_FLUSH_RELIEF_BOUNCE_TRIGGER"
    elif counterflow_pattern.get("active", False):
        trigger_pattern = "COUNTERFLOW_SQUEEZE_TRIGGER"
    elif features.get("fakeout_like", False):
        trigger_pattern = "FAILED_BREAKOUT_TRIGGER"
    elif features.get("breakout_touched", False) and features.get("hold_above_breakout_3bars", False):
        trigger_pattern = "BREAKOUT_ACCEPT_TRIGGER"
    elif features.get("breakout_touched", False):
        trigger_pattern = "BREAKOUT_IGNITION_TRIGGER"
    elif stage in {"PREPARE", "ARMED"}:
        trigger_pattern = "PREPARE_TO_ARMED_TRIGGER"

    execution_pattern = "OBSERVE_ONLY"
    if actionable_now:
        execution_pattern = "ACTIONABLE_EXECUTION"
    elif final_bucket == "Late":
        execution_pattern = "LATE_EXECUTION_BLOCK"
    elif final_bucket == "Failed":
        execution_pattern = "FAILED_EXECUTION_BLOCK"
    elif acceptance.get("state") == "no":
        execution_pattern = "NO_ACCEPTANCE_BLOCK"
    elif failure.get("continuation_state") in {"weak", "no"}:
        execution_pattern = "WEAK_CONTINUATION_BLOCK"
    elif stage == "ARMED":
        execution_pattern = "ARMED_WAIT_FOR_CONFIRMATION"
    elif stage == "PREPARE":
        execution_pattern = "PREPARE_WAIT_FOR_TRIGGER"

    return {
        "regime_pattern": regime_pattern,
        "trigger_pattern": trigger_pattern,
        "execution_pattern": execution_pattern,
    }


def build_final_decision(
    symbol: str,
    features: Dict[str, Any],
    family_info: Dict[str, Any],
    stage_info: Dict[str, Any],
    oi_info: Dict[str, Any],
    acceptance: Dict[str, Any],
    failure: Dict[str, Any],
) -> Dict[str, Any]:
    diagnostics: List[str] = []
    family = family_info["family"]
    oi_state = oi_info["oi_state"]
    gap = features.get("position_account_gap", {})
    counterflow_pattern = features.get("counterflow_pattern", {})
    ratio_alignment = features.get("ratio_alignment", {})
    ratio_conflict = features.get("ratio_conflict", {})
    extreme_engine = features.get("extreme_engine", {})
    extreme_patterns = extreme_engine.get("patterns", {})

    preignite_states = {"premove buildup", "buildup expansion", "squeeze_buildup"}
    discovery_state = "غير مكتشفة بنيويًا"
    discovery_reason = ""
    if family is not None and oi_state in preignite_states and not features["price_late"]:
        discovery_state = "مكتشفة مبكرًا"
        discovery_reason = "family + OI build-up + السعر ليس late"
        diagnostics.append("مرحلة التهيؤ متحققة")
    elif family is not None:
        discovery_state = "مكتشفة"
        discovery_reason = f"family = {family}"
        diagnostics.append(discovery_reason)

    oi_supportive = oi_state in {"premove buildup", "buildup expansion", "squeeze_buildup", "squeeze_post_covering"}
    if oi_state == "squeeze_covering":
        oi_supportive = features.get("short_crowding", False) and not failure["early_failure"]
        if oi_supportive:
            diagnostics.append("تم اعتبار squeeze_covering داعمًا")

    if features.get("oi_delta_3", 0.0) >= EXECUTION_RULES["min_oi_increase_for_support"]:
        oi_supportive = True
        diagnostics.append("oi_delta_3 موجب (زيادة) تم اعتباره داعمًا")
    if counterflow_pattern.get("active", False) and counterflow_pattern.get("explosive_oi", False):
        oi_supportive = True
        diagnostics.append("تم اعتماد OI كداعم بسبب بصمة counterflow squeeze المتفجرة")
    if oi_state == "price up OI flat" and not counterflow_pattern.get("active", False):
        oi_supportive = False
        diagnostics.append("PRICE_UP_OI_FLAT بقيت مريبة ولم تُعتمد كدعم OI")

    ignition_hit = features["breakout_touched"] or features["ret_3"] >= EXECUTION_RULES["ignition_return_5m_pct"]
    ratio_supportive = ratio_alignment.get("overall_supportive", False) or ratio_alignment.get("higher_tf_supportive", False)
    ratio_conflicted = ratio_alignment.get("overall_conflict", False) or ratio_conflict.get("state") == "conflicted"
    ignition_flow_ok = features["trade_expansion_last"] >= EXECUTION_RULES["min_trade_expansion"] and (ratio_supportive or not ratio_conflicted or counterflow_pattern.get("active", False))

    stage = stage_info.get("stage", "WATCH")
    stage_reason = stage_info.get("stage_reason", "")
    if features["price_late"] and (ignition_hit or acceptance["state"] != "no"):
        stage = "LATE"
        stage_reason = "الاشتعال/القبول جاء بعد امتداد سعري"
        diagnostics.append(stage_reason)
    elif ignition_hit and ignition_flow_ok and oi_supportive:
        stage = "TRIGGERED" if acceptance["state"] == "yes" else "ARMED"
        stage_reason = "breakout/ret3 + trade expansion + OI supportive"
        if counterflow_pattern.get("active", False):
            stage_reason += " + counterflow squeeze context"
        if acceptance["state"] == "yes":
            stage_reason += " + قبول مؤكد"
        diagnostics.append("مرحلة الاشتعال متحققة: " + stage_reason)
    elif family is not None and oi_state in preignite_states and not features["price_late"]:
        stage = "PREPARE"
        stage_reason = "4h/1h يظهران تراكمًا أو build-up دون قبول كامل"
        diagnostics.append(stage_reason)
    elif family is not None:
        stage = "WATCH"
        stage_reason = "family موجودة لكن لم تصل إلى مرحلة الاشتعال"
        diagnostics.append(stage_reason)

    acceptance_state = acceptance["state"]
    acceptance_reason = acceptance.get("acceptance_reason", "")
    continuation_state = failure["continuation_state"]
    continuation_reason = failure.get("failure_reason", "") if continuation_state != "yes" else "استمرار جيد (OI/gap يدعم)"

    promotion_reason = ""
    if (
        stage == "ARMED"
        and counterflow_pattern.get("armed_ready", False)
        and features.get("close_above_breakout", False)
        and ignition_flow_ok
        and oi_supportive
        and not features.get("price_late", False)
        and not failure.get("early_failure", False)
        and acceptance.get("state") in {"yes", "partial"}
    ):
        stage = "TRIGGERED"
        stage_reason = "تمت ترقية الحالة من ARMED إلى TRIGGERED بسبب بصمة MAGMA-like counterflow squeeze"
        acceptance_state = "yes"
        acceptance_reason = acceptance_reason or "ترقية قبول استثنائية بسبب counterflow squeeze قوي"
        continuation_state = "yes" if continuation_state != "no" else "weak"
        continuation_reason = "استمرار مرن مدعوم ببصمة counterflow"
        promotion_reason = "armed_exception_promoted_by_counterflow_squeeze"
        diagnostics.append(stage_reason)

    execution_state = "غير قابلة للتنفيذ"
    final_bucket = "Discovered but not actionable"
    not_actionable_reason = ""

    actionable_now = all([
        stage == "TRIGGERED",
        acceptance_state == "yes",
        continuation_state in ({"yes", "weak"} if counterflow_pattern.get("active", False) else {"yes"}),
        oi_supportive,
        not features["price_late"],
        not failure["early_failure"],
        (not ratio_conflicted or counterflow_pattern.get("active", False)),
    ])

    if actionable_now:
        execution_state = "قابلة للتنفيذ الآن"
        final_bucket = "Actionable now"
        diagnostics.append("تحققت شجرة القرار كاملة حتى مرحلة الاستمرار")
        if not promotion_reason:
            promotion_reason = f"stage={stage}, acceptance={acceptance_state}, continuation={continuation_state}, oi_supportive={oi_supportive}"
    elif stage == "LATE" or features["price_late"]:
        execution_state = "متأخرة"
        final_bucket = "Late"
        not_actionable_reason = "الحركة متأخرة (price_late)"
        diagnostics.append("تم تصنيفها Late لأن الجزء الأنظف غالبًا مضى")
    elif family is None or failure["early_failure"]:
        execution_state = "فاشلة"
        final_bucket = "Failed"
        not_actionable_reason = failure.get("failure_reason") or "لا بنية صعودية أو فشل مبكر"
        diagnostics.append("تم رفضها لأن البنية غير مفهومة أو لأن الفشل المبكر ظهر بوضوح")
    else:
        reasons = []
        if stage != "TRIGGERED":
            reasons.append(f"المرحلة {stage}")
        if acceptance_state != "yes":
            reasons.append(f"القبول {acceptance_state}")
        if continuation_state != "yes":
            reasons.append(f"الاستمرار {continuation_state}")
        if not oi_supportive:
            reasons.append("OI غير داعم")
        if ratio_conflicted and not counterflow_pattern.get("active", False):
            reasons.append("ratios متعدد الفريمات متعارضة")
        if features["price_late"]:
            reasons.append("السعر متأخر")
        if failure["early_failure"]:
            reasons.append("فشل مبكر")
        not_actionable_reason = " | ".join(reasons) if reasons else "لا يوجد"
        diagnostics.append("البصمة موجودة لكن لحظة التنفيذ الحالية ليست نظيفة بما يكفي")

    precomputed_post_flush = features.get("precomputed_post_flush_patterns", {}) or {}
    relief_bounce = precomputed_post_flush.get("relief_bounce") or detect_relief_bounce_after_flush(features)
    bullish_rebuild = precomputed_post_flush.get("bullish_rebuild") or detect_bullish_rebuild_after_flush(features)
    failed_rebuild = precomputed_post_flush.get("failed_rebuild") or detect_failed_rebuild_after_flush(features)

    post_flush_pattern_name = None
    post_flush_pattern = None
    if bullish_rebuild.get("active"):
        post_flush_pattern_name = "BULLISH_REBUILD_AFTER_FLUSH"
        post_flush_pattern = bullish_rebuild
        diagnostics.append("تم اكتشاف Bullish Rebuild After Flush")
        diagnostics.extend(bullish_rebuild.get("diagnostics", [])[:2])
        if bullish_rebuild.get("score", 0) >= POST_FLUSH_PATTERN_RULES["bullish_min_score"]:
            final_bucket = "Actionable now"
            execution_state = "قابلة للتنفيذ الآن"
            actionable_now = True
            acceptance_state = "yes"
            continuation_state = "yes"
            continuation_reason = "استمرار جيد بعد rebuild post-flush"
            not_actionable_reason = ""
            promotion_reason = promotion_reason or "bullish_rebuild_after_flush_promoted"
        else:
            final_bucket = "Discovered but not actionable"
            execution_state = "غير قابلة للتنفيذ"
            actionable_now = False
            not_actionable_reason = "إعادة البناء بعد flush موجودة لكنها لم تكتمل بما يكفي"
    elif failed_rebuild.get("active"):
        post_flush_pattern_name = "FAILED_REBUILD_AFTER_FLUSH"
        post_flush_pattern = failed_rebuild
        diagnostics.append("تم اكتشاف Failed Rebuild After Flush")
        diagnostics.extend(failed_rebuild.get("diagnostics", [])[:2])
        final_bucket = "Failed"
        execution_state = "فاشلة"
        actionable_now = False
        continuation_state = "no"
        continuation_reason = "فشل rebuild بعد flush"
        not_actionable_reason = "محاولة إعادة البناء بعد flush فشلت سعريًا وبنيويًا"
    elif relief_bounce.get("active"):
        post_flush_pattern_name = "RELIEF_BOUNCE_AFTER_FLUSH"
        post_flush_pattern = relief_bounce
        diagnostics.append("تم اكتشاف Relief Bounce After Flush")
        diagnostics.extend(relief_bounce.get("diagnostics", [])[:2])
        if final_bucket == "Actionable now":
            final_bucket = "Discovered but not actionable"
            execution_state = "غير قابلة للتنفيذ"
            actionable_now = False
        if not not_actionable_reason or final_bucket == "Discovered but not actionable":
            not_actionable_reason = "الارتداد الحالي relief bounce بعد flush وليس bullish rebuild clean"

    lower_extreme_squeeze = extreme_patterns.get("lower_extreme_squeeze", {})
    upper_extreme_bullish = extreme_patterns.get("upper_extreme_bullish_expansion", {})
    upper_extreme_failure = extreme_patterns.get("upper_extreme_failure", {})
    long_crowding_rollover = extreme_patterns.get("long_crowding_rollover", {})
    oi_counter_positioning = extreme_patterns.get("oi_expansion_without_bullish_confirmation", {})
    squeeze_probability = safe_float(extreme_engine.get("squeeze_probability"), 0.0)
    flush_probability = safe_float(extreme_engine.get("flush_probability"), 0.0)

    if lower_extreme_squeeze.get("active", False):
        diagnostics.append(f"lower-extreme squeeze probability={squeeze_probability:.1f}")
    if upper_extreme_bullish.get("active", False):
        diagnostics.append(f"upper-extreme bullish expansion probability={squeeze_probability:.1f}")

    if bullish_rebuild.get("active") is False and not failure.get("early_failure", False) and not features.get("price_late", False):
        if lower_extreme_squeeze.get("active", False) and not relief_bounce.get("active", False):
            if final_bucket == "Discovered but not actionable" and stage in {"ARMED", "TRIGGERED"} and acceptance_state in {"partial", "yes"} and oi_supportive and squeeze_probability >= 55 and not ratio_conflicted:
                final_bucket = "Actionable now"
                execution_state = "قابلة للتنفيذ الآن"
                actionable_now = True
                continuation_state = "yes" if continuation_state == "yes" else "weak"
                continuation_reason = "استمرار مرن بسبب lower-extreme squeeze"
                not_actionable_reason = ""
                promotion_reason = promotion_reason or "lower_extreme_squeeze_promoted"
                diagnostics.append("تمت ترقية الحالة بسبب lower-extreme squeeze regime")
        elif upper_extreme_bullish.get("active", False):
            if final_bucket == "Discovered but not actionable" and stage == "TRIGGERED" and acceptance_state == "yes" and oi_supportive and squeeze_probability >= 52 and not ratio_conflicted:
                final_bucket = "Actionable now"
                execution_state = "قابلة للتنفيذ الآن"
                actionable_now = True
                continuation_state = "yes" if continuation_state != "no" else "weak"
                continuation_reason = "استمرار جيد بسبب upper-extreme bullish expansion"
                not_actionable_reason = ""
                promotion_reason = promotion_reason or "upper_extreme_bullish_expansion_promoted"
                diagnostics.append("تمت ترقية الحالة بسبب upper-extreme bullish expansion")

    if upper_extreme_failure.get("active", False) or long_crowding_rollover.get("active", False) or oi_counter_positioning.get("active", False):
        bearish_reasons = []
        if upper_extreme_failure.get("active", False):
            bearish_reasons.append("Upper-Extreme failure")
        if long_crowding_rollover.get("active", False):
            bearish_reasons.append("Long Crowding Rollover")
        if oi_counter_positioning.get("active", False):
            bearish_reasons.append("OI expansion without bullish confirmation")
        diagnostics.append(" | ".join(bearish_reasons) + f" | flush_probability={flush_probability:.1f}")
        if flush_probability >= 68 or failed_rebuild.get("active", False):
            final_bucket = "Failed"
            execution_state = "فاشلة"
            actionable_now = False
            continuation_state = "no"
            continuation_reason = "ضعف/flush بعد upper extreme أو rebuild فاشل"
            not_actionable_reason = "خطر flush/rollover مرتفع بعد upper extreme أو ضعف القبول"
        else:
            if final_bucket == "Actionable now":
                final_bucket = "Discovered but not actionable"
                execution_state = "غير قابلة للتنفيذ"
                actionable_now = False
            if not not_actionable_reason:
                not_actionable_reason = "السياق يميل إلى flush risk / long crowding rollover رغم الارتداد الحالي"

    if ratio_supportive:
        diagnostics.append("ratios متعدد الفريمات داعمة للقرار")
    if ratio_conflicted and not counterflow_pattern.get("active", False):
        diagnostics.append("ratios متعدد الفريمات تضغط ضد القرار")

    triplet = derive_regime_trigger_execution_patterns(
        features=features,
        family_info=family_info,
        stage_info=stage_info,
        acceptance=acceptance,
        failure=failure,
        decision_ctx={
            "stage": stage,
            "final_bucket": final_bucket,
            "actionable_now": actionable_now,
            "post_flush_pattern_name": post_flush_pattern_name,
        },
    )

    decision_reason = "; ".join(diagnostics[-5:]) if diagnostics else "لا يوجد"
    return {
        "symbol": symbol,
        "family": family,
        "stage": stage,
        "oi_state": oi_state,
        "discovery_state": discovery_state,
        "execution_state": execution_state,
        "final_bucket": final_bucket,
        "actionable_now": actionable_now,
        "acceptance_state": acceptance_state,
        "continuation_state": continuation_state,
        "failure": failure,
        "decision_reason": decision_reason,
        "oi_supportive": oi_supportive,
        "position_account_gap": gap,
        "oi_dynamics": oi_info,
        "ratio_alignment": ratio_alignment,
        "ratio_conflict": ratio_conflict,
        "extreme_engine": extreme_engine,
        "discovery_reason": discovery_reason,
        "stage_reason": stage_reason,
        "acceptance_reason": acceptance_reason,
        "continuation_reason": continuation_reason,
        "promotion_reason": promotion_reason if actionable_now else "",
        "not_actionable_reason": not_actionable_reason,
        "counterflow_pattern": counterflow_pattern,
        "relief_bounce_after_flush": relief_bounce,
        "bullish_rebuild_after_flush": bullish_rebuild,
        "failed_rebuild_after_flush": failed_rebuild,
        "post_flush_pattern_name": post_flush_pattern_name,
        "post_flush_pattern": post_flush_pattern or {},
        "regime_pattern": triplet["regime_pattern"],
        "trigger_pattern": triplet["trigger_pattern"],
        "execution_pattern": triplet["execution_pattern"],
        "diagnostics": diagnostics,
    }


def analyze_symbol_legacy_material_provider(
    client: BinanceFuturesPublicClient,
    symbol: str,
    symbol_meta: Dict[str, Any],
    ticker_24h: Dict[str, Any],
    mark_info: Dict[str, Any],
) -> Dict[str, Any]:
    """مزود المواد الخام القديم: يبني الخصائص والقراءات الأولية فقط.
    يبقى موجودًا للتوافق الرجعي واستخراج المواد الخام البنيوية، لكنه لم يعد مركز القرار النهائي.
    """
    features = build_reference_market_features(client, symbol, symbol_meta, ticker_24h, mark_info)
    if not features.get("ok"):
        return {
            "ok": False,
            "symbol": symbol,
            "error": features.get("error", "unknown_error"),
            "diagnostics": features.get("diagnostics", []),
            "features": features,
        }

    family_info = resolve_primary_family_signal(features)
    stage_info = classify_signal_stage_from_reference(features, family_info)
    oi_info = classify_oi_state(features)
    features["oi_dynamics"] = oi_info
    features["squeeze_flags"] = {
        "setup": oi_info.get("squeeze_setup", {}).get("active", False),
        "ignition": oi_info.get("squeeze_ignition", {}).get("active", False),
        "continuation": oi_info.get("squeeze_continuation", {}).get("active", False),
    }
    features["precomputed_post_flush_patterns"] = {
        "relief_bounce": detect_relief_bounce_after_flush(features),
        "bullish_rebuild": detect_bullish_rebuild_after_flush(features),
        "failed_rebuild": detect_failed_rebuild_after_flush(features),
    }

    acceptance = detect_acceptance_state(features)
    failure = detect_failure_continuation(features, acceptance, oi_info)
    return {
        "ok": True,
        "symbol": symbol,
        "features": features,
        "family_info": family_info,
        "stage_info": stage_info,
        "oi_info": oi_info,
        "acceptance": acceptance,
        "failure": failure,
    }


def build_result_from_raw_materials(raw_materials: Dict[str, Any]) -> Dict[str, Any]:
    """يبني نتيجة تشغيلية موحدة من المواد الخام، مع جعل القرار القديم مجرد لقطة مرجعية."""
    symbol = raw_materials.get("symbol", "UNKNOWN")
    features = safe_dict_from_api(raw_materials.get("features"))
    family_info = safe_dict_from_api(raw_materials.get("family_info"))
    stage_info = safe_dict_from_api(raw_materials.get("stage_info"))
    oi_info = safe_dict_from_api(raw_materials.get("oi_info"))
    acceptance = safe_dict_from_api(raw_materials.get("acceptance"))
    failure = safe_dict_from_api(raw_materials.get("failure"))
    result = {
        "symbol": symbol,
        "analysis_source": "structural_orchestrator",
        "analysis_center": "structural_casefile",
        # مواد خام بنيوية فقط (بدون حاكمية Legacy)
        "family": family_info.get("family"),
        "stage": stage_info.get("stage", "WATCH"),
        "oi_state": oi_info.get("oi_state", "neutral"),
        "discovery_state": "مكتشفة بنيويًا" if family_info.get("family") else "غير مكتشفة بنيويًا",
        "execution_state": "قيد الحسم البنيوي",
        "final_bucket": "Discovered but not actionable",
        "actionable_now": False,
        "acceptance_state": acceptance.get("state", "no"),
        "continuation_state": failure.get("continuation_state", "no"),
        "decisive_factor": family_info.get("decisive_factor", "لا يوجد عامل حاسم بعد"),
        "funding_context": features.get("funding_context", "unknown"),
        "basis_context": features.get("basis_context", "missing"),
        "failure_risk": failure.get("risk_level", "منخفض"),
        "failure_reasons": failure.get("failure_flags", []),
        "failure": failure,
        "market_features": features,
        "position_account_gap": features.get("position_account_gap", {}),
        "gap_evolution": features.get("gap_evolution", {}),
        "crowding_regime": features.get("crowding_regime", {}),
        "cvd_metrics": features.get("cvd_metrics", {}),
        "zscores": features.get("zscores", {}),
        "oi_dynamics": oi_info,
        "counterflow_pattern": features.get("counterflow_pattern", {}),
        "relief_bounce_after_flush": {},
        "bullish_rebuild_after_flush": {},
        "failed_rebuild_after_flush": {},
        "post_flush_pattern_name": None,
        "post_flush_pattern": {},
        "family_scores": family_info.get("family_scores", {}),
        "family_reasons": family_info.get("family_reasons", {}),
        "family_diagnostics": family_info.get("diagnostics", []),
        "stage_diagnostics": stage_info.get("diagnostics", []),
        "oi_diagnostics": oi_info.get("diagnostics", []),
        "acceptance_diagnostics": acceptance.get("diagnostics", []),
        "failure_diagnostics": failure.get("diagnostics", []),
        "decision_diagnostics": [],
        "decision_reason": "قرار أولي قيد الحسم عبر Structural Thesis وExecution Verdict",
        "discovery_reason": family_info.get("decisive_factor", ""),
        "stage_reason": stage_info.get("stage_reason", ""),
        "acceptance_reason": acceptance.get("acceptance_reason", ""),
        "continuation_reason": failure.get("failure_reason", ""),
        "promotion_reason": "",
        "not_actionable_reason": "",
        "regime_pattern": "NEUTRAL_REGIME",
        "trigger_pattern": "NO_TRIGGER",
        "execution_pattern": "OBSERVE_ONLY",
        "ratio_alignment": features.get("ratio_alignment", {}),
        "ratio_conflict": features.get("ratio_conflict", {}),
        "extreme_engine": features.get("extreme_engine", {}),
        "asset_memory_profile": features.get("asset_memory_profile", {}),
        "asset_memory_context": features.get("asset_memory_context", {}),
        "leader_type": None,
        "leader_reason": "",
        "primary_hypothesis": None,
        "alternative_hypotheses": [],
        "hypothesis_scores": {},
        "confidence_score": 0.0,
        "uncertainty_score": 0.0,
        "conflict_score": 0.0,
        "invalidation_reason": "",
        "next_failure_mode": "",
        "causal_chain": [],
        "closest_case_match": None,
        "closest_case_score": 0.0,
        "state_transition": None,
        "state_transition_reason": "",
        "legacy_materials": {
            "family_info": family_info,
            "stage_info": stage_info,
            "oi_info": oi_info,
            "acceptance": acceptance,
            "failure": failure,
        },
        "diagnostics": (
            features.get("diagnostics", [])
            + family_info.get("diagnostics", [])
            + stage_info.get("diagnostics", [])
            + oi_info.get("diagnostics", [])
            + acceptance.get("diagnostics", [])
            + failure.get("diagnostics", [])
            + (features.get("extreme_engine", {}) or {}).get("diagnostics", [])
            + safe_dict_from_api(features.get("ratio_conflict")).get("diagnostics", [])
        ),
    }
    return result


def analyze_symbol_structural(
    client: BinanceFuturesPublicClient,
    symbol: str,
    symbol_meta: Dict[str, Any],
    ticker_24h: Dict[str, Any],
    mark_info: Dict[str, Any],
) -> Dict[str, Any]:
    """الأوركستريتور البنيوي الرسمي.
    الدوال القديمة تبني مواد خام فقط، بينما الحكم النهائي يمر عبر:
    قضية بنيوية -> فرضية -> مراجعة مضادة -> قرار تنفيذ.
    """
    try:
        raw_materials = analyze_symbol_legacy_material_provider(client, symbol, symbol_meta, ticker_24h, mark_info)
        if not raw_materials.get("ok"):
            features = safe_dict_from_api(raw_materials.get("features"))
            return {
                "symbol": symbol,
                "analysis_source": "structural_orchestrator",
                "analysis_center": "structural_casefile",
                "error": raw_materials.get("error", "unknown_error"),
                "family": None,
                "stage": "WATCH",
                "oi_state": "neutral",
                "discovery_state": "غير مكتشفة بنيويًا",
                "execution_state": "فاشلة",
                "final_bucket": "Failed",
                "actionable_now": False,
                "acceptance_state": "no",
                "continuation_state": "no",
                "decisive_factor": "تعذر بناء الخصائص المرجعية",
                "funding_context": features.get("funding_context", "unknown"),
                "basis_context": features.get("basis_context", "missing"),
                "decision_reason": "تعذر بناء المواد الخام البنيوية",
                "not_actionable_reason": "تعذر بناء الخصائص المرجعية أو تعطل endpoint",
                "regime_pattern": "NEUTRAL_REGIME",
                "trigger_pattern": "NO_TRIGGER",
                "execution_pattern": "FAILED_EXECUTION_BLOCK",
                "failure": {"failure_reason": "تعذر بناء الخصائص المرجعية"},
                "diagnostics": raw_materials.get("diagnostics", []),
                "failure_risk": "مرتفع",
                "failure_reasons": ["نقص بيانات أو تعطل endpoint"],
                "importance_score": 0.0,
            }

        result = build_result_from_raw_materials(raw_materials)
        features = safe_dict_from_api(result.get("market_features"))

        asset_memory = load_asset_memory(symbol)
        features["asset_memory_profile"] = build_asset_behavior_profile(symbol, features, asset_memory)
        features["ratio_extreme_profile"] = build_ratio_extreme_profile(features, features["asset_memory_profile"])
        features["ratio_inflection_profile"] = detect_extreme_inflection(features["ratio_extreme_profile"], features)

        leader_info = determine_leader_type(features)
        result["leader_type"] = leader_info.get("leader_type")
        result["leader_reason"] = build_leader_reason(features, leader_info)
        result["leader_evidence"] = safe_dict_from_api(leader_info.get("leader_scores"))

        hypothesis_scores = build_hypothesis_scores(
            features,
            {
                "regime_pattern": result.get("regime_pattern"),
                "trigger_pattern": result.get("trigger_pattern"),
                "execution_pattern": result.get("execution_pattern"),
            },
            leader_info,
        )
        ranked_hypothesis = rank_hypotheses(hypothesis_scores)
        selected_hypothesis = select_primary_hypothesis(ranked_hypothesis)
        result["hypothesis_scores"] = hypothesis_scores
        result["primary_hypothesis"] = selected_hypothesis.get("primary_hypothesis")
        result["alternative_hypotheses"] = selected_hypothesis.get("alternative_hypotheses", [])
        result["hypothesis_inputs"] = {
            "leader_type": leader_info.get("leader_type"),
            "regime_pattern": result.get("regime_pattern"),
            "trigger_pattern": result.get("trigger_pattern"),
        }

        result["confidence_score"] = compute_confidence_score(features, result)
        result["uncertainty_score"] = compute_uncertainty_score(features, result)
        result["conflict_score"] = compute_conflict_score(features, result)
        result["invalidation_reason"] = build_invalidation_reason(features, result)
        result["next_failure_mode"] = build_next_failure_mode(features, result)
        result["causal_chain"] = build_causal_chain(features, result)

        prev_state = load_previous_symbol_state(symbol)
        transition = detect_state_transition(prev_state, result)
        result["state_transition"] = transition.get("state_transition")
        result["state_transition_reason"] = transition.get("reason", "")

        case_match = match_closest_case(build_case_vector(features, result), load_case_library())
        result["closest_case_match"] = case_match.get("case_name")
        result["closest_case_score"] = safe_float(case_match.get("score"), 0.0)
        result = apply_v2_decision_gate(result, features)
        result["market_features"] = features

        asset_local_memory = safe_dict_from_api((result.get("market_features") or {}).get("asset_local_memory"))
        result["state_transition_context"] = build_asset_state_transition_context(asset_local_memory, result)
        result["case_similarity_context"] = build_asset_case_similarity_context(asset_local_memory, result.get("market_features") or {}, result)
        result["behavior_profile_context"] = build_asset_behavior_profile_context(asset_local_memory, result)
        result["importance_score"] = compute_importance_score(result)
        result.update(build_arabic_output_fields(result))
        result = apply_behavior_profile_influence(result)
        result.update(build_arabic_output_fields(result))
        result = attach_structural_casefile(result)
        result = apply_structural_center(result)
        result = enrich_result_v2_fields(result)
        result["importance_score"] = compute_importance_score(result)
        if isinstance(result.get("structural_case"), dict):
            result["structural_case"]["importance_score"] = result["importance_score"]

        # إعادة حساب الانتقال/التشابه بعد اكتمال القرار البنيوي النهائي
        final_transition = detect_state_transition(prev_state, result)
        result["state_transition"] = final_transition.get("state_transition")
        result["state_transition_reason"] = final_transition.get("reason", "")
        asset_local_memory_latest = load_asset_memory(symbol)
        result["state_transition_context"] = build_asset_state_transition_context(asset_local_memory_latest, result)
        result["case_similarity_context"] = build_asset_case_similarity_context(asset_local_memory_latest, result.get("market_features") or {}, result)
        final_case_match = match_closest_case(build_case_vector(result.get("market_features") or {}, result), load_case_library())
        result["closest_case_match"] = final_case_match.get("case_name")
        result["closest_case_score"] = safe_float(final_case_match.get("score"), 0.0)

        result.update(build_arabic_output_fields(result))
        result["liquidation_absorption_proxy"] = detect_liquidation_absorption_proxy(result.get("market_features") or {}, result)
        save_current_symbol_state(symbol, result)
        update_asset_memory(symbol, result.get("market_features") or {}, result)
        return result

    except Exception as exc:
        return {
            "symbol": symbol,
            "analysis_source": "structural_orchestrator",
            "analysis_center": "structural_casefile",
            "error": str(exc),
            "family": None,
            "stage": "WATCH",
            "oi_state": "neutral",
            "discovery_state": "غير مكتشفة بنيويًا",
            "execution_state": "فاشلة",
            "final_bucket": "Failed",
            "actionable_now": False,
            "acceptance_state": "no",
            "continuation_state": "no",
            "decisive_factor": "حدث استثناء أثناء التحليل البنيوي",
            "funding_context": "unknown",
            "basis_context": "missing",
            "decision_reason": "تعطل داخلي أثناء بناء الأوركستريتور البنيوي",
            "discovery_reason": "",
            "stage_reason": "WATCH بسبب الاستثناء",
            "acceptance_reason": "",
            "continuation_reason": "",
            "promotion_reason": "",
            "not_actionable_reason": "تعطل داخلي أثناء التحليل البنيوي",
            "regime_pattern": "NEUTRAL_REGIME",
            "trigger_pattern": "NO_TRIGGER",
            "execution_pattern": "FAILED_EXECUTION_BLOCK",
            "failure": {"failure_reason": "تعطل داخلي أثناء التحليل البنيوي"},
            "diagnostics": [f"Exception: {exc}", traceback.format_exc(limit=2)],
            "failure_risk": "مرتفع",
            "failure_reasons": ["تعطل داخلي أثناء التحليل البنيوي"],
            "importance_score": 0.0,
        }


# المسمى الرسمي المستخدم في بقية الملف
analyze_symbol = analyze_symbol_structural


# ============================================================
# بناء المرشحين ثم المسح الكامل
# ============================================================


def prepare_candidates(
    client: BinanceFuturesPublicClient,
) -> Tuple[List[Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]], Dict[str, Any]]:
    exchange_info = safe_dict_from_api(client.get_exchange_info())
    all_tickers = safe_list_from_api(client.get_all_tickers_24h())
    all_mark = safe_list_from_api(client.get_all_mark_prices())

    symbol_universe = build_symbol_universe(exchange_info)
    ticker_map = {row.get("symbol"): row for row in all_tickers if isinstance(row, dict) and row.get("symbol")}
    mark_map = {row.get("symbol"): row for row in all_mark if isinstance(row, dict) and row.get("symbol")}

    filtered: List[Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any], float]] = []
    skipped_counter = defaultdict(int)
    full_scan_mode = bool(DYNAMIC_SETTINGS.get("FULL_UNIVERSE_SCAN_ENABLED", False))

    for symbol, symbol_meta in symbol_universe.items():
        ticker_24h = ticker_map.get(symbol)
        mark_info = mark_map.get(symbol)
        if not ticker_24h or not mark_info:
            skipped_counter["missing_market_snapshot"] += 1
            continue

        if full_scan_mode:
            passed, reasons = universe_filter_for_full_scan(symbol, symbol_meta, mark_info)
            if not passed:
                skipped_counter[reasons[0] if reasons else "universe_filter_reject"] += 1
                continue
        else:
            passed, reasons = quick_filter(symbol, symbol_meta, ticker_24h, mark_info)
            if not passed:
                skipped_counter[reasons[0] if reasons else "quick_filter_reject"] += 1
                continue

        score_hint = safe_float(ticker_24h.get("quoteVolume"))
        filtered.append((symbol, symbol_meta, ticker_24h, mark_info, score_hint))

    filtered.sort(key=lambda item: item[4], reverse=True)

    if full_scan_mode:
        final_candidates = filtered
    else:
        filtered = filtered[: DYNAMIC_SETTINGS["TOP_N_BY_24H_VOLUME"]]
        # نقتطع عددًا أصغر للتحليل العميق حتى لا نرهق البيئة المحدودة
        final_candidates = filtered[: DYNAMIC_SETTINGS["MAX_CANDIDATES_AFTER_FILTER"]]

    return [(s, sm, t, m) for s, sm, t, m, _ in final_candidates], {
        "universe_size": len(symbol_universe),
        "snapshot_tickers": len(ticker_map),
        "snapshot_mark": len(mark_map),
        "full_scan_mode": full_scan_mode,
        "quick_filter_passed": len(filtered),
        "deep_candidates": len(final_candidates),
        "skipped_counter": dict(skipped_counter),
    }



def scan_once(client: BinanceFuturesPublicClient) -> Dict[str, Any]:
    cycle_started = utc_now_iso()
    candidates, prep_stats = prepare_candidates(client)

    results: List[Dict[str, Any]] = []

    if PERFORMANCE_SETTINGS["ENABLE_THREADPOOL"] and DYNAMIC_SETTINGS["MAX_WORKERS"] > 1:
        with ThreadPoolExecutor(max_workers=DYNAMIC_SETTINGS["MAX_WORKERS"]) as executor:
            futures = {
                executor.submit(analyze_symbol, client, symbol, symbol_meta, ticker_24h, mark_info): symbol
                for symbol, symbol_meta, ticker_24h, mark_info in candidates
            }
            for future in as_completed(futures):
                results.append(future.result())
    else:
        for symbol, symbol_meta, ticker_24h, mark_info in candidates:
            results.append(analyze_symbol(client, symbol, symbol_meta, ticker_24h, mark_info))

    market_context = build_market_regime_context(client)

    enhanced_results: List[Dict[str, Any]] = []
    for row in results:
        row = dict(row)
        row["importance_score"] = compute_importance_score(row)
        row["market_context"] = market_context
        features = safe_dict_from_api(row.get("market_features"))
        row["cross_asset_similarity_context"] = (
            build_cross_asset_similarity_context(features, row)
            if features else {
                "available": False,
                "summary_ar": "لا يمكن بناء تشابه عرضي لأن features غير متاحة.",
                "top_cases": [],
            }
        )
        row = apply_market_context_influence(row)
        row = apply_structural_center(row)
        row.update(build_arabic_output_fields(row))
        enhanced_results.append(row)
        update_global_case_library(row)

    enhanced_results.sort(
        key=lambda r: (
            STRUCTURAL_EXECUTION_ORDER.get(_safe_state_text(r.get("structural_execution_state"), "failed"), 99),
            -safe_float(r.get("importance_score"), 0.0),
            -safe_float((r.get("market_features") or {}).get("vol_expansion_last"), 0.0),
            -safe_float((r.get("market_features") or {}).get("trade_expansion_last"), 0.0),
        )
    )

    summary = defaultdict(int)
    for row in enhanced_results:
        summary[f"structural::{row.get('structural_execution_state', 'unknown')}"] += 1
        if OUTPUT_SETTINGS.get("SHOW_LEGACY_SUMMARY", False):
            summary[row.get("final_bucket", "Failed")] += 1
            summary[f"stage::{row.get('stage', 'WATCH')}"] += 1
            if row.get("family"):
                summary[f"family::{row['family']}"] += 1

    printed_results = filter_and_sort_results_for_output(enhanced_results)

    return {
        "started_at": cycle_started,
        "finished_at": utc_now_iso(),
        "prep_stats": prep_stats,
        "summary": dict(summary),
        "results": enhanced_results,
        "printed_results": printed_results,
        "market_context": market_context,
    }


# ============================================================
# طبقة التعريب والتفسير البشري للمخرجات
# ============================================================


ARABIC_FAMILY_LABELS: Dict[str, str] = {
    "POSITION_LED_SQUEEZE_BUILDUP": "بناء ضغط صاعد تقوده المراكز",
    "ACCOUNT_LED_ACCUMULATION": "تراكم صاعد تقوده الحسابات",
    "CONSENSUS_BULLISH_EXPANSION": "توسع صاعد توافقي",
    "FLOW_LIQUIDITY_VACUUM_BREAKOUT": "اختراق تدفقي مع فراغ سيولة",
    "ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION": "توسع صاعد توافقي تقوده الحسابات",
}

ARABIC_STAGE_LABELS: Dict[str, str] = {
    "WATCH": "مراقبة",
    "PREPARE": "تهيؤ",
    "ARMED": "جاهزية",
    "TRIGGERED": "مفعّلة",
    "LATE": "متأخرة",
}

ARABIC_OI_STATE_LABELS: Dict[str, str] = {
    "premove buildup": "بناء مبكر قبل الحركة",
    "buildup expansion": "توسع بنائي مع الصعود",
    "neutral": "محايد",
    "covering": "تغطية مراكز",
    "price up OI flat": "صعود سعري مع OI مسطح",
    "squeeze_buildup": "بناء ضغط عصر",
    "squeeze_covering": "تغطية عصر",
    "squeeze_post_covering": "إعادة بناء بعد العصر",
}

ARABIC_BUCKET_LABELS: Dict[str, str] = {
    "Actionable now": "قابلة للتنفيذ الآن",
    "Discovered but not actionable": "مكتشفة لكن غير قابلة للتنفيذ الآن",
    "Late": "متأخرة",
    "Failed": "فاشلة",
}

ARABIC_REGIME_LABELS: Dict[str, str] = {
    "NEUTRAL_REGIME": "نظام محايد",
    "LOWER_EXTREME_SHORT_SQUEEZE_REGIME": "نظام تطرف سفلي صالح لعصر صاعد",
    "UPPER_EXTREME_BULLISH_EXPANSION_REGIME": "نظام تطرف علوي مع توسع صاعد",
    "UPPER_EXTREME_FAILURE_REGIME": "نظام تطرف علوي مهدد بالفشل",
    "LONG_CROWDING_ROLLOVER_REGIME": "نظام ازدحام شرائي يتعرض للانقلاب",
    "COUNTERFLOW_REPRICING_REGIME": "نظام إعادة تسعير عكسي",
    "CROWDED_SHORT_REGIME": "نظام ازدحام بيعي",
    "CROWDED_LONG_REGIME": "نظام ازدحام شرائي",
    "COUNTERFLOW_REGIME": "نظام تدفق عكسي",
    "MIXED_REGIME": "نظام مختلط",
    "MICRO_CROWDED_SHORT_REGIME": "ازدحام بيعي قصير المدى",
    "MICRO_CROWDED_LONG_REGIME": "ازدحام شرائي قصير المدى",
}

ARABIC_TRIGGER_LABELS: Dict[str, str] = {
    "NO_TRIGGER": "لا يوجد زناد مكتمل بعد",
    "POST_FLUSH_BULLISH_REBUILD_TRIGGER": "زناد إعادة بناء صاعدة بعد التفريغ",
    "POST_FLUSH_FAILED_REBUILD_TRIGGER": "زناد فشل إعادة البناء بعد التفريغ",
    "POST_FLUSH_RELIEF_BOUNCE_TRIGGER": "زناد ارتداد تقني بعد التفريغ",
    "COUNTERFLOW_SQUEEZE_TRIGGER": "زناد عصر عكسي ضد التموضع الظاهر",
    "FAILED_BREAKOUT_TRIGGER": "زناد فشل الاختراق",
    "BREAKOUT_ACCEPT_TRIGGER": "زناد اختراق مع قبول",
    "BREAKOUT_IGNITION_TRIGGER": "زناد اشتعال الاختراق",
    "PREPARE_TO_ARMED_TRIGGER": "زناد انتقال من التهيؤ إلى الجاهزية",
}

ARABIC_EXECUTION_LABELS: Dict[str, str] = {
    "OBSERVE_ONLY": "مراقبة فقط",
    "ACTIONABLE_EXECUTION": "تنفيذ ممكن الآن",
    "LATE_EXECUTION_BLOCK": "ممنوعة تنفيذيًا بسبب التأخر",
    "FAILED_EXECUTION_BLOCK": "ممنوعة تنفيذيًا بسبب الفشل",
    "NO_ACCEPTANCE_BLOCK": "ممنوعة تنفيذيًا بسبب غياب القبول",
    "WEAK_CONTINUATION_BLOCK": "ممنوعة تنفيذيًا بسبب ضعف الاستمرار",
    "ARMED_WAIT_FOR_CONFIRMATION": "جاهزة لكن تنتظر تأكيدًا",
    "PREPARE_WAIT_FOR_TRIGGER": "تهيؤ وتنتظر الزناد",
}

ARABIC_FUNDING_LABELS: Dict[str, str] = {
    "unknown": "غير معروف",
    "funding quiet": "تمويل هادئ",
    "funding negative improving": "تمويل سلبي يتحسن",
    "non-crowded": "تمويل غير مزدحم",
    "crowded long side": "ازدحام على جهة الشراء",
    "funding hostile": "تمويل ضاغط",
}

ARABIC_BASIS_LABELS: Dict[str, str] = {
    "missing": "بيانات غير كافية",
    "neutral": "حيادي",
    "supportive": "داعم",
    "adverse": "ضاغط",
}

ARABIC_CROWDING_STATE_LABELS: Dict[str, str] = {
    "neutral_regime": "نظام محايد",
    "crowded_short_regime": "ازدحام بيعي على الفريمات الأعلى",
    "crowded_long_regime": "ازدحام شرائي على الفريمات الأعلى",
    "counterflow_regime": "نظام تدفق عكسي",
    "mixed_regime": "نظام مختلط",
    "micro_crowded_short_regime": "ازدحام بيعي قصير المدى",
    "micro_crowded_long_regime": "ازدحام شرائي قصير المدى",
}

ARABIC_POST_FLUSH_LABELS: Dict[str, str] = {
    "RELIEF_BOUNCE_AFTER_FLUSH": "ارتداد تقني بعد التفريغ",
    "BULLISH_REBUILD_AFTER_FLUSH": "إعادة بناء صاعدة بعد التفريغ",
    "FAILED_REBUILD_AFTER_FLUSH": "فشل إعادة البناء بعد التفريغ",
}


def translate_label(value: Any, mapping: Dict[str, str], default: str = "غير معروف") -> str:
    if value is None:
        return default
    value_str = str(value)
    return mapping.get(value_str, value_str)


def _bool_ar(value: bool) -> str:
    return "نعم" if value else "لا"


def derive_leader_type(result: Dict[str, Any]) -> str:
    counterflow_pattern = result.get("counterflow_pattern") or {}
    if counterflow_pattern.get("active", False):
        return "counterflow_led"

    family = result.get("family")
    if family == "POSITION_LED_SQUEEZE_BUILDUP":
        return "position_led"
    if family in {"ACCOUNT_LED_ACCUMULATION", "ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION"}:
        return "account_led"
    if family == "CONSENSUS_BULLISH_EXPANSION":
        return "consensus_led"
    if family == "FLOW_LIQUIDITY_VACUUM_BREAKOUT":
        return "flow_led"

    features = result.get("market_features") or {}
    ratio_alignment = result.get("ratio_alignment") or features.get("ratio_alignment") or {}
    pos_norm = safe_float((ratio_alignment.get("position") or {}).get("normalized"), 0.0)
    acc_norm = safe_float((ratio_alignment.get("account") or {}).get("normalized"), 0.0)
    cons_norm = safe_float((ratio_alignment.get("consensus") or {}).get("normalized"), 0.0)
    if pos_norm > acc_norm and pos_norm > 0:
        return "position_led"
    if acc_norm > pos_norm and acc_norm > 0:
        return "account_led"
    if cons_norm > 0:
        return "consensus_led"
    if (ratio_alignment.get("overall_conflict") or False):
        return "mixed_led"
    return "undetermined"


def derive_leader_type_ar(result: Dict[str, Any]) -> str:
    leader_type = derive_leader_type(result)
    mapping = {
        "counterflow_led": "قيادة عكسية مع إعادة تسعير ضد التموضع الظاهر",
        "position_led": "القيادة للمراكز",
        "account_led": "القيادة للحسابات",
        "consensus_led": "قيادة توافقية بين المراكز والحسابات",
        "flow_led": "القيادة للتدفق والتنفيذ",
        "mixed_led": "قيادة مختلطة ومتعارضة",
        "undetermined": "قيادة غير محسومة بعد",
    }
    return mapping.get(leader_type, "قيادة غير محسومة بعد")


def _build_hypothesis_scores_legacy(result: Dict[str, Any]) -> Dict[str, float]:
    scores: Dict[str, float] = {
        "bullish_rebuild_after_flush": 0.0,
        "relief_bounce_after_flush": 0.0,
        "failed_rebuild_after_flush": 0.0,
        "lower_extreme_short_squeeze_up": 0.0,
        "upper_extreme_bullish_expansion": 0.0,
        "upper_extreme_failed_acceptance": 0.0,
        "long_crowding_rollover": 0.0,
        "oi_expansion_without_bullish_confirmation": 0.0,
        "counterflow_short_squeeze_expansion": 0.0,
        "position_led_buildup": 0.0,
        "account_led_accumulation": 0.0,
        "consensus_bullish_expansion": 0.0,
        "late_blowoff": 0.0,
    }

    features = result.get("market_features") or {}
    ratio_alignment = result.get("ratio_alignment") or features.get("ratio_alignment") or {}
    ratio_conflict = result.get("ratio_conflict") or features.get("ratio_conflict") or {}
    extreme_engine = result.get("extreme_engine") or features.get("extreme_engine") or {}
    patterns = (extreme_engine.get("patterns") or {}) if isinstance(extreme_engine, dict) else {}
    crowding_state = ((result.get("crowding_regime") or features.get("crowding_regime") or {}).get("state") or "")
    leader_type = derive_leader_type(result)
    acceptance_lifecycle = safe_dict_from_api(result.get("acceptance_lifecycle"))
    flow_mismatch = safe_dict_from_api(result.get("flow_quality_mismatch"))
    short_pressure = safe_dict_from_api(result.get("short_pressure_transition"))
    pre_rise = safe_dict_from_api(result.get("pre_rise_signature"))
    post_flush = safe_dict_from_api(result.get("post_flush_structures"))
    acceptance_quality = _safe_state_text(acceptance_lifecycle.get("acceptance_quality"), "unaccepted")
    oi_supportive = bool(result.get("oi_supportive", False))
    price_late = bool(features.get("price_late", False))
    squeeze_probability = safe_float(extreme_engine.get("squeeze_probability"), 0.0)
    flush_probability = safe_float(extreme_engine.get("flush_probability"), 0.0)

    if result.get("bullish_rebuild_after_flush", {}).get("active", False):
        scores["bullish_rebuild_after_flush"] += 0.60
        scores["bullish_rebuild_after_flush"] += min(safe_float(result.get("bullish_rebuild_after_flush", {}).get("score"), 0.0) / 14.0, 0.25)
    if result.get("relief_bounce_after_flush", {}).get("active", False):
        scores["relief_bounce_after_flush"] += 0.60
        scores["relief_bounce_after_flush"] += min(safe_float(result.get("relief_bounce_after_flush", {}).get("score"), 0.0) / 14.0, 0.20)
    if result.get("failed_rebuild_after_flush", {}).get("active", False):
        scores["failed_rebuild_after_flush"] += 0.62
        scores["failed_rebuild_after_flush"] += min(safe_float(result.get("failed_rebuild_after_flush", {}).get("score"), 0.0) / 14.0, 0.22)

    if (patterns.get("lower_extreme_squeeze") or {}).get("active", False):
        scores["lower_extreme_short_squeeze_up"] += 0.50 + min(squeeze_probability / 140.0, 0.22)
    if (patterns.get("upper_extreme_bullish_expansion") or {}).get("active", False):
        scores["upper_extreme_bullish_expansion"] += 0.48 + min(squeeze_probability / 150.0, 0.20)
    if (patterns.get("upper_extreme_failure") or {}).get("active", False):
        scores["upper_extreme_failed_acceptance"] += 0.50 + min(flush_probability / 140.0, 0.22)
    if (patterns.get("long_crowding_rollover") or {}).get("active", False):
        scores["long_crowding_rollover"] += 0.50 + min(flush_probability / 150.0, 0.20)
    if (patterns.get("oi_expansion_without_bullish_confirmation") or {}).get("active", False):
        scores["oi_expansion_without_bullish_confirmation"] += 0.47 + min(flush_probability / 150.0, 0.18)

    if (result.get("counterflow_pattern") or {}).get("active", False):
        scores["counterflow_short_squeeze_expansion"] += 0.55
        scores["counterflow_short_squeeze_expansion"] += min(safe_float((result.get("counterflow_pattern") or {}).get("score"), 0.0) / 16.0, 0.18)

    family = result.get("family")
    if family == "POSITION_LED_SQUEEZE_BUILDUP":
        scores["position_led_buildup"] += 0.52
    elif family == "ACCOUNT_LED_ACCUMULATION":
        scores["account_led_accumulation"] += 0.52
    elif family == "CONSENSUS_BULLISH_EXPANSION":
        scores["consensus_bullish_expansion"] += 0.54
    elif family == "ACCOUNT_LED_CONSENSUS_BULLISH_EXPANSION":
        scores["account_led_accumulation"] += 0.26
        scores["consensus_bullish_expansion"] += 0.24

    if leader_type == "position_led":
        scores["position_led_buildup"] += 0.12
        scores["lower_extreme_short_squeeze_up"] += 0.06
    elif leader_type == "account_led":
        scores["account_led_accumulation"] += 0.12
        scores["upper_extreme_bullish_expansion"] += 0.04
    elif leader_type == "consensus_led":
        scores["consensus_bullish_expansion"] += 0.12
    elif leader_type == "counterflow_led":
        scores["counterflow_short_squeeze_expansion"] += 0.12

    if pre_rise.get("state") == "true_early_bullish_signature":
        for key in ["position_led_buildup", "lower_extreme_short_squeeze_up", "consensus_bullish_expansion", "bullish_rebuild_after_flush"]:
            scores[key] += 0.08
    elif pre_rise.get("state") == "account_led_early_signature":
        scores["account_led_accumulation"] += 0.10
        scores["upper_extreme_bullish_expansion"] += 0.04

    if short_pressure.get("pressure_state") == "true_short_squeeze":
        scores["lower_extreme_short_squeeze_up"] += 0.08
        scores["counterflow_short_squeeze_expansion"] += 0.07
        scores["bullish_rebuild_after_flush"] += 0.04
    elif short_pressure.get("pressure_state") == "ordinary_covering":
        scores["relief_bounce_after_flush"] += 0.07
    elif short_pressure.get("pressure_state") == "fake_expansion_risk":
        scores["oi_expansion_without_bullish_confirmation"] += 0.09

    if ratio_alignment.get("overall_supportive", False):
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "upper_extreme_bullish_expansion",
            "counterflow_short_squeeze_expansion",
            "position_led_buildup",
            "account_led_accumulation",
            "consensus_bullish_expansion",
        ]:
            scores[key] += 0.06
    if ratio_alignment.get("higher_tf_supportive", False):
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "upper_extreme_bullish_expansion",
            "position_led_buildup",
            "account_led_accumulation",
            "consensus_bullish_expansion",
        ]:
            scores[key] += 0.05
    if ratio_alignment.get("overall_conflict", False) or ratio_conflict.get("state") == "conflicted":
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "upper_extreme_bullish_expansion",
            "position_led_buildup",
            "account_led_accumulation",
            "consensus_bullish_expansion",
            "counterflow_short_squeeze_expansion",
        ]:
            scores[key] -= 0.10
        for key in [
            "upper_extreme_failed_acceptance",
            "long_crowding_rollover",
            "oi_expansion_without_bullish_confirmation",
            "failed_rebuild_after_flush",
        ]:
            scores[key] += 0.06

    if oi_supportive:
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "upper_extreme_bullish_expansion",
            "counterflow_short_squeeze_expansion",
            "position_led_buildup",
            "account_led_accumulation",
            "consensus_bullish_expansion",
        ]:
            scores[key] += 0.07
    else:
        for key in [
            "upper_extreme_failed_acceptance",
            "long_crowding_rollover",
            "oi_expansion_without_bullish_confirmation",
            "relief_bounce_after_flush",
            "failed_rebuild_after_flush",
        ]:
            scores[key] += 0.06

    if acceptance_quality in {"clean_repricing", "rebuild_reacceptance"}:
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "upper_extreme_bullish_expansion",
            "counterflow_short_squeeze_expansion",
            "consensus_bullish_expansion",
        ]:
            scores[key] += 0.10
    elif acceptance_quality == "early_reacceptance":
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "counterflow_short_squeeze_expansion",
            "position_led_buildup",
            "account_led_accumulation",
        ]:
            scores[key] += 0.06
    elif acceptance_quality == "fragile_acceptance":
        for key in [
            "bullish_rebuild_after_flush",
            "upper_extreme_bullish_expansion",
            "position_led_buildup",
            "account_led_accumulation",
        ]:
            scores[key] += 0.03
        for key in ["upper_extreme_failed_acceptance", "failed_rebuild_after_flush"]:
            scores[key] += 0.02
    elif acceptance_quality == "failed_acceptance":
        for key in [
            "upper_extreme_failed_acceptance",
            "long_crowding_rollover",
            "oi_expansion_without_bullish_confirmation",
            "failed_rebuild_after_flush",
        ]:
            scores[key] += 0.10
    else:
        for key in ["relief_bounce_after_flush", "oi_expansion_without_bullish_confirmation"]:
            scores[key] += 0.05

    if flow_mismatch.get("mismatch", False):
        for key in [
            "bullish_rebuild_after_flush",
            "lower_extreme_short_squeeze_up",
            "upper_extreme_bullish_expansion",
            "counterflow_short_squeeze_expansion",
            "consensus_bullish_expansion",
        ]:
            scores[key] -= 0.08
        for key in ["oi_expansion_without_bullish_confirmation", "failed_rebuild_after_flush"]:
            scores[key] += 0.08

    if crowding_state in {"crowded_short_regime", "micro_crowded_short_regime"}:
        scores["lower_extreme_short_squeeze_up"] += 0.07
        scores["counterflow_short_squeeze_expansion"] += 0.05
    elif crowding_state in {"crowded_long_regime", "micro_crowded_long_regime"}:
        scores["upper_extreme_bullish_expansion"] += 0.04
        scores["upper_extreme_failed_acceptance"] += 0.06
        scores["long_crowding_rollover"] += 0.07
    elif crowding_state == "counterflow_regime":
        scores["counterflow_short_squeeze_expansion"] += 0.08
    elif crowding_state == "mixed_regime":
        for k in scores:
            scores[k] -= 0.02

    if post_flush.get("pattern") == "bullish_rebuild_after_flush":
        scores["bullish_rebuild_after_flush"] += 0.06
    elif post_flush.get("pattern") == "relief_bounce_after_flush":
        scores["relief_bounce_after_flush"] += 0.06
    elif post_flush.get("pattern") == "failed_rebuild_after_flush":
        scores["failed_rebuild_after_flush"] += 0.07

    if price_late:
        scores["late_blowoff"] += 0.74
        for key in ["bullish_rebuild_after_flush", "lower_extreme_short_squeeze_up", "upper_extreme_bullish_expansion", "counterflow_short_squeeze_expansion"]:
            scores[key] -= 0.12

    for k in list(scores.keys()):
        scores[k] = round(clamp(scores[k], 0.0, 0.99), 2)
    return scores

def derive_primary_hypothesis(result: Dict[str, Any]) -> Tuple[str, List[str], Dict[str, float]]:
    hypothesis_scores = build_hypothesis_scores(result)
    ranked = sorted(hypothesis_scores.items(), key=lambda x: x[1], reverse=True)
    primary = ranked[0][0] if ranked else "undetermined"
    alternatives = [name for name, score in ranked[1:4] if score >= 0.18]
    return primary, alternatives, hypothesis_scores


def derive_primary_hypothesis_ar(result: Dict[str, Any]) -> Tuple[str, List[str]]:
    primary, alternatives, _ = derive_primary_hypothesis(result)
    mapping = {
        "bullish_rebuild_after_flush": "إعادة بناء صاعدة بعد التفريغ",
        "relief_bounce_after_flush": "ارتداد تقني بعد التفريغ",
        "failed_rebuild_after_flush": "فشل إعادة البناء بعد التفريغ",
        "lower_extreme_short_squeeze_up": "عصر صاعد من تطرف سفلي",
        "upper_extreme_bullish_expansion": "توسع صاعد من تطرف علوي",
        "upper_extreme_failed_acceptance": "فشل قبول من تطرف علوي",
        "long_crowding_rollover": "انقلاب ازدحام شرائي",
        "oi_expansion_without_bullish_confirmation": "اتساع OI بلا تأكيد صاعد",
        "counterflow_short_squeeze_expansion": "توسع صاعد عكسي ضد التموضع الظاهر",
        "position_led_buildup": "بناء صاعد تقوده المراكز",
        "account_led_accumulation": "تراكم صاعد تقوده الحسابات",
        "consensus_bullish_expansion": "توسع صاعد توافقي",
        "late_blowoff": "امتداد متأخر / ذروة اندفاع",
        "undetermined": "فرضية غير محسومة بعد",
    }
    primary_ar = mapping.get(primary, "فرضية غير محسومة بعد")
    alternatives_ar = [mapping.get(item, item) for item in alternatives]
    return primary_ar, alternatives_ar


def derive_conflict_score(result: Dict[str, Any]) -> float:
    features = result.get("market_features") or {}
    ratio_alignment = result.get("ratio_alignment") or features.get("ratio_alignment") or {}
    ratio_conflict = result.get("ratio_conflict") or features.get("ratio_conflict") or {}
    crowding_state = ((result.get("crowding_regime") or features.get("crowding_regime") or {}).get("state") or "")
    behavior = safe_dict_from_api(result.get("behavior_profile_context"))
    score = 0.05
    score += clamp(safe_float(ratio_conflict.get("penalty"), 0.0) / 2.5, 0.0, 0.35)
    if ratio_alignment.get("overall_conflict", False):
        score += 0.18
    if crowding_state == "mixed_regime":
        score += 0.12
    if result.get("failure", {}).get("early_failure", False):
        score += 0.10
    if result.get("counterflow_pattern", {}).get("active", False):
        score += 0.05
    if behavior.get("available", False) and behavior.get("fit_state") == "misaligned":
        score += 0.07
    if result.get("acceptance_state") == "yes" and result.get("continuation_state") == "yes":
        score -= 0.06
    return round(clamp(score, 0.0, 0.98), 2)

def derive_uncertainty_score(result: Dict[str, Any]) -> float:
    _, _, scores = derive_primary_hypothesis(result)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top = ranked[0][1] if ranked else 0.0
    second = ranked[1][1] if len(ranked) > 1 else 0.0
    gap = top - second
    behavior = safe_dict_from_api(result.get("behavior_profile_context"))

    score = 0.20
    score += derive_conflict_score(result) * 0.55
    if gap < 0.05:
        score += 0.25
    elif gap < 0.10:
        score += 0.16
    elif gap < 0.15:
        score += 0.08
    if result.get("final_bucket") == "Discovered but not actionable":
        score += 0.05
    if behavior.get("available", False) and behavior.get("fit_state") == "misaligned":
        score += 0.06
    if result.get("actionable_now", False):
        score -= 0.06
    return round(clamp(score, 0.02, 0.98), 2)

def derive_confidence_score(result: Dict[str, Any]) -> float:
    _, _, scores = derive_primary_hypothesis(result)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top = ranked[0][1] if ranked else 0.0
    second = ranked[1][1] if len(ranked) > 1 else 0.0
    behavior = safe_dict_from_api(result.get("behavior_profile_context"))

    score = 0.18 + top * 0.52 + (top - second) * 0.18
    if result.get("actionable_now", False):
        score += 0.08
    if result.get("acceptance_state") == "yes":
        score += 0.05
    if result.get("continuation_state") == "yes":
        score += 0.04
    if result.get("oi_supportive", False):
        score += 0.03
    if behavior.get("available", False):
        score += safe_float(behavior.get("decision_bias_bonus"), 0.0)
        score -= safe_float(behavior.get("decision_bias_penalty"), 0.0)
    score -= derive_conflict_score(result) * 0.12
    score -= derive_uncertainty_score(result) * 0.10
    return round(clamp(score, 0.05, 0.98), 2)

def derive_next_failure_mode_ar(result: Dict[str, Any]) -> str:
    primary, _ = derive_primary_hypothesis_ar(result)
    if primary == "إعادة بناء صاعدة بعد التفريغ":
        return "الخطر التالي هو فشل الحفاظ على المستوى المستعاد ثم التحول إلى فشل إعادة بناء بعد التفريغ."
    if primary == "ارتداد تقني بعد التفريغ":
        return "الخطر التالي هو انتهاء الارتداد وعودة اختبار القاع أو امتداد الهبوط من جديد."
    if primary == "فشل إعادة البناء بعد التفريغ":
        return "الخطر التالي هو إعادة اختبار القاع أو امتداد موجة هبوط ثانية."
    if primary == "عصر صاعد من تطرف سفلي":
        return "الخطر التالي هو اختراق خادع أو تحوّل الحركة إلى ارتداد تقني إذا انكمش OI بسرعة."
    if primary == "توسع صاعد من تطرف علوي":
        return "الخطر التالي هو فشل قبول من تطرف علوي أو انقلاب ازدحام شرائي إذا ظهرت شموع رفض من القمة."
    if primary in {"فشل قبول من تطرف علوي", "انقلاب ازدحام شرائي", "اتساع OI بلا تأكيد صاعد"}:
        return "الخطر التالي هو تفريغ هابط سريع أو امتداد ضغط بيعي مع ازدياد هشاشة اللونغات المتأخرة."
    if primary == "توسع صاعد عكسي ضد التموضع الظاهر":
        return "الخطر التالي هو توقف إعادة التسعير العكسية ثم انهيار الحركة إذا لم يتحول الضغط القسري إلى قبول حقيقي."
    return "الخطر التالي هو فشل القبول أو ضعف الاستمرار إذا لم تؤكد OI والنسب والسعر الحركة الحالية."


def derive_invalidation_reason_ar(result: Dict[str, Any]) -> str:
    primary, _ = derive_primary_hypothesis_ar(result)
    if primary == "إعادة بناء صاعدة بعد التفريغ":
        return "يُبطَل هذا السيناريو إذا فُقد المستوى المستعاد، وعادت النسب للتراجع، وانكمش OI ضد الحركة."
    if primary == "ارتداد تقني بعد التفريغ":
        return "يُبطَل هذا الارتداد إذا فشل السعر في الحفاظ على الارتداد الحالي وعاد الضغط البيعي مع غياب إعادة بناء في النسب."
    if primary == "فشل إعادة البناء بعد التفريغ":
        return "يُبطَل هذا الحكم فقط إذا عاد OI للارتفاع بوضوح، وتحسنت Acco وPosit والنسبة العامة، ونجح السعر في إعادة القبول."
    if primary in {"عصر صاعد من تطرف سفلي", "توسع صاعد عكسي ضد التموضع الظاهر", "توسع صاعد من تطرف علوي", "بناء صاعد تقوده المراكز", "تراكم صاعد تقوده الحسابات", "توسع صاعد توافقي"}:
        return "يُبطَل هذا السيناريو إذا فشل الاختراق أو عاد السعر داخل النطاق، أو فقدت الحركة دعم OI، أو تراجعت النسب بدل أن تؤكدها."
    if primary in {"فشل قبول من تطرف علوي", "انقلاب ازدحام شرائي", "اتساع OI بلا تأكيد صاعد", "امتداد متأخر / ذروة اندفاع"}:
        return "يُبطَل هذا السيناريو إذا استعاد السعر القمة أو المنطقة المرجعية مع قبول واضح وعاد OI والنسب إلى البناء الإيجابي."
    return "يُبطَل هذا السيناريو إذا فقد السعر المستوى المرجعي الحالي، أو انكمش OI ضد الحركة، أو تراجعت النسب بدل أن تؤكدها."


def build_causal_chain_ar(result: Dict[str, Any]) -> str:
    features = result.get("market_features") or {}
    tf_1h = (features.get("multi_tf") or {}).get("1h", {})
    tf_4h = (features.get("multi_tf") or {}).get("4h", {})
    regime_ar = translate_label(result.get("regime_pattern"), ARABIC_REGIME_LABELS)
    trigger_ar = translate_label(result.get("trigger_pattern"), ARABIC_TRIGGER_LABELS)
    leader_ar = derive_leader_type_ar(result)
    primary, alternatives = derive_primary_hypothesis_ar(result)
    ratio_alignment = result.get("ratio_alignment") or {}

    higher_tf_parts = []
    if tf_4h.get("accumulation"):
        higher_tf_parts.append("أظهر الأربع ساعات تراكمًا بنيويًا")
    if tf_1h.get("accumulation"):
        higher_tf_parts.append("وأظهر فريم الساعة تراكمًا داعمًا")
    if tf_1h.get("ignition"):
        higher_tf_parts.append("وبدأ فريم الساعة يعطي اشتعالًا أوليًا")
    htf_text = " ".join(higher_tf_parts) if higher_tf_parts else "بقيت الفريمات الأعلى في دور تأكيدي محدود"

    reason_parts: List[str] = []
    if result.get("bullish_rebuild_after_flush", {}).get("active", False):
        reason_parts.append("لأن OI أعاد البناء بعد التفريغ")
        reason_parts.append("وتحسنت Acco وPosit والنسبة العامة")
        reason_parts.append("وحافظ السعر على المنطقة المستعادة")
    elif result.get("relief_bounce_after_flush", {}).get("active", False):
        reason_parts.append("لأن الارتداد جاء مع انكماش OI")
        reason_parts.append("ولم تُظهر النسب إعادة بناء كافية")
    elif result.get("failed_rebuild_after_flush", {}).get("active", False):
        reason_parts.append("لأن السوق حاول إعادة البناء ثم فشل في القبول")
        reason_parts.append("وعاد الضعف إلى OI أو النسب")
    else:
        if result.get("acceptance_reason"):
            reason_parts.append(result.get("acceptance_reason"))
        if result.get("continuation_reason") and result.get("continuation_state") == "yes":
            reason_parts.append(result.get("continuation_reason"))
        if ratio_alignment.get("higher_tf_supportive", False):
            reason_parts.append("والفريمات الأعلى دعمت هذا الاتجاه")
        elif ratio_alignment.get("overall_conflict", False):
            reason_parts.append("رغم وجود تعارض نسبي بين الفريمات")
        elif result.get("stage_reason"):
            reason_parts.append(result.get("stage_reason"))

    reason_text = "، ".join(part for part in reason_parts if part) or result.get("decision_reason", "لا توجد سلسلة سببية مكتملة بعد")
    alt_text = alternatives[0] if alternatives else "لا يوجد بديل قريب واضح"
    return (
        f"الأصل يتحرك داخل {regime_ar} على الفريمات الأعلى، و{htf_text}، وقد قدّمت فريما 15 دقيقة و5 دقائق {trigger_ar}. "
        f"القيادة الحالية هي: {leader_ar}. الفرضية الأقوى هي: {primary}، والبديل الأقرب هو: {alt_text}. "
        f"تم ترجيح الفرضية الأساسية {reason_text}."
    )


def _derive_structural_oi_role(result: Dict[str, Any]) -> Tuple[str, str, bool]:
    features = safe_dict_from_api(result.get("market_features"))
    oi_state = _safe_state_text(result.get("oi_state"), "neutral")
    post_name = _safe_state_text(result.get("post_flush_pattern_name"), "")
    if post_name == "BULLISH_REBUILD_AFTER_FLUSH":
        return "rebuild_after_flush", "OI يعيد البناء بعد التفريغ ويدعم استئناف الحركة.", True
    if post_name == "RELIEF_BOUNCE_AFTER_FLUSH":
        return "contracting_relief", "OI ينكمش أثناء الارتداد، ما يرجّح ارتدادًا تقنيًا أكثر من بناء جديد.", False
    if post_name == "FAILED_REBUILD_AFTER_FLUSH":
        return "failed_rebuild", "OI أو النسب لم يكملا إعادة البناء، ما يبقي خطر الفشل قائمًا.", False
    if oi_state in {"squeeze_buildup", "premove buildup", "buildup expansion", "squeeze_post_covering"}:
        return "supportive_build", "OI يعمل هنا كوقود داعم للبناء أو للاستمرار الصاعد.", True
    if oi_state in {"covering", "squeeze_covering"}:
        return "covering_phase", "OI في طور تغطية/تفريغ، ويحتاج متابعة لمعرفة هل سيتحول إلى بناء جديد أم لا.", False
    if safe_float(features.get("oi_delta_3"), 0.0) > 0 and result.get("oi_supportive", False):
        return "expanding_support", "OI يتوسع مع الحركة الحالية ويمنحها دعمًا إضافيًا.", True
    return "ambiguous_oi", "قراءة OI هنا غير حاسمة بمفردها وتحتاج ترجيحًا من السعر والنسب.", bool(result.get("oi_supportive", False))


def _derive_structural_flow_quality(result: Dict[str, Any]) -> Tuple[str, str, bool]:
    features = safe_dict_from_api(result.get("market_features"))
    mismatch = safe_dict_from_api(result.get("flow_quality_mismatch") or result.get("flow_mismatch") or {})
    buy_ratio = safe_float(features.get("recent_buy_ratio"), 0.0)
    trade_exp = safe_float(features.get("trade_expansion_last"), 0.0)
    vol_exp = safe_float(features.get("vol_expansion_last"), 0.0)
    if mismatch.get("mismatch", False):
        return "mismatch", "التدفق يبدو قويًا، لكنه غير موثوق بنيويًا لأنه لم يتحول إلى قبول واستمرار واضحين.", False
    if buy_ratio >= EXECUTION_RULES["persistent_taker_buy_ratio"] and trade_exp >= EXECUTION_RULES["min_trade_activity_hold"] and vol_exp >= 1.10:
        return "persistent", "التدفق الشرائي مستمر نسبيًا وليس مجرد ومضة تنفيذية واحدة.", True
    if buy_ratio >= EXECUTION_RULES["strong_taker_buy_ratio"]:
        return "ignition_spike", "التدفق قوي لحظيًا ويصلح كاشتعال أولي، لكنه يحتاج تأكيدًا لاحقًا.", False
    return "weak_or_mixed", "التدفق هنا مختلط أو ضعيف ولا يكفي وحده لبناء قرار تنفيذي نظيف.", False


def _derive_structural_price_verdict(result: Dict[str, Any]) -> Tuple[str, str]:
    acceptance_state = _safe_state_text(result.get("acceptance_state"), "no")
    continuation_state = _safe_state_text(result.get("continuation_state"), "no")
    if result.get("final_bucket") == "Late":
        return "late_price", "السعر متأخر نسبيًا وقد أخذ الجزء الأنظف من الحركة بالفعل."
    if acceptance_state == "yes" and continuation_state == "yes":
        return "accepted_and_continuing", "السوق قبل الحركة ويعيش فوقها مع استمرار مقبول."
    if acceptance_state == "yes":
        return "accepted_but_fragile", "السوق قبل الحركة مبدئيًا، لكن الاستمرار ما زال هشًا أو غير مكتمل."
    if acceptance_state == "partial":
        return "early_reacceptance", "بدأت إعادة القبول مبكرًا لكن الحكم النهائي لم يكتمل بعد."
    if result.get("failure", {}).get("early_failure", False):
        return "failed_acceptance", "فشل السعر في تحويل الحركة الحالية إلى قبول واضح."
    return "unaccepted", "الاختراق أو الارتداد لم يتحولا بعد إلى قبول سعري حقيقي."



def build_transition_story(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    ratio_alignment = safe_dict_from_api(result.get("ratio_alignment") or features.get("ratio_alignment"))
    crowding_regime = safe_dict_from_api(result.get("crowding_regime") or features.get("crowding_regime"))
    extreme_engine = safe_dict_from_api(result.get("extreme_engine") or features.get("extreme_engine"))
    patterns = safe_dict_from_api(extreme_engine.get("patterns"))
    post_flush_name = _safe_state_text(result.get("post_flush_pattern_name"), "")
    sequence: List[str] = []
    sequence_ar: List[str] = []

    regime_pattern = _safe_state_text(result.get("regime_pattern"), "NEUTRAL_REGIME")
    if regime_pattern != "NEUTRAL_REGIME":
        sequence.append(regime_pattern.lower())
        sequence_ar.append(f"تم رصد النظام البنيوي: {translate_label(regime_pattern, ARABIC_REGIME_LABELS, regime_pattern)}")

    if patterns.get("lower_extreme_squeeze", {}).get("active", False):
        sequence.append("lower_extreme_edge_detected")
        sequence_ar.append("ظهر تطرف سفلي فعّال يضع الأصل على حافة عصر صاعد محتمل.")
    if patterns.get("upper_extreme_bullish_expansion", {}).get("active", False):
        sequence.append("upper_extreme_expansion_detected")
        sequence_ar.append("ظهر تطرف علوي داعم لاتساع صاعد أو لتسعير توافقي.")
    if patterns.get("upper_extreme_failure", {}).get("active", False):
        sequence.append("upper_extreme_failure_detected")
        sequence_ar.append("ظهرت بصمة فشل قبول من تطرف علوي.")
    if patterns.get("long_crowding_rollover", {}).get("active", False):
        sequence.append("long_crowding_rollover_detected")
        sequence_ar.append("ظهر انقلاب ازدحام شرائي داخلي مع بداية تآكل الدعم العددي.")

    if safe_float(features.get("oi_delta_3"), 0.0) > 0:
        sequence.append("oi_supportive_expansion")
        sequence_ar.append("الـ OI يتوسع ويدعم البنية الحالية بدل أن يعارضها.")
    elif safe_float(features.get("oi_delta_3"), 0.0) < 0:
        sequence.append("oi_contraction_phase")
        sequence_ar.append("الـ OI في طور انكماش، ما يستدعي التمييز بين تغطية وبناء جديد.")

    if ratio_alignment.get("higher_tf_supportive", False):
        sequence.append("higher_tf_alignment")
        sequence_ar.append("الفريمات الأعلى متوافقة بنيويًا مع الحركة الحالية.")
    elif ratio_alignment.get("overall_conflict", False):
        sequence.append("ratio_conflict")
        sequence_ar.append("هناك تعارض نسبي بين الفريمات، ما يقلل نقاء القراءة.")

    if result.get("trigger_pattern") and result.get("trigger_pattern") != "NO_TRIGGER":
        sequence.append(_safe_state_text(result.get("trigger_pattern")).lower())
        sequence_ar.append(f"الزناد الأقرب حاليًا: {translate_label(result.get('trigger_pattern'), ARABIC_TRIGGER_LABELS, result.get('trigger_pattern'))}.")

    if result.get("acceptance_state") == "yes":
        sequence.append("price_acceptance_confirmed")
        sequence_ar.append("السعر قبل الحركة الحالية ولم يعد مجرد اختراق شكلي.")
    elif result.get("acceptance_state") == "partial":
        sequence.append("early_reacceptance")
        sequence_ar.append("توجد إعادة قبول مبكرة لكن الحكم النهائي لم يكتمل بعد.")
    else:
        sequence.append("acceptance_not_confirmed")
        sequence_ar.append("القبول السعري لم يثبت بعد بصورة كاملة.")

    if post_flush_name:
        sequence.append(post_flush_name.lower())
        sequence_ar.append(f"السياق التالي بعد الحدث القوي/التفريغ: {translate_label(post_flush_name, ARABIC_POST_FLUSH_LABELS, post_flush_name)}.")

    transition_quality = "coherent"
    if ratio_alignment.get("overall_conflict", False):
        transition_quality = "mixed"
    if result.get("final_bucket") in {"Late", "Failed"}:
        transition_quality = "fragile"

    return {
        "sequence": sequence,
        "sequence_ar": sequence_ar,
        "transition_quality": transition_quality,
    }


def build_leadership_model(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    ratio_alignment = safe_dict_from_api(result.get("ratio_alignment") or features.get("ratio_alignment"))
    leader_type = derive_leader_type(result)
    leader_type_ar = derive_leader_type_ar(result)

    pos_score = safe_float((ratio_alignment.get("position") or {}).get("normalized"), 0.0)
    acc_score = safe_float((ratio_alignment.get("account") or {}).get("normalized"), 0.0)
    cons_score = safe_float((ratio_alignment.get("consensus") or {}).get("normalized"), 0.0)
    global_score = safe_float((ratio_alignment.get("global") or {}).get("normalized"), 0.0)

    sequence: List[str] = []
    evidence: List[str] = []
    if safe_float(features.get("oi_delta_3"), 0.0) > 0:
        sequence.append("oi_expansion")
    if pos_score > 0:
        sequence.append("positions_improve")
    if acc_score > 0:
        sequence.append("accounts_follow")
    if global_score > 0:
        sequence.append("global_participation_follows")

    if leader_type == "position_led":
        evidence.append("التموضع الحجمي يتقدم على الانتشار العددي للحسابات.")
    elif leader_type == "account_led":
        evidence.append("عدد الحسابات المنحازة يتحسن أسرع من أحجام التموضع.")
    elif leader_type == "consensus_led":
        evidence.append("المراكز والحسابات والسوق الأوسع تتحرك في اتجاه واحد تقريبًا.")
    elif leader_type == "counterflow_led":
        evidence.append("الحركة تُقاد بإعادة تسعير عكسية ضد التموضع الظاهر لا بإجماع نظيف.")
    elif leader_type == "flow_led":
        evidence.append("التنفيذ اللحظي هو القائد الأوضح حاليًا أكثر من البنية التموضعية.")
    else:
        evidence.append("القيادة غير محسومة بالكامل وتحتاج مزيدًا من التتابع الزمني.")

    structural_quality = "medium"
    if leader_type in {"position_led", "consensus_led"} and not ratio_alignment.get("overall_conflict", False):
        structural_quality = "high"
    elif leader_type in {"undetermined", "mixed_led"} or ratio_alignment.get("overall_conflict", False):
        structural_quality = "mixed"

    return {
        "leader_type": leader_type,
        "leader_type_ar": leader_type_ar,
        "sequence": sequence,
        "evidence": evidence,
        "structural_quality": structural_quality,
        "scores": {
            "position": pos_score,
            "account": acc_score,
            "consensus": cons_score,
            "global": global_score,
        },
    }


def evaluate_pre_rise_signature(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    leadership = safe_dict_from_api(result.get("leadership_model"))
    ratio_alignment = safe_dict_from_api(result.get("ratio_alignment") or features.get("ratio_alignment"))
    buy_ratio = safe_float(features.get("recent_buy_ratio"), 0.0)
    oi_delta_3 = safe_float(features.get("oi_delta_3"), 0.0)
    oi_value_last = safe_float(features.get("oi_value_last"), 0.0)
    price_late = bool(features.get("price_late", False))

    state = "undetermined_pre_rise"
    confidence = 0.30
    spot_confirmation = buy_ratio >= EXECUTION_RULES["persistent_taker_buy_ratio"]
    crowd_state = "not_late_yet" if not price_late else "late_or_exposed"
    oi_build_quality = "weak"
    if oi_delta_3 >= EXECUTION_RULES["oi_buildup_pct"] and oi_value_last > 0:
        oi_build_quality = "gradual_expansion"
    elif oi_delta_3 > 0:
        oi_build_quality = "early_expansion"

    leader = _safe_state_text(leadership.get("leader_type"), derive_leader_type(result))
    if (
        oi_build_quality in {"gradual_expansion", "early_expansion"}
        and leader in {"position_led", "consensus_led"}
        and not price_late
        and not ratio_alignment.get("overall_conflict", False)
    ):
        state = "true_early_bullish_signature"
        confidence = 0.78 if leader == "position_led" else 0.74
    elif oi_build_quality != "weak" and leader == "account_led":
        state = "account_led_early_signature"
        confidence = 0.62
    elif price_late:
        state = "late_revealed_signature"
        confidence = 0.42

    return {
        "state": state,
        "leader": leader,
        "oi_build_quality": oi_build_quality,
        "crowd_state": crowd_state,
        "spot_confirmation": spot_confirmation,
        "confidence": confidence,
    }


def classify_short_pressure_transition(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    post_flush_name = _safe_state_text(result.get("post_flush_pattern_name"), "")
    crowding_regime = safe_dict_from_api(result.get("crowding_regime") or features.get("crowding_regime"))
    buy_ratio = safe_float(features.get("recent_buy_ratio"), 0.0)
    oi_delta_1 = safe_float(features.get("oi_delta_1"), 0.0)
    oi_delta_3 = safe_float(features.get("oi_delta_3"), 0.0)

    pressure_state = "neutral_pressure"
    phase = "undetermined"
    continuation_potential = "medium"

    if post_flush_name == "BULLISH_REBUILD_AFTER_FLUSH":
        pressure_state = "true_short_squeeze"
        phase = "covering_then_new_demand"
        continuation_potential = "high"
    elif post_flush_name == "RELIEF_BOUNCE_AFTER_FLUSH":
        pressure_state = "ordinary_covering"
        phase = "covering_without_rebuild"
        continuation_potential = "low"
    elif crowding_regime.get("state") == "crowded_short" and buy_ratio >= EXECUTION_RULES["strong_taker_buy_ratio"]:
        pressure_state = "true_short_squeeze"
        phase = "active_short_pressure_release"
        continuation_potential = "medium_high"
    elif oi_delta_1 < 0 and oi_delta_3 < 0 and buy_ratio > 0.50:
        pressure_state = "ordinary_covering"
        phase = "shorts_closing_without_clean_rebuild"
        continuation_potential = "low_medium"
    elif buy_ratio >= EXECUTION_RULES["strong_taker_buy_ratio"] and oi_delta_3 >= 0 and result.get("acceptance_state") == "no":
        pressure_state = "fake_expansion_risk"
        phase = "flow_spike_without_acceptance"
        continuation_potential = "low"

    return {
        "pressure_state": pressure_state,
        "phase": phase,
        "continuation_potential": continuation_potential,
    }


def detect_acceptance_lifecycle(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    acceptance_state = _safe_state_text(result.get("acceptance_state"), "no")
    continuation_state = _safe_state_text(result.get("continuation_state"), "no")
    close_above = bool(features.get("close_above_breakout", False))
    hold_above = bool(features.get("hold_above_breakout_3bars", False))
    breakout_touched = bool(features.get("breakout_touched", False))
    higher_low_bias = bool(features.get("higher_low_bias", False))
    post_flush_name = _safe_state_text(result.get("post_flush_pattern_name"), "")

    acceptance_quality = "unaccepted"
    post_breakout_structure = "no_valid_lifecycle"
    verdict = "market_not_yet_living_above_breakout"

    if acceptance_state == "yes" and continuation_state == "yes":
        acceptance_quality = "clean_repricing"
        post_breakout_structure = "hold_retest_followthrough"
        verdict = "market_lives_above_breakout"
    elif acceptance_state == "yes":
        acceptance_quality = "fragile_acceptance"
        post_breakout_structure = "accepted_but_followthrough_limited"
        verdict = "market_above_breakout_but_fragile"
    elif acceptance_state == "partial" and (close_above or higher_low_bias or breakout_touched):
        acceptance_quality = "early_reacceptance"
        post_breakout_structure = "ignition_then_partial_reacceptance"
        verdict = "market_attempts_to_live_above_breakout"
    elif post_flush_name == "BULLISH_REBUILD_AFTER_FLUSH":
        acceptance_quality = "rebuild_reacceptance"
        post_breakout_structure = "post_flush_reclaim"
        verdict = "market_reclaims_after_flush_but_needs_more_hold"
    elif result.get("failure", {}).get("early_failure", False):
        acceptance_quality = "failed_acceptance"
        post_breakout_structure = "rejection_or_failed_hold"
        verdict = "market_rejected_the_new_price_zone"

    return {
        "acceptance_state": acceptance_state,
        "acceptance_quality": acceptance_quality,
        "post_breakout_structure": post_breakout_structure,
        "verdict": verdict,
        "hold_above_breakout": hold_above,
        "meaningful_close": close_above,
    }


def detect_flow_quality_mismatch(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    buy_ratio = safe_float(features.get("recent_buy_ratio"), 0.0)
    trade_exp = safe_float(features.get("trade_expansion_last"), 0.0)
    vol_exp = safe_float(features.get("vol_expansion_last"), 0.0)
    acceptance_state = _safe_state_text(result.get("acceptance_state"), "no")
    ratio_alignment = safe_dict_from_api(result.get("ratio_alignment") or features.get("ratio_alignment"))
    funding_context = _safe_state_text(result.get("funding_context"), features.get("funding_context", "unknown"))

    mismatch = False
    pattern = "healthy_or_neutral_flow"
    warning = ""

    if (
        buy_ratio >= EXECUTION_RULES["strong_taker_buy_ratio"]
        and trade_exp >= EXECUTION_RULES["min_trade_expansion"]
        and acceptance_state == "no"
    ):
        mismatch = True
        pattern = "ignition_without_followthrough"
        warning = "التدفق قوي لحظيًا لكن السعر لم يثبت قبولًا بنيويًا بعد."
    elif ratio_alignment.get("overall_conflict", False) and buy_ratio >= 0.53:
        mismatch = True
        pattern = "flow_against_alignment"
        warning = "التنفيذ الشرائي موجود لكن الفريمات والنسب لا تمنح توافقًا مريحًا."
    elif funding_context in {"crowded long side", "funding hostile"} and buy_ratio >= 0.56 and acceptance_state != "yes":
        mismatch = True
        pattern = "hot_funding_cold_structure"
        warning = "التمويل ساخن نسبيًا بينما البنية السعرية لم تتحول بعد إلى قبول واضح."
    elif vol_exp >= 1.35 and trade_exp >= 1.35 and acceptance_state == "partial":
        pattern = "flow_needs_confirmation"
        warning = "هناك ignition جيد، لكنه ما زال يحتاج قبولًا واستمرارًا أوضح قبل الثقة الكاملة."

    return {
        "mismatch": mismatch,
        "pattern": pattern,
        "warning": warning,
    }


def detect_post_flush_structures(result: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    precomputed = safe_dict_from_api(features.get("precomputed_post_flush_patterns") or result.get("post_flush_patterns"))
    relief = safe_dict_from_api(precomputed.get("relief_bounce"))
    bullish = safe_dict_from_api(precomputed.get("bullish_rebuild"))
    failed = safe_dict_from_api(precomputed.get("failed_rebuild"))

    pattern = "none"
    confidence = 0.0
    if bullish.get("active", False):
        pattern = "bullish_rebuild_after_flush"
        confidence = safe_float(bullish.get("score"), 0.0) / 10.0
    elif failed.get("active", False):
        pattern = "failed_rebuild_after_flush"
        confidence = safe_float(failed.get("score"), 0.0) / 10.0
    elif relief.get("active", False):
        pattern = "relief_bounce_after_flush"
        confidence = safe_float(relief.get("score"), 0.0) / 10.0

    return {
        "active": pattern != "none",
        "pattern": pattern,
        "confidence": clamp(confidence, 0.0, 1.0),
        "relief": relief,
        "bullish": bullish,
        "failed": failed,
    }

def build_structural_thesis(result: Dict[str, Any]) -> Dict[str, Any]:
    primary, alternatives, scores = derive_primary_hypothesis(result)
    primary_ar, alternatives_ar = derive_primary_hypothesis_ar(result)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_score = safe_float(ranked[0][1], 0.0) if ranked else 0.0
    second_score = safe_float(ranked[1][1], 0.0) if len(ranked) > 1 else 0.0
    gap = top_score - second_score
    conflict = derive_conflict_score(result)
    uncertainty = derive_uncertainty_score(result)

    thesis_quality = "fragile"
    thesis_quality_ar = "فرضية هشة"
    if top_score >= 0.70 and gap >= 0.12 and conflict <= 0.30:
        thesis_quality = "coherent"
        thesis_quality_ar = "فرضية متماسكة"
    elif top_score >= 0.55 and gap >= 0.06 and conflict <= 0.50:
        thesis_quality = "usable"
        thesis_quality_ar = "فرضية قابلة للاستخدام"
    elif top_score >= 0.45:
        thesis_quality = "contested"
        thesis_quality_ar = "فرضية متنازع عليها"

    summary_ar = (
        f"الفرضية الرئيسية هي {primary_ar} بدرجة ترجيح {format_num(top_score, 2)} "
        f"وفجوة {format_num(gap, 2)} عن أقرب بديل، مع تعارض {format_num(conflict, 2)} "
        f"وعدم يقين {format_num(uncertainty, 2)}."
    )

    return {
        "primary_hypothesis": primary,
        "primary_hypothesis_ar": primary_ar,
        "alternative_hypotheses": alternatives,
        "alternative_hypotheses_ar": alternatives_ar,
        "scores": scores,
        "top_score": round(top_score, 2),
        "second_score": round(second_score, 2),
        "gap": round(gap, 2),
        "thesis_quality": thesis_quality,
        "thesis_quality_ar": thesis_quality_ar,
        "summary_ar": summary_ar,
    }


def adversarial_thesis_review(result: Dict[str, Any], thesis: Dict[str, Any]) -> Dict[str, Any]:
    features = safe_dict_from_api(result.get("market_features"))
    ratio_alignment = safe_dict_from_api(result.get("ratio_alignment") or features.get("ratio_alignment"))
    acceptance_lifecycle = safe_dict_from_api(result.get("acceptance_lifecycle") or detect_acceptance_lifecycle(result))
    flow_mismatch = safe_dict_from_api(result.get("flow_quality_mismatch") or detect_flow_quality_mismatch(result))
    crowding_regime = safe_dict_from_api(result.get("crowding_regime") or features.get("crowding_regime"))
    funding_context = _safe_state_text(result.get("funding_context"), features.get("funding_context", "unknown"))
    primary = _safe_state_text(thesis.get("primary_hypothesis"), "undetermined")

    main_risk = "structural_ambiguity"
    main_risk_ar = "الغموض البنيوي ما زال مرتفعًا"
    trap_type = "none"
    trap_type_ar = "لا يوجد فخ واضح مهيمن"

    if flow_mismatch.get("mismatch", False):
        main_risk = "flow_trap"
        main_risk_ar = flow_mismatch.get("warning") or "التدفق أقوى من جودة البنية الحالية."
        trap_type = _safe_state_text(flow_mismatch.get("pattern"), "flow_trap")
        trap_type_ar = "فخ تدفق/اشتعال بلا استمرارية"
    elif ratio_alignment.get("overall_conflict", False):
        main_risk = "alignment_conflict"
        main_risk_ar = "الفريمات أو مكونات Acco/Posit/Ratio ليست متوافقة بما يكفي."
        trap_type = "structural_conflict"
        trap_type_ar = "فخ تعارض بنيوي"
    elif acceptance_lifecycle.get("acceptance_quality") in {"failed_acceptance", "unaccepted"}:
        main_risk = "acceptance_failure"
        main_risk_ar = "السوق لم يقبل المنطقة الجديدة بعد أو رفضها سريعًا."
        trap_type = "failed_breakout"
        trap_type_ar = "فخ اختراق فاشل"
    elif crowding_regime.get("state") in {"crowded_long_regime", "micro_crowded_long_regime"} and primary in {"upper_extreme_bullish_expansion", "consensus_bullish_expansion"}:
        main_risk = "late_crowding"
        main_risk_ar = "الازدحام الشرائي قد يكون متأخرًا ويزيد خطر الانقلاب من القمة."
        trap_type = "late_crowded_long"
        trap_type_ar = "فخ ازدحام شرائي متأخر"
    elif funding_context in {"crowded long side", "funding hostile"}:
        main_risk = "funding_overheat"
        main_risk_ar = "التمويل أصبح ساخنًا نسبيًا وقد يضغط على استمرارية الحركة."
        trap_type = "hot_funding_trap"
        trap_type_ar = "فخ تمويل ساخن"

    flip_condition = derive_invalidation_reason_ar(result)
    skepticism = derive_conflict_score(result)
    if flow_mismatch.get("mismatch", False):
        skepticism = clamp(skepticism + 0.12, 0.0, 0.98)

    return {
        "main_risk": main_risk,
        "main_risk_ar": main_risk_ar,
        "trap_type": trap_type,
        "trap_type_ar": trap_type_ar,
        "flip_condition": flip_condition,
        "skepticism_score": round(skepticism, 2),
    }


def build_execution_verdict(result: Dict[str, Any], thesis: Dict[str, Any], adversarial: Dict[str, Any]) -> Dict[str, Any]:
    primary = _safe_state_text(thesis.get("primary_hypothesis"), "undetermined")
    top_score = safe_float(thesis.get("top_score"), 0.0)
    thesis_quality = _safe_state_text(thesis.get("thesis_quality"), "fragile")
    features = safe_dict_from_api(result.get("market_features"))
    price_late = bool(features.get("price_late", False))
    flow_mismatch = safe_dict_from_api(result.get("flow_quality_mismatch"))
    acceptance_lifecycle = safe_dict_from_api(result.get("acceptance_lifecycle"))
    pre_rise_signature = safe_dict_from_api(result.get("pre_rise_signature"))
    short_pressure = safe_dict_from_api(result.get("short_pressure_transition"))
    post_flush = safe_dict_from_api(result.get("post_flush_structures"))
    leadership_model = safe_dict_from_api(result.get("leadership_model"))
    ratio_alignment = safe_dict_from_api((result.get("ratio_alignment") or features.get("ratio_alignment") or {}))
    conflict_score = derive_conflict_score(result)
    skepticism = safe_float(adversarial.get("skepticism_score"), 0.0)
    acceptance_quality = _safe_state_text(acceptance_lifecycle.get("acceptance_quality"), "unaccepted")
    price_verdict = _safe_state_text(acceptance_lifecycle.get("verdict"), "market_not_yet_living_above_breakout")
    leader_type = _safe_state_text(leadership_model.get("leader_type"), derive_leader_type(result))
    oi_supportive = bool(result.get("oi_supportive", False))
    flow_persistent = bool(leadership_model.get("structural_quality") == "high") or _safe_state_text(result.get("flow_quality"), "") in {"persistent", "supportive"}

    state = "discovered_not_actionable"
    state_ar = "مرصودة لكنها غير قابلة للتنفيذ بعد"
    reason = "البنية موجودة لكن التنفيذ لم يصل بعد إلى درجة النظافة المطلوبة."

    bearish_or_failure = {
        "upper_extreme_failed_acceptance",
        "long_crowding_rollover",
        "oi_expansion_without_bullish_confirmation",
        "failed_rebuild_after_flush",
        "relief_bounce_after_flush",
        "late_blowoff",
    }
    early_actionable_bullish = {
        "bullish_rebuild_after_flush",
        "lower_extreme_short_squeeze_up",
        "counterflow_short_squeeze_expansion",
    }
    high_conviction_bullish = early_actionable_bullish | {
        "consensus_bullish_expansion",
        "upper_extreme_bullish_expansion",
    }
    rebuild_wait_states = {"position_led_buildup", "account_led_accumulation", "consensus_bullish_expansion", "upper_extreme_bullish_expansion"}

    if price_late or primary == "late_blowoff":
        state = "late"
        state_ar = "متأخرة تنفيذيًا"
        reason = "الحركة أخذت الجزء الأنظف وأصبحت مطاردة أكثر من كونها فرصة مبكرة."
    elif primary in bearish_or_failure or post_flush.get("pattern") == "failed_rebuild_after_flush" or acceptance_quality == "failed_acceptance":
        state = "failed"
        state_ar = "فاشلة بنيويًا"
        reason = adversarial.get("main_risk_ar") or "البنية الحالية تميل إلى الفشل أو الانعكاس لا إلى استمرار صاعد نظيف."
    elif flow_mismatch.get("mismatch", False) and acceptance_quality not in {"clean_repricing", "rebuild_reacceptance"}:
        state = "no_trade_flow_trap"
        state_ar = "امتناع بسبب فخ تدفق"
        reason = flow_mismatch.get("warning") or "التدفق قوي لكنه غير مترجم إلى قبول واستمرار موثوقين."
    elif conflict_score >= 0.70 or skepticism >= 0.72 or ratio_alignment.get("overall_conflict", False):
        state = "no_trade_structural_conflict"
        state_ar = "امتناع بسبب تعارض بنيوي"
        reason = adversarial.get("main_risk_ar") or "هناك تعارض مرتفع بين الفريمات أو بين القيادة والبنية الحالية."
    elif (
        primary in high_conviction_bullish
        and top_score >= 0.58
        and thesis_quality in {"coherent", "usable"}
        and oi_supportive
        and acceptance_quality in {"clean_repricing", "rebuild_reacceptance"}
        and price_verdict in {"market_lives_above_breakout", "market_reclaims_after_flush_but_needs_more_hold"}
        and not price_late
        and not flow_mismatch.get("mismatch", False)
        and conflict_score <= 0.55
    ):
        state = "actionable_now"
        state_ar = "قابلة للتنفيذ الآن"
        reason = "الفرضية الرئيسية متماسكة، وOI يدعم، والسعر أعطى قبولًا بنيويًا صالحًا للتنفيذ الآن."
    elif (
        primary in early_actionable_bullish
        and top_score >= 0.54
        and thesis_quality in {"coherent", "usable"}
        and oi_supportive
        and acceptance_quality == "early_reacceptance"
        and price_verdict == "market_attempts_to_live_above_breakout"
        and leader_type in {"position_led", "counterflow_led", "consensus_led"}
        and (
            pre_rise_signature.get("state") in {"true_early_bullish_signature", "account_led_early_signature"}
            or short_pressure.get("pressure_state") == "true_short_squeeze"
            or post_flush.get("pattern") == "bullish_rebuild_after_flush"
        )
        and not flow_mismatch.get("mismatch", False)
        and conflict_score <= 0.45
        and skepticism <= 0.45
    ):
        state = "actionable_now"
        state_ar = "قابلة للتنفيذ الآن"
        reason = "هذه حالة قوية نظاميًا؛ القبول المبكر كافٍ هنا لأن النظام والـ OI والقيادة يؤكدون إعادة التسعير قبل اكتمال hold الكلاسيكي."
    elif (
        primary in early_actionable_bullish
        and acceptance_quality in {"early_reacceptance", "unaccepted", "fragile_acceptance"}
    ):
        state = "watch_for_acceptance"
        state_ar = "تنتظر قبولًا أوضح"
        reason = "الحالة البنيوية قوية لكن السوق لم يمنح بعد قبولًا كافيًا فوق المستوى المرجعي."
    elif (
        primary in rebuild_wait_states
        or pre_rise_signature.get("state") in {"true_early_bullish_signature", "developing_bullish_signature", "account_led_early_signature"}
        or short_pressure.get("pressure_state") == "true_short_squeeze"
    ):
        state = "watch_for_rebuild"
        state_ar = "تنتظر إعادة بناء/تأكيد"
        reason = "النظام البنيوي موجود لكن ما زال يحتاج زنادًا أو قبولًا أو استمرارًا أكثر وضوحًا."
    elif acceptance_quality in {"early_reacceptance", "fragile_acceptance"} or not flow_persistent:
        state = "discovered_not_actionable"
        state_ar = "مرصودة لكنها غير قابلة للتنفيذ بعد"
        reason = "هناك أجزاء صحيحة من الفرضية، لكن درجة القبول/الاستمرار ما زالت دون مستوى التنفيذ النظيف."

    return {
        "execution_state": state,
        "execution_state_ar": state_ar,
        "reason_ar": reason,
    }

def build_structural_casefile(result: Dict[str, Any]) -> StructuralCase:
    primary_ar, alternatives_ar = derive_primary_hypothesis_ar(result)
    primary_raw, alternatives_raw, _ = derive_primary_hypothesis(result)
    structural_thesis = safe_dict_from_api(result.get("structural_thesis")) or build_structural_thesis(result)
    adversarial_review = safe_dict_from_api(result.get("adversarial_review")) or adversarial_thesis_review(result, structural_thesis)
    execution_verdict = safe_dict_from_api(result.get("execution_verdict")) or build_execution_verdict(result, structural_thesis, adversarial_review)
    regime_pattern = _safe_state_text(result.get("regime_pattern"), "NEUTRAL_REGIME")
    regime_pattern_ar = translate_label(regime_pattern, ARABIC_REGIME_LABELS, "نظام غير محسوم")
    leadership_model = safe_dict_from_api(result.get("leadership_model")) or build_leadership_model(result)
    pre_rise_signature = safe_dict_from_api(result.get("pre_rise_signature")) or evaluate_pre_rise_signature(result)
    transition_story = safe_dict_from_api(result.get("transition_story")) or build_transition_story(result)
    acceptance_lifecycle = safe_dict_from_api(result.get("acceptance_lifecycle")) or detect_acceptance_lifecycle(result)
    flow_quality_mismatch = safe_dict_from_api(result.get("flow_quality_mismatch")) or detect_flow_quality_mismatch(result)
    short_pressure_transition = safe_dict_from_api(result.get("short_pressure_transition")) or classify_short_pressure_transition(result)

    leader_type = _safe_state_text(leadership_model.get("leader_type"), derive_leader_type(result))
    leader_type_ar = _safe_state_text(leadership_model.get("leader_type_ar"), derive_leader_type_ar(result))
    oi_role, oi_role_ar, oi_supportive = _derive_structural_oi_role(result)
    flow_quality, flow_quality_ar, flow_persistent = _derive_structural_flow_quality(result)
    price_verdict, price_verdict_ar = _derive_structural_price_verdict(result)
    if flow_quality_mismatch.get("mismatch", False):
        flow_quality = "mismatch"
        flow_quality_ar = flow_quality_mismatch.get("warning") or flow_quality_ar
        flow_persistent = False
    if acceptance_lifecycle.get("acceptance_quality") == "clean_repricing":
        price_verdict = "accepted_and_continuing"
        price_verdict_ar = "السوق قبل الحركة ويعيش فوق المستوى الجديد مع استمرارية معقولة."
    elif acceptance_lifecycle.get("acceptance_quality") == "rebuild_reacceptance":
        price_verdict = "rebuild_reacceptance"
        price_verdict_ar = "السعر يستعيد القبول بعد التفريغ لكن يحتاج تثبيتًا إضافيًا."

    evidence = list(safe_list_from_api(leadership_model.get("evidence")))
    evidence.extend([translate_label(_safe_state_text(s), {}, _safe_state_text(s)) for s in safe_list_from_api(transition_story.get("sequence")) if s])
    if result.get("acceptance_reason"):
        evidence.append(result.get("acceptance_reason"))
    if result.get("continuation_reason"):
        evidence.append(result.get("continuation_reason"))
    if safe_dict_from_api(result.get("ratio_alignment")).get("higher_tf_supportive", False):
        evidence.append("الفريمات الأعلى متوافقة مع الاتجاه الحالي")
    elif safe_dict_from_api(result.get("ratio_alignment")).get("overall_conflict", False):
        evidence.append("هناك تعارض نسبي بين الفريمات يحتاج حذرًا")

    invalidation_signals = []
    if adversarial_review.get("flip_condition"):
        invalidation_signals.append(_safe_state_text(adversarial_review.get("flip_condition"), ""))
    if adversarial_review.get("main_risk_ar"):
        invalidation_signals.append(_safe_state_text(adversarial_review.get("main_risk_ar"), ""))
    if not invalidation_signals:
        invalidation_signals = [derive_invalidation_reason_ar(result)]

    case = StructuralCase(
        symbol=_safe_state_text(result.get("symbol"), "UNKNOWN"),
        regime_pattern=regime_pattern,
        regime_pattern_ar=regime_pattern_ar,
        regime_confidence=max(safe_float(result.get("confidence_score"), 0.0), safe_float(pre_rise_signature.get("confidence"), 0.0)),
        leader_type=leader_type,
        leader_type_ar=leader_type_ar,
        leader_evidence=evidence,
        oi_role=oi_role,
        oi_role_ar=oi_role_ar,
        oi_supportive=oi_supportive,
        flow_quality=flow_quality,
        flow_quality_ar=flow_quality_ar,
        flow_persistent=flow_persistent,
        breakout_state=_safe_state_text(acceptance_lifecycle.get("post_breakout_structure"), _safe_state_text(result.get("trigger_pattern"), "NO_TRIGGER")),
        acceptance_state=_safe_state_text(acceptance_lifecycle.get("acceptance_quality"), "unaccepted"),
        price_verdict=price_verdict,
        price_verdict_ar=price_verdict_ar,
        primary_hypothesis=primary_raw,
        primary_hypothesis_ar=primary_ar,
        alternative_hypotheses=alternatives_raw,
        alternative_hypotheses_ar=alternatives_ar,
        invalidation_signals=invalidation_signals,
        next_failure_mode=_safe_state_text(adversarial_review.get("trap_type_ar"), derive_next_failure_mode_ar(result)),
        execution_state=_safe_state_text(execution_verdict.get("execution_state"), "discovered_not_actionable"),
        execution_state_ar=_safe_state_text(execution_verdict.get("execution_state_ar"), "مرصودة لكنها غير قابلة للتنفيذ بعد"),
        no_trade_reason=_safe_state_text(execution_verdict.get("reason_ar"), ""),
        causal_chain=[build_causal_chain_ar(result)],
        confidence_score=max(safe_float(result.get("confidence_score"), 0.0), safe_float(pre_rise_signature.get("confidence"), 0.0)),
        uncertainty_score=safe_float(result.get("uncertainty_score"), 0.0),
        conflict_score=safe_float(result.get("conflict_score"), 0.0),
        importance_score=safe_float(result.get("importance_score"), 0.0),
        thesis_quality=_safe_state_text(structural_thesis.get("thesis_quality"), ""),
        thesis_quality_ar=_safe_state_text(structural_thesis.get("thesis_quality_ar"), ""),
        thesis_confidence=safe_float(structural_thesis.get("top_score"), 0.0),
        adversarial_warning=_safe_state_text(adversarial_review.get("main_risk"), ""),
        adversarial_warning_ar=_safe_state_text(adversarial_review.get("main_risk_ar"), ""),
        adversarial_trap=_safe_state_text(adversarial_review.get("trap_type"), ""),
        adversarial_trap_ar=_safe_state_text(adversarial_review.get("trap_type_ar"), ""),
        flip_condition=_safe_state_text(adversarial_review.get("flip_condition"), ""),
    )
    return case

def structural_case_to_dict(case: StructuralCase) -> Dict[str, Any]:
    return {
        "symbol": case.symbol,
        "regime_pattern": case.regime_pattern,
        "regime_pattern_ar": case.regime_pattern_ar,
        "regime_confidence": case.regime_confidence,
        "leader_type": case.leader_type,
        "leader_type_ar": case.leader_type_ar,
        "leader_evidence": case.leader_evidence,
        "oi_role": case.oi_role,
        "oi_role_ar": case.oi_role_ar,
        "oi_supportive": case.oi_supportive,
        "flow_quality": case.flow_quality,
        "flow_quality_ar": case.flow_quality_ar,
        "flow_persistent": case.flow_persistent,
        "breakout_state": case.breakout_state,
        "acceptance_state": case.acceptance_state,
        "price_verdict": case.price_verdict,
        "price_verdict_ar": case.price_verdict_ar,
        "primary_hypothesis": case.primary_hypothesis,
        "primary_hypothesis_ar": case.primary_hypothesis_ar,
        "alternative_hypotheses": case.alternative_hypotheses,
        "alternative_hypotheses_ar": case.alternative_hypotheses_ar,
        "invalidation_signals": case.invalidation_signals,
        "next_failure_mode": case.next_failure_mode,
        "execution_state": case.execution_state,
        "execution_state_ar": case.execution_state_ar,
        "no_trade_reason": case.no_trade_reason,
        "causal_chain": case.causal_chain,
        "confidence_score": case.confidence_score,
        "uncertainty_score": case.uncertainty_score,
        "conflict_score": case.conflict_score,
        "importance_score": case.importance_score,
        "thesis_quality": case.thesis_quality,
        "thesis_quality_ar": case.thesis_quality_ar,
        "thesis_confidence": case.thesis_confidence,
        "adversarial_warning": case.adversarial_warning,
        "adversarial_warning_ar": case.adversarial_warning_ar,
        "adversarial_trap": case.adversarial_trap,
        "adversarial_trap_ar": case.adversarial_trap_ar,
        "flip_condition": case.flip_condition,
    }


def attach_structural_casefile(result: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(result)
    result["transition_story"] = build_transition_story(result)
    result["leadership_model"] = build_leadership_model(result)
    result["pre_rise_signature"] = evaluate_pre_rise_signature(result)
    result["short_pressure_transition"] = classify_short_pressure_transition(result)
    result["acceptance_lifecycle"] = detect_acceptance_lifecycle(result)
    result["flow_quality_mismatch"] = detect_flow_quality_mismatch(result)
    result["post_flush_structures"] = detect_post_flush_structures(result)
    result["structural_thesis"] = build_structural_thesis(result)
    result["adversarial_review"] = adversarial_thesis_review(result, result["structural_thesis"])
    result["execution_verdict"] = build_execution_verdict(result, result["structural_thesis"], result["adversarial_review"])
    case = build_structural_casefile(result)
    result["structural_case"] = structural_case_to_dict(case)
    return result


def rank_hypotheses(scores: Dict[str, float]) -> List[Tuple[str, float]]:
    return sorted(scores.items(), key=lambda item: item[1], reverse=True)


def select_primary_hypothesis(ranked: List[Tuple[str, float]]) -> Dict[str, Any]:
    if not ranked:
        return {"primary_hypothesis": "undetermined", "alternative_hypotheses": []}
    return {
        "primary_hypothesis": ranked[0][0],
        "alternative_hypotheses": [name for name, score in ranked[1:4] if score >= 0.18],
    }


def build_leader_reason(features: Dict[str, Any], leader_info: Dict[str, Any]) -> str:
    leader_type = _safe_state_text(leader_info.get("leader_type"), "undetermined")
    strength = safe_float(leader_info.get("strength"), 0.0)
    ratio_alignment = safe_dict_from_api(features.get("ratio_alignment"))
    oi_supportive = bool(features.get("oi_buildup_supported", False)) or safe_float(features.get("oi_delta_3"), 0.0) > 0.0
    flow_supported = bool(features.get("flow_supported", False))
    return (
        f"leader={leader_type} | strength={strength:.2f} | "
        f"ratio_supportive={ratio_alignment.get('overall_supportive', False)} | "
        f"oi_supportive={oi_supportive} | flow_supported={flow_supported}"
    )


def enrich_result_v2_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(result)
    features = safe_dict_from_api(result.get("market_features"))
    leadership_model = safe_dict_from_api(result.get("leadership_model"))
    case_similarity = safe_dict_from_api(result.get("case_similarity_context"))
    state_transition = safe_dict_from_api(result.get("state_transition_context"))
    thesis = safe_dict_from_api(result.get("structural_thesis"))

    primary_raw, alternatives_raw, hypothesis_scores = derive_primary_hypothesis(result)
    ranked = rank_hypotheses(hypothesis_scores)
    selected = select_primary_hypothesis(ranked)

    result["leader_type"] = _safe_state_text(leadership_model.get("leader_type"), derive_leader_type(result))
    result["leader_reason"] = build_leader_reason(features, leadership_model)
    result["primary_hypothesis"] = selected.get("primary_hypothesis", primary_raw)
    result["alternative_hypotheses"] = selected.get("alternative_hypotheses", alternatives_raw)
    result["hypothesis_scores"] = hypothesis_scores
    result["confidence_score"] = derive_confidence_score(result)
    result["uncertainty_score"] = derive_uncertainty_score(result)
    result["conflict_score"] = derive_conflict_score(result)
    causal_text = build_causal_chain_ar(result)
    result["causal_chain"] = [causal_text] if causal_text else []
    result["invalidation_reason"] = _safe_state_text(result.get("invalidation_reason"), derive_invalidation_reason_ar(result))
    result["next_failure_mode"] = _safe_state_text(result.get("next_failure_mode"), derive_next_failure_mode_ar(result))
    result["state_transition"] = state_transition.get("transition_key")
    result["state_transition_reason"] = state_transition.get("transition_ar", "")

    top_cases = safe_list_from_api(case_similarity.get("top_cases"))
    if top_cases:
        first = safe_dict_from_api(top_cases[0])
        result["closest_case_match"] = first.get("primary_hypothesis") or first.get("primary_hypothesis_ar")
        result["closest_case_score"] = safe_float(first.get("similarity"), 0.0)
    else:
        result["closest_case_match"] = None
        result["closest_case_score"] = 0.0

    result["hypothesis_inputs"] = {
        "top_thesis_score": safe_float(thesis.get("top_score"), 0.0),
        "thesis_quality": _safe_state_text(thesis.get("thesis_quality"), "unknown"),
    }
    return result


def build_arabic_output_fields(result: Dict[str, Any]) -> Dict[str, Any]:
    primary_ar, alternatives_ar = derive_primary_hypothesis_ar(result)
    primary_raw, alternatives_raw, hypothesis_scores = derive_primary_hypothesis(result)
    conflict_score = derive_conflict_score(result)
    uncertainty_score = derive_uncertainty_score(result)
    confidence_score = derive_confidence_score(result)
    leader_type_raw = derive_leader_type(result)
    return {
        "family_ar": translate_label(result.get("family"), ARABIC_FAMILY_LABELS, "غير محسومة"),
        "stage_ar": translate_label(result.get("stage"), ARABIC_STAGE_LABELS, "غير معروفة"),
        "oi_state_ar": translate_label(result.get("oi_state"), ARABIC_OI_STATE_LABELS, "غير معروفة"),
        "final_bucket_ar": translate_label(result.get("final_bucket"), ARABIC_BUCKET_LABELS, "غير معروفة"),
        "regime_pattern_ar": translate_label(result.get("regime_pattern"), ARABIC_REGIME_LABELS, "نظام غير محسوم"),
        "trigger_pattern_ar": translate_label(result.get("trigger_pattern"), ARABIC_TRIGGER_LABELS, "زناد غير محسوم"),
        "execution_pattern_ar": translate_label(result.get("execution_pattern"), ARABIC_EXECUTION_LABELS, "تنفيذ غير محسوم"),
        "funding_context_ar": translate_label(result.get("funding_context"), ARABIC_FUNDING_LABELS, "غير معروف"),
        "basis_context_ar": translate_label(result.get("basis_context"), ARABIC_BASIS_LABELS, "غير معروف"),
        "crowding_regime_ar": translate_label(((result.get("crowding_regime") or {}).get("state")), ARABIC_CROWDING_STATE_LABELS, "غير معروف"),
        "post_flush_pattern_ar": translate_label(result.get("post_flush_pattern_name"), ARABIC_POST_FLUSH_LABELS, "لا يوجد"),
        "leader_type": leader_type_raw,
        "leader_type_ar": derive_leader_type_ar(result),
        "primary_hypothesis": primary_raw,
        "primary_hypothesis_ar": primary_ar,
        "alternative_hypotheses": alternatives_raw,
        "alternative_hypotheses_ar": alternatives_ar,
        "hypothesis_scores": hypothesis_scores,
        "confidence_score": confidence_score,
        "uncertainty_score": uncertainty_score,
        "conflict_score": conflict_score,
        "causal_chain_ar": build_causal_chain_ar(result),
        "invalidation_reason_ar": derive_invalidation_reason_ar(result),
        "next_failure_mode_ar": derive_next_failure_mode_ar(result),
        "structural_thesis_ar": safe_dict_from_api(result.get("structural_thesis")).get("summary_ar", "لا توجد أطروحة بنيوية مكتملة بعد."),
        "thesis_quality_ar": safe_dict_from_api(result.get("structural_thesis")).get("thesis_quality_ar", "غير محسومة"),
        "thesis_confidence": safe_dict_from_api(result.get("structural_thesis")).get("top_score", 0.0),
        "adversarial_warning_ar": safe_dict_from_api(result.get("adversarial_review")).get("main_risk_ar", "لا يوجد اعتراض مضاد بارز بعد."),
        "adversarial_trap_ar": safe_dict_from_api(result.get("adversarial_review")).get("trap_type_ar", "لا يوجد فخ واضح مهيمن"),
        "flip_condition_ar": safe_dict_from_api(result.get("adversarial_review")).get("flip_condition", "لا يوجد شرط قلب واضح بعد."),
        "structural_execution_state_ar": safe_dict_from_api(result.get("execution_verdict")).get("execution_state_ar", "غير محسوم"),
        "structural_execution_reason_ar": safe_dict_from_api(result.get("execution_verdict")).get("reason_ar", "لا يوجد تفسير تنفيذي مستقل بعد."),
        "asset_memory_context_ar": safe_dict_from_api(result.get("asset_memory_context")).get("summary", "لا توجد ذاكرة محلية كافية لهذا الأصل بعد."),
        "asset_change_state_ar": safe_dict_from_api(result.get("asset_memory_context")).get("change_state_ar", "لا توجد ذاكرة محلية كافية لهذا الأصل بعد."),
        "state_transition_ar": safe_dict_from_api(result.get("state_transition_context")).get("transition_ar", "لا توجد انتقالات حالة محفوظة لهذا الأصل بعد."),
        "case_similarity_ar": safe_dict_from_api(result.get("case_similarity_context")).get("summary_ar", "لا توجد حالات مرجعية مشابهة محفوظة لهذا الأصل بعد."),
        "behavior_profile_ar": safe_dict_from_api(result.get("behavior_profile_context")).get("summary_ar", "لا توجد بعد ذاكرة سلوكية محلية كافية لهذا الأصل."),
        "behavior_fit_ar": safe_dict_from_api(result.get("behavior_profile_context")).get("fit_ar", "طبع الأصل ما زال قيد البناء."),
    }


# ============================================================
# الطباعة النهائية العربية
# ============================================================


def print_symbol_result(result: Dict[str, Any]) -> None:
    view = build_arabic_output_fields(result)
    execution_verdict = safe_dict_from_api(result.get("execution_verdict"))
    structural_state = _safe_state_text(result.get("structural_execution_state"), execution_verdict.get("execution_state", "unknown"))
    structural_state_ar = view.get("structural_execution_state_ar", "غير محسوم")
    actionable_now = structural_state == "actionable_now"
    symbol_text = _safe_state_text(result.get("symbol"), "UNKNOWN")
    symbol_text = colorize_text(symbol_text, "green") if actionable_now else symbol_text
    state_text = colorize_text(structural_state_ar, "green") if actionable_now else structural_state_ar

    alternatives = view.get("alternative_hypotheses_ar") or []
    closest_case = result.get("closest_case_match") or "لا توجد حالة قريبة محفوظة بعد"
    closest_score = format_num(result.get("closest_case_score"), 2)

    print(STATIC_SETTINGS["PRINT_HORIZONTAL_LINE"])
    print("الملخص")
    print(f"- الرمز: {symbol_text}")
    print(f"- الأهمية: {format_num(result.get('importance_score'), 2)}")
    print(f"- السلة النهائية: {view.get('final_bucket_ar', result.get('final_bucket', 'غير معروف'))}")
    print(f"- القرار التنفيذي: {state_text}")

    print("\nالقراءة البنيوية")
    print(f"- النظام: {view.get('regime_pattern_ar', 'غير معروف')}")
    print(f"- الزناد: {view.get('trigger_pattern_ar', 'غير معروف')}")
    print(f"- حالة التنفيذ: {view.get('execution_pattern_ar', 'غير معروف')}")
    print(f"- القيادة: {view.get('leader_type_ar', 'غير معروف')}")
    print(f"- نظام الازدحام: {view.get('crowding_regime_ar', 'غير معروف')}")
    print(f"- الفرضية الرئيسية: {view.get('primary_hypothesis_ar', 'غير محسومة')}")
    print(f"- الفرضيات البديلة: {', '.join(alternatives) if alternatives else 'لا توجد بدائل قريبة'}")

    print("\nتسلسل البحث")
    print(f"- السياق: {view.get('regime_pattern_ar', 'غير معروف')}")
    print(f"- القيادة: {view.get('leader_type_ar', 'غير معروف')}")
    print(f"- OI: {view.get('oi_state_ar', 'غير معروف')}")
    print(f"- التدفق: {_safe_state_text(result.get('flow_quality'), 'غير محسوم')}")
    print(f"- الزناد: {view.get('trigger_pattern_ar', 'غير معروف')}")
    print(f"- القبول: {_safe_state_text(result.get('acceptance_reason'), 'غير محسوم')}")
    print(f"- الفرضية: {view.get('primary_hypothesis_ar', 'غير محسومة')}")
    print(f"- القرار: {state_text}")
    print(f"- الإبطال: {view.get('invalidation_reason_ar', 'لا يوجد إبطال محدد بعد')}")

    print("\nلماذا")
    print(f"- السلسلة السببية: {view.get('causal_chain_ar', 'لا توجد سلسلة سببية جاهزة بعد')}")
    print(f"- سبب القبول: {_safe_state_text(result.get('acceptance_reason'), 'غير متاح')}")
    print(f"- سبب الاستمرار: {_safe_state_text(result.get('continuation_reason'), 'غير متاح')}")

    print("\nالمخاطر")
    print(f"- الثقة: {format_num(view.get('confidence_score'), 2)}")
    print(f"- عدم اليقين: {format_num(view.get('uncertainty_score'), 2)}")
    print(f"- التعارض: {format_num(view.get('conflict_score'), 2)}")
    print(f"- سبب الإبطال: {view.get('invalidation_reason_ar', 'لا يوجد إبطال محدد بعد')}")
    print(f"- نمط الفشل التالي: {view.get('next_failure_mode_ar', 'غير محدد بعد')}")

    print("\nالذاكرة")
    print(f"- أقرب حالة: {closest_case} (الدرجة={closest_score})")
    print(f"- الانتقال من الحالة السابقة: {_safe_state_text(result.get('state_transition_reason'), view.get('state_transition_ar', 'غير متاح'))}")

def print_cycle_report(report: Dict[str, Any]) -> None:
    print("\n" + STATIC_SETTINGS["PRINT_HORIZONTAL_LINE"])
    print("ماسح فرص Binance Futures USDT-M")
    print(STATIC_SETTINGS["PRINT_HORIZONTAL_LINE"])
    print(f"بداية الدورة: {report['started_at']}")
    print(f"نهاية الدورة:  {report['finished_at']}")
    print("\nملخص التحضير:")
    prep = report["prep_stats"]
    print(f"- حجم Universe: {prep['universe_size']}")
    print(f"- Snapshot ticker24h: {prep['snapshot_tickers']}")
    print(f"- Snapshot mark price: {prep['snapshot_mark']}")
    print(f"- وضع الفحص الشامل: {'مفعّل' if prep.get('full_scan_mode') else 'غير مفعّل'}")
    print(f"- مجتازة للتصفية/الكون: {prep['quick_filter_passed']}")
    print(f"- رموز التحليل العميق الفعلية: {prep['deep_candidates']}")

    print("\nالملخص البنيوي للدورة:")
    summary = report["summary"]
    printed_results = report.get("printed_results", [])
    magma_only = OUTPUT_SETTINGS.get("PRINT_MAGMA_PATTERN_ONLY", False)
    print(f"- actionable_now: {summary.get('structural::actionable_now', 0)}")
    print(f"- watch_for_acceptance: {summary.get('structural::watch_for_acceptance', 0)}")
    print(f"- watch_for_rebuild: {summary.get('structural::watch_for_rebuild', 0)}")
    print(f"- discovered_not_actionable: {summary.get('structural::discovered_not_actionable', 0)}")
    print(f"- no_trade_flow_trap: {summary.get('structural::no_trade_flow_trap', 0)}")
    print(f"- no_trade_structural_conflict: {summary.get('structural::no_trade_structural_conflict', 0)}")
    print(f"- late: {summary.get('structural::late', 0)}")
    print(f"- failed: {summary.get('structural::failed', 0)}")

    if OUTPUT_SETTINGS.get("SHOW_LEGACY_SUMMARY", False):
        print("\nملخص التصنيف القديم (للمقارنة فقط):")
        print(f"- قابلة للتنفيذ الآن: {summary.get('Actionable now', 0)}")
        print(f"- مكتشفة لكن غير قابلة للتنفيذ الآن: {summary.get('Discovered but not actionable', 0)}")
        print(f"- متأخرة: {summary.get('Late', 0)}")
        print(f"- فاشلة: {summary.get('Failed', 0)}")

    if magma_only:
        magma_total = sum(
            1 for row in report.get("results", [])
            if ((row.get("market_features") or {}).get("counterflow_pattern") or {}).get("active", False)
        )
        print(f"- مرشحات MAGMA-like المكتشفة في الدورة: {magma_total}")
        print(f"- المطبوعة بعد الفلترة والترتيب: {len(printed_results)}")

    title = "\nالتقارير البنيوية المطبوعة بعد الفلترة والترتيب:"
    if magma_only:
        title = "\nالتقارير البنيوية ذات بصمة MAGMA-like المطبوعة باللون الأحمر:"
    elif OUTPUT_SETTINGS.get("PRINT_IMPORTANT_ONLY", False):
        title = "\nالتقارير البنيوية المهمة المطبوعة:"
    print(title)

    if not printed_results:
        print("- لا توجد نتائج مطابقة للفلتر الحالي في هذه الدورة")
    for row in printed_results:
        print_symbol_result(row)

    market_context = safe_dict_from_api(report.get("market_context"))
    if market_context.get("available", False):
        print("\nسياق السوق العام لهذه الدورة:")
        print(f"- {market_context.get('market_regime_ar', 'غير متاح')}")
        if market_context.get("summary_ar"):
            print(f"- {market_context.get('summary_ar', '')}")
    print(STATIC_SETTINGS["PRINT_HORIZONTAL_LINE"])

# ============================================================
# نقطة التشغيل
# ============================================================


def main() -> int:
    client = BinanceFuturesPublicClient()

    loop_enabled = DYNAMIC_SETTINGS.get("SCAN_LOOP_ENABLED", False)
    interval_sec = safe_int(DYNAMIC_SETTINGS.get("SCAN_LOOP_INTERVAL_SEC", 120), 120)

    while True:
        report = scan_once(client)
        print_cycle_report(report)

        if not loop_enabled:
            break

        print(f"\n[INFO] انتظار {interval_sec} ثانية قبل الدورة التالية...\n")
        time.sleep(interval_sec)

    return 0



# ============================================================
# توسعات عليا: سياق السوق + ليكويداشن/امتصاص تقريبي + ذاكرة تشابه بين الأصول
# ============================================================

_GLOBAL_CASE_LIBRARY_FILENAME = "_global_case_library.json"


def _global_case_library_path() -> str:
    base_dir = DYNAMIC_SETTINGS.get("ASSET_MEMORY_DIR", "asset_memory_store")
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, _GLOBAL_CASE_LIBRARY_FILENAME)


def load_global_case_library() -> Dict[str, Any]:
    data = load_json_file(_global_case_library_path(), {"cases": []})
    if not isinstance(data, dict):
        return {"cases": []}
    data["cases"] = safe_list_from_api(data.get("cases"))
    return data


def save_global_case_library(data: Dict[str, Any]) -> None:
    save_json_atomic(_global_case_library_path(), data)


def update_global_case_library(result: Dict[str, Any]) -> None:
    try:
        if not DYNAMIC_SETTINGS.get("ENABLE_ASSET_LOCAL_MEMORY", True):
            return
        features = safe_dict_from_api(result.get("market_features"))
        if not features:
            return
        library = load_global_case_library()
        cases = safe_list_from_api(library.get("cases"))
        entry = {
            "timestamp": utc_now_iso(),
            "symbol": result.get("symbol"),
            "primary_hypothesis": result.get("primary_hypothesis"),
            "primary_hypothesis_ar": result.get("primary_hypothesis_ar"),
            "final_bucket": result.get("final_bucket"),
            "final_bucket_ar": result.get("final_bucket_ar"),
            "leader_type": result.get("leader_type"),
            "leader_type_ar": result.get("leader_type_ar"),
            "importance_score": safe_float(result.get("importance_score"), 0.0),
            "confidence_score": safe_float(result.get("confidence_score"), 0.0),
            "vector": build_asset_case_vector(features, result),
        }
        cases.append(entry)
        max_cases = 1200
        library["cases"] = cases[-max_cases:]
        save_global_case_library(library)
    except Exception:
        pass


def build_cross_asset_similarity_context(features: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    library = load_global_case_library()
    cases = safe_list_from_api(library.get("cases"))
    if not cases:
        return {
            "available": False,
            "summary_ar": "لا توجد بعد مكتبة حالات مشتركة بين الأصول للمقارنة العرضية.",
            "top_cases": [],
        }
    current_symbol = _safe_state_text(result.get("symbol"), "")
    current_vector = build_asset_case_vector(features, result)
    scored = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        if _safe_state_text(case.get("symbol"), "") == current_symbol:
            continue
        sim = _compute_case_similarity_score(current_vector, safe_dict_from_api(case.get("vector")))
        enriched = dict(case)
        enriched["similarity"] = sim
        scored.append(enriched)
    if not scored:
        return {
            "available": False,
            "summary_ar": "لا توجد حالات مشابهة من أصول أخرى داخل المكتبة الحالية.",
            "top_cases": [],
        }
    scored.sort(key=lambda x: (safe_float(x.get("similarity"), 0.0), safe_float(x.get("importance_score"), 0.0)), reverse=True)
    top_cases = scored[:3]
    parts = []
    for case in top_cases:
        symbol = _safe_state_text(case.get("symbol"), "?")
        sim = safe_float(case.get("similarity"), 0.0)
        hyp = _safe_state_text(case.get("primary_hypothesis_ar") or case.get("primary_hypothesis"), "غير معروف")
        bucket = _safe_state_text(case.get("final_bucket_ar") or case.get("final_bucket"), "غير معروف")
        parts.append(f"أقرب أصل مشابه: {symbol} بدرجة {sim:.2f}، وكانت فرضيته {hyp} وانتهت حالته إلى {bucket}")
    return {
        "available": True,
        "summary_ar": " | ".join(parts),
        "top_cases": top_cases,
    }


def _fetch_reference_asset_context(client: BinanceFuturesPublicClient, symbol: str) -> Dict[str, Any]:
    try:
        raw_5m = safe_list_from_api(client.get_klines(symbol, "5m", 36))
        raw_1h = safe_list_from_api(client.get_klines(symbol, "1h", 36))
        bars_5m = parse_klines(raw_5m)
        bars_1h = parse_klines(raw_1h)
        raw_oi_5m = safe_list_from_api(client.get_open_interest_hist(symbol, "5m", 24))
        raw_oi_1h = safe_list_from_api(client.get_open_interest_hist(symbol, "1h", 24))
        oi_5m = [safe_float(row.get("sumOpenInterestValue")) for row in raw_oi_5m if isinstance(row, dict)]
        oi_1h = [safe_float(row.get("sumOpenInterestValue")) for row in raw_oi_1h if isinstance(row, dict)]
        if len(bars_5m) < 6 or len(bars_1h) < 6:
            return {"symbol": symbol, "available": False}
        ret_5m = pct_change(bars_5m[-1]["close"], bars_5m[-4]["close"])
        ret_1h = pct_change(bars_1h[-1]["close"], bars_1h[-4]["close"])
        oi_5m_delta = pct_change(oi_5m[-1], oi_5m[-4]) if len(oi_5m) >= 4 else 0.0
        oi_1h_delta = pct_change(oi_1h[-1], oi_1h[-4]) if len(oi_1h) >= 4 else 0.0
        buy_ratio = avg([b.get("taker_buy_ratio", 0.0) for b in bars_5m[-3:]], 0.0)
        return {
            "symbol": symbol,
            "available": True,
            "ret_5m": ret_5m,
            "ret_1h": ret_1h,
            "oi_5m_delta": oi_5m_delta,
            "oi_1h_delta": oi_1h_delta,
            "buy_ratio": buy_ratio,
        }
    except Exception:
        return {"symbol": symbol, "available": False}


def build_market_regime_context(client: BinanceFuturesPublicClient) -> Dict[str, Any]:
    btc = _fetch_reference_asset_context(client, "BTCUSDT")
    eth = _fetch_reference_asset_context(client, "ETHUSDT")
    if not btc.get("available") and not eth.get("available"):
        return {
            "available": False,
            "market_regime": "unknown",
            "market_regime_ar": "سياق السوق العام غير متاح حاليًا.",
            "summary_ar": "تعذر بناء سياق عام من BTC وETH في هذه الدورة.",
            "risk_bias": 0.0,
        }
    positives = 0
    negatives = 0
    oi_pos = 0
    oi_neg = 0
    for ref in (btc, eth):
        if not ref.get("available"):
            continue
        if safe_float(ref.get("ret_1h"), 0.0) > 0 and safe_float(ref.get("buy_ratio"), 0.0) >= 0.50:
            positives += 1
        if safe_float(ref.get("ret_1h"), 0.0) < 0 and safe_float(ref.get("buy_ratio"), 0.0) < 0.50:
            negatives += 1
        if safe_float(ref.get("oi_1h_delta"), 0.0) > 0:
            oi_pos += 1
        if safe_float(ref.get("oi_1h_delta"), 0.0) < 0:
            oi_neg += 1

    market_regime = "mixed"
    market_regime_ar = "السوق العام مختلط بين شهية المخاطرة والحذر."
    risk_bias = 0.0
    if positives >= 2 and oi_pos >= 1:
        market_regime = "risk_on"
        market_regime_ar = "السوق العام يميل إلى وضعية شهية مخاطرة داعمة للحركات الصاعدة."
        risk_bias = 0.12
    elif negatives >= 2 and oi_neg >= 1:
        market_regime = "risk_off"
        market_regime_ar = "السوق العام يميل إلى وضعية نفور من المخاطرة وضغط على الحركات الصاعدة."
        risk_bias = -0.12
    elif positives >= 1 and negatives == 0:
        market_regime = "mild_risk_on"
        market_regime_ar = "السوق العام يميل إيجابيًا لكن دون إجماع قوي كامل."
        risk_bias = 0.06
    elif negatives >= 1 and positives == 0:
        market_regime = "mild_risk_off"
        market_regime_ar = "السوق العام يميل سلبيًا لكن دون إجماع حاسم كامل."
        risk_bias = -0.06

    summary_ar = (
        f"BTC: عائد 1h = {format_pct(btc.get('ret_1h'))}، OI 1h = {format_pct(btc.get('oi_1h_delta'))}، buy ratio = {format_num(btc.get('buy_ratio'), 3)} | "
        f"ETH: عائد 1h = {format_pct(eth.get('ret_1h'))}، OI 1h = {format_pct(eth.get('oi_1h_delta'))}، buy ratio = {format_num(eth.get('buy_ratio'), 3)} | "
        f"الخلاصة: {market_regime_ar}"
    )
    return {
        "available": True,
        "market_regime": market_regime,
        "market_regime_ar": market_regime_ar,
        "summary_ar": summary_ar,
        "risk_bias": risk_bias,
        "btc": btc,
        "eth": eth,
    }


def detect_liquidation_absorption_proxy(features: Dict[str, Any], result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    bars = safe_list_from_api(features.get("bars_5m"))
    if len(bars) < 3:
        return {
            "state": "unknown",
            "state_ar": "لا توجد بيانات كافية لبناء قراءة تقريبية للسيولة/التصفية.",
            "summary_ar": "بيانات 5m غير كافية.",
        }
    last = safe_dict_from_api(bars[-1])
    high = safe_float(last.get("high"), 0.0)
    low = safe_float(last.get("low"), 0.0)
    close = safe_float(last.get("close"), 0.0)
    open_ = safe_float(last.get("open"), close)
    candle_range = max(high - low, 1e-12)
    upper_wick_ratio = safe_div(max(high - max(close, open_), 0.0), candle_range, 0.0)
    lower_wick_ratio = safe_div(max(min(close, open_) - low, 0.0), candle_range, 0.0)
    oi_1 = safe_float(features.get("oi_delta_1"), 0.0)
    oi_3 = safe_float(features.get("oi_delta_3"), 0.0)
    ret_1 = safe_float(features.get("ret_1"), 0.0)
    ret_3 = safe_float(features.get("ret_3"), 0.0)
    buy_ratio = safe_float(features.get("recent_buy_ratio"), 0.0)
    trade_exp = safe_float(features.get("trade_expansion_last"), 0.0)
    acceptance_state = _safe_state_text((result or {}).get("acceptance_state"), "no")
    ratio_alignment = safe_dict_from_api((result or {}).get("ratio_alignment") or features.get("ratio_alignment"))

    state = "balanced"
    state_ar = "القراءة التقريبية للسيولة متوازنة دون بصمة تصفية حادة واضحة."
    if ret_1 > 0.35 and (oi_1 < 0 or oi_3 < 0):
        state = "short_covering_lift"
        state_ar = "الصعود الحالي يحمل بصمة تغطية شورت/تصفية مراكز قصيرة أكثر من كونه بناء لونغات جديدة خالصًا."
    elif ret_1 < -0.35 and (oi_1 < 0 or oi_3 < 0):
        state = "long_liquidation_flush"
        state_ar = "الهبوط الحالي يحمل بصمة تصفية لونغات/تفريغ انكشاف أكثر من كونه بناء شورتات هادئ فقط."
    elif ret_1 > 0.35 and oi_1 > 0 and buy_ratio >= 0.54 and trade_exp >= 1.10:
        state = "aggressive_long_build"
        state_ar = "هناك بناء لونغات هجومي جديد مدعوم بتوسع OI وتدفّق شراء تنفيذي."
    elif ret_3 > 0.60 and oi_3 > 0 and acceptance_state != "yes" and not ratio_alignment.get("overall_supportive", False):
        state = "counter_positioning_risk"
        state_ar = "السعر يصعد مع توسع OI لكن من دون قبول كافٍ أو دعم نسبي مريح، ما يرجح إعادة تموضع مضادة أو ضغط انعكاسي لاحق."
    elif upper_wick_ratio >= 0.45 and ret_3 > 0.0:
        state = "overhead_absorption"
        state_ar = "يوجد امتصاص علوي/رفض من الأعلى، ما يوحي بأن السوق يواجه بيعًا عند المنطقة الحالية."
    elif lower_wick_ratio >= 0.45 and ret_3 < 0.0:
        state = "downside_absorption"
        state_ar = "يوجد امتصاص سفلي/رفض للهبوط، ما يوحي بوجود مشترٍ يدافع عن المنطقة الحالية."

    summary_ar = (
        f"العائد 1 بار = {format_pct(ret_1)}، العائد 3 أشرطة = {format_pct(ret_3)}، OIΔ1 = {format_pct(oi_1)}، OIΔ3 = {format_pct(oi_3)}، "
        f"الذيل العلوي = {format_num(upper_wick_ratio, 2)}، الذيل السفلي = {format_num(lower_wick_ratio, 2)}. {state_ar}"
    )
    return {
        "state": state,
        "state_ar": state_ar,
        "summary_ar": summary_ar,
        "upper_wick_ratio": upper_wick_ratio,
        "lower_wick_ratio": lower_wick_ratio,
    }


def apply_market_context_influence(result: Dict[str, Any]) -> Dict[str, Any]:
    market_context = safe_dict_from_api(result.get("market_context"))
    if not market_context.get("available", False):
        return result
    bias = safe_float(market_context.get("risk_bias"), 0.0)
    hyp = _safe_state_text(result.get("primary_hypothesis"), "unknown")
    bucket = _safe_state_text(result.get("final_bucket"), "Failed")
    bullish_hypotheses = {
        "lower_extreme_short_squeeze_up",
        "position_led_buildup",
        "account_led_accumulation",
        "consensus_bullish_expansion",
        "upper_extreme_bullish_expansion",
        "counterflow_short_squeeze_expansion",
        "bullish_rebuild_after_flush",
    }
    bearish_hypotheses = {
        "upper_extreme_failed_acceptance",
        "long_crowding_rollover",
        "oi_expansion_without_bullish_confirmation",
        "failed_rebuild_after_flush",
        "late_blowoff",
    }
    conf = safe_float(result.get("confidence_score"), 0.0)
    unc = safe_float(result.get("uncertainty_score"), 0.0)
    imp = safe_float(result.get("importance_score"), 0.0)
    if hyp in bullish_hypotheses:
        conf += max(bias, 0.0)
        unc += max(-bias, 0.0) * 0.6
        imp += bias * 55.0
    elif hyp in bearish_hypotheses or bucket == "Failed":
        conf += max(-bias, 0.0)
        unc += max(bias, 0.0) * 0.6
        imp -= bias * 35.0
    result["confidence_score"] = round(clamp(conf, 0.0, 1.0), 3)
    result["uncertainty_score"] = round(clamp(unc, 0.0, 1.0), 3)
    result["importance_score"] = round(max(0.0, imp), 2)
    return result

if __name__ == "__main__":
    sys.exit(main())
