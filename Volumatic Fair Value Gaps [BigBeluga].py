import math
import time
import json
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

# =========================
# SETTINGS (1h baseline)
# =========================
EXCHANGE_ID = "binanceusdm"
TIMEFRAME = "1h"         # <-- طلبك
BASE_CANDLES_1H = 300    # على 1h: 300 شمعة => تحتاج ~18000 شمعة 1m (معقول على الموبايل)
SYMBOLS = None           # None = كل USDT-M
POLL_SECONDS = 60

# Touch detection (last N candles)
TOUCH_LOOKBACK = 10
PRINT_TOUCH_EVENTS = True
SHOW_ONLY_TOUCH_EVENTS = True

# Pine inputs
PARAMS = {
    "mitigationSrc": "close",   # "close" أو "high/low"
    "bullGaps": True,
    "bearGaps": True,
    "volumeBars": True,

    # ثوابت من المؤشر
    "percentile_len": 1000,
    "filter_threshold": 10,
    "max_fvgs": 10,
    "min_bar_index": 100,  # bar_index > 100 gate
}

# مخرجات منظمة
PRINT_EVENTS = True          # إنشاء/حذف
PRINT_ACTIVE_SUMMARY = True  # ملخص الفجوات النشطة آخر بار
ACTIVE_TOP_N = 8
PRINT_SYMBOL_DONE = True     # اطبع عند انتهاء تحليل كل عملة
PRINT_TOUCH_PRETTY = True    # عرض منسق لأحداث اللمس

# Fast pre-filter (skip symbols that already moved)
PRICE_CHANGE_FILTER_ENABLED = True
PRICE_CHANGE_LOOKBACK = 12   # عدد الشموع لحساب نسبة التغير
PRICE_CHANGE_MIN_PCT = 5.0   # تخطّي العملات التي ارتفعت >= هذه النسبة
PRICE_CHANGE_SIDE = "up"     # "up" أو "down"

# =========================
# Timeframe helpers (TV-like)
# =========================
def tf_to_seconds(tf: str) -> int:
    tf = tf.strip()
    if tf.endswith(("S", "s")):
        return int(tf[:-1])
    if tf.endswith("m"):
        return int(tf[:-1]) * 60
    if tf.endswith("h"):
        return int(tf[:-1]) * 3600
    if tf.endswith(("d", "D")):
        return int(tf[:-1]) * 86400
    if tf.isdigit():
        return int(tf) * 60
    raise ValueError(f"Unsupported timeframe: {tf}")

def tv_lower_tf_string_from_chart_tf(chart_tf: str) -> str:
    # Pine: tf = time_<=1 ? "5S" : tostring(ceil(time_/div))
    sec = tf_to_seconds(chart_tf)
    time_min = sec / 60.0
    div = 10.0
    if time_min <= 1.0:
        return "5S"
    return str(int(math.ceil(time_min / div)))  # minutes string

def format_volume_tv_like(v: float) -> str:
    if pd.isna(v):
        return "na"
    av = abs(float(v))
    if av >= 1e9:
        return f"{v/1e9:.3f}B"
    if av >= 1e6:
        return f"{v/1e6:.3f}M"
    if av >= 1e3:
        return f"{v/1e3:.3f}K"
    return f"{v:.0f}"

def format_percent_tv_like(v: float) -> str:
    if pd.isna(v):
        return "na"
    return f"{int(v)}"

def color_text(text: str, color: str) -> str:
    colors = {
        "green": "\033[92m",
        "red": "\033[91m",
        "reset": "\033[0m",
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def format_touch_line(sym: str, fvg_type: str, top: float, bottom: float) -> str:
    color = "green" if fvg_type == "BULL" else "red"
    return f"{color_text(sym, color)} | {fvg_type} | top={top} bottom={bottom}"

# =========================
# Rolling helpers
# =========================
def rolling_max_full(series: np.ndarray, length: int) -> np.ndarray:
    s = pd.Series(series, dtype="float64")
    return s.rolling(length, min_periods=1).max().to_numpy(dtype=float)

# ============================================================
# Core indicator (matches Pine v6 logic)
# Returns:
#   result_df: debug series for validation
#   fvgs: history list of all fvgs (active + deleted) with create/remove info
#   dashboard_last: dict at last bar (like TV table)
# ============================================================
def run_indicator(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    df = df.copy()
    for c in ["open", "high", "low", "close", "volume", "sumBull", "sumBear", "totalVolume"]:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c} (100% requires lower-tf sums attached)")

    n = len(df)
    o = df["open"].to_numpy(float)
    h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)
    v = df["volume"].to_numpy(float)

    sumBull = df["sumBull"].to_numpy(float)
    sumBear = df["sumBear"].to_numpy(float)
    totalVol = df["totalVolume"].to_numpy(float)

    percentile_len = int(params.get("percentile_len", 1000))
    filter_threshold = float(params.get("filter_threshold", 10))
    min_bar_index = int(params.get("min_bar_index", 100))
    max_fvgs = int(params.get("max_fvgs", 10))

    mitigationSrc = str(params.get("mitigationSrc", "close")).lower()
    bullGaps = bool(params.get("bullGaps", True))
    bearGaps = bool(params.get("bearGaps", True))
    volumeBars = bool(params.get("volumeBars", True))

    # --- diff (exact)
    diff = np.full(n, np.nan, float)
    for i in range(n):
        if i < 2:
            continue
        if pd.isna(c[i-1]) or pd.isna(o[i-1]):
            continue
        if c[i-1] > o[i-1]:
            if not pd.isna(l[i]) and not pd.isna(h[i-2]):
                diff[i] = (l[i] - h[i-2]) / l[i] * 100.0
        else:
            if not pd.isna(l[i-2]) and not pd.isna(h[i]):
                diff[i] = (l[i-2] - h[i]) / h[i] * 100.0

    # percentile_nearest_rank(diff, 1000, 100) == rolling max
    p100 = rolling_max_full(diff, percentile_len)

    sizeFVG = np.full(n, np.nan, float)
    filterFVG = np.full(n, np.nan, float)
    for i in range(n):
        if pd.isna(diff[i]) or pd.isna(p100[i]):
            continue
        sizeFVG[i] = diff[i] / p100[i] * 100.0
        filterFVG[i] = 1.0 if (sizeFVG[i] > filter_threshold) else 0.0

    isBull_gap = np.full(n, np.nan, float)
    isBear_gap = np.full(n, np.nan, float)
    for i in range(n):
        if i < 2 or pd.isna(filterFVG[i]):
            continue
        f_ok = (filterFVG[i] == 1.0)
        bull_cond = (h[i-2] < l[i]) and (h[i-2] < h[i-1]) and (l[i-2] < l[i]) and f_ok
        bear_cond = (l[i-2] > h[i]) and (l[i-2] > l[i-1]) and (h[i-2] > h[i]) and f_ok
        isBull_gap[i] = 1.0 if bull_cond else 0.0
        isBear_gap[i] = 1.0 if bear_cond else 0.0

    # ===== stateful fvgs =====
    active: List[Dict[str, Any]] = []
    history: List[Dict[str, Any]] = []
    hist_by_id: Dict[int, Dict[str, Any]] = {}
    next_id = 1

    created_id = np.full(n, np.nan, float)
    removed_count = np.zeros(n, float)
    active_count = np.zeros(n, float)

    def pct_int_or_nan(num: float, den: float) -> float:
        if pd.isna(num) or pd.isna(den) or den == 0:
            return float("nan")
        return float(int((num / den) * 100.0))

    for i in range(n):
        if not (i > min_bar_index):
            active_count[i] = len(active)
            continue

        # create uses sumBull[1], sumBear[1], totalVol[1]
        prev_total = totalVol[i-1] if i >= 1 else np.nan
        prev_bull = sumBull[i-1] if i >= 1 else np.nan
        prev_bear = sumBear[i-1] if i >= 1 else np.nan
        bullPct = pct_int_or_nan(prev_bull, prev_total)
        bearPct = pct_int_or_nan(prev_bear, prev_total)
        totalV = float(prev_total) if not pd.isna(prev_total) else np.nan

        # ---- CREATE BULL
        if isBull_gap[i] == 1.0 and bullGaps:
            body_top = float(l[i])
            body_bot = float(h[i-2])
            mid = (body_top + body_bot) / 2.0

            fvg = {
                "id": next_id,
                "isBull": True,
                "bull": bullPct,
                "bear": bearPct,
                "totalVol": totalV,
                "created_at_bar": int(i),
                "removed_at_bar": None,
                "removed_reason": None,

                "body": {"left": i-1, "right": i+5, "top": body_top, "bottom": body_bot, "text": "", "deleted": False},
                "bullBar": {"left": i-1, "right": i-1, "top": mid, "bottom": body_bot, "text": "", "deleted": False},
                "bearBar": {"left": i-1, "right": i-1, "top": body_top, "bottom": mid, "text": "", "deleted": False},
            }
            active.append(fvg)
            history.append(fvg)
            hist_by_id[next_id] = fvg
            created_id[i] = next_id
            next_id += 1

        # ---- CREATE BEAR
        if isBear_gap[i] == 1.0 and bearGaps:
            body_top = float(l[i-2])
            body_bot = float(h[i])
            mid = (body_top + body_bot) / 2.0

            fvg = {
                "id": next_id,
                "isBull": False,
                "bull": bullPct,
                "bear": bearPct,
                "totalVol": totalV,
                "created_at_bar": int(i),
                "removed_at_bar": None,
                "removed_reason": None,

                "body": {"left": i-1, "right": i+5, "top": body_top, "bottom": body_bot, "text": "", "deleted": False},
                "bullBar": {"left": i-1, "right": i-1, "top": mid, "bottom": body_bot, "text": "", "deleted": False},
                "bearBar": {"left": i-1, "right": i-1, "top": body_top, "bottom": mid, "text": "", "deleted": False},
            }
            active.append(fvg)
            history.append(fvg)
            hist_by_id[next_id] = fvg
            created_id[i] = next_id
            next_id += 1

        # ---- REMOVE CROSSED + UPDATE (Pine loops over fvgs and may remove; we emulate safely via snapshot)
        snapshot = list(active)
        for fvg in snapshot:
            body = fvg["body"]

            # source
            if mitigationSrc == "close":
                src1 = c[i]
                src2 = c[i]
            else:
                src1 = l[i]
                src2 = h[i]

            left = float(body["left"])
            right_old = float(body["right"])  # get_right BEFORE set_right
            top = float(body["top"])
            bot = float(body["bottom"])

            # remove conditions
            removed = False
            if fvg["isBull"]:
                if (not pd.isna(src1)) and (src1 < bot):
                    removed = True
            else:
                if (not pd.isna(src2)) and (src2 > top):
                    removed = True

            if removed:
                # delete + remove from active
                body["deleted"] = True
                fvg["bullBar"]["deleted"] = True
                fvg["bearBar"]["deleted"] = True
                fvg["removed_at_bar"] = int(i)
                fvg["removed_reason"] = "mitigation"
                if fvg in active:
                    active.remove(fvg)
                removed_count[i] += 1.0

            # update right/text even if removed (TV calls on deleted objects = effectively no-op)
            if not body.get("deleted", False):
                body["right"] = i + 25
                body["text"] = format_volume_tv_like(fvg["totalVol"])

                if volumeBars:
                    width = float(int(right_old - left))
                    size = width / 200.0
                    fvg["bullBar"]["right"] = left + size * float(fvg["bull"])
                    fvg["bearBar"]["right"] = left + size * float(fvg["bear"])
                    if pd.isna(fvg["bull"]):
                        fvg["bullBar"]["text"] = "na"
                    else:
                        fvg["bullBar"]["text"] = f"{int(fvg['bull'])}%"
                    if pd.isna(fvg["bear"]):
                        fvg["bearBar"]["text"] = "na"
                    else:
                        fvg["bearBar"]["text"] = f"{int(fvg['bear'])}%"
                else:
                    fvg["bullBar"]["text"] = ""
                    fvg["bearBar"]["text"] = ""

        # ---- REMOVE OVERLAP (matches Pine condition top1 < top and top1 > bot)
        initial_size = len(active)
        for i_idx in range(initial_size):
            if i_idx >= len(active):
                break
            fvg = active[i_idx]
            top = float(fvg["body"]["top"])
            bot = float(fvg["body"]["bottom"])
            for j_idx in range(initial_size):
                if j_idx >= len(active):
                    continue
                if i_idx == j_idx:
                    continue
                other = active[j_idx]
                top1 = float(other["body"]["top"])
                if (top1 < top) and (top1 > bot):
                    fvg["body"]["deleted"] = True
                    fvg["bullBar"]["deleted"] = True
                    fvg["bearBar"]["deleted"] = True
                    fvg["removed_at_bar"] = int(i)
                    fvg["removed_reason"] = "overlap"
                    active.pop(i_idx)
                    removed_count[i] += 1.0
                    break

        # ---- CONTROL SIZE (shift oldest)
        while len(active) > max_fvgs:
            old = active.pop(0)
            old["body"]["deleted"] = True
            old["bullBar"]["deleted"] = True
            old["bearBar"]["deleted"] = True
            if old["removed_at_bar"] is None:
                old["removed_at_bar"] = int(i)
                old["removed_reason"] = "size"
            removed_count[i] += 1.0

        active_count[i] = float(len(active))

    # dashboard at last bar
    dashboard_last = None
    if n > 0:
        bullCount = 0
        bearCount = 0
        bullVolume = 0.0
        bearVolume = 0.0
        for f in active:
            if f["isBull"]:
                bullCount += 1
                if not pd.isna(f["totalVol"]):
                    bullVolume += float(f["totalVol"])
            else:
                bearCount += 1
                if not pd.isna(f["totalVol"]):
                    bearVolume += float(f["totalVol"])
        dashboard_last = {
            "bullCount": bullCount,
            "bearCount": bearCount,
            "bullTotalVolume": bullVolume,
            "bearTotalVolume": bearVolume,
            "active": len(active),
        }

    out = pd.DataFrame(index=df.index)
    out["diff"] = diff
    out["p100"] = p100
    out["sizeFVG"] = sizeFVG
    out["filterFVG"] = filterFVG
    out["isBull_gap"] = isBull_gap
    out["isBear_gap"] = isBear_gap
    out["sumBull"] = sumBull
    out["sumBear"] = sumBear
    out["totalVolume"] = totalVol
    out["created_id"] = created_id
    out["removed_count"] = removed_count
    out["active_count"] = active_count

    return out, history, dashboard_last

# ============================================================
# CCXT: Fetch helpers (pagination for 1m)
# ============================================================
def _make_exchange():
    try:
        import ccxt  # type: ignore
    except Exception as e:
        raise RuntimeError("ccxt غير مثبت. ثبته عبر: pip install ccxt") from e
    ex_class = getattr(ccxt, EXCHANGE_ID)
    return ex_class({"enableRateLimit": True, "options": {"defaultType": "future"}})

def fetch_ohlcv_paged(ex, symbol: str, timeframe: str, since_ms: Optional[int], limit: int, max_bars: int) -> pd.DataFrame:
    """
    يجلب تاريخ OHLCV بالترقيم (pagination) حتى max_bars أو حتى ينتهي.
    """
    rows: List[List[float]] = []
    next_since = since_ms
    safety = 0
    while len(rows) < max_bars and safety < 200:
        safety += 1
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=next_since, limit=limit)
        if not batch:
            break
        if rows and batch[0][0] <= rows[-1][0]:
            # prevent infinite loop
            batch = [r for r in batch if r[0] > rows[-1][0]]
        if not batch:
            break
        rows.extend(batch)
        next_since = batch[-1][0] + 1  # move forward
        if len(batch) < limit:
            break
        # rate limit handled by ccxt enableRateLimit
    rows = rows[-max_bars:] if len(rows) > max_bars else rows
    arr = np.array(rows, dtype=float)
    df = pd.DataFrame(arr, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    return df

def attach_lower_tf_volume_sums_1h(df_high: pd.DataFrame, df_1m: pd.DataFrame, chart_tf: str) -> pd.DataFrame:
    """
    1h => TV lower tf = 6 minutes.
    نحاكي request.security_lower_tf عبر:
      1m -> resample 6m -> classify bull/bear volume -> sum داخل كل 1h
    """
    df_high = df_high.copy()
    df_1m = df_1m.copy()

    high_sec = tf_to_seconds(chart_tf)
    tf_str = tv_lower_tf_string_from_chart_tf(chart_tf)  # on 1h => "6"
    lower_sec = tf_to_seconds(tf_str)

    if lower_sec < 60:
        # 5S not available -> cannot be 100% on <=1m charts
        lower_sec = 60

    base = df_1m.sort_values("time").set_index("time")

    # resample to 6m (rule "6T")
    rule = f"{int(lower_sec/60)}T"
    lowtf = base.resample(rule, label="left", closed="left").agg({
        "open":"first","high":"max","low":"min","close":"last","volume":"sum"
    }).dropna()

    lowtf["bull_v"] = np.where(lowtf["close"] > lowtf["open"], lowtf["volume"], 0.0)
    lowtf["bear_v"] = np.where(lowtf["close"] < lowtf["open"], lowtf["volume"], 0.0)

    # align lower-tf bars into chart bars using actual chart open times
    high_ns = df_high["time"].view("int64").to_numpy()
    low_ns = lowtf.index.view("int64")

    idx = np.searchsorted(high_ns, low_ns, side="right") - 1
    high_window_ns = np.int64(high_sec) * np.int64(1_000_000_000)
    valid = (idx >= 0) & (idx < len(high_ns))
    valid = valid & (low_ns < (high_ns[idx] + high_window_ns))
    lowtf = lowtf.loc[valid]
    idx = idx[valid]

    lowtf["bucket"] = idx
    grp = lowtf.groupby("bucket", sort=False)

    sb = grp["bull_v"].sum().to_dict()
    sr = grp["bear_v"].sum().to_dict()

    sumBull = np.zeros(len(df_high), float)
    sumBear = np.zeros(len(df_high), float)
    for i in range(len(df_high)):
        sumBull[i] = float(sb.get(int(i), 0.0))
        sumBear[i] = float(sr.get(int(i), 0.0))

    df_high["sumBull"] = sumBull
    df_high["sumBear"] = sumBear
    df_high["totalVolume"] = sumBull + sumBear
    return df_high

def list_usdtm_symbols(ex) -> List[str]:
    markets = ex.load_markets()
    syms = []
    for s, m in markets.items():
        if not m.get("active", True):
            continue
        if m.get("swap", False) and m.get("linear", False) and m.get("quote") == "USDT":
            syms.append(s)
    return sorted(set(syms))

# ============================================================
# Scanner (1h)
# ============================================================
def scan_once(state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    state = state or {}
    seen = state.get("seen", {})  # (symbol,event,fvg_id) -> last_bar

    ex = _make_exchange()
    symbols = SYMBOLS if SYMBOLS is not None else list_usdtm_symbols(ex)

    high_sec = tf_to_seconds(TIMEFRAME)
    lower_tf = tv_lower_tf_string_from_chart_tf(TIMEFRAME)  # "6" on 1h
    base_history_sec = BASE_CANDLES_1H * 3600
    num_candles = max(150, int(math.ceil(base_history_sec / high_sec)))
    # need 1m bars to cover high window:
    need_1m = int(num_candles * (high_sec / 60)) + 500  # buffer

    for sym in symbols:
        last_time = None
        status = "ok"
        try:
            # fetch chart tf candles
            df_high = ex.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=num_candles)
            if not df_high or len(df_high) < 150:
                status = "skip_short"
                continue
            df_high = pd.DataFrame(np.array(df_high, float), columns=["time","open","high","low","close","volume"])
            df_high["time"] = pd.to_datetime(df_high["time"], unit="ms", utc=True)

            # fast pre-filter to skip symbols with strong move (saves lower-tf fetch)
            if PRICE_CHANGE_FILTER_ENABLED and len(df_high) > PRICE_CHANGE_LOOKBACK:
                last_close = float(df_high["close"].iloc[-1])
                past_close = float(df_high["close"].iloc[-1 - PRICE_CHANGE_LOOKBACK])
                if past_close != 0:
                    pct_change = ((last_close - past_close) / past_close) * 100.0
                    if PRICE_CHANGE_SIDE == "up" and pct_change >= PRICE_CHANGE_MIN_PCT:
                        status = f"skip_move_up({pct_change:.2f}%)"
                        continue
                    if PRICE_CHANGE_SIDE == "down" and pct_change <= -PRICE_CHANGE_MIN_PCT:
                        status = f"skip_move_down({pct_change:.2f}%)"
                        continue

            start_ms = int(df_high["time"].iloc[0].value // 10**6)
            end_ms = int((df_high["time"].iloc[-1].value // 10**6) + high_sec * 1000)

            # fetch 1m bars covering the same range (paged)
            df_1m = fetch_ohlcv_paged(ex, sym, "1m", since_ms=start_ms, limit=1500, max_bars=need_1m)
            if df_1m.empty:
                status = "skip_no_1m"
                continue

            # attach lower tf sums (1h -> 6m)
            df_hi2 = attach_lower_tf_volume_sums_1h(df_high, df_1m, TIMEFRAME)

            # run indicator
            res, fvgs, dash = run_indicator(df_hi2[["open","high","low","close","volume","sumBull","sumBear","totalVolume"]], PARAMS)

            last_bar = len(res) - 1
            last_time = str(df_hi2["time"].iloc[-1])

            def emit(payload: Dict[str, Any]):
                if PRINT_EVENTS:
                    if SHOW_ONLY_TOUCH_EVENTS and payload.get("event") != "FVG_TOUCHED":
                        return
                    print("[OUT] " + json.dumps(payload, ensure_ascii=False))

            # أحداث آخر شمعة (إنشاء/حذف)
            cid = res["created_id"].iloc[last_bar]
            if not pd.isna(cid):
                fvg_id = int(cid)
                key = (sym, "CREATE", fvg_id)
                if key not in seen:
                    f = next((x for x in fvgs if int(x["id"]) == fvg_id), None)
                    if f:
                        emit({
                            "symbol": sym,
                            "event": "FVG_CREATE",
                            "tf": TIMEFRAME,
                            "lower_tf": lower_tf,
                            "time": last_time,
                            "fvg_id": fvg_id,
                            "type": "BULL" if f["isBull"] else "BEAR",
                            "top": float(f["body"]["top"]),
                            "bottom": float(f["body"]["bottom"]),
                            "bull%": format_percent_tv_like(f["bull"]),
                            "bear%": format_percent_tv_like(f["bear"]),
                            "totalVol": format_volume_tv_like(f["totalVol"]),
                        })
                    seen[key] = last_bar

            rc = float(res["removed_count"].iloc[last_bar])
            if rc > 0:
                # اعرض كل ما تم حذفه على آخر بار
                removed_now = [x for x in fvgs if x["removed_at_bar"] == last_bar]
                for f in removed_now:
                    key = (sym, "REMOVE", int(f["id"]))
                    if key in seen:
                        continue
                    emit({
                        "symbol": sym,
                        "event": "FVG_REMOVE",
                        "tf": TIMEFRAME,
                        "lower_tf": lower_tf,
                        "time": last_time,
                        "fvg_id": int(f["id"]),
                        "type": "BULL" if f["isBull"] else "BEAR",
                        "reason": f["removed_reason"],
                        "top": float(f["body"]["top"]),
                        "bottom": float(f["body"]["bottom"]),
                        "bull%": format_percent_tv_like(f["bull"]),
                        "bear%": format_percent_tv_like(f["bear"]),
                        "totalVol": format_volume_tv_like(f["totalVol"]),
                    })
                    seen[key] = last_bar

            # ملخص الفجوات النشطة (مثل ما تراه على الشارت)
            if PRINT_ACTIVE_SUMMARY and dash is not None:
                active_now = [x for x in fvgs if x["removed_at_bar"] is None]
                active_now.sort(key=lambda z: int(z["created_at_bar"]), reverse=True)
                active_now = active_now[:ACTIVE_TOP_N]
                emit({
                    "symbol": sym,
                    "event": "ACTIVE_SUMMARY",
                    "tf": TIMEFRAME,
                    "lower_tf": lower_tf,
                    "time": last_time,
                    "bullCount": dash["bullCount"],
                    "bearCount": dash["bearCount"],
                    "bullTotalVolume": format_volume_tv_like(dash["bullTotalVolume"]),
                    "bearTotalVolume": format_volume_tv_like(dash["bearTotalVolume"]),
                    "active": dash["active"],
                    "top": [
                        {
                            "id": int(f["id"]),
                            "type": "BULL" if f["isBull"] else "BEAR",
                            "top": float(f["body"]["top"]),
                            "bottom": float(f["body"]["bottom"]),
                            "bull%": format_percent_tv_like(f["bull"]),
                            "bear%": format_percent_tv_like(f["bear"]),
                            "totalVol": format_volume_tv_like(f["totalVol"]),
                        }
                        for f in active_now
                    ]
                })

            # لمس المنطقة خلال آخر شموع قابلة للتخصيص
            if PRINT_TOUCH_EVENTS and TOUCH_LOOKBACK > 0:
                active_now = [x for x in fvgs if x["removed_at_bar"] is None]
                if active_now:
                    end_idx = len(df_hi2)
                    start_idx = max(0, end_idx - TOUCH_LOOKBACK)
                    highs = df_hi2["high"].to_numpy(float)[start_idx:end_idx]
                    lows = df_hi2["low"].to_numpy(float)[start_idx:end_idx]
                    touched = []
                    for f in active_now:
                        touch_key = (sym, "TOUCH", int(f["id"]))
                        if touch_key in seen:
                            continue
                        top = float(f["body"]["top"])
                        bottom = float(f["body"]["bottom"])
                        hit = np.any((lows <= top) & (highs >= bottom))
                        if hit:
                            seen[touch_key] = last_bar
                            touched.append({
                                "id": int(f["id"]),
                                "type": "BULL" if f["isBull"] else "BEAR",
                                "top": float(f["body"]["top"]),
                                "bottom": float(f["body"]["bottom"]),
                                "bull%": format_percent_tv_like(f["bull"]),
                                "bear%": format_percent_tv_like(f["bear"]),
                                "totalVol": format_volume_tv_like(f["totalVol"]),
                            })
                    if touched:
                        emit({
                            "symbol": sym,
                            "event": "FVG_TOUCHED",
                            "tf": TIMEFRAME,
                            "lower_tf": lower_tf,
                            "time": last_time,
                            "lookback": TOUCH_LOOKBACK,
                            "count": len(touched),
                            "fvgs": touched,
                        })
                        if PRINT_TOUCH_PRETTY:
                            for f in touched:
                                print("[TOUCH] " + format_touch_line(sym, f["type"], f["top"], f["bottom"]))

        except Exception as e:
            status = "error"
            print(f"[WARN] {sym}: {e}")
        finally:
            if PRINT_SYMBOL_DONE:
                print(f"[DONE] {sym} tf={TIMEFRAME} lower_tf={lower_tf} candles={num_candles} status={status} last={last_time}")

    state["seen"] = seen
    return state

def scan_loop():
    state = {"seen": {}}
    while True:
        state = scan_once(state=state)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    # تشغيل تلقائي
    print("== Android Note ==")
    print("- على 1h: يحتاج تحميل 1m لإعادة بناء 6m (قد يأخذ وقت على أول تشغيل).")
    print("- لو يتوقف بالخلفية: عطّل Battery Optimization / اترك الشاشة شغالة.")
    print("==================")
    try:
        scan_loop()
    except KeyboardInterrupt:
        print("\n[STOP] تم الإيقاف.")
