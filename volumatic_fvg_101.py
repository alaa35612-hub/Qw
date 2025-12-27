import json
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# =========================
# SETTINGS (scanner)
# =========================
EXCHANGE_ID = "binanceusdm"
TIMEFRAME = "1h"
NUM_CANDLES = 300
SYMBOLS = None  # None = all USDT-M
POLL_SECONDS = 60

# Pine inputs
PARAMS = {
    "mitigationSrc": "close",  # "close" or "high/low"
    "bullGaps": True,
    "bearGaps": True,
    "volumeBars": True,
    "percentile_len": 1000,
    "filter_threshold": 10,
    "max_fvgs": 10,
    "min_bar_index": 100,
}

# Output toggles
PRINT_EVENTS = True

# Touch detection: only emit first-time touches
TOUCH_LOOKBACK = 10


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
    sec = tf_to_seconds(chart_tf)
    time_min = sec / 60.0
    div = 10.0
    if time_min <= 1.0:
        return "5S"
    return str(int(math.ceil(time_min / div)))


def format_volume_tv_like(v: float) -> str:
    if pd.isna(v):
        return "na"
    av = abs(float(v))
    if av >= 1e9:
        return f"{v/1e9:.2f}B"
    if av >= 1e6:
        return f"{v/1e6:.2f}M"
    if av >= 1e3:
        return f"{v/1e3:.2f}K"
    return f"{v:.0f}"


def format_percent_tv_like(v: float) -> str:
    if pd.isna(v):
        return "na"
    return f"{int(v)}"


def percentile_nearest_rank_max(series: np.ndarray, length: int) -> np.ndarray:
    s = pd.Series(series, dtype="float64")
    return s.rolling(length, min_periods=length).max().to_numpy(dtype=float)


@dataclass
class FVGBox:
    left: int
    right: int
    top: float
    bottom: float
    text: str = ""
    deleted: bool = False


@dataclass
class FVGState:
    fvg_id: int
    body: FVGBox
    bull: float
    bear: float
    is_bull: bool
    bull_bar: FVGBox
    bear_bar: FVGBox
    total_vol: float
    created_at_bar: int
    removed_at_bar: Optional[int] = None
    removed_reason: Optional[str] = None


def run_indicator(
    df: pd.DataFrame,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[FVGState], Optional[Dict[str, Any]]]:
    df = df.copy()
    for c in ["open", "high", "low", "close", "volume", "sumBull", "sumBear", "totalVolume"]:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c} (needs lower-tf sums attached)")

    n = len(df)
    o = df["open"].to_numpy(float)
    h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)
    sum_bull = df["sumBull"].to_numpy(float)
    sum_bear = df["sumBear"].to_numpy(float)
    total_vol = df["totalVolume"].to_numpy(float)

    percentile_len = int(params.get("percentile_len", 1000))
    filter_threshold = float(params.get("filter_threshold", 10))
    min_bar_index = int(params.get("min_bar_index", 100))
    max_fvgs = int(params.get("max_fvgs", 10))

    mitigation_src = str(params.get("mitigationSrc", "close")).lower()
    bull_gaps = bool(params.get("bullGaps", True))
    bear_gaps = bool(params.get("bearGaps", True))
    volume_bars = bool(params.get("volumeBars", True))

    diff = np.full(n, np.nan, float)
    for i in range(n):
        if i < 2:
            continue
        if pd.isna(c[i - 1]) or pd.isna(o[i - 1]):
            continue
        if c[i - 1] > o[i - 1]:
            if not pd.isna(l[i]) and not pd.isna(h[i - 2]):
                diff[i] = (l[i] - h[i - 2]) / l[i] * 100.0
        else:
            if not pd.isna(l[i - 2]) and not pd.isna(h[i]):
                diff[i] = (l[i - 2] - h[i]) / h[i] * 100.0

    p100 = percentile_nearest_rank_max(diff, percentile_len)

    size_fvg = np.full(n, np.nan, float)
    filter_fvg = np.full(n, np.nan, float)
    for i in range(n):
        if pd.isna(diff[i]) or pd.isna(p100[i]) or p100[i] == 0:
            continue
        size_fvg[i] = diff[i] / p100[i] * 100.0
        filter_fvg[i] = 1.0 if (size_fvg[i] > filter_threshold) else 0.0

    is_bull_gap = np.full(n, np.nan, float)
    is_bear_gap = np.full(n, np.nan, float)
    for i in range(n):
        if i < 2 or pd.isna(filter_fvg[i]):
            continue
        f_ok = filter_fvg[i] == 1.0
        bull_cond = (h[i - 2] < l[i]) and (h[i - 2] < h[i - 1]) and (l[i - 2] < l[i]) and f_ok
        bear_cond = (l[i - 2] > h[i]) and (l[i - 2] > l[i - 1]) and (h[i - 2] > h[i]) and f_ok
        is_bull_gap[i] = 1.0 if bull_cond else 0.0
        is_bear_gap[i] = 1.0 if bear_cond else 0.0

    active: List[FVGState] = []
    history: List[FVGState] = []
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

        prev_total = total_vol[i - 1] if i >= 1 else np.nan
        prev_bull = sum_bull[i - 1] if i >= 1 else np.nan
        prev_bear = sum_bear[i - 1] if i >= 1 else np.nan
        bull_pct = pct_int_or_nan(prev_bull, prev_total)
        bear_pct = pct_int_or_nan(prev_bear, prev_total)
        total_v = float(prev_total) if not pd.isna(prev_total) else np.nan

        if is_bull_gap[i] == 1.0 and bull_gaps:
            body_top = float(l[i])
            body_bot = float(h[i - 2])
            mid = (body_top + body_bot) / 2.0
            body = FVGBox(left=i - 1, right=i + 5, top=body_top, bottom=body_bot)
            bear_bar = FVGBox(left=i - 1, right=i - 1, top=body_top, bottom=mid)
            bull_bar = FVGBox(left=i - 1, right=i - 1, top=mid, bottom=body_bot)
            fvg = FVGState(
                fvg_id=next_id,
                body=body,
                bull=bull_pct,
                bear=bear_pct,
                is_bull=True,
                bull_bar=bull_bar,
                bear_bar=bear_bar,
                total_vol=total_v,
                created_at_bar=int(i),
            )
            active.append(fvg)
            history.append(fvg)
            created_id[i] = next_id
            next_id += 1

        if is_bear_gap[i] == 1.0 and bear_gaps:
            body_top = float(l[i - 2])
            body_bot = float(h[i])
            mid = (body_top + body_bot) / 2.0
            body = FVGBox(left=i - 1, right=i + 5, top=body_top, bottom=body_bot)
            bear_bar = FVGBox(left=i - 1, right=i - 1, top=body_top, bottom=mid)
            bull_bar = FVGBox(left=i - 1, right=i - 1, top=mid, bottom=body_bot)
            fvg = FVGState(
                fvg_id=next_id,
                body=body,
                bull=bull_pct,
                bear=bear_pct,
                is_bull=False,
                bull_bar=bull_bar,
                bear_bar=bear_bar,
                total_vol=total_v,
                created_at_bar=int(i),
            )
            active.append(fvg)
            history.append(fvg)
            created_id[i] = next_id
            next_id += 1

        snapshot = list(active)
        for fvg in snapshot:
            if mitigation_src == "close":
                src1 = c[i]
                src2 = c[i]
            else:
                src1 = l[i]
                src2 = h[i]

            left = float(fvg.body.left)
            right_old = float(fvg.body.right)
            top = float(fvg.body.top)
            bot = float(fvg.body.bottom)

            removed = False
            if fvg.is_bull:
                if not pd.isna(src1) and src1 < bot:
                    removed = True
            else:
                if not pd.isna(src2) and src2 > top:
                    removed = True

            if removed:
                fvg.body.deleted = True
                fvg.bull_bar.deleted = True
                fvg.bear_bar.deleted = True
                fvg.removed_at_bar = int(i)
                fvg.removed_reason = "mitigation"
                if fvg in active:
                    active.remove(fvg)
                removed_count[i] += 1.0

            if not fvg.body.deleted:
                fvg.body.right = i + 25
                fvg.body.text = format_volume_tv_like(fvg.total_vol)

                if volume_bars:
                    size = int(right_old - left) / 200
                    fvg.bull_bar.right = left + size * float(fvg.bull)
                    fvg.bear_bar.right = left + size * float(fvg.bear)
                    fvg.bull_bar.text = "na" if pd.isna(fvg.bull) else f"{int(fvg.bull)}%"
                    fvg.bear_bar.text = "na" if pd.isna(fvg.bear) else f"{int(fvg.bear)}%"
                else:
                    fvg.bull_bar.text = ""
                    fvg.bear_bar.text = ""

        initial_size = len(active)
        for i_idx in range(initial_size):
            if i_idx >= len(active):
                break
            fvg = active[i_idx]
            top = float(fvg.body.top)
            bot = float(fvg.body.bottom)
            for j_idx in range(initial_size):
                if j_idx >= len(active):
                    continue
                if i_idx == j_idx:
                    continue
                other = active[j_idx]
                top1 = float(other.body.top)
                if top1 < top and top1 > bot:
                    fvg.body.deleted = True
                    fvg.bull_bar.deleted = True
                    fvg.bear_bar.deleted = True
                    fvg.removed_at_bar = int(i)
                    fvg.removed_reason = "overlap"
                    active.pop(i_idx)
                    removed_count[i] += 1.0
                    break

        if len(active) > max_fvgs:
            old = active.pop(0)
            old.body.deleted = True
            old.bull_bar.deleted = True
            old.bear_bar.deleted = True
            if old.removed_at_bar is None:
                old.removed_at_bar = int(i)
                old.removed_reason = "size"
            removed_count[i] += 1.0

        active_count[i] = float(len(active))

    dashboard_last = None
    if n > 0:
        bull_count = 0
        bear_count = 0
        bull_volume = 0.0
        bear_volume = 0.0
        for f in active:
            if f.is_bull:
                bull_count += 1
                if not pd.isna(f.total_vol):
                    bull_volume += float(f.total_vol)
            else:
                bear_count += 1
                if not pd.isna(f.total_vol):
                    bear_volume += float(f.total_vol)
        dashboard_last = {
            "bullCount": bull_count,
            "bearCount": bear_count,
            "bullTotalVolume": bull_volume,
            "bearTotalVolume": bear_volume,
            "active": len(active),
        }

    out = pd.DataFrame(index=df.index)
    out["diff"] = diff
    out["p100"] = p100
    out["sizeFVG"] = size_fvg
    out["filterFVG"] = filter_fvg
    out["isBull_gap"] = is_bull_gap
    out["isBear_gap"] = is_bear_gap
    out["sumBull"] = sum_bull
    out["sumBear"] = sum_bear
    out["totalVolume"] = total_vol
    out["created_id"] = created_id
    out["removed_count"] = removed_count
    out["active_count"] = active_count

    return out, history, dashboard_last


def _make_exchange():
    try:
        import ccxt  # type: ignore
    except Exception as exc:
        raise RuntimeError("ccxt غير مثبت. ثبته عبر: pip install ccxt") from exc
    ex_class = getattr(ccxt, EXCHANGE_ID)
    return ex_class({"enableRateLimit": True, "options": {"defaultType": "future"}})


def fetch_ohlcv_paged(
    ex,
    symbol: str,
    timeframe: str,
    since_ms: Optional[int],
    limit: int,
    max_bars: int,
) -> pd.DataFrame:
    rows: List[List[float]] = []
    next_since = since_ms
    safety = 0
    while len(rows) < max_bars and safety < 200:
        safety += 1
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=next_since, limit=limit)
        if not batch:
            break
        if rows and batch[0][0] <= rows[-1][0]:
            batch = [r for r in batch if r[0] > rows[-1][0]]
        if not batch:
            break
        rows.extend(batch)
        next_since = batch[-1][0] + 1
        if len(batch) < limit:
            break
    rows = rows[-max_bars:] if len(rows) > max_bars else rows
    arr = np.array(rows, dtype=float)
    df = pd.DataFrame(arr, columns=["time", "open", "high", "low", "close", "volume"])
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    return df


def attach_lower_tf_volume_sums(df_high: pd.DataFrame, df_low: pd.DataFrame, chart_tf: str) -> pd.DataFrame:
    df_high = df_high.copy()
    df_low = df_low.copy()

    high_sec = tf_to_seconds(chart_tf)
    tf_str = tv_lower_tf_string_from_chart_tf(chart_tf)
    lower_sec = tf_to_seconds(tf_str)
    if lower_sec < 60:
        lower_sec = 60

    base = df_low.sort_values("time").set_index("time")
    rule = f"{int(lower_sec/60)}T"
    lowtf = base.resample(rule, label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()

    lowtf["bull_v"] = np.where(lowtf["close"] > lowtf["open"], lowtf["volume"], 0.0)
    lowtf["bear_v"] = np.where(lowtf["close"] < lowtf["open"], lowtf["volume"], 0.0)

    high_times = df_high["time"].to_numpy(dtype="datetime64[ns]")
    low_times = lowtf.index.to_numpy(dtype="datetime64[ns]")
    high_ns = high_times.astype(np.int64)
    low_ns = low_times.astype(np.int64)
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

    sum_bull = np.zeros(len(df_high), float)
    sum_bear = np.zeros(len(df_high), float)
    for i in range(len(df_high)):
        sum_bull[i] = float(sb.get(int(i), 0.0))
        sum_bear[i] = float(sr.get(int(i), 0.0))

    df_high["sumBull"] = sum_bull
    df_high["sumBear"] = sum_bear
    df_high["totalVolume"] = sum_bull + sum_bear
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


def scan_once(state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    state = state or {}
    touched = state.get("touched", set())

    ex = _make_exchange()
    symbols = SYMBOLS if SYMBOLS is not None else list_usdtm_symbols(ex)

    high_sec = tf_to_seconds(TIMEFRAME)
    lower_tf = tv_lower_tf_string_from_chart_tf(TIMEFRAME)
    need_1m = int(NUM_CANDLES * (high_sec / 60)) + 500

    for sym in symbols:
        try:
            df_high = ex.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=NUM_CANDLES)
            if not df_high or len(df_high) < 150:
                continue
            df_high = pd.DataFrame(np.array(df_high, float), columns=["time", "open", "high", "low", "close", "volume"])
            df_high["time"] = pd.to_datetime(df_high["time"], unit="ms", utc=True)

            start_ms = int(df_high["time"].iloc[0].value // 10**6)
            df_1m = fetch_ohlcv_paged(ex, sym, "1m", since_ms=start_ms, limit=1500, max_bars=need_1m)
            if df_1m.empty:
                continue

            df_hi2 = attach_lower_tf_volume_sums(df_high, df_1m, TIMEFRAME)
            res, fvgs, dash = run_indicator(
                df_hi2[["open", "high", "low", "close", "volume", "sumBull", "sumBear", "totalVolume"]],
                PARAMS,
            )

            last_bar = len(res) - 1
            last_time = str(df_hi2["time"].iloc[-1])

            def emit(payload: Dict[str, Any]):
                if PRINT_EVENTS:
                    print("[OUT] " + json.dumps(payload, ensure_ascii=False))

            if TOUCH_LOOKBACK > 0:
                active_now = [x for x in fvgs if x.removed_at_bar is None]
                if active_now:
                    end_idx = len(df_hi2)
                    start_idx = max(0, end_idx - TOUCH_LOOKBACK)
                    highs = df_hi2["high"].to_numpy(float)[start_idx:end_idx]
                    lows = df_hi2["low"].to_numpy(float)[start_idx:end_idx]
                    touched_now = []
                    for f in active_now:
                        key = (sym, f.fvg_id)
                        if key in touched:
                            continue
                        top = float(f.body.top)
                        bottom = float(f.body.bottom)
                        hit = np.any((lows <= top) & (highs >= bottom))
                        if hit:
                            touched_now.append(f)
                    if touched_now:
                        emit(
                            {
                                "symbol": sym,
                                "event": "FVG_FIRST_TOUCH",
                                "tf": TIMEFRAME,
                                "lower_tf": lower_tf,
                                "time": last_time,
                                "count": len(touched_now),
                                "fvgs": [
                                    {
                                        "id": f.fvg_id,
                                        "type": "BULL" if f.is_bull else "BEAR",
                                        "top": float(f.body.top),
                                        "bottom": float(f.body.bottom),
                                        "bull%": format_percent_tv_like(f.bull),
                                        "bear%": format_percent_tv_like(f.bear),
                                        "totalVol": format_volume_tv_like(f.total_vol),
                                    }
                                    for f in touched_now
                                ],
                            }
                        )
                        for f in touched_now:
                            touched.add((sym, f.fvg_id))
        except Exception as exc:
            print(f"[WARN] {sym}: {exc}")

    state["touched"] = touched
    return state


def scan_loop():
    state = {"seen": {}}
    while True:
        state = scan_once(state=state)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    print("== Android Note ==")
    print("- على 1h: يحتاج تحميل 1m لإعادة بناء 6m.")
    print("- لو يتوقف بالخلفية: عطّل Battery Optimization / اترك الشاشة شغالة.")
    print("==================")
    try:
        scan_loop()
    except KeyboardInterrupt:
        print("\n[STOP] تم الإيقاف.")
