#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Xdecow SFT v2.5 Binance Futures structural scanner (lightweight implementation).

- يعتمد فقط على: LSR, OI, Number of Trades, RSI
- يضيف طبقة Posit/Acco عند توفرها
- يطبع المخرجات بالصيغة المختصرة المطلوبة
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

BASE = "https://fapi.binance.com"
TIMEOUT = 8


@dataclass
class SymbolSnapshot:
    symbol: str
    trade_count_24h: float
    quote_volume_24h: float
    rsi: Optional[float]
    oi_delta_pct: Optional[float]
    oi_gain_count: int
    oi_loss_count: int
    lsr_current: Optional[float]
    lsr_improving: bool
    lsr_gain_count: int
    trades_accel: bool
    base_activity_flag: bool
    taker_buy_sell_ratio: Optional[float]
    posit_ratio: Optional[float]
    acco_ratio: Optional[float]


class BinanceFuturesClient:
    def __init__(self) -> None:
        self.user_agent = "Mozilla/5.0 Xdecow-SFT25-Scanner/1.0"

    def get(self, path: str, params: Dict[str, Any]) -> Any:
        query = urllib.parse.urlencode(params)
        url = f"{BASE}{path}?{query}" if query else f"{BASE}{path}"
        last_err = None
        for _ in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": self.user_agent}, method="GET")
                with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                    payload = resp.read().decode("utf-8")
                    return json.loads(payload)
            except Exception as e:  # pylint: disable=broad-except
                last_err = e
            time.sleep(0.35)
        raise RuntimeError(str(last_err))

    def tickers_24h(self) -> List[Dict[str, Any]]:
        data = self.get("/fapi/v1/ticker/24hr", {})
        if isinstance(data, dict):
            return [data]
        return data

    def klines(self, symbol: str, interval: str = "15m", limit: int = 120) -> List[List[Any]]:
        return self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def oi_hist(self, symbol: str, period: str = "15m", limit: int = 30) -> List[Dict[str, Any]]:
        return self.get(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
        )

    def global_lsr(self, symbol: str, period: str = "15m", limit: int = 30) -> List[Dict[str, Any]]:
        return self.get(
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )

    def taker_ratio(self, symbol: str, period: str = "5m", limit: int = 30) -> List[Dict[str, Any]]:
        return self.get(
            "/futures/data/takerlongshortRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )

    def top_position_ratio(self, symbol: str, period: str = "15m", limit: int = 30) -> List[Dict[str, Any]]:
        return self.get(
            "/futures/data/topLongShortPositionRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )

    def top_account_ratio(self, symbol: str, period: str = "15m", limit: int = 30) -> List[Dict[str, Any]]:
        return self.get(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )


def to_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except Exception:  # pylint: disable=broad-except
        return None


def calc_rsi_from_closes(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def build_snapshot(client: BinanceFuturesClient, ticker: Dict[str, Any], min_trades: int) -> Optional[SymbolSnapshot]:
    symbol = ticker.get("symbol", "")
    if not symbol.endswith("USDT"):
        return None

    trade_count = to_float(ticker.get("count")) or 0.0
    quote_vol = to_float(ticker.get("quoteVolume")) or 0.0
    if trade_count < min_trades:
        return None

    try:
        k = client.klines(symbol, "15m", 120)
        closes = [to_float(x[4]) for x in k if len(x) > 4 and to_float(x[4]) is not None]
        rsi = calc_rsi_from_closes(closes)

        oi = client.oi_hist(symbol, "15m", 30)
        oi_vals = [to_float(x.get("sumOpenInterest")) for x in oi]
        oi_vals = [x for x in oi_vals if x and x > 0]
        oi_delta = None
        if len(oi_vals) >= 2:
            oi_delta = ((oi_vals[-1] - oi_vals[0]) / oi_vals[0]) * 100
        oi_gain_count = sum(1 for i in range(1, len(oi_vals)) if oi_vals[i] > oi_vals[i - 1])
        oi_loss_count = sum(1 for i in range(1, len(oi_vals)) if oi_vals[i] < oi_vals[i - 1])

        lsr = client.global_lsr(symbol, "15m", 30)
        lsr_vals = [to_float(x.get("longShortRatio")) for x in lsr]
        lsr_vals = [x for x in lsr_vals if x]
        lsr_current = lsr_vals[-1] if lsr_vals else None
        lsr_improving = len(lsr_vals) >= 3 and (lsr_vals[-1] > statistics.mean(lsr_vals[-3:]))
        lsr_gain_count = sum(1 for i in range(1, len(lsr_vals)) if lsr_vals[i] > lsr_vals[i - 1])

        taker = client.taker_ratio(symbol, "5m", 30)
        taker_vals = [to_float(x.get("buySellRatio")) for x in taker]
        taker_vals = [x for x in taker_vals if x]
        taker_current = taker_vals[-1] if taker_vals else None

        posit = client.top_position_ratio(symbol, "15m", 30)
        posit_vals = [to_float(x.get("longShortRatio")) for x in posit]
        posit_vals = [x for x in posit_vals if x]
        posit_current = posit_vals[-1] if posit_vals else None

        acco = client.top_account_ratio(symbol, "15m", 30)
        acco_vals = [to_float(x.get("longShortRatio")) for x in acco]
        acco_vals = [x for x in acco_vals if x]
        acco_current = acco_vals[-1] if acco_vals else None

        recent_counts = []
        if len(k) > 40:
            # proxy لتسارع التنفيذ: مقارنة حركة آخر 20 شمعة بحركة 20 شمعة قبلها عبر أحجام الإغلاق
            last20 = [to_float(x[5]) or 0.0 for x in k[-20:]]
            prev20 = [to_float(x[5]) or 0.0 for x in k[-40:-20]]
            if sum(prev20) > 0:
                recent_counts.append((sum(last20) / sum(prev20)) > 1.2)
        trades_accel = any(recent_counts)

        return SymbolSnapshot(
            symbol=symbol,
            trade_count_24h=trade_count,
            quote_volume_24h=quote_vol,
            rsi=rsi,
            oi_delta_pct=oi_delta,
            oi_gain_count=oi_gain_count,
            oi_loss_count=oi_loss_count,
            lsr_current=lsr_current,
            lsr_improving=lsr_improving,
            lsr_gain_count=lsr_gain_count,
            trades_accel=trades_accel,
            base_activity_flag=trade_count >= 20_000,
            taker_buy_sell_ratio=taker_current,
            posit_ratio=posit_current,
            acco_ratio=acco_current,
        )
    except Exception:
        return None


def classify(s: SymbolSnapshot) -> Dict[str, str]:
    rsi = s.rsi if s.rsi is not None else 50.0
    oi = s.oi_delta_pct if s.oi_delta_pct is not None else 0.0
    lsr_good = (s.lsr_current or 1.0) > 1.0 or s.lsr_improving

    gold_a = (rsi < 20) and (oi >= 10) and s.trades_accel and lsr_good
    h0_score = 0
    h0_score += 1 if s.trades_accel else 0
    h0_score += 1 if s.oi_loss_count >= 12 else 0
    h0_score += 1 if (s.lsr_gain_count >= 10 or s.lsr_improving) else 0
    h0_score += 1 if s.base_activity_flag else 0

    gold_s = (h0_score >= 3) and (s.oi_loss_count > s.oi_gain_count)

    whale_div = (
        s.posit_ratio is not None
        and s.acco_ratio is not None
        and (s.posit_ratio > 1.0)
        and (s.posit_ratio > s.acco_ratio)
    )

    type_j_early = whale_div and oi > 1.5 and s.trades_accel and (50 <= rsi <= 85)

    flush = (rsi > 85) and (oi < 0) and not lsr_good and s.trades_accel

    if gold_a:
        return {"bucket": "gold_a", "label": "🔥 Gold-A", "conf": "عالية"}
    if gold_s:
        return {"bucket": "gold_s", "label": "🔥 Gold-S", "conf": "عالية" if h0_score == 4 else "متوسطة"}
    if h0_score >= 3:
        return {"bucket": "h0", "label": "🚨 H0 / Pre-Squeeze", "conf": "متوسطة"}
    if type_j_early:
        return {"bucket": "type_j", "label": "🧠 Type J مبكر", "conf": "متوسطة"}
    if flush:
        return {"bucket": "bear", "label": "⚠️ Flush Alert", "conf": "مرتفعة"}

    if oi > 1.5 and s.trades_accel and lsr_good:
        return {"bucket": "bull", "label": "Type 0A", "conf": "متوسطة"}
    if s.trades_accel and oi <= 0 and lsr_good:
        return {"bucket": "watch", "label": "Type 0S", "conf": "مراقبة"}

    return {"bucket": "none", "label": "", "conf": ""}


def main() -> None:
    p = argparse.ArgumentParser(description="Xdecow Binance Futures structural scanner")
    p.add_argument("--top", type=int, default=35, help="عدد العملات الأعلى سيولة لفحصها")
    p.add_argument("--min-trades", type=int, default=3000, help="الحد الأدنى لعدد تداولات 24h")
    p.add_argument("--workers", type=int, default=8, help="عدد المسارات المتوازية")
    args = p.parse_args()

    client = BinanceFuturesClient()
    tickers = client.tickers_24h()
    tickers = sorted(
        [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")],
        key=lambda x: to_float(x.get("quoteVolume")) or 0,
        reverse=True,
    )[: args.top]

    snapshots: List[SymbolSnapshot] = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(build_snapshot, client, t, args.min_trades) for t in tickers]
        for fut in cf.as_completed(futures):
            s = fut.result()
            if s:
                snapshots.append(s)

    buckets: Dict[str, List[str]] = {
        "bull": [],
        "gold_a": [],
        "gold_s": [],
        "h0": [],
        "type_j": [],
        "watch": [],
        "bear": [],
    }

    for s in sorted(snapshots, key=lambda x: x.quote_volume_24h, reverse=True):
        c = classify(s)
        if c["bucket"] == "none":
            continue
        rsi_val = s.rsi if s.rsi is not None else 0.0
        oi_val = s.oi_delta_pct if s.oi_delta_pct is not None else 0.0
        lsr_val = s.lsr_current if s.lsr_current is not None else 1.0
        line = (
            f"· {s.symbol} — {c['label']} — ثقة {c['conf']} "
            f"(RSI={rsi_val:.1f}, OIΔ={oi_val:.2f}%, "
            f"LSR={lsr_val:.3f}, Trades24h={int(s.trade_count_24h)})"
        )
        buckets[c["bucket"]].append(line)

    print("✅ مرشحة للصعود:")
    for x in buckets["bull"]:
        print(x)

    print("\n🔥 Gold-A — انفجار تجميعي وشيك:")
    for x in buckets["gold_a"]:
        print(x)

    print("\n🔥 Gold-S — انفجار سحقي وشيك:")
    for x in buckets["gold_s"]:
        print(x)

    print("\n🚨 H0 / Pre-Squeeze Alert:")
    for x in buckets["h0"]:
        print(x)

    print("\n🧠 Type J — بناء الحيتان المتباين:")
    for x in buckets["type_j"]:
        print(x)

    print("\n📡 مراقبة خاصة:")
    for x in buckets["watch"]:
        print(x)

    print("\n🔻 مرشحة للهبوط:")
    for x in buckets["bear"]:
        print(x)


if __name__ == "__main__":
    main()
