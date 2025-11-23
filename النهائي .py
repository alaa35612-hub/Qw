#!/usr/bin/env python3
""" ماسح Golden Zone بسيط لكل أزواج Binance USDT-M.

الغرض:
    - حذف جميع الاستراتيجيات والتنبيهات والأحداث السابقة.
    - فحص كل الأزواج التي لا تزال أسعارها ضمن منطقة "Golden Zone" (الجيب الذهبي
      0.618 → 0.65 من آخر حركة رئيسية) عبر مجموعة إطارات زمنية دفعة واحدة:
      1m, 5m, 15m, 1h, 4h, 1d.
    - أي زوج لا يطابق الشرط على أي إطار يُستبعد فورًا.
    - إظهار التنبيه مباشرة في شاشة المحرر بلون أزرق لتمييز النتائج الصالحة.

الملاحظات:
    * يعتمد على ccxt (Binance USDT-M) للبيانات الحية. تأكد من وجود اتصال.
    * يتم تحديد آخر حركة رئيسية من خلال أعلى قمة وأدنى قاع في نافذة تاريخية
      محددة (افتراضيًا آخر 300 شمعة لكل إطار). إذا تساوى موقع القمة والقاع أو
      لم يكن هناك بيانات كافية، يتم تجاهل الزوج لهذا الإطار.
    * "Golden Zone" هنا هي جيب فيبوناتشي الشائع بين 61.8% و 65% من آخر حركة.

تشغيل سريع:
    python "النهائي .py"

خيارات CLI:
    --symbols BTCUSDT,ETHUSDT   لتحديد قائمة محددة من الأزواج.
    --limit 50                  لحصر عدد الأزواج الممسوحة من القائمة الكاملة.
    --lookback 300              لتعديل عدد الشموع المستخدمة في حساب الحركة.
    --workers 8                 عدد مؤشرات الترابط لتسريع الفحص.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import sys
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - ccxt مطلوب للوضع الحي
    ccxt = None  # type: ignore

ANSI_BLUE = "\033[94m"
ANSI_RESET = "\033[0m"

GOLDEN_ZONE_MIN = 0.618
GOLDEN_ZONE_MAX = 0.65
DEFAULT_TIMEFRAMES: Tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h", "1d")


@dataclass
class GoldenZoneState:
    """وضع زوج واحد عبر إطار زمني محدد."""

    timeframe: str
    price: float
    zone_low: float
    zone_high: float
    direction: str

    @property
    def in_zone(self) -> bool:
        return self.zone_low <= self.price <= self.zone_high


@dataclass
class SymbolGoldenZone:
    """نتيجة الفحص لزوج عبر جميع الإطارات الزمنية المطلوبة."""

    symbol: str
    frames: List[GoldenZoneState]

    @property
    def valid(self) -> bool:
        return all(frame.in_zone for frame in self.frames)


def _ensure_ccxt() -> "ccxt.binanceusdm":
    if ccxt is None:
        raise RuntimeError("ccxt غير متوفر. رجاءً ثبّت الحزمة: pip install ccxt")
    return ccxt.binanceusdm({"enableRateLimit": True})


def load_usdtm_symbols(exchange: "ccxt.binanceusdm", limit: Optional[int] = None) -> List[str]:
    markets = exchange.load_markets()
    symbols: List[str] = []
    for m in markets.values():
        if m.get("quote") != "USDT":
            continue
        if not m.get("linear"):
            continue
        if not m.get("active", True):
            continue
        symbols.append(m["id"])
    symbols.sort()
    if limit is not None:
        return symbols[:limit]
    return symbols


def fetch_ohlcv(
    exchange: "ccxt.binanceusdm", symbol: str, timeframe: str, lookback: int
) -> List[List[float]]:
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=lookback)


def _swing_points(ohlcv: Sequence[Sequence[float]]) -> Tuple[int, int]:
    highs = [row[2] for row in ohlcv]
    lows = [row[3] for row in ohlcv]
    high_idx = max(range(len(highs)), key=highs.__getitem__)
    low_idx = min(range(len(lows)), key=lows.__getitem__)
    return high_idx, low_idx


def golden_zone_for_ohlcv(ohlcv: Sequence[Sequence[float]]) -> Optional[Tuple[float, float, str]]:
    if len(ohlcv) < 2:
        return None
    high_idx, low_idx = _swing_points(ohlcv)
    if high_idx == low_idx:
        return None

    high = ohlcv[high_idx][2]
    low = ohlcv[low_idx][3]

    move = high - low
    if move <= 0:
        return None

    if low_idx < high_idx:
        # حركة صاعدة: حساب التصحيح من القمة نزولاً
        upper = high - move * GOLDEN_ZONE_MIN
        lower = high - move * GOLDEN_ZONE_MAX
        direction = "bullish"
    else:
        # حركة هابطة: حساب التصحيح من القاع صعوداً
        lower = low + move * GOLDEN_ZONE_MIN
        upper = low + move * GOLDEN_ZONE_MAX
        direction = "bearish"

    zone_low, zone_high = sorted((lower, upper))
    return zone_low, zone_high, direction


def evaluate_symbol(
    exchange: "ccxt.binanceusdm",
    symbol: str,
    timeframes: Iterable[str],
    lookback: int,
) -> Optional[SymbolGoldenZone]:
    frames: List[GoldenZoneState] = []
    for tf in timeframes:
        candles = fetch_ohlcv(exchange, symbol, tf, lookback)
        if len(candles) < 2:
            return None
        zone = golden_zone_for_ohlcv(candles)
        if zone is None:
            return None
        zone_low, zone_high, direction = zone
        price = candles[-1][4]
        frames.append(GoldenZoneState(tf, price, zone_low, zone_high, direction))
        if not frames[-1].in_zone:
            return None
    return SymbolGoldenZone(symbol, frames)


def scan_symbols(
    exchange: "ccxt.binanceusdm",
    symbols: Sequence[str],
    timeframes: Sequence[str],
    lookback: int,
    workers: int,
) -> List[SymbolGoldenZone]:
    results: List[SymbolGoldenZone] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(evaluate_symbol, exchange, sym, timeframes, lookback): sym
            for sym in symbols
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
            except Exception as exc:  # pragma: no cover - تسجيل الخطأ ومتابعة
                print(f"[⚠️] تخطي {futures[future]}: {exc}")
                continue
            if res is not None and res.valid:
                results.append(res)
    results.sort(key=lambda r: r.symbol)
    return results


def _print_result(results: Sequence[SymbolGoldenZone]) -> None:
    if not results:
        print("لا توجد أزواج ضمن منطقة Golden Zone على جميع الإطارات المحددة.")
        return

    print(f"{ANSI_BLUE}الأزواج داخل Golden Zone (1m/5m/15m/1h/4h/1d):{ANSI_RESET}")
    for res in results:
        frame_parts = []
        for frame in res.frames:
            frame_parts.append(
                f"{frame.timeframe}: السعر={frame.price:.4f} "
                f"المنطقة=[{frame.zone_low:.4f} → {frame.zone_high:.4f}]"
            )
        joined = " | ".join(frame_parts)
        print(f"{ANSI_BLUE}[تنبيه]{ANSI_RESET} {res.symbol} — {joined}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ماسح Golden Zone لإطارات متعددة")
    p.add_argument(
        "--symbols",
        type=str,
        help="قائمة رموز مفصولة بفواصل (مثل: BTCUSDT,ETHUSDT). افتراضيًا يتم فحص جميع USDT-M."
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="الحد الأقصى لعدد الأزواج عند استخدام القائمة الكاملة من بينانس.",
    )
    p.add_argument(
        "--lookback",
        type=int,
        default=300,
        help="عدد الشموع المستخدمة لتحديد الحركة الرئيسية لكل إطار.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="عدد مؤشرات الترابط للفحص المتوازي.",
    )
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        exchange = _ensure_ccxt()
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = load_usdtm_symbols(exchange, limit=args.limit)

    if not symbols:
        print("لم يتم العثور على أزواج USDT-M مطابقة.")
        return 0

    results = scan_symbols(exchange, symbols, DEFAULT_TIMEFRAMES, args.lookback, args.workers)
    _print_result(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
