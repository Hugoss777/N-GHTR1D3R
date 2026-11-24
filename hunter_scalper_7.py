"""
HUNTER SCALPER 7.1 - внутридневные сигналы
BingX + OKX, USDT linear perpetuals.
Только сигналы и мониторинг: бот НЕ открывает ордера.

7.1 (срочные правки: бот не останавливался при серии убытков)
  * АВТОСТОП: пауза новых сигналов после N стопов подряд, при дневном лимите убытка (R)
    и при плохом среднем результате последних сделок; повторные срабатывания за сутки
    удлиняют паузу (x2, x4, до 24 ч). Слабая стратегия или монета отключаются отдельно.
  * Не больше MAX_SAME_SIDE сигналов в одну сторону одновременно (альты ходят вместе с BTC).
  * По умолчанию строже к комиссиям: MIN_RR 1.0, MAX_FEE_R 0.25, MAX_OPEN_SIGNALS 3.
  * Состояние автостопа хранится в state и переживает перезапуск бота.

Что изменилось по сравнению с 6.0
  * Сигнал = ЗОНА ВХОДА + ОДИН TP + СТОП + СРОК ЖИЗНИ.
  * Жизненный цикл: PENDING -> ACTIVE -> TP / STOP / TIMEOUT,
    либо EXPIRED / CANCELLED (если вход так и не случился).
  * Монитор идёт по high/low 1m-свечей (фитили внутри минуты не пропускаются).
  * Две стратегии: "тренд + откат" (EMA20/VWAP) и "sweep + reclaim".
  * Скан только при закрытии новой 5m-свечи; 15m и 1h кэшируются.
  * Фильтры комиссий: TP должен быть >= FEE_MULT * taker-комиссия, комиссии <= MAX_FEE_R от риска.
  * CSV-журнал: результат каждого сигнала в R (gross и net после комиссий) + статистика.

Установка:
    pip install ccxt pandas numpy requests

Запуск:
    python hunter_scalper_7.py            # бот
    python hunter_scalper_7.py stats      # статистика по журналу за всё время
    python hunter_scalper_7.py stats 7    # статистика за 7 дней

Настройки: файл hunter_scalper_config.yml рядом с ботом (путь можно сменить через CONFIG_FILE).
Переменные окружения с теми же именами главнее файла. Всё необязательно:
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
    EXCHANGES=bingx,okx        MAX_SYMBOLS=30         MIN_VOLUME=3000000
    MIN_SCORE=65               SCAN_SECONDS=30        COOLDOWN_MINUTES=30
    ENTRY_TTL_MIN=15           (сколько ждём вход в зону)
    TRADE_TTL_MIN=120          (максимум удержания после входа)
    MIN_RR=0.8  MAX_RR=1.5     (границы R для единственного TP)
    TAKER_FEE_PCT=0.05  FEE_MULT=4  MAX_FEE_R=0.35
    ADX_MIN=20  VOL_MIN_RATIO=1.1
    MAX_SPREAD_PCT=0.10  MAX_STOP_PCT=1.0  MIN_ATR_PCT=0.10  MAX_ATR_PCT=1.2
    STRATEGIES=pullback,sweep  TRADE_HOURS=9-23  (часы по TIMEZONE, пусто = всегда)
    MAX_OPEN_SIGNALS=5  MAX_SIGNAL_AGE_SEC=150
    RUN_ONCE=true              (один цикл и выход, для проверки)

Допущения журнала (консервативные):
  * Вход считается по худшей границе зоны (верх зоны для LONG, низ для SHORT).
  * На свече входа проверяется только стоп; TP - со следующей свечи.
  * Если в одной 1m-свече задеты и стоп, и TP, засчитывается стоп.
  * Выход по стопу/TP - ровно по цене уровня, без проскальзывания.
"""

import os
import sys
import csv
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
import ccxt
import numpy as np
import pandas as pd

# ----------------------------- CONFIG FILE (YAML) -----------------------------
# Приоритет настроек: переменная окружения > hunter_scalper_config.yml > значение по умолчанию.

CONFIG_FILE = Path(os.environ.get("CONFIG_FILE", "hunter_scalper_config.yml"))


def _parse_flat_yaml(text):
    """Запасной парсер для плоского 'KEY: value' (если PyYAML не установлен)."""
    data = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, _, value = line.partition(":")
        value = value.strip()
        if value[:1] in ("'", '"'):
            quote = value[0]
            end = value.find(quote, 1)
            value = value[1:end] if end > 0 else value[1:]
        else:
            value = value.split(" #", 1)[0].strip()
        data[key.strip()] = value
    return data


def _load_yaml_config(path):
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
        try:
            import yaml
            data = yaml.safe_load(text) or {}
        except ImportError:
            data = _parse_flat_yaml(text)
        if not isinstance(data, dict):
            raise ValueError("config root must be a mapping 'KEY: value'")
        return {str(k).strip().upper(): v for k, v in data.items() if v is not None}
    except Exception as exc:
        print(f"WARNING: cannot read config {path}: {exc}", file=sys.stderr)
        return {}


YAML_CFG = _load_yaml_config(CONFIG_FILE)


def cfg(name, default=""):
    """Значение настройки как строка: env -> yml -> default."""
    if name in os.environ:
        return os.environ[name]
    value = YAML_CFG.get(name)
    if value is None:
        return default
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    return str(value)


# ----------------------------- CONFIG -----------------------------

TZ = ZoneInfo(cfg("TIMEZONE", "Europe/Istanbul"))
STATE_FILE = Path(cfg("STATE_FILE", "hunter_scalper7_state.json"))
JOURNAL_FILE = Path(cfg("JOURNAL_FILE", "hunter_scalper_journal.csv"))
LOG_FILE = cfg("LOG_FILE", "hunter_scalper.log")

TG_TOKEN = cfg("TELEGRAM_TOKEN", "")
TG_CHAT = cfg("TELEGRAM_CHAT_ID", "")

EXCHANGE_NAMES = [
    x.strip().lower()
    for x in cfg("EXCHANGES", "bingx,okx").split(",")
    if x.strip()
]
STRATEGIES = {x.strip() for x in cfg("STRATEGIES", "pullback,sweep").split(",") if x.strip()}

MIN_SCORE = int(cfg("MIN_SCORE", "65"))
MAX_SYMBOLS = int(cfg("MAX_SYMBOLS", "30"))
MIN_VOLUME = float(cfg("MIN_VOLUME", "3000000"))
SCAN_SECONDS = max(10, int(cfg("SCAN_SECONDS", "30")))
COOLDOWN_MINUTES = max(1, int(cfg("COOLDOWN_MINUTES", "30")))
MAX_SPREAD_PCT = float(cfg("MAX_SPREAD_PCT", "0.10"))
MAX_STOP_PCT = float(cfg("MAX_STOP_PCT", "1.0"))
MIN_ATR_PCT = float(cfg("MIN_ATR_PCT", "0.10"))
MAX_ATR_PCT = float(cfg("MAX_ATR_PCT", "1.2"))
ADX_MIN = float(cfg("ADX_MIN", "20"))
VOL_MIN_RATIO = float(cfg("VOL_MIN_RATIO", "1.1"))

ENTRY_TTL_MIN = int(cfg("ENTRY_TTL_MIN", "15"))
TRADE_TTL_MIN = int(cfg("TRADE_TTL_MIN", "120"))
MIN_RR = float(cfg("MIN_RR", "1.0"))
MAX_RR = float(cfg("MAX_RR", "1.5"))

TAKER_FEE_PCT = float(cfg("TAKER_FEE_PCT", "0.05"))
FEE_MULT = float(cfg("FEE_MULT", "4"))
MIN_TP_PCT = FEE_MULT * TAKER_FEE_PCT
MAX_FEE_R = float(cfg("MAX_FEE_R", "0.25"))

MAX_OPEN_SIGNALS = int(cfg("MAX_OPEN_SIGNALS", "3"))
MAX_SAME_SIDE = int(cfg("MAX_SAME_SIDE", "2"))   # альты коррелируют с BTC: 5 лонгов = одна ставка x5

# --- Автостоп (risk guard). Считается по net R (после комиссий), только по сделкам с входом ---
GUARD_ENABLED = cfg("GUARD_ENABLED", "true").lower() in ("1", "true", "yes")
MAX_CONSEC_LOSSES = int(cfg("MAX_CONSEC_LOSSES", "3"))       # убытков подряд -> пауза
MAX_DAILY_LOSS_R = float(cfg("MAX_DAILY_LOSS_R", "4.0"))     # убыток за день (R) -> пауза до полуночи
ROLL_N = int(cfg("ROLL_N", "20"))                            # окно скользящей проверки
ROLL_MIN_AVG_R = float(cfg("ROLL_MIN_AVG_R", "-0.15"))       # средний net R окна ниже -> пауза
PAUSE_MINUTES = int(cfg("PAUSE_MINUTES", "180"))             # базовая пауза; повторные срабатывания за сутки x2, x4... (макс. 24ч)
STRAT_ROLL_N = int(cfg("STRAT_ROLL_N", "10"))                # окно для отдельной стратегии
STRAT_MIN_AVG_R = float(cfg("STRAT_MIN_AVG_R", "-0.30"))     # стратегия хуже -> отключается
STRAT_PAUSE_HOURS = int(cfg("STRAT_PAUSE_HOURS", "24"))
SYMBOL_MAX_LOSSES = int(cfg("SYMBOL_MAX_LOSSES", "2"))       # убытков подряд по монете -> монета в бан
SYMBOL_PAUSE_HOURS = int(cfg("SYMBOL_PAUSE_HOURS", "12"))
MAX_DAILY_STOPS = int(cfg("MAX_DAILY_STOPS", "3"))           # стопов за день -> пауза до полуночи
STOP_BUFFER_ATR = float(cfg("STOP_BUFFER_ATR", "0.1"))       # запас за ближайшим хаем/лоем, в ATR
TP_BUFFER_ATR = float(cfg("TP_BUFFER_ATR", "0.05"))          # TP чуть не доходит до уровня, в ATR
STRUCT_LOOKBACK = int(cfg("STRUCT_LOOKBACK", "40"))          # окно поиска хаев/лоёв для стопа
TP_LOOKBACK = int(cfg("TP_LOOKBACK", "60"))                  # окно поиска первого уровня для TP
FRACTAL_K = int(cfg("FRACTAL_K", "2"))                       # свечей с каждой стороны для подтверждения хая/лоя
MAX_SIGNAL_AGE_SEC = int(cfg("MAX_SIGNAL_AGE_SEC", "150"))
TRADE_HOURS = cfg("TRADE_HOURS", "").strip()

TF_ENTRY, TF_TREND, TF_REGIME = "5m", "15m", "1h"
TF_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000}
OHLCV_LIMIT = int(cfg("OHLCV_LIMIT", "300"))
MIN_ROWS = 200
CANDLE_DELAY_MS = 2_000
SYMBOLS_TTL = 120
PULLBACK_BARS = 6

OPEN_STATUSES = ("PENDING", "ACTIVE")

logging.basicConfig(
    level=cfg("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")]
)
log = logging.getLogger("HUNTER_SCALPER")

# Рантайм-кэши (не сохраняются на диск).
FRAME_CACHE = {}      # (exchange, symbol, tf) -> (expire_ms, df)
SYMBOL_CACHE = {}     # exchange -> (expire_ts, symbols)
SCANNED = {}          # (exchange, symbol) -> ts последней просканированной 5m-свечи
SCAN_DONE = {}        # exchange -> ts 5m-свечи, по которой скан завершён


# ----------------------------- STATE / UTILITIES -----------------------------

def now():
    return datetime.now(TZ)


def now_ms():
    return int(time.time() * 1000)


def hhmm(ms):
    return datetime.fromtimestamp(ms / 1000, TZ).strftime("%H:%M")


def fmt(value):
    return f"{float(value):.8g}"


def default_state():
    return {"signals": {}, "last_signal": {}, "daily": {}, "last_report": None,
            "results": [], "guard": {}}


def load_state():
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("State root is not an object")
        defaults = default_state()
        for key, value in defaults.items():
            if not isinstance(data.get(key), type(value)) and not (key == "last_report" and data.get(key) is None):
                data[key] = value
        return data
    except (OSError, ValueError, TypeError):
        return default_state()


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_FILE)


def daily_stats(state):
    key = now().strftime("%Y-%m-%d")
    return state["daily"].setdefault(key, {
        "scanned": 0, "signals": 0, "rejected": 0, "errors": 0
    })


def telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        log.info("Telegram not configured; message:\n%s", text)
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "disable_web_page_preview": True},
            timeout=15
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload))
        return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def in_trade_hours():
    """TRADE_HOURS='9-23' -> торгуем с 09:00 до 22:59 по TIMEZONE. Пусто = всегда."""
    if not TRADE_HOURS:
        return True
    try:
        start, end = (int(x) for x in TRADE_HOURS.split("-"))
    except ValueError:
        return True
    hour = now().hour
    if start <= end:
        return start <= hour < end
    return hour >= start or hour < end


# ----------------------------- EXCHANGES / MARKETS -----------------------------

def make_exchange(name):
    exchange_class = getattr(ccxt, name, None)
    if exchange_class is None:
        raise ValueError(f"CCXT exchange not found: {name}")
    return exchange_class({
        "enableRateLimit": True,
        "timeout": 25000,
        "options": {"defaultType": "swap"},
    })


def load_exchanges():
    result = {}
    for name in EXCHANGE_NAMES:
        try:
            exchange = make_exchange(name)
            exchange.load_markets()
            result[name] = exchange
            log.info("Connected to public market data: %s", name)
        except Exception as exc:
            log.exception("Cannot initialize %s: %s", name, exc)
    return result


def is_linear_usdt_swap(market):
    return (
        market.get("active") is not False
        and market.get("swap")
        and market.get("linear")
        and market.get("quote") == "USDT"
        and market.get("settle") == "USDT"
    )


def get_symbols(exchange):
    tickers = exchange.fetch_tickers()
    ranked = []
    for symbol, market in exchange.markets.items():
        if not is_linear_usdt_swap(market):
            continue
        ticker = tickers.get(symbol) or {}
        try:
            quote_volume = ticker.get("quoteVolume")
            if quote_volume is None:
                quote_volume = float(ticker.get("baseVolume") or 0) * float(ticker.get("last") or 0)
            quote_volume = float(quote_volume or 0)
            last = float(ticker.get("last") or 0)
            bid, ask = float(ticker.get("bid") or 0), float(ticker.get("ask") or 0)
            spread_pct = ((ask - bid) / ((ask + bid) / 2) * 100) if bid > 0 and ask > 0 else 999
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if quote_volume >= MIN_VOLUME and last > 0 and spread_pct <= MAX_SPREAD_PCT:
            ranked.append((symbol, quote_volume, spread_pct))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked[:MAX_SYMBOLS]


def get_symbols_cached(exchange):
    hit = SYMBOL_CACHE.get(exchange.id)
    if hit and time.time() < hit[0]:
        return hit[1]
    symbols = get_symbols(exchange)
    SYMBOL_CACHE[exchange.id] = (time.time() + SYMBOLS_TTL, symbols)
    return symbols


# ----------------------------- INDICATORS -----------------------------

def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = losses.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    value = 100 - 100 / (1 + rs)
    value = value.mask((avg_loss == 0) & (avg_gain > 0), 100)
    value = value.mask((avg_gain == 0) & (avg_loss > 0), 0)
    return value


def true_range(df):
    previous_close = df.close.shift(1)
    return pd.concat([
        df.high - df.low,
        (df.high - previous_close).abs(),
        (df.low - previous_close).abs()
    ], axis=1).max(axis=1)


def adx_dmi(df, true_rng, period=14):
    up = df.high.diff()
    down = -df.low.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    alpha = 1 / period
    atr_w = true_rng.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean() / atr_w
    minus_di = 100 * minus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean() / atr_w
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_value = dx.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    return adx_value, plus_di, minus_di


def prepare_ohlcv(rows, tf_ms, now_ts_ms):
    """Только ЗАКРЫТЫЕ свечи (формирующаяся отбрасывается по времени, а не слепо по iloc[-1])."""
    if not rows or len(rows) < MIN_ROWS:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna()
    df = df[df.timestamp + tf_ms <= now_ts_ms]
    df = df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
    if len(df) < MIN_ROWS - 5:
        return pd.DataFrame()

    df["ema9"] = ema(df.close, 9)
    df["ema20"] = ema(df.close, 20)
    df["ema50"] = ema(df.close, 50)
    df["ema200"] = ema(df.close, 200)
    df["rsi"] = rsi(df.close)

    tr = true_range(df)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    df["adx"], df["plus_di"], df["minus_di"] = adx_dmi(df, tr)

    df["vol_avg"] = df.volume.rolling(20).mean()
    df["vol_ratio"] = df.volume / df.vol_avg.replace(0, np.nan)

    typical = (df.high + df.low + df.close) / 3
    df["vwap"] = (typical * df.volume).rolling(72).sum() / df.volume.rolling(72).sum().replace(0, np.nan)

    return df.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)


def get_frame(exchange, symbol, timeframe):
    """Кэш свечей до закрытия следующей свечи этого ТФ: 15m/1h не перезапрашиваются каждую минуту."""
    tf_ms = TF_MS[timeframe]
    key = (exchange.id, symbol, timeframe)
    ts = now_ms()
    hit = FRAME_CACHE.get(key)
    if hit and ts < hit[0]:
        return hit[1]
    rows = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=OHLCV_LIMIT)
    df = prepare_ohlcv(rows, tf_ms, ts)
    expected_last = (ts // tf_ms - 1) * tf_ms
    fresh = (not df.empty) and int(df.timestamp.iloc[-1]) >= expected_last
    expire = (ts // tf_ms + 1) * tf_ms + CANDLE_DELAY_MS if fresh else ts + 5_000
    FRAME_CACHE[key] = (expire, df)
    return df


def market_regime(df):
    if df.empty:
        return "UNKNOWN"
    x = df.iloc[-1]
    if x.close > x.ema50 > x.ema200:
        return "BULLISH"
    if x.close < x.ema50 < x.ema200:
        return "BEARISH"
    return "SIDEWAYS"


def candle_parts(x):
    """Доли свечи: тело, нижний и верхний фитиль (от полного диапазона)."""
    rng = float(x.high - x.low)
    if rng <= 0:
        return 0.0, 0.0, 0.0
    body = abs(float(x.close - x.open)) / rng
    lower = (min(float(x.open), float(x.close)) - float(x.low)) / rng
    upper = (float(x.high) - max(float(x.open), float(x.close))) / rng
    return body, lower, upper


# ----------------------------- STRATEGIES -----------------------------

def common_adjust(side, btc_regime, spread_pct):
    delta, notes = 0, []
    want = "BULLISH" if side == "LONG" else "BEARISH"
    against = "BEARISH" if side == "LONG" else "BULLISH"
    if btc_regime == want:
        delta += 8
        notes.append("BTC: режим поддерживает сделку")
    elif btc_regime == against:
        delta -= 12
        notes.append("BTC: режим против сделки")
    if spread_pct <= MAX_SPREAD_PCT * 0.5:
        delta += 5
        notes.append(f"Спред низкий: {spread_pct:.3f}%")
    else:
        delta -= 3
        notes.append(f"Спред повышенный: {spread_pct:.3f}%")
    return delta, notes


def pullback_setup(side, entry_df, trend_df, regime_df, btc_regime, spread_pct):
    """Тренд (1H + 15M + ADX) -> откат к EMA20/VWAP на 5M -> подтверждающая свеча."""
    up = side == "LONG"
    e, p = entry_df.iloc[-1], entry_df.iloc[-2]
    t, h = trend_df.iloc[-1], regime_df.iloc[-1]
    atr = float(e.atr)
    body, lower_wick, upper_wick = candle_parts(e)
    recent = entry_df.tail(PULLBACK_BARS)

    if t.adx < ADX_MIN or e.vol_ratio < VOL_MIN_RATIO:
        return None

    if up:
        if not (h.ema20 > h.ema50 and h.close > h.ema50):
            return None
        if not (t.ema20 > t.ema50 and t.close > t.ema50 and t.plus_di > t.minus_di):
            return None
        pb_extreme = float(recent.low.min())
        touched = pb_extreme <= e.ema20 + 0.25 * atr
        depth = float(entry_df.high.tail(20).max()) - pb_extreme
        intact = pb_extreme >= e.ema50 - 0.2 * atr
        confirm = e.close > e.open and e.close > e.ema20 and e.close > e.ema9 and body >= 0.4
        not_chasing = (e.close - e.ema20) <= 0.7 * atr
        rsi_ok = 40 <= e.rsi <= 65
    else:
        if not (h.ema20 < h.ema50 and h.close < h.ema50):
            return None
        if not (t.ema20 < t.ema50 and t.close < t.ema50 and t.minus_di > t.plus_di):
            return None
        pb_extreme = float(recent.high.max())
        touched = pb_extreme >= e.ema20 - 0.25 * atr
        depth = pb_extreme - float(entry_df.low.tail(20).min())
        intact = pb_extreme <= e.ema50 + 0.2 * atr
        confirm = e.close < e.open and e.close < e.ema20 and e.close < e.ema9 and body >= 0.4
        not_chasing = (e.ema20 - e.close) <= 0.7 * atr
        rsi_ok = 35 <= e.rsi <= 60

    if not (touched and depth >= 0.6 * atr and intact and confirm and not_chasing and rsi_ok):
        return None

    close = float(e.close)
    if up:
        support = max(float(e.ema20), float(e.vwap)) if e.vwap <= e.close else float(e.ema20)
        zone_low = max(support, close - 0.7 * atr)
        zone_high = close + 0.1 * atr
        stop = structural_stop(entry_df, "LONG", zone_low, zone_high, atr)
    else:
        resistance = min(float(e.ema20), float(e.vwap)) if e.vwap >= e.close else float(e.ema20)
        zone_high = min(resistance, close + 0.7 * atr)
        zone_low = close - 0.1 * atr
        stop = structural_stop(entry_df, "SHORT", zone_low, zone_high, atr)

    if stop is None:
        return None
    score = 45
    reasons = [
        f"1H и 15M: тренд {'вверх' if up else 'вниз'}, ADX15 = {t.adx:.0f}",
        f"5M: откат к EMA20 и подтверждающая {'бычья' if up else 'медвежья'} свеча",
    ]
    if t.adx >= 25:
        score += 8
        reasons.append("Сильный тренд: ADX ≥ 25")
    if e.vol_ratio >= 1.5:
        score += 8
        reasons.append(f"Объём {e.vol_ratio:.2f}x от среднего")
    else:
        score += 4
        reasons.append(f"Объём {e.vol_ratio:.2f}x от среднего")
    if (e.close > e.vwap) if up else (e.close < e.vwap):
        score += 6
        reasons.append("Цена по нужную сторону VWAP")
    if (lower_wick if up else upper_wick) >= 0.3:
        score += 5
        reasons.append("Отскок от уровня с фитилём")
    if (45 <= e.rsi <= 58) if up else (42 <= e.rsi <= 55):
        score += 4
        reasons.append(f"RSI {e.rsi:.1f}: запас хода есть")
    delta, notes = common_adjust(side, btc_regime, spread_pct)
    score += delta
    reasons += notes

    return {
        "strategy": "pullback", "side": side, "score": max(0, min(100, int(round(score)))),
        "reasons": reasons, "zone_low": zone_low, "zone_high": zone_high, "stop": stop,
    }


def sweep_setup(side, entry_df, trend_df, regime_df, btc_regime, spread_pct):
    """Снятие локального хая/лоя (~4 часа) и возврат за уровень на закрытой 5M-свече."""
    up = side == "LONG"
    e = entry_df.iloc[-1]
    t, h = trend_df.iloc[-1], regime_df.iloc[-1]
    atr = float(e.atr)
    body, lower_wick, upper_wick = candle_parts(e)
    ref = entry_df.iloc[-52:-3]
    if len(ref) < 30 or e.vol_ratio < 1.3:
        return None

    if up:
        if h.close < h.ema50 < h.ema200:
            return None  # сильный нисходящий режим на 1H - лонг от лоя не берём
        level = float(ref.low.min())
        swept = e.low < level - 0.05 * atr and e.close > level
        depth = level - float(e.low)
        wick = lower_wick
        direction_ok = e.close > e.open or wick >= 0.55
    else:
        if h.close > h.ema50 > h.ema200:
            return None
        level = float(ref.high.max())
        swept = e.high > level + 0.05 * atr and e.close < level
        depth = float(e.high) - level
        wick = upper_wick
        direction_ok = e.close < e.open or wick >= 0.55

    if not (swept and direction_ok and wick >= 0.4 and 0.05 * atr <= depth <= 1.5 * atr
            and abs(float(e.close) - level) <= 1.0 * atr):
        return None

    close = float(e.close)
    if up:
        zone_low = max(level, close - 0.7 * atr)
        zone_high = close + 0.1 * atr
        stop = structural_stop(entry_df, "LONG", zone_low, zone_high, atr)
    else:
        zone_high = min(level, close + 0.7 * atr)
        zone_low = close - 0.1 * atr
        stop = structural_stop(entry_df, "SHORT", zone_low, zone_high, atr)

    if stop is None:
        return None
    score = 45
    reasons = [f"5M: снят {'лой' if up else 'хай'} последних ~4ч ({fmt(level)}) и возврат за уровень"]
    if e.vol_ratio >= 1.8:
        score += 10
        reasons.append(f"Всплеск объёма {e.vol_ratio:.2f}x")
    else:
        score += 5
        reasons.append(f"Объём {e.vol_ratio:.2f}x от среднего")
    if wick >= 0.55:
        score += 8
        reasons.append(f"Длинный фитиль снятия ({wick * 100:.0f}% свечи)")
    if (h.ema20 > h.ema50) if up else (h.ema20 < h.ema50):
        score += 8
        reasons.append("1H: структура поддерживает сделку")
    if (t.close > t.ema50) if up else (t.close < t.ema50):
        score += 6
        reasons.append("15M: цена по нужную сторону EMA50")
    if (e.rsi <= 40) if up else (e.rsi >= 60):
        score += 5
        reasons.append(f"RSI {e.rsi:.1f}: рынок перегнан в сторону снятия")
    delta, notes = common_adjust(side, btc_regime, spread_pct)
    score += delta
    reasons += notes

    return {
        "strategy": "sweep", "side": side, "score": max(0, min(100, int(round(score)))),
        "reasons": reasons, "zone_low": zone_low, "zone_high": zone_high, "stop": stop,
    }


STRATEGY_FUNCS = {"pullback": pullback_setup, "sweep": sweep_setup}


# ----------------------------- LEVELS: ONE TP -----------------------------

def fractals(df, lookback, k=None):
    """Подтверждённые хаи и лои: свеча выше/ниже k соседей с каждой стороны. Последние k свечей не подтверждены."""
    k = FRACTAL_K if k is None else k
    d = df.tail(lookback + 2 * k + 1)
    h = d.high.to_numpy(dtype=float)
    l = d.low.to_numpy(dtype=float)
    highs, lows = [], []
    for i in range(k, len(d) - k):
        if h[i] >= h[i - k:i + k + 1].max():
            highs.append(float(h[i]))
        if l[i] <= l[i - k:i + k + 1].min():
            lows.append(float(l[i]))
    return highs, lows


def structural_stop(df, side, zone_low, zone_high, atr):
    """
    Стоп за БЛИЖАЙШИМ хаем (шорт) или лоем (лонг) относительно зоны входа, с запасом STOP_BUFFER_ATR.
    Учитываются подтверждённые фракталы и экстремум последних 3 свечей. Нет уровня - None (сигнала нет).
    """
    buf = STOP_BUFFER_ATR * atr
    highs, lows = fractals(df, STRUCT_LOOKBACK)
    if side == "SHORT":
        cands = [x for x in highs if x > zone_high]
        recent = float(df.high.tail(3).max())
        if recent > zone_high:
            cands.append(recent)
        return (min(cands) + buf) if cands else None
    cands = [x for x in lows if x < zone_low]
    recent = float(df.low.tail(3).min())
    if recent < zone_low:
        cands.append(recent)
    return (max(cands) - buf) if cands else None


def choose_tp(entry_df, side, entry_ref, stop, atr):
    """
    Один TP: до ПЕРВОГО подтверждённого хая (лонг) или лоя (шорт) от входа, но не дальше MAX_RR.
    Уровня нет или до него меньше MIN_RR - сигнала нет.
    """
    up = side == "LONG"
    risk = (entry_ref - stop) if up else (stop - entry_ref)
    if risk <= 0:
        return None
    highs, lows = fractals(entry_df, TP_LOOKBACK)
    if up:
        levels = [x for x in highs if x > entry_ref]
        if not levels:
            return None
        tp = min(min(levels) - TP_BUFFER_ATR * atr, entry_ref + MAX_RR * risk)
        reward = tp - entry_ref
    else:
        levels = [x for x in lows if x < entry_ref]
        if not levels:
            return None
        tp = max(max(levels) + TP_BUFFER_ATR * atr, entry_ref - MAX_RR * risk)
        reward = entry_ref - tp
    rr = reward / risk
    reward_pct = reward / entry_ref * 100
    if rr < MIN_RR or reward_pct < MIN_TP_PCT:
        return None
    return tp, rr, reward_pct


def build_candidate(setup, entry_df, atr_pct):
    up = setup["side"] == "LONG"
    atr = float(entry_df.iloc[-1].atr)
    entry_ref = setup["zone_high"] if up else setup["zone_low"]   # худшая граница зоны
    risk = (entry_ref - setup["stop"]) if up else (setup["stop"] - entry_ref)
    if risk <= 0:
        return None
    risk_pct = risk / entry_ref * 100
    if risk_pct > MAX_STOP_PCT:
        return None
    if 2 * TAKER_FEE_PCT / risk_pct > MAX_FEE_R:
        return None  # комиссии съедают слишком большую долю риска
    tp_info = choose_tp(entry_df, setup["side"], entry_ref, setup["stop"], atr)
    if not tp_info:
        return None
    tp, rr, reward_pct = tp_info
    return {
        **setup, "entry_ref": entry_ref, "risk": risk, "risk_pct": risk_pct,
        "tp": tp, "rr": rr, "reward_pct": reward_pct, "atr_pct": atr_pct,
    }


# Причины отказа за текущий цикл сканирования. Печатаются в лог и в сводку молчания.
REJECTS = {}


def reject(reason):
    REJECTS[reason] = REJECTS.get(reason, 0) + 1


def analyze_symbol(entry_df, trend_df, regime_df, btc_regime, spread_pct, strategies=None):
    e = entry_df.iloc[-1]
    atr_pct = float(e.atr / e.close * 100)
    if not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
        reject("ATR вне диапазона")
        return None
    allowed = STRATEGIES if strategies is None else strategies
    candidates = []
    for side in ("LONG", "SHORT"):
        for name, func in STRATEGY_FUNCS.items():
            if name not in allowed:
                continue
            tag = f"{name} {side}"
            setup = func(side, entry_df, trend_df, regime_df, btc_regime, spread_pct)
            if not setup:
                reject(f"{tag}: нет условий входа")
                continue
            if setup["score"] < MIN_SCORE:
                reject(f"{tag}: оценка ниже {MIN_SCORE}")
                continue
            candidate = build_candidate(setup, entry_df, atr_pct)
            if candidate:
                candidates.append(candidate)
            else:
                reject(f"{tag}: стоп/TP/комиссии не прошли")
    if not candidates:
        return None
    return max(candidates, key=lambda c: c["score"])


# ----------------------------- JOURNAL / STATS -----------------------------

JOURNAL_COLUMNS = [
    "id", "created_at", "closed_at", "hour", "exchange", "symbol", "strategy", "side", "score",
    "zone_low", "zone_high", "entry_ref", "stop", "tp", "risk_pct", "reward_pct", "rr",
    "entered", "outcome", "exit_price", "gross_r", "net_r", "minutes_in_trade",
]


def journal_write(sig):
    try:
        JOURNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
        need_header = (not JOURNAL_FILE.exists()) or JOURNAL_FILE.stat().st_size == 0
        row = dict(sig)
        row["outcome"] = sig["status"]
        try:
            row["hour"] = datetime.fromisoformat(sig["created_at"]).hour
        except (ValueError, TypeError):
            row["hour"] = ""
        for key in ("risk_pct", "reward_pct", "rr"):
            row[key] = round(float(sig.get(key, 0)), 4)
        with JOURNAL_FILE.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=JOURNAL_COLUMNS, extrasaction="ignore")
            if need_header:
                writer.writeheader()
            writer.writerow(row)
    except OSError:
        log.exception("Journal write failed")


def load_journal():
    if not JOURNAL_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(JOURNAL_FILE)
    except Exception:
        log.exception("Journal read failed")
        return pd.DataFrame()


def stats_text(days=None):
    df = load_journal()
    label = f"{days} дн." if days else "всё время"
    if df.empty or "outcome" not in df:
        return f"📈 Статистика ({label}): журнал пока пуст."
    for col in ("net_r", "gross_r", "entered", "hour", "score"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if days:
        closed = pd.to_datetime(df["closed_at"], utc=True, errors="coerce")
        df = df[closed >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)]
    total = len(df)
    if total == 0:
        return f"📈 Статистика ({label}): сигналов нет."
    trades = df[df["entered"] == 1]
    if trades.empty:
        return f"📈 Статистика ({label}): сигналов {total}, входов не было."

    n = len(trades)
    count = trades["outcome"].value_counts()
    wins = int(count.get("TP", 0))
    pos = trades.net_r[trades.net_r > 0].sum()
    neg = -trades.net_r[trades.net_r < 0].sum()
    pf = pos / neg if neg > 0 else float("inf")
    pf_text = f"{pf:.2f}" if np.isfinite(pf) else "∞"

    lines = [
        f"📈 Статистика ({label})",
        f"Сигналов: {total} | входов: {n} | без входа: {total - n}",
        f"TP {wins} | STOP {int(count.get('STOPPED', 0))} | TIMEOUT {int(count.get('TIMEOUT', 0))}",
        f"Winrate {wins / n * 100:.0f}% | средний net R {trades.net_r.mean():+.2f} | "
        f"сумма {trades.net_r.sum():+.1f}R | PF {pf_text}",
    ]
    by_strategy = trades.groupby("strategy").agg(
        n=("net_r", "size"), avg=("net_r", "mean"), total=("net_r", "sum"),
        wr=("outcome", lambda s: (s == "TP").mean() * 100),
    )
    for name, row in by_strategy.iterrows():
        lines.append(f"• {name}: {int(row.n)} сд., WR {row.wr:.0f}%, ср. {row.avg:+.2f}R, сумма {row.total:+.1f}R")

    by_hour = trades.groupby("hour").net_r.agg(["count", "sum"])
    by_hour = by_hour[by_hour["count"] >= 3].sort_values("sum")
    if len(by_hour) >= 2:
        worst = by_hour.head(3)
        best = by_hour.tail(3).iloc[::-1]
        lines.append("Лучшие часы: " + ", ".join(f"{int(h):02d}:00 ({r['sum']:+.1f}R)" for h, r in best.iterrows()))
        lines.append("Худшие часы: " + ", ".join(f"{int(h):02d}:00 ({r['sum']:+.1f}R)" for h, r in worst.iterrows()))
    return "\n".join(lines)


# ----------------------------- SIGNALS -----------------------------

def open_count(state):
    return sum(1 for s in state["signals"].values() if s.get("status") in OPEN_STATUSES)


def has_open_signal(state, symbol):
    return any(
        s.get("status") in OPEN_STATUSES and s.get("symbol") == symbol
        for s in state["signals"].values()
    )


def cooldown_active(state, symbol):
    stamp = state["last_signal"].get(symbol)
    if not stamp:
        return False
    try:
        last = datetime.fromisoformat(stamp)
        return (now() - last).total_seconds() < COOLDOWN_MINUTES * 60
    except (ValueError, TypeError):
        return False


def make_signal(exchange_name, symbol, cand, btc_regime, last_price):
    created_ms = now_ms()
    valid_from = (created_ms // 60_000 + 1) * 60_000   # 1m-свечи строго после создания сигнала
    sid = hashlib.sha1(f"{exchange_name}:{symbol}:{cand['side']}".encode()).hexdigest()[:8] + f"_{created_ms // 1000}"
    return {
        "id": sid, "exchange": exchange_name, "symbol": symbol,
        "side": cand["side"], "strategy": cand["strategy"], "score": cand["score"],
        "reasons": cand["reasons"],
        "zone_low": cand["zone_low"], "zone_high": cand["zone_high"],
        "entry_ref": cand["entry_ref"], "stop": cand["stop"], "tp": cand["tp"],
        "risk": cand["risk"], "risk_pct": cand["risk_pct"],
        "reward_pct": cand["reward_pct"], "rr": cand["rr"], "atr_pct": cand["atr_pct"],
        "btc_regime": btc_regime,
        "created_at": now().isoformat(), "created_ms": created_ms,
        "valid_from": valid_from, "checked_ts": valid_from - 60_000,
        "expires_entry_ms": created_ms + ENTRY_TTL_MIN * 60_000,
        "status": "PENDING", "entered": 0, "entry_ts": None,
        "closed_at": None, "exit_price": None, "gross_r": 0.0, "net_r": 0.0,
        "minutes_in_trade": 0.0, "last_price": last_price,
    }


STRATEGY_TITLES = {"pullback": "Тренд + откат", "sweep": "Sweep + reclaim"}


def signal_message(sig, exchange=None):
    def p(value):
        try:
            return exchange.price_to_precision(sig["symbol"], value)
        except Exception:
            return fmt(value)

    up = sig["side"] == "LONG"
    bullets = "\n".join("• " + item for item in sig["reasons"])
    zone_a, zone_b = sorted((sig["zone_low"], sig["zone_high"]))
    cancel_level = sig["entry_ref"] + 0.5 * (sig["tp"] - sig["entry_ref"])
    fee_r = 2 * TAKER_FEE_PCT / sig["risk_pct"]
    return (
        f"⚡ HUNTER SCALPER 7.1\n"
        f"{'🟢' if up else '🔴'} {sig['side']} - {sig['symbol']}\n"
        f"{sig['exchange'].upper()} | {STRATEGY_TITLES.get(sig['strategy'], sig['strategy'])} | "
        f"оценка {sig['score']}/100\n\n"
        f"📍 ЗОНА ВХОДА: {p(zone_a)} – {p(zone_b)}\n"
        f"🎯 TP: {p(sig['tp'])} (+{sig['reward_pct']:.2f}% | {sig['rr']:.2f}R)\n"
        f"🛑 СТОП: {p(sig['stop'])} (-{sig['risk_pct']:.2f}% от худшей границы зоны)\n"
        f"⏱ Входить до {hhmm(sig['expires_entry_ms'])}, держать не дольше {TRADE_TTL_MIN} мин после входа\n\n"
        f"Причины:\n{bullets}\n\n"
        f"Отмена: цена дошла до {p(cancel_level)} без захода в зону или пробила стоп.\n"
        f"BTC: {sig['btc_regime']} | комиссии ≈ {fee_r:.2f}R\n"
        f"⚠️ Сигнал аналитический, ордера бот не открывает. Учитывайте проскальзывание и риск."
    )


def close_signal(sig, outcome, exit_price, ts_ms):
    sig["status"] = outcome
    sig["closed_at"] = datetime.fromtimestamp(ts_ms / 1000, TZ).isoformat()
    sig["exit_price"] = exit_price
    entered = sig.get("entry_ts") is not None
    sig["entered"] = 1 if entered else 0
    if entered and exit_price is not None:
        up = sig["side"] == "LONG"
        pnl = (exit_price - sig["entry_ref"]) if up else (sig["entry_ref"] - exit_price)
        gross = pnl / sig["risk"]
        fee_r = 2 * TAKER_FEE_PCT / sig["risk_pct"]
        sig["gross_r"] = round(gross, 3)
        sig["net_r"] = round(gross - fee_r, 3)
        sig["minutes_in_trade"] = round(max(0, ts_ms - sig["entry_ts"]) / 60_000, 1)
    else:
        sig["gross_r"] = sig["net_r"] = 0.0
        sig["minutes_in_trade"] = 0.0
    journal_write(sig)


def result_text(sig, title):
    head = f"{title}\n{sig['symbol']} {sig['side']} ({sig['exchange'].upper()}, {STRATEGY_TITLES.get(sig['strategy'], '')})"
    if sig.get("entered") and sig.get("exit_price") is not None:
        return (
            f"{head}\nВыход: {fmt(sig['exit_price'])} | {sig['gross_r']:+.2f}R "
            f"(с комиссиями {sig['net_r']:+.2f}R) | в сделке {sig['minutes_in_trade']:.0f} мин"
        )
    return head


def advance_signal(sig, candles, ts_now):
    """
    Прогоняет 1m-свечи [ts, o, h, l, c, v] через автомат состояний сигнала.
    Возвращает список сообщений для Telegram. Меняет sig на месте.
    """
    messages = []
    up = sig["side"] == "LONG"
    stop, tp = sig["stop"], sig["tp"]
    half = sig["entry_ref"] + 0.5 * (tp - sig["entry_ref"])
    last_close = sig.get("last_price")

    for candle in candles:
        if sig["status"] not in OPEN_STATUSES:
            break
        ts, _o, high, low, close = candle[:5]
        if None in (ts, high, low, close):
            continue
        if ts <= sig["checked_ts"] or ts < sig["valid_from"]:
            continue
        last_close = close

        if sig["status"] == "PENDING":
            in_zone = low <= sig["zone_high"] and high >= sig["zone_low"]
            if in_zone:
                sig["status"] = "ACTIVE"
                sig["entry_ts"] = ts
                sig["entered"] = 1
                if ts - sig["created_ms"] > 120_000:   # вход случился не сразу - сообщаем
                    messages.append(
                        f"✅ ЦЕНА В ЗОНЕ ВХОДА\n{sig['symbol']} {sig['side']} ({sig['exchange'].upper()})\n"
                        f"TP {fmt(tp)} | СТОП {fmt(stop)}"
                    )
            else:
                invalid = (low <= stop) if up else (high >= stop)
                ran_away = (high >= half) if up else (low <= half)
                if invalid:
                    close_signal(sig, "CANCELLED", None, ts)
                    messages.append(result_text(sig, "❎ ОТМЕНА: цена ушла за стоп, входа не было"))
                elif ran_away:
                    close_signal(sig, "CANCELLED", None, ts)
                    messages.append(result_text(sig, "❎ ОТМЕНА: цена ушла к цели без входа"))

        if sig["status"] == "ACTIVE":
            stop_hit = (low <= stop) if up else (high >= stop)
            # На свече входа TP не засчитываем (порядок внутри свечи неизвестен).
            tp_hit = ((high >= tp) if up else (low <= tp)) and ts > sig["entry_ts"]
            if stop_hit:   # если в одной свече и стоп, и TP - засчитываем стоп
                close_signal(sig, "STOPPED", stop, ts)
                messages.append(result_text(sig, "🛑 STOP"))
            elif tp_hit:
                close_signal(sig, "TP", tp, ts)
                messages.append(result_text(sig, "🎯 TP"))

        if ts + 60_000 <= ts_now:
            sig["checked_ts"] = ts

    if sig["status"] in OPEN_STATUSES:
        if last_close is not None:
            sig["last_price"] = last_close
        if sig["status"] == "PENDING" and ts_now > sig["expires_entry_ms"]:
            close_signal(sig, "EXPIRED", None, ts_now)
            messages.append(result_text(sig, f"⌛ СИГНАЛ ИСТЁК: цена не вошла в зону за {ENTRY_TTL_MIN} мин"))
        elif sig["status"] == "ACTIVE" and ts_now > sig["entry_ts"] + TRADE_TTL_MIN * 60_000:
            close_signal(sig, "TIMEOUT", sig["last_price"], ts_now)
            messages.append(result_text(sig, f"⏱ ВЫХОД ПО ВРЕМЕНИ ({TRADE_TTL_MIN} мин)"))
    return messages


# ----------------------------- RISK GUARD (АВТОСТОП) -----------------------------

def get_guard(state):
    g = state.setdefault("guard", {})
    g.setdefault("paused_until", 0)
    g.setdefault("reason", "")
    g.setdefault("reset_after", 0)
    g.setdefault("trips", [])
    g.setdefault("strategy_until", {})
    g.setdefault("strategy_reset", {})
    g.setdefault("symbol_until", {})
    g.setdefault("resume_notified", True)
    return g


def local_midnight_ms():
    nxt = (now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(nxt.timestamp() * 1000)


def stamp(ms):
    return datetime.fromtimestamp(ms / 1000, TZ).strftime("%d.%m %H:%M")


def day_of(ms):
    return datetime.fromtimestamp(ms / 1000, TZ).strftime("%Y-%m-%d")


def guard_numbers(state):
    """Текущая серия убытков (после последней паузы) и результат за сегодня в R."""
    g = get_guard(state)
    streak = 0
    for r in reversed([r for r in state["results"] if r["ts"] > g["reset_after"]]):
        if r["net_r"] < 0:
            streak += 1
        else:
            break
    today = now().strftime("%Y-%m-%d")
    daily = sum(r["net_r"] for r in state["results"] if day_of(r["ts"]) == today)
    return streak, daily


def guard_summary(state):
    streak, daily = guard_numbers(state)
    return (f"📉 Серия убытков: {streak} из {MAX_CONSEC_LOSSES} | сегодня {daily:+.1f}R "
            f"(лимит -{MAX_DAILY_LOSS_R:g}R)")


def guard_paused(state):
    """Причина паузы, если новые сигналы сейчас запрещены, иначе None."""
    if not GUARD_ENABLED:
        return None
    g = get_guard(state)
    if now_ms() < g["paused_until"]:
        return g.get("reason") or "пауза"
    return None


def guard_active_strategies(state):
    if not GUARD_ENABLED:
        return set(STRATEGIES)
    until = get_guard(state)["strategy_until"]
    t = now_ms()
    return {s for s in STRATEGIES if until.get(s, 0) <= t}


def symbol_blocked(state, symbol):
    if not GUARD_ENABLED:
        return False
    return get_guard(state)["symbol_until"].get(symbol, 0) > now_ms()


def side_count(state, side):
    return sum(1 for s in state["signals"].values()
               if s.get("status") in OPEN_STATUSES and s.get("side") == side)


def trip(g, reason, until_ms=None):
    t = now_ms()
    g["trips"] = [x for x in g["trips"] if t - x < 86_400_000] + [t]
    if until_ms is None:
        minutes = min(PAUSE_MINUTES * 2 ** (len(g["trips"]) - 1), 1440)
        until_ms = t + minutes * 60_000
    g["paused_until"] = max(g["paused_until"], until_ms)
    g["reason"] = reason
    g["reset_after"] = t
    g["resume_notified"] = False
    log.warning("GUARD TRIP: %s | пауза до %s", reason, stamp(g["paused_until"]))
    return (f"⛔ HUNTER SCALPER: НОВЫЕ СИГНАЛЫ ПРИОСТАНОВЛЕНЫ\nПричина: {reason}\n"
            f"Возобновление: {stamp(g['paused_until'])}\n"
            f"Открытые сделки продолжают отслеживаться.")


def record_result(state, sig):
    """Учитывает закрытую сделку с входом. Возвращает (сводка, [уведомления о срабатывании защиты])."""
    notes = []
    results = state["results"]
    t = now_ms()
    results.append({"ts": t, "symbol": sig["symbol"], "strategy": sig["strategy"],
                    "net_r": float(sig.get("net_r") or 0.0), "outcome": sig.get("status")})
    del results[:-200]
    if not GUARD_ENABLED:
        return "", notes

    g = get_guard(state)
    summary = guard_summary(state)          # считается ДО срабатывания, чтобы показать реальную серию
    streak, daily = guard_numbers(state)
    paused = t < g["paused_until"]

    midnight = local_midnight_ms()
    recent = [r for r in results if r["ts"] > g["reset_after"]]
    window = recent[-ROLL_N:]
    roll_avg = sum(r["net_r"] for r in window) / len(window) if len(window) >= ROLL_N else None

    stops_today = sum(1 for r in results if day_of(r["ts"]) == now().strftime("%Y-%m-%d")
                      and r.get("outcome") == "STOPPED")
    if stops_today >= MAX_DAILY_STOPS and g["paused_until"] < midnight:
        notes.append(trip(g, f"{stops_today} стопа за день (лимит {MAX_DAILY_STOPS})", until_ms=midnight))
    elif daily <= -MAX_DAILY_LOSS_R and g["paused_until"] < midnight:
        notes.append(trip(g, f"дневной лимит убытка: {daily:+.1f}R (лимит -{MAX_DAILY_LOSS_R:g}R)", until_ms=midnight))
    elif not paused and streak >= MAX_CONSEC_LOSSES:
        notes.append(trip(g, f"{streak} убыточных сделок подряд"))
    elif not paused and roll_avg is not None and roll_avg < ROLL_MIN_AVG_R:
        notes.append(trip(g, f"средний результат последних {ROLL_N} сделок {roll_avg:+.2f}R "
                             f"(порог {ROLL_MIN_AVG_R:+.2f}R)"))
    elif paused:
        g["reset_after"] = t                # убытки, закрывшиеся во время паузы, на новую серию не влияют

    strat = sig["strategy"]
    srs = [r for r in results
           if r["strategy"] == strat and r["ts"] > g["strategy_reset"].get(strat, 0)][-STRAT_ROLL_N:]
    if len(srs) >= STRAT_ROLL_N:
        avg = sum(r["net_r"] for r in srs) / len(srs)
        if avg < STRAT_MIN_AVG_R:
            until = t + STRAT_PAUSE_HOURS * 3_600_000
            g["strategy_until"][strat] = until
            g["strategy_reset"][strat] = until
            notes.append(f"🚫 Стратегия «{STRATEGY_TITLES.get(strat, strat)}» отключена до {stamp(until)}: "
                         f"средний результат последних {STRAT_ROLL_N} сделок {avg:+.2f}R")

    sym = sig["symbol"]
    last_sym = [r for r in results if r["symbol"] == sym][-SYMBOL_MAX_LOSSES:]
    if (len(last_sym) == SYMBOL_MAX_LOSSES and all(r["net_r"] < 0 for r in last_sym)
            and g["symbol_until"].get(sym, 0) <= t):
        until = t + SYMBOL_PAUSE_HOURS * 3_600_000
        g["symbol_until"][sym] = until
        notes.append(f"🚫 {sym}: {SYMBOL_MAX_LOSSES} убытка подряд, монета исключена до {stamp(until)}")
    return summary, notes


def guard_tick(state):
    """Раз в цикл: уведомляет о возобновлении после паузы, чистит старые блокировки."""
    g = get_guard(state)
    t = now_ms()
    if g["reason"] and t >= g["paused_until"] and not g["resume_notified"]:
        g["resume_notified"] = True
        telegram(f"▶️ HUNTER SCALPER: пауза закончилась, сигналы возобновлены.\nБыла причина: {g['reason']}")
        g["reason"] = ""
    for key in ("strategy_until", "symbol_until"):
        for name, until in list(g[key].items()):
            if until <= t:
                del g[key][name]


def guard_status_line(state):
    g = get_guard(state)
    t = now_ms()
    parts = []
    if t < g["paused_until"]:
        parts.append(f"ПАУЗА до {stamp(g['paused_until'])}: {g['reason']}")
    off = [s for s, u in g["strategy_until"].items() if u > t]
    if off:
        parts.append("отключены стратегии: " + ", ".join(off))
    banned = [s for s, u in g["symbol_until"].items() if u > t]
    if banned:
        parts.append("исключены монеты: " + ", ".join(banned))
    return " | ".join(parts) if parts else "активна, ограничений нет"


def monitor_signals(state, exchanges):
    changed = False
    ts_now = now_ms()
    for sig in list(state["signals"].values()):
        if sig.get("status") not in OPEN_STATUSES:
            continue
        exchange = exchanges.get(sig.get("exchange"))
        if exchange is None:
            continue
        candles = []
        try:
            since = max(sig["checked_ts"], sig["valid_from"] - 60_000)
            candles = exchange.fetch_ohlcv(sig["symbol"], timeframe="1m", since=since, limit=300) or []
        except Exception as exc:
            log.warning("Monitor %s %s: fetch failed: %s", sig.get("exchange"), sig.get("symbol"), exc)
        try:
            msgs = advance_signal(sig, candles, ts_now)
            if sig.get("status") not in OPEN_STATUSES and sig.get("entered"):
                summary, notes = record_result(state, sig)       # сделка с входом закрыта -> учёт в автостопе
                if msgs and summary:
                    msgs[-1] += "\n" + summary
                msgs += notes
            for text in msgs:
                telegram(text)
            changed = True
        except Exception:
            log.exception("Monitor %s: advance failed", sig.get("symbol"))
    if changed:
        save_state(state)


# ----------------------------- SCANNER -----------------------------

def scan_exchange(exchange, state, btc_regime):
    name = exchange.id.lower()
    stats = daily_stats(state)
    if not in_trade_hours():
        return

    tf_ms = TF_MS[TF_ENTRY]
    expected = (now_ms() // tf_ms - 1) * tf_ms          # open-time последней закрытой 5m-свечи
    if SCAN_DONE.get(name) == expected:
        return
    if (now_ms() - (expected + tf_ms)) / 1000 > MAX_SIGNAL_AGE_SEC:
        SCAN_DONE[name] = expected                      # свеча уже "старая" - ждём следующую
        return

    paused = guard_paused(state)
    if paused:
        SCAN_DONE[name] = expected
        log.info("%s: сканирование на паузе (автостоп): %s", name, paused)
        return
    active_strategies = guard_active_strategies(state)
    if not active_strategies:
        SCAN_DONE[name] = expected
        log.info("%s: все стратегии отключены автостопом", name)
        return

    try:
        symbols = get_symbols_cached(exchange)
    except Exception:
        stats["errors"] += 1
        log.exception("%s: failed to fetch tickers/markets", name)
        return

    log.info("Scanning %s: %d symbols, candle %s", name, len(symbols), hhmm(expected))
    retry = False
    for symbol, quote_volume, spread_pct in symbols:
        key = (name, symbol)
        if SCANNED.get(key) == expected:
            continue
        ts_now = now_ms()
        if (ts_now - (expected + tf_ms)) / 1000 > MAX_SIGNAL_AGE_SEC:
            SCANNED[key] = expected
            continue
        try:
            if (has_open_signal(state, symbol) or cooldown_active(state, symbol)
                    or symbol_blocked(state, symbol) or open_count(state) >= MAX_OPEN_SIGNALS):
                SCANNED[key] = expected
                continue

            entry_df = get_frame(exchange, symbol, TF_ENTRY)
            if entry_df.empty or len(entry_df) < 100:
                stats["rejected"] += 1
                SCANNED[key] = expected
                continue
            if int(entry_df.timestamp.iloc[-1]) != expected:
                if ts_now - (expected + tf_ms) < 60_000:
                    retry = True                          # биржа ещё не отдала закрытую свечу
                else:
                    stats["rejected"] += 1
                    SCANNED[key] = expected
                continue

            SCANNED[key] = expected
            stats["scanned"] += 1
            trend_df = get_frame(exchange, symbol, TF_TREND)
            regime_df = get_frame(exchange, symbol, TF_REGIME)
            if trend_df.empty or regime_df.empty or len(trend_df) < 5 or len(regime_df) < 5:
                stats["rejected"] += 1
                continue

            best = analyze_symbol(entry_df, trend_df, regime_df, btc_regime, spread_pct, active_strategies)
            if not best:
                stats["rejected"] += 1
                continue
            if side_count(state, best["side"]) >= MAX_SAME_SIDE:
                stats["rejected"] += 1                       # уже достаточно сделок в эту сторону
                continue

            signal = make_signal(name, symbol, best, btc_regime, float(entry_df.iloc[-1].close))
            state["signals"][signal["id"]] = signal
            state["last_signal"][symbol] = now().isoformat()
            stats["signals"] += 1
            state["last_signal_ms"] = now_ms()
            save_state(state)
            telegram(signal_message(signal, exchange))
            log.info("SIGNAL %s %s %s %s score=%s rr=%.2f volume=%.0f",
                     name, symbol, signal["side"], signal["strategy"], signal["score"], signal["rr"], quote_volume)
        except ccxt.NetworkError as exc:
            stats["errors"] += 1
            log.warning("%s %s network error: %s", name, symbol, exc)
        except ccxt.ExchangeError as exc:
            stats["errors"] += 1
            log.warning("%s %s exchange error: %s", name, symbol, exc)
        except Exception:
            stats["errors"] += 1
            log.exception("%s %s scan failed", name, symbol)

    totals = state.setdefault("reject_totals", {})
    for k, v in REJECTS.items():
        totals[k] = totals.get(k, 0) + v
    top = sorted(REJECTS.items(), key=lambda kv: -kv[1])[:6]
    log.info("Итог скана %s: монет %d, отказов по причинам: %s",
             name, len(symbols), ", ".join(f"{k}={v}" for k, v in top) or "нет")
    REJECTS.clear()

    if not retry:
        SCAN_DONE[name] = expected


# ----------------------------- HOUSEKEEPING / REPORT -----------------------------

def purge_state(state):
    cutoff = time.time() - 24 * 3600
    for sid, sig in list(state["signals"].items()):
        if sig.get("status") in OPEN_STATUSES:
            continue
        try:
            closed = datetime.fromisoformat(sig.get("closed_at")).timestamp()
        except (ValueError, TypeError):
            closed = 0
        if closed < cutoff:
            del state["signals"][sid]
    for day in sorted(state["daily"])[:-14]:
        state["daily"].pop(day, None)
    for symbol, stamp in list(state["last_signal"].items()):
        try:
            age = (now() - datetime.fromisoformat(stamp)).total_seconds()
        except (ValueError, TypeError):
            age = 1e9
        if age > 86400:
            del state["last_signal"][symbol]
    stale_before = now_ms() - 3_600_000
    for key, (expire, _df) in list(FRAME_CACHE.items()):
        if expire < stale_before:
            del FRAME_CACHE[key]


SILENCE_HOURS = float(cfg("SILENCE_HOURS", "12"))


def silence_check(state):
    """Если сигналов не было SILENCE_HOURS, шлёт в Telegram причины отказов (раз в интервал)."""
    t = now_ms()
    last = state.get("last_signal_ms") or state.get("started_ms") or t
    state.setdefault("started_ms", t)
    if t - last < SILENCE_HOURS * 3_600_000:
        return
    if t - state.get("silence_sent_ms", 0) < SILENCE_HOURS * 3_600_000:
        return
    state["silence_sent_ms"] = t
    hours = (t - last) / 3_600_000
    causes = state.get("reject_totals", {})
    top = sorted(causes.items(), key=lambda kv: -kv[1])[:6]
    lines = "\n".join(f"• {k}: {v}" for k, v in top) or "• данных нет (сканер не отработал)"
    telegram(f"🔕 HUNTER SCALPER: нет сигналов уже {hours:.0f} ч.\nЧаще всего отсеивалось:\n{lines}\n"
             f"Если так и должно быть, ничего делать не надо. Иначе ослабь фильтры в yml.")
    state["reject_totals"] = {}


def daily_report(state):
    stats = daily_stats(state)
    text = (
        f"📊 HUNTER SCALPER - отчёт за {now().strftime('%d.%m.%Y')}\n\n"
        f"Проверено инструментов: {stats['scanned']}\n"
        f"Сигналов: {stats['signals']}\n"
        f"Отклонено фильтрами: {stats['rejected']}\n"
        f"Ошибок: {stats['errors']}\n"
        f"Открытых сигналов: {open_count(state)}\n"
        f"Автостоп: {guard_status_line(state)}\n\n"
        f"{stats_text(1)}\n\n{stats_text(14)}"
    )
    telegram(text)


# ----------------------------- MAIN LOOP -----------------------------

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else None
        print(stats_text(days))
        return

    log.info(
        "HUNTER SCALPER 7.1 starting | exchanges=%s | strategies=%s | score>=%s | TF=%s/%s/%s",
        EXCHANGE_NAMES, sorted(STRATEGIES), MIN_SCORE, TF_ENTRY, TF_TREND, TF_REGIME
    )
    exchanges = load_exchanges()
    if not exchanges:
        raise RuntimeError("No exchanges initialized. Check CCXT support and network.")

    state = load_state()
    log.info("Автостоп: %s", guard_status_line(state) if GUARD_ENABLED else "ВЫКЛЮЧЕН")
    last_report_day = state.get("last_report")
    run_once = cfg("RUN_ONCE", "false").lower() in ("1", "true", "yes")

    while True:
        cycle_start = time.time()
        try:
            monitor_signals(state, exchanges)
            guard_tick(state)
        except Exception:
            log.exception("Monitor cycle failed")

        for name, exchange in exchanges.items():
            try:
                btc = market_regime(get_frame(exchange, "BTC/USDT:USDT", TF_TREND))
                scan_exchange(exchange, state, btc)
            except Exception:
                daily_stats(state)["errors"] += 1
                log.exception("Exchange cycle failed: %s", name)

        purge_state(state)
        try:
            silence_check(state)
        except Exception:
            log.exception("silence check failed")
        current_day = now().strftime("%Y-%m-%d")
        if last_report_day != current_day and now().hour == 23 and now().minute >= 55:
            daily_report(state)
            last_report_day = current_day
            state["last_report"] = current_day

        save_state(state)
        if run_once:
            log.info("RUN_ONCE enabled: cycle completed")
            break

        elapsed = time.time() - cycle_start
        time.sleep(max(3, SCAN_SECONDS - elapsed))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped by user")
    except Exception:
        log.exception("Fatal startup error")
        raise
