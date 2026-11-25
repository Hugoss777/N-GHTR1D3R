import os
import json
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

try:
    import ccxt  # needed for BingX only; Bitunix uses its own public REST client
except ImportError:
    ccxt = None

# GPT 2.1 QUANT INTELLIGENCE
# Public market data only. No order execution.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def env_num(name, default, cast=float):
    """Env var as a number. Empty or invalid values fall back to the default
    (GitHub Actions passes an empty string for an unset variable)."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return cast(raw)
    except ValueError:
        logging.warning("Bad value for %s=%r, using %s", name, raw, default)
        return default


TZ = ZoneInfo("Europe/Istanbul")
STATE_FILE = Path(os.getenv("STATE_FILE", "").strip() or "gpt21_state.json")

TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

EXCHANGES = tuple(
    x.strip().lower()
    for x in (os.getenv("EXCHANGES", "").strip() or "bitunix,bingx").split(",")
    if x.strip()
)
TIMEFRAME = "15m"
HTF = "1h"
TF_MS = {"15m": 15 * 60 * 1000, "1h": 60 * 60 * 1000}
BAR_MS = TF_MS[TIMEFRAME]

# Rule-based ranking (50..100), NOT a win probability.
MIN_SCORE = env_num("MIN_SCORE", 60, int)
# 24h turnover. Cuts micro perps where a 15m breakout is mostly noise.
MIN_QUOTE_VOLUME = env_num("MIN_QUOTE_VOLUME", 750000.0)
# Breakout candle must actually participate, not print on a dead tape.
MIN_VOLUME_RATIO = env_num("MIN_VOLUME_RATIO", 1.05)
# One coin + direction, then silence for this many 15m candles.
COOLDOWN_BARS = env_num("COOLDOWN_BARS", 6, int)
# A scalp target past this is not an intraday level.
MAX_TARGET_ATR = env_num("MAX_TARGET_ATR", 6.0)
# Minimum reward / risk of the single take profit.
MIN_RR = env_num("MIN_RR", 1.2)
# Signal candle must have closed no more than this many bars ago.
MAX_SIGNAL_AGE_BARS = env_num("MAX_SIGNAL_AGE_BARS", 2, int)
# Cap per run: if the whole market breaks out, send only the best ones.
MAX_SIGNALS_PER_RUN = env_num("MAX_SIGNALS_PER_RUN", 10, int)
# Whole-run time budget, split between exchanges.
MAX_RUNTIME_SEC = env_num("MAX_RUNTIME_SEC", 600, int)
# No scanning and no messages before this hour (Istanbul time). 0 disables.
QUIET_END_HOUR = env_num("QUIET_END_HOUR", 7, int)

http = requests.Session()


class TransientError(Exception):
    """Temporary exchange problem: timeout, 429, 5xx."""


TRANSIENT = (TransientError, requests.RequestException)
if ccxt is not None:
    TRANSIENT += (ccxt.NetworkError,)


def now():
    return datetime.now(TZ)


def quiet_hours():
    return now().hour < QUIET_END_HOUR


# ---------------------------------------------------------------- state

def load_state():
    state = {"signals": {}, "last_scan": {}}
    try:
        data = json.loads(STATE_FILE.read_text())
        state["signals"] = {
            str(k): int(v) for k, v in data.get("signals", {}).items()
        }
        state["last_scan"] = data.get("last_scan", {})
    except (OSError, ValueError, AttributeError, TypeError):
        pass
    return state


def save_state(state):
    # Cooldowns are a few candles long; drop anything older than 3 days.
    cutoff = int(time.time() * 1000) - 3 * 24 * 60 * 60 * 1000
    state["signals"] = {
        k: v for k, v in state["signals"].items() if v >= cutoff
    }

    fd, name = tempfile.mkstemp(
        dir=str(STATE_FILE.resolve().parent),
        prefix="gpt21_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, STATE_FILE)
    finally:
        if os.path.exists(name):
            os.remove(name)


# ------------------------------------------------------------- telegram

def telegram(message):
    if quiet_hours():
        logging.info("Night quiet hours: message blocked")
        return False

    if not TOKEN or not CHAT_ID:
        raise RuntimeError("Telegram secrets are missing")

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "disable_web_page_preview": True
    }

    # requests puts the URL (with the bot token) into its exception text,
    # so failures are re-raised without it.
    for attempt in range(3):
        try:
            response = http.post(url, json=payload, timeout=20)
        except requests.RequestException as exc:
            if attempt == 2:
                raise RuntimeError(
                    f"Telegram request failed: {type(exc).__name__}"
                ) from None
            time.sleep(2)
            continue

        if response.status_code == 429:
            try:
                delay = response.json()["parameters"]["retry_after"]
            except (ValueError, KeyError, TypeError):
                delay = 5
            time.sleep(min(float(delay), 30))
            continue

        if not response.ok:
            raise RuntimeError(f"Telegram HTTP {response.status_code}")

        try:
            ok = response.json().get("ok")
        except ValueError:
            ok = False
        if not ok:
            raise RuntimeError("Telegram returned ok=false")

        return True

    raise RuntimeError("Telegram: too many retries")


# ------------------------------------------------------------ exchanges
# Every source exposes: name, symbols(), volumes(), ohlcv(), coin(), close().

class BitunixSource:
    """Bitunix USDT perpetuals via the public futures REST API."""

    name = "bitunix"
    BASE = "https://fapi.bitunix.com/api/v1/futures/market"
    MIN_INTERVAL = 0.13  # API limit is 10 requests/sec per IP

    def __init__(self):
        self.session = requests.Session()
        self._last_call = 0.0
        self._bases = {}

    def _get(self, path, params=None):
        last_error = "unknown"

        for attempt in range(4):
            pause = self.MIN_INTERVAL - (time.monotonic() - self._last_call)
            if pause > 0:
                time.sleep(pause)
            self._last_call = time.monotonic()

            try:
                response = self.session.get(
                    f"{self.BASE}/{path}", params=params, timeout=20
                )
            except requests.RequestException as exc:
                last_error = type(exc).__name__
            else:
                if response.status_code == 429 or response.status_code >= 500:
                    last_error = f"HTTP {response.status_code}"
                elif not response.ok:
                    raise RuntimeError(
                        f"Bitunix {path}: HTTP {response.status_code}"
                    )
                else:
                    body = response.json()
                    if body.get("code") != 0:
                        raise RuntimeError(
                            f"Bitunix {path}: code={body.get('code')} "
                            f"msg={body.get('msg')}"
                        )
                    return body.get("data") or []

            time.sleep(2 ** attempt)

        raise TransientError(f"Bitunix {path}: {last_error}")

    def symbols(self):
        result = []
        now_ms = int(time.time() * 1000)

        for item in self._get("trading_pairs"):
            symbol = item.get("symbol")
            if not symbol or item.get("quote") != "USDT":
                continue
            if item.get("symbolStatus") != "OPEN":
                continue
            delist = item.get("delistTime")
            if delist and int(delist) <= now_ms:
                continue

            self._bases[symbol] = item.get("base") or symbol[:-4]
            result.append(symbol)

        return sorted(set(result))

    def volumes(self):
        result = {}

        for item in self._get("tickers"):
            symbol = item.get("symbol")
            if not symbol:
                continue
            try:
                result[symbol] = turnover(
                    float(item.get("quoteVol") or 0),
                    float(item.get("baseVol") or 0),
                    float(item.get("lastPrice") or item.get("last") or 0)
                )
            except (TypeError, ValueError):
                continue

        return result

    def ohlcv(self, symbol, timeframe, limit=100):
        rows = self._get("kline", {
            "symbol": symbol,
            "interval": timeframe,
            "limit": min(limit, 200)
        })

        result = []
        for row in rows:
            try:
                result.append([
                    int(row["time"]),
                    float(row["open"]), float(row["high"]),
                    float(row["low"]), float(row["close"]),
                    float(row["baseVol"])
                ])
            except (KeyError, TypeError, ValueError):
                continue

        return result

    def coin(self, symbol):
        return self._bases.get(symbol) or symbol.removesuffix("USDT")

    def close(self):
        self.session.close()


class CcxtSource:
    """USDT-margined perpetuals of any ccxt exchange (BingX)."""

    def __init__(self, name):
        if ccxt is None:
            raise RuntimeError("ccxt is not installed (pip install ccxt)")

        self.name = name
        self.exchange = getattr(ccxt, name)({
            "enableRateLimit": True,
            "timeout": 30000,
            "options": {"defaultType": "swap"}
        })

    def symbols(self):
        self.exchange.load_markets()
        result = []

        for market in self.exchange.markets.values():
            if market.get("active") is False:
                continue
            if not market.get("swap"):
                continue
            if market.get("linear") is not True:
                continue
            if market.get("quote") != "USDT":
                continue
            if market.get("settle") != "USDT":
                continue
            if not market.get("symbol"):
                continue
            if market.get("expiry") is not None:
                continue

            result.append(market["symbol"])

        return sorted(set(result))

    def volumes(self):
        result = {}

        for symbol, ticker in self.exchange.fetch_tickers().items():
            volume = ticker.get("quoteVolume")
            if volume is None and ticker.get("baseVolume") and ticker.get("last"):
                volume = ticker["baseVolume"] * ticker["last"]
            if volume is not None:
                result[symbol] = float(volume)

        return result

    def ohlcv(self, symbol, timeframe, limit=100):
        return self.exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, limit=limit
        )

    def coin(self, symbol):
        return symbol.split(":")[0].split("/")[0]

    def close(self):
        self.exchange.close()


def make_source(name):
    if name == "bitunix":
        return BitunixSource()
    return CcxtSource(name)


def turnover(quote_vol, base_vol, last):
    """24h turnover in USDT. Bitunix docs are vague about which of the two
    ticker volumes is which, so the pair is checked against the price:
    quote / base must be close to the last price."""
    if quote_vol > 0 and base_vol > 0 and last > 0:
        if 0.5 * last <= quote_vol / base_vol <= 2 * last:
            return quote_vol
        if 0.5 * last <= base_vol / quote_vol <= 2 * last:
            return base_vol
        return quote_vol
    if quote_vol > 0:
        return quote_vol
    return base_vol * last if base_vol > 0 and last > 0 else 0.0


def liquid_symbols(symbols, volumes):
    """Drop illiquid markets; most liquid first, so a cut-short run still
    covers the markets that matter."""
    if not symbols or MIN_QUOTE_VOLUME <= 0 or not volumes:
        return symbols

    known = [s for s in symbols if s in volumes]

    # A broken ticker payload must not mute the whole exchange.
    if len(known) < max(10, len(symbols) // 10):
        logging.info("Volume data too thin, scanning all markets")
        return symbols

    kept = [s for s in known if volumes[s] >= MIN_QUOTE_VOLUME]
    kept.sort(key=lambda s: volumes[s], reverse=True)

    logging.info(
        "Volume filter: %d -> %d markets (min %.0f USDT)",
        len(symbols), len(kept), MIN_QUOTE_VOLUME
    )
    return kept


def closed_candles(source, symbol, timeframe, limit=100):
    rows = source.ohlcv(symbol, timeframe, limit)

    duration = TF_MS[timeframe]
    current_ms = int(time.time() * 1000)
    clean = {}

    for row in rows:
        if len(row) < 6 or any(v is None for v in row[:6]):
            continue

        ts = int(row[0])
        o, h, l, c, v = (float(x) for x in row[1:6])

        # Only fully closed, sane candles.
        if ts + duration > current_ms:
            continue
        if h < l or min(o, h, l, c) <= 0:
            continue

        clean[ts] = [ts, o, h, l, c, v]

    return [clean[k] for k in sorted(clean)]


# ----------------------------------------------------------- indicators

def ema(values, period):
    if len(values) < period:
        return None

    alpha = 2 / (period + 1)
    value = sum(values[:period]) / period

    for price in values[period:]:
        value = alpha * price + (1 - alpha) * value

    return value


def atr(candles, period=14):
    """Wilder ATR."""
    if len(candles) < period + 1:
        return None

    ranges = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        ranges.append(max(
            c[2] - c[3],
            abs(c[2] - p[4]),
            abs(c[3] - p[4])
        ))

    value = sum(ranges[:period]) / period
    for tr in ranges[period:]:
        value = (value * (period - 1) + tr) / period

    return value


def rsi(values, period=14):
    """Wilder RSI."""
    if len(values) < period + 1:
        return None

    changes = [values[i] - values[i - 1] for i in range(1, len(values))]

    gain = sum(max(x, 0) for x in changes[:period]) / period
    loss = sum(max(-x, 0) for x in changes[:period]) / period

    for x in changes[period:]:
        gain = (gain * (period - 1) + max(x, 0)) / period
        loss = (loss * (period - 1) + max(-x, 0)) / period

    if loss == 0:
        return 100.0 if gain > 0 else 50.0

    return 100 - 100 / (1 + gain / loss)


def market_bias(candles):
    closes = [c[4] for c in candles]

    fast = ema(closes, 20)
    slow = ema(closes, 50)

    if fast is None or slow is None:
        return "NEUTRAL"

    if closes[-1] > fast > slow:
        return "BULLISH"

    if closes[-1] < fast < slow:
        return "BEARISH"

    return "NEUTRAL"


def htf_bias(source, symbol):
    candles = closed_candles(source, symbol, HTF, 100)
    if len(candles) < 55:
        return "NEUTRAL"
    return market_bias(candles)


def swing_levels(candles, direction):
    """Intraday swings, about 12 hours on 15m. Signal candle excluded."""
    window = candles[-49:-1]
    levels = []

    if len(window) < 8:
        return levels

    for i in range(2, len(window) - 2):
        if direction == "LONG":
            high = window[i][2]
            neighbors = (
                window[i - 2][2], window[i - 1][2],
                window[i + 1][2], window[i + 2][2]
            )
            if high > max(neighbors):
                levels.append(high)
        else:
            low = window[i][3]
            neighbors = (
                window[i - 2][3], window[i - 1][3],
                window[i + 1][3], window[i + 2][3]
            )
            if low < min(neighbors):
                levels.append(low)

    return levels


def scale(value, low, high):
    """0..1 position of value between low and high."""
    if high <= low:
        return 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))


# ------------------------------------------------------------- strategy

def find_setup(candles, htf_bias_fn):
    """
    Hybrid trend + breakout/retest + momentum model.
    Uses only closed candles. The 1h bias is requested lazily, only for
    candles that already pass every cheap 15m filter.
    """

    if len(candles) < 60:
        return None

    closes = [c[4] for c in candles]
    current = candles[-1]

    current_atr = atr(candles)
    current_rsi = rsi(closes)

    fast = ema(closes, 20)
    slow = ema(closes, 50)

    if any(x is None for x in (current_atr, current_rsi, fast, slow)):
        return None

    if current_atr <= 0:
        return None

    open_price, high, low, close, volume = current[1:6]

    candle_range = max(high - low, 1e-12)
    body = abs(close - open_price)

    previous = candles[-21:-1]
    average_body = max(
        sum(abs(c[4] - c[1]) for c in previous) / 20, 1e-12
    )
    average_volume = sum(c[5] for c in previous) / 20
    volume_ratio = volume / average_volume if average_volume > 0 else 0

    # Previous range excludes the current candle.
    resistance = max(c[2] for c in previous)
    support = min(c[3] for c in previous)

    if (
        close > resistance
        and close > open_price
        and close > fast > slow
        and 54 <= current_rsi <= 76
    ):
        direction, wanted_bias = "LONG", "BULLISH"
    elif (
        close < support
        and close < open_price
        and close < fast < slow
        and 24 <= current_rsi <= 48
    ):
        direction, wanted_bias = "SHORT", "BEARISH"
    else:
        return None

    # Require meaningful candle body and volume participation.
    if body < max(average_body * 1.15, current_atr * 0.30):
        return None

    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    close_strength = (
        (close - low) / candle_range
        if direction == "LONG"
        else (high - close) / candle_range
    )

    if close_strength < 0.70:
        return None

    # Do not chase a spike or a move already stretched from the 15m mean.
    if candle_range > current_atr * 2.5:
        return None

    stretch = (
        (close - fast) if direction == "LONG" else (fast - close)
    )
    if stretch > current_atr * 2.2:
        return None

    # Higher-timeframe trend must agree (one extra request, only here).
    if htf_bias_fn() != wanted_bias:
        return None

    # Structural stop based on recent swing, with ATR buffer.
    recent = candles[-8:-1]

    if direction == "LONG":
        swing = min(c[3] for c in recent)
        stop = swing - current_atr * 0.20
    else:
        swing = max(c[2] for c in recent)
        stop = swing + current_atr * 0.20

    broken = resistance if direction == "LONG" else support

    # Entry is a zone: retest of the broken level, not the candle extreme.
    # If the close already ran, the zone stays next to the level.
    if direction == "LONG":
        if (close - broken) > current_atr:
            entry_low = broken
            entry_high = broken + current_atr * 0.50
        else:
            entry_low = broken
            entry_high = close
        if stop >= entry_low or entry_high <= entry_low:
            return None
        risk = entry_high - stop
        edge = entry_high
    else:
        if (broken - close) > current_atr:
            entry_high = broken
            entry_low = broken - current_atr * 0.50
        else:
            entry_low = close
            entry_high = broken
        if stop <= entry_high or entry_high <= entry_low:
            return None
        risk = stop - entry_low
        edge = entry_low

    if risk <= 0:
        return None

    # Reject both noise-sized and excessively wide stops.
    if risk < current_atr * 0.60:
        return None

    if risk > current_atr * 3.0:
        return None

    # One target: nearest intraday zone with room, else the broken
    # range, else one projection of the signal candle. No R-ladder.
    min_distance = max(current_atr * 0.80, risk * MIN_RR)
    max_distance = current_atr * MAX_TARGET_ATR
    swings = swing_levels(candles, direction)
    range_height = resistance - support

    if direction == "LONG":
        blockers = [
            level for level in swings
            if edge < level < edge + min_distance
        ]
        reachable = [
            level for level in swings
            if edge + min_distance <= level <= edge + max_distance
        ]
        structural = min(reachable) if reachable else None
        measured = broken + range_height
        from_candle = high + candle_range
    else:
        blockers = [
            level for level in swings
            if edge - min_distance < level < edge
        ]
        reachable = [
            level for level in swings
            if edge - max_distance <= level <= edge - min_distance
        ]
        structural = max(reachable) if reachable else None
        measured = broken - range_height
        from_candle = low - candle_range

    if blockers:
        return None

    tp = None
    for candidate in (structural, measured, from_candle):
        if candidate is None:
            continue
        distance = (
            (candidate - edge) if direction == "LONG" else (edge - candidate)
        )
        if min_distance <= distance <= max_distance:
            tp = candidate
            break

    if tp is None:
        return None

    rr = abs(tp - edge) / risk

    # Score: 50 for passing every gate, up to 50 more for quality.
    # A ranking to pick the best setups, NOT a win probability.
    ideal_rsi = 62 if direction == "LONG" else 38
    rsi_fit = max(0.0, 1 - abs(current_rsi - ideal_rsi) / 14)
    ema_gap = abs(fast - slow) / current_atr

    score = 50.0
    score += 15 * scale(volume_ratio, MIN_VOLUME_RATIO, 2.5)
    score += 10 * scale(close_strength, 0.70, 1.0)
    score += 10 * scale(body / average_body, 1.15, 2.5)
    score += 5 * rsi_fit
    score += 5 * scale(rr, MIN_RR, 3.0)
    score += 5 * scale(ema_gap, 0.2, 1.5)
    score = round(score)

    if score < MIN_SCORE:
        return None

    return {
        "direction": direction,
        "entry": close,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "risk": risk,
        "tp": tp,
        "rr": rr,
        "score": score,
        "atr": current_atr,
        "rsi": current_rsi,
        "volume_ratio": volume_ratio,
        "bias": wanted_bias,
        "candle_time": int(current[0])
    }


# -------------------------------------------------------------- messages

def fmt(value):
    magnitude = abs(value)
    if magnitude >= 100:
        digits = 2
    elif magnitude >= 10:
        digits = 3
    elif magnitude >= 1:
        digits = 4
    elif magnitude >= 0.1:
        digits = 5
    elif magnitude >= 0.01:
        digits = 6
    else:
        digits = 8
    text = f"{value:.{digits}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def build_message(coin, signal):
    return (
        f"{coin} · {signal['direction']}\n"
        f"ТП: {fmt(signal['tp'])}\n"
        f"СЛ: {fmt(signal['stop'])}"
    )


def on_cooldown(state, coin, direction, candle_time):
    """Cooldown is per coin + direction across ALL exchanges, so the same
    breakout is never sent twice from two venues or two overlapping runs."""
    previous = state["signals"].get(f"{coin}|{direction}")
    if previous is None:
        return False
    return (candle_time - previous) < COOLDOWN_BARS * BAR_MS


# ------------------------------------------------------------------ scan

def scan_source(source, state, deadline, limit):
    name = source.name

    symbols = source.symbols()

    volumes = None
    try:
        volumes = source.volumes()
    except Exception as exc:
        logging.warning(
            "%s: volume filter skipped (%s)", name, type(exc).__name__
        )

    symbols = liquid_symbols(symbols, volumes)

    logging.info("%s: %d markets to scan", name, len(symbols))

    scanned = 0
    errors = 0
    streak = 0
    found = []

    for symbol in symbols:
        if quiet_hours():
            logging.info("Night stop reached")
            break

        if time.monotonic() > deadline:
            logging.warning("%s: time budget reached, stopping scan", name)
            break

        if streak >= 10:
            logging.error("%s: 10 errors in a row, exchange skipped", name)
            break

        try:
            candles = closed_candles(source, symbol, TIMEFRAME)
            streak = 0

            if len(candles) < 60:
                continue

            # Dead or halted market: the last candle is old.
            age = int(time.time() * 1000) - (candles[-1][0] + BAR_MS)
            if age > MAX_SIGNAL_AGE_BARS * BAR_MS:
                continue

            scanned += 1

            signal = find_setup(
                candles, lambda s=symbol: htf_bias(source, s)
            )

            if signal is not None:
                found.append((signal["score"], symbol, signal))

        except TRANSIENT as exc:
            errors += 1
            streak += 1
            logging.warning(
                "%s %s: %s %s", name, symbol, type(exc).__name__, exc
            )
            time.sleep(2)

        except Exception:
            errors += 1
            streak += 1
            logging.exception("Scan error: %s %s", name, symbol)

    # Best setups first; the per-run cap keeps a market-wide pump quiet.
    found.sort(key=lambda item: item[0], reverse=True)
    sent = 0

    for score, symbol, signal in found:
        if sent >= limit or quiet_hours():
            break

        coin = source.coin(symbol)
        direction = signal["direction"]

        if on_cooldown(state, coin, direction, signal["candle_time"]):
            continue

        try:
            delivered = telegram(build_message(coin, signal))
        except Exception as exc:
            errors += 1
            logging.error("Telegram send failed for %s: %s", coin, exc)
            continue

        if delivered:
            state["signals"][f"{coin}|{direction}"] = signal["candle_time"]
            sent += 1
            save_state(state)

            logging.info(
                "SIGNAL SENT %s %s %s score %d rr %.2f entry %s-%s "
                "stop %s tp %s rsi %.1f vol %.2f",
                name, coin, direction, score, signal["rr"],
                fmt(signal["entry_low"]), fmt(signal["entry_high"]),
                fmt(signal["stop"]), fmt(signal["tp"]),
                signal["rsi"], signal["volume_ratio"]
            )

    state["last_scan"][name] = {
        "time": now().isoformat(),
        "markets_found": len(symbols),
        "markets_scanned": scanned,
        "setups_found": len(found),
        "signals_sent": sent,
        "errors": errors
    }

    save_state(state)

    logging.info(
        "%s finished: markets=%d scanned=%d setups=%d sent=%d errors=%d",
        name, len(symbols), scanned, len(found), sent, errors
    )

    return sent


def main():
    if quiet_hours():
        logging.info("Quiet hours: no scanning or Telegram messages")
        return

    if not TOKEN or not CHAT_ID:
        raise RuntimeError("Missing Telegram secrets")

    state = load_state()
    overall_deadline = time.monotonic() + MAX_RUNTIME_SEC
    remaining_signals = MAX_SIGNALS_PER_RUN

    for index, name in enumerate(EXCHANGES):
        if quiet_hours() or remaining_signals <= 0:
            break

        # Each exchange gets an equal share of the time that is left.
        left = overall_deadline - time.monotonic()
        if left <= 0:
            break
        deadline = time.monotonic() + left / (len(EXCHANGES) - index)

        source = None

        try:
            logging.info("Starting exchange %s", name)

            source = make_source(name)
            remaining_signals -= scan_source(
                source, state, deadline, remaining_signals
            )

        except Exception:
            logging.exception("Exchange failure: %s", name)

        finally:
            if source is not None:
                try:
                    source.close()
                except Exception:
                    pass

            save_state(state)

    logging.info("GPT 2.1 scan completed")


if __name__ == "__main__":
    main()
