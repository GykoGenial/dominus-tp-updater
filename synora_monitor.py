#!/usr/bin/env python3
"""
SYNORA Monitor  v1.1  (2026-06-06)
════════════════════════════════════════════════════════════════
Vollautomatischer SYNORA-Signal-Executor auf Bybit (Sub-Account).

Funktionsweise:
  • Lauscht via Telethon auf den SYNORA Telegram-Kanal
  • Parst eingehende Signale (LONG/SHORT, Hebel variabel, Entry, SL, 2 DCAs)
  • Führt automatisch aus auf Bybit (Sub-Account mit eigenen API-Keys):
      – Market Order: 10% des Budgets
      – Limit Order DCA1: 25% des Budgets
      – Limit Order DCA2: 65% des Budgets
  • Setzt SL nach Order-Fill
  • Reagiert auf SYNORA UPDATE-Nachrichten → setzt TP-Order
  • Benachrichtigt via Telegram Bot

Signal-Format (aus Schulungsfilm):
  🔥 SYNORA SIGNAL INCOMING! 🔥
  Wir gehen SHORT auf SUSHIUSDT mit 10X Hebel!
  (Aktueller Signal-Preis: 0.2299)
  🟢 Jetzt: Market Order (Nutze nur 10% deines Kapitals!)
  🟣 Nachkaufen (Limit): Bei 0.2365 (25%) und 0.2430 (65%)
  🛡 ABSICHERUNG: Wenn der Kurs auf 0.2476 fällt...

  🟩 SYNORA UPDATE
  SUSHIUSDT
  Maximaler Gewinn: 25 %

Railway-Variablen (NEU — separate Synora-Konfiguration):
  BYBIT_SYNORA_API_KEY     → Bybit Sub-Account API Key (aus Bybit API-Verwaltung)
  BYBIT_SYNORA_PRIVATE_KEY → RSA Private Key (PEM-Inhalt, \n durch \\n ersetzen für Railway)
                             Generieren: openssl genrsa -out synora_private.pem 2048
  SYNORA_CHANNEL_ID       → Telegram-Kanal-ID (int, z.B. -1001234567890)
                            Alternativ: Einladungslink t.me/+...
  SYNORA_BUDGET_CAP_USDT  → Maximales Budget in USDT (Default: 0 = kein Cap, nutze vollen Balance)
                            Beispiel: 500 → nutzt min(Bybit-Balance, 500 USDT)
  SYNORA_STATE_FILE       → Pfad zur State-JSON (Default: /app/data/synora_state.json)
  SYNORA_TRADES_CSV       → Pfad zum Trade-Archiv (Default: /app/data/synora_trades.csv)
  SYNORA_DASHBOARD_SECRET → Secret für /dashboard URL (leer = kein Schutz)
  SYNORA_DASHBOARD_PORT   → Port für Flask-Dashboard (Default: 8080)
  SYNORA_MAX_GAIN_MODE    → "roi" (Default) oder "price":
                            "roi"   = Maximaler Gewinn % bezieht sich auf Margin-ROI
                                      → TP-Preis = entry ± (gain%/leverage)%
                            "price" = Maximaler Gewinn % = direkter Preisbewegung
                                      → TP-Preis = entry ± gain%

Geteilte Railway-Variablen (bereits vorhanden):
  TELEGRAM_API_ID         → my.telegram.org API ID
  TELEGRAM_API_HASH       → my.telegram.org API Hash
  DOMINUS_SESSION_STRING  → Telethon StringSession (wird geteilt)
  TELEGRAM_TOKEN          → Bot-Token (für Benachrichtigungen)
  TELEGRAM_CHAT_ID        → Deine Chat-ID

Deployment:
  Eigener Railway-Service mit diesem Script als Startbefehl:
    python synora_monitor.py
"""

import os
import re
import json
import time
import base64
import asyncio
import logging
import requests
import math
import threading
import csv as csv_module
from datetime import datetime, timezone

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ── Telethon ────────────────────────────────────────────────────
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ── Flask (Dashboard) ───────────────────────────────────────────
from flask import Flask, request, Response

# ═══════════════════════════════════════════════════════════════
# KONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Telegram / Telethon
API_ID            = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH          = os.environ.get("TELEGRAM_API_HASH", "")
SESSION_STRING    = os.environ.get("DOMINUS_SESSION_STRING", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")

# Synora-spezifisch
SYNORA_CHANNEL    = os.environ.get("SYNORA_CHANNEL_ID", "")   # int-ID oder Einladungslink
SYNORA_STATE_FILE = os.environ.get("SYNORA_STATE_FILE", "/app/data/synora_state.json")
SYNORA_MAX_GAIN_MODE = os.environ.get("SYNORA_MAX_GAIN_MODE", "roi")  # "roi" oder "price"

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    try:
        return float(raw) if raw.strip() else default
    except (ValueError, TypeError):
        return default

SYNORA_BUDGET_CAP_USDT = _env_float("SYNORA_BUDGET_CAP_USDT", 0.0)
# 0 = kein Cap (volles Balance); >0 = maximales Budget in USDT
# Beispiel: SYNORA_BUDGET_CAP_USDT=500 → nutzt min(Balance, 500 USDT)

SYNORA_TRADES_CSV        = os.environ.get("SYNORA_TRADES_CSV", "/app/data/synora_trades.csv")
SYNORA_DASHBOARD_SECRET  = os.environ.get("SYNORA_DASHBOARD_SECRET", "")
SYNORA_DASHBOARD_PORT    = int(os.environ.get("SYNORA_DASHBOARD_PORT", "8080"))
POSITION_POLL_INTERVAL   = 60   # Sekunden zwischen Bybit-Position-Checks

# Bybit Sub-Account (RSA-Auth für AI Subaccount)
BYBIT_SYNORA_API_KEY     = os.environ.get("BYBIT_SYNORA_API_KEY", "")
BYBIT_SYNORA_PRIVATE_KEY = os.environ.get("BYBIT_SYNORA_PRIVATE_KEY", "")
# PEM-Inhalt als Railway-Variable. Railway ersetzt \n automatisch durch echte Zeilenumbrüche.
# Falls nicht: manuell \\n → \n konvertieren (wird unten gemacht).
BYBIT_BASE_URL           = "https://api.bybit.com"
BYBIT_RECV_WINDOW        = "5000"
BYBIT_CATEGORY           = "linear"   # USDT Perpetual

# Capital-Split (wie in Synora-Doku: 10% / 25% / 65%)
SPLIT_ENTRY = 0.10
SPLIT_DCA1  = 0.25
SPLIT_DCA2  = 0.65

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SYNORA] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("synora")


# ═══════════════════════════════════════════════════════════════
# STATE (Persistenz)
# ═══════════════════════════════════════════════════════════════

_state: dict = {}

def load_state() -> None:
    global _state
    try:
        with open(SYNORA_STATE_FILE, "r") as f:
            _state = json.load(f)
        log.info(f"State geladen: {len(_state.get('trades', {}))} offene Trades")
    except FileNotFoundError:
        _state = {"trades": {}}
        log.info("Neuer State erstellt")
    except Exception as e:
        log.error(f"State-Ladefehler: {e}")
        _state = {"trades": {}}

def save_state() -> None:
    try:
        os.makedirs(os.path.dirname(SYNORA_STATE_FILE), exist_ok=True)
        with open(SYNORA_STATE_FILE, "w") as f:
            json.dump(_state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"State-Speicherfehler: {e}")


# ═══════════════════════════════════════════════════════════════
# TRADE-ARCHIV (CSV)
# ═══════════════════════════════════════════════════════════════

CSV_HEADERS = [
    "opened_at", "closed_at", "symbol", "side", "lev",
    "entry", "sl", "tp", "budget_usdt",
    "close_price", "pnl_usdt", "outcome",
]

def csv_log_synora_trade(record: dict) -> None:
    """Schreibt einen abgeschlossenen Trade in synora_trades.csv (thread-safe)."""
    try:
        os.makedirs(os.path.dirname(SYNORA_TRADES_CSV), exist_ok=True)
        file_exists = os.path.isfile(SYNORA_TRADES_CSV)
        with open(SYNORA_TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv_module.DictWriter(f, fieldnames=CSV_HEADERS)
            if not file_exists:
                writer.writeheader()
            row = {k: record.get(k, "") for k in CSV_HEADERS}
            writer.writerow(row)
        log.info(f"CSV: {record.get('symbol')} {record.get('outcome')} geloggt")
    except Exception as e:
        log.error(f"CSV-Log Fehler: {e}")

def read_trades_csv() -> list:
    """Liest alle archivierten Trades aus der CSV."""
    trades = []
    try:
        if not os.path.isfile(SYNORA_TRADES_CSV):
            return []
        with open(SYNORA_TRADES_CSV, newline="", encoding="utf-8") as f:
            reader = csv_module.DictReader(f)
            for row in reader:
                trades.append(dict(row))
    except Exception as e:
        log.error(f"CSV-Lese-Fehler: {e}")
    return trades


# ═══════════════════════════════════════════════════════════════
# TELEGRAM BOT API (Benachrichtigungen)
# ═══════════════════════════════════════════════════════════════

def tg(msg: str, parse_mode: str = "HTML") -> None:
    """Sendet eine Nachricht an den Telegram-Chat."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.info(f"[TG] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       msg,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        log.error(f"Telegram-Fehler: {e}")


# ═══════════════════════════════════════════════════════════════
# BYBIT SUB-ACCOUNT API  (RSA-Signierung für AI Subaccount)
# ═══════════════════════════════════════════════════════════════

_rsa_private_key = None

def _load_rsa_key():
    """Lädt den RSA Private Key aus der Env-Variable (lazy, gecacht)."""
    global _rsa_private_key
    if _rsa_private_key is not None:
        return _rsa_private_key
    pem = BYBIT_SYNORA_PRIVATE_KEY
    if not pem:
        raise ValueError("BYBIT_SYNORA_PRIVATE_KEY ist leer")
    # Railway kann \n als Literal-String speichern → normalisieren
    pem = pem.replace("\\n", "\n")
    if not pem.strip().startswith("-----"):
        raise ValueError("BYBIT_SYNORA_PRIVATE_KEY sieht nicht wie PEM aus")
    _rsa_private_key = serialization.load_pem_private_key(
        pem.encode(), password=None
    )
    log.info("RSA Private Key geladen ✓")
    return _rsa_private_key

def _sign_bybit_rsa(message: str) -> str:
    """Signiert eine Nachricht mit RSA-SHA256, gibt Base64-String zurück."""
    key = _load_rsa_key()
    sig = key.sign(message.encode(), padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(sig).decode()

def _headers_bybit(body_or_query: str = "") -> dict:
    ts = str(int(time.time() * 1000))
    message = ts + BYBIT_SYNORA_API_KEY + BYBIT_RECV_WINDOW + body_or_query
    return {
        "X-BAPI-API-KEY":     BYBIT_SYNORA_API_KEY,
        "X-BAPI-SIGN":        _sign_bybit_rsa(message),
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW,
        "Content-Type":       "application/json",
    }

def bybit_get(path: str, params: dict = None) -> dict:
    query_str = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    url = BYBIT_BASE_URL + path + ("?" + query_str if query_str else "")
    for attempt in range(3):
        try:
            r = requests.get(url, headers=_headers_bybit(query_str), timeout=10)
            if r.status_code >= 500 and attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            try:
                data = r.json()
            except Exception as json_err:
                log.error(f"Bybit JSON-Fehler ({path}): {json_err} | HTTP {r.status_code} | Body: {r.text[:300]}")
                return {}
            return data
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            log.error(f"Bybit GET Fehler ({path}): {e}")
            return {}
    return {}

def bybit_post(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    for attempt in range(3):
        try:
            r = requests.post(BYBIT_BASE_URL + path,
                              headers=_headers_bybit(body_str),
                              data=body_str, timeout=10)
            data = r.json()
            if r.status_code >= 500 and attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            return data
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            log.error(f"Bybit POST Fehler ({path}): {e}")
            return {}
    return {}

def bybit_ok(res: dict) -> bool:
    return int(res.get("retCode", -1)) == 0


def get_available_balance() -> float:
    """
    Liest den verfügbaren USDT-Balance vom Bybit Sub-Account.
    Probiert UNIFIED → CONTRACT → SPOT der Reihe nach (AI Subaccounts haben oft kein UTA).
    Wendet SYNORA_BUDGET_CAP_USDT als optionale Obergrenze an.
    """
    for account_type in ("UNIFIED", "CONTRACT", "SPOT"):
        res = bybit_get("/v5/account/wallet-balance", {"accountType": account_type})
        rc = res.get("retCode", -1) if res else -1
        if not res or rc != 0:
            log.info(f"Balance ({account_type}): retCode={rc} msg={res.get('retMsg','?') if res else 'no response'}")
            continue
        try:
            accounts = (res.get("result") or {}).get("list") or []
            for account in accounts:
                coins = account.get("coin") or []
                # Debug: alle Coins + Felder loggen
                for coin in coins:
                    if coin.get("coin") == "USDT":
                        log.info(f"USDT Felder ({account_type}): {json.dumps(coin)}")
                        balance = float(
                            coin.get("availableToWithdraw") or
                            coin.get("availableBalance") or
                            coin.get("walletBalance") or 0
                        )
                        if SYNORA_BUDGET_CAP_USDT > 0:
                            balance = min(balance, SYNORA_BUDGET_CAP_USDT)
                        log.info(f"Bybit Balance ({account_type}): {balance:.2f} USDT"
                                 + (f" (Cap: {SYNORA_BUDGET_CAP_USDT:.0f})" if SYNORA_BUDGET_CAP_USDT > 0 else ""))
                        return balance
                if not coins:
                    log.info(f"Balance ({account_type}): leere coin-Liste — kein Geld auf Sub-Account?")
        except Exception as e:
            log.error(f"Balance-Abruf Fehler ({account_type}): {e}")

    log.warning("Balance-Abruf: kein USDT-Saldo in UNIFIED/CONTRACT/SPOT gefunden → 0.0")
    return 0.0


# ─── Precision-Caches ────────────────────────────────────────
_price_dec_cache: dict = {}
_qty_step_cache:  dict = {}
_max_lev_cache:   dict = {}

def _load_instrument_info(symbol: str) -> None:
    res = bybit_get("/v5/market/instruments-info", {
        "category": BYBIT_CATEGORY, "symbol": symbol
    })
    try:
        items = (res.get("result") or {}).get("list") or []
        for item in items:
            if item.get("symbol") != symbol:
                continue
            lot_f   = item.get("lotSizeFilter", {})
            price_f = item.get("priceFilter", {})
            lev_f   = item.get("leverageFilter", {})

            qty_step = float(lot_f.get("qtyStep", "0.001") or "0.001")
            _qty_step_cache[symbol] = qty_step

            tick = float(price_f.get("tickSize", "0.0001") or "0.0001")
            if tick >= 1:
                pd = 0
            else:
                s = f"{tick:.10f}".rstrip("0")
                pd = len(s.split(".")[-1]) if "." in s else 4
            _price_dec_cache[symbol] = pd

            max_lev = int(float(lev_f.get("maxLeverage", "25") or "25"))
            _max_lev_cache[symbol] = max_lev
    except Exception as e:
        log.error(f"instrument-info Fehler ({symbol}): {e}")

def price_decimals(symbol: str) -> int:
    if symbol not in _price_dec_cache:
        _load_instrument_info(symbol)
    return _price_dec_cache.get(symbol, 4)

def snap_qty(symbol: str, qty: float) -> float:
    """Qty auf qtyStep flooren."""
    step = _qty_step_cache.get(symbol)
    if step is None:
        _load_instrument_info(symbol)
        step = _qty_step_cache.get(symbol, 0.001)
    if not step or step <= 0:
        step = 0.001
    return math.floor(qty / step) * step

def fmt_qty(symbol: str, qty: float) -> str:
    step = _qty_step_cache.get(symbol, 0.001)
    if step >= 1:
        return str(int(snap_qty(symbol, qty)))
    decimals = len(f"{step:.10f}".rstrip("0").split(".")[-1]) if "." in f"{step:.10f}".rstrip("0") else 3
    return f"{snap_qty(symbol, qty):.{decimals}f}"

def fmt_price(symbol: str, price: float) -> str:
    pd = price_decimals(symbol)
    return f"{price:.{pd}f}"

def max_leverage(symbol: str) -> int:
    if symbol not in _max_lev_cache:
        _load_instrument_info(symbol)
    return _max_lev_cache.get(symbol, 25)


# ═══════════════════════════════════════════════════════════════
# SIGNAL-PARSER
# ═══════════════════════════════════════════════════════════════

# Regex für Haupt-Signal
RE_SIGNAL = re.compile(
    r"SYNORA SIGNAL INCOMING",
    re.IGNORECASE,
)
RE_SIDE_SYMBOL_LEV = re.compile(
    r"gehen\s+(LONG|SHORT)\s+auf\s+(\w+USDT)\s+mit\s+(\d+)X\s+Hebel",
    re.IGNORECASE,
)
RE_ENTRY_PRICE = re.compile(
    r"Signal-Preis[:\s]*([\d.]+)",
    re.IGNORECASE,
)
RE_DCA_PRICES = re.compile(
    r"Bei\s+([\d.]+)\s+\(25%\).*?(?:und|,)\s*([\d.]+)\s+\(65%\)",
    re.IGNORECASE | re.DOTALL,
)
RE_SL_PRICE = re.compile(
    r"Kurs auf\s+([\d.]+)\s+fällt",
    re.IGNORECASE,
)

# Regex für UPDATE-Nachricht
RE_UPDATE = re.compile(
    r"SYNORA UPDATE.*?(\w+USDT).*?Maximaler Gewinn[:\s]*([\d.]+)\s*%",
    re.IGNORECASE | re.DOTALL,
)

# Regex für CANCEL/CLOSE-Nachricht (falls Synora sowas sendet)
RE_CLOSE = re.compile(
    r"SYNORA.*?(?:CANCEL|CLOSE|STOP|ABBRUCH).*?(\w+USDT)",
    re.IGNORECASE | re.DOTALL,
)


def parse_signal(text: str) -> dict | None:
    """Parst ein SYNORA SIGNAL INCOMING! und gibt ein Dict zurück oder None."""
    if not RE_SIGNAL.search(text):
        return None

    m_sdl = RE_SIDE_SYMBOL_LEV.search(text)
    m_entry = RE_ENTRY_PRICE.search(text)
    m_dca   = RE_DCA_PRICES.search(text)
    m_sl    = RE_SL_PRICE.search(text)

    if not (m_sdl and m_entry and m_dca and m_sl):
        log.warning(f"Signal unvollständig — fehlende Felder:\n{text[:300]}")
        return None

    side   = m_sdl.group(1).upper()        # "LONG" oder "SHORT"
    symbol = m_sdl.group(2).upper()        # z.B. "SUSHIUSDT"
    lev    = int(m_sdl.group(3))           # z.B. 10
    entry  = float(m_entry.group(1))       # Market-Entry-Preis
    dca1   = float(m_dca.group(1))         # Limit-Preis DCA1 (25%)
    dca2   = float(m_dca.group(2))         # Limit-Preis DCA2 (65%)
    sl     = float(m_sl.group(1))          # Stop-Loss-Preis

    return {
        "side":   side,
        "symbol": symbol,
        "lev":    lev,
        "entry":  entry,
        "dca1":   dca1,
        "dca2":   dca2,
        "sl":     sl,
    }


def parse_update(text: str) -> dict | None:
    """Parst ein SYNORA UPDATE und gibt {symbol, max_gain_pct} zurück oder None."""
    m = RE_UPDATE.search(text)
    if not m:
        return None
    return {
        "symbol":       m.group(1).upper(),
        "max_gain_pct": float(m.group(2)),
    }


def parse_close(text: str) -> str | None:
    """Parst eine SYNORA CANCEL/CLOSE Nachricht → Symbol oder None."""
    m = RE_CLOSE.search(text)
    return m.group(1).upper() if m else None


# ═══════════════════════════════════════════════════════════════
# BYBIT ORDER-EXECUTION
# ═══════════════════════════════════════════════════════════════

def set_leverage(symbol: str, lev: int) -> bool:
    """Setzt den Hebel auf dem Bybit Sub-Account."""
    max_lev = max_leverage(symbol)
    effective_lev = min(lev, max_lev)
    if lev > max_lev:
        log.warning(f"Hebel {lev}x > Max {max_lev}x für {symbol}, nutze {max_lev}x")
    res = bybit_post("/v5/position/set-leverage", {
        "category":     BYBIT_CATEGORY,
        "symbol":       symbol,
        "buyLeverage":  str(effective_lev),
        "sellLeverage": str(effective_lev),
    })
    ok = bybit_ok(res)
    if not ok:
        # Fehler 110043 = Hebel unverändert → kein echtes Problem
        if res.get("retCode") == 110043:
            return True
        log.error(f"set-leverage Fehler: {res}")
    return ok or res.get("retCode") == 110043


def get_mark_price(symbol: str) -> float:
    """Holt den aktuellen Mark-Preis von Bybit."""
    res = bybit_get("/v5/market/tickers", {"category": BYBIT_CATEGORY, "symbol": symbol})
    try:
        items = (res.get("result") or {}).get("list") or []
        for item in items:
            if item.get("symbol") == symbol:
                return float(item.get("markPrice", 0))
    except Exception as e:
        log.error(f"markPrice Fehler ({symbol}): {e}")
    return 0.0


def calc_qty(symbol: str, usdt_margin: float, lev: int, ref_price: float) -> float:
    """Berechnet Kontrakt-Qty aus USDT-Margin, Hebel und Referenzpreis."""
    notional = usdt_margin * lev
    qty = notional / ref_price
    return snap_qty(symbol, qty)


def place_market_order(symbol: str, side: str, qty: float) -> dict:
    """Platziert eine Market-Order (Entry)."""
    bybit_side = "Buy" if side == "LONG" else "Sell"
    res = bybit_post("/v5/order/create", {
        "category":    BYBIT_CATEGORY,
        "symbol":      symbol,
        "side":        bybit_side,
        "orderType":   "Market",
        "qty":         fmt_qty(symbol, qty),
        "timeInForce": "GTC",
        "reduceOnly":  False,
        "closeOnTrigger": False,
    })
    return res


def place_limit_order(symbol: str, side: str, qty: float, price: float) -> dict:
    """Platziert eine Limit-Order (DCA)."""
    bybit_side = "Buy" if side == "LONG" else "Sell"
    res = bybit_post("/v5/order/create", {
        "category":    BYBIT_CATEGORY,
        "symbol":      symbol,
        "side":        bybit_side,
        "orderType":   "Limit",
        "qty":         fmt_qty(symbol, qty),
        "price":       fmt_price(symbol, price),
        "timeInForce": "GTC",
        "reduceOnly":  False,
        "closeOnTrigger": False,
    })
    return res


def set_sl(symbol: str, side: str, sl_price: float) -> dict:
    """Setzt den Stop-Loss auf der offenen Position."""
    res = bybit_post("/v5/position/trading-stop", {
        "category":  BYBIT_CATEGORY,
        "symbol":    symbol,
        "positionIdx": 0,
        "stopLoss":  fmt_price(symbol, sl_price),
        "slTriggerBy": "MarkPrice",
    })
    return res


def set_tp(symbol: str, side: str, tp_price: float) -> dict:
    """Setzt Take-Profit auf der offenen Position."""
    res = bybit_post("/v5/position/trading-stop", {
        "category":  BYBIT_CATEGORY,
        "symbol":    symbol,
        "positionIdx": 0,
        "takeProfit": fmt_price(symbol, tp_price),
        "tpTriggerBy": "MarkPrice",
    })
    return res


def cancel_open_orders(symbol: str) -> None:
    """Storniert alle offenen Orders für ein Symbol (DCA-Orders)."""
    bybit_post("/v5/order/cancel-all", {
        "category": BYBIT_CATEGORY,
        "symbol":   symbol,
    })


def close_position_market(symbol: str, side: str, qty: float) -> dict:
    """Schließt eine Position per Market-Order."""
    close_side = "Sell" if side == "LONG" else "Buy"
    res = bybit_post("/v5/order/create", {
        "category":      BYBIT_CATEGORY,
        "symbol":        symbol,
        "side":          close_side,
        "orderType":     "Market",
        "qty":           fmt_qty(symbol, qty),
        "timeInForce":   "GTC",
        "reduceOnly":    True,
        "closeOnTrigger": False,
    })
    return res


def calc_tp_price(side: str, entry: float, max_gain_pct: float, lev: int) -> float:
    """
    Berechnet den TP-Preis aus dem Maximalen Gewinn %.

    SYNORA_MAX_GAIN_MODE = "roi":
        max_gain_pct = ROI auf die Margin (z.B. 25% bei 10X = 2.5% Preisbewegung)
        TP = entry ± (max_gain_pct / lev) %

    SYNORA_MAX_GAIN_MODE = "price":
        max_gain_pct = direkte Preisbewegung in % vom Entry
        TP = entry ± max_gain_pct %
    """
    if SYNORA_MAX_GAIN_MODE == "roi":
        pct_move = max_gain_pct / lev / 100.0
    else:
        pct_move = max_gain_pct / 100.0

    if side == "LONG":
        return entry * (1 + pct_move)
    else:
        return entry * (1 - pct_move)


# ═══════════════════════════════════════════════════════════════
# SIGNAL-AUSFÜHRUNG
# ═══════════════════════════════════════════════════════════════

async def execute_signal(sig: dict) -> None:
    """Führt ein gepartes Synora-Signal vollständig aus."""
    symbol = sig["symbol"]
    side   = sig["side"]
    lev    = sig["lev"]
    entry  = sig["entry"]
    dca1   = sig["dca1"]
    dca2   = sig["dca2"]
    sl     = sig["sl"]

    log.info(f"Führe aus: {side} {symbol} {lev}X | Entry={entry} SL={sl} DCA1={dca1} DCA2={dca2}")

    # Prüfen ob bereits ein Trade offen
    if symbol in _state.get("trades", {}):
        tg(f"⚠️ SYNORA: {symbol} bereits offen — Signal ignoriert")
        log.warning(f"Trade für {symbol} bereits im State, übersprungen")
        return

    # 1. Hebel setzen
    if not set_leverage(symbol, lev):
        tg(f"❌ SYNORA: Hebel-Fehler für {symbol} — Trade abgebrochen")
        return
    log.info(f"Hebel {lev}X gesetzt")

    # 2. Precision laden (wird gecacht)
    _load_instrument_info(symbol)

    # 3. Mark-Preis holen (für Qty-Berechnung)
    mark = get_mark_price(symbol)
    ref_price = mark if mark > 0 else entry
    log.info(f"Mark-Preis: {ref_price}")

    # 4. Budget vom Sub-Account lesen (live, inkl. optionalem Cap)
    budget = get_available_balance()
    if budget < 5.0:
        tg(f"❌ SYNORA: Balance zu gering ({budget:.2f} USDT) — Trade abgebrochen")
        log.error(f"Balance {budget:.2f} USDT unter Minimum — abgebrochen")
        return

    qty_entry = calc_qty(symbol, budget * SPLIT_ENTRY, lev, ref_price)
    qty_dca1  = calc_qty(symbol, budget * SPLIT_DCA1,  lev, dca1)
    qty_dca2  = calc_qty(symbol, budget * SPLIT_DCA2,  lev, dca2)

    if qty_entry <= 0:
        tg(f"❌ SYNORA: Qty=0 für {symbol} (Budget zu klein?) — Trade abgebrochen")
        return

    # 5. Market Entry
    res_entry = place_market_order(symbol, side, qty_entry)
    if not bybit_ok(res_entry):
        err = res_entry.get("retMsg", "unbekannt")
        tg(f"❌ SYNORA: Market Order Fehler {symbol}: {err}")
        log.error(f"Market Order Fehler: {res_entry}")
        return
    entry_order_id = (res_entry.get("result") or {}).get("orderId", "?")
    log.info(f"Market Entry platziert: OrderId={entry_order_id}")

    # 6. Kurz warten damit Position existiert
    await asyncio.sleep(2)

    # 7. Stop-Loss setzen
    res_sl = set_sl(symbol, side, sl)
    if not bybit_ok(res_sl):
        log.warning(f"SL-Fehler (nicht kritisch): {res_sl}")

    # 8. DCA Limit Orders
    dca1_id = dca2_id = None
    if qty_dca1 > 0:
        res_dca1 = place_limit_order(symbol, side, qty_dca1, dca1)
        if bybit_ok(res_dca1):
            dca1_id = (res_dca1.get("result") or {}).get("orderId")
            log.info(f"DCA1 Limit {dca1} × {fmt_qty(symbol, qty_dca1)} platziert")
        else:
            log.warning(f"DCA1 Fehler: {res_dca1}")

    if qty_dca2 > 0:
        res_dca2 = place_limit_order(symbol, side, qty_dca2, dca2)
        if bybit_ok(res_dca2):
            dca2_id = (res_dca2.get("result") or {}).get("orderId")
            log.info(f"DCA2 Limit {dca2} × {fmt_qty(symbol, qty_dca2)} platziert")
        else:
            log.warning(f"DCA2 Fehler: {res_dca2}")

    # 9. State speichern
    _state.setdefault("trades", {})[symbol] = {
        "side":           side,
        "lev":            lev,
        "entry":          entry,
        "dca1":           dca1,
        "dca2":           dca2,
        "sl":             sl,
        "qty_entry":      qty_entry,
        "qty_dca1":       qty_dca1,
        "qty_dca2":       qty_dca2,
        "dca1_order_id":  dca1_id,
        "dca2_order_id":  dca2_id,
        "tp":             None,
        "opened_at":      datetime.utcnow().isoformat(),
        "budget_usdt":    budget,
    }
    save_state()

    # 10. Telegram-Benachrichtigung
    sl_dist_pct = abs(sl - entry) / entry * 100
    tg(
        f"🟣 <b>SYNORA Trade eröffnet</b>\n"
        f"Symbol: <b>{symbol}</b> {side} {lev}X\n"
        f"Entry (Market): {entry}\n"
        f"SL: {sl} ({sl_dist_pct:.2f}%)\n"
        f"DCA1: {dca1} × {fmt_qty(symbol, qty_dca1)} ({int(SPLIT_DCA1*100)}%)\n"
        f"DCA2: {dca2} × {fmt_qty(symbol, qty_dca2)} ({int(SPLIT_DCA2*100)}%)\n"
        f"Budget: {budget:.2f} USDT (live vom Sub-Account)"
    )


async def handle_update(upd: dict) -> None:
    """Verarbeitet eine SYNORA UPDATE Nachricht — setzt TP."""
    symbol      = upd["symbol"]
    max_gain    = upd["max_gain_pct"]

    trade = _state.get("trades", {}).get(symbol)
    if not trade:
        log.info(f"UPDATE für {symbol} — kein offener Trade im State")
        return

    side  = trade["side"]
    lev   = trade["lev"]
    entry = trade["entry"]

    tp_price = calc_tp_price(side, entry, max_gain, lev)

    log.info(f"UPDATE {symbol}: max_gain={max_gain}% → TP={tp_price:.6f}")

    res = set_tp(symbol, side, tp_price)
    if bybit_ok(res):
        _state["trades"][symbol]["tp"] = tp_price
        save_state()
        tg(
            f"🎯 <b>SYNORA TP gesetzt</b>\n"
            f"Symbol: <b>{symbol}</b>\n"
            f"Maximaler Gewinn: {max_gain}%\n"
            f"TP-Preis: {fmt_price(symbol, tp_price)}\n"
            f"(Mode: {SYNORA_MAX_GAIN_MODE})"
        )
    else:
        err = res.get("retMsg", "unbekannt")
        tg(f"⚠️ SYNORA: TP-Fehler {symbol}: {err}")
        log.warning(f"TP-Fehler: {res}")


async def handle_close(symbol: str, reason: str = "CANCEL") -> None:
    """Schließt eine Synora-Position und storniert offene Orders."""
    trade = _state.get("trades", {}).get(symbol)
    if not trade:
        log.info(f"CLOSE für {symbol} — kein offener Trade")
        return

    # Offene Limit-Orders stornieren
    cancel_open_orders(symbol)
    await asyncio.sleep(1)

    # Position per Market schließen
    total_qty = trade["qty_entry"] + trade["qty_dca1"] + trade["qty_dca2"]
    res = close_position_market(symbol, trade["side"], total_qty)

    if bybit_ok(res):
        await asyncio.sleep(2)  # kurz warten bis Bybit P&L verfügbar
        # Closed P&L von Bybit holen
        pnl_res = bybit_get("/v5/position/closed-pnl", {
            "category": BYBIT_CATEGORY, "symbol": symbol, "limit": "1",
        })
        pnl_items = (pnl_res.get("result") or {}).get("list") or []
        closed_pnl  = 0.0
        close_price = 0.0
        if pnl_items:
            p = pnl_items[0]
            closed_pnl  = float(p.get("closedPnl",    0) or 0)
            close_price = float(p.get("avgExitPrice", 0) or 0)

        csv_log_synora_trade({
            "opened_at":   trade.get("opened_at", ""),
            "closed_at":   datetime.now(timezone.utc).isoformat(),
            "symbol":      symbol,
            "side":        trade.get("side", ""),
            "lev":         trade.get("lev", ""),
            "entry":       trade.get("entry", ""),
            "sl":          trade.get("sl", ""),
            "tp":          trade.get("tp", ""),
            "budget_usdt": trade.get("budget_usdt", ""),
            "close_price": round(close_price, 6),
            "pnl_usdt":    round(closed_pnl, 4),
            "outcome":     reason,
        })
        del _state["trades"][symbol]
        save_state()
        pnl_str = f"+{closed_pnl:.2f}" if closed_pnl >= 0 else f"{closed_pnl:.2f}"
        tg(f"🔴 <b>SYNORA Position geschlossen</b>\n{symbol} ({reason})\nP&L: {pnl_str} USDT")
    else:
        err = res.get("retMsg", "unbekannt")
        tg(f"⚠️ SYNORA: Close-Fehler {symbol}: {err}")
        log.warning(f"Close-Fehler: {res}")


# ═══════════════════════════════════════════════════════════════
# BYBIT POSITION-POLLING  (erkennt TP/SL-Hits automatisch)
# ═══════════════════════════════════════════════════════════════

def _determine_outcome(trade: dict, close_price: float) -> str:
    """Ermittelt ob Trade via TP, SL oder manuell geschlossen wurde."""
    tp  = trade.get("tp")
    sl  = trade.get("sl")
    side = trade.get("side", "LONG")
    if not tp or not sl or not close_price:
        return "closed"
    tp_f  = float(tp)
    sl_f  = float(sl)
    cp    = float(close_price)
    tol   = 0.005   # 0.5% Toleranz
    if side == "LONG":
        if cp >= tp_f * (1 - tol):
            return "TP"
        elif cp <= sl_f * (1 + tol):
            return "SL"
    else:   # SHORT
        if cp <= tp_f * (1 + tol):
            return "TP"
        elif cp >= sl_f * (1 - tol):
            return "SL"
    return "manual"

async def check_closed_positions() -> None:
    """Periodischer Loop: prüft ob Synora-Positionen auf Bybit geschlossen wurden."""
    log.info("Position-Polling gestartet (Intervall: %ds)", POSITION_POLL_INTERVAL)
    while True:
        await asyncio.sleep(POSITION_POLL_INTERVAL)
        trades_snapshot = dict(_state.get("trades", {}))
        if not trades_snapshot:
            continue

        for symbol, trade in trades_snapshot.items():
            try:
                # Aktuelle Position auf Bybit abfragen
                res = bybit_get("/v5/position/list", {
                    "category": BYBIT_CATEGORY,
                    "symbol":   symbol,
                })
                items = (res.get("result") or {}).get("list") or []
                pos_size = 0.0
                for item in items:
                    if item.get("symbol") == symbol:
                        pos_size = float(item.get("size", "0") or "0")
                        break

                if pos_size > 0:
                    continue   # Position noch offen

                # Position ist zu — Closed-P&L von Bybit holen
                pnl_res = bybit_get("/v5/position/closed-pnl", {
                    "category": BYBIT_CATEGORY,
                    "symbol":   symbol,
                    "limit":    "1",
                })
                pnl_items = (pnl_res.get("result") or {}).get("list") or []
                closed_pnl  = 0.0
                close_price = 0.0
                if pnl_items:
                    p = pnl_items[0]
                    closed_pnl  = float(p.get("closedPnl",    0) or 0)
                    close_price = float(p.get("avgExitPrice", 0) or 0)

                outcome = _determine_outcome(trade, close_price)

                record = {
                    "opened_at":   trade.get("opened_at", ""),
                    "closed_at":   datetime.now(timezone.utc).isoformat(),
                    "symbol":      symbol,
                    "side":        trade.get("side", ""),
                    "lev":         trade.get("lev", ""),
                    "entry":       trade.get("entry", ""),
                    "sl":          trade.get("sl", ""),
                    "tp":          trade.get("tp", ""),
                    "budget_usdt": trade.get("budget_usdt", ""),
                    "close_price": round(close_price, 6),
                    "pnl_usdt":    round(closed_pnl, 4),
                    "outcome":     outcome,
                }
                csv_log_synora_trade(record)

                # Aus State entfernen
                _state["trades"].pop(symbol, None)
                save_state()

                emoji    = "✅" if closed_pnl >= 0 else "❌"
                pnl_str  = f"+{closed_pnl:.2f}" if closed_pnl >= 0 else f"{closed_pnl:.2f}"
                tg(
                    f"{emoji} <b>SYNORA Trade geschlossen</b>\n"
                    f"Symbol: <b>{symbol}</b> | {outcome}\n"
                    f"Close: {close_price} | P&L: <b>{pnl_str} USDT</b>"
                )
                log.info(f"Trade {symbol} geschlossen: {outcome} | P&L {pnl_str}")

            except Exception as e:
                log.error(f"Position-Polling Fehler ({symbol}): {e}")


# ═══════════════════════════════════════════════════════════════
# FLASK DASHBOARD
# ═══════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

def _compute_stats(trades: list) -> dict:
    total = len(trades)
    if total == 0:
        return dict(total=0, wins=0, losses=0, winrate=0,
                    net_pnl=0, avg_win=0, avg_loss=0)
    pnls  = [float(t.get("pnl_usdt", 0) or 0) for t in trades]
    wins  = [p for p in pnls if p > 0]
    losses= [p for p in pnls if p <= 0]
    return dict(
        total   = total,
        wins    = len(wins),
        losses  = len(losses),
        winrate = len(wins) / total * 100,
        net_pnl = sum(pnls),
        avg_win = sum(wins) / len(wins) if wins else 0,
        avg_loss= sum(losses) / len(losses) if losses else 0,
    )

def _equity_series(trades: list) -> list:
    running = 0.0
    series  = []
    for t in trades:
        running += float(t.get("pnl_usdt", 0) or 0)
        label    = (t.get("closed_at") or "")[:10]
        series.append({"x": label, "y": round(running, 2)})
    return series

def _outcome_counts(trades: list) -> dict:
    counts = {}
    for t in trades:
        o = t.get("outcome", "?")
        counts[o] = counts.get(o, 0) + 1
    return counts

@flask_app.route("/dashboard")
def dashboard():
    secret = request.args.get("secret", "")
    if SYNORA_DASHBOARD_SECRET and secret != SYNORA_DASHBOARD_SECRET:
        return Response("403 Forbidden", status=403)

    trades     = read_trades_csv()
    stats      = _compute_stats(trades)
    equity     = _equity_series(trades)
    outcomes   = _outcome_counts(trades)
    open_pos   = _state.get("trades", {})
    recent     = list(reversed(trades[-50:]))   # neueste zuerst
    live_bal   = get_available_balance()         # live vom Bybit Sub-Account

    eq_labels  = json.dumps([p["x"] for p in equity])
    eq_values  = json.dumps([p["y"] for p in equity])
    oc_labels  = json.dumps(list(outcomes.keys()))
    oc_values  = json.dumps(list(outcomes.values()))

    pnl_color  = "#22c55e" if stats["net_pnl"] >= 0 else "#ef4444"
    wr_color   = "#22c55e" if stats["winrate"] >= 50 else "#ef4444"

    open_rows = ""
    for sym, tr in open_pos.items():
        tp_str = f"{tr['tp']:.4f}" if tr.get("tp") else "—"
        open_rows += (
            f"<tr>"
            f"<td>{sym}</td>"
            f"<td class='{'long' if tr['side']=='LONG' else 'short'}'>{tr['side']}</td>"
            f"<td>{tr['lev']}x</td>"
            f"<td>{tr['entry']}</td>"
            f"<td>{tr['sl']}</td>"
            f"<td>{tp_str}</td>"
            f"<td>{(tr.get('opened_at','')[:16]).replace('T',' ')}</td>"
            f"</tr>"
        )

    trade_rows = ""
    for t in recent:
        pnl   = float(t.get("pnl_usdt", 0) or 0)
        pclass = "pos" if pnl >= 0 else "neg"
        tp_str = (t.get("tp") or "—")
        side_class = "long" if t.get("side","") == "LONG" else "short"
        trade_rows += (
            f"<tr>"
            f"<td>{(t.get('closed_at','')[:16]).replace('T',' ')}</td>"
            f"<td>{t.get('symbol','')}</td>"
            f"<td class='{side_class}'>{t.get('side','')}</td>"
            f"<td>{t.get('lev','')}x</td>"
            f"<td>{t.get('entry','')}</td>"
            f"<td>{t.get('close_price','')}</td>"
            f"<td>{t.get('outcome','')}</td>"
            f"<td class='{pclass}'>{'+' if pnl>0 else ''}{pnl:.2f}</td>"
            f"</tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SYNORA Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0f1117;color:#e2e8f0;font-family:'Segoe UI',system-ui,sans-serif;padding:20px}}
  h1{{font-size:1.6rem;margin-bottom:4px;color:#a78bfa}}
  .subtitle{{color:#64748b;font-size:.85rem;margin-bottom:24px}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin-bottom:28px}}
  .card{{background:#1e2330;border-radius:10px;padding:18px;border:1px solid #2d3748}}
  .card label{{font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;display:block;margin-bottom:6px}}
  .card .val{{font-size:1.7rem;font-weight:700}}
  .pos{{color:#22c55e}} .neg{{color:#ef4444}}
  .long{{color:#22c55e}} .short{{color:#ef4444}}
  .chart-wrap{{background:#1e2330;border-radius:10px;padding:20px;margin-bottom:24px;border:1px solid #2d3748}}
  .chart-wrap h3{{color:#94a3b8;font-size:.85rem;text-transform:uppercase;letter-spacing:.05em;margin-bottom:14px}}
  .two-col{{display:grid;grid-template-columns:2fr 1fr;gap:16px;margin-bottom:24px}}
  @media(max-width:700px){{.two-col{{grid-template-columns:1fr}}}}
  table{{width:100%;border-collapse:collapse;font-size:.82rem}}
  th{{color:#64748b;text-align:left;padding:6px 10px;border-bottom:1px solid #2d3748;font-weight:500}}
  td{{padding:7px 10px;border-bottom:1px solid #1a2030}}
  tr:hover td{{background:#252d3d}}
  .section{{background:#1e2330;border-radius:10px;padding:20px;margin-bottom:24px;border:1px solid #2d3748}}
  .section h3{{color:#94a3b8;font-size:.85rem;text-transform:uppercase;letter-spacing:.05em;margin-bottom:14px}}
  .open-badge{{display:inline-block;background:#7c3aed22;color:#a78bfa;border:1px solid #7c3aed55;border-radius:6px;padding:2px 10px;font-size:.78rem;margin-left:8px}}
  .ts{{color:#64748b;font-size:.78rem;margin-bottom:24px}}
</style>
</head>
<body>
<h1>🟣 SYNORA Performance Dashboard</h1>
<p class="ts">Stand: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC &nbsp;·&nbsp; {stats['total']} Trades archiviert</p>

<div class="grid">
  <div class="card"><label>💰 Balance (live)</label><div class="val" style="color:#a78bfa">{live_bal:.2f} <span style="font-size:1rem;color:#64748b">USDT</span></div></div>
  <div class="card"><label>Trades gesamt</label><div class="val">{stats['total']}</div></div>
  <div class="card"><label>Wins / Losses</label><div class="val"><span class="pos">{stats['wins']}</span> / <span class="neg">{stats['losses']}</span></div></div>
  <div class="card"><label>Win Rate</label><div class="val" style="color:{wr_color}">{stats['winrate']:.1f}%</div></div>
  <div class="card"><label>Net P&amp;L (USDT)</label><div class="val" style="color:{pnl_color}">{'+' if stats['net_pnl']>=0 else ''}{stats['net_pnl']:.2f}</div></div>
  <div class="card"><label>Ø Gewinn</label><div class="val pos">{stats['avg_win']:.2f}</div></div>
  <div class="card"><label>Ø Verlust</label><div class="val neg">{stats['avg_loss']:.2f}</div></div>
</div>

<div class="two-col">
  <div class="chart-wrap">
    <h3>Equity-Kurve</h3>
    <canvas id="equity" height="120"></canvas>
  </div>
  <div class="chart-wrap">
    <h3>Exit-Ursachen</h3>
    <canvas id="outcomes" height="120"></canvas>
  </div>
</div>

{"" if not open_pos else f'''
<div class="section">
  <h3>Offene Positionen <span class="open-badge">{len(open_pos)} aktiv</span></h3>
  <table>
    <tr><th>Symbol</th><th>Side</th><th>Hb</th><th>Entry</th><th>SL</th><th>TP</th><th>Eröffnet</th></tr>
    {open_rows}
  </table>
</div>'''}

<div class="section">
  <h3>Letzte Trades (max. 50)</h3>
  <table>
    <tr><th>Geschlossen</th><th>Symbol</th><th>Side</th><th>Hb</th><th>Entry</th><th>Close</th><th>Outcome</th><th>P&amp;L</th></tr>
    {"<tr><td colspan='8' style='color:#64748b;text-align:center;padding:20px'>Noch keine Trades</td></tr>" if not recent else trade_rows}
  </table>
</div>

<script>
const EQ_LABELS = {eq_labels};
const EQ_VALUES = {eq_values};
const OC_LABELS = {oc_labels};
const OC_VALUES = {oc_values};
const GRID_COLOR = 'rgba(255,255,255,0.05)';
Chart.defaults.color = '#64748b';

if(EQ_LABELS.length > 0){{
  new Chart(document.getElementById('equity'), {{
    type: 'line',
    data: {{
      labels: EQ_LABELS,
      datasets: [{{
        label: 'Equity (USDT)',
        data: EQ_VALUES,
        borderColor: '#a78bfa',
        backgroundColor: 'rgba(167,139,250,0.12)',
        fill: true,
        tension: 0.3,
        pointRadius: EQ_LABELS.length < 30 ? 3 : 0,
        borderWidth: 2,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: GRID_COLOR }}, ticks: {{ maxTicksLimit: 8 }} }},
        y: {{ grid: {{ color: GRID_COLOR }}, ticks: {{ callback: v => v.toFixed(2) + ' $' }} }}
      }}
    }}
  }});
}}

if(OC_LABELS.length > 0){{
  const COLORS = ['#22c55e','#ef4444','#f59e0b','#60a5fa','#a78bfa'];
  new Chart(document.getElementById('outcomes'), {{
    type: 'doughnut',
    data: {{
      labels: OC_LABELS,
      datasets: [{{ data: OC_VALUES, backgroundColor: COLORS, borderWidth: 0 }}]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ padding: 12, font: {{ size: 11 }} }} }} }}
    }}
  }});
}}
</script>
</body>
</html>"""
    return Response(html, content_type="text/html; charset=utf-8")


def run_flask() -> None:
    """Startet Flask in einem Daemon-Thread."""
    flask_app.run(host="0.0.0.0", port=SYNORA_DASHBOARD_PORT, debug=False, use_reloader=False)


# ═══════════════════════════════════════════════════════════════
# TELETHON — BOT/KANAL-LISTENER
# ═══════════════════════════════════════════════════════════════

async def main() -> None:
    load_state()

    if not all([API_ID, API_HASH, SESSION_STRING]):
        log.error("TELEGRAM_API_ID / TELEGRAM_API_HASH / DOMINUS_SESSION_STRING fehlen!")
        return
    if not SYNORA_CHANNEL:
        log.error("SYNORA_CHANNEL_ID fehlt!")
        return
    if not BYBIT_SYNORA_API_KEY or not BYBIT_SYNORA_PRIVATE_KEY:
        log.error("BYBIT_SYNORA_API_KEY / BYBIT_SYNORA_PRIVATE_KEY fehlen!")
        return

    # ── Flask-Dashboard in Daemon-Thread starten ─────────────
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    log.info(f"Dashboard läuft auf Port {SYNORA_DASHBOARD_PORT} ✓")

    cap_info = f" | Cap: {SYNORA_BUDGET_CAP_USDT:.0f} USDT" if SYNORA_BUDGET_CAP_USDT > 0 else " | kein Cap"
    log.info(f"Starte Synora Monitor | Budget: live vom Sub-Account{cap_info} | Source: {SYNORA_CHANNEL}")
    cap_str = f"{SYNORA_BUDGET_CAP_USDT:.0f} USDT Cap" if SYNORA_BUDGET_CAP_USDT > 0 else "kein Cap"
    tg(f"🟣 <b>SYNORA Monitor gestartet</b>\nBudget: live vom Sub-Account ({cap_str}) | Bybit Sub-Account")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    # ── Position-Polling starten ─────────────────────────────
    asyncio.ensure_future(check_closed_positions())

    # Source resolven — funktioniert für Bot-Chats (int user_id) UND Kanäle (int/-100... oder Link)
    try:
        channel_id_raw = int(SYNORA_CHANNEL)
    except ValueError:
        channel_id_raw = SYNORA_CHANNEL  # Einladungslink

    try:
        source = await client.get_entity(channel_id_raw)
        label = getattr(source, 'title', None) or getattr(source, 'username', None) or str(channel_id_raw)
        log.info(f"Source verbunden: {label}")
    except Exception:
        # Fallback: direkt mit der rohen ID registrieren (klappt wenn Entity bereits im Session-Cache)
        log.warning(f"get_entity fehlgeschlagen — nutze ID direkt: {channel_id_raw}")
        source = channel_id_raw

    @client.on(events.NewMessage(chats=source))
    async def on_message(event):
        text = event.message.message or ""
        log.info(f"Neue Nachricht ({len(text)} Zeichen):\n{text[:200]}")

        # ── Signal ──────────────────────────────────────────
        sig = parse_signal(text)
        if sig:
            log.info(f"Signal erkannt: {sig}")
            await execute_signal(sig)
            return

        # ── Update (TP) ──────────────────────────────────────
        upd = parse_update(text)
        if upd:
            log.info(f"Update erkannt: {upd}")
            await handle_update(upd)
            return

        # ── Close / Cancel ───────────────────────────────────
        close_sym = parse_close(text)
        if close_sym:
            log.info(f"Close erkannt: {close_sym}")
            await handle_close(close_sym, reason="SYNORA CANCEL")
            return

    log.info("Warte auf Signale …")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
