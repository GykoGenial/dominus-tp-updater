#!/usr/bin/env python3
"""
SYNORA Monitor  v1.6  (2026-06-09)
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
import hmac
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

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
SESSION_STRING    = os.environ.get("SYNORA_SESSION_STRING") or os.environ.get("DOMINUS_SESSION_STRING", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Telegram Forum Topics (Option C — Supergruppe mit Coin-Threads) ───────────
TELEGRAM_FORUM_GROUP_ID   = int(os.environ.get("TELEGRAM_FORUM_GROUP_ID",   "0") or "0")
TELEGRAM_SYSTEM_TOPIC_ID  = int(os.environ.get("TELEGRAM_SYSTEM_TOPIC_ID",  "0") or "0")
TELEGRAM_REPORTS_TOPIC_ID = int(os.environ.get("TELEGRAM_REPORTS_TOPIC_ID", "0") or "0")
TELEGRAM_STATUS_TOPIC_ID  = int(os.environ.get("TELEGRAM_STATUS_TOPIC_ID",  "0") or "0")
# Strategie-Emoji-Prefix für Coin-Topics (z.B. "🟡" → "🟡 BTCUSDT").
# Ermöglicht Koexistenz mehrerer Strategien in derselben Telegram-Supergroup.
STRATEGY_EMOJI            = os.environ.get("STRATEGY_EMOJI", "").strip()
_FORUM_ENABLED = bool(TELEGRAM_FORUM_GROUP_ID)

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
POSITION_POLL_INTERVAL   = 60   # Sekunden zwischen Position-Checks

# BingX (Fallback wenn Symbol nicht auf Bybit verfügbar)
BINGX_API_KEY    = os.environ.get("BINGX_API_KEY", "")
BINGX_SECRET_KEY = os.environ.get("BINGX_SECRET_KEY", "")
BINGX_BASE_URL   = "https://open-api.bingx.com"

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

# Dynamisches Profit-Modell
# 1 = einfacher TP via UPDATE-Nachricht (alter Modus)
# 3 = 6-Stufen-Modell (aktiviert nach UPDATE-Nachricht)
SYNORA_PROFIT_MODEL = int(os.environ.get("SYNORA_PROFIT_MODEL", "3"))

# Tabelle: (profit_pct, payout_pct_of_initial, sl_offset_pct_or_None)
# profit_pct    = ROI auf Margin (% bei roi-Mode) oder Preis-% (bei price-Mode)
# payout_pct    = % der INITIALEN Positions-Grösse zu schliessen
# sl_offset_pct = Profit-% der im SL gesichert wird (None = kein Update)
DYNAMIC_MODEL_LEVELS = [
    (10,   25.0,  0),    # +10%: 25% schliessen, SL → Break-Even
    (15,   12.5,  None), # +15%: 12.5% schliessen, SL unverändert (bereits Break-Even)
    (20,   25.0,  5),    # +20%: 25% schliessen, SL → +5% gesichert
    (30,   12.5,  15),   # +30%: 12.5% schliessen, SL → +15% gesichert
    (40,   12.5,  25),   # +40%: 12.5% schliessen, SL → +25% gesichert
    (50,   12.5,  None), # +50%: letzte 12.5%, SL-Update optional
]

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
    """Sendet eine Nachricht an den Telegram-Chat (Fallback / Direkt-Chat)."""
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
# TELEGRAM FORUM TOPICS  (Option C — Supergruppe mit Coin-Threads)
# ═══════════════════════════════════════════════════════════════
#
# Farb-Konstanten (Telegram API icon_color):
#   13338331 = teal     → offener Long-Trade
#   16766590 = orange   → offener Short-Trade
#    8311585 = grün     → Trade gewonnen (TP)
#    7322096 = rot      → Trade verloren (SL)
#    6316715 = blau     → System / Alarme
#
# Konzept für 160 Coins:
#   Topics werden LAZY erstellt — nur wenn ein Trade öffnet.
#   Nach Trade-Close wird das Topic archiviert (closeForumTopic).
#   → Im Hub sieht man nur aktuell offene Trades (~2-10 gleichzeitig).
#   Geschlossene Topics sinken zum Boden, bleiben aber als History abrufbar.
#
TG_COLOR_LONG_OPEN  = 13338331   # teal
TG_COLOR_SHORT_OPEN = 16766590   # orange
TG_COLOR_WIN        = 8311585    # grün
TG_COLOR_LOSS       = 7322096    # rot
TG_COLOR_SYSTEM     = 6316715    # blau

_coin_topic_cache: dict = {}     # symbol → thread_id (in-memory; befüllt aus State beim Zugriff)


def _forum_api(method: str, params: dict) -> dict:
    """Sendet einen Telegram Bot API Request."""
    if not TELEGRAM_TOKEN:
        return {}
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
            json=params, timeout=10,
        )
        return r.json()
    except Exception as e:
        log.error(f"Forum-API Fehler ({method}): {e}")
        return {}


def tg_topic(msg: str, thread_id: int = 0, parse_mode: str = "HTML") -> None:
    """Sendet eine Nachricht an ein spezifisches Forum-Topic.
    Falls Forum nicht konfiguriert oder thread_id=0: Fallback auf TELEGRAM_CHAT_ID.
    """
    if not TELEGRAM_TOKEN:
        log.info(f"[TG-TOPIC] {msg}")
        return
    use_forum = _FORUM_ENABLED and bool(thread_id)
    chat_id   = str(TELEGRAM_FORUM_GROUP_ID) if use_forum else TELEGRAM_CHAT_ID
    body: dict = {
        "chat_id":                  chat_id,
        "text":                     msg,
        "parse_mode":               parse_mode,
        "disable_web_page_preview": True,
    }
    if use_forum:
        body["message_thread_id"] = thread_id
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=body, timeout=10,
        )
    except Exception as e:
        log.error(f"Telegram-Topic Fehler: {e}")


def tg_coin(symbol: str, msg: str) -> None:
    """Sendet ans coin-spezifische Trade-Topic (oder Fallback auf normalen Chat)."""
    if not _FORUM_ENABLED:
        tg(msg)
        return
    tid = _get_coin_topic_id(symbol)
    tg_topic(msg, thread_id=tid)


def tg_system(msg: str) -> None:
    """Sendet ans System-Alarme Topic (oder Fallback auf normalen Chat)."""
    tg_topic(msg, thread_id=TELEGRAM_SYSTEM_TOPIC_ID) if _FORUM_ENABLED else tg(msg)


def tg_report(msg: str) -> None:
    """Sendet ans Berichte Topic (oder Fallback auf normalen Chat)."""
    tg_topic(msg, thread_id=TELEGRAM_REPORTS_TOPIC_ID) if _FORUM_ENABLED else tg(msg)


def tg_status(msg: str) -> None:
    """Sendet ans Bot-Status Topic (oder Fallback auf normalen Chat)."""
    tg_topic(msg, thread_id=TELEGRAM_STATUS_TOPIC_ID) if _FORUM_ENABLED else tg(msg)


def _forum_create_topic(name: str, icon_color: int = TG_COLOR_SYSTEM) -> int:
    """Erstellt ein neues Forum-Topic; gibt thread_id zurück (0 = Fehler)."""
    res = _forum_api("createForumTopic", {
        "chat_id":    TELEGRAM_FORUM_GROUP_ID,
        "name":       name,
        "icon_color": icon_color,
    })
    return int((res.get("result") or {}).get("message_thread_id") or 0)


def _forum_close_topic(thread_id: int) -> None:
    """Archiviert ein Forum-Topic (erscheint am Boden der Liste)."""
    _forum_api("closeForumTopic", {
        "chat_id":           TELEGRAM_FORUM_GROUP_ID,
        "message_thread_id": thread_id,
    })


def _forum_reopen_topic(thread_id: int) -> None:
    """Öffnet ein archiviertes Forum-Topic wieder."""
    _forum_api("reopenForumTopic", {
        "chat_id":           TELEGRAM_FORUM_GROUP_ID,
        "message_thread_id": thread_id,
    })


def _forum_edit_color(thread_id: int, icon_color: int) -> None:
    """Ändert die Icon-Farbe eines Forum-Topics."""
    _forum_api("editForumTopic", {
        "chat_id":           TELEGRAM_FORUM_GROUP_ID,
        "message_thread_id": thread_id,
        "icon_color":        icon_color,
    })


def _get_coin_topic_id(symbol: str) -> int:
    """Gibt die thread_id für ein Coin-Topic zurück (Cache → State → 0)."""
    if symbol in _coin_topic_cache:
        return _coin_topic_cache[symbol]
    tid = int((_state.get("telegram_topics") or {}).get(symbol, {}).get("thread_id") or 0)
    if tid:
        _coin_topic_cache[symbol] = tid
    return tid


def _open_coin_topic(symbol: str, side: str) -> int:
    """
    Erstellt oder öffnet (reopen) ein Coin-Trade-Topic beim Trade-Entry.
    Speichert thread_id im State und Cache. Gibt thread_id zurück.
    """
    if not _FORUM_ENABLED:
        return 0
    color = TG_COLOR_LONG_OPEN if side == "LONG" else TG_COLOR_SHORT_OPEN
    tid   = 0

    existing = (_state.get("telegram_topics") or {}).get(symbol)
    if existing:
        tid = int(existing.get("thread_id") or 0)

    if tid:
        # Bereits bekanntes Topic — reopenen + Farbe setzen
        _forum_reopen_topic(tid)
        _forum_edit_color(tid, color)
        _state.setdefault("telegram_topics", {})[symbol]["status"] = "open"
    else:
        # Neues Topic erstellen (lazy)
        coin_name  = symbol.replace("USDT", "").replace("PERP", "")
        dir_icon   = '📈' if side == 'LONG' else '📉'
        prefix     = f"{STRATEGY_EMOJI} " if STRATEGY_EMOJI else ""
        label      = f"{prefix}{dir_icon} {coin_name}"
        tid        = _forum_create_topic(label, icon_color=color)
        if not tid:
            log.error(f"Forum: Topic konnte nicht erstellt werden für {symbol}")
            return 0
        _state.setdefault("telegram_topics", {})[symbol] = {
            "thread_id": tid,
            "status":    "open",
        }

    _coin_topic_cache[symbol] = tid
    save_state()
    return tid


def _close_coin_topic(symbol: str, won: Optional[bool] = None) -> None:
    """
    Schliesst das Coin-Topic nach Trade-Ende und setzt die finale Icon-Farbe.
    won=True  → grün (TP-Win)
    won=False → rot  (SL-Loss)
    won=None  → Farbe unverändert
    Topic wird archiviert (sinkt in der Gruppen-Liste nach unten).
    """
    if not _FORUM_ENABLED:
        return
    tid = _get_coin_topic_id(symbol)
    if not tid:
        return
    if won is True:
        _forum_edit_color(tid, TG_COLOR_WIN)
    elif won is False:
        _forum_edit_color(tid, TG_COLOR_LOSS)
    _forum_close_topic(tid)

    topics = _state.setdefault("telegram_topics", {})
    if symbol in topics:
        topics[symbol]["status"] = "closed"
        save_state()

    _coin_topic_cache.pop(symbol, None)
    log.info(f"Forum: Topic für {symbol} geschlossen (won={won})")


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


_balance_cache: dict = {"value": 0.0, "ts": 0.0}
_main_loop = None   # wird in main() gesetzt, für Flask→Asyncio Bridge
_paused    = False  # /pause → True, /resume → False
_cmd_offset = 0     # getUpdates long-polling offset
BALANCE_CACHE_TTL = 30   # Sekunden

def get_available_balance() -> float:
    """
    Liest den verfügbaren USDT-Balance vom Bybit UNIFIED-Wallet.
    Ergebnis wird 30s gecacht damit Dashboard-Refreshes kein API-Spam erzeugen.
    Wendet SYNORA_BUDGET_CAP_USDT als optionale Obergrenze an.
    """
    now = time.time()
    if now - _balance_cache["ts"] < BALANCE_CACHE_TTL:
        return _balance_cache["value"]

    res = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    rc = res.get("retCode", -1) if res else -1
    if not res or rc != 0:
        log.warning(f"Balance UNIFIED: retCode={rc} msg={res.get('retMsg','?') if res else 'no response'}")
        return _balance_cache["value"]   # letzten bekannten Wert zurückgeben

    try:
        accounts = (res.get("result") or {}).get("list") or []
        for account in accounts:
            for coin in (account.get("coin") or []):
                if coin.get("coin") == "USDT":
                    balance = float(
                        coin.get("availableToWithdraw") or
                        coin.get("availableBalance") or
                        coin.get("walletBalance") or 0
                    )
                    if SYNORA_BUDGET_CAP_USDT > 0:
                        balance = min(balance, SYNORA_BUDGET_CAP_USDT)
                    log.info(f"Bybit Balance (UNIFIED): {balance:.2f} USDT")
                    _balance_cache["value"] = balance
                    _balance_cache["ts"]    = now
                    return balance
    except Exception as e:
        log.error(f"Balance-Abruf Fehler: {e}")

    log.warning("Balance-Abruf: kein USDT in UNIFIED gefunden → 0.0")
    return 0.0


def fetch_bybit_balance_raw() -> float:
    """Liest Bybit-Balance OHNE Cap und OHNE Cache — für /balance Befehl."""
    res = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    rc = res.get("retCode", -1) if res else -1
    if not res or rc != 0:
        return -1.0
    try:
        accounts = (res.get("result") or {}).get("list") or []
        for account in accounts:
            for coin in (account.get("coin") or []):
                if coin.get("coin") == "USDT":
                    return float(
                        coin.get("availableToWithdraw") or
                        coin.get("availableBalance") or
                        coin.get("walletBalance") or 0
                    )
    except Exception:
        pass
    return -1.0


def fetch_bingx_balance_raw() -> float:
    """Liest BingX-Balance OHNE Cap und OHNE Cache — für /balance Befehl."""
    if not BINGX_API_KEY:
        return -2.0  # BingX nicht konfiguriert
    res = bingx_get("/openApi/swap/v2/user/balance")
    try:
        data = res.get("data") or {}
        return float(
            data.get("availableMargin") or
            data.get("available") or
            data.get("balance") or 0
        )
    except Exception:
        pass
    return -1.0


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
# BINGX API  (HMAC-SHA256 — Fallback wenn Symbol nicht auf Bybit)
# ═══════════════════════════════════════════════════════════════

def to_bingx_symbol(synora_symbol: str) -> str:
    """
    Konvertiert Synora-Symbol in BingX-Format.
    BROCCOLIF3BUSDT → BROCCOLIF3B-USDT
    SUSHIUSDT → SUSHI-USDT
    """
    if synora_symbol.endswith("USDT") and "-" not in synora_symbol:
        return synora_symbol[:-4] + "-USDT"
    return synora_symbol


def _bingx_sign(query_str: str) -> str:
    return hmac.new(
        BINGX_SECRET_KEY.encode("utf-8"),
        query_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _bingx_headers() -> dict:
    return {"X-BX-APIKEY": BINGX_API_KEY}


def _bingx_qs(params: dict) -> str:
    """Baut Query-String inkl. Timestamp + Signature."""
    p = dict(params)
    p["timestamp"] = str(int(time.time() * 1000))
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    return qs + "&signature=" + _bingx_sign(qs)


def bingx_get(path: str, params: dict = None) -> dict:
    url = BINGX_BASE_URL + path + "?" + _bingx_qs(params or {})
    for attempt in range(3):
        try:
            r = requests.get(url, headers=_bingx_headers(), timeout=10)
            data = r.json()
            if r.status_code >= 500 and attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            return data
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            log.error(f"BingX GET Fehler ({path}): {e}")
            return {}
    return {}


def bingx_post(path: str, params: dict = None) -> dict:
    url = BINGX_BASE_URL + path + "?" + _bingx_qs(params or {})
    for attempt in range(3):
        try:
            r = requests.post(url, headers=_bingx_headers(), timeout=10)
            data = r.json()
            if r.status_code >= 500 and attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            return data
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            log.error(f"BingX POST Fehler ({path}): {e}")
            return {}
    return {}


def bingx_delete(path: str, params: dict = None) -> dict:
    url = BINGX_BASE_URL + path + "?" + _bingx_qs(params or {})
    try:
        r = requests.delete(url, headers=_bingx_headers(), timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"BingX DELETE Fehler ({path}): {e}")
        return {}


def bingx_ok(res: dict) -> bool:
    return res.get("code", -1) == 0


# ─── BingX Precision-Caches ──────────────────────────────────
_bingx_price_dec_cache: dict = {}
_bingx_qty_step_cache:  dict = {}
_bingx_max_lev_cache:   dict = {}


def _load_bingx_instrument_info(symbol: str) -> None:
    """symbol = BingX-Format z.B. SUSHI-USDT (public endpoint, kein Auth nötig)"""
    res = requests.get(
        BINGX_BASE_URL + "/openApi/swap/v2/quote/contracts",
        timeout=10,
    ).json()
    try:
        data = res.get("data") or []
        for item in data:
            if item.get("symbol") != symbol:
                continue
            price_prec = int(item.get("pricePrecision", 4))
            qty_prec   = int(item.get("quantityPrecision", 3))
            max_lev    = int(item.get("maxLeverage", 25))
            _bingx_price_dec_cache[symbol] = price_prec
            _bingx_qty_step_cache[symbol]  = 10 ** (-qty_prec)
            _bingx_max_lev_cache[symbol]   = max_lev
            log.info(f"BingX instrument {symbol}: priceDec={price_prec} qtyStep={10**(-qty_prec)} maxLev={max_lev}")
            return
        log.warning(f"BingX: Symbol {symbol} nicht in Contracts-Liste")
    except Exception as e:
        log.error(f"BingX instrument-info Fehler ({symbol}): {e}")


def bingx_price_decimals(symbol: str) -> int:
    if symbol not in _bingx_price_dec_cache:
        _load_bingx_instrument_info(symbol)
    return _bingx_price_dec_cache.get(symbol, 4)


def bingx_snap_qty(symbol: str, qty: float) -> float:
    step = _bingx_qty_step_cache.get(symbol)
    if step is None:
        _load_bingx_instrument_info(symbol)
        step = _bingx_qty_step_cache.get(symbol, 0.001)
    if not step or step <= 0:
        step = 0.001
    return math.floor(qty / step) * step


def bingx_fmt_qty(symbol: str, qty: float) -> str:
    step = _bingx_qty_step_cache.get(symbol, 0.001)
    if step >= 1:
        return str(int(bingx_snap_qty(symbol, qty)))
    decimals = len(f"{step:.10f}".rstrip("0").split(".")[-1]) if "." in f"{step:.10f}".rstrip("0") else 3
    return f"{bingx_snap_qty(symbol, qty):.{decimals}f}"


def bingx_fmt_price(symbol: str, price: float) -> str:
    return f"{price:.{bingx_price_decimals(symbol)}f}"


def bingx_max_leverage(symbol: str) -> int:
    if symbol not in _bingx_max_lev_cache:
        _load_bingx_instrument_info(symbol)
    return _bingx_max_lev_cache.get(symbol, 25)


# ─── BingX Balance ───────────────────────────────────────────

_bingx_balance_cache: dict = {"value": 0.0, "ts": 0.0}


def get_bingx_balance() -> float:
    now = time.time()
    if now - _bingx_balance_cache["ts"] < BALANCE_CACHE_TTL:
        return _bingx_balance_cache["value"]
    res = bingx_get("/openApi/swap/v2/user/balance")
    try:
        data = res.get("data") or {}
        balance = float(
            data.get("availableMargin") or
            data.get("available") or
            data.get("balance") or 0
        )
        log.info(f"BingX raw balance data: {data}")   # zum Debuggen der Feldnamen
        if SYNORA_BUDGET_CAP_USDT > 0:
            balance = min(balance, SYNORA_BUDGET_CAP_USDT)
        log.info(f"BingX Balance: {balance:.2f} USDT")
        _bingx_balance_cache["value"] = balance
        _bingx_balance_cache["ts"]    = now
        return balance
    except Exception as e:
        log.error(f"BingX Balance Fehler: {e}")
    return _bingx_balance_cache["value"]


# ─── BingX Order-Funktionen ──────────────────────────────────

def bingx_set_leverage(symbol: str, side: str, lev: int) -> bool:
    """Setzt Hebel für LONG und SHORT getrennt (BingX Hedge-Mode)."""
    max_lev = bingx_max_leverage(symbol)
    eff_lev = min(lev, max_lev)
    if lev > max_lev:
        log.warning(f"BingX: Hebel {lev}x > Max {max_lev}x für {symbol}, nutze {max_lev}x")
    ok_l = bingx_ok(bingx_post("/openApi/swap/v2/trade/leverage", {
        "symbol": symbol, "side": "LONG", "leverage": eff_lev,
    }))
    ok_s = bingx_ok(bingx_post("/openApi/swap/v2/trade/leverage", {
        "symbol": symbol, "side": "SHORT", "leverage": eff_lev,
    }))
    if not (ok_l or ok_s):
        log.error(f"BingX set-leverage Fehler für {symbol}")
    return ok_l or ok_s


def bingx_get_mark_price(symbol: str) -> float:
    res = bingx_get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    try:
        data = res.get("data") or {}
        return float(data.get("price", 0) or 0)
    except Exception as e:
        log.error(f"BingX markPrice Fehler ({symbol}): {e}")
    return 0.0


def bingx_calc_qty(symbol: str, usdt_margin: float, lev: int, ref_price: float) -> float:
    notional = usdt_margin * lev
    return bingx_snap_qty(symbol, notional / ref_price)


def bingx_place_market_order(symbol: str, side: str, qty: float) -> dict:
    """side = LONG oder SHORT (Positionsseite)"""
    order_side = "BUY" if side == "LONG" else "SELL"
    return bingx_post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         order_side,
        "positionSide": side,
        "type":         "MARKET",
        "quantity":     bingx_fmt_qty(symbol, qty),
    })


def bingx_place_limit_order(symbol: str, side: str, qty: float, price: float) -> dict:
    order_side = "BUY" if side == "LONG" else "SELL"
    return bingx_post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         order_side,
        "positionSide": side,
        "type":         "LIMIT",
        "quantity":     bingx_fmt_qty(symbol, qty),
        "price":        bingx_fmt_price(symbol, price),
        "timeInForce":  "GTC",
    })


def bingx_place_tp_order(symbol: str, side: str, qty: float, price: float) -> dict:
    """Platziert eine Reduce-Only Limit-Order als Teil-TP (Modell 3)."""
    close_side = "SELL" if side == "LONG" else "BUY"
    return bingx_post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         close_side,
        "positionSide": side,
        "type":         "LIMIT",
        "quantity":     bingx_fmt_qty(symbol, qty),
        "price":        bingx_fmt_price(symbol, price),
        "timeInForce":  "GTC",
        "reduceOnly":   "true",
    })


def bingx_set_sl(symbol: str, side: str, sl_price: float) -> dict:
    close_side = "SELL" if side == "LONG" else "BUY"
    return bingx_post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         close_side,
        "positionSide": side,
        "type":         "STOP_MARKET",
        "stopPrice":    bingx_fmt_price(symbol, sl_price),
        "closePosition": "true",
        "workingType":  "MARK_PRICE",
    })


def bingx_set_tp(symbol: str, side: str, tp_price: float) -> dict:
    close_side = "SELL" if side == "LONG" else "BUY"
    return bingx_post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         close_side,
        "positionSide": side,
        "type":         "TAKE_PROFIT_MARKET",
        "stopPrice":    bingx_fmt_price(symbol, tp_price),
        "closePosition": "true",
        "workingType":  "MARK_PRICE",
    })


def bingx_cancel_open_orders(symbol: str) -> None:
    bingx_delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})


def bingx_close_position_market(symbol: str, side: str, qty: float) -> dict:
    close_side = "SELL" if side == "LONG" else "BUY"
    return bingx_post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         close_side,
        "positionSide": side,
        "type":         "MARKET",
        "quantity":     bingx_fmt_qty(symbol, qty),
        "reduceOnly":   "true",
    })


def bingx_get_position_size(symbol: str, side: str) -> float:
    """Gibt Positions-Grösse für Symbol + Seite zurück (0 = keine Position)."""
    res = bingx_get("/openApi/swap/v2/user/positions", {"symbol": symbol})
    try:
        data = res.get("data") or []
        for pos in data:
            if pos.get("symbol") == symbol and pos.get("positionSide") == side:
                amt = abs(float(pos.get("positionAmt", 0) or 0))
                log.info(f"BingX Position {symbol} {side}: {amt}")
                return amt
    except Exception as e:
        log.error(f"BingX position Fehler ({symbol}): {e}")
    return 0.0


def bingx_get_closed_pnl(symbol: str) -> tuple:
    """Gibt (closed_pnl, avg_close_price) des letzten abgeschlossenen Trades zurück."""
    res = bingx_get("/openApi/swap/v2/trade/marginOrders", {
        "symbol": symbol, "limit": "5",
    })
    try:
        data = res.get("data") or {}
        log.info(f"BingX closed-pnl raw: {data}")   # Feldnamen prüfen beim ersten Trade
        orders = data.get("marginOrderList") or data.get("orders") or []
        # Suche nach letzter gefüllter Close-Order
        for o in orders:
            status = o.get("status", "")
            reduce = o.get("reduceOnly", False)
            if status == "FILLED" and reduce:
                pnl   = float(o.get("profit", 0) or o.get("realizedPnl", 0) or 0)
                price = float(o.get("avgPrice", 0) or o.get("price", 0) or 0)
                return (pnl, price)
    except Exception as e:
        log.error(f"BingX closed P&L Fehler ({symbol}): {e}")
    return (0.0, 0.0)


# ─── Exchange-Routing ─────────────────────────────────────────

def resolve_exchange(symbol: str) -> tuple:
    """
    Prüft ob Symbol auf Bybit handelbar ist.
    Falls nicht → Fallback auf BingX (symbol in BingX-Format).
    Returns (exchange, exchange_symbol): ("bybit", "SUSHIUSDT") oder ("bingx", "SUSHI-USDT")
    """
    if not BINGX_API_KEY or not BINGX_SECRET_KEY:
        log.info(f"Exchange-Routing: {symbol} → Bybit (kein BingX konfiguriert)")
        return ("bybit", symbol)

    # Bybit instruments-info abrufen
    res = bybit_get("/v5/market/instruments-info", {
        "category": BYBIT_CATEGORY, "symbol": symbol,
    })
    items = (res.get("result") or {}).get("list") or []
    for item in items:
        if item.get("symbol") == symbol and item.get("status") == "Trading":
            log.info(f"Exchange-Routing: {symbol} → Bybit ✓")
            return ("bybit", symbol)

    # Nicht auf Bybit → BingX
    bx_sym = to_bingx_symbol(symbol)
    log.info(f"Exchange-Routing: {symbol} nicht auf Bybit → BingX ({bx_sym})")
    return ("bingx", bx_sym)


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
    r"Kurs auf\s+([\d.]+)",
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


def place_tp_order(symbol: str, side: str, qty: float, price: float) -> dict:
    """Platziert eine Reduce-Only Limit-Order als Teil-TP (Modell 3)."""
    close_side = "Sell" if side == "LONG" else "Buy"
    res = bybit_post("/v5/order/create", {
        "category":       BYBIT_CATEGORY,
        "symbol":         symbol,
        "side":           close_side,
        "orderType":      "Limit",
        "qty":            fmt_qty(symbol, qty),
        "price":          fmt_price(symbol, price),
        "timeInForce":    "GTC",
        "reduceOnly":     True,
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


def _current_profit_pct(side: str, entry: float, current_price: float, lev: int) -> float:
    """
    Berechnet den aktuellen unrealisierten Profit % (konsistent mit SYNORA_MAX_GAIN_MODE).
    roi-Mode:   ROI auf Margin = Preisbewegung% × Hebel
    price-Mode: direkte Preisbewegung %
    """
    if side == "LONG":
        price_move_pct = (current_price - entry) / entry * 100
    else:
        price_move_pct = (entry - current_price) / entry * 100
    if SYNORA_MAX_GAIN_MODE == "roi":
        return price_move_pct * lev
    return price_move_pct


async def _check_dca_fills(symbol: str, trade: dict, pos_size: float, avg_price: float) -> None:
    """
    Erkennt gefüllte DCA-Limit-Orders anhand der tatsächlichen Positionsgrösse.
    Sendet Benachrichtigung ins Coin-Topic wenn DCA1 oder DCA2 gefüllt wurde.
    """
    side      = trade.get("side", "LONG")
    lev       = int(trade.get("lev", 1))
    exchange  = trade.get("exchange", "bybit")
    ex_sym    = trade.get("exchange_symbol", symbol)
    qty_entry = float(trade.get("qty_entry", 0))
    qty_dca1  = float(trade.get("qty_dca1",  0))
    qty_dca2  = float(trade.get("qty_dca2",  0))
    sl        = float(trade.get("sl", 0))
    notified  = set(trade.get("dca_fills_notified", []))

    # Wenn DCAs bereits storniert wurden, kein Check mehr nötig
    if trade.get("dca_cancelled"):
        return

    def _avg_str() -> str:
        if avg_price > 0:
            return fmt_price(symbol, avg_price) if exchange == "bybit" else bingx_fmt_price(ex_sym, avg_price)
        return "—"

    def _sl_dist() -> str:
        if avg_price > 0 and sl > 0:
            return f" ({abs(sl - avg_price) / avg_price * 100:.2f}%)"
        return ""

    changed = False

    # ── DCA1 fill prüfen ─────────────────────────────────────────
    if "dca1" not in notified and qty_dca1 > 0:
        if pos_size >= qty_entry + qty_dca1 * 0.7:
            tg_coin(symbol,
                f"🔄 <b>SYNORA DCA1 gefüllt</b>\n"
                f"<b>{symbol}</b> {side} {lev}x\n"
                f"Neuer Avg: {_avg_str()}\n"
                f"SL (unverändert): {sl:.6g}{_sl_dist()}"
            )
            _state["trades"][symbol].setdefault("dca_fills_notified", []).append("dca1")
            notified.add("dca1")
            changed = True

    # ── DCA2 fill prüfen ─────────────────────────────────────────
    if "dca2" not in notified and qty_dca2 > 0:
        if pos_size >= qty_entry + qty_dca1 + qty_dca2 * 0.7:
            tg_coin(symbol,
                f"🔄 <b>SYNORA DCA2 gefüllt</b>\n"
                f"<b>{symbol}</b> {side} {lev}x\n"
                f"Neuer Avg: {_avg_str()}\n"
                f"SL (unverändert): {sl:.6g}{_sl_dist()}"
            )
            _state["trades"][symbol].setdefault("dca_fills_notified", []).append("dca2")
            changed = True

    if changed:
        save_state()


async def _verify_sl(symbol: str, trade: dict) -> None:
    """
    Liest den tatsächlich auf der Exchange gesetzten SL zurück.
    Falls er fehlt oder um mehr als 0.1% vom State abweicht → korrigiert ihn.
    Wird bei jedem Polling-Zyklus aufgerufen solange Position offen ist.
    """
    exchange = trade.get("exchange", "bybit")
    ex_sym   = trade.get("exchange_symbol", symbol)
    side     = trade["side"]
    sl_state = float(trade.get("sl", 0))
    if sl_state <= 0:
        return  # kein SL im State → nichts zu prüfen

    try:
        if exchange == "bybit":
            res   = bybit_get("/v5/position/list", {"category": BYBIT_CATEGORY, "symbol": symbol})
            items = (res.get("result") or {}).get("list") or []
            sl_live = 0.0
            for item in items:
                if item.get("symbol") == symbol:
                    sl_live = float(item.get("stopLoss", "0") or "0")
                    break
        else:
            res   = bingx_get("/openApi/swap/v2/user/positions", {"symbol": ex_sym})
            data  = res.get("data") or []
            sl_live = 0.0
            for pos in data:
                if pos.get("symbol") == ex_sym and pos.get("positionSide") == side:
                    sl_live = float(pos.get("stopLoss", 0) or 0)
                    break
    except Exception as e:
        log.warning(f"SL-Monitor: Fehler beim Lesen ({symbol}): {e}")
        return

    # SL fehlt oder weicht mehr als 0.1% ab
    if sl_live <= 0 or abs(sl_live - sl_state) / sl_state > 0.001:
        log.warning(f"SL-Monitor {symbol}: live={sl_live} ≠ state={sl_state} → korrigiere")
        if exchange == "bybit":
            res_fix = set_sl(symbol, side, sl_state)
            ok_fix  = bybit_ok(res_fix)
        else:
            res_fix = bingx_set_sl(ex_sym, side, sl_state)
            ok_fix  = bingx_ok(res_fix)

        if ok_fix:
            log.info(f"SL-Monitor {symbol}: SL auf {sl_state} korrigiert ✓")
            if sl_live <= 0:
                tg_coin(symbol,
                    f"🚨 <b>SYNORA: SL fehlte!</b>\n"
                    f"{symbol} {side} — SL war nicht gesetzt!\n"
                    f"Automatisch korrigiert auf {sl_state}"
                )
        else:
            log.error(f"SL-Monitor {symbol}: Korrektur fehlgeschlagen! {res_fix}")
            tg_coin(symbol,
                f"🚨 <b>SYNORA: SL-Korrektur FEHLGESCHLAGEN</b>\n"
                f"{symbol} — SL={sl_state} nicht gesetzt!\n"
                f"Bitte manuell setzen!"
            )


def _sl_is_at_breakeven_or_better(side: str, entry: float, sl: float) -> bool:
    """
    Prüft ob SL auf Break-Even oder Profit-Sicherung liegt.
    LONG:  SL ≥ entry × 0.999  →  Break-Even oder besser
    SHORT: SL ≤ entry × 1.001  →  Break-Even oder besser
    """
    if entry <= 0 or sl <= 0:
        return False
    if side == "LONG":
        return sl >= entry * 0.999
    else:
        return sl <= entry * 1.001


async def _cancel_dca_if_sl_at_be(symbol: str, trade: dict) -> None:
    """
    Storniert noch offene DCA-Limit-Orders wenn:
      • SL wurde auf Break-Even oder Profit-Sicherung bewegt, ODER
      • Level 10 des Dynamic-Models wurde ausgeführt (SL → BE).
    Wird bei jedem Polling-Zyklus aufgerufen.
    """
    side     = trade["side"]
    entry    = float(trade.get("entry", 0))
    sl_state = float(trade.get("sl", 0))
    exchange = trade.get("exchange", "bybit")
    ex_sym   = trade.get("exchange_symbol", symbol)
    levels_done = list(trade.get("dynamic_levels_done", []))

    level_10_done = 10 in levels_done
    sl_at_be      = _sl_is_at_breakeven_or_better(side, entry, sl_state)

    if not (level_10_done or sl_at_be):
        return  # DCAs dürfen noch offen sein

    # Offene Limit-Orders prüfen (nur canceln wenn wirklich welche vorhanden)
    try:
        if exchange == "bybit":
            ord_res   = bybit_get("/v5/order/realtime", {"category": BYBIT_CATEGORY, "symbol": symbol})
            ord_items = (ord_res.get("result") or {}).get("list") or []
            dca_open  = [o for o in ord_items if o.get("orderType") == "Limit"]
        else:
            ord_res  = bingx_get("/openApi/swap/v2/trade/openOrders", {"symbol": ex_sym})
            dca_open = ((ord_res.get("data") or {}).get("orders") or [])
    except Exception as e:
        log.warning(f"DCA-Cancel-Check Fehler ({symbol}): {e}")
        return

    if not dca_open:
        return

    reason = "Level 10 done" if level_10_done else f"SL@BE ({sl_state:.6g})"
    log.info(f"DCA-Cancel: storniere {len(dca_open)} Order(s) für {symbol} — {reason}")
    if exchange == "bybit":
        cancel_open_orders(symbol)
    else:
        bingx_cancel_open_orders(ex_sym)
    tg_coin(symbol,
        f"ℹ️ <b>SYNORA: DCA-Orders storniert</b>\n"
        f"<b>{symbol}</b> — {reason}\n"
        f"{len(dca_open)} DCA-Limit-Order(s) waren noch offen — automatisch storniert."
    )


async def reconcile_trade_state(startup: bool = False) -> None:
    """
    Vollständige State-Überprüfung aller offenen Trades gegen die Exchange.
    Korrigiert Abweichungen automatisch und sendet bei startup=True einen
    Telegram-Bericht mit dem vollständigen Status jedes Trades.

    Prüft + korrigiert:
      1. Position existiert noch  → sonst State bereinigen
      2. qty_total_initial        → initialisieren falls fehlend
      3. dynamic_levels_done      → initialisieren falls fehlend
      4. SL gesetzt + korrekt     → korrigieren falls abweichend (nur startup)
      5. DCA-Orders               → stornieren falls SL auf/über Break-Even

    Aufruf:
      startup=True  → einmalig aus main() nach client.start()
      startup=False → aus check_closed_positions() (leichtgewichtig, via _cancel_dca_if_sl_at_be)
    """
    trades_snapshot = dict(_state.get("trades", {}))
    if not trades_snapshot:
        if startup:
            tg_status("🟣 <b>SYNORA Reconcile</b>: Keine offenen Trades — bereit ✓")
        return

    fixes        = []   # gesammelte Korrekturen für Startup-Bericht
    removed      = []   # aus State bereinigt
    status_lines = []

    for symbol, trade in trades_snapshot.items():
        exchange   = trade.get("exchange", "bybit")
        ex_sym     = trade.get("exchange_symbol", symbol)
        side       = trade["side"]
        lev        = int(trade.get("lev", 1))

        # ── 0. Forum-Topic erstellen falls Trade vor Forum-Feature geöffnet ──
        if _FORUM_ENABLED:
            existing_tid = (_state.get("telegram_topics") or {}).get(symbol, {}).get("thread_id")
            if not existing_tid:
                new_tid = _open_coin_topic(symbol, side)
                if new_tid:
                    fixes.append(f"{symbol}: Forum-Topic nachträglich erstellt (thread_id={new_tid})")
                    log.info(f"Reconcile: Forum-Topic für {symbol} erstellt (thread_id={new_tid})")
        entry      = float(trade.get("entry", 0))
        sl_state   = float(trade.get("sl", 0))
        dyn_active = trade.get("dynamic_model_active", False)
        dyn_done   = list(trade.get("dynamic_levels_done", []))

        pos_size     = -1.0
        sl_live      = 0.0
        avg_entry_ex = 0.0

        # ── 1. Position-Existenz prüfen (nur Startup) ────────────
        if startup:
            try:
                if exchange == "bybit":
                    res   = bybit_get("/v5/position/list", {"category": BYBIT_CATEGORY, "symbol": symbol})
                    items = (res.get("result") or {}).get("list") or []
                    pos_size = 0.0
                    for item in items:
                        if item.get("symbol") == symbol:
                            pos_size     = float(item.get("size",      "0") or "0")
                            sl_live      = float(item.get("stopLoss",  "0") or "0")
                            avg_entry_ex = float(item.get("avgPrice",  "0") or "0")
                            break
                else:
                    res  = bingx_get("/openApi/swap/v2/user/positions", {"symbol": ex_sym})
                    data = res.get("data") or []
                    pos_size = 0.0
                    for pos in data:
                        if pos.get("symbol") == ex_sym and pos.get("positionSide") == side:
                            pos_size     = abs(float(pos.get("positionAmt", 0) or 0))
                            sl_live      = float(pos.get("stopLoss", 0) or 0)
                            avg_entry_ex = float(pos.get("avgPrice",  0) or 0)
                            break
            except Exception as e:
                log.error(f"Reconcile: Position-Abfrage Fehler ({symbol}): {e}")
                pos_size = -1.0  # unbekannt

            if pos_size == 0.0:
                log.warning(f"Reconcile STARTUP: {symbol} hat keine Position! State bereinigt.")
                _state["trades"].pop(symbol, None)
                save_state()
                removed.append(symbol)
                continue

        # ── 2. qty_total_initial initialisieren ──────────────────
        if "qty_total_initial" not in trade or float(trade.get("qty_total_initial", 0)) <= 0:
            qty_init = (float(trade.get("qty_entry", 0)) +
                        float(trade.get("qty_dca1",  0)) +
                        float(trade.get("qty_dca2",  0)))
            if qty_init <= 0 and pos_size > 0:
                qty_init = pos_size   # Fallback: aktuelle Pos-Grösse
            if qty_init > 0:
                _state["trades"][symbol]["qty_total_initial"] = qty_init
                fixes.append(f"{symbol}: qty_total_initial={qty_init:.4f} gesetzt")
                log.info(f"Reconcile: qty_total_initial={qty_init} für {symbol} initialisiert")

        # ── 3. dynamic_levels_done initialisieren ─────────────────
        if "dynamic_levels_done" not in trade:
            _state["trades"][symbol]["dynamic_levels_done"] = []
            fixes.append(f"{symbol}: dynamic_levels_done initialisiert")

        # ── 4. SL prüfen + korrigieren (nur Startup) ─────────────
        if startup and sl_state > 0:
            sl_ok = sl_live > 0 and abs(sl_live - sl_state) / sl_state <= 0.001
            if not sl_ok:
                log.warning(f"Reconcile STARTUP SL: {symbol} live={sl_live} state={sl_state} → korrigiere")
                if exchange == "bybit":
                    res_sl   = set_sl(symbol, side, sl_state)
                    sl_fixed = bybit_ok(res_sl)
                else:
                    res_sl   = bingx_set_sl(ex_sym, side, sl_state)
                    sl_fixed = bingx_ok(res_sl)
                if sl_fixed:
                    fixes.append(f"{symbol}: SL {sl_live:.6g} → {sl_state:.6g} korrigiert ✓")
                    log.info(f"Reconcile SL fix {symbol}: {sl_live} → {sl_state} ✓")
                else:
                    fixes.append(f"{symbol}: ⚠️ SL-Korrektur FEHLGESCHLAGEN! ({sl_live:.6g} → {sl_state:.6g})")
                    log.error(f"Reconcile SL-Fehler {symbol}: {res_sl}")

        # ── 5. DCA-Orders stornieren falls SL auf Break-Even ──────
        level_10_done = 10 in dyn_done
        sl_at_be      = _sl_is_at_breakeven_or_better(side, entry, sl_state)

        if level_10_done or sl_at_be:
            try:
                if exchange == "bybit":
                    ord_res   = bybit_get("/v5/order/realtime", {"category": BYBIT_CATEGORY, "symbol": symbol})
                    ord_items = (ord_res.get("result") or {}).get("list") or []
                    dca_open  = [o for o in ord_items if o.get("orderType") == "Limit"]
                else:
                    ord_res  = bingx_get("/openApi/swap/v2/trade/openOrders", {"symbol": ex_sym})
                    dca_open = ((ord_res.get("data") or {}).get("orders") or [])
            except Exception as e:
                log.warning(f"Reconcile: Order-Check Fehler ({symbol}): {e}")
                dca_open = []

            if dca_open:
                reason = "Level 10 done" if level_10_done else f"SL@BE ({sl_state:.6g})"
                if exchange == "bybit":
                    cancel_open_orders(symbol)
                else:
                    bingx_cancel_open_orders(ex_sym)
                _state["trades"][symbol]["dca_cancelled"] = True
                fixes.append(f"{symbol}: {len(dca_open)} DCA-Order(s) storniert ({reason})")
                log.info(f"Reconcile: DCA storniert für {symbol} — {reason}")
                tg_coin(symbol,
                    f"ℹ️ <b>SYNORA: DCA-Orders storniert</b>\n"
                    f"<b>{symbol}</b> — {reason}\n"
                    f"{len(dca_open)} Limit-Order(s) waren noch offen!"
                )

        save_state()

        # Status-Zeile für Startup-Bericht
        if startup:
            sl_live_str = f"{sl_live:.6g}" if sl_live > 0 else "⚠️ fehlt!"
            dyn_str     = (f"Modell 3 aktiv ({len(dyn_done)}/6 Stufen)"
                           if dyn_active else "⏳ warte auf SYNORA UPDATE")
            be_flag     = " | 🔒SL@BE" if (sl_at_be or level_10_done) else ""
            pos_str     = f"{pos_size:.4g}" if pos_size >= 0 else "?"
            status_lines.append(
                f"<b>{symbol}</b> {side} {lev}x{be_flag}\n"
                f"  Pos={pos_str} | avgEntry={avg_entry_ex:.6g}\n"
                f"  SL(state)={sl_state:.6g} | SL(live)={sl_live_str}\n"
                f"  {dyn_str}"
            )

    if not startup:
        return

    # ── Startup-Telegram-Bericht ────────────────────────────────
    parts = ["🔄 <b>SYNORA Startup-Reconcile</b>\n"]

    if removed:
        parts.append(f"⚠️ <b>Bereinigt</b> (keine Position): {', '.join(removed)}\n")

    if status_lines:
        parts.append("📊 <b>Offene Trades:</b>\n" + "\n\n".join(status_lines))

    if fixes:
        parts.append("\n🔧 <b>Korrekturen:</b>\n" + "\n".join(f"• {f}" for f in fixes))
    elif not removed and _state.get("trades"):
        parts.append("\n✅ Alles in Ordnung — keine Korrekturen nötig")
    elif not _state.get("trades"):
        parts.append("\nKeine offenen Trades nach Reconcile.")

    tg_status("".join(parts))


async def _check_dynamic_profit_levels(symbol: str, trade: dict, pos_size: float) -> None:
    """
    Prüft und führt das Dynamische Profit-Modell 3 (6 Stufen) aus.
    Wird aus check_closed_positions() aufgerufen wenn Position noch offen ist.
    """
    if not trade.get("dynamic_model_active"):
        return

    exchange = trade.get("exchange", "bybit")
    ex_sym   = trade.get("exchange_symbol", symbol)
    side     = trade["side"]
    lev      = int(trade.get("lev", 1))
    entry    = float(trade["entry"])

    # Aktuellen Mark-Preis holen
    if exchange == "bybit":
        current_price = get_mark_price(symbol)
    else:
        current_price = bingx_get_mark_price(ex_sym)

    if current_price <= 0:
        log.warning(f"Dynamic: kein Mark-Preis für {symbol} ({exchange})")
        return

    current_pct   = _current_profit_pct(side, entry, current_price, lev)
    levels_done   = list(trade.get("dynamic_levels_done", []))
    initial_qty   = float(trade.get("qty_total_initial", pos_size))
    remaining_qty = pos_size   # wird nach jedem Close aktualisiert

    for profit_pct, payout_pct, sl_offset in DYNAMIC_MODEL_LEVELS:
        if profit_pct in levels_done:
            continue
        if current_pct < profit_pct:
            break   # noch nicht erreicht (Liste ist aufsteigend sortiert)

        # Stufe ist neu erreicht
        qty_to_close = snap_qty(symbol, initial_qty * (payout_pct / 100)) \
                       if exchange == "bybit" \
                       else bingx_snap_qty(ex_sym, initial_qty * (payout_pct / 100))
        qty_to_close = min(qty_to_close, remaining_qty)   # nicht mehr als vorhanden

        if qty_to_close <= 0:
            log.warning(f"Dynamic Level +{profit_pct}%: qty_to_close=0 für {symbol} — übersprungen")
            levels_done.append(profit_pct)
            continue

        qty_fmt = fmt_qty(symbol, qty_to_close) if exchange == "bybit" \
                  else bingx_fmt_qty(ex_sym, qty_to_close)
        log.info(f"Dynamic Level +{profit_pct}%: schliesse {payout_pct}% "
                 f"({qty_fmt}) von {symbol} | Preis={current_price} Profit={current_pct:.1f}%")

        # Partial Close
        if exchange == "bybit":
            res = close_position_market(symbol, side, qty_to_close)
            ok  = bybit_ok(res)
            err = res.get("retMsg", "?")
        else:
            res = bingx_close_position_market(ex_sym, side, qty_to_close)
            ok  = bingx_ok(res)
            err = res.get("msg", "?")

        if not ok:
            log.error(f"Dynamic Partial Close Fehler Level +{profit_pct}%: {err}")
            tg_system(f"⚠️ SYNORA Dynamic: Partial-Close Fehler bei +{profit_pct}% {symbol}: {err}")
            continue   # nächste Stufe trotzdem prüfen

        remaining_qty -= qty_to_close
        levels_done.append(profit_pct)
        _state["trades"][symbol]["dynamic_levels_done"] = levels_done
        log.info(f"Dynamic Level +{profit_pct}% ausgeführt ✓ — verbleibend: {remaining_qty:.4f}")

        # DCA-Orders stornieren sobald SL auf Break-Even geht (erste Stufe +10%)
        # → offene DCAs würden Position bei ungünstiger Rückbewegung vergrössern
        if sl_offset == 0 and profit_pct == 10:
            log.info(f"Dynamic: storniere offene DCA-Orders für {symbol} (SL → Break-Even)")
            if exchange == "bybit":
                cancel_open_orders(symbol)
            else:
                bingx_cancel_open_orders(ex_sym)
            _state["trades"][symbol]["dca_cancelled"] = True
            save_state()

        # SL-Anpassung
        sl_msg = ""
        if sl_offset is not None:
            new_sl = calc_tp_price(side, entry, sl_offset, lev)  # sl_offset=0 → Break-Even
            if exchange == "bybit":
                res_sl = set_sl(symbol, side, new_sl)
                sl_ok  = bybit_ok(res_sl)
            else:
                res_sl = bingx_set_sl(ex_sym, side, new_sl)
                sl_ok  = bingx_ok(res_sl)
            if sl_ok:
                _state["trades"][symbol]["sl"] = new_sl
                label = "Break-Even" if sl_offset == 0 else f"+{sl_offset}%"
                sl_msg = f"\nSL → {label} ({fmt_price(symbol, new_sl) if exchange == 'bybit' else bingx_fmt_price(ex_sym, new_sl)})"
                log.info(f"Dynamic SL aktualisiert: {label} = {new_sl:.6f}")
            else:
                sl_msg = f"\n⚠️ SL-Update fehlgeschlagen (+{sl_offset}%)"
                log.warning(f"Dynamic SL-Fehler Level +{profit_pct}%: {res_sl}")

        save_state()

        tg_coin(
            symbol,
            f"📈 <b>SYNORA Partial Close</b> [Stufe +{profit_pct}%]\n"
            f"Symbol: <b>{symbol}</b> {side} {lev}X\n"
            f"Geschlossen: {payout_pct}% ({qty_fmt})\n"
            f"Aktuell: {current_pct:.1f}% Profit{sl_msg}"
        )

        if remaining_qty <= 0:
            break   # Position vollständig geschlossen


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
        tg_system(f"⚠️ SYNORA: {symbol} bereits offen — Signal ignoriert")
        log.warning(f"Trade für {symbol} bereits im State, übersprungen")
        return

    # 1. Exchange-Routing: Bybit bevorzugt, Fallback BingX
    exchange, ex_sym = resolve_exchange(symbol)
    exchange_label   = "Bybit" if exchange == "bybit" else "BingX"

    # 2. Precision laden + Hebel setzen
    if exchange == "bybit":
        _load_instrument_info(symbol)
        if not set_leverage(symbol, lev):
            tg_system(f"❌ SYNORA: Hebel-Fehler für {symbol} (Bybit) — Trade abgebrochen")
            return
        mark     = get_mark_price(symbol)
        budget   = get_available_balance()
    else:
        _load_bingx_instrument_info(ex_sym)
        if not bingx_set_leverage(ex_sym, side, lev):
            tg_system(f"❌ SYNORA: Hebel-Fehler für {ex_sym} (BingX) — Trade abgebrochen")
            return
        mark     = bingx_get_mark_price(ex_sym)
        budget   = get_bingx_balance()

    ref_price = mark if mark > 0 else entry
    log.info(f"Mark-Preis ({exchange_label}): {ref_price} | Budget: {budget:.2f} USDT")

    # 2b. Signal-Preis Toleranz prüfen (max. 3% Abweichung gemäss Synora-Strategie)
    if mark > 0:
        deviation_pct = abs(ref_price - entry) / entry * 100
        tolerance_exceeded = (side == "LONG" and ref_price > entry * 1.03) or \
                             (side == "SHORT" and ref_price < entry * 0.97)
        if tolerance_exceeded:
            tg_system(
                f"⚠️ <b>SYNORA: Toleranz überschritten — {symbol}</b>\n"
                f"Signal-Preis: {entry} | Mark jetzt: {ref_price:.6g}\n"
                f"Abweichung: {deviation_pct:.1f}% (Max. 3%) — Trade abgebrochen"
            )
            log.warning(f"Toleranz {deviation_pct:.1f}% > 3% für {symbol} — Trade abgebrochen")
            return
        log.info(f"Toleranz-Check OK: {deviation_pct:.1f}% Abweichung vom Signal-Preis")

    if budget < 5.0:
        tg_system(f"❌ SYNORA: Balance zu gering ({budget:.2f} USDT) — Trade abgebrochen")
        return

    # 3. Qty berechnen — Entry + DCA1 aus aktuellem Budget, DCA2 live nach DCA1
    if exchange == "bybit":
        qty_entry = calc_qty(symbol, budget * SPLIT_ENTRY, lev, ref_price)
        qty_dca1  = calc_qty(symbol, budget * SPLIT_DCA1,  lev, dca1)
        qty_dca2  = calc_qty(symbol, budget * SPLIT_DCA2,  lev, dca2)
    else:
        qty_entry = bingx_calc_qty(ex_sym, budget * SPLIT_ENTRY, lev, ref_price)
        qty_dca1  = bingx_calc_qty(ex_sym, budget * SPLIT_DCA1,  lev, dca1)
        qty_dca2  = bingx_calc_qty(ex_sym, budget * SPLIT_DCA2,  lev, dca2)

    if qty_entry <= 0:
        tg_system(f"❌ SYNORA: Qty=0 für {symbol} (Budget zu klein?) — Trade abgebrochen")
        return

    # 4. Market Entry
    if exchange == "bybit":
        res_entry = place_market_order(symbol, side, qty_entry)
        ok_entry  = bybit_ok(res_entry)
        err_entry = res_entry.get("retMsg", "unbekannt")
        entry_id  = (res_entry.get("result") or {}).get("orderId", "?")
    else:
        res_entry = bingx_place_market_order(ex_sym, side, qty_entry)
        ok_entry  = bingx_ok(res_entry)
        err_entry = res_entry.get("msg", "unbekannt")
        entry_id  = (res_entry.get("data") or {}).get("orderId", "?")

    if not ok_entry:
        tg_system(f"❌ SYNORA: Market Order Fehler {symbol} ({exchange_label}): {err_entry}")
        log.error(f"Market Order Fehler: {res_entry}")
        return
    log.info(f"Market Entry platziert ({exchange_label}): OrderId={entry_id}")

    # 5. Kurz warten damit Position existiert
    await asyncio.sleep(2)

    # 6. Stop-Loss setzen (mit Retry)
    sl_set = False
    for attempt in range(3):
        if attempt > 0:
            await asyncio.sleep(2)
        if exchange == "bybit":
            res_sl = set_sl(symbol, side, sl)
            sl_set = bybit_ok(res_sl)
        else:
            res_sl = bingx_set_sl(ex_sym, side, sl)
            sl_set = bingx_ok(res_sl)
        if sl_set:
            log.info(f"SL gesetzt: {sl} (Versuch {attempt+1})")
            break
        log.warning(f"SL-Fehler Versuch {attempt+1}/3: {res_sl}")
    if not sl_set:
        tg_system(f"🚨 <b>SYNORA: SL NICHT GESETZT</b> für {symbol}!\nSL={sl} | Manuell setzen!")

    # 7. DCA Limit Orders
    dca1_id = dca2_id = None
    if exchange == "bybit":
        if qty_dca1 > 0:
            res_dca1 = place_limit_order(symbol, side, qty_dca1, dca1)
            if bybit_ok(res_dca1):
                dca1_id = (res_dca1.get("result") or {}).get("orderId")
                log.info(f"DCA1 Limit {dca1} × {fmt_qty(symbol, qty_dca1)} platziert")
            else:
                log.warning(f"DCA1 Fehler: {res_dca1}")
        if qty_dca2 > 0:
            # Balance nach Entry+DCA1 neu lesen damit DCA2 nicht wegen Margin-Reserve scheitert
            _balance_cache["ts"] = 0
            budget_remaining = get_available_balance()
            qty_dca2 = calc_qty(symbol, budget_remaining, lev, dca2)
            log.info(f"DCA2: verbleibender Balance={budget_remaining:.2f} USDT → qty={fmt_qty(symbol, qty_dca2)}")
            if qty_dca2 > 0:
                res_dca2 = place_limit_order(symbol, side, qty_dca2, dca2)
                if bybit_ok(res_dca2):
                    dca2_id = (res_dca2.get("result") or {}).get("orderId")
                    log.info(f"DCA2 Limit {dca2} × {fmt_qty(symbol, qty_dca2)} platziert")
                else:
                    log.warning(f"DCA2 Fehler: {res_dca2}")
    else:
        if qty_dca1 > 0:
            res_dca1 = bingx_place_limit_order(ex_sym, side, qty_dca1, dca1)
            if bingx_ok(res_dca1):
                dca1_id = (res_dca1.get("data") or {}).get("orderId")
                log.info(f"BingX DCA1 Limit {dca1} × {bingx_fmt_qty(ex_sym, qty_dca1)} platziert")
            else:
                log.warning(f"BingX DCA1 Fehler: {res_dca1}")
        if qty_dca2 > 0:
            _bingx_balance_cache["ts"] = 0
            budget_remaining = get_bingx_balance()
            qty_dca2 = bingx_calc_qty(ex_sym, budget_remaining, lev, dca2)
            log.info(f"BingX DCA2: verbleibender Balance={budget_remaining:.2f} USDT → qty={bingx_fmt_qty(ex_sym, qty_dca2)}")
            if qty_dca2 > 0:
                res_dca2 = bingx_place_limit_order(ex_sym, side, qty_dca2, dca2)
                if bingx_ok(res_dca2):
                    dca2_id = (res_dca2.get("data") or {}).get("orderId")
                    log.info(f"BingX DCA2 Limit {dca2} × {bingx_fmt_qty(ex_sym, qty_dca2)} platziert")
                else:
                    log.warning(f"BingX DCA2 Fehler: {res_dca2}")

    # 8. State speichern (inkl. Exchange-Info für alle Folge-Aktionen)
    _state.setdefault("trades", {})[symbol] = {
        "side":               side,
        "lev":                lev,
        "entry":              entry,
        "dca1":               dca1,
        "dca2":               dca2,
        "sl":                 sl,
        "qty_entry":          qty_entry,
        "qty_dca1":           qty_dca1,
        "qty_dca2":           qty_dca2,
        "qty_total_initial":  qty_entry + qty_dca1 + qty_dca2,  # Basis für Dynamic-Model
        "dca1_order_id":      dca1_id,
        "dca2_order_id":      dca2_id,
        "tp":                 None,
        "opened_at":          datetime.now(timezone.utc).isoformat(),
        "budget_usdt":        budget,
        "exchange":           exchange,        # "bybit" oder "bingx"
        "exchange_symbol":    ex_sym,          # z.B. "SUSHI-USDT" für BingX
        "dynamic_model_active": False,         # wird via UPDATE aktiviert
        "dynamic_levels_done":  [],            # bereits ausgeführte Stufen
    }
    # 10. Profit-Modell 3 sofort aktivieren (kein UPDATE nötig)
    if SYNORA_PROFIT_MODEL == 3:
        _state["trades"][symbol]["dynamic_model_active"] = True

    save_state()
    _open_coin_topic(symbol, side)   # Forum-Topic für diesen Coin erstellen/öffnen

    # 9. Telegram-Benachrichtigung
    sl_dist_pct  = abs(sl - entry) / entry * 100
    qty_dca1_fmt = fmt_qty(symbol, qty_dca1) if exchange == "bybit" else bingx_fmt_qty(ex_sym, qty_dca1)
    qty_dca2_fmt = fmt_qty(symbol, qty_dca2) if exchange == "bybit" else bingx_fmt_qty(ex_sym, qty_dca2)

    # Basis-Meldung
    tg_coin(
        symbol,
        f"🟣 <b>SYNORA Trade eröffnet</b> [{exchange_label}]\n"
        f"Symbol: <b>{symbol}</b> {side} {lev}X\n"
        f"Entry (Market): {entry}\n"
        f"SL: {sl} ({sl_dist_pct:.2f}%)\n"
        f"DCA1: {dca1} × {qty_dca1_fmt} ({int(SPLIT_DCA1*100)}%)\n"
        f"DCA2: {dca2} × {qty_dca2_fmt} ({int(SPLIT_DCA2*100)}%)\n"
        f"Budget: {budget:.2f} USDT (live vom Sub-Account)"
    )

    # Profit-Modell 3: Level-Tabelle sofort anzeigen
    if SYNORA_PROFIT_MODEL == 3:
        level_lines = []
        for pct, payout, sl_off in DYNAMIC_MODEL_LEVELS:
            tp_p     = calc_tp_price(side, entry, pct, lev)
            sl_lbl   = f"SL→+{sl_off}%" if sl_off else ("SL→BE" if sl_off == 0 else "—")
            price_fmt = fmt_price(symbol, tp_p) if exchange == "bybit" else bingx_fmt_price(ex_sym, tp_p)
            level_lines.append(f"  +{pct:2d}% → {price_fmt} | {payout}% | {sl_lbl}")
        tg_coin(
            symbol,
            f"📊 <b>Profit-Modell 3 aktiv</b>\n"
            f"Mode: {SYNORA_MAX_GAIN_MODE} | Basis: {entry}\n"
            f"<pre>" + "\n".join(level_lines) + "</pre>"
        )
        log.info(f"Profit-Modell 3 sofort aktiviert für {symbol}")


async def handle_update(upd: dict) -> None:
    """Verarbeitet eine SYNORA UPDATE Nachricht — setzt TP."""
    symbol   = upd["symbol"]
    max_gain = upd["max_gain_pct"]

    trade = _state.get("trades", {}).get(symbol)
    if not trade:
        log.info(f"UPDATE für {symbol} — kein offener Trade im State")
        return

    side     = trade["side"]
    lev      = trade["lev"]
    entry    = trade["entry"]
    exchange = trade.get("exchange", "bybit")
    ex_sym   = trade.get("exchange_symbol", symbol)

    tp_price = calc_tp_price(side, entry, max_gain, lev)
    log.info(f"UPDATE {symbol} ({exchange}): max_gain={max_gain}% → TP={tp_price:.6f}")

    # ── Dynamisches Profit-Modell 3 ──────────────────────────────
    if SYNORA_PROFIT_MODEL == 3:
        _state["trades"][symbol]["dynamic_model_active"] = True
        _state["trades"][symbol].setdefault("dynamic_levels_done", [])
        _state["trades"][symbol]["tp"] = tp_price   # als Info-Referenz
        save_state()

        # Aufschlüsselung der 6 Stufen für Telegram
        level_lines = []
        for pct, payout, sl_off in DYNAMIC_MODEL_LEVELS:
            p = calc_tp_price(side, entry, pct, lev)
            sl_lbl = f"SL→+{sl_off}%" if sl_off else ("SL→BE" if sl_off == 0 else "—")
            price_fmt = fmt_price(symbol, p) if exchange == "bybit" else bingx_fmt_price(ex_sym, p)
            level_lines.append(f"  +{pct:2d}% → {price_fmt} | {payout}% | {sl_lbl}")

        tg_coin(
            symbol,
            f"📊 <b>SYNORA Dynamisches Profit-Modell 3 aktiviert</b>\n"
            f"Symbol: <b>{symbol}</b> | Mode: {SYNORA_MAX_GAIN_MODE}\n"
            f"Max. Ziel: {max_gain}% → {fmt_price(symbol, tp_price) if exchange == 'bybit' else bingx_fmt_price(ex_sym, tp_price)}\n\n"
            f"<pre>" + "\n".join(level_lines) + "</pre>"
        )
        log.info(f"Dynamisches Profit-Modell 3 aktiviert für {symbol}")
        return

    # ── Einfacher TP (Modell 1) ───────────────────────────────────
    if exchange == "bybit":
        res = set_tp(symbol, side, tp_price)
        ok  = bybit_ok(res)
        err = res.get("retMsg", "unbekannt")
        tp_str = fmt_price(symbol, tp_price)
    else:
        res = bingx_set_tp(ex_sym, side, tp_price)
        ok  = bingx_ok(res)
        err = res.get("msg", "unbekannt")
        tp_str = bingx_fmt_price(ex_sym, tp_price)

    if ok:
        _state["trades"][symbol]["tp"] = tp_price
        save_state()
        tg_coin(
            symbol,
            f"🎯 <b>SYNORA TP gesetzt</b>\n"
            f"Symbol: <b>{symbol}</b>\n"
            f"Maximaler Gewinn: {max_gain}%\n"
            f"TP-Preis: {tp_str}\n"
            f"(Mode: {SYNORA_MAX_GAIN_MODE})"
        )
    else:
        tg_system(f"⚠️ SYNORA: TP-Fehler {symbol}: {err}")
        log.warning(f"TP-Fehler: {res}")


async def handle_close(symbol: str, reason: str = "CANCEL") -> None:
    """Schließt eine Synora-Position und storniert offene Orders."""
    trade = _state.get("trades", {}).get(symbol)
    if not trade:
        log.info(f"CLOSE für {symbol} — kein offener Trade")
        return

    exchange = trade.get("exchange", "bybit")
    ex_sym   = trade.get("exchange_symbol", symbol)

    # Offene Limit-Orders stornieren
    if exchange == "bybit":
        cancel_open_orders(symbol)
    else:
        bingx_cancel_open_orders(ex_sym)
    await asyncio.sleep(1)

    # Echte Position-Grösse holen (keine theoretische Summe — DCAs evtl. nicht gefüllt)
    actual_qty = 0.0
    try:
        if exchange == "bybit":
            res_pos = bybit_get("/v5/position/list", {"category": BYBIT_CATEGORY, "symbol": symbol})
            for item in (res_pos.get("result") or {}).get("list") or []:
                if item.get("symbol") == symbol:
                    actual_qty = float(item.get("size", "0") or "0")
                    break
        else:
            actual_qty = bingx_get_position_size(ex_sym, trade["side"])
    except Exception as e:
        log.error(f"Position-Abfrage für Close fehlgeschlagen ({symbol}): {e}")

    if actual_qty <= 0:
        log.info(f"CLOSE {symbol}: Position bereits geschlossen (size=0)")
        del _state["trades"][symbol]
        save_state()
        tg_coin(symbol, f"ℹ️ SYNORA: {symbol} war bereits geschlossen ({reason})")
        _close_coin_topic(symbol)
        return

    log.info(f"CLOSE {symbol} ({exchange}): schliesse {actual_qty} Kontrakte")

    if exchange == "bybit":
        res = close_position_market(symbol, trade["side"], actual_qty)
        ok  = bybit_ok(res)
        err = res.get("retMsg", "unbekannt")
    else:
        res = bingx_close_position_market(ex_sym, trade["side"], actual_qty)
        ok  = bingx_ok(res)
        err = res.get("msg", "unbekannt")

    if ok:
        await asyncio.sleep(2)
        if exchange == "bybit":
            pnl_res   = bybit_get("/v5/position/closed-pnl", {
                "category": BYBIT_CATEGORY, "symbol": symbol, "limit": "1",
            })
            pnl_items = (pnl_res.get("result") or {}).get("list") or []
            closed_pnl = close_price = 0.0
            if pnl_items:
                p = pnl_items[0]
                closed_pnl  = float(p.get("closedPnl",    0) or 0)
                close_price = float(p.get("avgExitPrice", 0) or 0)
        else:
            closed_pnl, close_price = bingx_get_closed_pnl(ex_sym)

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
        tg_coin(symbol, f"🔴 <b>SYNORA Position geschlossen</b>\n{symbol} ({reason})\nP&L: {pnl_str} USDT")
        _close_coin_topic(symbol, won=closed_pnl >= 0)
    else:
        tg_system(f"⚠️ SYNORA: Close-Fehler {symbol}: {err}")
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
    """Periodischer Loop: prüft ob Synora-Positionen geschlossen wurden (Bybit oder BingX)."""
    log.info("Position-Polling gestartet (Intervall: %ds)", POSITION_POLL_INTERVAL)
    while True:
        await asyncio.sleep(POSITION_POLL_INTERVAL)
        trades_snapshot = dict(_state.get("trades", {}))
        if not trades_snapshot:
            continue

        for symbol, trade in trades_snapshot.items():
            try:
                exchange = trade.get("exchange", "bybit")
                ex_sym   = trade.get("exchange_symbol", symbol)

                # ── Position-Grösse abfragen ────────────────
                if exchange == "bybit":
                    res = bybit_get("/v5/position/list", {
                        "category": BYBIT_CATEGORY, "symbol": symbol,
                    })
                    items = (res.get("result") or {}).get("list") or []
                    pos_size  = 0.0
                    avg_price = 0.0
                    for item in items:
                        if item.get("symbol") == symbol:
                            pos_size  = float(item.get("size",     "0") or "0")
                            avg_price = float(item.get("avgPrice", "0") or "0")
                            break

                    # Migration: alter State ohne 'exchange'-Feld
                    # → wenn Bybit size=0, auch BingX prüfen bevor wir als "closed" werten
                    if pos_size == 0 and "exchange" not in trade and BINGX_API_KEY:
                        bx_sym   = to_bingx_symbol(symbol)
                        bx_size  = bingx_get_position_size(bx_sym, trade["side"])
                        if bx_size > 0:
                            log.info(f"Migration: {symbol} auf BingX ({bx_sym}) gefunden — State aktualisiert")
                            _state["trades"][symbol]["exchange"]        = "bingx"
                            _state["trades"][symbol]["exchange_symbol"] = bx_sym
                            save_state()
                            continue   # Position noch offen auf BingX
                else:
                    pos_size  = bingx_get_position_size(ex_sym, trade["side"])
                    avg_price = 0.0   # BingX: avg_price nicht im size-Call enthalten

                if pos_size > 0:
                    # ── DCA-Fill-Erkennung ────────────────────────────────────────
                    await _check_dca_fills(symbol, trade, pos_size, avg_price)
                    # ── SL-Monitor: prüfe ob SL auf Exchange korrekt gesetzt ist ──
                    await _verify_sl(symbol, trade)
                    # ── DCA stornieren falls SL auf Break-Even (Reconcile) ───────
                    await _cancel_dca_if_sl_at_be(symbol, trade)
                    # ── Dynamisches Profit-Modell 3 prüfen ───────────────────────
                    if SYNORA_PROFIT_MODEL == 3:
                        await _check_dynamic_profit_levels(symbol, trade, pos_size)
                    continue   # Position noch offen

                # ── Position ist geschlossen ─────────────────
                if exchange == "bybit":
                    cancel_open_orders(symbol)
                    pnl_res   = bybit_get("/v5/position/closed-pnl", {
                        "category": BYBIT_CATEGORY, "symbol": symbol, "limit": "1",
                    })
                    pnl_items = (pnl_res.get("result") or {}).get("list") or []
                    closed_pnl = close_price = 0.0
                    if pnl_items:
                        p = pnl_items[0]
                        closed_pnl  = float(p.get("closedPnl",    0) or 0)
                        close_price = float(p.get("avgExitPrice", 0) or 0)
                else:
                    bingx_cancel_open_orders(ex_sym)
                    closed_pnl, close_price = bingx_get_closed_pnl(ex_sym)

                outcome = _determine_outcome(trade, close_price)
                record  = {
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
                _state["trades"].pop(symbol, None)
                save_state()

                emoji   = "✅" if closed_pnl >= 0 else "❌"
                pnl_str = f"+{closed_pnl:.2f}" if closed_pnl >= 0 else f"{closed_pnl:.2f}"
                tg_coin(
                    symbol,
                    f"{emoji} <b>SYNORA Trade geschlossen</b>\n"
                    f"Symbol: <b>{symbol}</b> | {outcome}\n"
                    f"Close: {close_price} | P&L: <b>{pnl_str} USDT</b>"
                )
                _close_coin_topic(symbol, won=closed_pnl >= 0)
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

    # ── Offene Positionen: Card-Rendering mit allen State-Feldern ──
    open_cards = ""
    for sym, tr in open_pos.items():
        side      = tr.get("side", "?")
        lev       = tr.get("lev", "?")
        entry     = float(tr.get("entry", 0))
        sl        = float(tr.get("sl", 0))
        tp        = tr.get("tp")
        dca1      = float(tr.get("dca1", 0))
        dca2      = float(tr.get("dca2", 0))
        exchange  = tr.get("exchange", "bybit")
        ex_sym    = tr.get("exchange_symbol", sym)
        budget    = float(tr.get("budget_usdt", 0))
        opened    = (tr.get("opened_at", "")[:16]).replace("T", " ")
        q_entry   = float(tr.get("qty_entry", 0))
        q_dca1    = float(tr.get("qty_dca1", 0))
        q_dca2    = float(tr.get("qty_dca2", 0))
        q_init    = float(tr.get("qty_total_initial", q_entry + q_dca1 + q_dca2))
        dyn_active = tr.get("dynamic_model_active", False)
        dyn_done   = tr.get("dynamic_levels_done", [])

        # Live Mark-Preis + unrealisierter Profit
        try:
            if exchange == "bybit":
                mark = get_mark_price(sym)
            else:
                mark = bingx_get_mark_price(ex_sym)
        except Exception:
            mark = 0.0
        if mark > 0 and entry > 0:
            live_pct = _current_profit_pct(side, entry, mark, int(lev))
            pct_color = "#22c55e" if live_pct >= 0 else "#ef4444"
            mark_str  = f"{mark:.6g}"
            pct_str   = f'<span style="color:{pct_color};font-weight:700">{live_pct:+.1f}%</span>'
        else:
            mark_str = "—"
            pct_str  = "—"

        # TP-Anzeige
        tp_str = f"{float(tp):.6g}" if tp else "—"

        # SL-Distanz
        if entry > 0 and sl > 0:
            sl_dist = abs(sl - entry) / entry * 100
            sl_str  = f"{sl:.6g} <small style='color:#64748b'>({sl_dist:.1f}%)</small>"
        else:
            sl_str = "—"

        # Exchange-Badge
        ex_badge = (
            f"<span style='background:#1e40af33;color:#60a5fa;border:1px solid #1e40af55;"
            f"border-radius:4px;padding:1px 7px;font-size:.72rem'>Bybit</span>"
            if exchange == "bybit" else
            f"<span style='background:#92400e33;color:#fbbf24;border:1px solid #92400e55;"
            f"border-radius:4px;padding:1px 7px;font-size:.72rem'>BingX</span>"
        )

        # Dynamic Model Fortschritts-Badges
        if dyn_active:
            level_badges = ""
            for pct, payout, sl_off in DYNAMIC_MODEL_LEVELS:
                done = pct in dyn_done
                if done:
                    badge_style = "background:#16532433;color:#22c55e;border:1px solid #22c55e55"
                    icon = "✓"
                else:
                    badge_style = "background:#1e233033;color:#475569;border:1px solid #2d3748"
                    icon = ""
                level_badges += (
                    f"<span style='{badge_style};border-radius:4px;padding:2px 7px;"
                    f"font-size:.72rem;margin:1px;display:inline-block'>"
                    f"{icon}+{pct}%</span>"
                )
            dyn_block = (
                f"<div style='margin-top:10px'>"
                f"<div style='font-size:.72rem;color:#94a3b8;margin-bottom:4px'>📊 DYNAMISCHES MODELL 3</div>"
                f"{level_badges}"
                f"</div>"
            )
        elif tp:
            dyn_block = (
                f"<div style='margin-top:10px;font-size:.72rem;color:#64748b'>"
                f"Modell 1 (einfacher TP) — Warte auf Preis {tp_str}"
                f"</div>"
            )
        else:
            dyn_block = (
                f"<div style='margin-top:10px;font-size:.72rem;color:#64748b'>"
                f"⏳ Warte auf SYNORA UPDATE…"
                f"</div>"
            )

        side_color = "#22c55e" if side == "LONG" else "#ef4444"
        open_cards += f"""
<div style="background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:16px;margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
    <div style="display:flex;align-items:center;gap:10px">
      <span style="font-size:1.1rem;font-weight:700;color:#e2e8f0">{sym}</span>
      <span style="color:{side_color};font-weight:700;font-size:.9rem">{side}</span>
      <span style="color:#94a3b8;font-size:.85rem">{lev}x</span>
      {ex_badge}
    </div>
    <div style="text-align:right">
      <div style="font-size:.75rem;color:#64748b">Live: {mark_str} &nbsp;|&nbsp; Profit: {pct_str}</div>
      <div style="font-size:.72rem;color:#475569">eröffnet {opened} UTC</div>
    </div>
  </div>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;font-size:.8rem">
    <div style="background:#0f1117;border-radius:6px;padding:8px">
      <div style="color:#64748b;font-size:.68rem;margin-bottom:2px">ENTRY</div>
      <div style="color:#e2e8f0;font-weight:600">{entry:.6g}</div>
    </div>
    <div style="background:#0f1117;border-radius:6px;padding:8px">
      <div style="color:#64748b;font-size:.68rem;margin-bottom:2px">STOP-LOSS</div>
      <div style="color:#ef4444">{sl_str}</div>
    </div>
    <div style="background:#0f1117;border-radius:6px;padding:8px">
      <div style="color:#64748b;font-size:.68rem;margin-bottom:2px">DCA1 (25%)</div>
      {"<div style='color:#374151;text-decoration:line-through'>" if tr.get('dca_cancelled') else "<div style='color:#94a3b8'>"}{dca1:.6g} <small style="color:#374151">×{q_dca1:.4g}</small>{"<span style='color:#6b7280;font-size:.65rem;margin-left:4px'>storniert</span>" if tr.get('dca_cancelled') else ""}</div>
    </div>
    <div style="background:#0f1117;border-radius:6px;padding:8px">
      <div style="color:#64748b;font-size:.68rem;margin-bottom:2px">DCA2 (65%)</div>
      {"<div style='color:#374151;text-decoration:line-through'>" if tr.get('dca_cancelled') else "<div style='color:#94a3b8'>"}{dca2:.6g} <small style="color:#374151">×{q_dca2:.4g}</small>{"<span style='color:#6b7280;font-size:.65rem;margin-left:4px'>storniert</span>" if tr.get('dca_cancelled') else ""}</div>
    </div>
    <div style="background:#0f1117;border-radius:6px;padding:8px">
      <div style="color:#64748b;font-size:.68rem;margin-bottom:2px">TP / MAX</div>
      <div style="color:#22c55e">{tp_str}</div>
    </div>
    <div style="background:#0f1117;border-radius:6px;padding:8px">
      <div style="color:#64748b;font-size:.68rem;margin-bottom:2px">QTY INITIAL</div>
      <div style="color:#94a3b8">{q_init:.4g} <small style="color:#475569">({budget:.0f} USDT)</small></div>
    </div>
  </div>
  {dyn_block}
</div>"""

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
  {open_cards}
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


@flask_app.route("/force_signal")
def force_signal():
    """
    Manuelles Signal auslösen — für verpasste oder Test-Signale.
    URL: /force_signal?secret=XXX&symbol=BROCCOLIF3BUSDT&side=SHORT&lev=3
         &entry=0.005710&sl=0.007500&dca1=0.006400&dca2=0.006800
    """
    secret = request.args.get("secret", "")
    if SYNORA_DASHBOARD_SECRET and secret != SYNORA_DASHBOARD_SECRET:
        return Response("403 Forbidden", status=403)

    try:
        sig = {
            "symbol": request.args["symbol"].upper(),
            "side":   request.args["side"].upper(),
            "lev":    int(request.args["lev"]),
            "entry":  float(request.args["entry"]),
            "sl":     float(request.args["sl"]),
            "dca1":   float(request.args["dca1"]),
            "dca2":   float(request.args["dca2"]),
        }
    except (KeyError, ValueError) as e:
        return Response(f"Fehlende/ungültige Parameter: {e}", status=400)

    if _main_loop is None:
        return Response("Event-Loop noch nicht bereit", status=503)

    log.info(f"[force_signal] Manuelles Signal: {sig}")
    try:
        future = asyncio.run_coroutine_threadsafe(execute_signal(sig), _main_loop)
        future.result(timeout=30)
        return Response(f"✅ Signal ausgeführt: {sig['side']} {sig['symbol']} {sig['lev']}x", status=200)
    except Exception as e:
        log.error(f"[force_signal] Fehler: {e}")
        return Response(f"❌ Fehler: {e}", status=500)


@flask_app.route("/activate_model")
def activate_model():
    """
    Aktiviert das Dynamische Profit-Modell 3 manuell für einen laufenden Trade.
    Nützlich für Trades die vor dem Feature-Deploy eröffnet wurden.

    URL: /activate_model?secret=XXX&symbol=WLDUSDT
    Optional: &qty_initial=37.6  (überschreibt qty_total_initial im State)
    """
    secret = request.args.get("secret", "")
    if SYNORA_DASHBOARD_SECRET and secret != SYNORA_DASHBOARD_SECRET:
        return Response("403 Forbidden", status=403)

    symbol = request.args.get("symbol", "").upper()
    if not symbol:
        return Response("❌ Parameter 'symbol' fehlt", status=400)

    trade = _state.get("trades", {}).get(symbol)
    if not trade:
        return Response(f"❌ Kein offener Trade für {symbol} im State", status=404)

    if trade.get("dynamic_model_active"):
        done = trade.get("dynamic_levels_done", [])
        return Response(
            f"ℹ️ {symbol}: Dynamisches Modell bereits aktiv. "
            f"Erledigte Stufen: {done or 'keine'}",
            status=200,
        )

    # qty_total_initial setzen falls noch nicht vorhanden
    if "qty_total_initial" not in trade:
        qty_override = request.args.get("qty_initial")
        if qty_override:
            try:
                qty_init = float(qty_override)
            except ValueError:
                return Response("❌ Ungültiger Wert für qty_initial", status=400)
        else:
            # Aus Entry+DCA1+DCA2 rekonstruieren (DCAs evtl. nicht gefüllt — konservativ)
            qty_init = float(trade.get("qty_entry", 0))
        _state["trades"][symbol]["qty_total_initial"] = qty_init
        log.info(f"activate_model: qty_total_initial={qty_init} für {symbol} gesetzt")

    _state["trades"][symbol]["dynamic_model_active"] = True
    _state["trades"][symbol].setdefault("dynamic_levels_done", [])
    save_state()

    qty_init = _state["trades"][symbol]["qty_total_initial"]
    entry    = float(trade.get("entry", 0))
    side     = trade.get("side", "?")
    lev      = int(trade.get("lev", 1))

    # Stufen-Preise berechnen für Anzeige
    lines = [f"✅ Dynamisches Modell 3 aktiviert für {symbol}\n"]
    lines.append(f"Entry: {entry} | Side: {side} | Hebel: {lev}x")
    lines.append(f"qty_total_initial: {qty_init}\n")
    lines.append("Stufen:")
    for pct, payout, sl_off in DYNAMIC_MODEL_LEVELS:
        price = calc_tp_price(side, entry, pct, lev)
        sl_lbl = f"SL→BE" if sl_off == 0 else (f"SL→+{sl_off}%" if sl_off else "—")
        lines.append(f"  +{pct:2d}% ({price:.6g}) → {payout}% schliessen | {sl_lbl}")

    msg = "\n".join(lines)
    log.info(f"[activate_model] {symbol}: Modell 3 aktiviert, qty_init={qty_init}")
    tg_coin(symbol, f"📊 <b>SYNORA Modell 3 manuell aktiviert</b>\n<b>{symbol}</b> {side} {lev}x\nqty_initial: {qty_init}")
    return Response(msg, content_type="text/plain; charset=utf-8", status=200)


@flask_app.route("/verify")
def verify():
    """
    Prüft alle offenen Trades live gegen Bybit/BingX.
    Zeigt: Position (Size, Entry, SL, TP) + offene Limit-Orders (DCA1/DCA2).
    URL: /verify?secret=XXX
    """
    secret = request.args.get("secret", "")
    if SYNORA_DASHBOARD_SECRET and secret != SYNORA_DASHBOARD_SECRET:
        return Response("403 Forbidden", status=403)

    trades = _state.get("trades", {})
    if not trades:
        return Response("ℹ️ Keine offenen Trades im State.", content_type="text/plain; charset=utf-8")

    lines = ["=== SYNORA VERIFY ===\n"]
    for symbol, trade in trades.items():
        exchange = trade.get("exchange", "bybit")
        ex_sym   = trade.get("exchange_symbol", symbol)
        lines.append(f"── {symbol} [{exchange.upper()}] ──")
        lines.append(f"  State: {trade.get('side')} {trade.get('lev')}x | Entry={trade.get('entry')} SL={trade.get('sl')} TP={trade.get('tp') or '—'}")

        if exchange == "bybit":
            # Position live abfragen
            pos_res = bybit_get("/v5/position/list", {"category": BYBIT_CATEGORY, "symbol": symbol})
            pos_items = (pos_res.get("result") or {}).get("list") or []
            pos_found = False
            for p in pos_items:
                if p.get("symbol") == symbol and float(p.get("size", 0) or 0) > 0:
                    lines.append(f"  Bybit Position: size={p.get('size')} | avgEntry={p.get('avgPrice')} | SL={p.get('stopLoss') or '—'} | TP={p.get('takeProfit') or '—'} | uPnL={p.get('unrealisedPnl')}")
                    pos_found = True
            if not pos_found:
                lines.append("  ⚠️ Bybit: KEINE offene Position gefunden!")

            # Offene Orders (DCA)
            ord_res = bybit_get("/v5/order/realtime", {"category": BYBIT_CATEGORY, "symbol": symbol})
            ord_items = (ord_res.get("result") or {}).get("list") or []
            if ord_items:
                lines.append(f"  Bybit Orders ({len(ord_items)}):")
                for o in ord_items:
                    lines.append(f"    {o.get('side')} {o.get('orderType')} qty={o.get('qty')} price={o.get('price')} status={o.get('orderStatus')}")
            else:
                lines.append("  ⚠️ Bybit: Keine offenen Limit-Orders (DCA fehlt?)")

        else:
            # BingX Position
            bx_size = bingx_get_position_size(ex_sym, trade["side"])
            if bx_size > 0:
                lines.append(f"  BingX Position: size={bx_size}")
            else:
                lines.append("  ⚠️ BingX: KEINE offene Position gefunden!")

            # BingX offene Orders
            ord_res = bingx_get("/openApi/swap/v2/trade/openOrders", {"symbol": ex_sym})
            ord_items = (ord_res.get("data") or {}).get("orders") or []
            if ord_items:
                lines.append(f"  BingX Orders ({len(ord_items)}):")
                for o in ord_items:
                    lines.append(f"    {o.get('side')} {o.get('type')} qty={o.get('origQty')} price={o.get('price')}")
            else:
                lines.append("  ⚠️ BingX: Keine offenen Limit-Orders (DCA fehlt?)")

        lines.append("")

    result = "\n".join(lines)
    log.info(f"[verify] {result}")
    return Response(result, content_type="text/plain; charset=utf-8")


def run_flask() -> None:
    """Startet Flask in einem Daemon-Thread."""
    flask_app.run(host="0.0.0.0", port=SYNORA_DASHBOARD_PORT, debug=False, use_reloader=False)


# ═══════════════════════════════════════════════════════════════
# TELEGRAM COMMANDS  (Bot API long-polling)
# ═══════════════════════════════════════════════════════════════

def tg_reply(chat_id, msg: str) -> None:
    """Antwortet direkt in den Anfrager-Chat (kein Forum-Routing)."""
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":                  chat_id,
                "text":                     msg,
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        log.error(f"tg_reply Fehler: {e}")


def build_verify_msg() -> str:
    """
    Prüft alle offenen Trades live gegen Bybit/BingX.
    Gibt eine Telegram-HTML-Nachricht zurück mit Position, SL, TP und DCA-Orders.
    """
    trades = _state.get("trades", {})
    if not trades:
        return "🟣 <b>SYNORA Verify</b>\n\nKeine offenen Trades im State."

    lines = [f"🔍 <b>SYNORA Verify</b> — {len(trades)} Trade(s)\n"]

    for symbol, trade in trades.items():
        exchange = trade.get("exchange", "bybit")
        ex_sym   = trade.get("exchange_symbol", symbol)
        side     = trade.get("side", "?")
        lev      = trade.get("lev", "?")
        icon     = "📈" if side == "LONG" else "📉"
        lines.append(f"{icon} <b>{symbol}</b> {side} {lev}x [{exchange.upper()}]")

        if exchange == "bybit":
            # ── Position ──
            pos_res   = bybit_get("/v5/position/list", {"category": BYBIT_CATEGORY, "symbol": symbol})
            pos_items = (pos_res.get("result") or {}).get("list") or []
            pos_found = False
            for p in pos_items:
                if p.get("symbol") == symbol and float(p.get("size", 0) or 0) > 0:
                    sl_live = p.get("stopLoss") or "—"
                    tp_live = p.get("takeProfit") or "—"
                    upnl    = float(p.get("unrealisedPnl") or 0)
                    upnl_icon = "🟢" if upnl >= 0 else "🔴"
                    lines.append(
                        f"  Pos: {p.get('size')} | Entry: {p.get('avgPrice')}\n"
                        f"  SL: {sl_live} | TP: {tp_live}\n"
                        f"  uPnL: {upnl_icon} {upnl:.2f} USDT"
                    )
                    pos_found = True
            if not pos_found:
                lines.append("  ⚠️ <b>KEINE Position auf Bybit gefunden!</b>")

            # ── Offene Orders (DCA) ──
            ord_res   = bybit_get("/v5/order/realtime", {"category": BYBIT_CATEGORY, "symbol": symbol})
            ord_items = (ord_res.get("result") or {}).get("list") or []
            if ord_items:
                lines.append(f"  Orders ({len(ord_items)}):")
                for o in ord_items:
                    lines.append(
                        f"    {o.get('side')} {o.get('orderType')} "
                        f"qty={o.get('qty')} @ {o.get('price')}"
                    )
            else:
                lines.append("  ⚠️ Keine offenen DCA-Orders gefunden")

        else:
            # ── BingX Position ──
            bx_size = bingx_get_position_size(ex_sym, side)
            if bx_size > 0:
                lines.append(f"  Pos: {bx_size}")
            else:
                lines.append("  ⚠️ <b>KEINE Position auf BingX gefunden!</b>")

            # ── BingX Orders ──
            ord_res   = bingx_get("/openApi/swap/v2/trade/openOrders", {"symbol": ex_sym})
            ord_items = (ord_res.get("data") or {}).get("orders") or []
            if ord_items:
                lines.append(f"  Orders ({len(ord_items)}):")
                for o in ord_items:
                    lines.append(
                        f"    {o.get('side')} {o.get('type')} "
                        f"qty={o.get('origQty')} @ {o.get('price')}"
                    )
            else:
                lines.append("  ⚠️ Keine offenen DCA-Orders gefunden")

        lines.append("")

    return "\n".join(lines).rstrip()


async def fix_trade(symbol: str) -> str:
    """
    Korrigiert einen offenen Trade auf Exchange-Niveau:
      1. SL     → richtig für aktuelles Stadium (dynamic_levels_done)
      2. DCAs   → fehlende re-platzieren (falls nicht gefüllt)
      3. Modell → dynamic_model_active sicherstellen
    Gibt einen Bericht-String (Telegram HTML) zurück.
    """
    trade = _state.get("trades", {}).get(symbol)
    if not trade:
        return f"❌ Kein offener Trade für <b>{symbol}</b> im State."

    side     = trade.get("side", "LONG")
    lev      = int(trade.get("lev", 1))
    entry    = float(trade.get("entry", 0))
    sl_orig  = float(trade.get("sl", 0))
    dca1     = float(trade.get("dca1", 0))
    dca2     = float(trade.get("dca2", 0))
    qty_entry = float(trade.get("qty_entry", 0))
    qty_dca1  = float(trade.get("qty_dca1", 0))
    qty_dca2  = float(trade.get("qty_dca2", 0))
    exchange  = trade.get("exchange", "bybit")
    ex_sym    = trade.get("exchange_symbol", symbol)
    levels_done = list(trade.get("dynamic_levels_done", []))
    dca_notified = set(trade.get("dca_fills_notified", []))

    fixes  = []
    errors = []

    # ── 1. Live-Position von Exchange holen ──────────────────────
    pos_size  = 0.0
    avg_price = entry  # Fallback: Signal-Entry
    sl_live   = 0.0

    if exchange == "bybit":
        pos_res   = bybit_get("/v5/position/list", {"category": BYBIT_CATEGORY, "symbol": symbol})
        pos_items = (pos_res.get("result") or {}).get("list") or []
        for p in pos_items:
            if p.get("symbol") == symbol and float(p.get("size", 0) or 0) > 0:
                pos_size  = float(p.get("size", 0) or 0)
                avg_price = float(p.get("avgPrice", entry) or entry)
                sl_live   = float(p.get("stopLoss", 0) or 0)
                break
    else:
        pos_size = bingx_get_position_size(ex_sym, side)
        avg_price = entry   # BingX: avg aus State

    if pos_size == 0:
        return f"⚠️ <b>{symbol}</b>: Keine offene Position auf Exchange — nichts zu korrigieren."

    # ── 2. Korrekten SL für aktuelles Stadium berechnen ──────────
    # Finde höchsten sl_offset aus abgeschlossenen Stufen
    correct_sl = sl_orig
    for pct, _, sl_off in DYNAMIC_MODEL_LEVELS:
        if pct not in levels_done:
            continue
        if sl_off is None:
            continue
        if sl_off == 0:
            correct_sl = avg_price   # Break-Even = aktueller Avg
        else:
            correct_sl = calc_tp_price(side, avg_price, sl_off, lev)

    # SL korrigieren falls Abweichung > 0.1%
    sl_needs_fix = sl_live == 0 or (sl_orig > 0 and abs(sl_live - correct_sl) / correct_sl > 0.001)
    if sl_needs_fix:
        if exchange == "bybit":
            res_sl = set_sl(symbol, side, correct_sl)
            ok_sl  = bybit_ok(res_sl)
        else:
            res_sl = bingx_set_sl(ex_sym, side, correct_sl)
            ok_sl  = bingx_ok(res_sl)
        stage_label = f"Stufe {max(levels_done)}" if levels_done else "Original"
        if ok_sl:
            fixes.append(f"SL → {correct_sl:.6g} ({stage_label}) ✓")
            _state["trades"][symbol]["sl"] = correct_sl
        else:
            err = (res_sl.get("retMsg") or res_sl.get("msg") or "?") if res_sl else "keine Antwort"
            errors.append(f"SL-Fehler: {err}")
    else:
        fixes.append(f"SL: {sl_live:.6g} ✓ (korrekt)")

    # ── 3. DCAs prüfen und re-platzieren ─────────────────────────
    # Offene Orders holen
    open_prices: set[float] = set()
    if exchange == "bybit":
        ord_res   = bybit_get("/v5/order/realtime", {"category": BYBIT_CATEGORY, "symbol": symbol})
        ord_items = (ord_res.get("result") or {}).get("list") or []
        for o in ord_items:
            try:
                open_prices.add(float(o.get("price", 0) or 0))
            except (ValueError, TypeError):
                pass
    else:
        ord_res   = bingx_get("/openApi/swap/v2/trade/openOrders", {"symbol": ex_sym})
        ord_items = (ord_res.get("data") or {}).get("orders") or []
        for o in ord_items:
            try:
                open_prices.add(float(o.get("price", 0) or 0))
            except (ValueError, TypeError):
                pass

    def _price_present(target: float, tolerance: float = 0.003) -> bool:
        """Prüft ob eine Order nahe am Zielpreis vorhanden ist."""
        return any(abs(p - target) / target <= tolerance for p in open_prices if p > 0)

    dca1_filled = "dca1" in dca_notified or pos_size >= qty_entry + qty_dca1 * 0.7
    dca2_filled = "dca2" in dca_notified or pos_size >= qty_entry + qty_dca1 + qty_dca2 * 0.7

    # DCA1 re-platzieren
    if dca1 > 0 and qty_dca1 > 0 and not dca1_filled and not _price_present(dca1):
        if exchange == "bybit":
            res_dca1 = place_limit_order(symbol, side, qty_dca1, dca1)
            ok_dca1  = bybit_ok(res_dca1)
            new_id   = (res_dca1.get("result") or {}).get("orderId")
        else:
            res_dca1 = bingx_place_limit_order(ex_sym, side, qty_dca1, dca1)
            ok_dca1  = bingx_ok(res_dca1)
            new_id   = (res_dca1.get("data") or {}).get("orderId")
        if ok_dca1:
            fixes.append(f"DCA1 @ {dca1} re-platziert ✓")
            if new_id:
                _state["trades"][symbol]["dca1_order_id"] = new_id
        else:
            err = (res_dca1.get("retMsg") or res_dca1.get("msg") or "?") if res_dca1 else "?"
            errors.append(f"DCA1-Fehler: {err}")
    elif dca1_filled:
        fixes.append(f"DCA1: gefüllt (übersprungen)")
    else:
        fixes.append(f"DCA1 @ {dca1} ✓ (vorhanden)")

    # DCA2 re-platzieren
    if dca2 > 0 and qty_dca2 > 0 and not dca2_filled and not _price_present(dca2):
        if exchange == "bybit":
            res_dca2 = place_limit_order(symbol, side, qty_dca2, dca2)
            ok_dca2  = bybit_ok(res_dca2)
            new_id   = (res_dca2.get("result") or {}).get("orderId")
        else:
            res_dca2 = bingx_place_limit_order(ex_sym, side, qty_dca2, dca2)
            ok_dca2  = bingx_ok(res_dca2)
            new_id   = (res_dca2.get("data") or {}).get("orderId")
        if ok_dca2:
            fixes.append(f"DCA2 @ {dca2} re-platziert ✓")
            if new_id:
                _state["trades"][symbol]["dca2_order_id"] = new_id
        else:
            err = (res_dca2.get("retMsg") or res_dca2.get("msg") or "?") if res_dca2 else "?"
            errors.append(f"DCA2-Fehler: {err}")
    elif dca2_filled:
        fixes.append(f"DCA2: gefüllt (übersprungen)")
    else:
        fixes.append(f"DCA2 @ {dca2} ✓ (vorhanden)")

    # ── 4. TP-Orders für ausstehende Modell-3-Stufen setzen ──────
    if SYNORA_PROFIT_MODEL == 3:
        initial_qty = float(trade.get("qty_total_initial", pos_size))

        # Reduce-only TP-Orders: nur für noch nicht abgeschlossene Stufen
        # und nur wenn noch keine Order in der Nähe des TP-Preises existiert
        for pct, payout, _ in DYNAMIC_MODEL_LEVELS:
            if pct in levels_done:
                continue   # Stufe bereits abgeschlossen

            tp_price   = calc_tp_price(side, avg_price, pct, lev)
            qty_close  = initial_qty * payout / 100.0

            # Qty auf Exchange-Precision runden
            if exchange == "bybit":
                qty_close = snap_qty(symbol, qty_close) if qty_close > 0 else 0
                price_fmt = fmt_price(symbol, tp_price)
            else:
                step = _bingx_qty_step_cache.get(ex_sym, 0.001)
                qty_close = math.floor(qty_close / step) * step
                price_fmt = bingx_fmt_price(ex_sym, tp_price)

            if qty_close <= 0:
                continue

            # Prüfen ob bereits eine Order nahe diesem Preis existiert
            if _price_present(tp_price):
                fixes.append(f"TP +{pct}% @ {price_fmt} ✓ (vorhanden)")
                continue

            # Platzieren
            if exchange == "bybit":
                res_tp = place_tp_order(symbol, side, qty_close, tp_price)
                ok_tp  = bybit_ok(res_tp)
                err_tp = (res_tp.get("retMsg") or "?") if res_tp else "?"
            else:
                res_tp = bingx_place_tp_order(ex_sym, side, qty_close, tp_price)
                ok_tp  = bingx_ok(res_tp)
                err_tp = (res_tp.get("msg") or "?") if res_tp else "?"

            if ok_tp:
                qty_fmt = fmt_qty(symbol, qty_close) if exchange == "bybit" else bingx_fmt_qty(ex_sym, qty_close)
                fixes.append(f"TP +{pct}% @ {price_fmt} × {qty_fmt} gesetzt ✓")
            else:
                errors.append(f"TP +{pct}%-Fehler: {err_tp}")

    # ── 5. Modell-3-Status sicherstellen ─────────────────────────
    if SYNORA_PROFIT_MODEL == 3 and not trade.get("dynamic_model_active"):
        _state["trades"][symbol]["dynamic_model_active"] = True
        fixes.append("Profit-Modell 3 aktiviert ✓")
    else:
        fixes.append("Profit-Modell 3: aktiv ✓")

    save_state()

    # ── Bericht zusammenstellen ───────────────────────────────────
    icon    = "📈" if side == "LONG" else "📉"
    stage   = f"{len(levels_done)}/6 Stufen" if levels_done else "0/6 Stufen"
    lines   = [
        f"🔧 <b>SYNORA Fix — {symbol}</b> {icon} {side} {lev}x",
        f"Pos: {pos_size} | Avg: {avg_price:.6g} | {stage}",
        "━━━━━━━━━━━━",
    ]
    for f in fixes:
        lines.append(f"  ✅ {f}")
    for e in errors:
        lines.append(f"  ❌ {e}")

    return "\n".join(lines)


def build_synora_status_msg() -> str:
    """Erstellt die /status Nachricht aus dem aktuellen State."""
    trades = _state.get("trades", {})
    pause_line = "\n⏸ <b>Bot pausiert</b> — keine neuen Trades.\n" if _paused else ""
    if not trades:
        return f"🟣 <b>SYNORA Status</b>{pause_line}\n\nKeine offenen Trades."
    lines = [f"🟣 <b>SYNORA Status</b> — {len(trades)} offene Trade(s){pause_line}\n"]
    for sym, t in trades.items():
        side   = t.get("side", "?")
        lev    = t.get("lev", "?")
        entry  = t.get("entry", 0)
        sl     = t.get("sl", 0)
        exch   = t.get("exchange", "bybit").upper()
        icon   = "📈" if side == "LONG" else "📉"
        opened = t.get("opened_at", "")[:16].replace("T", " ")
        lines.append(
            f"{icon} <b>{sym}</b> {side} {lev}x @ {entry:.6g}"
            f"\n   SL: {sl:.6g} | {exch} | {opened} UTC"
        )
    return "\n".join(lines)


def build_synora_report_msg(period: str = "today") -> str:
    """P&L-Report aus der CSV.  period: 'today' | 'yesterday' | 'month'"""
    all_trades = read_trades_csv()
    now = datetime.now(timezone.utc)

    if period == "month":
        label = f"Monats-Report {now.strftime('%m/%Y')}"
        key = now.strftime("%Y-%m")
        filtered = [r for r in all_trades if r.get("closed_at", "")[:7] == key]
    elif period == "yesterday":
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        label = f"Tages-Report {(now - timedelta(days=1)).strftime('%d.%m.%Y')}"
        filtered = [r for r in all_trades if r.get("closed_at", "")[:10] == yesterday]
    else:  # today
        label = f"Tages-Report {now.strftime('%d.%m.%Y')}"
        filtered = [r for r in all_trades if r.get("closed_at", "")[:10] == now.strftime("%Y-%m-%d")]

    if not filtered:
        return f"🟣 <b>SYNORA {label}</b>\n\nKeine abgeschlossenen Trades."

    wins  = [r for r in filtered if r.get("outcome") == "TP"]
    losses = [r for r in filtered if r.get("outcome") == "SL"]
    total_pnl = sum(float(r.get("pnl_usdt") or 0) for r in filtered)
    total_n   = len(filtered)
    wr        = len(wins) / total_n * 100 if total_n else 0

    lines = [
        f"🟣 <b>SYNORA {label}</b>",
        f"Trades: {total_n} | ✅ {len(wins)} TP / ❌ {len(losses)} SL",
        f"Winrate: {wr:.0f}%",
        f"P&amp;L: <b>{'%+.2f' % total_pnl} USDT</b>",
        "",
    ]
    for r in filtered[-10:]:
        sym  = r.get("symbol", "?")
        side = r.get("side", "?")
        pnl  = float(r.get("pnl_usdt") or 0)
        out  = "✅" if r.get("outcome") == "TP" else "❌"
        icon = "📈" if side == "LONG" else "📉"
        lines.append(f"{out} {icon} {sym} {side}  {'+' if pnl >= 0 else ''}{pnl:.2f} USDT")

    return "\n".join(lines)


def handle_synora_command(text: str, chat_id) -> None:
    """Verarbeitet Telegram-Befehle (läuft im Thread, nicht async)."""
    global _paused
    cmd = text.strip().lower().split()[0].split("@")[0]
    log.info(f"Befehl: {cmd} (chat={chat_id})")

    if cmd == "/status":
        msg = build_synora_status_msg()
        tg_reply(chat_id, msg)
        tg_status(msg)

    elif cmd == "/report":
        args = text.strip().lower().split()
        period = "month" if len(args) > 1 and "monat" in args[1] else "today"
        msg = build_synora_report_msg(period)
        tg_reply(chat_id, msg)
        tg_report(msg)

    elif cmd == "/refresh":
        tg_reply(chat_id, "🔄 Lade State neu …")
        load_state()
        n = len(_state.get("trades", {}))
        tg_reply(chat_id, f"✅ State neu geladen — {n} offene Trade(s).")

    elif cmd == "/pause":
        _paused = True
        msg = "⏸ <b>SYNORA pausiert</b> — neue Signale werden ignoriert."
        tg_reply(chat_id, msg)
        tg_status(msg)

    elif cmd == "/resume":
        _paused = False
        msg = "▶️ <b>SYNORA aktiv</b> — verarbeite wieder Signale."
        tg_reply(chat_id, msg)
        tg_status(msg)

    elif cmd == "/balance":
        bybit_raw  = fetch_bybit_balance_raw()
        bingx_raw  = fetch_bingx_balance_raw()
        cap        = SYNORA_BUDGET_CAP_USDT

        def fmt_line(name: str, raw: float) -> str:
            if raw == -2.0:
                return f"• {name}: <i>nicht konfiguriert</i>"
            if raw < 0:
                return f"• {name}: ❌ Abruf fehlgeschlagen"
            if cap > 0:
                effective = min(raw, cap)
                return (
                    f"• {name}: <b>{raw:.2f} USDT</b> verfügbar\n"
                    f"  └ Effektiv (nach Cap): <b>{effective:.2f} USDT</b>"
                )
            return f"• {name}: <b>{raw:.2f} USDT</b> verfügbar"

        cap_info = f"Cap: {cap:.0f} USDT" if cap > 0 else "kein Cap"
        msg = (
            f"💰 <b>SYNORA Balance</b> ({cap_info})\n"
            f"━━━━━━━━━━━━\n"
            + fmt_line("Bybit Sub-Account", bybit_raw) + "\n"
            + fmt_line("BingX", bingx_raw)
        )
        tg_reply(chat_id, msg)

    elif cmd == "/verify":
        tg_reply(chat_id, "🔍 Prüfe live gegen Exchange …")
        msg = build_verify_msg()
        tg_reply(chat_id, msg)

    elif cmd == "/fix":
        parts = text.strip().split()
        if len(parts) < 2:
            open_syms = list(_state.get("trades", {}).keys())
            if len(open_syms) == 1:
                symbol = open_syms[0]
            else:
                tg_reply(chat_id, "⚠️ Bitte Symbol angeben: <code>/fix WLDUSDT</code>")
                return
        else:
            symbol = parts[1].upper()
            if not symbol.endswith("USDT"):
                symbol += "USDT"
        tg_reply(chat_id, f"🔧 Korrigiere <b>{symbol}</b> …")
        msg = asyncio.run(fix_trade(symbol))
        tg_reply(chat_id, msg)

    elif cmd == "/hilfe":
        tg_reply(chat_id,
            "🟣 <b>SYNORA Befehle</b>\n\n"
            "/status — offene Trades\n"
            "/verify — TP &amp; DCA live prüfen (Exchange)\n"
            "/fix [SYMBOL] — SL/DCA/Modell korrigieren\n"
            "/balance — verfügbare Balance (Bybit + BingX)\n"
            "/report — heutiger P&amp;L-Report\n"
            "/report monat — Monats-Report\n"
            "/pause — neue Signale pausieren\n"
            "/resume — Bot wieder aktivieren\n"
            "/refresh — State neu laden\n"
            "/hilfe — diese Hilfe"
        )
    else:
        tg_reply(chat_id, f"❓ Unbekannter Befehl: <code>{cmd}</code>\nTippe /hilfe für alle Befehle.")


async def poll_synora_commands() -> None:
    """Asyncio-Task: Bot API long-polling für Telegram-Befehle."""
    global _cmd_offset
    log.info("Command-Polling gestartet ✓")
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={
                    "offset":           _cmd_offset,
                    "timeout":          30,
                    "allowed_updates":  ["message"],
                },
                timeout=35,
            )
            data = r.json()
            if not data.get("ok"):
                await asyncio.sleep(5)
                continue
            for upd in data.get("result", []):
                _cmd_offset = upd["update_id"] + 1
                msg  = upd.get("message", {})
                text = msg.get("text", "")
                cid  = msg.get("chat", {}).get("id")
                if text.startswith("/") and cid:
                    threading.Thread(
                        target=handle_synora_command,
                        args=(text, cid),
                        daemon=True,
                    ).start()
        except Exception as e:
            log.error(f"Command-Polling Fehler: {e}")
            await asyncio.sleep(10)


async def check_auto_daily_report() -> None:
    """Asyncio-Task: sendet jeden Morgen um 08:00 UTC den gestrigen Report."""
    log.info("Auto-Daily-Report Task gestartet ✓")
    last_sent_date = None
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == 8 and now.minute < 1 and now.date() != last_sent_date:
            try:
                msg = build_synora_report_msg("yesterday")
                tg_report(msg)
                last_sent_date = now.date()
                log.info("Auto-Daily-Report gesendet ✓")
            except Exception as e:
                log.error(f"Auto-Daily-Report Fehler: {e}")
        await asyncio.sleep(55)


# ═══════════════════════════════════════════════════════════════
# TELETHON — BOT/KANAL-LISTENER
# ═══════════════════════════════════════════════════════════════

async def catchup_missed_messages(client, source, lookback_seconds: int = 1800) -> None:
    """
    Liest beim Startup die letzten `lookback_seconds` Sekunden (Default: 30 Min) des Synora-Kanals
    und führt verpasste Signale/Updates/Closes sofort aus.

    Reihenfolge: chronologisch (älteste zuerst), damit DCA-Updates nach dem
    Signal ankommen falls beide im Fenster liegen.

    Schutz gegen Doppel-Entry:
      - Signal:  nur ausführen wenn Symbol NICHT bereits im State
      - Update:  nur ausführen wenn Symbol bereits im State
      - Close:   nur ausführen wenn Symbol bereits im State
    """
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=lookback_seconds)

    try:
        missed: list = []
        # limit=None → Telethon iteriert bis break; die Datum-Prüfung stoppt
        # den Loop sobald wir ausserhalb des Fensters sind
        async for msg in client.iter_messages(source, limit=None):
            if not msg.date:
                continue
            msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
            if msg_date < cutoff:
                break
            missed.append((msg_date, msg.message or ""))

        if not missed:
            log.info(f"Catchup: Keine Nachrichten in den letzten {lookback_seconds//60} Min — nichts nachzuholen.")
            return

        # Chronologisch verarbeiten (iter_messages liefert newest-first)
        missed.reverse()
        log.info(f"Catchup: {len(missed)} Nachricht(en) im 60s-Fenster gefunden → prüfe …")

        executed: list[str] = []

        for msg_date, text in missed:
            if not text:
                continue

            # ── Signal ──
            sig = parse_signal(text)
            if sig:
                symbol = sig.get("symbol", "?")
                if symbol in _state.get("trades", {}):
                    log.info(f"Catchup: Signal {symbol} bereits im State → übersprungen")
                    continue
                if _paused:
                    log.info(f"Catchup: Bot pausiert — Signal {symbol} ignoriert")
                    continue
                log.info(f"Catchup: Signal {symbol} {sig.get('side')} wird nachgeholt …")
                tg_system(
                    f"⏱ <b>SYNORA Catchup</b>: Signal <b>{symbol}</b> "
                    f"({sig.get('side')}) während Neustart verpasst — führe jetzt aus."
                )
                await execute_signal(sig)
                executed.append(f"Signal {symbol} {sig.get('side')}")
                continue

            # ── Update (TP) ──
            upd = parse_update(text)
            if upd:
                symbol = upd.get("symbol", "?")
                if symbol not in _state.get("trades", {}):
                    log.info(f"Catchup: Update {symbol} nicht im State → übersprungen")
                    continue
                log.info(f"Catchup: TP-Update {symbol} wird nachgeholt …")
                await handle_update(upd)
                executed.append(f"TP-Update {symbol}")
                continue

            # ── Close ──
            close_sym = parse_close(text)
            if close_sym:
                if close_sym not in _state.get("trades", {}):
                    log.info(f"Catchup: Close {close_sym} nicht im State → übersprungen")
                    continue
                log.info(f"Catchup: Close {close_sym} wird nachgeholt …")
                await handle_close(close_sym, reason="SYNORA CANCEL (catchup)")
                executed.append(f"Close {close_sym}")
                continue

        if executed:
            log.info(f"Catchup abgeschlossen: {', '.join(executed)}")
        else:
            log.info("Catchup: Alle Nachrichten bereits verarbeitet oder nicht relevant — nichts nachgeholt.")

    except Exception as e:
        log.error(f"Catchup Fehler: {e}", exc_info=True)
        tg_system(f"⚠️ <b>SYNORA Catchup Fehler</b>: {e}")


async def main() -> None:
    global _main_loop
    _main_loop = asyncio.get_event_loop()
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
    tg_status(f"🟣 <b>SYNORA Monitor gestartet</b>\nBudget: live vom Sub-Account ({cap_str}) | Bybit Sub-Account")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    # ── Startup-Reconcile: alle offenen Trades prüfen + korrigieren ──
    await reconcile_trade_state(startup=True)

    # ── Position-Polling + Commands + Auto-Report starten ───
    asyncio.ensure_future(check_closed_positions())
    asyncio.ensure_future(poll_synora_commands())
    asyncio.ensure_future(check_auto_daily_report())

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
            if _paused:
                log.info("Bot pausiert — Signal ignoriert.")
                tg_status(f"⏸ <b>SYNORA pausiert</b> — Signal ignoriert:\n{sig.get('symbol')} {sig.get('side')}")
                return
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

    # ── Startup-Catchup: verpasste Nachrichten der letzten 60s ──
    await catchup_missed_messages(client, source, lookback_seconds=1800)

    log.info("Warte auf Signale …")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
