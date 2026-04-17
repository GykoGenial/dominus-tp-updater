"""
DOMINUS Trade-Automatisierung v4
══════════════════════════════════════════════════════════════
Vollautomatisches Setup nach DOMINUS-Strategie (Handbuch März 2026)
Finanzmathematische Optimierungen:
  ① Hebel-Empfehlung  — Hebel = 25 / SL-Abstand%
  ② R:R-Filter        — kein Trade unter 1.5 R:R
  ③ Kelly-Kriterium   — optimale Positionsgrösse
  ④ Asymm. TPs        — 15/20/25/40% statt 25/25/25/25%
  ⑤ Telegram Polling  — /berechnen /trade /status /hilfe

WAS PASSIERT AUTOMATISCH:
  1. Neuer Trade erkannt → Hebel-Check + R:R-Check
                         → DCA1 + DCA2 Limit-Orders
                         → TP1–TP4 (15/20/25/40% + Rest)
                         → Telegram-Vollbericht
  2. Nachkauf erkannt   → Alle TPs neu berechnet
  3. TP1 ausgelöst      → SL auf Entry gezogen
     TP2 ausgelöst      → Trailing SL auf TP1-Preis
     TP3 ausgelöst      → Trailing SL auf TP2-Preis

WAS DU TUN MUSST:
  - Market Order auf Bitget platzieren (Long/Short)
  - Hebel setzen (Script zeigt Empfehlung)
  - SL (Ausstiegslinie) auf Bitget setzen
  - Script macht den Rest

RAILWAY VARIABLES:
  API_KEY           → Bitget API Key
  SECRET_KEY        → Bitget Secret Key
  PASSPHRASE        → Bitget Passphrase
  WINRATE           → eigene Winrate 0.0–1.0 (Standard: 0.55)
  MIN_RR            → Mindest-R:R Ratio (Standard: 1.5)
  TELEGRAM_TOKEN    → optional
  TELEGRAM_CHAT_ID  → optional
══════════════════════════════════════════════════════════════
"""

import hashlib
import hmac
import base64
import time
import json
import os
import threading
import requests
import math
from datetime import datetime
try:
    from flask import Flask, request as flask_request, jsonify
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════
# KONFIGURATION — aus Railway Variables
# ═══════════════════════════════════════════════════════════════

API_KEY    = os.environ.get("API_KEY", "")
SECRET_KEY = os.environ.get("SECRET_KEY", "")
PASSPHRASE = os.environ.get("PASSPHRASE", "")

PRODUCT_TYPE = "usdt-futures"
MARGIN_COIN  = "USDT"

# DOMINUS Auszahlungsschema — asymmetrisch optimiert (+16% EV)
TP1_ROI   = 0.10   # 10% ROI
TP2_ROI   = 0.20   # 20% ROI
TP3_ROI   = 0.30   # 30% ROI
TP4_ROI   = 0.40   # 40% ROI
# Asymmetrische Schliess-Grössen: weniger bei TP1 (bereits gesichert),
# mehr bei TP3/TP4 (maximaler Gewinn)
TP_CLOSE_PCTS = [0.15, 0.20, 0.25, 0.40]  # TP1/TP2/TP3/TP4-Anteil

# DCA-Platzierung relativ zu Entry→SL Abstand
DCA1_RATIO = 1/3      # DCA1 bei 1/3 des Weges zum SL
DCA2_RATIO = 2/3      # DCA2 bei 2/3 des Weges zum SL

# Progressives DCA Sizing 20/30/50 — immer aktiv
# Verhältnis zum Initial-Trade:
#   DCA1 = Initial × 1.5  (30/20)
#   DCA2 = Initial × 2.5  (50/20)
# Gesamtsetup: Market 20% + DCA1 30% + DCA2 50% = 100% des Einsatzes
DCA1_MULTIPLIER = 1.5   # DCA1 = 1.5× Initial
DCA2_MULTIPLIER = 2.5   # DCA2 = 2.5× Initial

POLL_INTERVAL = 20    # Sekunden zwischen Checks

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "dominus")  # Token für TradingView

# Finanzmathematische Parameter
WINRATE = float(os.environ.get("WINRATE", "0.55"))  # eigene Winrate (historisch)
MIN_RR  = float(os.environ.get("MIN_RR", "1.5"))    # Mindest R:R Ratio

BASE_URL = "https://api.bitget.com"

# ═══════════════════════════════════════════════════════════════
# ZUSTANDSSPEICHER
# ═══════════════════════════════════════════════════════════════

last_known_avg:   dict = {}   # {symbol: avg_price}
last_known_size:  dict = {}   # {symbol: position_size}
sl_at_entry:      dict = {}   # {symbol: bool}
new_trade_done:   dict = {}   # {symbol: bool} — DCA bereits gesetzt?
price_decimals_cache: dict = {}
last_update_id:   int  = 0    # Telegram: letzter verarbeiteter Update-ID

# Trade-Daten für Auswertung bei Abschluss
# {symbol: {entry, direction, leverage, sl, peak_size, open_ts}}
trade_data: dict = {}

# H4 Trigger-Puffer: sammelt Alerts, sendet gebündelt nach Zeitfenster
h4_buffer:     list = []
h4_buffer_lock = __import__("threading").Lock()
H4_BUFFER_SEC  = int(os.environ.get("H4_BUFFER_SEC", "300"))  # 5 Min

trailing_sl_level: dict = {}  # {symbol: int} — 0=initial, 1=Entry, 2=TP1-Preis, 3=TP2-Preis

# Daily P&L Report — Aufzeichnung abgeschlossener Trades
closed_trades: list = []
daily_report_sent_date: str = ""   # "2026-04-17" — verhindert Doppelversand pro Tag

# Harsi-Ausstiegslinie
harsi_sl: dict = {}


# ═══════════════════════════════════════════════════════════════
# BASIS-FUNKTIONEN
# ═══════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def dir_icon(direction: str) -> str:
    """Richtungs-Icon: 🟢↗️ für Long, 🔴↘️ für Short."""
    return "🟢↗️" if direction == "long" else "🔴↘️"


def telegram(msg: str):
    """Sendet Telegram-Nachricht wenn konfiguriert."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5
        )
    except Exception:
        pass


def sign(timestamp: str, method: str, path: str, body: str = "") -> str:
    msg = timestamp + method.upper() + path + body
    sig = hmac.new(SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()


def make_headers(method: str, path: str, body: str = "") -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "ACCESS-KEY":        API_KEY,
        "ACCESS-SIGN":       sign(ts, method, path, body),
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "ACCESS-TIMESTAMP":  ts,
        "Content-Type":      "application/json",
        "locale":            "en-US",
    }


def api_get(path: str, params: dict = None) -> dict:
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    full_path = path + query
    try:
        r = requests.get(BASE_URL + full_path,
                         headers=make_headers("GET", full_path), timeout=10)
        return r.json()
    except Exception as e:
        log(f"GET Fehler ({path}): {e}")
        return {}


def api_post(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    try:
        r = requests.post(BASE_URL + path,
                          headers=make_headers("POST", path, body_str),
                          data=body_str, timeout=10)
        return r.json()
    except Exception as e:
        log(f"POST Fehler ({path}): {e}")
        return {}


# ═══════════════════════════════════════════════════════════════
# PREIS-PRÄZISION
# ═══════════════════════════════════════════════════════════════

def get_price_decimals(symbol: str) -> int:
    """Erlaubte Dezimalstellen von Bitget Contract-API (gecacht)."""
    if symbol in price_decimals_cache:
        return price_decimals_cache[symbol]
    result = api_get("/api/v2/mix/market/contracts", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    decimals = 4
    try:
        contracts = result.get("data", [])
        if contracts:
            decimals = int(contracts[0].get("pricePlace", "4"))
    except Exception:
        pass
    price_decimals_cache[symbol] = decimals
    return decimals


def round_price(price: float, decimals: int) -> str:
    return f"{price:.{decimals}f}"


# ═══════════════════════════════════════════════════════════════
# MARKTDATEN
# ═══════════════════════════════════════════════════════════════

def get_mark_price(symbol: str) -> float:
    """Aktuellen Mark Price von Bitget."""
    result = api_get("/api/v2/mix/market/symbol-price", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    try:
        data = result.get("data", [])
        if data:
            return float(data[0].get("markPrice", 0))
    except Exception:
        pass
    return 0.0


def get_futures_balance() -> float:
    """Verfügbares USDT-Guthaben im Futures-Konto."""
    result = api_get("/api/v2/mix/account/accounts", {
        "productType": PRODUCT_TYPE,
    })
    try:
        for acc in result.get("data", []):
            if acc.get("marginCoin") == MARGIN_COIN:
                # usdtEquity = Gesamtwert inkl. offener Positionen
                return float(acc.get("usdtEquity", 0))
    except Exception:
        pass
    return 0.0


# ═══════════════════════════════════════════════════════════════
# POSITIONEN & FILLS
# ═══════════════════════════════════════════════════════════════

def get_all_positions() -> list:
    """Alle offenen Futures-Positionen."""
    result = api_get("/api/v2/mix/position/all-position", {
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") != "00000":
        return []
    return [p for p in (result.get("data") or [])
            if float(p.get("total", 0)) > 0]


def get_recent_fills_all(since_ms: int) -> list:
    """Kürzlich ausgeführte Orders über alle Symbole."""
    result = api_get("/api/v2/mix/order/fill-history", {
        "productType": PRODUCT_TYPE,
        "startTime":   str(since_ms),
        "limit":       "50",
    })
    if result.get("code") != "00000":
        return []
    return (result.get("data") or {}).get("fillList") or []


# ═══════════════════════════════════════════════════════════════
# SL LESEN
# ═══════════════════════════════════════════════════════════════

def _get_plan_orders(symbol: str) -> list:
    """
    Liest alle offenen Plan-Orders (TPs, SL) für ein Symbol.

    Bitget v2 bietet zwei relevante Endpoints — beide werden versucht:
      1. orders-plan-pending  (mit / ohne marginCoin, mit / ohne symbol)
      2. tpsl-pending-orders  (als Fallback)

    Deckt ab: profit_plan (TP1-TP3), loss_plan / pos_loss (SL).
    """
    def _parse(raw) -> list:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("entrustedList", "planList", "orderList", "data"):
                lst = raw.get(key)
                if isinstance(lst, list) and lst:
                    return lst
            # leeres dict aber code 00000 → leere Liste ist korrekt
            return []
        return []

    # Versuch 1: orders-plan-pending mit symbol + marginCoin
    r = api_get("/api/v2/mix/order/orders-plan-pending", {
        "productType": PRODUCT_TYPE,
        "symbol":      symbol,
        "marginCoin":  MARGIN_COIN,
    })
    if r.get("code") == "00000":
        return _parse(r.get("data"))

    # Versuch 2: orders-plan-pending nur mit productType (dann symbol-Filter in Python)
    r2 = api_get("/api/v2/mix/order/orders-plan-pending", {
        "productType": PRODUCT_TYPE,
    })
    if r2.get("code") == "00000":
        all_orders = _parse(r2.get("data"))
        return [o for o in all_orders if o.get("symbol") == symbol]

    # Versuch 3: tpsl-pending-orders mit marginCoin
    r3 = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if r3.get("code") == "00000":
        return _parse(r3.get("data"))

    log(f"  [WARN] Alle Plan-Order Endpoints für {symbol} fehlgeschlagen: "
        f"1={r.get('msg','?')} | 2={r2.get('msg','?')} | 3={r3.get('msg','?')}")
    return []


def get_sl_price(symbol: str, direction: str) -> float:
    """
    Liest den SL-Preis für eine offene Position.

    Methode 1: stopLossPrice aus Positionsdaten (place-pos-tpsl Typ).
    Methode 2: loss_plan / pos_loss aus orders-plan-pending (plan-basierter SL).

    ACHTUNG: liquidationPrice wird NICHT als SL gewertet — das ist der
    Zwangsliquidierungspreis der Exchange, kein User-gesetzter Stop-Loss.
    """
    # Methode 1: SL aus Positionsdaten (position-level TP/SL via place-pos-tpsl)
    result = api_get("/api/v2/mix/position/single-position", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") == "00000":
        for pos in (result.get("data") or []):
            if pos.get("holdSide") == direction:
                for field in ("stopLossPrice", "stopLoss", "stopLossTriggerPrice",
                              "slPrice", "sl", "stopPrice"):
                    sl = float(pos.get(field, 0) or 0)
                    if sl > 0:
                        log(f"  SL aus Position.{field}: {sl}")
                        return sl

    # Methode 2: Plan-Orders lesen (plan-basierter SL via place-tpsl-order)
    orders = _get_plan_orders(symbol)
    SL_TYPES = {"loss_plan", "pos_loss", "moving_plan"}
    for o in orders:
        if o.get("planType") in SL_TYPES and o.get("holdSide") == direction:
            price = float(o.get("triggerPrice", 0) or 0)
            if price > 0:
                log(f"  SL aus plan-orders: {o.get('planType')} @ {price}")
                return price

    return 0.0


# ═══════════════════════════════════════════════════════════════
# TP-ORDERS VERWALTEN
# ═══════════════════════════════════════════════════════════════

def calc_optimal_leverage(entry: float, sl: float) -> int:
    """
    Optimaler Hebel: Hebel = 25 / SL-Abstand%
    Normiert den max. Verlust immer auf 25% des Kapitaleinsatzes.
    """
    if entry == 0 or sl == 0:
        return 10
    sl_dist_pct = abs(entry - sl) / entry * 100
    if sl_dist_pct == 0:
        return 10
    optimal = 25.0 / sl_dist_pct
    # Auf Bitget-gültige Werte clampen (1–125)
    return max(1, min(125, round(optimal)))


def calc_rr(entry: float, sl: float, leverage: int, direction: str) -> float:
    """
    R:R Ratio = TP4-Preisbewegung% / SL-Abstand%
    TP4 bei 40% ROI: Preisbewegung = 0.40 / Hebel
    """
    if entry == 0 or sl == 0:
        return 0.0
    sl_dist_pct = abs(entry - sl) / entry * 100
    tp4_move_pct = (TP4_ROI / leverage) * 100
    if sl_dist_pct == 0:
        return 0.0
    return round(tp4_move_pct / sl_dist_pct, 2)


def kelly_recommendation(balance: float, winrate: float) -> dict:
    """
    Kelly-Kriterium: optimale Positionsgrösse basierend auf Winrate.
    b = Gewinn/Verlust Ratio (25% Gewinn / 25% Verlust = 1.0 bei ausgeglichenem R:R)
    f* = (p*b - q) / b
    """
    avg_win  = sum(TP_CLOSE_PCTS[i] * [TP1_ROI, TP2_ROI, TP3_ROI, TP4_ROI][i]
                   for i in range(4)) * 100  # % des Einsatzes
    avg_loss = 25.0  # % bei SL
    b        = avg_win / avg_loss
    q        = 1 - winrate
    kelly    = (winrate * b - q) / b
    half_kelly = kelly / 2

    return {
        "kelly_pct":      max(0, round(kelly * 100, 1)),
        "half_kelly_pct": max(0, round(half_kelly * 100, 1)),
        "kelly_usdt":     max(0, round(balance * kelly, 2)),
        "half_kelly_usdt":max(0, round(balance * half_kelly, 2)),
        "avg_win_pct":    round(avg_win, 2),
        "b_ratio":        round(b, 2),
    }


def cancel_all_tp_orders(symbol: str):
    """
    Storniert alle TP-Orders (profit_plan) für ein Symbol.
    Liest via orders-plan-pending, storniert via cancel-plan-order.
    SL-Typen (loss_plan, pos_loss) werden nie angefasst.
    """
    orders = _get_plan_orders(symbol)
    tp_orders = [o for o in orders if o.get("planType") == "profit_plan"]

    if not tp_orders:
        log(f"  Keine TP-Orders gefunden")
        return

    log(f"  {len(tp_orders)} TP(s) stornieren...")
    for order in tp_orders:
        oid   = order.get("orderId")
        price = order.get("triggerPrice", "?")
        res   = api_post("/api/v2/mix/order/cancel-plan-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     oid,
        })
        if res.get("code") == "00000":
            log(f"    ✓ TP storniert @ {price}: {oid}")
        else:
            log(f"    ✗ Fehler: {res.get('msg', res)}")


def calc_tp_price(avg: float, roi: float,
                  direction: str, leverage: int) -> float:
    factor = roi / leverage
    return avg * (1 + factor) if direction == "long" else avg * (1 - factor)


def place_tp_orders(symbol: str, avg: float, size: float,
                    direction: str, leverage: int,
                    mark_price: float, known_sl: float = 0) -> tuple:
    """
    Setzt TP1–TP4 nach DOMINUS-Schema:
      TP1 (10% ROI): schliesst 15% der Position  → place-tpsl-order
      TP2 (20% ROI): schliesst 20% der Position  → place-tpsl-order
      TP3 (30% ROI): schliesst 25% der Position  → place-tpsl-order
      TP4 (40% ROI): Full Position Close         → place-pos-tpsl
                     Schliesst automatisch ALLES was noch offen ist.
                     (DOMINUS: "letzte TP schliesst Trade automatisch")
    """
    decimals = get_price_decimals(symbol)
    count    = 0
    prices   = []

    # ── TP1–TP3: Teilschliessungen ───────────────────────────────
    partial_tps = [
        (TP1_ROI, "TP1 (10%)", TP_CLOSE_PCTS[0]),
        (TP2_ROI, "TP2 (20%)", TP_CLOSE_PCTS[1]),
        (TP3_ROI, "TP3 (30%)", TP_CLOSE_PCTS[2]),
    ]

    partial_tps_skipped = 0  # zählt übersprungene Teilschliessungen (Position zu klein)

    for roi, label, pct in partial_tps:
        tp_raw = calc_tp_price(avg, roi, direction, leverage)
        tp_str = round_price(tp_raw, decimals)
        tp_val = float(tp_str)

        # Qty-Berechnung: ganzzahlige Kontrakte (>= 1.0) vs. Dezimal-Kontrakte (< 1.0)
        # max(1, floor()) war falsch für BTC: floor(0.0013 × 0.15)=0 → "1 BTC schliessen"
        if size >= 1.0:
            qty = math.floor(size * pct)
        else:
            qty = round(size * pct, 4)

        if qty <= 0:
            log(f"    ⏭ {label}: Position zu klein für Teilschliessung (size={size}, pct={pct}) — übersprungen")
            partial_tps_skipped += 1
            continue

        if mark_price > 0:
            if direction == "long"  and tp_val <= mark_price:
                log(f"    ⏭ {label} @ {tp_str} bereits überschritten — übersprungen")
                continue
            if direction == "short" and tp_val >= mark_price:
                log(f"    ⏭ {label} @ {tp_str} bereits überschritten — übersprungen")
                continue

        res = api_post("/api/v2/mix/order/place-tpsl-order", {
            "symbol":       symbol,
            "productType":  PRODUCT_TYPE,
            "marginCoin":   MARGIN_COIN,
            "planType":     "profit_plan",
            "triggerPrice": tp_str,
            "triggerType":  "mark_price",
            "executePrice": "0",
            "holdSide":     direction,
            "size":         str(qty),
        })
        if res.get("code") == "00000":
            log(f"    ✓ {label} @ {tp_str} USDT (Qty: {qty})")
            count += 1
            prices.append(f"{label}: {tp_str}")
        else:
            msg = res.get("msg", str(res))
            log(f"    ✗ {label} FEHLER: {msg}")
            if "checkBDScale" in msg or "checkScale" in msg:
                new_dec = max(1, decimals - 1)
                price_decimals_cache[symbol] = new_dec
                tp_str2 = round_price(tp_raw, new_dec)
                res2 = api_post("/api/v2/mix/order/place-tpsl-order", {
                    "symbol":       symbol,
                    "productType":  PRODUCT_TYPE,
                    "marginCoin":   MARGIN_COIN,
                    "planType":     "profit_plan",
                    "triggerPrice": tp_str2,
                    "triggerType":  "mark_price",
                    "executePrice": "0",
                    "holdSide":     direction,
                    "size":         str(qty),
                })
                if res2.get("code") == "00000":
                    log(f"    ✓ {label} @ {tp_str2} USDT [retry OK]")
                    count += 1
                    prices.append(f"{label}: {tp_str2}")

    # Warnung wenn Teilschliessungen wegen zu kleiner Position nicht setzbar waren
    if partial_tps_skipped > 0:
        warn_msg = (
            f"⚠️ {symbol}: {partial_tps_skipped}/3 Teilschliessung(en) konnten nicht gesetzt werden "
            f"(Position {size} Kontrakte zu klein für Teilschliessung).\n"
            f"TP1–SL-Mechanismus greift nicht automatisch — SL manuell überwachen!"
        )
        log(warn_msg)
        send_telegram(warn_msg)

    # ── TP4: Full Position Close via place-pos-tpsl ──────────────
    # WICHTIG: place-pos-tpsl verwaltet NUR EINEN kombinierten
    # Eintrag pro Position. TP4 und SL MÜSSEN zusammen gesetzt werden,
    # sonst überschreibt TP4 den bestehenden SL (oder umgekehrt).
    tp4_raw = calc_tp_price(avg, TP4_ROI, direction, leverage)
    tp4_str = round_price(tp4_raw, decimals)
    tp4_val = float(tp4_str)

    tp4_skip = (direction == "long"  and mark_price > 0 and tp4_val <= mark_price) or                (direction == "short" and mark_price > 0 and tp4_val >= mark_price)

    if tp4_skip:
        log(f"    ⏭ TP4 (40%) @ {tp4_str} bereits überschritten — übersprungen")
    else:
        # Aktuellen SL-Preis lesen um ihn mitzuschicken.
        # Fallback-Kette: API → übergebener known_sl → trade_data
        current_sl = get_sl_price(symbol, direction)
        if current_sl == 0 and known_sl > 0:
            current_sl = known_sl
            log(f"    SL aus Trade-Setup als Fallback: {current_sl}")
        if current_sl == 0 and symbol in trade_data:
            current_sl = trade_data[symbol].get("sl", 0)
            if current_sl > 0:
                log(f"    SL aus Trade-Daten als Fallback: {current_sl}")
        if current_sl == 0:
            log(f"    ⚠ Kein SL ermittelbar — TP4 wird ohne SL gesetzt "
                f"(bestehender Bitget-SL bleibt erhalten wenn API das unterstützt)")
        sl_for_tp4 = round_price(current_sl, decimals) if current_sl > 0 else "0"

        body4 = {
            "symbol":                 symbol,
            "productType":            PRODUCT_TYPE,
            "marginCoin":             MARGIN_COIN,
            "holdSide":               direction,
            "takeProfitTriggerPrice": tp4_str,
            "takeProfitTriggerType":  "mark_price",
        }
        # SL mitschicken wenn vorhanden — verhindert Überschreiben
        if current_sl > 0:
            body4["stopLossTriggerPrice"] = sl_for_tp4
            body4["stopLossTriggerType"]  = "mark_price"
            log(f"    TP4 kombiniert mit SL @ {sl_for_tp4}")

        res4 = api_post("/api/v2/mix/order/place-pos-tpsl", body4)
        if res4.get("code") == "00000":
            log(f"    ✓ TP4 Full Close @ {tp4_str} USDT "
                f"(schliesst gesamte Restposition)")
            count += 1
            prices.append(f"TP4 Full Close: {tp4_str}")
        else:
            msg4 = res4.get("msg", str(res4))
            log(f"    ✗ TP4 FEHLER: {msg4}")
            if "checkBDScale" in msg4 or "checkScale" in msg4:
                new_dec   = max(1, decimals - 1)
                price_decimals_cache[symbol] = new_dec
                tp4_str2  = round_price(tp4_raw, new_dec)
                body4b    = {
                    "symbol":                 symbol,
                    "productType":            PRODUCT_TYPE,
                    "marginCoin":             MARGIN_COIN,
                    "holdSide":               direction,
                    "takeProfitTriggerPrice": tp4_str2,
                    "takeProfitTriggerType":  "mark_price",
                }
                if current_sl > 0:
                    body4b["stopLossTriggerPrice"] = sl_for_tp4
                    body4b["stopLossTriggerType"]  = "mark_price"
                res4b = api_post("/api/v2/mix/order/place-pos-tpsl", body4b)
                if res4b.get("code") == "00000":
                    log(f"    ✓ TP4 Full Close @ {tp4_str2} USDT [retry OK]")
                    count += 1
                    prices.append(f"TP4 Full Close: {tp4_str2}")

    return count, prices


# ═══════════════════════════════════════════════════════════════
# SL AUF ENTRY SETZEN (nach TP1)
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# DCA ORDERS STORNIEREN (nach TP1)
# ═══════════════════════════════════════════════════════════════

def cancel_open_dca_orders(symbol: str, direction: str):
    """
    Storniert alle noch offenen DCA Limit-Orders nach TP1.
    Nach SL auf Entry sind DCA-Nachkäufe nicht mehr gewünscht.
    """
    result = api_get("/api/v2/mix/order/orders-pending", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result.get("code") != "00000":
        log(f"  ⚠ DCA-Abruf Fehler: {result.get('msg', result)}")
        return

    data   = result.get("data") or {}
    orders = data.get("entrustedList") or [] if isinstance(data, dict) else data

    # Nur Open-Orders in Richtung des Trades (DCA = gleiche Seite wie Entry)
    side = "buy" if direction == "long" else "sell"
    dca_orders = [
        o for o in orders
        if o.get("side") == side
        and o.get("tradeSide") == "open"
        and o.get("orderType") == "limit"
    ]

    if not dca_orders:
        log(f"  Keine offenen DCA-Orders gefunden")
        return

    log(f"  {len(dca_orders)} DCA-Order(s) stornieren...")
    for order in dca_orders:
        oid = order.get("orderId")
        price = order.get("price", "?")
        res = api_post("/api/v2/mix/order/cancel-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "orderId":     oid,
        })
        if res.get("code") == "00000":
            log(f"    ✓ DCA storniert @ {price} USDT: {oid}")
        else:
            log(f"    ✗ DCA Stornierung fehlgeschlagen: "
                f"{res.get('msg', res)}")


def get_existing_dca_orders(symbol: str, direction: str) -> list:
    """
    Gibt alle offenen DCA Limit-Orders für ein Symbol/Richtung zurück.
    Gleiche Filterlogik wie cancel_open_dca_orders, aber ohne Stornierung.
    """
    result = api_get("/api/v2/mix/order/orders-pending", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result.get("code") != "00000":
        return []
    data   = result.get("data") or {}
    orders = data.get("entrustedList") or [] if isinstance(data, dict) else data
    side   = "buy" if direction == "long" else "sell"
    return [
        o for o in orders
        if o.get("side") == side
        and o.get("tradeSide") == "open"
        and o.get("orderType") == "limit"
    ]


def get_closed_pnl(symbol: str, since_ms: int) -> dict:
    """
    Berechnet realisierten P&L eines vollständigen Trades via Fill-History.
    Summiert ALLE Schliessungen (TP1, TP2, ... + SL) seit Trade-Eröffnung.
    So wird der echte Gesamt-P&L korrekt erfasst — auch bei Teiltrades.
    """
    result = api_get("/api/v2/mix/order/fill-history", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "startTime":   str(since_ms),
        "limit":       "50",
    })
    if result.get("code") != "00000":
        return {}

    fills = (result.get("data") or {}).get("fillList") or []
    # Nur Close-Fills (tradeSide=close)
    close_fills = [f for f in fills if f.get("tradeSide") == "close"]
    if not close_fills:
        return {}

    total_pnl   = sum(float(f.get("profit", 0) or 0) for f in close_fills)
    total_fee   = sum(float(f.get("fee", 0) or 0) for f in close_fills)
    total_size  = sum(float(f.get("size", 0) or 0) for f in close_fills)
    net_pnl     = total_pnl - abs(total_fee)

    # Durchschnittlichen Schlusskurs berechnen
    prices = [float(f.get("price", 0) or 0) for f in close_fills if float(f.get("price",0))>0]
    avg_close = sum(prices) / len(prices) if prices else 0

    # Einzelne Schliessungen für Detail-Bericht
    tp_closes = []
    for f in close_fills:
        pnl_f = float(f.get("profit", 0) or 0)
        size_f = float(f.get("size", 0) or 0)
        price_f = float(f.get("price", 0) or 0)
        tp_closes.append({
            "size": size_f,
            "price": price_f,
            "pnl": pnl_f,
        })

    return {
        "realized_pnl": total_pnl,
        "fee":          abs(total_fee),
        "net_pnl":      net_pnl,
        "close_price":  avg_close,
        "total_size":   total_size,
        "tp_closes":    tp_closes,
        "num_closes":   len(close_fills),
    }


# ═══════════════════════════════════════════════════════════════
# POSITION GESCHLOSSEN ERKENNEN
# ═══════════════════════════════════════════════════════════════

def handle_position_closed(symbol: str, reason: str = ""):
    """
    Wird aufgerufen wenn eine Position vollständig geschlossen wurde.
    Berechnet P&L, sendet Auswertung per Telegram, bereinigt Status.
    """
    log(f"Position geschlossen: {symbol} ({reason})")

    # Trade-Daten für Auswertung holen
    td       = trade_data.get(symbol, {})
    entry    = td.get("entry", last_known_avg.get(symbol, 0))
    direction = td.get("direction", "?")
    leverage  = td.get("leverage", 10)
    sl_price  = td.get("sl", 0)
    open_ts   = td.get("open_ts", int(time.time() * 1000))
    peak_size = td.get("peak_size", last_known_size.get(symbol, 0))

    # Realisierten P&L von Bitget holen
    pnl_data = get_closed_pnl(symbol, open_ts)
    net_pnl  = pnl_data.get("net_pnl", 0)
    realized = pnl_data.get("realized_pnl", 0)
    fee      = pnl_data.get("fee", 0)
    close_px = pnl_data.get("close_price", 0)
    hold_h   = pnl_data.get("hold_time", "")

    # Gewinn/Verlust bestimmen
    won = net_pnl > 0
    icon = "🏆" if won else "🔴"
    result_label = "GEWINN" if won else "VERLUST"

    # ROI berechnen (auf eingesetztes Kapital)
    margin = (entry * peak_size / leverage) if entry and leverage else 0
    roi_pct = (net_pnl / margin * 100) if margin > 0 else 0

    # Haltedauer formatieren
    try:
        hold_ms = int(hold_h) if hold_h else 0
        hold_h_num = hold_ms // 3600000
        hold_m_num = (hold_ms % 3600000) // 60000
        hold_str = f"{hold_h_num}h {hold_m_num}m" if hold_ms else "?"
    except Exception:
        hold_str = str(hold_h) if hold_h else "?"

    log(f"  P&L: {net_pnl:+.2f} USDT | ROI: {roi_pct:+.1f}% | "
        f"Fee: {fee:.2f} USDT")

    # Detail-Schliessungen aus P&L-Daten
    tp_closes  = pnl_data.get("tp_closes", [])
    num_closes = pnl_data.get("num_closes", 0)

    msg_lines = [
        f"{icon} <b>Trade abgeschlossen — {symbol}</b>",
        f"━━━━━━━━━━━━",
        f"Ergebnis: <b>{result_label}</b>",
        f"",
        f"📊 <b>Auswertung:</b>",
        f"Netto P&L:   {net_pnl:+.2f} USDT",
        f"ROI:         {roi_pct:+.1f}% auf Margin",
        f"Realisiert:  {realized:+.2f} USDT",
        f"Gebühren:    {fee:.2f} USDT",
        f"Schliessungen: {num_closes}x",
    ]

    # Einzelne Schliessungen auflisten (TP1, TP2,... + SL)
    if tp_closes:
        msg_lines.append(f"")
        msg_lines.append(f"📋 <b>Einzelne Schliessungen:</b>")
        for i, c in enumerate(tp_closes):
            label = f"TP{i+1}" if i < num_closes - 1 else "SL/Close"
            sign  = "+" if c["pnl"] >= 0 else ""
            msg_lines.append(
                f"  {label}: {c['size']:.1f} Ktr @ {c['price']:.5f}"
                f" → {sign}{c['pnl']:.2f} USDT"
            )

    msg_lines += [
        f"",
        f"📋 <b>Trade-Details:</b>",
        f"Richtung: {dir_icon(direction)} {direction.upper()}  |  {leverage}x Hebel",
        f"Entry: {entry}",
    ]
    if close_px:
        msg_lines.append(f"Avg Close: {close_px:.5f}")
    if sl_price:
        msg_lines.append(f"SL war: {sl_price}")
    msg_lines += [
        f"",
        f"Status bereinigt ✓",
        f"/berechnen für neuen Trade",
    ]

    telegram("\n".join(msg_lines))

    # Trade für Daily Report aufzeichnen
    closed_trades.append({
        "symbol":       symbol,
        "direction":    direction,
        "leverage":     leverage,
        "entry":        entry,
        "close_price":  close_px,
        "net_pnl":      net_pnl,
        "realized_pnl": realized,
        "fee":          fee,
        "peak_size":    peak_size,
        "hold_str":     hold_str,
        "ts":           int(time.time()),
        "trailing_level": trailing_sl_level.get(symbol, 0),
        "won":          won,
    })
    save_state()

    # Internen Status zurücksetzen
    last_known_avg.pop(symbol, None)
    last_known_size.pop(symbol, None)
    new_trade_done.pop(symbol, None)
    sl_at_entry.pop(symbol, None)
    harsi_sl.pop(symbol, None)
    trailing_sl_level.pop(symbol, None)
    trade_data.pop(symbol, None)

    # Noch offene TP-Orders aufräumen (Sicherheit)
    cancel_all_tp_orders(symbol)


def _get_pos_tp_price(symbol: str, direction: str) -> float:
    """
    Liest den TP4-Preis für eine Position — zwei Wege:
    Methode 1: takeProfitPrice aus Positionsdaten (place-pos-tpsl).
    Methode 2: pos_profit aus orders-plan-pending (Bitget-UI "SL/TP"-Button).
    """
    result = api_get("/api/v2/mix/position/single-position", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") == "00000":
        for pos in (result.get("data") or []):
            if pos.get("holdSide") == direction:
                for field in ("takeProfitPrice", "takeProfit", "tpPrice",
                              "takeProfitTriggerPrice"):
                    tp = float(pos.get(field, 0) or 0)
                    if tp > 0:
                        log(f"  TP4 aus Position.{field}: {tp}")
                        return tp

    orders = _get_plan_orders(symbol)
    for o in orders:
        if o.get("planType") == "pos_profit":
            hold = o.get("holdSide", direction)
            if hold != direction:
                continue
            price = float(o.get("triggerPrice", 0) or 0)
            if price > 0:
                log(f"  TP4 aus plan-orders (pos_profit): {price}")
                return price

    return 0.0


def set_sl_at_entry(symbol: str, direction: str, entry_price: float,
                    cur_size: float = 0):
    """SL auf Einstiegspreis setzen — DOMINUS-Regel nach TP1."""
    decimals = get_price_decimals(symbol)
    sl_str   = round_price(entry_price, decimals)

    # Vorhandenen TP4 lesen — muss beim SL-Update mitgeschickt werden
    # (place-pos-tpsl überschreibt sonst den TP4)
    existing_tp4 = _get_pos_tp_price(symbol, direction)
    body_sl = {
        "symbol":               symbol,
        "productType":          PRODUCT_TYPE,
        "marginCoin":           MARGIN_COIN,
        "holdSide":             direction,
        "stopLossTriggerPrice": sl_str,
        "stopLossTriggerType":  "mark_price",
    }
    if existing_tp4 > 0:
        decimals_sl = get_price_decimals(symbol)
        body_sl["takeProfitTriggerPrice"] = round_price(existing_tp4, decimals_sl)
        body_sl["takeProfitTriggerType"]  = "mark_price"
        log(f"  TP4 @ {existing_tp4} wird mitgeführt")

    result = api_post("/api/v2/mix/order/place-pos-tpsl", body_sl)

    if result.get("code") == "00000":
        log(f"  ✓ SL auf Entry gesetzt: {sl_str} USDT ({symbol})")
        sl_at_entry[symbol] = True
        save_state()
        cancel_open_dca_orders(symbol, direction)
        td         = trade_data.get(symbol, {})
        leverage   = int(td.get("leverage", 20))
        peak_size  = td.get("peak_size", 0)
        tp1_price  = calc_tp_price(entry_price, TP1_ROI, direction, leverage)
        closed_qty = max(0, peak_size - cur_size) if cur_size > 0 and peak_size > 0 else 0
        tp1_profit = closed_qty * abs(tp1_price - entry_price) if closed_qty > 0 else 0
        size_str   = f"{cur_size:.2f}" if cur_size > 0 else "—"
        profit_str = f"+{tp1_profit:.2f} USDT" if tp1_profit > 0 else "—"
        telegram(
            f"🔒 <b>TP1 ausgelöst — {symbol}</b>\n"
            f"SL → Entry @ {sl_str} USDT (Break-even gesichert)\n"
            f"━━━━━━━━━━\n"
            f"💰 Realisiert:      {profit_str}\n"
            f"📦 Restposition:    {size_str} Kontrakte\n"
            f"🛡 Min. Gewinn Rest: 0 USDT (Break-even)\n"
            f"✓ DCA-Orders storniert"
        )
    else:
        log(f"  ✗ SL-Anpassung fehlgeschlagen: {result.get('msg', result)}")


sl_set_ts: dict = {}  # {symbol: float} — Timestamp der letzten SL-Setzung


def _get_pos_sl_price(symbol: str, direction: str) -> float:
    """
    Liest den aktuellen SL-Preis aus Positionsdaten oder Plan-Orders.
    Alias für get_sl_price — wird von set_sl_harsi() verwendet.
    """
    return get_sl_price(symbol, direction)


def set_sl_trailing(symbol: str, direction: str, sl_price: float, level: int,
                    cur_size: float = 0):
    """
    TP-Step Trailing: SL auf den vorherigen TP-Preis nachziehen.
    TP2 ausgelöst → SL auf TP1-Preis  (level=2)
    TP3 ausgelöst → SL auf TP2-Preis  (level=3)
    """
    if trailing_sl_level.get(symbol, 0) >= level:
        log(f"  Trailing Level {level} bereits gesetzt — skip")
        return

    decimals     = get_price_decimals(symbol)
    sl_str       = round_price(sl_price, decimals)
    existing_tp4 = _get_pos_tp_price(symbol, direction)

    body_sl = {
        "symbol":               symbol,
        "productType":          PRODUCT_TYPE,
        "marginCoin":           MARGIN_COIN,
        "holdSide":             direction,
        "stopLossTriggerPrice": sl_str,
        "stopLossTriggerType":  "mark_price",
    }
    if existing_tp4 > 0:
        body_sl["takeProfitTriggerPrice"] = round_price(existing_tp4, decimals)
        body_sl["takeProfitTriggerType"]  = "mark_price"
        log(f"  TP4 @ {existing_tp4} wird mitgeführt")

    tp_label   = {2: "TP2", 3: "TP3"}.get(level, "?")
    prev_label = {2: "TP1 (10% ROI)", 3: "TP2 (20% ROI)"}.get(level, "?")

    result = api_post("/api/v2/mix/order/place-pos-tpsl", body_sl)
    if result.get("code") == "00000":
        trailing_sl_level[symbol] = level
        sl_set_ts[symbol]         = time.time()
        sl_at_entry[symbol]       = True
        save_state()

        td    = trade_data.get(symbol, {})
        entry = float(td.get("entry", 0))
        if cur_size > 0 and entry > 0:
            guaranteed = (cur_size * (sl_price - entry) if direction == "long"
                          else cur_size * (entry - sl_price))
            size_str       = f"{cur_size:.2f} Kontrakte"
            guaranteed_str = (f"+{guaranteed:.2f} USDT" if guaranteed > 0
                              else f"{guaranteed:.2f} USDT")
        else:
            size_str       = "—"
            guaranteed_str = "—"

        log(f"  ✓ Trailing SL (Level {level}): SL → {sl_str} USDT")
        telegram(
            f"📈 <b>{tp_label} ausgelöst — {symbol}</b>\n"
            f"SL → {sl_str} USDT ({prev_label} gesichert)\n"
            f"━━━━━━━━━━\n"
            f"📦 Restposition:     {size_str}\n"
            f"🛡 Min. Gewinn Rest: {guaranteed_str}\n"
            f"  (falls SL greift bevor TP{level + 1})"
        )
    else:
        log(f"  ✗ Trailing SL Level {level} fehlgeschlagen: {result.get('msg', result)}")


def set_sl_harsi(symbol: str, direction: str, harsi_price: float, cur_size: float = 0):
    """
    Setzt den SL auf die Harsi-Ausstiegslinie — nur wenn schützender als aktueller SL.
    Long:  harsi_price > current_sl  → SL nachziehen
    Short: harsi_price < current_sl  → SL nachziehen
    """
    decimals   = get_price_decimals(symbol)
    sl_str     = round_price(harsi_price, decimals)
    current_sl = _get_pos_sl_price(symbol, direction)

    if current_sl > 0:
        if direction == "long"  and harsi_price <= current_sl:
            log(f"  Harsi-SL {harsi_price:.5f} nicht besser als SL {current_sl:.5f} — skip")
            return
        if direction == "short" and harsi_price >= current_sl:
            log(f"  Harsi-SL {harsi_price:.5f} nicht besser als SL {current_sl:.5f} — skip")
            return

    existing_tp4 = _get_pos_tp_price(symbol, direction)
    body_sl = {
        "symbol":               symbol,
        "productType":          PRODUCT_TYPE,
        "marginCoin":           MARGIN_COIN,
        "holdSide":             direction,
        "stopLossTriggerPrice": sl_str,
        "stopLossTriggerType":  "mark_price",
    }
    if existing_tp4 > 0:
        body_sl["takeProfitTriggerPrice"] = round_price(existing_tp4, decimals)
        body_sl["takeProfitTriggerType"]  = "mark_price"

    result = api_post("/api/v2/mix/order/place-pos-tpsl", body_sl)
    if result.get("code") == "00000":
        harsi_sl[symbol]  = harsi_price
        sl_set_ts[symbol] = time.time()
        save_state()

        td    = trade_data.get(symbol, {})
        entry = float(td.get("entry", 0))
        if cur_size > 0 and entry > 0:
            guaranteed = (cur_size * (harsi_price - entry) if direction == "long"
                          else cur_size * (entry - harsi_price))
            size_str       = f"{cur_size:.2f} Kontrakte"
            guaranteed_str = (f"+{guaranteed:.2f} USDT" if guaranteed >= 0
                              else f"{guaranteed:.2f} USDT")
        else:
            size_str       = "—"
            guaranteed_str = "—"

        prev_sl_str = f"{current_sl:.5f}" if current_sl > 0 else "—"
        log(f"  ✓ Harsi-SL gesetzt: {sl_str} USDT ({symbol})")
        telegram(
            f"📉 <b>Harsi-Ausstiegslinie — {symbol}</b>\n"
            f"SL → {sl_str} USDT  (vorher: {prev_sl_str})\n"
            f"━━━━━━━━━━\n"
            f"📦 Restposition:     {size_str}\n"
            f"🛡 Min. Gewinn:      {guaranteed_str}\n"
            f"⚠️ Harsi-Momentum dreht — SL auf Ausstiegslinie gesetzt"
        )
    else:
        log(f"  ✗ Harsi-SL fehlgeschlagen: {result.get('msg', result)}")
        telegram(f"❌ <b>Harsi-SL fehlgeschlagen — {symbol}</b>\nBitte SL manuell prüfen!")


# ═══════════════════════════════════════════════════════════════
# DCA LIMIT-ORDERS SETZEN
# ═══════════════════════════════════════════════════════════════

def place_dca_orders(symbol: str, entry: float, sl: float,
                     direction: str, base_size: float,
                     balance: float = 0, leverage: int = 10) -> list:
    """
    Setzt 2 DCA Limit-Orders — immer progressives Sizing 20/30/50.
    Basis: Initial-Trade (base_size = Market-Order Grösse)
      DCA1 = base_size × 1.5  (entspricht 30% bei 20% Initial)
      DCA2 = base_size × 2.5  (entspricht 50% bei 20% Initial)
    Beispiel: Initial = 10 → DCA1 = 15 → DCA2 = 25 → Total = 50
    """
    decimals  = get_price_decimals(symbol)
    sl_dist   = abs(entry - sl)

    if direction == "long":
        dca1 = entry - sl_dist * DCA1_RATIO
        dca2 = entry - sl_dist * DCA2_RATIO
        side = "buy"
    else:
        dca1 = entry + sl_dist * DCA1_RATIO
        dca2 = entry + sl_dist * DCA2_RATIO
        side = "sell"

    dca1_str  = round_price(dca1, decimals)
    dca2_str  = round_price(dca2, decimals)
    dca1_size = round(base_size * DCA1_MULTIPLIER, 4)
    dca2_size = round(base_size * DCA2_MULTIPLIER, 4)

    log(f"  DCA Sizing 20/30/50: "
        f"Market={base_size} | DCA1={dca1_size} | DCA2={dca2_size}")

    def fmt(s):
        return str(int(s)) if s == int(s) else str(round(s, 4))

    results = []
    for label, price_str, qty in [
        ("DCA1", dca1_str, dca1_size),
        ("DCA2", dca2_str, dca2_size),
    ]:
        res = api_post("/api/v2/mix/order/place-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginMode":  "isolated",
            "marginCoin":  MARGIN_COIN,
            "size":        fmt(qty),
            "price":       price_str,
            "side":        side,
            "tradeSide":   "open",
            "orderType":   "limit",
            "force":       "gtc",
        })
        if res.get("code") == "00000":
            log(f"  ✓ {label} Limit @ {price_str} USDT (Qty: {fmt(qty)})")
            results.append(f"{label}: {price_str} USDT × {fmt(qty)}")
        else:
            log(f"  ✗ {label} FEHLER: {res.get('msg', res)}")

    return results

def tv_chart_links(symbol: str) -> dict:
    """
    Generiert TradingView Chart-Links mit gespeichertem Layout lX5eDAis.
    Symbol wird automatisch getauscht, Timeframe angepasst.
    """
    tv_sym = symbol.upper()
    if not tv_sym.endswith(".P"):
        tv_sym = tv_sym + ".P"

    base = "https://www.tradingview.com/chart/lX5eDAis"
    return {
        "coin_h2":  f"{base}/?symbol=BITGET:{tv_sym}&interval=120",
        "coin_h4":  f"{base}/?symbol=BITGET:{tv_sym}&interval=240",
        "btc_h2":   f"{base}/?symbol=BITGET:BTCUSDT.P&interval=120",
        "total2":   f"{base}/?symbol=CRYPTOCAP:TOTAL2&interval=120",
    }


def setup_new_trade(pos: dict):
    """
    Vollständiges Setup für einen neuen Trade:
    1. SL aus pending Orders lesen
    2. DCA1 + DCA2 Limit-Orders setzen
    3. TP1–TP4 setzen
    4. Telegram-Zusammenfassung senden
    """
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    entry     = float(pos.get("openPriceAvg", 0))
    size      = float(pos.get("total", 0))
    leverage  = int(float(pos.get("leverage", 10)))
    mark      = get_mark_price(symbol)

    log(f"══ NEUER TRADE: {symbol} ══")
    log(f"  {direction.upper()} | Entry={entry} | "
        f"Qty={size} | Hebel={leverage}x | Mark={mark}")

    if entry == 0 or size == 0:
        log("  Ungültige Position. Überspringe.")
        return

    # ── 1. Kontostand & Kelly ────────────────────────────────
    balance      = get_futures_balance()
    max_margin   = balance * 0.10
    order_margin = (size * entry) / leverage
    kelly        = kelly_recommendation(balance, WINRATE)
    log(f"  Kontostand: {balance:.2f} USDT | "
        f"10%-Limit: {max_margin:.2f} USDT | "
        f"Margin/Order: {order_margin:.2f} USDT")
    log(f"  Kelly ({WINRATE*100:.0f}% Winrate): "
        f"{kelly['kelly_pct']}% = {kelly['kelly_usdt']} USDT | "
        f"Half-Kelly: {kelly['half_kelly_pct']}% = {kelly['half_kelly_usdt']} USDT")

    # Warnung wenn Initial-Order > 1/3 des 10%-Limits
    if order_margin > max_margin * 0.37 and balance > 0:
        log(f"  ⚠ Initial-Order nutzt {order_margin/balance*100:.1f}% "
            f"des Kapitals — prüfe ob DCA noch ins 10%-Limit passt")

    # ── 2. SL aus pending Orders lesen ──────────────────────
    time.sleep(1)  # kurz warten bis Order-Book aktualisiert
    sl_price = get_sl_price(symbol, direction)

    if sl_price == 0:
        # ── Kein SL gesetzt → automatisch auf -25% berechnen ─────────
        # Long:  SL = entry * (1 - 0.25 / leverage)
        # Short: SL = entry * (1 + 0.25 / leverage)
        factor   = 0.25 / leverage
        sl_auto  = entry * (1 - factor) if direction == "long"                    else entry * (1 + factor)
        decimals = get_price_decimals(symbol)
        sl_str   = round_price(sl_auto, decimals)
        sl_auto  = float(sl_str)
        sl_dist  = abs(entry - sl_auto) / entry * 100

        log(f"  ⚠ Kein SL gefunden — Auto-SL bei -25%: {sl_str} USDT")

        res = api_post("/api/v2/mix/order/place-pos-tpsl", {
            "symbol":               symbol,
            "productType":          PRODUCT_TYPE,
            "marginCoin":           MARGIN_COIN,
            "holdSide":             direction,
            "stopLossTriggerPrice": sl_str,
            "stopLossTriggerType":  "mark_price",
        })

        links = tv_chart_links(symbol)
        if res.get("code") == "00000":
            log(f"  ✓ Auto-SL gesetzt @ {sl_str} (-25% Schutz)")
            telegram(
                f"\U0001f6e1 <b>Auto-SL gesetzt \u2014 {symbol}</b>\n"
                f"Kein SL gefunden \u2192 -25% Schutz aktiviert\n\n"
                f"SL: {sl_str} USDT ({sl_dist:.1f}% Abstand)\n"
                f"Hebel: {leverage}x | Entry: {entry}\n\n"
                f"\u26a0\ufe0f Bitte pr\u00fcfen ob SL mit deiner\n"
                f"Ausstiegslinie \u00fcbereinstimmt!\n\n"
                f"H4 {symbol}: {links['coin_h4']}\n"
                f"H2 {symbol}: {links['coin_h2']}"
            )
            sl_price = sl_auto
        else:
            log(f"  ✗ Auto-SL fehlgeschlagen: {res.get('msg', res)}")
            telegram(
                f"\u274c <b>Kein SL \u2014 {symbol}</b>\n"
                f"Auto-SL fehlgeschlagen. Bitte manuell setzen!\n"
                f"Empfehlung: {sl_str} USDT ({sl_dist:.1f}%)\n\n"
                f"H4: {links['coin_h4']}"
            )
            cancel_all_tp_orders(symbol)
            time.sleep(1)
            place_tp_orders(symbol, entry, size, direction, leverage, mark,
                            known_sl=sl_price)
            last_known_avg[symbol]  = entry
            last_known_size[symbol] = size
            new_trade_done[symbol]  = True
            return

    # ── Hebel-Empfehlung & R:R-Check ─────────────────────
    sl_dist_pct   = abs(entry - sl_price) / entry * 100
    optimal_lev   = calc_optimal_leverage(entry, sl_price)
    rr            = calc_rr(entry, sl_price, leverage, direction)
    rr_warn       = rr < MIN_RR
    lev_diff      = abs(leverage - optimal_lev)

    log(f"  SL: {sl_price} USDT | Abstand: {sl_dist_pct:.2f}%")
    log(f"  Empfohlener Hebel: {optimal_lev}x (aktuell: {leverage}x"
        + (f" ⚠ Abweichung {lev_diff}x" if lev_diff > 2 else " ✓") + ")")
    log(f"  R:R Ratio: {rr}"
        + (" ⚠ UNTER MINIMUM!" if rr_warn else " ✓"))

    if rr_warn:
        log(f"  ⚠ R:R {rr} < Minimum {MIN_RR} — Trade gemäss Filter nicht empfohlen!")
        telegram(
            "\u26a0\ufe0f <b>R:R-Warnung \u2014 " + symbol + "</b>\n"
            "R:R = " + str(rr) + " liegt unter dem Minimum von " + str(MIN_RR) + "\n"
            "Setup wird trotzdem aufgebaut \u2014 bitte manuell pr\u00fcfen."
        )

    # Validierung: SL auf richtiger Seite
    if direction == "long" and sl_price >= entry:
        log(f"  ✗ SL ({sl_price}) liegt über Entry ({entry}) bei Long!")
    elif direction == "short" and sl_price <= entry:
        log(f"  ✗ SL ({sl_price}) liegt unter Entry ({entry}) bei Short!")

    # ── 3. DCA Limit-Orders setzen ──────────────────────────
    log(f"  Setze DCA-Orders...")
    dca_results = place_dca_orders(
        symbol, entry, sl_price, direction, size,
        balance=balance, leverage=leverage
    )

    # ── 4. TP1–TP4 setzen ───────────────────────────────────
    # TPs auf Basis der tatsächlichen aktuellen Position setzen.
    # DCA-Orders sind noch nicht gefüllt — wird automatisch angepasst wenn sie füllen.
    log(f"  Setze TPs für aktuelle Position (Qty={size})...")
    cancel_all_tp_orders(symbol)
    time.sleep(1)
    count, tp_prices = place_tp_orders(
        symbol, entry, size, direction, leverage, mark, known_sl=sl_price
    )

    # ── 5. Status speichern ─────────────────────────────────
    last_known_avg[symbol]  = entry
    last_known_size[symbol] = size
    new_trade_done[symbol]  = True
    sl_at_entry[symbol]     = False
    trailing_sl_level[symbol] = 0
    harsi_sl.pop(symbol, None)
    # Trade-Daten für spätere Auswertung
    trade_data[symbol] = {
        "entry":     entry,
        "direction": direction,
        "leverage":  leverage,
        "sl":        sl_price,
        "peak_size": size,
        "open_ts":   int(time.time() * 1000),
    }

    # ── 6. Telegram-Zusammenfassung ─────────────────────────
    dca1_str = dca_results[0] if len(dca_results) > 0 else "Fehler"
    dca2_str = dca_results[1] if len(dca_results) > 1 else "Fehler"

    rr_icon  = "⚠️" if rr_warn else "✅"
    lev_icon = "⚠️" if lev_diff > 2 else "✅"
    msg = (
        f"🚀 {dir_icon(direction)} <b>Neuer Trade — {symbol}</b>\n"
        f"━━━━━━━━━━━━\n"
        f"Richtung: {dir_icon(direction)} {direction.upper()} | Hebel: {leverage}x\n"
        f"Entry: {entry} USDT | SL: {sl_price} USDT\n\n"
        f"📐 <b>Analyse:</b>\n"
        f"{rr_icon} R:R Ratio: {rr} (Min: {MIN_RR})\n"
        f"{lev_icon} Empf. Hebel: {optimal_lev}x (aktuell: {leverage}x)\n"
        f"📊 Kelly {WINRATE*100:.0f}%: {kelly['kelly_pct']}% | "
        f"Half-Kelly: {kelly['half_kelly_pct']}%\n\n"
        f"📦 <b>Orders gesetzt:</b>\n"
        f"Market:  {entry} USDT × {size}\n"
        f"{dca1_str} × {size}\n"
        f"{dca2_str} × {size}\n\n"
        f"🎯 <b>Take-Profits (15/20/25/40%):</b>\n"
        + "\n".join(tp_prices) + "\n\n"
        f"💰 Margin/Order ≈ {order_margin:.2f} USDT\n"
        f"📊 Total ≈ {order_margin*3:.2f} USDT "
        f"({order_margin*3/balance*100:.1f}% Kapital)\n\n"
        f"📈 Charts:\n"
        f"H2 {symbol}: {tv_chart_links(symbol)['coin_h2']}\n"
        f"H4 {symbol}: {tv_chart_links(symbol)['coin_h4']}\n"
        f"BTC H2: {tv_chart_links(symbol)['btc_h2']}\n"
        f"Total2: {tv_chart_links(symbol)['total2']}"
        if balance > 0 else ""
    )
    telegram(msg)

    status = "✓ Alle 4" if count == 4 else f"⚠ {count}/4"
    log(f"  {status} TPs gesetzt | "
        f"{len(dca_results)}/2 DCA-Orders gesetzt")


def get_existing_tps(symbol: str) -> list:
    """
    Holt bestehende profit_plan TP-Orders via orders-plan-pending.
    Gibt Liste mit {'price': float, 'qty': float, 'orderId': str} zurück.
    """
    orders = _get_plan_orders(symbol)
    tps = []
    direction_last = "long"
    for o in orders:
        if o.get("planType") == "profit_plan":
            tps.append({
                "price":   float(o.get("triggerPrice", 0)),
                "qty":     float(o.get("size", 0)),
                "orderId": o.get("orderId", ""),
            })
            direction_last = o.get("holdSide", "long")
    tps.sort(key=lambda x: x["price"], reverse=(direction_last == "short"))
    return tps


def tps_are_correct(existing: list, avg: float, total: float,
                    direction: str, leverage: int,
                    decimals: int, mark: float) -> bool:
    """
    Prüft ob bestehende TPs mit dem erwarteten Schema übereinstimmen.
    Toleranz: 0.05% Preisabweichung (Rundungsdifferenzen)
    """
    if len(existing) == 0:
        return False  # Keine TPs → müssen gesetzt werden

    # Erwartete TP-Preise berechnen
    expected_prices = []
    for roi in [TP1_ROI, TP2_ROI, TP3_ROI, TP4_ROI]:
        tp = calc_tp_price(avg, roi, direction, leverage)
        tp_str = round_price(tp, decimals)
        tp_val = float(tp_str)
        # Nur TPs die noch nicht überschritten sind
        if direction == "long" and tp_val > mark:
            expected_prices.append(tp_val)
        elif direction == "short" and tp_val < mark:
            expected_prices.append(tp_val)

    if len(existing) != len(expected_prices):
        log(f"  TP-Anzahl stimmt nicht: {len(existing)} vorhanden, "
            f"{len(expected_prices)} erwartet")
        return False

    # Preise vergleichen (0.05% Toleranz)
    existing_prices = sorted([t["price"] for t in existing])
    expected_prices_sorted = sorted(expected_prices)
    for ex, exp in zip(existing_prices, expected_prices_sorted):
        if exp == 0:
            continue
        diff_pct = abs(ex - exp) / exp * 100
        if diff_pct > 0.05:
            log(f"  TP-Preis stimmt nicht: {ex} vs erwartet {exp:.5f} "
                f"(Δ {diff_pct:.3f}%)")
            return False

    return True



# ═══════════════════════════════════════════════════════════════
# TP UPDATE (bei Nachkauf)
# ═══════════════════════════════════════════════════════════════

def update_tp_for_position(pos: dict, reason: str):
    """
    Wird bei DCA-Fill aufgerufen: löscht alle bestehenden TPs und setzt sie
    auf Basis des neuen Durchschnittspreises neu (10/20/30/40% ROI, 15/20/25/40%).
    WICHTIG: Der SL wird nicht verändert. Er wird beim TP4-Setzen explizit
             mitgeführt, damit place-pos-tpsl ihn nicht überschreibt.
    """
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    avg       = float(pos.get("openPriceAvg", 0))
    total     = float(pos.get("total", 0))
    leverage  = int(float(pos.get("leverage", 10)))
    mark      = get_mark_price(symbol)
    decimals  = get_price_decimals(symbol)

    log(f"  {symbol} | {direction.upper()} | Avg={avg} | "
        f"Qty={total} | Hebel={leverage}x | Mark={mark}")

    if avg == 0 or total == 0:
        log("  Ungültige Position.")
        return

    # SL-Preis vor dem TP-Löschen lesen — wird beim TP4 zwingend mitgeführt
    known_sl = get_sl_price(symbol, direction)
    if known_sl == 0 and symbol in trade_data:
        known_sl = trade_data[symbol].get("sl", 0)
    if known_sl > 0:
        log(f"  SL @ {known_sl} wird unverändert beibehalten")
    else:
        log(f"  ⚠ Kein SL ermittelbar — TP4-Aufruf ohne SL-Parameter")

    log(f"  TPs löschen und neu setzen (Grund: {reason})")

    min_size_for_tps = 4
    if total < min_size_for_tps:
        log(f"  ⚠ Position zu klein (Qty={total}, min. {min_size_for_tps}) — "
            f"TPs manuell überwachen")
        last_known_avg[symbol]  = avg
        last_known_size[symbol] = total
        return

    cancel_all_tp_orders(symbol)
    time.sleep(1)
    count, prices = place_tp_orders(
        symbol, avg, total, direction, leverage, mark, known_sl=known_sl
    )

    if count > 0:
        status = "✓ Alle 4" if count == 4 else f"⚠ {count}/4"
        log(f"  {status} TPs für {symbol} gesetzt.")
        sl_info = f"{known_sl} USDT" if known_sl > 0 else "nicht ermittelbar"
        telegram(
            f"♻️ {dir_icon(direction)} <b>TPs nach DCA — {symbol}</b>\n"
            f"Richtung: {dir_icon(direction)} {direction.upper()} | Hebel: {leverage}x\n"
            f"Neuer Avg: {avg} USDT\n"
            f"SL (unverändert): {sl_info}\n"
            f"Grund: {reason}\n\n"
            + "\n".join(prices)
        )
    else:
        log(f"  ✗ Keine TPs gesetzt (Position evtl. im Ziel).")

    last_known_avg[symbol]  = avg
    last_known_size[symbol] = total
    # Peak-Grösse für Auswertung aktualisieren
    if symbol in trade_data:
        trade_data[symbol]["peak_size"] = max(
            trade_data[symbol].get("peak_size", 0), total
        )


# ═══════════════════════════════════════════════════════════════
# TELEGRAM BOT — EINGEHENDE BEFEHLE
# ═══════════════════════════════════════════════════════════════

def get_telegram_updates() -> list:
    """Holt neue Nachrichten vom Telegram Bot (Long Polling)."""
    global last_update_id
    if not TELEGRAM_TOKEN:
        return []
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 1, "limit": 10},
            timeout=5
        )
        data = r.json()
        if not data.get("ok"):
            return []
        updates = data.get("result", [])
        if updates:
            last_update_id = updates[-1]["update_id"]
        return updates
    except Exception:
        return []


def reply(text: str):
    """Sendet Antwort an den Telegram Chat."""
    telegram(text)


def cmd_berechnen():
    """
    /berechnen — Kontostand + offene Positionen + Money-Management Check
    """
    balance  = get_futures_balance()
    max_10   = balance * 0.10
    per_ord  = max_10 / 3
    kelly    = kelly_recommendation(balance, WINRATE)
    positions = get_all_positions()

    lines = [
        "💰 <b>Kontostand & Status</b>",
        f"━━━━━━━━━━━━",
        f"Konto:        {balance:.2f} USDT",
        f"10%-Limit:    {max_10:.2f} USDT",
        f"Pro Order (÷3): {per_ord:.2f} USDT",
        f"",
        f"📊 Kelly ({WINRATE*100:.0f}% Winrate):",
        f"  Empfohlen:  {kelly['kelly_pct']}% = {kelly['kelly_usdt']:.2f} USDT",
        f"  Half-Kelly: {kelly['half_kelly_pct']}% = {kelly['half_kelly_usdt']:.2f} USDT",
    ]

    if positions:
        lines += ["", f"📈 <b>Offene Positionen ({len(positions)}):</b>"]
        all_secured = True
        for pos in positions:
            sym  = pos.get("symbol", "?")
            qty  = float(pos.get("total", 0))
            avg  = float(pos.get("openPriceAvg", 0))
            lev  = int(float(pos.get("leverage", 10)))
            drct = pos.get("holdSide", "?").upper()
            pnl  = float(pos.get("unrealizedPL", 0))
            secured = sl_at_entry.get(sym, False)
            if not secured:
                all_secured = False
            icon = "🔒" if secured else "⚠️"
            lines.append(
                f"{icon} {sym} {drct} {lev}x | "
                f"Qty={qty} | Avg={avg:.4f} | PnL={pnl:+.2f}"
            )

        if all_secured:
            lines += ["", "✅ Alle Positionen gesichert → neuer Trade möglich"]
        else:
            lines += [
                "",
                "⚠️ Noch nicht alle Positionen auf SL=Entry",
                "→ Kein neuer Trade empfohlen (DOMINUS-Regel)"
            ]
    else:
        lines += ["", "✅ Keine offenen Positionen → bereit für neuen Trade"]

    lines += [
        "",
        "📈 <b>Markt-Übersicht:</b>",
        f"<a href=\"https://www.tradingview.com/chart/?symbol=BITGET:BTCUSDT.P&interval=120\">BTC H2</a>  |  "
        f"<a href=\"https://www.tradingview.com/chart/?symbol=CRYPTOCAP:TOTAL2&interval=120\">Total2 H2</a>",
        "",
        "📋 <b>Befehle:</b>",
        "/berechnen — dieser Status",
        "/trade SYMBOL LONG|SHORT HEBEL ENTRY SL",
        "   Beispiel: /trade ETHUSDT LONG 10 2850 2700",
        "/hilfe — alle Befehle"
    ]
    reply("\n".join(lines))


def cmd_trade(parts: list):
    """
    /trade ETHUSDT LONG 10 2850 2700
    Berechnet Setup-Vorschau ohne etwas auszuführen.
    """
    if len(parts) < 6:
        reply(
            "❌ Falsches Format\n"
            "Korrekt: /trade SYMBOL LONG|SHORT HEBEL ENTRY SL\n"
            "Beispiel: /trade ETHUSDT LONG 10 2850 2700"
        )
        return

    try:
        symbol    = parts[1].upper().replace("/", "").replace("-", "")
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        direction = parts[2].lower()
        leverage  = int(parts[3])
        entry     = float(parts[4])
        sl        = float(parts[5])
    except (ValueError, IndexError):
        reply("❌ Ungültige Werte. Zahlen für Hebel, Entry und SL eingeben.")
        return

    if direction not in ("long", "short"):
        reply("❌ Richtung muss LONG oder SHORT sein.")
        return

    # Validierungen
    if direction == "long" and sl >= entry:
        reply(f"❌ SL ({sl}) muss bei Long unter Entry ({entry}) liegen.")
        return
    if direction == "short" and sl <= entry:
        reply(f"❌ SL ({sl}) muss bei Short über Entry ({entry}) liegen.")
        return

    # Berechnungen
    balance      = get_futures_balance()
    max_margin   = balance * 0.10
    per_order    = max_margin / 3
    sl_dist_pct  = abs(entry - sl) / entry * 100
    opt_leverage = calc_optimal_leverage(entry, sl)
    rr           = calc_rr(entry, sl, leverage, direction)
    kelly        = kelly_recommendation(balance, WINRATE)

    # Position pro Order
    contracts = (per_order * leverage) / entry

    # DCA Preise
    sl_dist = abs(entry - sl)
    if direction == "long":
        dca1 = entry - sl_dist * DCA1_RATIO
        dca2 = entry - sl_dist * DCA2_RATIO
    else:
        dca1 = entry + sl_dist * DCA1_RATIO
        dca2 = entry + sl_dist * DCA2_RATIO

    # TPs
    tps = []
    for roi, label in [
        (TP1_ROI, "TP1 (10%)"),
        (TP2_ROI, "TP2 (20%)"),
        (TP3_ROI, "TP3 (30%)"),
        (TP4_ROI, "TP4 (40%)")
    ]:
        factor = roi / leverage
        if direction == "long":
            tp = entry * (1 + factor)
        else:
            tp = entry * (1 - factor)
        tps.append(f"  {label}: {tp:.4f}")

    # Warnungen
    warnings = []
    if rr < MIN_RR:
        warnings.append(f"⚠️ R:R {rr} unter Minimum {MIN_RR}!")
    if abs(leverage - opt_leverage) > 2:
        warnings.append(
            f"⚠️ Empfohlener Hebel: {opt_leverage}x (aktuell: {leverage}x)"
        )
    if per_order * 3 > max_margin * 1.05:
        warnings.append("⚠️ Setup überschreitet 10%-Limit!")

    rr_icon = "✅" if rr >= MIN_RR else "⚠️"
    lev_icon = "✅" if abs(leverage - opt_leverage) <= 2 else "⚠️"

    lines = [
        f"🧮 <b>Trade-Berechnung — {symbol}</b>",
        f"━━━━━━━━━━━━",
        f"Richtung: {dir_icon(direction)} {direction.upper()} | Hebel: {leverage}x",
        f"Entry: {entry} | SL: {sl} ({sl_dist_pct:.1f}%)",
        f"",
        f"📐 <b>Analyse:</b>",
        f"{rr_icon} R:R: {rr} (Min: {MIN_RR})",
        f"{lev_icon} Empf. Hebel: {opt_leverage}x",
        f"",
        f"📦 <b>Orders (je {contracts:.2f} Kontrakte):</b>",
        f"  Market:  {entry}",
        f"  DCA1:    {dca1:.4f}",
        f"  DCA2:    {dca2:.4f}",
        f"  SL:      {sl}",
        f"",
        f"🎯 <b>Take-Profits (15/20/25/40%):</b>",
    ] + tps + [
        f"",
        f"💰 <b>Money Management:</b>",
        f"  Konto:        {balance:.2f} USDT",
        f"  Pro Order:    {per_order:.2f} USDT",
        f"  Total Setup:  {per_order*3:.2f} USDT ({per_order*3/balance*100:.1f}%)",
        f"  Kelly:        {kelly['kelly_pct']}% empfohlen",
    ]

    if warnings:
        lines += ["", "⚠️ <b>Warnungen:</b>"] + warnings

    links       = tv_chart_links(symbol)
    coin_h2_url = links["coin_h2"]
    coin_h4_url = links["coin_h4"]
    btc_url     = links["btc_h2"]
    total2_url  = links["total2"]
    lines += [
        "",
        "✔︎ HARSI nicht in Extremzone?",
        "✔︎ DOMINUS Impuls im Premium-Bereich?",
        "✔︎ BTC + Total2 gleiche Richtung?",
        "",
        "✅ Wenn ja: Market Order + SL auf Bitget setzen.",
        "Script setzt DCA + TPs automatisch.",
        "",
        "📈 Charts:",
        f'H2 {symbol} (HARSI): {coin_h2_url}',
        f'H4 {symbol} (Premium): {coin_h4_url}',
        f'BTC H2 (Momentum): {btc_url}',
        f'Total2 H2 (Altcoins): {total2_url}',
    ]
    reply("\n".join(lines))


def cmd_hilfe():
    reply(
        "🤖 <b>DOMINUS Bot — Befehle</b>\n"
        "━━━━━━━━━━━━\n"
        "/berechnen — Kontostand + Positionen + BTC/Total2 Links\n"
        "/trade SYMBOL LONG|SHORT HEBEL ENTRY SL\n"
        "   → Setup berechnen + alle Chart-Links\n"
        "   Beispiel: /trade ETHUSDT LONG 10 2850 2700\n"
        "/status — Kurzstatus aller Positionen\n"
        "/refresh [SYMBOL] — SL/TP/DCA sofort prüfen & reparieren\n"
        "   Beispiel: /refresh BTCUSDT  (oder /refresh für alle)\n"
        "/hilfe — diese Übersicht\n"
        "\n"
        "🎯 <b>Premium Setup (DOMINUS):</b>\n"
        "Long: DOMINUS Impuls in dunkelgrüner Zone\n"
        "   (zwischen überverkauft und Mittellinie)\n"
        "Short: DOMINUS Impuls in dunkelroter Zone\n"
        "   (zwischen überkauft und Mittellinie)\n"
        "→ Kein Premium = Trade möglich, aber höheres Risiko\n"
        "\n"
        "⚙️ <b>Automatisch nach Einstieg:</b>\n"
        "• DCA1 + DCA2 Limit-Orders\n"
        "• TP1–TP4 (15/20/25/40%)\n"
        "• SL auf Entry nach TP1"
    )


def cmd_status():
    """Kurzer Positionsstatus."""
    positions = get_all_positions()
    if not positions:
        reply("✅ Keine offenen Positionen.")
        return
    lines = [f"📊 <b>{len(positions)} offene Position(en):</b>"]
    for pos in positions:
        sym  = pos.get("symbol", "?")
        qty  = float(pos.get("total", 0))
        drct = pos.get("holdSide", "?").upper()
        lev  = int(float(pos.get("leverage", 10)))
        pnl  = float(pos.get("unrealizedPL", 0))
        mark = get_mark_price(sym)
        secured = sl_at_entry.get(sym, False)
        trl_lvl = trailing_sl_level.get(sym, 0)
        trl_tag = {0: "", 1: " · SL=Entry", 2: " · Trail→TP1", 3: " · Trail→TP2"}.get(trl_lvl, "")
        icon = "🔒" if secured else "📈"
        lines.append(
            f"{icon} {sym} {drct} {lev}x | "
            f"Qty={qty:.2f} | Mark={mark} | PnL={pnl:+.2f} USDT{trl_tag}"
        )
    reply("\n".join(lines))


def cmd_refresh(parts: list):
    """
    /refresh [SYMBOL] — Prüft eine oder alle offenen Positionen sofort und
    repariert fehlende/falsche SL, TP und DCA-Orders gemäss DOMINUS-Schema.

    Beispiele:
      /refresh BTCUSDT   → nur BTCUSDT prüfen
      /refresh           → alle offenen Positionen prüfen
    """
    positions = get_all_positions()
    if not positions:
        reply("ℹ️ Keine offenen Positionen gefunden.")
        return

    # Symbol aus Argument extrahieren (z.B. /refresh BTCUSDT)
    target = parts[1].upper() if len(parts) > 1 else None

    # Ggf. filtern
    if target:
        matched = [p for p in positions if p.get("symbol", "").upper() == target]
        if not matched:
            # Freundlich: "BTCUSDT" kann auch als "BTC" eingegeben werden
            matched = [p for p in positions
                       if p.get("symbol", "").upper().startswith(target.rstrip("USDT"))]
        if not matched:
            reply(
                f"⚠️ Keine offene Position für <b>{target}</b> gefunden.\n"
                f"Offene Positionen: {', '.join(p.get('symbol','?') for p in positions)}"
            )
            return
        positions_to_check = matched
    else:
        positions_to_check = positions

    # Bestätigung vorab
    symbols_str = ", ".join(p.get("symbol", "?") for p in positions_to_check)
    reply(f"🔄 Refresh gestartet für: <b>{symbols_str}</b>\nPrüfe SL / TP / DCA…")

    log(f"[/refresh] Manueller Check für: {symbols_str}")

    for pos in positions_to_check:
        sym = pos.get("symbol", "?")
        log(f"[/refresh] ── {sym}")
        check_and_repair_position(pos)

    # Abschlussbericht
    lines = [f"✅ <b>Refresh abgeschlossen</b> — {symbols_str}"]
    for pos in positions_to_check:
        sym  = pos.get("symbol", "?")
        drct = pos.get("holdSide", "?").upper()
        lev  = int(float(pos.get("leverage", 10)))
        avg  = float(pos.get("openPriceAvg", 0))
        mark = get_mark_price(sym)
        sl   = get_sl_price(sym, pos.get("holdSide", "long"))
        tps  = get_existing_tps(sym)
        n_tp = len(tps)
        secured = sl_at_entry.get(sym, False)
        lock = "🔒 SL=Entry" if secured else (f"🛡 SL={sl:.4f}" if sl > 0 else "⚠️ Kein SL!")
        lines.append(
            f"\n📍 <b>{sym}</b> {drct} {lev}x\n"
            f"   Entry={avg:.4f} | Mark={mark}\n"
            f"   {lock} | TPs gesetzt: {n_tp}"
        )
    reply("\n".join(lines))


def build_daily_report(date_str: str = None) -> str:
    """
    Erstellt einen täglichen P&L-Report als HTML-String für Telegram.
    Zeigt: Trades des Tages, Monats-Gesamtperformance, offene Positionen.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    month_str = date_str[:7]  # "2026-04"

    # Tages-Trades filtern
    day_trades = [
        t for t in closed_trades
        if datetime.fromtimestamp(t.get("ts", 0)).strftime("%Y-%m-%d") == date_str
    ]
    # Monats-Trades filtern
    month_trades = [
        t for t in closed_trades
        if datetime.fromtimestamp(t.get("ts", 0)).strftime("%Y-%m") == month_str
    ]

    def _summary(trades: list) -> dict:
        if not trades:
            return {"count": 0, "wins": 0, "losses": 0, "total_pnl": 0.0,
                    "win_rate": 0.0, "best": 0.0, "worst": 0.0}
        wins    = sum(1 for t in trades if t.get("won", False))
        losses  = len(trades) - wins
        pnls    = [float(t.get("net_pnl", 0)) for t in trades]
        total   = sum(pnls)
        best    = max(pnls) if pnls else 0.0
        worst   = min(pnls) if pnls else 0.0
        return {
            "count":    len(trades),
            "wins":     wins,
            "losses":   losses,
            "total_pnl": total,
            "win_rate": wins / len(trades) * 100 if trades else 0.0,
            "best":     best,
            "worst":    worst,
        }

    day_s   = _summary(day_trades)
    month_s = _summary(month_trades)

    # Offene Positionen
    open_positions = get_all_positions()

    lines = [
        f"📊 <b>DOMINUS Daily Report — {date_str}</b>",
        "━━━━━━━━━━━━",
        f"",
        f"📅 <b>Heute ({date_str}):</b>",
        f"Trades: {day_s['count']}  |  🏆 {day_s['wins']} / 🔴 {day_s['losses']}",
        f"Win-Rate: {day_s['win_rate']:.0f}%",
        f"Netto P&L: {day_s['total_pnl']:+.2f} USDT",
        f"Bester: {day_s['best']:+.2f} USDT  |  Schlechtester: {day_s['worst']:+.2f} USDT",
    ]

    if day_trades:
        lines.append("")
        lines.append("📋 <b>Trades heute:</b>")
        for t in day_trades:
            icon = "🏆" if t.get("won") else "🔴"
            lines.append(
                f"  {icon} {t['symbol']} {t.get('direction','?').upper()} "
                f"| {float(t.get('net_pnl',0)):+.2f} USDT "
                f"| {t.get('hold_str','?')}"
            )

    lines += [
        "",
        f"📆 <b>Monat ({month_str}):</b>",
        f"Trades: {month_s['count']}  |  🏆 {month_s['wins']} / 🔴 {month_s['losses']}",
        f"Win-Rate: {month_s['win_rate']:.0f}%",
        f"Netto P&L: {month_s['total_pnl']:+.2f} USDT",
    ]

    if open_positions:
        lines.append("")
        lines.append(f"📈 <b>Offene Positionen ({len(open_positions)}):</b>")
        for pos in open_positions:
            sym  = pos.get("symbol", "?")
            drct = pos.get("holdSide", "?").upper()
            lev  = int(float(pos.get("leverage", 10)))
            pnl  = float(pos.get("unrealizedPL", 0))
            trl  = trailing_sl_level.get(sym, 0)
            trl_tag = {0: "", 1: " SL=Entry", 2: " Trail→TP1", 3: " Trail→TP2"}.get(trl, "")
            lines.append(f"  • {sym} {drct} {lev}x | PnL={pnl:+.2f}{trl_tag}")
    else:
        lines.append("")
        lines.append("✅ Keine offenen Positionen")

    return "\n".join(lines)


def cmd_report():
    """Sendet den täglichen P&L Report auf Telegram-Anfrage."""
    try:
        report = build_daily_report()
        telegram(report)
    except Exception as e:
        reply(f"❌ Report Fehler: {e}")


def poll_telegram_commands():
    """
    Prüft auf neue Telegram-Nachrichten und führt Befehle aus.
    Wird jeden Loop-Durchlauf aufgerufen.
    Nur Nachrichten von TELEGRAM_CHAT_ID werden akzeptiert (Sicherheit).
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    updates = get_telegram_updates()
    for update in updates:
        msg = update.get("message")
        if not msg:
            continue

        # Sicherheit: nur eigene Chat-ID
        chat_id = str(msg.get("chat", {}).get("id", ""))
        if chat_id != str(TELEGRAM_CHAT_ID):
            continue

        text = msg.get("text", "").strip()
        if not text.startswith("/"):
            continue

        parts = text.split()
        cmd   = parts[0].lower().split("@")[0]  # /berechnen@BotName → /berechnen

        log(f"Telegram Befehl empfangen: {text}")

        if cmd == "/berechnen":
            cmd_berechnen()
        elif cmd == "/trade":
            cmd_trade(parts)
        elif cmd == "/status":
            cmd_status()
        elif cmd == "/refresh":
            cmd_refresh(parts)
        elif cmd == "/report":
            cmd_report()
        elif cmd == "/hilfe" or cmd == "/start" or cmd == "/help":
            cmd_hilfe()
        else:
            reply(
                f"❓ Unbekannter Befehl: {cmd}\n"
                "Sende /hilfe für alle Befehle."
            )


# ═══════════════════════════════════════════════════════════════
# WEBHOOK SERVER — empfängt TradingView Alerts
# ═══════════════════════════════════════════════════════════════

def flush_h4_buffer():
    """Sendet gesammelte H4-Trigger als eine gebündelte Telegram-Nachricht."""
    global h4_buffer
    with h4_buffer_lock:
        if not h4_buffer:
            return
        items     = list(h4_buffer)
        h4_buffer = []

    longs  = [i for i in items if i["direction"] == "long"]
    shorts = [i for i in items if i["direction"] == "short"]
    now_str = __import__("datetime").datetime.now().strftime("%H:%M")

    lines = [
        f"\U0001f4cb <b>H4 Trigger-Zusammenfassung \u2014 {now_str} Uhr</b>",
        "\u2501" * 12,
    ]
    if longs:
        lines.append("🟢↗️ <b>LONG:</b>")
        for item in longs:
            lnk = tv_chart_links(item["symbol"])
            lines.append(f"  \u2022 {item['symbol']}  @ {item['entry']:.5f}")
            lines.append(f"    {lnk['coin_h4']}")
    if longs and shorts:
        lines.append("")
    if shorts:
        lines.append("🔴↘️ <b>SHORT:</b>")
        for item in shorts:
            lnk = tv_chart_links(item["symbol"])
            lines.append(f"  \u2022 {item['symbol']}  @ {item['entry']:.5f}")
            lines.append(f"    {lnk['coin_h4']}")

    btc_lnk = tv_chart_links("BTCUSDT")
    lines += [
        "",
        "\u23f1 H2-Alarm f\u00fcr relevante Coins aktivieren",
        "",
        "\U0001f4c8 Markt:",
        f"BTC H2: {btc_lnk['btc_h2']}",
        f"Total2: {btc_lnk['total2']}",
    ]
    telegram("\n".join(lines))
    log(f"H4-Zusammenfassung gesendet: {len(longs)} Long, {len(shorts)} Short")


def start_webhook_server():
    """
    Startet einen Flask HTTP-Server in einem separaten Thread.
    TradingView sendet Alerts per POST an /webhook?token=SECRET

    Erwartet JSON-Payload:
    {
      "symbol":    "ETHUSDT" oder "BITGET:ETHUSDT.P",
      "direction": "long" oder "short",
      "entry":     2850.50,        (optional — nimmt aktuellen Kurs wenn leer)
      "timeframe": "H2"            (optional — für Log)
    }
    """
    if not FLASK_AVAILABLE:
        log("Flask nicht installiert — Webhook-Server deaktiviert")
        log("requirements.txt: flask hinzufügen")
        return

    app = Flask(__name__)

    @app.route("/webhook", methods=["POST"])
    def webhook():
        # ── Token-Prüfung ─────────────────────────────────────
        token = flask_request.args.get("token", "")
        if token != WEBHOOK_SECRET:
            log("⚠ Webhook: Ungültiger Token")
            return jsonify({"error": "unauthorized"}), 401

        # ── Payload parsen ────────────────────────────────────
        try:
            data = flask_request.get_json(force=True) or {}
        except Exception:
            return jsonify({"error": "invalid json"}), 400

        raw_symbol = data.get("symbol", "").upper()
        entry      = float(data.get("entry", 0) or 0)
        timeframe  = data.get("timeframe", "H2").upper()

        # Richtung: explizit ODER aus Buy/Sell Plot-Werten
        direction = data.get("direction", "").lower()
        if direction not in ("long", "short"):
            buy_val  = float(data.get("buy",  0) or 0)
            sell_val = float(data.get("sell", 0) or 0)
            if buy_val > 0 and sell_val == 0:
                direction = "long"
            elif sell_val > 0 and buy_val == 0:
                direction = "short"

        # Symbol bereinigen: BITGET:ETHUSDT.P → ETHUSDT
        symbol = raw_symbol
        for prefix in ["BITGET:", "BYBIT:", "BINANCE:"]:
            symbol = symbol.replace(prefix, "")
        symbol = symbol.replace(".P", "").replace("PERP", "")
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        if not symbol or direction not in ("long", "short"):
            log(f"⚠ Webhook ignoriert: kein Signal "
                f"(buy={data.get('buy',0)} sell={data.get('sell',0)} "
                f"dir={data.get('direction','')})")
            return jsonify({"status": "ignored", "reason": "no signal"}), 200

        log(f"📡 TradingView Alert: {symbol} {direction.upper()} "
            f"@ {entry} [{timeframe}]")

        if entry == 0:
            entry = get_mark_price(symbol)

        signal_type = data.get("signal", "").upper()
        log(f"\U0001f4e1 Alert: {symbol} {direction.upper()} @ {entry} [{timeframe}]")

        if signal_type == "HARSI_EXIT":
            harsi_price = float(data.get("price", 0) or data.get("sl", 0) or 0)
            if harsi_price == 0:
                return jsonify({"status": "ignored", "reason": "no price"}), 200
            cur_size = 0
            for pos in get_all_positions():
                if pos.get("symbol") == symbol and pos.get("holdSide") == direction:
                    cur_size = float(pos.get("total", 0))
                    break
            if cur_size == 0:
                return jsonify({"status": "ignored", "reason": "no open position"}), 200
            set_sl_harsi(symbol, direction, harsi_price, cur_size=cur_size)
            return jsonify({"status": "ok", "symbol": symbol, "harsi_sl": harsi_price}), 200

        # H4 Trigger → gepuffert, nach 5 Min gebündelt senden
        if timeframe == "H4" or signal_type == "H4_TRIGGER":
            with h4_buffer_lock:
                exists = any(
                    i["symbol"] == symbol and i["direction"] == direction
                    for i in h4_buffer
                )
                if not exists:
                    h4_buffer.append({
                        "symbol": symbol, "direction": direction,
                        "entry": entry, "ts": __import__("time").time(),
                    })
                    log(f"  H4 gepuffert ({len(h4_buffer)} im Puffer)")
            return jsonify({"status": "buffered", "symbol": symbol}), 200

        # H2 Signal → H4 Puffer flushen dann sofort senden
        flush_h4_buffer()
        balance   = get_futures_balance()
        kelly     = kelly_recommendation(balance, WINRATE)
        links     = tv_chart_links(symbol)
        per_order = balance * 0.10 / 3
        chk_dir   = "gr\u00fcner" if direction == "long" else "roter"
        icon      = dir_icon(direction)
        msg_parts = [
            f"{icon} <b>H2 Signal \u2014 {symbol} {direction.upper()}</b>",
            "\u2501" * 12,
            f"Kurs: {entry}",
            "",
            "\U0001f4cb <b>DOMINUS Checkliste:</b>",
            "\u2610 DOMINUS Impuls Extremzone erreicht?",
            "\u2610 H4 Trigger best\u00e4tigt?",
            "\u2610 HARSI nicht in Extremzone?",
            "\u2610 BTC + Total2 gleiche Richtung?",
            f"\u2610 Premium Setup? (Impuls dunkel{chk_dir})",
            "",
            f"\U0001f4b0 {balance:.0f} USDT  |  Pro Order: {per_order:.0f} USDT",
            f"\U0001f4ca Kelly: {kelly['kelly_pct']}%",
            "",
            "\u23f1 <b>30-Min-Fenster l\u00e4uft!</b>",
            f"/trade {symbol} {direction.upper()} [HEBEL] {entry:.5f} [SL]",
            "",
            "\U0001f4c8 Charts:",
            f"H2 {symbol}: {links['coin_h2']}",
            f"H4 {symbol}: {links['coin_h4']}",
            f"BTC H2: {links['btc_h2']}",
            f"Total2: {links['total2']}",
        ]
        telegram("\n".join(msg_parts))
        return jsonify({"status": "ok", "symbol": symbol,
                        "direction": direction}), 200

    @app.route("/", methods=["GET"])
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "running", "version": "v4.32"}), 200

    port = int(os.environ.get("PORT", 8080))
    log(f"Webhook-Server gestartet auf Port {port}")
    log(f"Endpoint: /webhook?token={WEBHOOK_SECRET}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)



# ═══════════════════════════════════════════════════════════════
# STATE PERSISTENZ (speichern / laden)
# ═══════════════════════════════════════════════════════════════

STATE_FILE = os.environ.get("STATE_FILE", "/tmp/dominus_state.json")


def save_state():
    """Speichert den aktuellen In-Memory-State in eine JSON-Datei."""
    try:
        state = {
            "last_known_avg":          last_known_avg,
            "last_known_size":         last_known_size,
            "sl_at_entry":             sl_at_entry,
            "new_trade_done":          new_trade_done,
            "trade_data":              trade_data,
            "trailing_sl_level":       trailing_sl_level,
            "closed_trades":           [t for t in closed_trades if t.get("ts", 0) >= time.time() - 90*86400],
            "daily_report_sent_date":  daily_report_sent_date,
            "harsi_sl":                harsi_sl,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log(f"[save_state] Fehler: {e}")


def load_state():
    """Lädt den gespeicherten State aus der JSON-Datei beim Start."""
    global daily_report_sent_date
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r") as f:
            s = json.load(f)
        last_known_avg.update(s.get("last_known_avg", {}))
        last_known_size.update(s.get("last_known_size", {}))
        sl_at_entry.update(s.get("sl_at_entry", {}))
        new_trade_done.update(s.get("new_trade_done", {}))
        trade_data.update(s.get("trade_data", {}))
        trailing_sl_level.update(s.get("trailing_sl_level", {}))
        closed_trades.extend(s.get("closed_trades", []))
        daily_report_sent_date = s.get("daily_report_sent_date", "")
        harsi_sl.update(s.get("harsi_sl", {}))
        log(f"[load_state] State geladen: {len(last_known_avg)} Position(en)")
    except Exception as e:
        log(f"[load_state] Fehler: {e}")


# ═══════════════════════════════════════════════════════════════
# FILL-ANALYSE (Startup: welche TPs wurden tatsächlich ausgelöst?)
# ═══════════════════════════════════════════════════════════════

def get_symbol_close_fills(symbol: str, since_hours: int = 48) -> list:
    """
    Liest kürzlich ausgeführte Schliessungs-Fills für ein Symbol von Bitget.
    Wird beim Startup genutzt um fill-basiert zu erkennen welche TPs bereits
    ausgelöst wurden — zuverlässiger als rein preisbasierte Erkennung,
    da der Preis nach einem TP-Fill wieder zurückkommen kann.
    """
    since_ms = int((time.time() - since_hours * 3600) * 1000)
    result = api_get("/api/v2/mix/order/fill-history", {
        "productType": PRODUCT_TYPE,
        "symbol":      symbol,
        "startTime":   str(since_ms),
        "limit":       "100",
    })
    if result.get("code") != "00000":
        return []
    fills = (result.get("data") or {}).get("fillList") or []
    # Nur Schliessungs-Fills zurückgeben (tradeSide == "close" oder "reduce_only")
    close_fills = [f for f in fills
                   if f.get("tradeSide") in ("close", "reduce_only", "Close")]
    return close_fills


def detect_filled_tps(close_fills: list, avg: float, leverage: int,
                      direction: str) -> list:
    """
    Vergleicht ausgeführte Schliessungs-Fills mit den erwarteten TP-Preisen.
    Gibt eine Liste der tatsächlich ausgelösten TPs zurück.

    Toleranz: ±0.5% des TP-Preises (deckt manuelle Teilschliessungen und
    Slippage ab).
    """
    if not avg or not close_fills:
        return []

    rois   = [TP1_ROI, TP2_ROI, TP3_ROI, TP4_ROI]
    labels = ["TP1 (10%)", "TP2 (20%)", "TP3 (30%)", "TP4 (40%)"]
    pcts   = TP_CLOSE_PCTS
    filled = []

    fill_prices = []
    for f in close_fills:
        try:
            fp = float(f.get("price") or 0)
            if fp > 0:
                fill_prices.append(fp)
        except (ValueError, TypeError):
            pass

    if not fill_prices:
        return []

    for label, roi, pct in zip(labels, rois, pcts):
        tp_price  = calc_tp_price(avg, roi, direction, leverage)
        tolerance = tp_price * 0.005  # ±0.5%
        for fp in fill_prices:
            if abs(fp - tp_price) <= tolerance:
                filled.append({"label": label, "roi": roi, "pct": pct,
                               "price": tp_price})
                break

    return filled


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def analyse_trade_state(avg: float, mark: float, leverage: int,
                        direction: str) -> dict:
    """
    Errechnet den aktuellen Trade-Stand anhand von Entry-Preis und Mark-Preis.

    Vergleicht den aktuellen Kurs (mark) mit den errechneten TP-Preisen:
      Long:  TP ausgelöst wenn mark >= TP-Preis
      Short: TP ausgelöst wenn mark <= TP-Preis

    Gibt zurück:
      tps_hit        — Liste der bereits preislich passierten TPs (Label, ROI, Preis)
      tps_remaining  — Liste der noch offenstehenden TPs (Label, ROI, Pct, Preis)
      tp1_price_hit  — bool: TP1-Preislevel wurde passiert
      n_expected_profit_plan — Anzahl erwarteter profit_plan Orders (TP1–TP3 der verbleibenden)
      tp4_expected   — bool: TP4 sollte noch als Full-Close vorhanden sein
      pnl_roi_pct    — unrealisierter ROI auf Margin (Mark vs Entry, mit Hebel)
    """
    rois   = [TP1_ROI, TP2_ROI, TP3_ROI, TP4_ROI]
    labels = ["TP1 (10%)", "TP2 (20%)", "TP3 (30%)", "TP4 (40%)"]
    pcts   = TP_CLOSE_PCTS

    tps_hit       = []
    tps_remaining = []

    for label, roi, pct in zip(labels, rois, pcts):
        tp_price = calc_tp_price(avg, roi, direction, leverage)
        if direction == "long":
            hit = mark >= tp_price
        else:
            hit = mark <= tp_price
        entry = {"label": label, "roi": roi, "pct": pct, "price": tp_price}
        if hit:
            tps_hit.append(entry)
        else:
            tps_remaining.append(entry)

    tp1_price_hit = len(tps_hit) >= 1

    # profit_plan Orders: TP1–TP3 der noch nicht passierten TPs
    n_expected_profit_plan = sum(
        1 for t in tps_remaining if t["roi"] < TP4_ROI
    )
    tp4_expected = any(t["roi"] == TP4_ROI for t in tps_remaining)

    # Unrealisierter ROI auf Margin
    price_change_pct = (mark - avg) / avg * 100
    if direction == "short":
        price_change_pct = -price_change_pct
    pnl_roi_pct = price_change_pct * leverage

    return {
        "tps_hit":                tps_hit,
        "tps_remaining":          tps_remaining,
        "tp1_price_hit":          tp1_price_hit,
        "n_expected_profit_plan": n_expected_profit_plan,
        "tp4_expected":           tp4_expected,
        "pnl_roi_pct":            round(pnl_roi_pct, 1),
    }


def check_and_repair_position(pos: dict):
    """
    Startup-Check: Liest ZUERST alles aus Bitget, analysiert dann den Stand,
    und korrigiert anschliessend nur was tatsächlich fehlt oder falsch ist.

    Ablauf:
      0. READ  — Alle relevanten Daten aus Bitget laden:
                 Positionsdaten, bestehender SL, bestehende TPs,
                 Fill-Historie (letzte 48h) für fill-basierte TP-Erkennung.
      1. ANALYSE — Trade-Stand bestimmen (3 Quellen kombiniert):
                   a) Preisbasiert: Mark vs. TP-Preise
                   b) Fill-basiert: tatsächlich ausgeführte Schliessungen
                   c) SL-basiert:  SL auf Entry = TP1 war schon ausgelöst
      2. SL CHECK — Bestehendes SL wird RESPEKTIERT wenn es ≤ Auto-SL-Niveau
                    (d.h. besser als -25% Margin). Kein Überschreiben!
                    Kein SL gefunden + Position im Gewinn → SL auf Entry (Floor-Regel).
                    Kein SL + kein Gewinn → Auto-SL auf -25% Margin.
      3. TP CHECK — Nur fehlende/falsche TPs werden neu gesetzt.
      4. TP1 done → DCAs stornieren, fertig.
      5. Noch kein TP → 2 DCA Limit-Orders prüfen, fehlende nachsetzen.
    """
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    avg       = float(pos.get("openPriceAvg", 0))
    size      = float(pos.get("total", 0))
    leverage  = int(float(pos.get("leverage", 10)))
    decimals  = get_price_decimals(symbol)
    mark      = get_mark_price(symbol)

    log(f"  ── {symbol} | {direction.upper()} | "
        f"Avg={avg} | Qty={size} | {leverage}x | Mark={mark}")

    if avg == 0 or size == 0:
        log("  Ungültige Positionsdaten — übersprungen.")
        return

    # State vormerken
    last_known_avg[symbol]  = avg
    last_known_size[symbol] = size
    new_trade_done[symbol]  = True
    sl_at_entry[symbol]     = False

    # ══════════════════════════════════════════════════════════════
    # PHASE 0: ALLES AUS BITGET LESEN (Read-First-Prinzip)
    # ══════════════════════════════════════════════════════════════
    # Alle Daten werden ZUERST geladen, bevor irgendwas verändert wird.
    # Reihenfolge: Positionsdaten → SL → TPs → Fill-Historie

    sl_price    = get_sl_price(symbol, direction)
    sl_is_entry = False

    # Fill-Historie (letzte 48h) → fill-basierte TP-Erkennung
    close_fills  = get_symbol_close_fills(symbol, since_hours=48)
    filled_tps   = detect_filled_tps(close_fills, avg, leverage, direction)
    fill_tp1_hit = any(t["roi"] == TP1_ROI for t in filled_tps)

    if filled_tps:
        fill_labels = ", ".join(t["label"] for t in filled_tps)
        log(f"  Fill-Historie: {len(close_fills)} Schliessungen → ausgel. TPs: {fill_labels}")
    else:
        log(f"  Fill-Historie: {len(close_fills)} Schliessungen → kein TP-Fill erkannt")

    # ══════════════════════════════════════════════════════════════
    # PHASE 1: TRADE-STAND ANALYSIEREN (3 Quellen kombiniert)
    # ══════════════════════════════════════════════════════════════

    # 1a. Preisbasiert: Mark vs. TP-Preise
    state     = analyse_trade_state(avg, mark, leverage, direction)
    pnl       = state["pnl_roi_pct"]
    pnl_sign  = "+" if pnl >= 0 else ""

    log(f"  Unrealisierter ROI: {pnl_sign}{pnl}% auf Margin")

    # 1b. SL-basiert: SL nahe Entry = TP1 war schon ausgelöst
    if sl_price > 0:
        sl_dist_pct_read = abs(avg - sl_price) / avg * 100
        sl_is_entry      = sl_dist_pct_read <= 0.15
        if sl_is_entry:
            log(f"  SL gelesen: @ {sl_price} → auf Entry (TP1 bereits ausgelöst)")
        else:
            log(f"  SL gelesen: @ {sl_price} ({sl_dist_pct_read:.2f}% Abstand)")
    else:
        log(f"  SL gelesen: keiner gefunden auf Bitget")

    # 1c. Kombinierte TP1-Erkennung (preislich ODER fill-basiert ODER SL-on-Entry)
    tp1_done = state["tp1_price_hit"] or fill_tp1_hit or sl_is_entry
    if fill_tp1_hit and not state["tp1_price_hit"]:
        log(f"  TP1 via Fill erkannt (Preis hat sich erholt — fill-basierte Erkennung aktiv)")
    if state["tps_hit"] and not fill_tp1_hit:
        hit_labels = ", ".join(t["label"] for t in state["tps_hit"])
        log(f"  Preislich passierte TPs: {hit_labels}")
    elif not tp1_done:
        log(f"  Kein TP ausgelöst → alle 4 TPs erwartet")

    # Verbleibende TPs nach kombinierter Analyse:
    # Wenn fill_tp1_hit aber preis-basiert 0 TPs erkannt → fill-basiert korrekt übernehmen
    if fill_tp1_hit and not state["tps_hit"]:
        # Fills zeigen TP1 ausgelöst, Preis hat sich aber erholt
        # → state manuell auf "TP1 passiert" anpassen
        filled_rois = {t["roi"] for t in filled_tps}
        tps_remaining_adjusted = [t for t in state["tps_remaining"]
                                   if t["roi"] not in filled_rois]
        state = dict(state)   # shallow copy zum Überschreiben
        state["tps_hit"]               = filled_tps
        state["tps_remaining"]         = tps_remaining_adjusted
        state["tp1_price_hit"]         = True
        state["n_expected_profit_plan"] = sum(
            1 for t in tps_remaining_adjusted if t["roi"] < TP4_ROI
        )
        state["tp4_expected"] = any(t["roi"] == TP4_ROI for t in tps_remaining_adjusted)

    remaining_labels = [t["label"] for t in state["tps_remaining"]]
    log(f"  Noch offene TPs erwartet: "
        f"{', '.join(remaining_labels) if remaining_labels else 'keine (alle passiert)'}")

    # ══════════════════════════════════════════════════════════════
    # PHASE 2: SL PRÜFEN UND NUR BEI BEDARF KORRIGIEREN
    # ══════════════════════════════════════════════════════════════
    # Regel: Bestehender SL wird NICHT überschrieben wenn er bereits
    # besser (schützender) als der Auto-SL ist.
    # Auto-SL für SHORT = Entry × (1 + 0.025) = über Entry = Verlustzone
    # Bestehender SL bei/unter Entry = BESSER → beibehalten!

    # Auto-SL Referenzwert berechnen (für Vergleich)
    _factor   = 0.25 / leverage
    _sl_auto  = avg * (1 - _factor) if direction == "long" else avg * (1 + _factor)

    def _sl_is_better_than_auto(sl_val: float) -> bool:
        """True wenn der gegebene SL schützender ist als der Auto-SL."""
        if direction == "short":
            return sl_val <= _sl_auto   # SHORT: tiefer = besser
        else:
            return sl_val >= _sl_auto   # LONG:  höher  = besser

    if sl_price > 0:
        # SL vorhanden — respektieren wenn besser als Auto-SL
        sl_dist_pct = abs(avg - sl_price) / avg * 100
        sl_is_entry = sl_dist_pct <= 0.15

        if sl_is_entry:
            log(f"  ✓ SL auf Entry @ {sl_price} (bestätigt — wird beibehalten)")
        elif tp1_done and not sl_is_entry:
            # TP1 ausgelöst (aus irgendeiner Quelle) aber SL noch nicht auf Entry
            sl_str   = round_price(avg, decimals)
            sl_valid = (direction == "long" and avg <= mark) or \
                       (direction == "short" and avg >= mark)
            if sl_valid:
                log(f"  TP1 ausgelöst (SL @ {sl_price} noch nicht auf Entry) "
                    f"→ SL auf Entry ziehen @ {sl_str}")
                existing_tp4 = _get_pos_tp_price(symbol, direction)
                body_sl = {
                    "symbol":               symbol,
                    "productType":          PRODUCT_TYPE,
                    "marginCoin":           MARGIN_COIN,
                    "holdSide":             direction,
                    "stopLossTriggerPrice": sl_str,
                    "stopLossTriggerType":  "mark_price",
                }
                if existing_tp4 > 0:
                    body_sl["takeProfitTriggerPrice"] = round_price(existing_tp4, decimals)
                    body_sl["takeProfitTriggerType"]  = "mark_price"
                res = api_post("/api/v2/mix/order/place-pos-tpsl", body_sl)
                if res.get("code") == "00000":
                    sl_price    = avg
                    sl_is_entry = True
                    log(f"  ✓ SL auf Entry nachgezogen @ {sl_str}")
                    telegram(
                        f"🔒 <b>SL auf Entry nachgezogen — {symbol}</b>\n"
                        f"Script-Start: TP1 ausgelöst (fill-/preis-/SL-basiert)\n"
                        f"SL: {sl_str} USDT"
                    )
                else:
                    log(f"  ✗ SL nachziehen fehlgeschlagen: {res.get('msg', res)}")
            else:
                log(f"  ⚠ Mark {mark} hinter Entry {avg} — SL auf Entry nicht setzbar")
        elif _sl_is_better_than_auto(sl_price):
            # SL vorhanden und besser als Auto-SL → NICHT anfassen
            log(f"  ✓ SL @ {sl_price} ({sl_dist_pct:.2f}% Abstand) — besser als Auto-SL, wird beibehalten")
        else:
            log(f"  ✓ SL @ {sl_price} ({sl_dist_pct:.2f}% Abstand)")

    else:
        # Kein SL gefunden auf Bitget
        if tp1_done:
            # TP1 bereits ausgelöst → SL muss auf Entry
            sl_str   = round_price(avg, decimals)
            sl_valid = (direction == "long" and avg <= mark) or \
                       (direction == "short" and avg >= mark)
            if sl_valid:
                reason = ("fill-basiert" if fill_tp1_hit and not state["tp1_price_hit"]
                          else "preislich passiert")
                log(f"  TP1 ausgelöst ({reason}), kein SL → setze SL auf Entry @ {sl_str}")
                res = api_post("/api/v2/mix/order/place-pos-tpsl", {
                    "symbol":               symbol,
                    "productType":          PRODUCT_TYPE,
                    "marginCoin":           MARGIN_COIN,
                    "holdSide":             direction,
                    "stopLossTriggerPrice": sl_str,
                    "stopLossTriggerType":  "mark_price",
                })
                if res.get("code") == "00000":
                    sl_price    = avg
                    sl_is_entry = True
                    log(f"  ✓ SL auf Entry gesetzt @ {sl_str}")
                    telegram(
                        f"🔒 <b>SL auf Entry gesetzt — {symbol}</b>\n"
                        f"Script-Start: TP1 ausgelöst ({reason}), kein SL vorhanden\n"
                        f"SL: {sl_str} USDT"
                    )
                else:
                    log(f"  ✗ SL auf Entry fehlgeschlagen: {res.get('msg', res)}")
            else:
                log(f"  ⚠ Mark {mark} bereits hinter Entry {avg} — SL auf Entry nicht setzbar")
                telegram(f"⚠️ <b>{symbol}</b>: Position im Verlust, SL manuell setzen!")

        elif pnl > 0:
            # Position im Gewinn, kein SL gefunden — SL MINDESTENS auf Entry setzen
            # (Floor-Regel: verhindert, dass ein Auto-SL in Verlustzone gesetzt wird
            #  wenn die Position bereits einen Gewinn aufgebaut hat)
            sl_str   = round_price(avg, decimals)
            sl_valid = (direction == "long" and avg <= mark) or \
                       (direction == "short" and avg >= mark)
            if sl_valid:
                log(f"  Position im Gewinn ({pnl_sign}{pnl}%), kein SL → "
                    f"SL-Floor auf Entry @ {sl_str} (kein Auto-SL in Verlustzone!)")
                res = api_post("/api/v2/mix/order/place-pos-tpsl", {
                    "symbol":               symbol,
                    "productType":          PRODUCT_TYPE,
                    "marginCoin":           MARGIN_COIN,
                    "holdSide":             direction,
                    "stopLossTriggerPrice": sl_str,
                    "stopLossTriggerType":  "mark_price",
                })
                if res.get("code") == "00000":
                    sl_price    = avg
                    sl_is_entry = True
                    cancel_open_dca_orders(symbol, direction)
                    trailing_sl_level[symbol] = max(trailing_sl_level.get(symbol, 0), 1)
                    sl_set_ts[symbol] = time.time()
                    log(f"  ✓ SL auf Entry gesetzt (Gewinn-Floor) @ {sl_str}")
                    telegram(
                        f"🔒 <b>SL auf Entry gesetzt — {symbol}</b>\n"
                        f"Script-Start: Position im Gewinn ({pnl_sign}{pnl}% Margin), "
                        f"kein SL gefunden\n"
                        f"SL: {sl_str} USDT (Schutz: kein Rückfall in Verlust)\n"
                        f"⚠️ Mit Ausstiegslinie abgleichen!"
                    )
                else:
                    log(f"  ✗ SL-Floor fehlgeschlagen: {res.get('msg', res)}")
                    # Fallback: trotzdem Auto-SL versuchen damit nicht ganz ungeschützt
                    sl_str  = round_price(_sl_auto, decimals)
                    sl_dist = abs(avg - _sl_auto) / avg * 100
                    log(f"  → Fallback Auto-SL @ {sl_str}")
                    api_post("/api/v2/mix/order/place-pos-tpsl", {
                        "symbol":               symbol,
                        "productType":          PRODUCT_TYPE,
                        "marginCoin":           MARGIN_COIN,
                        "holdSide":             direction,
                        "stopLossTriggerPrice": sl_str,
                        "stopLossTriggerType":  "mark_price",
                    })
            else:
                log(f"  ⚠ Mark {mark} hinter Entry {avg} — SL auf Entry nicht setzbar")
                telegram(f"⚠️ <b>{symbol}</b>: Position im Verlust, SL manuell setzen!")

        else:
            # Kein TP passiert, kein Gewinn → Auto-SL auf -25% Margin
            sl_str  = round_price(_sl_auto, decimals)
            sl_auto = float(sl_str)
            sl_dist = abs(avg - sl_auto) / avg * 100
            log(f"  ⚠ Kein SL → Auto-SL @ {sl_str} (-{sl_dist:.1f}% / -25% Margin)")
            res = api_post("/api/v2/mix/order/place-pos-tpsl", {
                "symbol":               symbol,
                "productType":          PRODUCT_TYPE,
                "marginCoin":           MARGIN_COIN,
                "holdSide":             direction,
                "stopLossTriggerPrice": sl_str,
                "stopLossTriggerType":  "mark_price",
            })
            if res.get("code") == "00000":
                sl_price = sl_auto
                log(f"  ✓ Auto-SL gesetzt @ {sl_str}")
                telegram(
                    f"🛡 <b>Auto-SL gesetzt — {symbol}</b>\n"
                    f"Script-Start: kein SL gefunden\n"
                    f"SL: {sl_str} USDT ({sl_dist:.1f}% Abstand)\n"
                    f"⚠️ Bitte mit Ausstiegslinie abgleichen!"
                )
            else:
                log(f"  ✗ Auto-SL fehlgeschlagen: {res.get('msg', res)}")
                telegram(
                    f"❌ <b>Kein SL — {symbol}</b>\n"
                    f"Auto-SL fehlgeschlagen. Bitte manuell setzen!\n"
                    f"Empfehlung: {sl_str} USDT ({sl_dist:.1f}%)"
                )

    sl_at_entry[symbol] = sl_is_entry

    # ── Phase 2b: Trailing SL prüfen (TP2 / TP3) ─────────────────────────
    # Detects whether TP2 or TP3 were triggered using TWO complementary methods:
    #
    # Method A — Fill-based (primary): detect_filled_tps() matches recent fills
    #   to expected TP prices. Reliable but limited to 48h history.
    #
    # Method B — Size-based (fallback): compares current size to peak_size.
    #   TP2 hit: size < peak * 0.67  (35% closed: 15% TP1 + 20% TP2)
    #   TP3 hit: size < peak * 0.37  (63% closed: 15+20+25% = 60%)
    #   Catches cases where fills are older than 48h.
    #
    # Both methods must agree on at LEAST TP2 before acting, OR fills alone
    # if they are present. Size-alone is used as fallback when no fills found.

    fill_tp2_hit = any(t["roi"] == TP2_ROI for t in filled_tps)
    fill_tp3_hit = any(t["roi"] == TP3_ROI for t in filled_tps)

    # Size-based detection (fallback)
    _peak = trade_data.get(symbol, {}).get("peak_size", size)
    _ref  = _peak if _peak > 0 else size
    size_tp3_hit = (size < _ref * 0.37) if _ref > 0 else False
    size_tp2_hit = (size < _ref * 0.67) if _ref > 0 else False

    # Combine: fill takes priority; size is fallback when no fills at all
    has_any_fills = len(close_fills) > 0
    tp3_confirmed = fill_tp3_hit or (size_tp3_hit and not has_any_fills)
    tp2_confirmed = fill_tp2_hit or (size_tp2_hit and not has_any_fills)

    if tp3_confirmed:
        exp_trail_sl  = calc_tp_price(avg, TP2_ROI, direction, leverage)
        exp_trail_lvl = 3
        exp_tp_label  = "TP3"
    elif tp2_confirmed:
        exp_trail_sl  = calc_tp_price(avg, TP1_ROI, direction, leverage)
        exp_trail_lvl = 2
        exp_tp_label  = "TP2"
    else:
        exp_trail_sl  = 0
        exp_trail_lvl = 0
        exp_tp_label  = ""

    if exp_trail_lvl > 0:
        current_trail = trailing_sl_level.get(symbol, 0)

        def _sl_at_level(sl_val: float, expected: float) -> bool:
            if expected == 0 or sl_val == 0:
                return False
            return abs(sl_val - expected) / expected * 100 <= 0.15

        def _sl_better_than(sl_val: float, expected: float) -> bool:
            if sl_val == 0:
                return False
            return sl_val >= expected if direction == "long" else sl_val <= expected

        if _sl_at_level(sl_price, exp_trail_sl) or _sl_better_than(sl_price, exp_trail_sl):
            trailing_sl_level[symbol] = max(current_trail, exp_trail_lvl)
            log(f"  {exp_tp_label} erkannt (fill/size), "
                f"Trailing SL Level {exp_trail_lvl}: SL @ {sl_price} bereits korrekt ✓")
        elif current_trail < exp_trail_lvl:
            log(f"  {exp_tp_label} erkannt (fill/size), "
                f"SL noch nicht auf Trailing-Niveau — nachziehen auf {exp_trail_sl:.5f}")
            set_sl_trailing(symbol, direction, exp_trail_sl,
                            level=exp_trail_lvl, cur_size=size)

    # Trade-Daten für spätere Auswertung und als SL-Fallback in place_tp_orders
    trade_data[symbol] = {
        "entry":     avg,
        "direction": direction,
        "leverage":  leverage,
        "sl":        sl_price,
        "peak_size": size,
        "open_ts":   int(time.time() * 1000),
    }

    # ── 2. TPs prüfen ─────────────────────────────────────────────────────
    # Nur die TPs prüfen, die laut Preisvergleich noch offen sein sollten.
    existing_tps = get_existing_tps(symbol)
    tp4_pos      = _get_pos_tp_price(symbol, direction)

    n_exp_pp   = state["n_expected_profit_plan"]   # erwartete profit_plan Orders
    tp4_exp    = state["tp4_expected"]             # TP4 Full-Close erwartet?
    n_act_pp   = len(existing_tps)
    tp4_ok     = (tp4_pos > 0) if tp4_exp else True

    # profit_plan korrekt wenn Anzahl stimmt UND Preise übereinstimmen
    pp_count_ok  = (n_act_pp == n_exp_pp)
    pp_price_ok  = tps_are_correct(existing_tps, avg, size, direction,
                                   leverage, decimals, mark) if pp_count_ok else False
    tps_correct  = pp_count_ok and pp_price_ok and tp4_ok

    log(f"  TPs: erwartet {n_exp_pp} profit_plan"
        f" + {'TP4 Full-Close' if tp4_exp else 'kein TP4 (passiert)'}"
        f" | vorhanden {n_act_pp} profit_plan"
        f" + {'TP4 @ ' + str(tp4_pos) if tp4_pos > 0 else 'kein TP4'}")

    if not tps_correct:
        reasons = []
        if not pp_count_ok:
            reasons.append(f"profit_plan: {n_act_pp} statt {n_exp_pp} erwartet")
        elif not pp_price_ok:
            reasons.append("profit_plan Preise stimmen nicht")
        if not tp4_ok:
            reasons.append("TP4 Full-Close fehlt")
        log(f"  ⚠ TPs inkorrekt ({'; '.join(reasons)}) → neu setzen")

        if size < 4:
            log(f"  ⚠ Position zu klein (Qty={size}, min. 4) — TPs manuell setzen")
        else:
            cancel_all_tp_orders(symbol)
            time.sleep(1)
            count, tp_prices = place_tp_orders(
                symbol, avg, size, direction, leverage, mark, known_sl=sl_price
            )
            status = "✓ Alle" if count == len(state["tps_remaining"]) else f"⚠ {count}"
            log(f"  {status} TPs gesetzt")
            if count > 0:
                telegram(
                    f"🔧 <b>TPs repariert — {symbol}</b>\n"
                    f"Script-Start: TPs fehlend/falsch\n"
                    f"Verbleibende TPs: "
                    f"{', '.join(t['label'] for t in state['tps_remaining'])}\n"
                    + "\n".join(tp_prices)
                )
    else:
        log(f"  ✓ TPs korrekt")

    # ── 3. TP1 ausgelöst (kombinierte Erkennung: Preis / Fill / SL-on-Entry) ──
    # tp1_done wurde bereits oben in Phase 1 bestimmt und ggf. SL korrigiert
    if tp1_done:
        log(f"  TP1 ausgelöst → storniere offene DCA-Orders")
        cancel_open_dca_orders(symbol, direction)
        return

    # ── 4. DCAs prüfen (nur wenn kein TP passiert) ────────────────────────
    existing_dcas = get_existing_dca_orders(symbol, direction)
    n_dca         = len(existing_dcas)

    if n_dca >= 2:
        dca_info = " | ".join(
            f"{o.get('price','?')} × {o.get('size','?')}" for o in existing_dcas[:2]
        )
        log(f"  ✓ {n_dca} DCA-Orders vorhanden: {dca_info}")
    else:
        log(f"  ⚠ Nur {n_dca}/2 DCA-Orders vorhanden → fehlende setzen")
        if sl_price == 0:
            log("  Kein SL-Preis bekannt — DCA-Platzierung übersprungen")
            telegram(
                f"⚠️ <b>DCA fehlt — {symbol}</b>\n"
                f"Kein SL-Preis bekannt, DCAs konnten nicht gesetzt werden.\n"
                f"Bitte DCA-Orders manuell platzieren."
            )
        else:
            base_size  = size   # aktuelle Grösse = Initial-Order (kein DCA gefüllt)
            dca_result = place_dca_orders(
                symbol, avg, sl_price, direction, base_size,
                balance=get_futures_balance(), leverage=leverage
            )
            log(f"  {len(dca_result)}/2 DCAs gesetzt")
            if dca_result:
                telegram(
                    f"🔧 <b>DCAs repariert — {symbol}</b>\n"
                    f"Script-Start: {n_dca}/2 DCA-Orders gefehlt\n"
                    + "\n".join(dca_result)
                )


def report_position_startup(pos: dict):
    """
    Startup-Prüfung: Liest alles aus Bitget, analysiert den Stand,
    ändert NICHTS auf Bitget, meldet Unstimmigkeiten per Telegram.

    Der Nutzer kann anschliessend /refresh SYMBOL senden um die
    gefundenen Probleme automatisch reparieren zu lassen.

    Geprüft wird:
      - SL vorhanden? (via Position.stopLossPrice + orders-plan-pending)
      - SL korrekt? (Entry-SL nach TP1, oder sinnvoller Schutz-SL)
      - TPs korrekt? (Anzahl und Preise passend zum Trade-Stand)
      - TP1 bereits ausgelöst? (preislich + fill-basiert + SL-basiert)
      - DCAs vorhanden wenn kein TP passiert?
    """
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    avg       = float(pos.get("openPriceAvg", 0))
    size      = float(pos.get("total", 0))
    leverage  = int(float(pos.get("leverage", 10)))
    decimals  = get_price_decimals(symbol)
    mark      = get_mark_price(symbol)

    log(f"  ── {symbol} | {direction.upper()} | "
        f"Avg={avg} | Qty={size} | {leverage}x | Mark={mark}")

    if avg == 0 or size == 0:
        log("  Ungültige Positionsdaten — übersprungen.")
        return

    # In-Memory State für den laufenden Polling-Loop vormerken
    last_known_avg[symbol]  = avg
    last_known_size[symbol] = size
    new_trade_done[symbol]  = True
    sl_at_entry[symbol]     = False

    issues   = []   # Gesammelte Probleme → Telegram-Alert

    # ── 1. Zuverlässig lesbare Daten von Bitget holen ─────────────────────
    # SL/TP-Plan-Orders sind via API nicht lesbar (alle Endpoints schlagen fehl).
    # Verlässliche Quellen: Fill-Historie, offene DCA-Orders, Positionsdaten.
    close_fills   = get_symbol_close_fills(symbol, since_hours=48)
    filled_tps    = detect_filled_tps(close_fills, avg, leverage, direction)
    fill_tp1      = any(t["roi"] == TP1_ROI for t in filled_tps)
    fill_tp2      = any(t["roi"] == TP2_ROI for t in filled_tps)
    existing_dcas = get_existing_dca_orders(symbol, direction)

    log(f"  DCAs={len(existing_dcas)} | Fills (48h)={len(close_fills)}")
    if filled_tps:
        log(f"  Fill-basierte TPs: {', '.join(t['label'] for t in filled_tps)}")

    # ── 2. Trade-Stand analysieren ────────────────────────────────────────
    state    = analyse_trade_state(avg, mark, leverage, direction)
    pnl      = state["pnl_roi_pct"]
    pnl_sign = "+" if pnl >= 0 else ""
    log(f"  ROI: {pnl_sign}{pnl}% | Mark: {mark}")

    # TP1-Erkennung: preislich ODER fill-basiert
    tp1_done = state["tp1_price_hit"] or fill_tp1

    # Trade-Daten für Polling-Loop speichern
    trade_data[symbol] = {
        "entry":     avg,
        "direction": direction,
        "leverage":  leverage,
        "sl":        0,   # nicht lesbar via API
        "peak_size": size,
        "open_ts":   int(time.time() * 1000),
    }
    if tp1_done:
        sl_at_entry[symbol] = False   # unklar, da SL nicht lesbar

    # ── 3. Nur prüfen was wirklich erkennbar ist ──────────────────────────
    # REGEL 1: TP1 ausgelöst + DCAs noch offen = klarer Fehler
    if tp1_done and existing_dcas:
        n = len(existing_dcas)
        issues.append(
            f"❌ TP1 ausgelöst, {n} DCA(s) noch offen → stornieren"
        )

    # REGEL 2: TP1 ausgelöst + DCAs noch offen → SL auf Entry hinweisen
    # Wenn DCAs bereits = 0: früherer /refresh hat Cleanup erledigt → kein Alarm
    if tp1_done and existing_dcas:
        tp_src = []
        if state["tp1_price_hit"]: tp_src.append("preislich")
        if fill_tp1:               tp_src.append("fill-basiert")
        filled_labels = ", ".join(t["label"] for t in filled_tps) if filled_tps else "TP1"
        issues.append(
            f"⚠️ {filled_labels} ausgelöst ({', '.join(tp_src)})"
            f" — SL auf Entry ({avg}) prüfen!"
        )

    # ── 4. Telegram-Report wenn Probleme gefunden ─────────────────────────
    if issues:
        header = (
            f"🔍 <b>Startup — {symbol}</b>\n"
            f"━━━━━━━━━━━━\n"
            f"{dir_icon(direction)} {direction.upper()} | "
            f"Entry: {avg} | {leverage}x\n"
            f"Mark: {mark} | ROI: {pnl_sign}{pnl}%\n"
            f"━━━━━━━━━━━━\n"
        )
        body = "\n".join(issues)
        telegram(header + body)
        telegram(f"<code>/refresh {symbol}</code>")
        log(f"  ⚠ {len(issues)} Problem(e) → Telegram gesendet")
    else:
        log(f"  ✓ Kein Handlungsbedarf erkannt")


def main():
    if not API_KEY or not SECRET_KEY or not PASSPHRASE:
        log("FEHLER: API_KEY, SECRET_KEY oder PASSPHRASE fehlen!")
        log("In Railway → Variables eintragen.")
        return

    log("DOMINUS Trade-Automatisierung v4.32 gestartet — mit finanzmathematischen Optimierungen")
    log(f"Intervall: {POLL_INTERVAL}s")
    log("Warte auf neue Trades...")
    log("─" * 55)

    # Gespeicherten State laden
    load_state()

    # Webhook-Server in separatem Thread starten
    t = threading.Thread(target=start_webhook_server, daemon=True)
    t.start()

    # ── Startup: Positionen lesen, prüfen, NICHT auto-korrigieren ──────────
    # report_position_startup() liest alle Daten aus Bitget (SL, TPs, Fills),
    # analysiert den Stand und sendet bei Problemen einen Telegram-Alert mit
    # dem Hinweis /refresh SYMBOL — ändert selbst nichts auf Bitget.
    positions = get_all_positions()
    if positions:
        log(f"{'─'*55}")
        log(f"Startup-Check: {len(positions)} offene Position(en) — nur Lesen/Prüfen")
        log(f"{'─'*55}")
        for pos in positions:
            report_position_startup(pos)
        log(f"{'─'*55}")
        log("Startup-Check abgeschlossen. Polling startet...")
        log("Gefundene Probleme wurden per Telegram gemeldet → /refresh SYMBOL zum Reparieren")
    else:
        log("Keine offenen Positionen. Warte auf ersten Trade...")

    last_check_ms = int(time.time() * 1000)

    while True:
        time.sleep(POLL_INTERVAL)
        try:
            # ── 0. Telegram-Befehle prüfen ─────────────────
            poll_telegram_commands()

            # ── 0a. Auto Daily Report um 23:59 ─────────────
            global daily_report_sent_date
            _now   = datetime.now()
            _today = _now.strftime("%Y-%m-%d")
            if _now.hour == 23 and _now.minute == 59 and daily_report_sent_date != _today:
                log("[Auto-Report] Täglicher P&L Report wird gesendet (23:59)...")
                try:
                    telegram(build_daily_report(_today))
                    daily_report_sent_date = _today
                    save_state()
                    log("[Auto-Report] ✓ Report gesendet")
                except Exception as _re:
                    log(f"[Auto-Report] ✗ Fehler: {_re}")

            # ── 0b. H4 Puffer flushen wenn Zeitfenster abgelaufen ──
            if h4_buffer:
                with h4_buffer_lock:
                    oldest = min((i["ts"] for i in h4_buffer), default=0)
                if oldest and __import__("time").time() - oldest >= H4_BUFFER_SEC:
                    flush_h4_buffer()

            # ── 1. Neue Fills (Nachkäufe / Einstiege) ──────
            fills      = get_recent_fills_all(last_check_ms)
            open_fills = [f for f in fills if f.get("tradeSide") == "open"]

            if open_fills:
                affected = set(
                    f.get("symbol") for f in open_fills if f.get("symbol")
                )
                last_check_ms = int(time.time() * 1000)
                time.sleep(2)

                for pos in get_all_positions():
                    sym = pos.get("symbol", "")
                    if sym not in affected:
                        continue

                    # Neuer Trade oder Nachkauf?
                    if not new_trade_done.get(sym, False):
                        log(f"Neuer Trade via Fill erkannt: {sym}")
                        setup_new_trade(pos)
                    else:
                        log(f"Nachkauf erkannt: {sym}")
                        log(f"══ TP-Anpassung: {sym} ══")
                        update_tp_for_position(
                            pos, f"Nachkauf ({len(open_fills)} Fill(s))"
                        )

            else:
                # ── 2. Positionen überwachen ────────────────
                for pos in get_all_positions():
                    sym       = pos.get("symbol", "")
                    cur_avg   = float(pos.get("openPriceAvg", 0))
                    cur_size  = float(pos.get("total", 0))
                    kno_size  = last_known_size.get(sym, 0)
                    direction = pos.get("holdSide", "long")

                    # Neuer Trade (noch nie via Fill gesehen)
                    if kno_size == 0 and cur_size > 0 and cur_avg > 0:
                        if not new_trade_done.get(sym, False):
                            log(f"Neuer Trade erkannt: {sym}")
                            setup_new_trade(pos)

                    elif kno_size > 0:
                        _peak = trade_data.get(sym, {}).get("peak_size", kno_size)
                        _ref  = _peak if _peak > 0 else kno_size
                        _trl  = trailing_sl_level.get(sym, 0)
                        _td   = trade_data.get(sym, {})
                        _avg  = _td.get("entry", cur_avg)
                        _lev  = _td.get("leverage", 10)
                        _dir  = _td.get("direction", direction)

                        if cur_size < _ref * 0.87 and not sl_at_entry.get(sym, False):
                            red = (_ref - cur_size) / _ref * 100
                            log(f"TP1 erkannt ({sym}): peak={_ref:.2f} → jetzt={cur_size:.2f} (-{red:.0f}%) → SL auf Entry")
                            trailing_sl_level[sym] = 1
                            set_sl_at_entry(sym, _dir, _avg, cur_size=cur_size)
                            last_known_size[sym] = cur_size

                        elif cur_size < _ref * 0.67 and _trl < 2:
                            red = (_ref - cur_size) / _ref * 100
                            tp1_price = calc_tp_price(_avg, TP1_ROI, _dir, _lev)
                            log(f"TP2 erkannt ({sym}): peak={_ref:.2f} → jetzt={cur_size:.2f} (-{red:.0f}%) → Trailing SL auf TP1 @ {tp1_price:.5f}")
                            set_sl_trailing(sym, _dir, tp1_price, level=2, cur_size=cur_size)
                            last_known_size[sym] = cur_size

                        elif cur_size < _ref * 0.37 and _trl < 3:
                            red = (_ref - cur_size) / _ref * 100
                            tp2_price = calc_tp_price(_avg, TP2_ROI, _dir, _lev)
                            log(f"TP3 erkannt ({sym}): peak={_ref:.2f} → jetzt={cur_size:.2f} (-{red:.0f}%) → Trailing SL auf TP2 @ {tp2_price:.5f}")
                            set_sl_trailing(sym, _dir, tp2_price, level=3, cur_size=cur_size)
                            last_known_size[sym] = cur_size

                        elif cur_size != kno_size:
                            last_known_size[sym] = cur_size

                # Geschlossene Positionen erkennen
                # (Position war bekannt, ist jetzt nicht mehr in get_all_positions)
                active_symbols = {p.get("symbol") for p in get_all_positions()}
                for sym in list(last_known_avg.keys()):
                    if sym not in active_symbols and last_known_avg.get(sym, 0) > 0:
                        handle_position_closed(sym, "SL oder TP4 ausgelöst")

        except requests.exceptions.ConnectionError:
            log("Verbindungsfehler. Retry in 30s...")
            time.sleep(30)
        except requests.exceptions.Timeout:
            log("Timeout. Retry...")
        except Exception as e:
            log(f"Unerwarteter Fehler: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
