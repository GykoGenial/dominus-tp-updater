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


# ═══════════════════════════════════════════════════════════════
# BASIS-FUNKTIONEN
# ═══════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


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

def get_sl_price(symbol: str, direction: str) -> float:
    """
    Liest den SL-Preis direkt aus den Positionsdaten — zuverlässigste Methode.
    Bitget speichert den SL im Feld autoMarginReduction/stopLossPrice
    der Position selbst, unabhängig davon wie er gesetzt wurde
    (manuell loss_plan, via place-pos-tpsl pos_loss, etc.)

    Fallback: tpsl-pending-orders mit allen bekannten SL-Typen.
    """
    # Primär: SL aus Positionsdaten lesen
    result = api_get("/api/v2/mix/position/single-position", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") == "00000":
        for pos in (result.get("data") or []):
            if pos.get("holdSide") == direction:
                # Bitget liefert SL in stopLossPrice oder stopSurplusPrice
                sl = float(pos.get("stopLossPrice", 0) or 0)
                if sl > 0:
                    log(f"  SL aus Position: {sl} ({direction})")
                    return sl

    # Fallback: pending orders (alle SL-Typen)
    result2 = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result2.get("code") == "00000":
        orders = (result2.get("data") or {}).get("entrustedList") or []
        SL_TYPES = {"loss_plan", "pos_loss", "moving_plan"}
        for o in orders:
            if o.get("planType") in SL_TYPES and o.get("holdSide") == direction:
                price = float(o.get("triggerPrice", 0) or 0)
                if price > 0:
                    log(f"  SL aus Orders: {o.get('planType')} @ {price}")
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

    TPs werden via place-tpsl-order gesetzt → erscheinen in tpsl-pending-orders
    (NICHT in orders-plan-pending — das ist ein anderer Endpoint für andere Order-Typen).
    Stornierung via cancel-plan-order.
    SL (loss_plan, pos_loss) wird nie angefasst.
    """
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result.get("code") != "00000":
        log(f"  ⚠ tpsl-pending Fehler: {result.get('msg', result)}")
        return

    orders = (result.get("data") or {}).get("entrustedList") or []

    # Nur profit_plan — SL-Typen (loss_plan, pos_loss) nicht anfassen
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
                    mark_price: float) -> tuple:
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

    for roi, label, pct in partial_tps:
        tp_raw = calc_tp_price(avg, roi, direction, leverage)
        tp_str = round_price(tp_raw, decimals)
        tp_val = float(tp_str)
        qty    = max(1, math.floor(size * pct))

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
        # Aktuellen SL-Preis lesen um ihn mitzuschicken
        current_sl = get_sl_price(symbol, direction)
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
        f"━━━━━━━━━━━━━━━━━━━━━━",
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
        f"Richtung: {direction.upper()}  |  {leverage}x Hebel",
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

    # Internen Status zurücksetzen
    last_known_avg.pop(symbol, None)
    last_known_size.pop(symbol, None)
    new_trade_done.pop(symbol, None)
    sl_at_entry.pop(symbol, None)
    trade_data.pop(symbol, None)

    # Noch offene TP-Orders aufräumen (Sicherheit)
    cancel_all_tp_orders(symbol)


def _get_pos_tp_price(symbol: str, direction: str) -> float:
    """
    Liest den Positions-Level TP4-Preis (takeProfitPrice) direkt
    aus den Positionsdaten — nicht aus den pending TPSL-Orders.
    """
    result = api_get("/api/v2/mix/position/single-position", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") == "00000":
        for pos in (result.get("data") or []):
            if pos.get("holdSide") == direction:
                tp = float(pos.get("takeProfitPrice", 0) or 0)
                if tp > 0:
                    return tp
    return 0.0


def set_sl_at_entry(symbol: str, direction: str, entry_price: float):
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
        # DCA Limit-Orders stornieren — nicht mehr nötig nach TP1
        cancel_open_dca_orders(symbol, direction)
        telegram(
            f"🔒 <b>SL auf Entry — {symbol}</b>\n"
            f"TP1 ausgelöst → SL auf {sl_str} USDT\n"
            f"✓ Position abgesichert\n"
            f"✓ DCA-Orders storniert"
        )
    else:
        log(f"  ✗ SL-Anpassung fehlgeschlagen: {result.get('msg', result)}")


# ═══════════════════════════════════════════════════════════════
# DCA LIMIT-ORDERS SETZEN
# ═══════════════════════════════════════════════════════════════

def place_dca_orders(symbol: str, entry_sl: float, sl: float,
                     direction: str, base_size: float,
                     balance: float = 0, leverage: int = 10,
                     entry: float = 0) -> list:
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

    # 10%-Regel prüfen und ggf. korrigieren
    # DCA1 + DCA2 müssen zusammen mit Market innerhalb von 10% des Kapitals bleiben
    if balance > 0 and leverage > 0:
        total_limit  = balance * 0.10
        market_notional = base_size * entry if entry > 0 else 0
        market_margin   = market_notional / leverage if leverage > 0 else 0
        remaining_margin = total_limit - market_margin

        if remaining_margin > 0:
            # Verbleibende Margin auf DCA1 (30/80) und DCA2 (50/80) aufteilen
            dca1_margin_max = remaining_margin * (0.30 / 0.80)
            dca2_margin_max = remaining_margin * (0.50 / 0.80)
            dca1_size_max   = (dca1_margin_max * leverage) / dca1 if dca1 > 0 else dca1_size
            dca2_size_max   = (dca2_margin_max * leverage) / dca2 if dca2 > 0 else dca2_size
            # Kleinere von berechneter und maximaler Grösse nehmen
            dca1_size = min(dca1_size, dca1_size_max)
            dca2_size = min(dca2_size, dca2_size_max)
            dca1_size = max(1, round(dca1_size, 4))
            dca2_size = max(1, round(dca2_size, 4))

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
            place_tp_orders(symbol, entry, size, direction, leverage, mark)
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
        balance=balance, leverage=leverage, entry=entry
    )

    # ── 4. TP1–TP4 setzen ───────────────────────────────────
    # TPs auf Basis der tatsächlichen aktuellen Position setzen.
    # DCA-Orders sind noch nicht gefüllt — wird automatisch angepasst wenn sie füllen.
    log(f"  Setze TPs für aktuelle Position (Qty={size})...")
    cancel_all_tp_orders(symbol)
    time.sleep(1)
    count, tp_prices = place_tp_orders(
        symbol, entry, size, direction, leverage, mark
    )

    # ── 5. Status speichern ─────────────────────────────────
    last_known_avg[symbol]  = entry
    last_known_size[symbol] = size
    new_trade_done[symbol]  = True
    sl_at_entry[symbol]     = False
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
        f"🚀 <b>Neuer Trade — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Richtung: {direction.upper()} | Hebel: {leverage}x\n"
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
    Holt bestehende TP-Orders via tpsl-pending-orders.
    Gibt Liste mit {'price': float, 'qty': float, 'orderId': str} zurück.
    """
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result.get("code") != "00000":
        return []
    orders = (result.get("data") or {}).get("entrustedList") or []
    tps = []
    for o in orders:
        if o.get("planType") == "profit_plan":
            tps.append({
                "price":   float(o.get("triggerPrice", 0)),
                "qty":     float(o.get("size", 0)),
                "orderId": o.get("orderId", ""),
            })
    # Nach Preis sortieren
    tps.sort(key=lambda x: x["price"],
             reverse=(True if "short" in str(o.get("holdSide","")) else False))
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
    Prüft bestehende TPs, löscht nur wenn nötig und setzt neu.
    Ablauf: Bestehende TPs abrufen → prüfen → bei Abweichung: löschen + neu setzen.
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

    # TPs bei Nachkauf immer neu setzen (alter Avg-Preis → neuer Avg-Preis)
    log(f"  Setze TPs neu (Grund: {reason})")

    # Mindest-Grösse berechnen: mind. 4 Kontrakte für 4 TPs nötig
    # (TP1=15%: floor(4*0.15)=0 → max(1,0)=1 → aber 4 × 1 = 4 < size nötig)
    min_size_for_tps = 4
    if total < min_size_for_tps:
        log(f"  ⚠ Position zu klein (Qty={total}) — "
            f"TPs werden nicht gesetzt (min. {min_size_for_tps} Kontrakte nötig)")
        log(f"  Bitte Position manuell überwachen oder grösser eröffnen")
        last_known_avg[symbol]  = avg
        last_known_size[symbol] = total
        return

    cancel_all_tp_orders(symbol)
    time.sleep(1)
    count, prices = place_tp_orders(
        symbol, avg, total, direction, leverage, mark
    )

    if count > 0:
        status = "✓ Alle 4" if count == 4 else f"⚠ {count}/4"
        log(f"  {status} TPs für {symbol} gesetzt.")
        telegram(
            f"♻️ <b>TPs aktualisiert — {symbol}</b>\n"
            f"Richtung: {direction.upper()}\n"
            f"Neuer Avg: {avg} USDT | Hebel: {leverage}x\n"
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
        f"━━━━━━━━━━━━━━━━━━━━━━",
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
        f"━━━━━━━━━━━━━━━━━━━━━━",
        f"Richtung: {direction.upper()} | Hebel: {leverage}x",
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
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "/berechnen — Kontostand + Positionen + BTC/Total2 Links\n"
        "/trade SYMBOL LONG|SHORT HEBEL ENTRY SL\n"
        "   → Setup berechnen + alle Chart-Links\n"
        "   Beispiel: /trade ETHUSDT LONG 10 2850 2700\n"
        "/status — Kurzstatus aller Positionen\n"
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
        icon = "🔒" if secured else "📈"
        lines.append(
            f"{icon} {sym} {drct} {lev}x | "
            f"Qty={qty:.2f} | Mark={mark} | PnL={pnl:+.2f} USDT"
        )
    reply("\n".join(lines))


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
        msg = update.get("message") or update.get("edited_message")
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
        "\u2501" * 22,
    ]
    if longs:
        lines.append("\U0001f7e2 <b>LONG:</b>")
        for item in longs:
            lnk = tv_chart_links(item["symbol"])
            lines.append(f"  \u2022 {item['symbol']}  @ {item['entry']:.5f}")
            lines.append(f"    {lnk['coin_h4']}")
    if longs and shorts:
        lines.append("")
    if shorts:
        lines.append("\U0001f534 <b>SHORT:</b>")
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
        icon      = "\U0001f7e2" if direction == "long" else "\U0001f534"
        msg_parts = [
            f"{icon} <b>H2 Signal \u2014 {symbol} {direction.upper()}</b>",
            "\u2501" * 22,
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

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "running",
                        "version": "v4.1"}), 200

    port = int(os.environ.get("PORT", 8080))
    log(f"Webhook-Server gestartet auf Port {port}")
    log(f"Endpoint: /webhook?token={WEBHOOK_SECRET}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)



# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    if not API_KEY or not SECRET_KEY or not PASSPHRASE:
        log("FEHLER: API_KEY, SECRET_KEY oder PASSPHRASE fehlen!")
        log("In Railway → Variables eintragen.")
        return

    log("DOMINUS Trade-Automatisierung v4.1 gestartet — mit finanzmathematischen Optimierungen")
    log(f"Intervall: {POLL_INTERVAL}s")
    log("Warte auf neue Trades...")
    log("─" * 55)

    # Webhook-Server in separatem Thread starten
    t = threading.Thread(target=start_webhook_server, daemon=True)
    t.start()

    # Beim Start: bestehende Positionen als bekannt markieren.
    # SL wird einmalig geprüft — falls nicht gesetzt: Auto-SL (-25%).
    # TPs werden beim Start nie angepasst.
    positions = get_all_positions()
    if positions:
        log(f"{len(positions)} bestehende Position(en) beim Start:")
        for pos in positions:
            sym       = pos.get("symbol", "?")
            avg       = float(pos.get("openPriceAvg", 0))
            size      = float(pos.get("total", 0))
            lev       = int(float(pos.get("leverage", 10)))
            direction = pos.get("holdSide", "long")
            log(f"  {sym} | {direction.upper()} | "
                f"Avg={avg} | Qty={size} | {lev}x")

            last_known_avg[sym]  = avg
            last_known_size[sym] = size
            new_trade_done[sym]  = True
            # sl_at_entry wird erst nach SL-Prüfung gesetzt (unten)

            # ── SL prüfen — einmalig beim Start ──────────────
            if avg == 0 or size == 0:
                sl_at_entry[sym] = False
                continue

            sl_existing = get_sl_price(sym, direction)

            if sl_existing > 0:
                sl_dist = abs(avg - sl_existing) / avg * 100
                # Prüfen ob SL bereits auf Entry steht (Toleranz 0.15%)
                # Das bedeutet TP1 wurde bereits ausgelöst
                at_entry = sl_dist <= 0.15
                sl_at_entry[sym] = at_entry
                if at_entry:
                    log(f"  ✓ SL auf Entry @ {sl_existing} "
                        f"— TP1 bereits ausgelöst, SL wird nicht verändert")
                else:
                    log(f"  ✓ SL vorhanden @ {sl_existing} "
                        f"({sl_dist:.1f}% Abstand) — wird nicht verändert")
            else:
                # Kein SL gefunden → prüfen ob TPs bereits teils ausgelöst
                # Indikator: bestehende TPs starten nicht bei TP1-Preis
                existing_tps = get_existing_tps(sym)
                decimals     = get_price_decimals(sym)
                n_tps        = len(existing_tps)

                # Zuverlässigste Erkennung: Anzahl verbleibender TPs
                # Original = 4 TPs → nach TP1 nur noch 3 → nach TP2 nur noch 2
                # Wenn < 4 TPs vorhanden → mindestens TP1 wurde ausgelöst
                # → SL muss auf Entry stehen
                tp1_already_hit = n_tps < 4
                if tp1_already_hit:
                    log(f"  TP1 bereits ausgelöst: {n_tps}/4 TPs noch offen "
                        f"→ SL auf Entry")
                else:
                    log(f"  Alle 4 TPs noch offen → normaler Trade")

                if tp1_already_hit:
                    # SL auf Entry setzen — Position ist bereits abgesichert
                    sl_at_entry[sym] = True

                    # ── DCA-Orders stornieren ─────────────────────
                    # (wurden beim ursprünglichen Trade-Einstieg gesetzt,
                    #  sind nach TP1 nicht mehr nötig)
                    log(f"  Storniere offene DCA-Orders...")
                    cancel_open_dca_orders(sym, direction)

                    # ── SL auf Entry setzen ───────────────────────
                    # Prüfen ob Mark-Preis bereits unter Entry (Long)
                    # → Bitget lehnt SL über Marktpreis ab
                    mark = get_mark_price(sym)
                    sl_valid = (direction == "long"  and avg <= mark) or                                (direction == "short" and avg >= mark)

                    if not sl_valid:
                        log(f"  ⚠ Mark {mark} bereits hinter Entry {avg} "
                            f"— SL auf Entry nicht setzbar, Position im Verlust")
                        telegram(
                            f"\u26a0\ufe0f <b>SL auf Entry nicht möglich \u2014 {sym}</b>\n"
                            f"Mark-Preis {mark} bereits hinter Entry {avg}\n"
                            f"Position manuell überwachen!"
                        )
                    else:
                        sl_str = round_price(avg, decimals)
                        log(f"  SL auf Entry setzen @ {sl_str} "
                            f"(TP1 bereits ausgelöst)")
                        res = api_post("/api/v2/mix/order/place-pos-tpsl", {
                            "symbol":               sym,
                            "productType":          PRODUCT_TYPE,
                            "marginCoin":           MARGIN_COIN,
                            "holdSide":             direction,
                            "stopLossTriggerPrice": sl_str,
                            "stopLossTriggerType":  "mark_price",
                        })
                        if res.get("code") == "00000":
                            log(f"  ✓ SL auf Entry gesetzt @ {sl_str} USDT")
                            telegram(
                                f"\U0001f512 <b>SL auf Entry gesetzt \u2014 {sym}</b>\n"
                                f"Script-Start: TP1 bereits ausgelöst\n"
                                f"SL: {sl_str} USDT | DCA-Orders storniert \u2713"
                            )
                        else:
                            log(f"  ✗ SL auf Entry fehlgeschlagen: "
                                f"{res.get('msg', res)}")
                else:
                    # Kein TP ausgelöst → Auto-SL -25%
                    sl_at_entry[sym] = False
                    factor   = 0.25 / lev
                    sl_auto  = avg * (1 - factor) if direction == "long"                                else avg * (1 + factor)
                    sl_str   = round_price(sl_auto, decimals)
                    sl_auto  = float(sl_str)
                    sl_dist  = abs(avg - sl_auto) / avg * 100
                    log(f"  ⚠ Kein SL — Auto-SL @ {sl_str} ({sl_dist:.1f}%)")
                    res = api_post("/api/v2/mix/order/place-pos-tpsl", {
                        "symbol":               sym,
                        "productType":          PRODUCT_TYPE,
                        "marginCoin":           MARGIN_COIN,
                        "holdSide":             direction,
                        "stopLossTriggerPrice": sl_str,
                        "stopLossTriggerType":  "mark_price",
                    })
                    if res.get("code") == "00000":
                        log(f"  ✓ Auto-SL gesetzt @ {sl_str} USDT")
                        links = tv_chart_links(sym)
                        telegram(
                            f"\U0001f6e1 <b>Auto-SL gesetzt \u2014 {sym}</b>\n"
                            f"Script-Start: kein SL gefunden\n\n"
                            f"SL: {sl_str} USDT ({sl_dist:.1f}% Abstand)\n"
                            f"Entry: {avg} | Hebel: {lev}x\n\n"
                            f"\u26a0\ufe0f Bitte pr\u00fcfen ob SL mit "
                            f"Ausstiegslinie \u00fcbereinstimmt!\n"
                            f"H4: {links['coin_h4']}"
                        )
                    else:
                        log(f"  ✗ Auto-SL fehlgeschlagen: "
                            f"{res.get('msg', res)}")
    else:
        log("Keine offenen Positionen. Warte auf ersten Trade...")

    last_check_ms = int(time.time() * 1000)

    while True:
        time.sleep(POLL_INTERVAL)
        try:
            # ── 0. Telegram-Befehle prüfen ─────────────────
            poll_telegram_commands()

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

                    # TP1-Erkennung: Grösse ~25% kleiner → SL auf Entry
                    elif (kno_size > 0
                            and cur_size < kno_size * 0.85
                            and not sl_at_entry.get(sym, False)):
                        red = (kno_size - cur_size) / kno_size * 100
                        log(f"TP1 erkannt ({sym}): "
                            f"{kno_size:.2f} → {cur_size:.2f} "
                            f"(-{red:.0f}%) → SL auf Entry")
                        set_sl_at_entry(sym, direction, cur_avg)
                        last_known_size[sym] = cur_size

                    # Grösse aktualisieren (Position teilweise geschlossen)
                    elif kno_size > 0 and cur_size != kno_size:
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
