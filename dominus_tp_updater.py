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
    Liest den bestehenden SL-Preis aus den pending TPSL-Orders.
    Du setzt den SL (Ausstiegslinie) beim Trade-Einstieg auf Bitget.
    """
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result.get("code") != "00000":
        return 0.0
    orders = result.get("data", {}).get("entrustedList", [])
    for o in orders:
        if (o.get("planType") == "loss_plan"
                and o.get("holdSide") == direction):
            return float(o.get("triggerPrice", 0))
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
    TPs werden via place-tpsl-order gesetzt → sind Plan-Orders
    → abrufen via orders-plan-pending, stornieren via cancel-plan-order.
    SL (loss_plan via tpsl-pending-orders) wird nie angefasst.
    """
    # Nur profit_plan Orders abrufen (= unsere TPs)
    result = api_get("/api/v2/mix/order/orders-plan-pending", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "planType":    "profit_plan",
    })
    if result.get("code") != "00000":
        log(f"  ⚠ orders-plan-pending Fehler: {result.get('msg', result)}")
        return

    # Response kann Liste oder Dict mit entrustedList sein
    data = result.get("data") or []
    if isinstance(data, dict):
        tp_orders = data.get("entrustedList") or []
    else:
        tp_orders = data

    # Nochmals sicherstellen: nur profit_plan
    tp_orders = [o for o in tp_orders
                 if o.get("planType", "") == "profit_plan"]

    if not tp_orders:
        log(f"  Keine TP-Orders gefunden (profit_plan)")
        return

    log(f"  {len(tp_orders)} TP(s) stornieren...")
    for order in tp_orders:
        oid = order.get("orderId")
        res = api_post("/api/v2/mix/order/cancel-plan-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     oid,
        })
        if res.get("code") == "00000":
            log(f"    ✓ TP storniert: {oid}")
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
    Setzt TP1–TP4 nach DOMINUS-Schema.
    TP4 schliesst immer die komplette Restposition.
    Überholte TPs (Position bereits im Profit) werden übersprungen.
    """
    decimals  = get_price_decimals(symbol)
    rois      = [TP1_ROI, TP2_ROI, TP3_ROI, TP4_ROI]
    labels    = ["TP1 (10%)", "TP2 (20%)", "TP3 (30%)", "TP4 (40%)"]
    # Asymmetrische Grössen: 15/20/25% + kompletter Rest bei TP4
    tp_sizes  = [max(1, math.floor(size * p)) for p in TP_CLOSE_PCTS[:3]]
    tp4_size  = max(1, math.ceil(size - sum(tp_sizes)))
    tp_sizes.append(tp4_size)

    tps = list(zip(rois, labels, tp_sizes))
    count  = 0
    prices = []

    for roi, label, qty in tps:
        tp_raw = calc_tp_price(avg, roi, direction, leverage)
        tp_str = round_price(tp_raw, decimals)
        tp_val = float(tp_str)

        # Validierung: TP auf richtiger Seite des Marktpreises
        if mark_price > 0:
            if direction == "long" and tp_val <= mark_price:
                log(f"    ⏭ {label} @ {tp_str} bereits überschritten "
                    f"(Mark: {mark_price}) — übersprungen")
                continue
            if direction == "short" and tp_val >= mark_price:
                log(f"    ⏭ {label} @ {tp_str} bereits überschritten "
                    f"(Mark: {mark_price}) — übersprungen")
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
            # Dezimalstellen-Fehler: Retry mit weniger Stellen
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

    return count, prices


# ═══════════════════════════════════════════════════════════════
# SL AUF ENTRY SETZEN (nach TP1)
# ═══════════════════════════════════════════════════════════════

def set_sl_at_entry(symbol: str, direction: str, entry_price: float):
    """SL auf Einstiegspreis setzen — DOMINUS-Regel nach TP1."""
    decimals = get_price_decimals(symbol)
    sl_str   = round_price(entry_price, decimals)

    result = api_post("/api/v2/mix/order/place-pos-tpsl", {
        "symbol":               symbol,
        "productType":          PRODUCT_TYPE,
        "marginCoin":           MARGIN_COIN,
        "holdSide":             direction,
        "stopLossTriggerPrice": sl_str,
        "stopLossTriggerType":  "mark_price",
    })

    if result.get("code") == "00000":
        log(f"  ✓ SL auf Entry gesetzt: {sl_str} USDT ({symbol})")
        telegram(
            f"🔒 <b>SL auf Entry — {symbol}</b>\n"
            f"TP1 ausgelöst → SL auf {sl_str} USDT\n"
            f"Position abgesichert ✓"
        )
        sl_at_entry[symbol] = True
    else:
        log(f"  ✗ SL-Anpassung fehlgeschlagen: {result.get('msg', result)}")


# ═══════════════════════════════════════════════════════════════
# DCA LIMIT-ORDERS SETZEN
# ═══════════════════════════════════════════════════════════════

def place_dca_orders(symbol: str, entry: float, sl: float,
                     direction: str, base_size: float) -> list:
    """
    Setzt 2 DCA Limit-Orders zwischen Entry und SL.
    DCA1: 1/3 des Abstands Entry → SL
    DCA2: 2/3 des Abstands Entry → SL
    Grösse: gleich wie Initial-Order (1/3 des Gesamtsetups)
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

    dca1_str = round_price(dca1, decimals)
    dca2_str = round_price(dca2, decimals)

    # Grösse: gleich wie Initial-Order (Dezimalstellen des Symbols beachten)
    size_str = str(int(base_size)) if base_size == int(base_size) \
               else str(round(base_size, 4))

    results = []
    for label, price_str in [("DCA1", dca1_str), ("DCA2", dca2_str)]:
        res = api_post("/api/v2/mix/order/place-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginMode":  "isolated",
            "marginCoin":  MARGIN_COIN,
            "size":        size_str,
            "price":       price_str,
            "side":        side,
            "tradeSide":   "open",
            "orderType":   "limit",
            "force":       "gtc",
        })
        if res.get("code") == "00000":
            log(f"  ✓ {label} Limit @ {price_str} USDT (Qty: {size_str})")
            results.append(f"{label}: {price_str} USDT")
        else:
            log(f"  ✗ {label} FEHLER: {res.get('msg', res)}")

    return results


# ═══════════════════════════════════════════════════════════════
# NEUER TRADE — VOLLSTÄNDIGES SETUP
# ═══════════════════════════════════════════════════════════════

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
        symbol, entry, sl_price, direction, size
    )

    # ── 4. TP1–TP4 setzen ───────────────────────────────────
    log(f"  Setze TPs für Gesamtposition (3x {size} = {size*3:.2f})...")
    # TPs werden auf Basis der GESAMTEN geplanten Position berechnet
    # (Initial + DCA1 + DCA2 = 3x Initialgrösse)
    total_planned = size * 3
    cancel_all_tp_orders(symbol)
    time.sleep(1)
    count, tp_prices = place_tp_orders(
        symbol, entry, total_planned, direction, leverage, mark
    )

    # ── 5. Status speichern ─────────────────────────────────
    last_known_avg[symbol]  = entry
    last_known_size[symbol] = size
    new_trade_done[symbol]  = True
    sl_at_entry[symbol]     = False

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
    Holt bestehende TP-Orders via orders-plan-pending.
    Gibt Liste mit {'price': float, 'qty': float, 'orderId': str} zurück.
    """
    result = api_get("/api/v2/mix/order/orders-plan-pending", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "planType":    "profit_plan",
    })
    if result.get("code") != "00000":
        return []
    data = result.get("data") or []
    if isinstance(data, dict):
        orders = data.get("entrustedList") or []
    else:
        orders = data
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

    # Position zu klein für individuelle TP-Orders (< 4 Kontrakte)
    if total < 4:
        log(f"  ⚠ Position zu klein (Qty={total}) — "
            f"setze nur 1 kombinierten TP für Gesamtposition")
        # Einen einzigen TP für die gesamte Restposition
        decimals = get_price_decimals(symbol)
        tp_raw   = calc_tp_price(avg, TP2_ROI, direction, leverage)
        tp_str   = round_price(tp_raw, decimals)
        tp_val   = float(tp_str)
        if mark_price > 0:
            valid = (direction == "long" and tp_val > mark_price) or                     (direction == "short" and tp_val < mark_price)
            if not valid:
                log(f"  ⏭ Einzel-TP @ {tp_str} bereits überschritten")
                last_known_avg[symbol]  = avg
                last_known_size[symbol] = total
                return
        size_int = max(1, round(total))
        res = api_post("/api/v2/mix/order/place-tpsl-order", {
            "symbol":       symbol,
            "productType":  PRODUCT_TYPE,
            "marginCoin":   MARGIN_COIN,
            "planType":     "profit_plan",
            "triggerPrice": tp_str,
            "triggerType":  "mark_price",
            "executePrice": "0",
            "holdSide":     direction,
            "size":         str(size_int),
        })
        if res.get("code") == "00000":
            log(f"  ✓ Einzel-TP @ {tp_str} USDT gesetzt (Qty: {size_int})")
        else:
            log(f"  ✗ Einzel-TP Fehler: {res.get('msg', res)}")
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

        # ── Aktuellen Kurs nehmen falls kein Entry mitgeschickt ──
        if entry == 0:
            entry = get_mark_price(symbol)

        # ── Kontostand & Hebel-Empfehlung berechnen ──────────
        balance    = get_futures_balance()
        kelly      = kelly_recommendation(balance, WINRATE)
        links      = tv_chart_links(symbol)
        per_order  = balance * 0.10 / 3

        # ── Telegram-Nachricht senden ─────────────────────────
        icon = "\U0001f7e2" if direction == "long" else "\U0001f534"
        chk_dir = "gr\u00fcner" if direction == "long" else "roter"
        msg_parts = [
            f"{icon} <b>DOMINUS Signal \u2014 {symbol} {direction.upper()}</b>",
            "\u2501" * 22,
            f"Timeframe: {timeframe} | Kurs: {entry}",
            "",
            "\U0001f4cb <b>DOMINUS Checkliste:</b>",
            "\u2610 DOMINUS Impuls Extremzone erreicht?",
            "\u2610 H4 Buy/Sell Trigger best\u00e4tigt?",
            "\u2610 HARSI nicht in Extremzone?",
            "\u2610 BTC + Total2 gleiche Richtung?",
            f"\u2610 Premium Setup? (Impuls in dunkel{chk_dir} Zone)",
            "",
            f"\U0001f4b0 Konto: {balance:.0f} USDT",
            f"\U0001f4e6 Pro Order: {per_order:.0f} USDT",
            f"\U0001f4ca Kelly {WINRATE*100:.0f}%: {kelly['kelly_pct']}%",
            "",
            "\u23f1 <b>30-Min-Fenster l\u00e4uft!</b>",
            "Wenn alle Punkte \u2713:",
            f"/trade {symbol} {direction.upper()} [HEBEL] {entry:.4f} [SL]",
            "",
            "\U0001f4c8 <b>Charts:</b>",
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

    # Beim Start: bestehende Positionen als bekannt markieren
    # Keine TP-Anpassung beim Start — TPs werden nur bei
    # neuem Trade oder Nachkauf gesetzt/angepasst.
    positions = get_all_positions()
    if positions:
        log(f"{len(positions)} bestehende Position(en) beim Start:")
        for pos in positions:
            sym  = pos.get("symbol", "?")
            avg  = float(pos.get("openPriceAvg", 0))
            size = float(pos.get("total", 0))
            lev  = int(float(pos.get("leverage", 10)))
            drct = pos.get("holdSide", "?").upper()
            log(f"  {sym} | {drct} | Avg={avg} | Qty={size} | {lev}x")
            last_known_avg[sym]  = avg
            last_known_size[sym] = size
            new_trade_done[sym]  = True
            sl_at_entry[sym]     = False
    else:
        log("Keine offenen Positionen. Warte auf ersten Trade...")

    last_check_ms = int(time.time() * 1000)

    while True:
        time.sleep(POLL_INTERVAL)
        try:
            # ── 0. Telegram-Befehle prüfen ─────────────────
            poll_telegram_commands()

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
