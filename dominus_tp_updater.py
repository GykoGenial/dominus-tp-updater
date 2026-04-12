"""
DOMINUS Trade-Automatisierung v4
══════════════════════════════════════════════════════════════
Vollautomatisches Setup nach DOMINUS-Strategie (Handbuch März 2026)
Finanzmathematische Optimierungen:
  ① Hebel-Empfehlung  — Hebel = 25 / SL-Abstand%
  ② R:R-Filter        — kein Trade unter 1.5 R:R
  ③ Kelly-Kriterium   — optimale Positionsgrösse
  ④ Asymm. TPs        — 15/20/25/40% statt 25/25/25/25%

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
import requests
import math
from datetime import datetime

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
    """Alle bestehenden TP-Orders für ein Symbol stornieren."""
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    if result.get("code") != "00000":
        return
    orders    = result.get("data", {}).get("entrustedList", [])
    tp_orders = [o for o in orders if o.get("planType") == "profit_plan"]
    if not tp_orders:
        return
    log(f"  {len(tp_orders)} bestehende TP(s) stornieren...")
    for order in tp_orders:
        res = api_post("/api/v2/mix/order/cancel-plan-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     order.get("orderId"),
        })
        status = "✓" if res.get("code") == "00000" else "✗"
        log(f"    {status} {order.get('orderId')}")


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
        log(f"  ⚠ Kein SL gefunden! Bitte SL (Ausstiegslinie) auf "
            f"Bitget setzen. DCA-Orders werden NICHT gesetzt.")
        telegram(
            f"⚠️ <b>Neuer Trade: {symbol} {direction.upper()}</b>\n"
            f"Entry: {entry} | Hebel: {leverage}x\n\n"
            f"❌ Kein SL gesetzt!\n"
            f"Bitte SL (Ausstiegslinie) auf Bitget setzen.\n"
            f"DCA-Orders wurden nicht gesetzt."
        )
        # Nur TPs setzen
        cancel_all_tp_orders(symbol)
        time.sleep(1)
        count, tp_prices = place_tp_orders(
            symbol, entry, size, direction, leverage, mark
        )
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
        f"({order_margin*3/balance*100:.1f}% Kapital)"
        if balance > 0 else ""
    )
    telegram(msg)

    status = "✓ Alle 4" if count == 4 else f"⚠ {count}/4"
    log(f"  {status} TPs gesetzt | "
        f"{len(dca_results)}/2 DCA-Orders gesetzt")


# ═══════════════════════════════════════════════════════════════
# TP UPDATE (bei Nachkauf)
# ═══════════════════════════════════════════════════════════════

def update_tp_for_position(pos: dict, reason: str):
    """TPs neu berechnen nach Nachkauf."""
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    avg       = float(pos.get("openPriceAvg", 0))
    total     = float(pos.get("total", 0))
    leverage  = int(float(pos.get("leverage", 10)))
    mark      = get_mark_price(symbol)

    log(f"  {symbol} | {direction.upper()} | Avg={avg} | "
        f"Qty={total} | Hebel={leverage}x | Mark={mark}")

    if avg == 0 or total == 0:
        log("  Ungültige Position.")
        return

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

    # Beim Start: bestehende Positionen als bekannt markieren
    # (kein neues Setup — Trades laufen bereits)
    positions = get_all_positions()
    if positions:
        log(f"{len(positions)} bestehende Position(en) beim Start "
            f"(werden nicht neu aufgesetzt):")
        for pos in positions:
            sym  = pos.get("symbol", "?")
            avg  = float(pos.get("openPriceAvg", 0))
            size = float(pos.get("total", 0))
            log(f"  {sym} | Avg={avg} | Qty={size}")
            last_known_avg[sym]  = avg
            last_known_size[sym] = size
            new_trade_done[sym]  = True  # keine DCA-Orders mehr setzen
            # TPs dennoch setzen falls fehlend
            update_tp_for_position(pos, "Script-Start")
    else:
        log("Keine offenen Positionen. Warte auf ersten Trade...")

    last_check_ms = int(time.time() * 1000)

    while True:
        time.sleep(POLL_INTERVAL)
        try:
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
                # ── 2. Positionen auf Änderungen prüfen ────
                for pos in get_all_positions():
                    sym       = pos.get("symbol", "")
                    cur_avg   = float(pos.get("openPriceAvg", 0))
                    cur_size  = float(pos.get("total", 0))
                    kno_avg   = last_known_avg.get(sym, 0)
                    kno_size  = last_known_size.get(sym, 0)
                    direction = pos.get("holdSide", "long")

                    # Neuer Trade (noch nie gesehen)
                    if kno_size == 0 and cur_size > 0 and cur_avg > 0:
                        if not new_trade_done.get(sym, False):
                            log(f"Neuer Trade erkannt: {sym}")
                            setup_new_trade(pos)

                    # TP1-Erkennung: Grösse ~25% kleiner
                    elif (kno_size > 0
                            and cur_size < kno_size * 0.85
                            and not sl_at_entry.get(sym, False)):
                        red = (kno_size - cur_size) / kno_size * 100
                        log(f"TP1 erkannt ({sym}): "
                            f"{kno_size:.2f} → {cur_size:.2f} "
                            f"(-{red:.0f}%) → SL auf Entry")
                        set_sl_at_entry(sym, direction, cur_avg)
                        last_known_size[sym] = cur_size

                    # Avg-Preis geändert: Nachkauf (Fill-History missed)
                    elif (kno_size > 0
                            and cur_avg > 0
                            and cur_avg != kno_avg):
                        log(f"Avg-Preis geändert ({sym}): "
                            f"{kno_avg} → {cur_avg}")
                        log(f"══ TP-Anpassung: {sym} ══")
                        sl_at_entry[sym] = False
                        update_tp_for_position(pos, "Avg-Preis Änderung")

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
