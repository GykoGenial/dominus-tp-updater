"""
DOMINUS TP Auto-Updater v3
══════════════════════════════════════════════════════════════
Erkennt automatisch ALLE offenen Positionen auf Bitget und
passt die TP-Level bei jedem Nachkauf automatisch an.

Fixes v3:
  - Dezimalstellen werden pro Symbol von Bitget abgefragt
  - Short-TPs werden korrekt unter Marktpreis gesetzt
  - TP-Orders werden mit korrektem Endpoint abgerufen
  - Marktpreis-Validierung vor jedem TP-Setzen

RAILWAY VARIABLES (nur diese 3 nötig):
  API_KEY     → Bitget API Key
  SECRET_KEY  → Bitget Secret Key
  PASSPHRASE  → Bitget Passphrase
  (optional: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
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
# KONFIGURATION
# ═══════════════════════════════════════════════════════════════

API_KEY    = os.environ.get("API_KEY", "")
SECRET_KEY = os.environ.get("SECRET_KEY", "")
PASSPHRASE = os.environ.get("PASSPHRASE", "")

PRODUCT_TYPE = "usdt-futures"
MARGIN_COIN  = "USDT"

TP1_ROI      = 0.10
TP2_ROI      = 0.20
TP3_ROI      = 0.30
TP4_ROI      = 0.40
TP_CLOSE_PCT = 0.25
POLL_INTERVAL = 20

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BASE_URL = "https://api.bitget.com"

# Cache: letzter bekannter Avg-Preis pro Symbol
last_known_avg: dict = {}
# Cache: letzte bekannte Positionsgrösse pro Symbol (für TP1-Erkennung)
last_known_size: dict = {}
# Cache: ob SL bereits auf Entry gezogen wurde
sl_at_entry: dict = {}
# Cache: Preis-Präzision pro Symbol (Anzahl Dezimalstellen)
price_decimals_cache: dict = {}


# ═══════════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def telegram(msg: str):
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
    message = timestamp + method.upper() + path + body
    sig = hmac.new(
        SECRET_KEY.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.b64encode(sig).decode("utf-8")


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
# PREIS-PRÄZISION — vom Bitget Contract abrufen
# ═══════════════════════════════════════════════════════════════

def get_price_decimals(symbol: str) -> int:
    """
    Holt die erlaubten Dezimalstellen für einen Coin von Bitget.
    Ergebnis wird gecacht um API-Calls zu sparen.
    Fallback: aus dem Preis selbst ableiten.
    """
    if symbol in price_decimals_cache:
        return price_decimals_cache[symbol]

    result = api_get("/api/v2/mix/market/contracts", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })

    decimals = 4  # Fallback
    try:
        contracts = result.get("data", [])
        if contracts:
            price_place = contracts[0].get("pricePlace", "4")
            decimals = int(price_place)
    except Exception:
        pass

    price_decimals_cache[symbol] = decimals
    log(f"  Preis-Präzision {symbol}: {decimals} Dezimalstellen")
    return decimals


def round_price(price: float, decimals: int) -> str:
    """Rundet auf die erlaubten Dezimalstellen des Symbols."""
    return f"{price:.{decimals}f}"


# ═══════════════════════════════════════════════════════════════
# MARKTPREIS — aktuellen Kurs abrufen
# ═══════════════════════════════════════════════════════════════

def get_mark_price(symbol: str) -> float:
    """Aktuellen Richtpreis (Mark Price) von Bitget abrufen."""
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


# ═══════════════════════════════════════════════════════════════
# POSITIONEN & FILLS
# ═══════════════════════════════════════════════════════════════

def get_all_positions() -> list:
    """Alle offenen Positionen abrufen — kein fixes Symbol nötig."""
    result = api_get("/api/v2/mix/position/all-position", {
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") != "00000":
        log(f"Fehler Positionen: {result.get('msg', result)}")
        return []
    return [p for p in result.get("data", [])
            if float(p.get("total", 0)) > 0]


def get_recent_fills_all(since_ms: int) -> list:
    """Kürzlich ausgeführte Orders über alle Symbole."""
    result = api_get("/api/v2/mix/order/fill-history", {
        "productType": PRODUCT_TYPE,
        "startTime":   str(since_ms),
        "limit":       "50",
    })
    if result.get("code") != "00000":
        log(f"Fehler Fills: {result.get('msg', result)}")
        return []
    return result.get("data", {}).get("fillList", [])


# ═══════════════════════════════════════════════════════════════
# TP-ORDERS VERWALTEN
# ═══════════════════════════════════════════════════════════════

def cancel_all_tp_orders(symbol: str):
    """Alle bestehenden TP-Orders für ein Symbol stornieren."""
    # Korrekter Endpoint ohne isPlan-Parameter
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })

    if result.get("code") != "00000":
        # Kein Fehler loggen wenn einfach keine Orders vorhanden
        return

    orders = result.get("data", {}).get("entrustedList", [])
    tp_orders = [o for o in orders if o.get("planType") == "profit_plan"]

    if not tp_orders:
        log(f"  Keine bestehenden TP-Orders für {symbol}.")
        return

    log(f"  {len(tp_orders)} TP-Order(s) für {symbol} stornieren...")
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
                    direction: str, leverage: int, mark_price: float):
    """
    4 TP-Orders setzen mit Validierung:
    - Long:  TP muss ÜBER Marktpreis liegen
    - Short: TP muss UNTER Marktpreis liegen
    Bereits überholte TPs werden übersprungen.
    """
    decimals = get_price_decimals(symbol)
    per_tp   = max(1, math.floor(size * TP_CLOSE_PCT))

    tps = [
        (TP1_ROI, "TP1 (10%)", per_tp),
        (TP2_ROI, "TP2 (20%)", per_tp),
        (TP3_ROI, "TP3 (30%)", per_tp),
        (TP4_ROI, "TP4 (40%)", per_tp),
    ]

    count  = 0
    prices = []

    for roi, label, qty in tps:
        tp_raw = calc_tp_price(avg, roi, direction, leverage)
        tp_str = round_price(tp_raw, decimals)
        tp_val = float(tp_str)

        # Validierung: TP auf richtiger Seite des Marktpreises?
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

            # Dezimalstellen-Fehler: Cache löschen und mit 2 Stellen weniger retry
            if "checkBDScale" in msg or "checkScale" in msg:
                new_dec = max(1, decimals - 1)
                log(f"    → Retry mit {new_dec} Dezimalstellen...")
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
                    log(f"    ✓ {label} @ {tp_str2} USDT (Qty: {qty}) [retry OK]")
                    count += 1
                    prices.append(f"{label}: {tp_str2}")
                else:
                    log(f"    ✗ {label} Retry fehlgeschlagen: {res2.get('msg')}")

    return count, prices


# ═══════════════════════════════════════════════════════════════
# HAUPT-UPDATE FUNKTION
# ═══════════════════════════════════════════════════════════════

def set_sl_at_entry(symbol: str, direction: str, entry_price: float):
    """
    Setzt den Stop Loss auf den Einstiegspreis (Break-Even).
    Wird nach TP1-Auslösung aufgerufen — DOMINUS-Regel.
    """
    decimals = get_price_decimals(symbol)
    sl_str   = round_price(entry_price, decimals)

    result = api_post("/api/v2/mix/order/place-pos-tpsl", {
        "symbol":               symbol,
        "productType":          PRODUCT_TYPE,
        "marginCoin":           MARGIN_COIN,
        "holdSide":             direction,
        "stopLossTriggerPrice": sl_str,
        "stopLossTriggerType":  "mark_price",
        "stopLossExecutePrice": "0",
    })

    if result.get("code") == "00000":
        log(f"  ✓ SL auf Entry gezogen: {sl_str} USDT ({symbol})")
        telegram(
            f"🔒 <b>SL auf Entry — {symbol}</b>\n"
            f"TP1 ausgelöst → SL auf {sl_str} USDT\n"
            f"Position abgesichert."
        )
        sl_at_entry[symbol] = True
    else:
        log(f"  ✗ SL-Anpassung fehlgeschlagen: {result.get('msg', result)}")


def update_tp_for_position(pos: dict, reason: str):
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    avg       = float(pos.get("openPriceAvg", 0))
    total     = float(pos.get("total", 0))
    pnl       = float(pos.get("unrealizedPL", 0))
    leverage  = int(float(pos.get("leverage", 10)))

    # Aktuellen Marktpreis holen für Validierung
    mark = get_mark_price(symbol)

    log(f"  {symbol} | {direction.upper()} | Avg={avg} | "
        f"Qty={total} | Hebel={leverage}x | "
        f"Mark={mark} | PnL={pnl:.2f} USDT")

    if avg == 0 or total == 0:
        log("  Ungültige Position. Überspringe.")
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
            f"✅ <b>TP aktualisiert — {symbol}</b>\n"
            f"Richtung: {direction.upper()}\n"
            f"Avg-Preis: {avg} USDT\n"
            f"Hebel: {leverage}x\n"
            f"Grund: {reason}\n\n"
            + "\n".join(prices)
        )
    else:
        log(f"  ✗ Keine TPs für {symbol} gesetzt (Position evtl. bereits im Ziel).")

    # Immer setzen — verhindert Endlos-Loop wenn TPs fehlschlagen
    last_known_avg[symbol] = avg
    last_known_size[symbol] = total  # Grösse für TP1-Erkennung speichern


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    if not API_KEY or not SECRET_KEY or not PASSPHRASE:
        log("FEHLER: API_KEY, SECRET_KEY oder PASSPHRASE fehlen!")
        log("In Railway → Variables eintragen.")
        return

    log("DOMINUS TP-Updater v3 gestartet")
    log(f"Intervall: {POLL_INTERVAL}s")
    log("Symbol, Richtung und Hebel werden automatisch erkannt")
    log("─" * 55)

    # Beim Start: alle offenen Positionen behandeln
    positions = get_all_positions()
    if positions:
        log(f"{len(positions)} offene Position(en) beim Start:")
        for pos in positions:
            update_tp_for_position(pos, "Script-Start")
    else:
        log("Keine offenen Positionen. Warte auf Trades...")

    last_check_ms = int(time.time() * 1000)

    while True:
        time.sleep(POLL_INTERVAL)
        try:
            # 1. Neue Fills prüfen (Nachkäufe)
            fills      = get_recent_fills_all(last_check_ms)
            open_fills = [f for f in fills if f.get("tradeSide") == "open"]

            if open_fills:
                affected = set(
                    f.get("symbol") for f in open_fills if f.get("symbol")
                )
                log(f"Nachkauf erkannt: {', '.join(affected)}")
                last_check_ms = int(time.time() * 1000)
                time.sleep(2)  # warten bis Position aktualisiert

                for pos in get_all_positions():
                    if pos.get("symbol") in affected:
                        log(f"══ TP-Anpassung: {pos.get('symbol')} ══")
                        update_tp_for_position(
                            pos, f"Nachkauf ({len(open_fills)} Fill(s))"
                        )
            else:
                # 2. Positionen auf Änderungen prüfen
                for pos in get_all_positions():
                    sym      = pos.get("symbol", "")
                    cur_avg  = float(pos.get("openPriceAvg", 0))
                    cur_size = float(pos.get("total", 0))
                    kno_avg  = last_known_avg.get(sym, 0)
                    kno_size = last_known_size.get(sym, 0)
                    direction = pos.get("holdSide", "long")

                    # TP1-Erkennung: Grösse um ~25% gesunken + SL noch nicht gesetzt
                    if (kno_size > 0
                            and cur_size < kno_size * 0.85
                            and not sl_at_entry.get(sym, False)):
                        reduction = (kno_size - cur_size) / kno_size * 100
                        log(f"TP1 erkannt ({sym}): "
                            f"Grösse {kno_size:.2f} → {cur_size:.2f} "
                            f"(-{reduction:.0f}%) → SL auf Entry ziehen")
                        set_sl_at_entry(sym, direction, cur_avg)
                        last_known_size[sym] = cur_size

                    # Avg-Preis Änderung: Nachkauf ohne Fill erkannt
                    elif cur_avg > 0 and cur_avg != kno_avg:
                        log(f"Avg-Preis geändert ({sym}): "
                            f"{kno_avg} → {cur_avg}")
                        log(f"══ TP-Anpassung: {sym} ══")
                        # SL-Status zurücksetzen (neuer Entry)
                        sl_at_entry[sym] = False
                        update_tp_for_position(pos, "Avg-Preis Änderung")

                    # Grösse aktualisieren ohne TP1-Trigger (erste Erkennung)
                    elif kno_size == 0 and cur_size > 0:
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
