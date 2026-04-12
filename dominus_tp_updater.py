"""
DOMINUS TP Auto-Updater v2
══════════════════════════════════════════════════════════════
Erkennt automatisch ALLE offenen Positionen auf Bitget und
passt die TP-Level bei jedem Nachkauf automatisch an.

Kein fixes SYMBOL nötig — das Script scannt alle Positionen.

DOMINUS-Logik:
  TP1 = Avg + (Avg * 10% / Hebel) → schliesst 25%
  TP2 = Avg + (Avg * 20% / Hebel) → schliesst 25%
  TP3 = Avg + (Avg * 30% / Hebel) → schliesst 25%
  TP4 = Avg + (Avg * 40% / Hebel) → schliesst 25%

RAILWAY VARIABLES:
  API_KEY           → Bitget API Key
  SECRET_KEY        → Bitget Secret Key
  PASSPHRASE        → Bitget Passphrase
  LEVERAGE          → Hebel (Standard: 10)
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
# KONFIGURATION — aus Railway Variables (keine fixen Werte nötig)
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

# Speichert letzten bekannten Avg-Preis pro Symbol
last_known_avg: dict = {}


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
    signature = hmac.new(
        SECRET_KEY.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.b64encode(signature).decode("utf-8")


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
    r = requests.get(
        BASE_URL + full_path,
        headers=make_headers("GET", full_path),
        timeout=10
    )
    return r.json()


def api_post(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    r = requests.post(
        BASE_URL + path,
        headers=make_headers("POST", path, body_str),
        data=body_str,
        timeout=10
    )
    return r.json()


# ═══════════════════════════════════════════════════════════════
# KERNFUNKTIONEN
# ═══════════════════════════════════════════════════════════════

def get_all_positions() -> list:
    """Holt ALLE offenen Positionen – kein fixes Symbol nötig."""
    result = api_get("/api/v2/mix/position/all-position", {
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") != "00000":
        log(f"Fehler beim Abrufen der Positionen: {result}")
        return []
    positions = result.get("data", [])
    return [p for p in positions if float(p.get("total", 0)) > 0]


def get_recent_fills_all(since_ms: int) -> list:
    """Holt kürzlich ausgeführte Orders über ALLE Symbole."""
    result = api_get("/api/v2/mix/order/fill-history", {
        "productType": PRODUCT_TYPE,
        "startTime":   str(since_ms),
        "limit":       "50",
    })
    if result.get("code") != "00000":
        log(f"Fehler beim Abrufen der Fills: {result}")
        return []
    return result.get("data", {}).get("fillList", [])


def cancel_all_tp_orders(symbol: str):
    """Storniert alle bestehenden TP-Orders für ein Symbol."""
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "isPlan":      "profit_plan",
    })
    if result.get("code") != "00000":
        log(f"  Fehler TP-Orders abrufen ({symbol}): {result}")
        return

    orders    = result.get("data", {}).get("entrustedList", [])
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


def calc_tp_price(avg: float, roi: float, direction: str, leverage: int) -> float:
    factor = roi / leverage
    return avg * (1 + factor) if direction == "long" else avg * (1 - factor)


def round_price(price: float) -> str:
    if price >= 10000: return f"{price:.1f}"
    if price >= 1000:  return f"{price:.2f}"
    if price >= 10:    return f"{price:.3f}"
    if price >= 1:     return f"{price:.4f}"
    return f"{price:.5f}"


def place_tp_orders(symbol: str, avg: float, size: float, direction: str, leverage: int):
    per_tp = max(1, math.floor(size * TP_CLOSE_PCT))
    tps    = [
        (TP1_ROI, "TP1 (10%)", per_tp),
        (TP2_ROI, "TP2 (20%)", per_tp),
        (TP3_ROI, "TP3 (30%)", per_tp),
        (TP4_ROI, "TP4 (40%)", per_tp),
    ]
    count  = 0
    prices = []

    for roi, label, qty in tps:
        tp_str = round_price(calc_tp_price(avg, roi, direction, leverage))
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
            log(f"    ✗ {label} FEHLER: {res.get('msg', res)}")

    return count, prices


def update_tp_for_position(pos: dict, reason: str):
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    avg       = float(pos.get("openPriceAvg", 0))
    total     = float(pos.get("total", 0))
    pnl       = float(pos.get("unrealizedPL", 0))
    leverage  = int(float(pos.get("leverage", 10)))  # direkt aus Position

    log(f"  {symbol} | {direction.upper()} | Avg={avg} | "
        f"Qty={total} | Hebel={leverage}x | PnL={pnl:.2f} USDT")

    if avg == 0 or total == 0:
        log("  Ungültige Position. Überspringe.")
        return

    cancel_all_tp_orders(symbol)
    time.sleep(1)

    count, prices = place_tp_orders(symbol, avg, total, direction, leverage)

    if count == 4:
        telegram(
            f"✅ <b>TP aktualisiert — {symbol}</b>\n"
            f"Richtung: {direction.upper()}\n"
            f"Avg-Preis: {avg} USDT\n"
            f"Hebel: {leverage}x\n"
            f"Grund: {reason}\n\n"
            + "\n".join(prices)
        )
        log(f"  ✓ Alle 4 TPs für {symbol} gesetzt.")
        last_known_avg[symbol] = avg
    else:
        log(f"  ⚠ Nur {count}/4 TPs für {symbol} gesetzt.")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    if not API_KEY or not SECRET_KEY or not PASSPHRASE:
        log("FEHLER: API_KEY, SECRET_KEY oder PASSPHRASE fehlen!")
        log("In Railway unter Variables eintragen.")
        return

    log("DOMINUS TP-Updater v2 gestartet")
    log(f"Intervall: {POLL_INTERVAL}s | Hebel wird pro Position automatisch erkannt")
    log("Modus: Alle offenen Positionen werden automatisch erkannt")
    log("─" * 55)

    # Beim Start alle offenen Positionen sofort behandeln
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
            # 1. Neue Fills prüfen
            fills      = get_recent_fills_all(last_check_ms)
            open_fills = [f for f in fills if f.get("tradeSide") == "open"]

            if open_fills:
                affected = set(f.get("symbol") for f in open_fills if f.get("symbol"))
                log(f"Nachkauf erkannt: {', '.join(affected)}")
                last_check_ms = int(time.time() * 1000)
                time.sleep(2)

                all_pos = get_all_positions()
                for pos in all_pos:
                    if pos.get("symbol") in affected:
                        log(f"══ TP-Anpassung: {pos.get('symbol')} ══")
                        update_tp_for_position(
                            pos, f"Nachkauf ({len(open_fills)} Fill(s))"
                        )
            else:
                # 2. Fallback: Avg-Preis-Änderung erkennen
                for pos in get_all_positions():
                    sym     = pos.get("symbol", "")
                    cur_avg = float(pos.get("openPriceAvg", 0))
                    kno_avg = last_known_avg.get(sym, 0)
                    if cur_avg > 0 and cur_avg != kno_avg:
                        log(f"Avg-Preis geändert ({sym}): {kno_avg} → {cur_avg}")
                        log(f"══ TP-Anpassung: {sym} ══")
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
