"""
DOMINUS TP Auto-Updater
══════════════════════════════════════════════════════════════
Erkennt automatisch wenn eine Nachkauf-Limit-Order auf Bitget
ausgeführt wird und passt die 4 TP-Level (TP1–TP4) entsprechend
dem neuen Durchschnittspreis an.

DOMINUS-Logik:
  TP1 = Avg + (Avg * 10% / Hebel) → schliesst 25% der Position
  TP2 = Avg + (Avg * 20% / Hebel) → schliesst 25% der Position
  TP3 = Avg + (Avg * 30% / Hebel) → schliesst 25% der Position
  TP4 = Avg + (Avg * 40% / Hebel) → schliesst 25% der Position

EINRICHTUNG:
  1. Python 3.8+ installieren
  2. pip install requests
  3. Konfiguration unten anpassen (API_KEY, SECRET_KEY etc.)
  4. Script starten: python dominus_tp_updater.py

SICHERHEIT:
  - API-Key: nur "Lesen" + "Futures Trading" Berechtigung nötig
  - KEIN Withdraw-Recht vergeben!
  - IP-Whitelist in Bitget setzen (empfohlen)
══════════════════════════════════════════════════════════════
"""

import hashlib
import hmac
import base64
import time
import json
import requests
import math
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# KONFIGURATION — hier anpassen
# ═══════════════════════════════════════════════════════════════

API_KEY      = "DEIN_API_KEY"        # Bitget API Key
SECRET_KEY   = "DEIN_SECRET_KEY"     # Bitget Secret Key
PASSPHRASE   = "DEIN_PASSPHRASE"     # Bitget API Passphrase

SYMBOL       = "ETHUSDT"             # z.B. BTCUSDT, SOLUSDT, ETHUSDT
PRODUCT_TYPE = "usdt-futures"        # immer so lassen für USDT Perps
MARGIN_COIN  = "USDT"
LEVERAGE     = 10                    # dein Hebel
DIRECTION    = "long"                # "long" oder "short"

# TP-Levels nach DOMINUS (% ROI auf eingesetztes Kapital)
TP1_ROI = 0.10   # 10% → TP1
TP2_ROI = 0.20   # 20% → TP2
TP3_ROI = 0.30   # 30% → TP3
TP4_ROI = 0.40   # 40% → TP4

# Jede TP-Stufe schliesst 25% der Position
TP_CLOSE_PCT = 0.25

# Wie oft prüfen? (Sekunden)
POLL_INTERVAL = 20

# Telegram-Benachrichtigung (optional, leer lassen wenn nicht gewünscht)
TELEGRAM_TOKEN  = ""    # z.B. "123456789:ABCdef..."
TELEGRAM_CHAT_ID = ""   # z.B. "-1001234567890"

# ═══════════════════════════════════════════════════════════════
BASE_URL = "https://api.bitget.com"
# ═══════════════════════════════════════════════════════════════


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def telegram(msg):
    """Sendet Nachricht an Telegram wenn konfiguriert."""
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
    """Erstellt HMAC-SHA256 Signatur für Bitget API."""
    message = timestamp + method.upper() + path + body
    signature = hmac.new(
        SECRET_KEY.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.b64encode(signature).decode("utf-8")


def headers(method: str, path: str, body: str = "") -> dict:
    """Erstellt authentifizierte Header für Bitget API."""
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
    """GET Request zur Bitget API."""
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    full_path = path + query
    r = requests.get(
        BASE_URL + full_path,
        headers=headers("GET", full_path),
        timeout=10
    )
    return r.json()


def api_post(path: str, body: dict) -> dict:
    """POST Request zur Bitget API."""
    body_str = json.dumps(body)
    r = requests.post(
        BASE_URL + path,
        headers=headers("POST", path, body_str),
        data=body_str,
        timeout=10
    )
    return r.json()


def get_position() -> dict | None:
    """Holt aktuelle Position (avg Preis, Grösse, etc.)."""
    result = api_get("/api/v2/mix/position/single-position", {
        "symbol":      SYMBOL,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") != "00000":
        log(f"Fehler beim Abrufen der Position: {result}")
        return None
    data = result.get("data", [])
    for pos in data:
        if pos.get("holdSide") == DIRECTION:
            return pos
    return None


def get_recent_fills(since_ms: int) -> list:
    """Holt kürzlich ausgeführte Orders (Fills)."""
    result = api_get("/api/v2/mix/order/fill-history", {
        "symbol":      SYMBOL,
        "productType": PRODUCT_TYPE,
        "startTime":   str(since_ms),
        "limit":       "20",
    })
    if result.get("code") != "00000":
        log(f"Fehler beim Abrufen der Fills: {result}")
        return []
    return result.get("data", {}).get("fillList", [])


def cancel_all_tp_orders():
    """Storniert alle bestehenden TP-Orders für dieses Symbol."""
    result = api_get("/api/v2/mix/order/tpsl-pending-orders", {
        "symbol":      SYMBOL,
        "productType": PRODUCT_TYPE,
        "isPlan":      "profit_plan",
    })
    if result.get("code") != "00000":
        log(f"Fehler beim Abrufen der TP-Orders: {result}")
        return

    orders = result.get("data", {}).get("entrustedList", [])
    tp_orders = [o for o in orders if o.get("planType") == "profit_plan"]
    log(f"Bestehende TP-Orders: {len(tp_orders)} gefunden, werden storniert...")

    for order in tp_orders:
        cancel_result = api_post("/api/v2/mix/order/cancel-plan-order", {
            "symbol":      SYMBOL,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     order.get("orderId"),
        })
        if cancel_result.get("code") == "00000":
            log(f"  ✓ TP storniert: {order.get('orderId')}")
        else:
            log(f"  ✗ Stornierung fehlgeschlagen: {cancel_result}")


def calc_tp_price(avg_price: float, roi_pct: float) -> float:
    """
    Berechnet TP-Preis basierend auf avg Preis, ROI und Hebel.
    Long:  avg * (1 + roi / leverage)
    Short: avg * (1 - roi / leverage)
    """
    factor = roi_pct / LEVERAGE
    if DIRECTION == "long":
        return avg_price * (1 + factor)
    else:
        return avg_price * (1 - factor)


def round_price(price: float) -> str:
    """Rundet auf sinnvolle Dezimalstellen."""
    if price >= 1000:
        return f"{price:.1f}"
    elif price >= 10:
        return f"{price:.3f}"
    else:
        return f"{price:.5f}"


def place_tp_orders(avg_price: float, total_size: float):
    """
    Setzt 4 neue TP-Orders nach DOMINUS-Schema:
    TP1: 25% @ 10% ROI
    TP2: 25% @ 20% ROI
    TP3: 25% @ 30% ROI
    TP4: 25% @ 40% ROI
    """
    # Grösse pro TP-Stufe (25% der Position, mind. 1 Kontrakt)
    size_per_tp = max(1, math.floor(total_size * TP_CLOSE_PCT))

    tps = [
        (TP1_ROI, "TP1 (10%)", size_per_tp),
        (TP2_ROI, "TP2 (20%)", size_per_tp),
        (TP3_ROI, "TP3 (30%)", size_per_tp),
        (TP4_ROI, "TP4 (40%)", size_per_tp),
    ]

    success_count = 0
    tp_prices = []

    for roi, label, size in tps:
        tp_price = calc_tp_price(avg_price, roi)
        tp_price_str = round_price(tp_price)

        result = api_post("/api/v2/mix/order/place-tpsl-order", {
            "symbol":       SYMBOL,
            "productType":  PRODUCT_TYPE,
            "marginCoin":   MARGIN_COIN,
            "planType":     "profit_plan",
            "triggerPrice": tp_price_str,
            "triggerType":  "mark_price",
            "executePrice": "0",           # Market Order bei Trigger
            "holdSide":     DIRECTION,
            "size":         str(size),
        })

        if result.get("code") == "00000":
            log(f"  ✓ {label} gesetzt @ {tp_price_str} USDT (Grösse: {size})")
            success_count += 1
            tp_prices.append(f"{label}: {tp_price_str}")
        else:
            log(f"  ✗ {label} FEHLER: {result.get('msg', result)}")

    return success_count, tp_prices


def update_tp(reason: str = "Nachkauf"):
    """Hauptfunktion: Position holen → TPs stornieren → neue TPs setzen."""
    log(f"══ TP-Anpassung gestartet (Grund: {reason}) ══")

    pos = get_position()
    if not pos:
        log("Keine offene Position gefunden. Überspringe.")
        return

    avg_price  = float(pos.get("openPriceAvg", 0))
    total_size = float(pos.get("total", 0))
    unrealized = float(pos.get("unrealizedPL", 0))

    log(f"Position: Avg={avg_price:.4f} | Grösse={total_size} | PnL={unrealized:.2f} USDT")

    if avg_price == 0 or total_size == 0:
        log("Ungültige Position. Überspringe.")
        return

    # Alte TPs stornieren
    cancel_all_tp_orders()
    time.sleep(1)  # kurz warten

    # Neue TPs setzen
    count, prices = place_tp_orders(avg_price, total_size)

    if count == 4:
        msg = (
            f"✅ <b>TP aktualisiert — {SYMBOL}</b>\n"
            f"Avg-Preis: {avg_price:.4f} USDT\n"
            f"Grund: {reason}\n\n"
            + "\n".join(prices)
        )
        telegram(msg)
        log(f"✓ Alle 4 TP-Orders erfolgreich gesetzt.")
    else:
        log(f"⚠ Nur {count}/4 TP-Orders gesetzt.")


def main():
    log(f"DOMINUS TP-Updater gestartet")
    log(f"Symbol: {SYMBOL} | Richtung: {DIRECTION} | Hebel: {LEVERAGE}x")
    log(f"Poll-Intervall: {POLL_INTERVAL}s")
    log("─" * 50)

    # Beim Start direkt prüfen ob Position offen
    pos = get_position()
    if pos and float(pos.get("total", 0)) > 0:
        log("Offene Position gefunden beim Start.")
        update_tp("Script-Start")
    else:
        log("Keine offene Position beim Start. Warte auf Nachkauf...")

    # Zeitstempel der letzten geprüften Order
    last_check_ms = int(time.time() * 1000)
    last_known_avg = float(pos.get("openPriceAvg", 0)) if pos else 0

    while True:
        time.sleep(POLL_INTERVAL)

        try:
            # Neue Fills seit letztem Check holen
            new_fills = get_recent_fills(last_check_ms)

            # Nur Buy-Orders für diese Richtung filtern
            relevant_fills = [
                f for f in new_fills
                if f.get("side") == "buy" and f.get("tradeSide") == "open"
            ]

            if relevant_fills:
                log(f"{len(relevant_fills)} neuer Nachkauf erkannt!")
                last_check_ms = int(time.time() * 1000)

                # Kurz warten damit Bitget die Position aktualisiert
                time.sleep(2)

                # TP anpassen
                update_tp(f"Nachkauf ({len(relevant_fills)} Fill(s))")
                last_check_ms = int(time.time() * 1000)

            else:
                # Stille Prüfung ob avg Preis sich verändert hat
                pos = get_position()
                if pos:
                    current_avg = float(pos.get("openPriceAvg", 0))
                    if current_avg != last_known_avg and current_avg > 0:
                        log(f"Avg-Preis hat sich geändert: {last_known_avg:.4f} → {current_avg:.4f}")
                        update_tp("Avg-Preis Änderung")
                        last_known_avg = current_avg
                        last_check_ms = int(time.time() * 1000)

        except requests.exceptions.ConnectionError:
            log("Verbindungsfehler. Versuche erneut in 30s...")
            time.sleep(30)
        except requests.exceptions.Timeout:
            log("Timeout. Versuche erneut...")
        except Exception as e:
            log(f"Unerwarteter Fehler: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
