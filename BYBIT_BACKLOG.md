# BYBIT-Integration — Backlog & Roadmap

**Status:** Vorbereitung (v4.35 stabilisieren → API-Layer-Refactor → Bybit-Adapter)
**Letztes Update:** 2026-04-25
**Architektur-Entscheidung:** **Single-Service + Adapter-Pattern + Account-Level-Capital-Split.**
Eine Codebasis, EIN Railway-Service, ein Telegram-Bot. Symbol-Routing geschieht
zur Webhook-Empfangs-Zeit anhand `{{syminfo.prefix}}`. Kapital ist auf Konto-Ebene
gesplittet (z.B. 70% Bitget / 30% Bybit) — beide Konten haben separate Equity,
Kelly-Sizing rechnet pro Konto.

**Routing-Regel (Bitget-First):** Jeder Coin wird zuerst auf Bitget geprüft.
Verfügbar → Bitget. Nicht verfügbar → Bybit. Coin auf keiner der beiden →
fliegt aus der Watchlist. Pflege via `rebuild_watchlist_bitget_first.py`
(läuft lokal, nutzt nur Public-Endpoints, kein API-Key nötig).

**Strategische Klammer (Felix' Ziel):** Ziel ≥20'000/Monat Auszahlung,
Kapital-Aufbau-Pfad ~30k → 150k USDT. Bot soll möglichst autark laufen,
weil parallel gesundheitliche Genesung Priorität hat. Daraus folgt:
**Hard-Caps + Drawdown-Circuit-Breaker + Quarter-Kelly** sind nicht-verhandelbar.

---

## 1. Roadmap

| Phase | Zeitfenster | Deliverable | Status |
|---|---|---|---|
| **A — Stabilisierung v4.35** | 2026-04-25 → 2026-05-09 | DCA-Auto-Void-Fix muss mind. 2× im Live greifen. Keine neuen Features. | ☐ |
| **B — v4.36 API-Layer-Extraktion** | 2026-05-09 → 2026-05-23 | `exchange_bitget.py` als Modul. Adapter-Pattern (kein env-Switch — beide laufen parallel). Verhalten gegenüber Bitget unverändert. Quarter-Kelly + Drawdown-Circuit-Breaker eingebaut. | ☐ |
| **C — v4.37 Bybit-Adapter** | 2026-05-23 → 2026-06-06 | `exchange_bybit.py` als zweiter Adapter im selben Service. Symbol-Routing im Webhook-Handler. Paper-Trade auf Bybit-Testnet. | ☐ |
| **D — Bybit Live mit kleiner Watchlist** | 2026-06-06 → 2026-06-20 | Nur die 4 Bybit-only Coins (MNT, XDC, FLR, VELO). Bybit-`MAX_AUTO_TRADE_USDT` = 50% des Bitget-Werts. | ☐ |
| **E — Voller Multi-Exchange-Betrieb** | ab 2026-06-20 | Watchlist-Updates greifen automatisch. Capital-Split-Verhältnis fest definiert. | ☐ |

**Wichtig:** Phase B liefert auch das Risk-Management-Upgrade — diese Punkte
sind unabhängig von Bybit, aber Voraussetzung für Echtbetrieb mit
6-stelligem Kapital:

- **Quarter-Kelly** statt Full-Kelly (Sizing-Faktor 0.25)
- **Daily-Drawdown-Circuit-Breaker** (-10% Tag → AUTO_TRADE_ENABLED=false)
- **Weekly-Drawdown-Circuit-Breaker** (-20% Woche → AUTO_TRADE_ENABLED=false)
- **Hard-Cap pro Position** (max 2-3% Account-Equity)
- **Auto-Reset** des Circuit-Breakers nur per Telegram-Befehl `/risk_reset`

---

## 2. Symbol-Universen — automatische Curation via Bitget-First-Script

**Wichtig:** Die Universen sind **nicht mehr manuell gepflegt**. Sie ergeben sich aus
`rebuild_watchlist_bitget_first.py`. Lauf das Script in deinem lokalen Repo:

```bash
cd ~/path/to/dominus-tp-updater
python3 rebuild_watchlist_bitget_first.py --dry-run   # zuerst
python3 rebuild_watchlist_bitget_first.py             # echter Lauf
```

Das Script macht in einem Durchgang:

1. Backup der aktuellen `master_watchlist.txt` → `master_watchlist.bak_<timestamp>.txt`
2. Bitget Live-Contract-Liste fetchen (public, kein API-Key)
3. Bybit Live-Contract-Liste fetchen (public, optional via `--no-bybit` skipbar)
4. Jeden Coin re-prefixen nach Bitget-First-Regel
5. Doppel-Listings deduplizieren
6. Coins die auf KEINER Exchange existieren rauswerfen
7. Audit-Report `master_watchlist_audit_<timestamp>.txt` schreiben mit Zahlen +
   einzelnen Verschoben/Gelöscht-Listen
8. Neue `master_watchlist.txt` schreiben

`--insecure` Flag im Skript für macOS-SSL-Probleme als Fallback (nutzt nur public market-data).

### 2.1 Wann das Script erneut laufen sollte

- Bei jedem neuen Coin-Listing auf Bitget (Coin der vorher auf Bybit war kann
  jetzt vielleicht auf Bitget) — alle 2–4 Wochen
- Wenn ein Trade-Webhook mit `keine offene Position` für einen Coin
  feuert, der laut Watchlist auf Bitget sein sollte → Listing wurde evtl.
  delisted
- Vor jedem grösseren v4.x-Release als Routine-Hygiene

### 2.2 Aktueller Stand nach Lauf vom 2026-04-25

| Metrik | Wert |
|---|---|
| Eingelesen | 136 Watchlist-Einträge |
| Doppel-Listings entfernt | 4 (`ARBUSDT`, `CAKEUSDT`, `GRTUSDT`, `WIFUSDT`) |
| Coins auf KEINER Exchange | 1 (`DENTUSDT` — vermutlich delisted) |
| **Bitget-Universum** | **127 Symbole** |
| **Bybit-Universum** | **4 Symbole** (`XDCUSDT`, `MNTUSDT`, `FLRUSDT`, `VELOUSDT`) |

Die 4 Bybit-only Coins sind erhaltenswert, weil:
- **MNTUSDT** (Mantle, L2) — hohes Volumen, Mover
- **XDCUSDT** (XDC Network) — Enterprise-Blockchain, etabliert
- **FLRUSDT** (Flare Network) — etablierter Mid-Cap
- **VELOUSDT** — Exoter, kann später raus wenn ohne Trades

→ Plus: Bybit listet neue Coins oft schneller als Bitget. Watchlist wird
in Zukunft via Re-Run dynamisch wachsen.

---

## 3. Bitget-Spezifika im aktuellen Skript

Diese 15 Bitget-API-Endpoints werden in `dominus_tp_updater.py` v4.35 verwendet. Jede Call-Site bekommt in v4.36/v4.37 ein Bybit-Pendant:

| Bitget-Endpoint | Zweck | Bybit V5 Pendant (zu prüfen) |
|---|---|---|
| `/api/v2/mix/account/accounts` | Kontostand | `/v5/account/wallet-balance` |
| `/api/v2/mix/account/set-leverage` | Hebel setzen | `/v5/position/set-leverage` |
| `/api/v2/mix/market/contracts` | Decimal-Info (volumePlace) | `/v5/market/instruments-info` |
| `/api/v2/mix/market/symbol-price` | Mark-Price | `/v5/market/tickers` |
| `/api/v2/mix/order/cancel-all-plan-order` | Alle TPs/SLs stornieren | `/v5/order/cancel-all` (mit `orderFilter=tpslOrder`) |
| `/api/v2/mix/order/cancel-order` | Limit-Order stornieren | `/v5/order/cancel` |
| `/api/v2/mix/order/cancel-plan-order` | Plan-Order stornieren | `/v5/order/cancel` (mit `orderFilter=tpslOrder`) |
| `/api/v2/mix/order/fill-history` | P&L pro Trade | `/v5/execution/list` |
| `/api/v2/mix/order/orders-pending` | Offene Limit-Orders (DCA!) | `/v5/order/realtime` |
| `/api/v2/mix/order/orders-plan-pending` | Offene Plan-Orders (TP/SL) | `/v5/order/realtime` (mit `orderFilter=tpslOrder`) |
| `/api/v2/mix/order/place-order` | Limit/Market-Order | `/v5/order/create` |
| `/api/v2/mix/order/place-pos-tpsl` | Position-TP/SL | `/v5/position/trading-stop` |
| `/api/v2/mix/order/place-tpsl-order` | Per-TP-Plan-Order | `/v5/order/create` (mit `triggerPrice` + `orderFilter=tpslOrder`) |
| `/api/v2/mix/position/all-position` | Alle offenen Positionen | `/v5/position/list?category=linear&settleCoin=USDT` |
| `/api/v2/mix/position/single-position` | Einzelne Position | `/v5/position/list?symbol=...` |

### Konstanten die exchange-spezifisch sind

| Bitget | Bybit V5 |
|---|---|
| `PRODUCT_TYPE = "usdt-futures"` | `category = "linear"` |
| `MARGIN_COIN = "USDT"` | `settleCoin = "USDT"` |
| `BASE_URL = "https://api.bitget.com"` | `https://api.bybit.com` (Live) / `https://api-testnet.bybit.com` (Testnet) |
| `holdSide` ∈ `{long, short}` | `side` ∈ `{Buy, Sell}` + `positionIdx` 0/1/2 |
| `volumePlace` (aus contracts) | `qtyStep` + `minOrderQty` (aus instruments-info) |
| `tradeSide` ∈ `{open, close}` | `reduceOnly` flag |

### Auth-Header Unterschiede

| Bitget | Bybit V5 |
|---|---|
| `ACCESS-KEY`, `ACCESS-SIGN`, `ACCESS-TIMESTAMP`, `ACCESS-PASSPHRASE` | `X-BAPI-API-KEY`, `X-BAPI-SIGN`, `X-BAPI-TIMESTAMP`, `X-BAPI-RECV-WINDOW` |
| Sign = HMAC-SHA256(timestamp+method+path+body) Base64 | Sign = HMAC-SHA256(timestamp+apiKey+recvWindow+queryOrBody) Hex |
| Passphrase erforderlich | Keine Passphrase |

---

## 4. Pine — eine Änderung in jedem Alarm-Template

In `Dominus_Alarm_Templates.html` (alle JSON-Payloads) ein einziges Feld einfügen:

```json
{
  "signal":  "H2_SIGNAL",
  "symbol":  "{{ticker}}",
  "exchange":"{{syminfo.prefix}}",
  "side":    "{{strategy.order.action}}",
  "price":   "{{close}}",
  ...
}
```

`{{syminfo.prefix}}` ist eine TV-Built-in-Variable — TradingView füllt automatisch `BITGET` oder `BYBIT` ein, je nach Chart-Source. Keine Pine-Logik-Änderung erforderlich.

**Im Webhook-Handler (v4.36) — Routing-Logik:**
```python
exchange_in = (data.get("exchange") or "BITGET").upper()
if exchange_in == "BITGET":
    api = bitget_adapter   # exchange_bitget.py Instance
elif exchange_in == "BYBIT":
    api = bybit_adapter    # exchange_bybit.py Instance
else:
    log(f"⚠ Unbekannte Exchange im Webhook: {exchange_in}")
    return
# Ab hier alle API-Calls über `api.place_order()`, `api.get_balance()` ...
```

---

## 5. Telegram-Link Anpassung (v4.36)

Aktuell `tv_chart_links()` in `dominus_tp_updater.py` Zeile 3277 ist hart auf BITGET kodiert. v4.36-Version:

```python
def tv_chart_links(symbol: str, exchange: str = "BITGET") -> dict:
    """v4.36: exchange-aware. Defaults auf BITGET."""
    exchange = exchange.upper()
    tv_sym = symbol.upper()
    if not tv_sym.endswith(".P"):
        tv_sym = tv_sym + ".P"
    base_sym = symbol.upper().replace(".P", "")
    if not base_sym.endswith("USDT"):
        base_sym += "USDT"

    base = "https://www.tradingview.com/chart/lX5eDAis"
    if exchange == "BYBIT":
        trade_link = f"https://www.bybit.com/trade/usdt/{base_sym}"
    else:
        trade_link = f"https://www.bitget.com/futures/usdt/{base_sym}"
    return {
        "coin_h2":  f"{base}/?symbol={exchange}:{tv_sym}&interval=120",
        "coin_h4":  f"{base}/?symbol={exchange}:{tv_sym}&interval=240",
        "btc_h2":   f"{base}/?symbol=BITGET:BTCUSDT.P&interval=120",  # BTC bleibt Bitget
        "total2":   f"{base}/?symbol=CRYPTOCAP:TOTAL2&interval=120",
        "trade":    trade_link,
    }
```

**Touchpoints zum Umbenennen:** alle `links["bitget"]` → `links["trade"]` (grep-fähig).
Aufrufer müssen `exchange=...` mitgeben (kommt aus `state[symbol]["exchange"]` oder Webhook).

---

## 6. Telegram-Prefix für Multi-Exchange-Disambiguierung

Da im Single-Service-Modell beide Exchanges in den gleichen Telegram-Chat schreiben,
brauchen wir Prefix für visuellen Unterschied:

```python
def telegram(text, exchange="BITGET", ...):
    prefix = {"BITGET": "🔵", "BYBIT": "🟣"}.get(exchange, "")
    if prefix and not text.startswith(prefix):
        text = f"{prefix} {text}"
    ...
```

Aufrufer übergeben `exchange=` aus dem Trade-Kontext. Status-Befehle wie `/status`,
`/report` listen beide Exchanges getrennt mit Sub-Headers.

---

## 7. Railway-Variablen — eine Service-Konfig

| Variable | Wert |
|---|---|
| `BITGET_API_KEY` / `_SECRET` / `_PASSPHRASE` | Bitget-Sub-Account (nur Trade, kein Withdrawal) |
| `BYBIT_API_KEY` / `_SECRET` | Bybit-Sub-Account (nur Trade, kein Withdrawal) |
| `TELEGRAM_TOKEN` | identisch (ein Bot für alles) |
| `TELEGRAM_CHAT_ID` | identisch |
| `STATE_FILE` | `/app/data/dominus_state.json` (enthält beide Exchanges) |
| `TRADES_CSV` | `/app/data/trades.csv` (mit `exchange`-Spalte) |
| `WEBHOOK_SECRET` | identisch (ein Webhook für TV-Alarme beider Exchanges) |
| `MAX_AUTO_TRADE_USDT_BITGET` | aktueller Wert |
| `MAX_AUTO_TRADE_USDT_BYBIT` | initial 50% des Bitget-Werts |
| `KELLY_FRACTION` | NEU — `0.25` (Quarter-Kelly) |
| `DAILY_DD_LIMIT_PCT` | NEU — `10` (Circuit-Breaker bei -10% Tag) |
| `WEEKLY_DD_LIMIT_PCT` | NEU — `20` (Circuit-Breaker bei -20% Woche) |

**State-Schema-Erweiterung:** `dominus_state.json` bekommt `exchange`-Feld pro Position:
```json
{
  "BTCUSDT": {"exchange": "BITGET", "side": "long", ...},
  "MNTUSDT": {"exchange": "BYBIT",  "side": "short", ...}
}
```

**Trade-CSV-Schema-Erweiterung:** neue Spalte `exchange` (Bitget/Bybit) in `trades.csv`.
Migration: existierende Zeilen bekommen `exchange="BITGET"` als Default.

---

## 8. Bug-Fix-Tracking — Stand seit v4.35

Jeder neue Bitget-seitige Bug-Fix muss bei Phase C (Bybit-Adapter-Implementation) auch im Bybit-Adapter nachgezogen werden. Sammle hier:

| Version | Fix | Bitget-Funktion(en) | Bybit-Übertragung erforderlich? |
|---|---|---|---|
| v4.35 | DCA Auto-Void: orders-pending statt orders-plan-pending | `_void_passed_dcas()` | ☐ ja — `/v5/order/realtime` ohne `orderFilter=tpslOrder` |
| v4.35 | Watchlist-Master Drop-Counter | `_track_watchlist_drop()` | ☐ nein — exchange-agnostisch, Helper im Hauptskript |
| v4.35 | Auto-SL -25% Forensik-Dump | `place_tp_orders_after_dca` Vorlauf | ☐ ja — Plan-Order-Abfrage muss exchange-aware sein |
| v4.35 | Min-Qty Edge-Case (versuche erst, dann warne) | `place_tp_orders_after_dca` + Reconciliation | ☐ ja — `minOrderQty` aus Bybit instruments-info verwenden |
| _next_ | | | |

---

## 9. Offene Fragen für Felix

- [ ] Bitget Sub-Account: jetzt einrichten? (API-Keys nur mit Trade-Permission, Hauptkonto bleibt für Withdrawals)
- [ ] Bybit Sub-Account: gleich mitnehmen, sobald Bybit aktiviert wird
- [ ] Bybit Unified Trading Account (UTA) oder Classic? (V5-API-Verhalten leicht unterschiedlich, default UTA — UTA empfohlen für Multi-Margin)
- [ ] Initialer `MAX_AUTO_TRADE_USDT_BYBIT` — Vorschlag: 50% des Bitget-Werts während Bybit-Einlauf-Phase
- [ ] Bybit Testnet-API-Key verfügbar? (für Phase C Paper-Trade vor Real Money)
- [ ] Capital-Split-Verhältnis: starten wir mit 70/30 (Bitget/Bybit)? Bei aktuell 127 vs. 4 Symbolen wäre 90/10 angemessen
- [ ] Quarter-Kelly statt Full-Kelly OK? (reduziert Wachstum ~25%, halbiert Drawdown — bei Lebensunterhalt-Setup essentiell)
- [ ] Cold-Wallet-Plan: Welcher Anteil bleibt langfristig auf der Trading-Exchange? Vorschlag: max 40% des Krypto-Vermögens, Rest auf Hardware-Wallet

---

## 10. Referenzen

- Bybit V5 API Docs: https://bybit-exchange.github.io/docs/v5/intro
- Bybit V5 Auth: https://bybit-exchange.github.io/docs/v5/guide#authentication
- Bybit Testnet: https://testnet.bybit.com
- TradingView syminfo.prefix: https://www.tradingview.com/pine-script-reference/v5/#var_syminfo.prefix
- Bitget v2 API Docs (Referenz für 1:1-Mapping): https://www.bitget.com/api-doc/contract/intro
- Kelly Criterion Risk-Management: https://www.investopedia.com/articles/trading/04/091504.asp
