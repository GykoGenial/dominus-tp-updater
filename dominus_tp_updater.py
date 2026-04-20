"""
DOMINUS Trade-Automatisierung v4.10
══════════════════════════════════════════════════════════════
Vollautomatisches Setup nach DOMINUS-Strategie (Handbuch März 2026)
Finanzmathematische Optimierungen:
  ① Hebel-Empfehlung  — Hebel = 25 / SL-Abstand%
  ② R:R-Filter        — kein Trade unter 1.5 R:R
  ③ Kelly-Kriterium   — optimale Positionsgrösse
  ④ Asymm. TPs        — 15/20/25/40% statt 25/25/25/25%
  ⑤ Telegram Polling  — /berechnen /trade /status /hilfe /alarm

Changelog v4.10 — DOMINUS-konforme Premium-Zonen (ersetzt v4.9 Hard-Block):
  P1: Extremzonen werden als Premium-Entry-Bestätigung behandelt (Handbuch-konform),
      nicht mehr als Hard-Block
  P2: extreme_block() → extreme_warn() — gibt nur noch Telegram-Warntext zurück,
      blockiert keine Entries mehr
  P3: BTC_OVERSOLD + grüne Richtung + HARSI_EXIT → "🎯 Long Premium aktiv"
      BTC_OVERBOUGHT + rote Richtung + HARSI_EXIT → "🎯 Short Premium aktiv"
  P4: HARSI_EXIT / H2_SIGNAL / cmd_trade erhalten Premium-Hinweis (Soft-Info),
      aber der Trade wird wie üblich ausgeführt
  P5: /status zeigt aktive Premium-Zonen mit Restzeit + Dominus-Kompatibilität

Changelog v4.9 — Makro-Extremzonen (Hard-Block Variante, ersetzt durch v4.10):
  M1: macro_extreme-State + EXTREME_COOLDOWN_H (default 4h)
  M2: Webhook-Signale BTC_OVERSOLD/BTC_OVERBOUGHT/T2_OVERSOLD/T2_OVERBOUGHT
      setzen State + Telegram-Warnung (Block-Variante in v4.10 entfernt)
  M3: Entry-Guards sind in v4.10 durch Soft-Warn-Hinweise ersetzt worden
  M4: /status zeigt aktuellen Makro-Extremzonen-Zustand + Restzeit
  M5: State persistiert (save_state/load_state), Cooldown überlebt Restart

Changelog v4.8 — Copy-Paste Alarm-Generator für TradingView:
  A1: /alarm Command — generiert fertige Alarm-Vorlagen (Name, Bedingung, JSON, Webhook-URL)
      für Alarm 1/1b (H4), 2/2b (H2), 3/3b (HARSI_EXIT), 4/4b (HARSI_SL)
  A2: /alarm harsi SYMBOL long|short — berücksichtigt das 30-Min-Fenster aus
      last_h2_signal_time und zeigt Rest-Minuten bzw. Ablauf-Warnung
  A3: H2_SIGNAL-Webhook bei harsi_warn=1 → inline-Block mit kopierbarer
      Alarm-3/3b-Vorlage direkt in der Telegram-Nachricht
  A4: WEBHOOK_URL Railway-Variable — optional, wird 1:1 in Alarm-Vorlagen eingesetzt
  A5: /hilfe erweitert um /alarm

Changelog v4.7 — Railway Volume CSV-Archiv:
  V1: csv_log_trade() — Trade-Archiv als CSV direkt auf Railway Volume (/app/data/trades.csv)
  V2: TRADES_CSV — konfigurierbar via Railway Variable (Standard: /app/data/trades.csv)
  V3: STATE_FILE Default → /app/data/dominus_state.json (Railway Volume)
  V4: /app/data/-Verzeichnis wird automatisch angelegt (os.makedirs)

Changelog v4.6 — Google Sheets Archiv & Telegram-Überarbeitung:
  G1: sheets_log_trade() — permanentes Trade-Archiv in Google Sheets (non-blocking Thread)
  G2: _get_gsheet_ws() — lazy init, Sheet "Trades" wird auto-angelegt mit Header
  G3: GOOGLE_CREDENTIALS + GOOGLE_SHEET_ID als Railway Variables
  T1: /hilfe neu strukturiert (Info / Aktionen / Premium / Automatisch)
  T2: /status — Qty mit BaseCoin + USDT, Quick-Actions-Footer
  T3: /makro, /report, /refresh — Quick-Actions-Footer ergänzt
  T4: /berechnen — Footer auf /status|/makro|/report|/hilfe reduziert

Changelog v4.5 — Symbol-Präzision & Telegram USDT-Anzeige:
  P1: volumePlace (Mengenpräzision) von Bitget Contract-API gecacht pro Symbol
  P2: snap_qty() / round_qty() — korrekte Mengenrundung für alle Symbols (THETA, BTC, etc.)
  P3: place_tp_orders: qty via snap_qty statt hardcoded round(...,4) / math.floor
  P4: place_dca_orders: qty via snap_qty, fmt() entfernt
  P5: Telegram — Restposition neu als "X.xx COIN (≈ Y.yy USDT)" in TP1/TrailingSL/HarsiSL
  P6: Telegram — DCA-Bestätigung zeigt Menge in BaseCoin + USDT-Notional
  P7: TP-Log zeigt Qty in BaseCoin + USDT-Notional
  D1: DOCS_URL + _doc_link() — Alarm-Anleitung Links in allen Telegram-Nachrichten
  D2: H2-Signal HARSI-Warnung zeigt Ablaufzeit + Link zu Alarm 3/3b

Changelog v4.4 — Logik-Audit & Fixes:
  K1: TP3-Grössen-Schwellwert 0.37→0.42, TP2 0.67→0.69 (war zu tief für asym. TPs)
  K2: DCA-Fehler via Telegram gemeldet statt still ignoriert
  K3: TP-Kaskade sequentiell (statt elif) — TP1+TP2 werden im selben Tick erkannt
  H3: API retry (3×) bei 5xx / Timeout mit exp. Backoff
  H5: TP4 known_sl Fallback aus Trailing-Level (TP1/TP2-Preis) statt altem Entry-SL
  M1: h4_buffer Lock konsistent im Main-Loop
  M2: last_h2_signal_time Cleanup direkt beim Schreiben (kein Memory-Leak)
  M3: TP2-Schwellwert 0.67→0.69 (+4% Puffer gegen Slippage)
  M4: übersprungene TP-Qty (Position zu klein) wird auf nächsten TP addiert
  M6: STATE_FILE-Warnung beim Start wenn nicht als Railway-Variable gesetzt
  N3: sl_is_entry Toleranz preis-skaliert statt fix 0.15%

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
  DOCS_URL          → optional — URL zu Dominus_Alarm_Templates.html (z.B. https://dein-server/Dominus_Alarm_Templates.html)
  GOOGLE_CREDENTIALS → optional — Service Account JSON (komplett, als String)
  GOOGLE_SHEET_ID    → optional — ID des Google Sheets (aus der URL: /spreadsheets/d/ID/edit)
══════════════════════════════════════════════════════════════
"""

import hashlib
import hmac
import base64
import time
import json
import os
import html
import threading
import requests
import math
from datetime import datetime, timedelta
try:
    from flask import Flask, request as flask_request, jsonify
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

import csv

try:
    import gspread
    from google.oauth2.service_account import Credentials as _GCredentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

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
WEBHOOK_URL      = os.environ.get("WEBHOOK_URL", "")  # optional: vollständige Railway-URL inkl. ?token=… für /alarm-Vorlagen
DOCS_URL           = os.environ.get("DOCS_URL", "https://GykoGenial.github.io/dominus-tp-updater/Dominus_Alarm_Templates.html")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")  # Service Account JSON als String
GOOGLE_SHEET_ID    = os.environ.get("GOOGLE_SHEET_ID",    "")  # Spreadsheet ID aus URL
TRADES_CSV         = os.environ.get("TRADES_CSV", "/app/data/trades.csv")  # Persistentes Trade-Archiv auf Railway Volume

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
price_decimals_cache: dict = {}  # {symbol: int}  — Preis-Dezimalstellen
qty_decimals_cache:   dict = {}  # {symbol: int}  — Mengen-Dezimalstellen (volumePlace)
base_coin_cache:      dict = {}  # {symbol: str}  — BaseCoin-Name (z.B. "THETA", "BTC")
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

# DOMINUS 30-Min-Fenster: Zeitstempel letzter H2_SIGNAL pro Symbol+Richtung
# Key: "{SYMBOL}_{direction}"  z.B. "ETHUSDT_long"
# Value: datetime (UTC) des letzten H2_SIGNAL-Eingangs
last_h2_signal_time: dict = {}   # {symbol_dir: datetime}

# Makro-Kontext: BTC & Total2 DOM-DIR Impuls-Richtung
# Gesetzt via Webhook signal="BTC_DIR" / "T2_DIR" oder beim H2_SIGNAL-Empfang
# Werte: "long"              → Impuls ≥ 0, bestätigt bullish    (Alarm 5:  0 aufwärts)
#        "recovering"        → Impuls −10→0, Exit Oversold       (Alarm 5c: −10 aufwärts, nur Premium-Longs)
#        "short"             → Impuls ≤ 0, bestätigt bearish     (Alarm 5:  0 abwärts)
#        "recovering_short"  → Impuls 0→+10, Exit Overbought     (Alarm 5e: +10 abwärts, nur Premium-Shorts)
#        ""                  → unbekannt (noch kein Webhook)
# Long-Permission:   long+long → voll  |  long/recovering gemischt → nur premium=1  |  recovering+recovering → kein Trade
# Short-Permission:  short+short → voll  |  short/recovering_short gemischt → nur premium=1  |  recovering_short+recovering_short → kein Trade
btc_dir:  str = ""   # aktuelle BTC Impuls-Richtung
t2_dir:   str = ""   # aktuelle Total2 Impuls-Richtung

# ────────────────────────────────────────────────────────────────
# v4.10: Makro-Extremzonen als DOMINUS-Premium-Bestätigung
# ────────────────────────────────────────────────────────────────
# DOMINUS-Handbuch: Oversold (-10) + grüne Richtung = Long Premium
#                   Overbought (+10) + rote Richtung = Short Premium
# Wir blockieren nichts, sondern markieren die Zone als Premium-Kontext.
# state:  -1 = OVERSOLD   (Impuls ≤ -10) → Long Premium aktiv
#         +1 = OVERBOUGHT (Impuls ≥ +10) → Short Premium aktiv
#          0 = neutral
# until_ts: UNIX-Zeit, bis wann der Premium-Kontext gilt (gesetzt durch
#           Oversold/Overbought-Alarm + EXTREME_COOLDOWN_H Stunden).
#           Sliding Window: ein neuer Alarm verlängert die Zone.
#           Nach Ablauf: state bleibt, aber extreme_warn() gibt keinen
#           Premium-/Warn-Text mehr aus.
EXTREME_COOLDOWN_H = int(os.environ.get("EXTREME_COOLDOWN_H", "4"))

macro_extreme: dict = {
    "btc":    {"state": 0, "until_ts": 0.0},
    "total2": {"state": 0, "until_ts": 0.0},
}


# ═══════════════════════════════════════════════════════════════
# BASIS-FUNKTIONEN
# ═══════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def dir_icon(direction: str) -> str:
    """Richtungs-Icon: 🟢↗️ für Long, 🔴↘️ für Short."""
    return "🟢↗️" if direction == "long" else "🔴↘️"


def _doc_link(anchor: str, label: str) -> str:
    """Klickbarer Telegram-HTML-Link zur Alarm-Anleitung im HTML (via DOCS_URL + Anker).
    Fallback auf Text-Anker falls DOCS_URL nicht gesetzt."""
    if DOCS_URL:
        return f'🔗 <a href="{DOCS_URL}#{anchor}">{label}</a>'
    return f"🔗 #{anchor} in Dominus_Alarm_Templates.html"


# ═══════════════════════════════════════════════════════════════
# v4.10: MAKRO-EXTREMZONEN — DOMINUS-Premium-Bestätigung (Soft-Info)
# ═══════════════════════════════════════════════════════════════

def _set_macro_extreme(market: str, state_val: int) -> None:
    """Setzt Oversold (-1) oder Overbought (+1) für 'btc' oder 'total2'.
    until_ts = now + EXTREME_COOLDOWN_H Stunden (Sliding Window: ein neuer Alarm
    in der gleichen Richtung schiebt den Ablauf nach hinten)."""
    if market not in macro_extreme:
        return
    macro_extreme[market]["state"]    = state_val
    macro_extreme[market]["until_ts"] = time.time() + EXTREME_COOLDOWN_H * 3600


def _reset_macro_extreme(market: str) -> None:
    """Zurücksetzen nach Ablauf oder Gegen-Signal. Belässt state auf 0."""
    if market not in macro_extreme:
        return
    macro_extreme[market]["state"]    = 0
    macro_extreme[market]["until_ts"] = 0.0


def extreme_warn(direction: str) -> dict:
    """
    DOMINUS-konforme Auswertung der Makro-Extremzonen (v4.10 — kein Block!).

    Zurückgegeben wird ein Dict mit zwei Listen:
      • "premium"  → Extremzonen, die den Entry als DOMINUS-Premium bestätigen
                     (LONG in Oversold, SHORT in Overbought)
      • "warnings" → Extremzonen, die gegen den Entry sprechen
                     (SHORT in Oversold = Top-Selling in bottoming Markt,
                      LONG  in Overbought = Buying in toppping Markt)

    Abgelaufene Zonen werden automatisch zurückgesetzt. Kein Block, kein
    Entry wird verhindert — die Information dient als Hinweis per Telegram.
    """
    out: dict = {"premium": [], "warnings": []}
    if direction not in ("long", "short"):
        return out
    now = time.time()
    for market, label in (("btc", "BTC"), ("total2", "Total2")):
        s = macro_extreme.get(market, {})
        state    = int(s.get("state", 0))
        until_ts = float(s.get("until_ts", 0.0))
        if until_ts > 0 and until_ts <= now:
            _reset_macro_extreme(market)
            continue
        if state == 0 or until_ts == 0:
            continue
        remaining_h = max(0.0, (until_ts - now) / 3600.0)
        # DOMINUS Premium: Impulsrichtung stimmt mit Entry-Richtung überein
        if direction == "long" and state == -1:
            out["premium"].append(f"{label} OVERSOLD (noch {remaining_h:.1f}h) → Long Premium")
        elif direction == "short" and state == +1:
            out["premium"].append(f"{label} OVERBOUGHT (noch {remaining_h:.1f}h) → Short Premium")
        # Gegenrichtung → nur Warnung, kein Block
        elif direction == "long" and state == +1:
            out["warnings"].append(f"{label} OVERBOUGHT (noch {remaining_h:.1f}h) → LONG gegen Top")
        elif direction == "short" and state == -1:
            out["warnings"].append(f"{label} OVERSOLD (noch {remaining_h:.1f}h) → SHORT gegen Boden")
    return out


def macro_extreme_status_lines() -> list:
    """Lesbare Status-Zeilen für /status — zeigt DOMINUS-Premium-Kontext (kein Block)."""
    now = time.time()
    lines: list = []
    for market, label in (("btc", "BTC"), ("total2", "Total2")):
        s = macro_extreme.get(market, {})
        state    = int(s.get("state", 0))
        until_ts = float(s.get("until_ts", 0.0))
        if until_ts > 0 and until_ts <= now:
            _reset_macro_extreme(market)
            state, until_ts = 0, 0.0
        if state == 0:
            lines.append(f"  {label}: ✅ neutral")
            continue
        remaining_min = int((until_ts - now) / 60)
        h, m = divmod(remaining_min, 60)
        if state == -1:
            lines.append(f"  {label}: 🔻 OVERSOLD → 🎯 Long Premium aktiv (noch {h}h {m}min)")
        else:
            lines.append(f"  {label}: 🔺 OVERBOUGHT → 🎯 Short Premium aktiv (noch {h}h {m}min)")
    return lines


def format_extreme_info_msg(symbol: str, direction: str, info: dict, source: str) -> str:
    """DOMINUS-konforme Premium-/Warnmeldung (v4.10 — kein Block, nur Info)."""
    premium  = info.get("premium",  []) or []
    warnings = info.get("warnings", []) or []
    if not premium and not warnings:
        return ""
    if premium and not warnings:
        head = f"🎯 <b>DOMINUS Premium — {symbol} {direction.upper()}</b>"
        body_intro = "Extremzone deckt sich mit Entry-Richtung (Handbuch-konform):"
    elif warnings and not premium:
        head = f"⚠️ <b>Makro-Warnung — {symbol} {direction.upper()}</b>"
        body_intro = "Extremzone spricht GEGEN den Entry (antizyklisch):"
    else:
        head = f"⚠️ <b>Makro-gemischt — {symbol} {direction.upper()}</b>"
        body_intro = "Premium- und Gegen-Signal gleichzeitig aktiv:"
    lines = [head, "━" * 12, f"Quelle: {source}", "", body_intro]
    for r in premium:
        lines.append(f"  🎯 {r}")
    for r in warnings:
        lines.append(f"  ⚠️ {r}")
    lines += [
        "",
        "<i>Kein Block — Entry wird regulär ausgeführt. "
        "Premium bestätigt DOMINUS-Handbuch; Warnung erinnert an "
        "Gegenzyklik. Du entscheidest.</i>",
    ]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# GOOGLE SHEETS — Permanentes Trade-Archiv
# ═══════════════════════════════════════════════════════════════

_gsheet_ws = None   # gecachte Worksheet-Instanz (lazy init)

_GSHEET_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_GSHEET_HEADER = [
    "Datum", "Zeit (UTC)", "Symbol", "Richtung", "Hebel",
    "Entry", "Close", "PnL USDT", "ROI %", "Dauer",
    "Trailing Level", "Won", "Monat", "Jahr",
]


def _get_gsheet_ws():
    """Gibt gecachtes Worksheet zurück — lazy init beim ersten Aufruf."""
    global _gsheet_ws
    if _gsheet_ws is not None:
        return _gsheet_ws
    if not GSPREAD_AVAILABLE:
        log("[GSheets] gspread nicht installiert — pip install gspread google-auth")
        return None
    if not GOOGLE_CREDENTIALS or not GOOGLE_SHEET_ID:
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        creds      = _GCredentials.from_service_account_info(creds_dict, scopes=_GSHEET_SCOPES)
        gc         = gspread.authorize(creds)
        sh         = gc.open_by_key(GOOGLE_SHEET_ID)
        # Erstes Sheet "Trades" verwenden oder anlegen
        try:
            ws = sh.worksheet("Trades")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="Trades", rows=10000, cols=len(_GSHEET_HEADER))
            ws.append_row(_GSHEET_HEADER)
            ws.format("A1:N1", {"textFormat": {"bold": True}})
        _gsheet_ws = ws
        log("[GSheets] Verbindung OK — Worksheet 'Trades' bereit")
        return ws
    except Exception as e:
        log(f"[GSheets] Init-Fehler: {e}")
        return None


def sheets_log_trade(trade: dict):
    """Hängt einen abgeschlossenen Trade ans Google Sheet an (non-blocking)."""
    if not GOOGLE_CREDENTIALS or not GOOGLE_SHEET_ID:
        return

    def _append():
        try:
            ws = _get_gsheet_ws()
            if ws is None:
                return
            ts       = trade.get("ts", time.time())
            dt       = datetime.utcfromtimestamp(ts)
            pnl      = float(trade.get("net_pnl", 0))
            entry_px = float(trade.get("entry", 0))
            close_px = float(trade.get("close_price", 0))
            lev      = int(trade.get("leverage", 1))
            # ROI% = (PnL / (entry * size / lev)) * 100  — Näherung via net_pnl und entry
            # Einfachere Variante: direkt aus realized_pnl vs. margin
            roi_pct  = round((close_px - entry_px) / entry_px * lev * 100, 2) if entry_px > 0 else 0
            if trade.get("direction") == "short":
                roi_pct = -roi_pct
            row = [
                dt.strftime("%d.%m.%Y"),           # Datum
                dt.strftime("%H:%M"),               # Zeit UTC
                trade.get("symbol", ""),            # Symbol
                trade.get("direction", "").upper(), # Richtung
                lev,                                # Hebel
                entry_px,                           # Entry
                close_px,                           # Close
                round(pnl, 2),                      # PnL USDT
                roi_pct,                            # ROI %
                trade.get("hold_str", ""),          # Dauer
                trade.get("trailing_level", 0),     # Trailing Level
                "✓" if trade.get("won") else "✗",  # Won
                dt.strftime("%Y-%m"),               # Monat (für Pivot)
                dt.strftime("%Y"),                  # Jahr (für Pivot)
            ]
            ws.append_row(row, value_input_option="USER_ENTERED")
            log(f"[GSheets] Trade geloggt: {trade.get('symbol')} {pnl:+.2f} USDT")
        except Exception as e:
            log(f"[GSheets] Log-Fehler: {e}")

    # In separatem Thread — blockiert den Webhook-Handler nicht
    threading.Thread(target=_append, daemon=True).start()


_CSV_HEADER = [
    "Datum", "Zeit (UTC)", "Symbol", "Richtung", "Hebel",
    "Entry", "Close", "PnL USDT", "ROI %", "Dauer",
    "Trailing Level", "Won", "Monat", "Jahr",
]


def csv_log_trade(trade: dict):
    """Hängt einen abgeschlossenen Trade ans Railway-Volume CSV an (non-blocking)."""
    if not TRADES_CSV:
        return

    def _write():
        try:
            ts       = trade.get("ts", time.time())
            dt       = datetime.utcfromtimestamp(ts)
            pnl      = float(trade.get("net_pnl", 0))
            entry_px = float(trade.get("entry", 0))
            close_px = float(trade.get("close_price", 0))
            lev      = int(trade.get("leverage", 1))
            roi_pct  = round((close_px - entry_px) / entry_px * lev * 100, 2) if entry_px > 0 else 0
            if trade.get("direction") == "short":
                roi_pct = -roi_pct

            # Verzeichnis anlegen falls noch nicht vorhanden
            os.makedirs(os.path.dirname(TRADES_CSV), exist_ok=True)

            file_exists = os.path.isfile(TRADES_CSV)
            with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=";")
                if not file_exists:
                    writer.writerow(_CSV_HEADER)  # Header nur beim ersten Mal
                writer.writerow([
                    dt.strftime("%d.%m.%Y"),            # Datum
                    dt.strftime("%H:%M"),                # Zeit UTC
                    trade.get("symbol", ""),             # Symbol
                    trade.get("direction", "").upper(),  # Richtung
                    lev,                                 # Hebel
                    entry_px,                            # Entry
                    close_px,                            # Close
                    round(pnl, 2),                       # PnL USDT
                    roi_pct,                             # ROI %
                    trade.get("hold_str", ""),           # Dauer
                    trade.get("trailing_level", 0),      # Trailing Level
                    "Ja" if trade.get("won") else "Nein", # Won
                    dt.strftime("%Y-%m"),                # Monat (für Filterung)
                    dt.strftime("%Y"),                   # Jahr
                ])
            log(f"[CSV] Trade geloggt: {trade.get('symbol')} {pnl:+.2f} USDT → {TRADES_CSV}")
        except Exception as e:
            log(f"[CSV] Log-Fehler: {e}")

    threading.Thread(target=_write, daemon=True).start()


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
    # Retry bei transienten Fehlern (Timeout, Connection Error, 5xx)
    for attempt in range(3):
        try:
            r = requests.get(BASE_URL + full_path,
                             headers=make_headers("GET", full_path), timeout=10)
            data = r.json()
            # 5xx = transient Bitget-Fehler → retry
            if r.status_code >= 500 and attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            return data
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            log(f"GET Fehler ({path}): {e}")
            return {}
    return {}


def api_post(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    # Retry bei transienten Fehlern (Timeout, Connection Error, 5xx)
    for attempt in range(3):
        try:
            r = requests.post(BASE_URL + path,
                              headers=make_headers("POST", path, body_str),
                              data=body_str, timeout=10)
            data = r.json()
            # 5xx = transient Bitget-Fehler → retry
            if r.status_code >= 500 and attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            return data
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
                continue
            log(f"POST Fehler ({path}): {e}")
            return {}
    return {}


# ═══════════════════════════════════════════════════════════════
# PREIS-PRÄZISION
# ═══════════════════════════════════════════════════════════════

def get_price_decimals(symbol: str) -> int:
    """Erlaubte Preis-Dezimalstellen von Bitget Contract-API (gecacht).
    Befüllt gleichzeitig qty_decimals_cache und base_coin_cache."""
    if symbol in price_decimals_cache:
        return price_decimals_cache[symbol]
    result = api_get("/api/v2/mix/market/contracts", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
    })
    price_dec = 4
    qty_dec   = 4
    base_coin = symbol.replace("USDT", "").replace("USDC", "")
    try:
        contracts = result.get("data", [])
        if contracts:
            c = contracts[0]
            price_dec = int(c.get("pricePlace",  "4"))
            qty_dec   = int(c.get("volumePlace", "4"))
            base_coin = c.get("baseCoin", base_coin) or base_coin
    except Exception:
        pass
    price_decimals_cache[symbol] = price_dec
    qty_decimals_cache[symbol]   = qty_dec
    base_coin_cache[symbol]      = base_coin
    return price_dec


def get_qty_decimals(symbol: str) -> int:
    """Erlaubte Mengen-Dezimalstellen des Symbols (gecacht, wird via get_price_decimals geladen)."""
    if symbol not in qty_decimals_cache:
        get_price_decimals(symbol)
    return qty_decimals_cache.get(symbol, 4)


def get_base_coin(symbol: str) -> str:
    """BaseCoin-Name des Symbols (z.B. 'THETA' für 'THETAUSDT')."""
    if symbol not in base_coin_cache:
        get_price_decimals(symbol)
    return base_coin_cache.get(symbol, symbol.replace("USDT", ""))


def snap_qty(symbol: str, qty: float) -> float:
    """Rundet Menge numerisch auf die korrekte Symbol-Präzision (volumePlace)."""
    dec = get_qty_decimals(symbol)
    if dec == 0:
        return float(math.floor(qty))
    return round(qty, dec)


def round_price(price: float, decimals: int) -> str:
    return f"{price:.{decimals}f}"


def round_qty(symbol: str, qty: float) -> str:
    """Formatiert Menge als API-konformen String (korrekte Dezimalstellen je Symbol)."""
    dec = get_qty_decimals(symbol)
    if dec == 0:
        return str(int(math.floor(qty)))
    return f"{qty:.{dec}f}"


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

    # Versuch 4: orders-plan-pending mit GROSS-productType (manche Bitget-Endpoints
    # benötigen "USDT-FUTURES" statt "usdt-futures")
    r4 = api_get("/api/v2/mix/order/orders-plan-pending", {
        "productType": PRODUCT_TYPE.upper(),
    })
    if r4.get("code") == "00000":
        all_orders = _parse(r4.get("data"))
        return [o for o in all_orders if o.get("symbol") == symbol]

    log(f"  [WARN] Alle Plan-Order Endpoints für {symbol} fehlgeschlagen: "
        f"1={r.get('msg','?')} | 2={r2.get('msg','?')} | "
        f"3={r3.get('msg','?')} | 4={r4.get('msg','?')}")
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

    Strategie (zwei Stufen):
      1. Batch-Cancel via cancel-all-plan-order (kein vorheriges Lesen nötig).
         Funktioniert auch wenn _get_plan_orders fehlschlägt (z.B. DENT, sehr
         kleine Preise) — direkt nach planType=profit_plan filtern.
      2. Fallback: Lesen + Einzelstornierung (bisheriges Verhalten).
    SL-Typen (loss_plan, pos_loss) werden nie angefasst.
    """
    # ── Stufe 1: Batch-Cancel (kein Read nötig) ──────────────
    batch_res = api_post("/api/v2/mix/order/cancel-all-plan-order", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
        "planType":    "profit_plan",
    })
    if batch_res.get("code") == "00000":
        log(f"  ✓ Alle profit_plan TPs via Batch-Cancel storniert ({symbol})")
        return

    log(f"  Batch-Cancel nicht verfügbar ({batch_res.get('msg','?')}) — "
        f"versuche Einzelstornierung...")

    # ── Stufe 2: Lesen + Einzelstornierung (Fallback) ────────
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
    carry_qty = 0.0          # übersprungene Qty wird auf nächsten TP aufaddiert

    for roi, label, pct in partial_tps:
        tp_raw = calc_tp_price(avg, roi, direction, leverage)
        tp_str = round_price(tp_raw, decimals)
        tp_val = float(tp_str)

        # Qty-Berechnung: Symbol-spezifische Präzision via volumePlace (Bitget Contract-Info)
        # snap_qty() rundet korrekt: 0 Dezimalstellen = math.floor (ganze Kontrakte)
        base_qty = size * pct
        qty      = snap_qty(symbol, base_qty + carry_qty)

        if qty <= 0:
            log(f"    ⏭ {label}: Position zu klein für Teilschliessung (size={size}, pct={pct}) — "
                f"Qty {base_qty:.4f} wird auf nächsten TP übertragen")
            carry_qty += base_qty   # Menge für nächsten TP merken
            partial_tps_skipped += 1
            continue
        carry_qty = 0.0  # reset wenn erfolgreich verwendet

        if mark_price > 0:
            if direction == "long"  and tp_val <= mark_price:
                log(f"    ⏭ {label} @ {tp_str} bereits überschritten — übersprungen")
                continue
            if direction == "short" and tp_val >= mark_price:
                log(f"    ⏭ {label} @ {tp_str} bereits überschritten — übersprungen")
                continue

        qty_str  = round_qty(symbol, qty)
        res = api_post("/api/v2/mix/order/place-tpsl-order", {
            "symbol":       symbol,
            "productType":  PRODUCT_TYPE,
            "marginCoin":   MARGIN_COIN,
            "planType":     "profit_plan",
            "triggerPrice": tp_str,
            "triggerType":  "mark_price",
            "executePrice": "0",
            "holdSide":     direction,
            "size":         qty_str,
        })
        if res.get("code") == "00000":
            notional = tp_val * qty
            log(f"    ✓ {label} @ {tp_str} USDT × {qty_str} {get_base_coin(symbol)} (≈ {notional:.2f} USDT)")
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
                    "size":         qty_str,
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
        # Fallback-Kette: API → übergebener known_sl → Trailing-Level → trade_data (Original-SL)
        current_sl = get_sl_price(symbol, direction)
        if current_sl == 0 and known_sl > 0:
            current_sl = known_sl
            log(f"    SL aus Trade-Setup als Fallback: {current_sl}")
        # Trailing-Level-Fallback: genauer als Original-SL aus trade_data
        if current_sl == 0:
            _trl = trailing_sl_level.get(symbol, 0)
            _td  = trade_data.get(symbol, {})
            _e   = float(_td.get("entry", 0))
            _lev = int(_td.get("leverage", 10))
            _dir = _td.get("direction", direction)
            if _trl >= 3 and _e > 0:
                current_sl = calc_tp_price(_e, TP2_ROI, _dir, _lev)
                log(f"    SL aus Trailing Level 3 (TP2-Preis): {current_sl:.5f}")
            elif _trl == 2 and _e > 0:
                current_sl = calc_tp_price(_e, TP1_ROI, _dir, _lev)
                log(f"    SL aus Trailing Level 2 (TP1-Preis): {current_sl:.5f}")
            elif _trl == 1 and _e > 0:
                current_sl = _e
                log(f"    SL aus Trailing Level 1 (Entry): {current_sl:.5f}")
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

    # Trade für Daily Report aufzeichnen + Google Sheets Archiv
    _trade_record = {
        "symbol":         symbol,
        "direction":      direction,
        "leverage":       leverage,
        "entry":          entry,
        "close_price":    close_px,
        "net_pnl":        net_pnl,
        "realized_pnl":   realized,
        "fee":            fee,
        "peak_size":      peak_size,
        "hold_str":       hold_str,
        "ts":             int(time.time()),
        "trailing_level": trailing_sl_level.get(symbol, 0),
        "won":            won,
    }
    closed_trades.append(_trade_record)
    csv_log_trade(_trade_record)     # → Railway Volume CSV (non-blocking, immer aktiv)
    sheets_log_trade(_trade_record)  # → Google Sheets (non-blocking, optional)
    save_state()

    # Internen Status zurücksetzen
    last_known_avg.pop(symbol, None)
    last_known_size.pop(symbol, None)
    new_trade_done.pop(symbol, None)
    sl_at_entry.pop(symbol, None)
    harsi_sl.pop(symbol, None)
    trailing_sl_level.pop(symbol, None)
    trade_data.pop(symbol, None)

    # Noch offene TP-Orders aufräumen
    cancel_all_tp_orders(symbol)

    # DCA Limit-Orders stornieren — wichtig bei manuellem Close VOR TP1.
    # Ohne diesen Aufruf bleiben DCA-Limit-Orders auf Bitget aktiv und würden
    # bei einem Kursrücklauf füllen → Ghost-Position, die der Server nicht kennt.
    if direction and direction != "?":
        cancel_open_dca_orders(symbol, direction)


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

    # Mark-Price-Guard: Entry-SL darf nicht auf der falschen Seite des Mark-Price liegen
    _mark = get_mark_price(symbol)
    if _mark > 0:
        if direction == "long" and entry_price >= _mark:
            log(f"  ⚠ SL-auf-Entry: entry {entry_price:.5f} >= Mark {_mark:.5f} "
                f"— skip (Bitget würde ablehnen)")
            return
        if direction == "short" and entry_price <= _mark:
            log(f"  ⚠ SL-auf-Entry: entry {entry_price:.5f} <= Mark {_mark:.5f} "
                f"— skip (Bitget würde ablehnen)")
            return

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
        base_coin  = get_base_coin(symbol)
        qty_dec    = get_qty_decimals(symbol)
        if cur_size > 0:
            pos_usdt = cur_size * entry_price
            size_str = f"{cur_size:.{qty_dec}f} {base_coin} (≈ {pos_usdt:.2f} USDT)"
        else:
            size_str = "—"
        profit_str = f"+{tp1_profit:.2f} USDT" if tp1_profit > 0 else "—"
        telegram(
            f"🔒 <b>TP1 ausgelöst — {symbol}</b>\n"
            f"SL → Entry @ {sl_str} USDT (Break-even gesichert)\n"
            f"━━━━━━━━━━\n"
            f"💰 Realisiert:      {profit_str}\n"
            f"📦 Restposition:    {size_str}\n"
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

    # ── Mark-Price-Guard ─────────────────────────────────────────
    # Bitget lehnt ab wenn SL-Preis auf der falschen Seite des Mark-Price liegt.
    # Long:  SL muss < Mark-Price (SL über dem Markt → Fehler)
    # Short: SL muss > Mark-Price (SL unter dem Markt → Fehler)
    _mark = get_mark_price(symbol)
    if _mark > 0:
        if direction == "long" and sl_price >= _mark:
            log(f"  ⚠ Trailing SL Level {level}: sl_price {sl_price:.5f} >= Mark {_mark:.5f} "
                f"— skip (Bitget würde ablehnen: SL muss < Mark für Long)")
            return
        if direction == "short" and sl_price <= _mark:
            log(f"  ⚠ Trailing SL Level {level}: sl_price {sl_price:.5f} <= Mark {_mark:.5f} "
                f"— skip (Bitget würde ablehnen: SL muss > Mark für Short)")
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

        td        = trade_data.get(symbol, {})
        entry     = float(td.get("entry", 0))
        base_coin = get_base_coin(symbol)
        qty_dec   = get_qty_decimals(symbol)
        if cur_size > 0 and entry > 0:
            guaranteed = (cur_size * (sl_price - entry) if direction == "long"
                          else cur_size * (entry - sl_price))
            pos_usdt       = cur_size * entry
            size_str       = f"{cur_size:.{qty_dec}f} {base_coin} (≈ {pos_usdt:.2f} USDT)"
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

        td        = trade_data.get(symbol, {})
        entry     = float(td.get("entry", 0))
        base_coin = get_base_coin(symbol)
        qty_dec   = get_qty_decimals(symbol)
        if cur_size > 0 and entry > 0:
            guaranteed = (cur_size * (harsi_price - entry) if direction == "long"
                          else cur_size * (entry - harsi_price))
            pos_usdt       = cur_size * entry
            size_str       = f"{cur_size:.{qty_dec}f} {base_coin} (≈ {pos_usdt:.2f} USDT)"
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
    dca1_size = snap_qty(symbol, base_size * DCA1_MULTIPLIER)
    dca2_size = snap_qty(symbol, base_size * DCA2_MULTIPLIER)
    base_coin = get_base_coin(symbol)

    log(f"  DCA Sizing 20/30/50: "
        f"Market={base_size} | DCA1={dca1_size} | DCA2={dca2_size}")

    results = []
    for label, price_str, qty in [
        ("DCA1", dca1_str, dca1_size),
        ("DCA2", dca2_str, dca2_size),
    ]:
        qty_str  = round_qty(symbol, qty)
        notional = float(price_str) * qty
        res = api_post("/api/v2/mix/order/place-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginMode":  "isolated",
            "marginCoin":  MARGIN_COIN,
            "size":        qty_str,
            "price":       price_str,
            "side":        side,
            "tradeSide":   "open",
            "orderType":   "limit",
            "force":       "gtc",
        })
        if res.get("code") == "00000":
            log(f"  ✓ {label} Limit @ {price_str} USDT × {qty_str} {base_coin} (≈ {notional:.2f} USDT)")
            results.append(f"{label}: {price_str} USDT × {qty_str} {base_coin} (≈ {notional:.2f} USDT)")
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


def send_deviation_warnings(
    symbol: str, direction: str,
    leverage: int, optimal_lev: int,
    rr: float,
    order_margin: float, kelly: dict,
    sl_dist_pct: float,
):
    """
    Sendet eine konsolidierte Telegram-Warnung wenn Hebel, Kelly-Grösse oder R:R
    von den empfohlenen Einstellungen abweichen.
    Wird nach der Haupt-Trade-Nachricht aufgerufen.
    """
    warnings = []

    # ── 1. Hebel-Abweichung ──────────────────────────────────
    lev_diff = leverage - optimal_lev
    if abs(lev_diff) > 2:
        if lev_diff > 0:
            lev_hint = f"zu hoch ↑ (+{lev_diff}x) — mehr Risiko als empfohlen"
        else:
            lev_hint = f"zu niedrig ↓ ({lev_diff}x) — Kapital wird nicht optimal genutzt"
        warnings.append(
            f"⚡ <b>Hebel:</b> {leverage}x gesetzt — {optimal_lev}x empfohlen\n"
            f"   {lev_hint}\n"
            f"   Formel: 25 / {sl_dist_pct:.2f}% SL-Abstand = {optimal_lev}x"
        )

    # ── 2. Positionsgrösse vs. Kelly ─────────────────────────
    half_k = kelly.get("half_kelly_usdt", 0)
    full_k = kelly.get("kelly_usdt", 0)
    if half_k > 0 and order_margin > half_k:
        pct_of_full = (order_margin / full_k * 100) if full_k > 0 else 0
        warnings.append(
            f"📊 <b>Positionsgrösse:</b> Initial-Margin {order_margin:.2f} USDT\n"
            f"   überschreitet Half-Kelly ({half_k:.2f} USDT)\n"
            f"   Full-Kelly: {full_k:.2f} USDT "
            f"→ Initial-Order = {pct_of_full:.0f}% des Kelly-Optimums\n"
            f"   Total mit DCA ca. {order_margin * 5:.2f} USDT"
        )

    # ── 3. R:R Ratio ─────────────────────────────────────────
    if rr < MIN_RR:
        warnings.append(
            f"📉 <b>R:R Ratio:</b> {rr} unter Minimum {MIN_RR}\n"
            f"   Trade ist statistisch nicht optimal — ggf. SL oder Hebel anpassen"
        )

    if warnings:
        icon = dir_icon(direction)
        header = (
            f"⚠️ {icon} <b>Abweichungen erkannt — {symbol}</b>\n"
            f"━━━━━━━━━━━━\n"
        )
        telegram(header + "\n\n".join(warnings))
        log(f"  ⚠ Abweichungs-Warnung gesendet ({len(warnings)} Punkt(e))")


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
        # Separate Warnung folgt gebündelt via send_deviation_warnings (nach Haupt-Nachricht)

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
    # new_trade_done IMMER setzen damit der Loop diesen Trade nicht nochmals
    # als Neu-Trade behandelt. Bei DCA-Fehler: Telegram-Warnung + /refresh empfehlen.
    new_trade_done[symbol] = True
    if not dca_results:
        log(f"  ⚠ DCA-Platzierung komplett fehlgeschlagen — /refresh {symbol} zum Nachholen")
        telegram(
            f"⚠️ <b>DCA fehlgeschlagen — {symbol}</b>\n"
            f"Beide DCA-Orders konnten nicht gesetzt werden.\n"
            f"Bitte <b>/refresh {symbol}</b> ausführen um sie nachzuholen."
        )
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

    # Abweichungs-Warnung (Hebel / Kelly / R:R) — separate Nachricht nach Haupt-Report
    send_deviation_warnings(
        symbol      = symbol,
        direction   = direction,
        leverage    = leverage,
        optimal_lev = optimal_lev,
        rr          = rr,
        order_margin= order_margin,
        kelly       = kelly,
        sl_dist_pct = sl_dist_pct,
    )

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
    # Fallback-Kette: API → Trailing-Level (genau) → trade_data (Original-SL)
    known_sl = get_sl_price(symbol, direction)
    if known_sl == 0:
        _trl = trailing_sl_level.get(symbol, 0)
        _td  = trade_data.get(symbol, {})
        _e   = float(_td.get("entry", 0))
        _lev = int(_td.get("leverage", 10))
        if _trl >= 3 and _e > 0:
            known_sl = calc_tp_price(_e, TP2_ROI, direction, _lev)
            log(f"  SL aus Trailing Level 3 (TP2-Preis): {known_sl:.5f}")
        elif _trl == 2 and _e > 0:
            known_sl = calc_tp_price(_e, TP1_ROI, direction, _lev)
            log(f"  SL aus Trailing Level 2 (TP1-Preis): {known_sl:.5f}")
        elif _trl == 1 and _e > 0:
            known_sl = _e
            log(f"  SL aus Trailing Level 1 (Entry): {known_sl:.5f}")
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

    # Makro-Kontext: BTC & Total2 Impuls-Richtung
    def _dir_icon(d: str) -> str:
        return ("🟢" if d == "long"
                else "🟡" if d == "recovering"
                else "🟠" if d == "recovering_short"
                else "🔴" if d == "short"
                else "⬜")
    def _dir_lbl(d: str) -> str:
        return ("LONG (bestätigt)"              if d == "long"
                else "RECOVERING (−10→0, nur Premium-Longs)"    if d == "recovering"
                else "RECOVERING (0→+10, nur Premium-Shorts)"   if d == "recovering_short"
                else "SHORT (bestätigt)"         if d == "short"
                else "unbekannt")
    btc_icon = _dir_icon(btc_dir)
    t2_icon  = _dir_icon(t2_dir)
    btc_lbl  = _dir_lbl(btc_dir)
    t2_lbl   = _dir_lbl(t2_dir)
    _macro_full         = btc_dir == "long"              and t2_dir == "long"
    _macro_full_short   = btc_dir == "short"             and t2_dir == "short"
    _macro_partial_long = (btc_dir in ("long", "recovering")       and t2_dir in ("long", "recovering")       and not _macro_full)
    _macro_partial_short= (btc_dir in ("short", "recovering_short") and t2_dir in ("short", "recovering_short") and not _macro_full_short)
    _macro_partial      = _macro_partial_long or _macro_partial_short
    macro_ok   = _macro_full or _macro_full_short or _macro_partial
    macro_icon = ("✅" if (_macro_full or _macro_full_short)
                  else "🟡" if _macro_partial_long
                  else "🟠" if _macro_partial_short
                  else "⚠️" if (btc_dir and t2_dir)
                  else "❓")

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
        f"",
        f"📡 <b>Makro-Kontext (DOM-DIR):</b>",
        f"  {btc_icon} BTC:    {btc_lbl}",
        f"  {t2_icon} Total2: {t2_lbl}",
        f"  {macro_icon} {'Beide bestätigt ✓' if (_macro_full or _macro_full_short) else 'Long-Recovery — nur Premium-Longs!' if _macro_partial_long else 'Short-Recovery — nur Premium-Shorts!' if _macro_partial_short else 'Abweichung — kein neuer Trade!' if (btc_dir and t2_dir) else 'Noch kein DOM-DIR Webhook empfangen'}",
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

            trl  = trailing_sl_level.get(sym, 0)
            sec  = sl_at_entry.get(sym, False) or trl >= 1

            if trl >= 3:
                icon     = "🏆"
                sl_label = "SL@TP2 — Gewinn gesichert"
            elif trl >= 2:
                icon     = "✅"
                sl_label = "SL@TP1 — Auf Sicher"
            elif trl >= 1 or sec:
                icon     = "🔒"
                sl_label = "SL@Entry — Break-even"
            else:
                icon     = "⚠️"
                sl_label = "SL unter Entry — Risiko"
                all_secured = False

            lines.append(
                f"{icon} <b>{sym}</b> {drct} {lev}x | "
                f"Avg={avg:.5g} | PnL={pnl:+.2f} USDT\n"
                f"   └ {sl_label}"
            )

        if all_secured:
            lines += ["", "✅ Alle Positionen auf Sicher → neuer Trade möglich"]
        else:
            lines += [
                "",
                "⚠️ Noch nicht alle Positionen gesichert",
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
        "📋 /trade ETHUSDT LONG 10 2850 2700",
        "/status | /makro | /report | /hilfe"
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

    # v4.10: Makro-Extremzonen als DOMINUS-Premium-Info (kein Block mehr)
    _xinfo = extreme_warn(direction)
    _xmsg  = format_extreme_info_msg(symbol, direction, _xinfo, "/trade")
    if _xmsg:
        reply(_xmsg)

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


def cmd_makro():
    """BTC & Total2 DOMINUS Impuls Richtungsstatus."""
    def _icon(d: str) -> str:
        return ("🟢" if d == "long"
                else "🟡" if d == "recovering"
                else "🟠" if d == "recovering_short"
                else "🔴" if d == "short"
                else "⬜")
    def _lbl(d: str) -> str:
        return ("LONG — bestätigt (≥ 0)"                  if d == "long"
                else "LONG RECOVERY — nur Premium (−10→0)" if d == "recovering"
                else "SHORT RECOVERY — nur Premium (0→+10)"if d == "recovering_short"
                else "SHORT — bestätigt (≤ 0)"             if d == "short"
                else "unbekannt — noch kein Webhook")

    btc_icon = _icon(btc_dir)
    t2_icon  = _icon(t2_dir)
    btc_lbl  = _lbl(btc_dir)
    t2_lbl   = _lbl(t2_dir)

    # Gesamtstatus
    if btc_dir == "long" and t2_dir == "long":
        overall = "✅ Beide bestätigt — alle Long-Setups erlaubt"
    elif btc_dir == "short" and t2_dir == "short":
        overall = "✅ Beide bestätigt — alle Short-Setups erlaubt"
    elif btc_dir in ("long","recovering") and t2_dir in ("long","recovering"):
        overall = "🟡 Long-Recovery aktiv — nur Premium-Longs"
    elif btc_dir in ("short","recovering_short") and t2_dir in ("short","recovering_short"):
        overall = "🟠 Short-Recovery aktiv — nur Premium-Shorts"
    elif btc_dir and t2_dir:
        overall = "⚠️ Abweichung BTC vs. Total2 — kein neuer Trade"
    else:
        overall = "❓ Noch kein Makro-Webhook empfangen"

    from datetime import datetime as _dt
    ts = _dt.utcnow().strftime("%d.%m.%Y %H:%M UTC")

    reply(
        f"📡 <b>Makro-Kontext — BTC &amp; Total2</b>\n"
        f"━━━━━━━━━━━━\n"
        f"{btc_icon} <b>BTC:</b>    {btc_lbl}\n"
        f"{t2_icon} <b>Total2:</b> {t2_lbl}\n"
        f"━━━━━━━━━━━━\n"
        f"{overall}\n"
        f"\n"
        f"<i>Stand: {ts}</i>\n"
        f"<i>Aktualisiert durch Alarm 5/5b/5c/5d/5e/5f Webhooks</i>\n"
        f"\n"
        f"📋 /status | /berechnen | /report"
    )


# ═══════════════════════════════════════════════════════════════
# ALARM-VORLAGEN-GENERATOR (v4.8) — Copy & Paste für TradingView
# ───────────────────────────────────────────────────────────────
# Erzeugt fertige Alarm-Vorlagen (Name, Bedingung, Message-JSON,
# Webhook-URL, Einstellungen) die der User nur noch per Copy-Paste
# in den TradingView-Alarm-Dialog einfügen muss.
# Abgedeckt: Alarm 1/1b (H4_TRIGGER), 2/2b (H2_SIGNAL),
#            3/3b (HARSI_EXIT — inkl. 30-Min-Fenster-Status),
#            4/4b (HARSI_SL).
# ═══════════════════════════════════════════════════════════════

_WEBHOOK_URL_PLACEHOLDER = "<deine-railway-domain>"


def _alarm_webhook_url() -> str:
    """Vollständige Webhook-URL inkl. Token für die Alarm-Vorlage.
    Nutzt WEBHOOK_URL (Railway Variable) 1:1; sonst Fallback mit Hinweis-
    Placeholder für die Domain."""
    if WEBHOOK_URL:
        return WEBHOOK_URL
    token = WEBHOOK_SECRET or "dominus"
    return f"https://{_WEBHOOK_URL_PLACEHOLDER}/webhook?token={token}"


def _alarm_window_status(symbol: str, direction: str) -> tuple:
    """(status_line_html, is_active) — prüft last_h2_signal_time[symbol_direction].
    Gibt dem User sofort Klarheit ob der HARSI-Alarm überhaupt noch Sinn macht."""
    key = f"{symbol}_{direction}"
    ts  = last_h2_signal_time.get(key)
    if ts is None:
        return (
            "ℹ️ <i>Kein aktives H2-Signal für diese Richtung gespeichert — "
            "Alarm-Vorlage kann trotzdem genutzt werden, das 30-Min-Fenster "
            "startet sobald Alarm 2/2b das nächste Mal feuert.</i>",
            False,
        )
    elapsed_sec = (datetime.utcnow() - ts).total_seconds()
    elapsed_min = int(elapsed_sec // 60)
    if elapsed_sec > 1800:
        return (
            f"⛔ <b>30-Min-Fenster abgelaufen</b> — H2-Signal vor {elapsed_min} Min empfangen.\n"
            f"Signal gilt laut DOMINUS-Regel <b>nicht mehr</b> — HARSI-Exit jetzt nicht mehr einsteigen!",
            False,
        )
    remaining = 30 - elapsed_min
    expiry = (ts + timedelta(minutes=30)).strftime("%d.%m.%Y %H:%M UTC")
    return (
        f"⏱ <b>Fenster offen — noch ca. {remaining} Min gültig</b> (läuft {expiry} ab)",
        True,
    )


def _alarm_block(title: str, alarm_name: str, condition_html: str, json_msg: str,
                 trigger_cfg: str, doc_anchor: str, window_line: str = "") -> str:
    """Einheitlicher Copy-Paste-Block — Name/JSON/URL als <code> für
    Telegram „tap to copy". html.escape() nur auf externe Strings
    (Symbol/Webhook/etc.) — das Pine-Template {{close}} darf NICHT
    escaped werden, sonst funktioniert es in TradingView nicht."""
    webhook = _alarm_webhook_url()
    parts = [f"🔔 <b>{title}</b>", "━" * 12]
    if window_line:
        parts += [window_line, ""]
    parts += [
        "<b>① Alarm-Name</b> — kopieren:",
        f"<code>{html.escape(alarm_name)}</code>",
        "",
        "<b>② Bedingung</b> im TV-Alarm-Dialog einstellen:",
        condition_html,
        "",
        "<b>③ Message (JSON)</b> — kopieren &amp; ins Message-Feld einfügen:",
        f"<code>{html.escape(json_msg)}</code>",
        "",
        "<b>④ Webhook-URL</b> — unter <i>Benachrichtigungen → Webhook-URL</i>:",
        f"<code>{html.escape(webhook)}</code>",
        "",
        f"<b>⑤ Einstellungen:</b> {trigger_cfg}",
        "",
        _doc_link(doc_anchor, "Detaillierte Anleitung im Handbuch"),
    ]
    return "\n".join(parts)


def build_alarm_harsi_exit(symbol: str, direction: str, show_window: bool = True) -> str:
    """Alarm 3/3b — HARSI_EXIT. Berücksichtigt das 30-Min-Fenster
    aus last_h2_signal_time."""
    is_long = direction == "long"
    label   = "Long" if is_long else "Short"
    side    = "long" if is_long else "short"
    alarm_name = f"⏰ DOMINUS {label} HARSI frei"
    condition = (
        f"Indikator <b>DOMINUS Orchestrator</b> → Bedingung <b>DOMINUS {label} HARSI frei</b>\n"
        f"Intervall: <b>H2</b> · Auslösung: <b>Einmal pro Bar Close</b> · Typ: <b>Einzelner Coin</b>"
    )
    # JSON identisch zum HTML-Template (Alarm 3/3b)
    json_msg = (
        '{"symbol":"' + symbol + '","side":"' + side +
        '","entry":{{close}},"timeframe":"{{interval}}","signal":"HARSI_EXIT"}'
    )
    trigger_cfg = "Einmal pro Bar · Unbefristet · App + Ton · Typ: Technisch"
    anchor = "sec-alarm3" if is_long else "sec-alarm3b"
    title  = f"Alarm {'3' if is_long else '3b'} — HARSI Exit {label} ({symbol})"

    window_line = ""
    if show_window:
        wl, _active = _alarm_window_status(symbol, direction)
        window_line = wl
    # Strikte 30-Min-Regel immer explizit anhängen (auch bei leerem Fenster-Status)
    strict = (
        "⚠️ <b>Strikt 30 Min ab H2-Signal</b> — danach Alarm in TV löschen; "
        "spätere Feuerungen nicht mehr traden."
    )
    window_line = (window_line + "\n" + strict) if window_line else strict
    return _alarm_block(title, alarm_name, condition, json_msg, trigger_cfg, anchor, window_line)


def build_alarm_harsi_sl(symbol: str, direction: str) -> str:
    """Alarm 4/4b — HARSI_SL: zieht SL bei offenem Trade auf HARSI-Ausstiegslinie."""
    is_long = direction == "long"
    label   = "Long" if is_long else "Short"
    direction_val = "long" if is_long else "short"
    alarm_name = f"🛡 DOMINUS HARSI SL {label}"
    cross_dir  = "aufwärts" if is_long else "abwärts"
    cross_val  = "−20" if is_long else "+20"
    condition = (
        f"Indikator <b>HARSI</b> → Plot <b>RSI Histogram</b> · kreuzt <b>{cross_dir}</b> · "
        f"Wert <code>{cross_val}</code>\n"
        f"Intervall: <b>H2</b> · Auslösung: <b>Einmal pro Bar Close</b> · Typ: <b>Einzelner Coin</b>\n"
        f"<i>Wichtig:</i> im JSON-Message unten <code>RSI Overlay</code> (echter Preis) — "
        f"<b>nicht</b> RSI Histogram!"
    )
    # JSON identisch zum HTML-Template (Alarm 4/4b)
    json_msg = (
        '{"symbol":"' + symbol + '","direction":"' + direction_val +
        '","signal":"HARSI_SL","price":{{plot("RSI Overlay")}}}'
    )
    trigger_cfg = "Einmal pro Bar · Unbefristet · App + Ton · Typ: Technisch"
    anchor = "sec-alarm4" if is_long else "sec-alarm4b"
    title  = f"Alarm {'4' if is_long else '4b'} — HARSI SL {label} ({symbol})"
    return _alarm_block(title, alarm_name, condition, json_msg, trigger_cfg, anchor)


def build_alarm_h2_entry(symbol: str, direction: str) -> str:
    """Alarm 2/2b — H2_SIGNAL Entry (mit Plot-Werten für premium/harsi_warn/btc_t2_warn)."""
    is_long = direction == "long"
    label   = "Long" if is_long else "Short"
    side    = "long" if is_long else "short"
    icon    = "🟢" if is_long else "🔴"
    alarm_name = f"{icon} DOMINUS {label} Entry"
    condition = (
        f"Indikator <b>DOMINUS Orchestrator</b> → Bedingung <b>DOMINUS {label}</b>\n"
        f"Intervall: <b>H2</b> · Auslösung: <b>Einmal pro Bar Close</b> · Typ: <b>Einzelner Coin</b>"
    )
    plot_prem   = '{{plot("Premium ' + label + '")}}'
    plot_harsi  = '{{plot("' + label + ' HARSI Warnung")}}'
    plot_btc_t2 = '{{plot("' + label + ' BTC/Total2 Warn")}}'
    json_msg = (
        '{"symbol":"' + symbol + '","side":"' + side +
        '","entry":{{close}},"timeframe":"{{interval}}","signal":"H2_SIGNAL"' +
        ',"premium":' + plot_prem +
        ',"harsi_warn":' + plot_harsi +
        ',"btc_t2_warn":' + plot_btc_t2 + '}'
    )
    trigger_cfg = (
        "Einmal pro Bar · Unbefristet · App + Ton · Typ: <b>Technisch</b> "
        "(NICHT Watchlist — sonst bleiben Plots leer!)"
    )
    anchor = "sec-alarm2" if is_long else "sec-alarm2b"
    title  = f"Alarm {'2' if is_long else '2b'} — H2 {label} Entry ({symbol})"
    return _alarm_block(title, alarm_name, condition, json_msg, trigger_cfg, anchor)


def build_alarm_h4_trigger(direction: str) -> str:
    """Alarm 1/1b — H4_TRIGGER (Watchlist-Alarm, nutzt {{ticker}})."""
    is_long  = direction == "long"
    label    = "Long" if is_long else "Short"
    side     = "long" if is_long else "short"
    buy_sell = "Buy" if is_long else "Sell"
    alarm_name = f"🔔 H4 {label}-Trigger"
    condition = (
        f"Indikator <b>DOMINUS Buy/Sell</b> → Plot <b>{buy_sell}</b> · "
        f"kreuzt nach oben · Wert <code>0</code>\n"
        f"Intervall: <b>H4</b> · Auslösung: <b>Einmal pro Bar Close</b> · Typ: <b>Watchlist</b>"
    )
    json_msg = (
        '{"symbol":"{{ticker}}","side":"' + side +
        '","entry":{{close}},"timeframe":"{{interval}}","signal":"H4_TRIGGER"}'
    )
    trigger_cfg = "Einmal pro Bar · Unbefristet · App + Ton · Typ: <b>Watchlist</b>"
    anchor = "sec-alarm1" if is_long else "sec-alarm1b"
    title  = f"Alarm {'1' if is_long else '1b'} — H4 {label}-Trigger (Watchlist, alle Coins)"
    return _alarm_block(title, alarm_name, condition, json_msg, trigger_cfg, anchor)


def _list_active_harsi_windows() -> list:
    """Liste (symbol, direction, remaining_min, expiry_str) aller aktiven Fenster."""
    out = []
    now = datetime.utcnow()
    for key, ts in list(last_h2_signal_time.items()):
        elapsed_min = int((now - ts).total_seconds() // 60)
        if elapsed_min > 30:
            continue
        try:
            _sym, _dir = key.rsplit("_", 1)
        except ValueError:
            continue
        remaining = 30 - elapsed_min
        expiry    = (ts + timedelta(minutes=30)).strftime("%H:%M UTC")
        out.append((_sym, _dir, remaining, expiry))
    # Längste Rest-Zeit zuerst
    out.sort(key=lambda x: -x[2])
    return out


def cmd_alarm(parts: list):
    """
    Copy-Paste ready Alarm-Vorlagen für TradingView.

    /alarm                             → Übersicht & aktive HARSI-Fenster
    /alarm SYMBOL LONG|SHORT           → Kurzform → Alarm 3/3b (HARSI Exit)
    /alarm harsi  SYMBOL LONG|SHORT    → Alarm 3/3b (HARSI_EXIT) inkl. 30-Min-Status
    /alarm harsisl SYMBOL LONG|SHORT   → Alarm 4/4b (HARSI_SL, für offene Trades)
    /alarm h2     SYMBOL LONG|SHORT    → Alarm 2/2b (H2_SIGNAL Entry)
    /alarm h4     LONG|SHORT           → Alarm 1/1b (H4_TRIGGER Watchlist)
    """
    # 1) Keine Args → Übersicht
    if len(parts) == 1:
        lines = [
            "🔔 <b>Alarm-Vorlagen — Copy &amp; Paste für TradingView</b>",
            "━" * 12,
            "",
            "<b>Befehle:</b>",
            "<code>/alarm SYMBOL LONG|SHORT</code> — Kurzform → HARSI Exit (Alarm 3/3b)",
            "<code>/alarm harsi SYMBOL LONG|SHORT</code> — Alarm 3/3b (HARSI Exit)",
            "<code>/alarm harsisl SYMBOL LONG|SHORT</code> — Alarm 4/4b (HARSI SL)",
            "<code>/alarm h2 SYMBOL LONG|SHORT</code> — Alarm 2/2b (H2 Entry)",
            "<code>/alarm h4 LONG|SHORT</code> — Alarm 1/1b (H4 Watchlist)",
            "",
        ]
        active = _list_active_harsi_windows()
        if active:
            lines.append("<b>🟢 Aktive HARSI-Fenster (30-Min-Timer läuft):</b>")
            for sym, drct, rem, exp in active:
                icon = dir_icon(drct)
                lines.append(f"  {icon} <b>{sym}</b> {drct.upper()} — noch {rem} Min (bis {exp})")
                lines.append(f"     → <code>/alarm harsi {sym} {drct.upper()}</code>")
        else:
            lines.append("<i>Aktuell kein aktives HARSI-Fenster.</i>")
        lines += [
            "",
            "💡 <i>WEBHOOK_URL in Railway setzen damit die URL automatisch "
            "in jede Vorlage eingesetzt wird.</i>" if not WEBHOOK_URL else "",
            "📋 /hilfe für alle Befehle",
        ]
        reply("\n".join([l for l in lines if l is not None]))
        return

    # 2) Args parsen
    def _parse_direction(tok: str) -> str:
        t = tok.lower()
        if t in ("long", "buy", "l"):
            return "long"
        if t in ("short", "sell", "s"):
            return "short"
        return ""

    def _parse_symbol(tok: str) -> str:
        sym = tok.upper().replace("/", "").replace("-", "")
        if not sym.endswith("USDT"):
            sym += "USDT"
        return sym

    tokens = [p.strip() for p in parts[1:]]
    sub    = tokens[0].lower()

    # 3) Expliziter Sub-Command
    if sub in ("harsi", "harsisl", "h2", "h4"):
        if sub == "h4":
            if len(tokens) < 2:
                reply("❌ Format: <code>/alarm h4 LONG|SHORT</code>\nBeispiel: /alarm h4 LONG")
                return
            direction = _parse_direction(tokens[1])
            if not direction:
                reply("❌ Richtung muss LONG oder SHORT sein.")
                return
            reply(build_alarm_h4_trigger(direction))
            return

        if len(tokens) < 3:
            reply(f"❌ Format: <code>/alarm {sub} SYMBOL LONG|SHORT</code>\n"
                  f"Beispiel: /alarm {sub} ETHUSDT LONG")
            return
        symbol    = _parse_symbol(tokens[1])
        direction = _parse_direction(tokens[2])
        if not direction:
            reply("❌ Richtung muss LONG oder SHORT sein.")
            return

        if sub == "harsi":
            reply(build_alarm_harsi_exit(symbol, direction))
        elif sub == "harsisl":
            reply(build_alarm_harsi_sl(symbol, direction))
        else:  # h2
            reply(build_alarm_h2_entry(symbol, direction))
        return

    # 4) Kurzform: /alarm SYMBOL LONG|SHORT → HARSI Exit
    if len(tokens) >= 2:
        direction = _parse_direction(tokens[1])
        if direction:
            symbol = _parse_symbol(tokens[0])
            reply(build_alarm_harsi_exit(symbol, direction))
            return

    reply(
        "❌ Nicht erkannt.\n"
        "Sende <code>/alarm</code> ohne Argumente für die Übersicht."
    )


def cmd_hilfe():
    reply(
        "🤖 <b>DOMINUS Bot — Befehle</b>\n"
        "━━━━━━━━━━━━\n"
        "\n"
        "📊 <b>Info &amp; Überblick:</b>\n"
        "/status — Kurzstatus aller offenen Positionen\n"
        "/berechnen — Kontostand + Money-Management + Chart-Links\n"
        "/makro — BTC &amp; Total2 Impuls-Richtung + Trade-Erlaubnis\n"
        "/report — Tages- &amp; Monats-P&amp;L Report\n"
        "\n"
        "⚙️ <b>Aktionen:</b>\n"
        "/trade SYMBOL LONG|SHORT HEBEL ENTRY SL\n"
        "   → Setup berechnen + alle Chart-Links\n"
        "   Beispiel: /trade ETHUSDT LONG 10 2850 2700\n"
        "/refresh [SYMBOL] — SL/TP/DCA sofort prüfen &amp; reparieren\n"
        "   Beispiel: /refresh BTCUSDT  (oder /refresh für alle)\n"
        "\n"
        "🔔 <b>Alarm-Vorlagen (Copy-Paste für TradingView):</b>\n"
        "/alarm — Übersicht + aktive HARSI-Fenster\n"
        "/alarm SYMBOL LONG|SHORT — Kurzform → Alarm 3/3b (HARSI Exit)\n"
        "/alarm harsi|harsisl|h2 SYMBOL LONG|SHORT\n"
        "/alarm h4 LONG|SHORT\n"
        "\n"
        "🎯 <b>Premium Setup (DOMINUS):</b>\n"
        "✅ Long+Long: beide Impulse ≥ 0 → voller Trade\n"
        "🟡 Long+Recovering: einer im Oversold-Exit → nur Premium\n"
        "🔴 Short+Short: beide Impulse ≤ 0 → voller Short-Trade\n"
        "🟠 Short+Recovering: einer im Overbought-Exit → nur Premium\n"
        "\n"
        "⚡ <b>Automatisch nach /trade:</b>\n"
        "• DCA1 + DCA2 Limit-Orders (20/30/50% Sizing)\n"
        "• TP1–TP4 (15/20/25/40% ROI)\n"
        "• SL → Entry nach TP1 | Trailing SL nach TP2/TP3\n"
        "\n"
        "🎯 <b>v4.10 DOMINUS Premium-Zonen (Handbuch-konform):</b>\n"
        "• TradingView-Alarme BTC_OVERSOLD / BTC_OVERBOUGHT\n"
        "  + T2_OVERSOLD / T2_OVERBOUGHT markieren Premium-Fenster\n"
        "• Oversold + grüne Richtung → Long Premium\n"
        "  Overbought + rote Richtung → Short Premium\n"
        "• Dauer 4h (EXTREME_COOLDOWN_H) — kein Block, nur Info\n"
        "• Gegenrichtung bekommt ⚠️ Warnung (antizyklisch)\n"
        "• Status der Premium-Zonen sichtbar unter /status"
    )


def cmd_status():
    """Kurzer Positionsstatus + v4.10 DOMINUS Premium-Zonen."""
    # v4.10: Makro-Premium-Zonen vorab auflisten (auch ohne offene Positionen relevant)
    _extreme_lines = macro_extreme_status_lines()
    _any_premium   = any("Premium" in l for l in _extreme_lines)
    _macro_header  = "🌍 <b>Makro-Premium-Zonen:</b>" + (" 🎯" if _any_premium else "")

    positions = get_all_positions()
    if not positions:
        reply(
            "✅ Keine offenen Positionen.\n"
            "\n"
            + _macro_header + "\n"
            + "\n".join(_extreme_lines) + "\n"
            "\n"
            "📋 /berechnen | /makro | /report"
        )
        return
    lines = [f"📊 <b>{len(positions)} offene Position(en):</b>"]
    for pos in positions:
        sym      = pos.get("symbol", "?")
        qty      = float(pos.get("total", 0))
        drct     = pos.get("holdSide", "?").upper()
        lev      = int(float(pos.get("leverage", 10)))
        pnl      = float(pos.get("unrealizedPL", 0))
        mark     = get_mark_price(sym)
        secured  = sl_at_entry.get(sym, False)
        trl_lvl  = trailing_sl_level.get(sym, 0)
        trl_tag  = {0: "", 1: " · SL=Entry", 2: " · Trail→TP1", 3: " · Trail→TP2"}.get(trl_lvl, "")
        icon     = "🔒" if secured else "📈"
        base     = get_base_coin(sym)
        qty_dec  = get_qty_decimals(sym)
        pos_usdt = qty * mark if mark > 0 else 0
        qty_str  = f"{qty:.{qty_dec}f} {base} (≈ {pos_usdt:.2f} USDT)" if pos_usdt > 0 else f"{qty:.{qty_dec}f} {base}"
        lines.append(
            f"{icon} <b>{sym}</b> {drct} {lev}x\n"
            f"   Qty: {qty_str}\n"
            f"   Mark: {mark} | PnL: {pnl:+.2f} USDT{trl_tag}"
        )
    lines += [
        "",
        _macro_header,
        *_extreme_lines,
        "",
        "📋 /berechnen | /makro | /report | /refresh",
    ]
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
        sym      = pos.get("symbol", "?")
        drct     = pos.get("holdSide", "?").upper()
        lev      = int(float(pos.get("leverage", 10)))
        avg      = float(pos.get("openPriceAvg", 0))
        qty      = float(pos.get("total", 0))
        mark     = get_mark_price(sym)
        sl       = get_sl_price(sym, pos.get("holdSide", "long"))
        tps      = get_existing_tps(sym)
        n_tp     = len(tps)
        secured  = sl_at_entry.get(sym, False)
        base     = get_base_coin(sym)
        qty_dec  = get_qty_decimals(sym)
        pos_usdt = qty * mark if mark > 0 else qty * avg
        qty_str  = f"{qty:.{qty_dec}f} {base} (≈ {pos_usdt:.2f} USDT)"
        lock     = "🔒 SL=Entry" if secured else (f"🛡 SL={sl:.4f}" if sl > 0 else "⚠️ Kein SL!")
        lines.append(
            f"\n📍 <b>{sym}</b> {drct} {lev}x\n"
            f"   Qty: {qty_str}\n"
            f"   Entry={avg:.4f} | Mark={mark}\n"
            f"   {lock} | TPs gesetzt: {n_tp}"
        )
    lines += ["", "📋 /status | /makro | /report"]
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
        report += "\n\n📋 /status | /makro | /berechnen"
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
        elif cmd == "/makro":
            cmd_makro()
        elif cmd == "/refresh":
            cmd_refresh(parts)
        elif cmd == "/report":
            cmd_report()
        elif cmd == "/alarm":
            cmd_alarm(parts)
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
        global btc_dir, t2_dir
        # ── Token-Prüfung ─────────────────────────────────────
        # Token aus URL-Parameter ODER aus JSON-Body akzeptieren.
        # WICHTIG: Wir geben bei ungültigem Token TROTZDEM 200 zurück —
        # ein 401/403 würde TradingView als Fehler anzeigen und Retries auslösen.
        # Stattdessen: intern loggen und still verwerfen.
        token_url  = flask_request.args.get("token", "")
        # Payload zuerst roh lesen (vor JSON-Parse, damit wir token aus body lesen können)
        raw_body   = flask_request.get_data(as_text=True) or ""

        # ── Body-Bereinigung vor JSON-Parse ───────────────────
        import re as _re

        # Fix 1: Unaufgelöste TradingView-Platzhalter → 0
        # Bei Watchlist-Alarmen ersetzt TradingView {{plot(...)}} NICHT —
        # der Literal-Text landet im JSON und macht es ungültig.
        # Betrifft: harsi_warn, btc_t2_warn, premium bei Watchlist-Alarmen.
        # Defaultwert 0 = "kein Problem erkannt" (sicherster Fallback).
        raw_clean = _re.sub(r'\{\{[^}]+\}\}', '0', raw_body)

        # Fix 2: NaN / Infinity (Pine Script 'na' → TradingView NaN)
        # NaN ist kein valides JSON → null ersetzen.
        raw_clean = _re.sub(r'\bNaN\b',       'null', raw_clean)
        raw_clean = _re.sub(r'\bInfinity\b',  'null', raw_clean)
        raw_clean = _re.sub(r'\b-Infinity\b', 'null', raw_clean)

        # ── Payload parsen ────────────────────────────────────
        # IMMER 200 zurückgeben — nie 400/401/500 an TradingView senden.
        # Fehler intern loggen + Telegram-Alert, damit nichts lautlos verschwindet.
        try:
            data = json.loads(raw_clean) if raw_clean.strip() else {}
        except Exception as _e:
            log(f"⚠ Webhook: JSON-Parse-Fehler: {_e} | Body (erste 200 Z.): {raw_body[:200]}")
            telegram(f"⚠️ <b>Webhook Parse-Fehler</b>\n{_e}\nBody: <code>{raw_body[:150]}</code>")
            return jsonify({"status": "ignored", "reason": "parse_error"}), 200

        # Token-Prüfung (nach Parse, damit body-Token auch funktioniert)
        token_body = str(data.get("token", ""))
        token      = token_url or token_body
        if WEBHOOK_SECRET and token != WEBHOOK_SECRET:
            log(f"⚠ Webhook: Ungültiger Token (url='{token_url}' body='{token_body}')")
            return jsonify({"status": "ignored", "reason": "unauthorized"}), 200

        raw_symbol = data.get("symbol", "").upper()
        entry      = float(data.get("entry", 0) or 0)
        timeframe  = data.get("timeframe", "H2").upper()

        # Richtung: "direction" ODER "side" (beide Feldnamen akzeptieren)
        # Alarm-Templates senden "side":"long"/"short" — Script liest beide.
        direction = data.get("direction", "").lower()
        if direction not in ("long", "short"):
            direction = data.get("side", "").lower()
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
                f"dir={data.get('direction','')} side={data.get('side','')} "
                f"sym={raw_symbol})")
            return jsonify({"status": "ignored", "reason": "no signal"}), 200

        log(f"📡 TradingView Alert: {symbol} {direction.upper()} "
            f"@ {entry} [{timeframe}]")

        if entry == 0:
            entry = get_mark_price(symbol)

        signal_type = data.get("signal", "").upper()
        log(f"\U0001f4e1 Alert: {symbol} {direction.upper()} @ {entry} [{timeframe}]")

        # ─────────────────────────────────────────────────────────────
        # v4.10: BTC_OVERSOLD / BTC_OVERBOUGHT / T2_OVERSOLD / T2_OVERBOUGHT
        # DOMINUS-Handbuch-konform: Extremzone = Premium-Entry-Bestätigung.
        # Kein Entry-Block — nur Premium-Info + Soft-Warnung für Gegenposen.
        # ─────────────────────────────────────────────────────────────
        if signal_type in ("BTC_OVERSOLD", "BTC_OVERBOUGHT",
                            "T2_OVERSOLD",  "T2_OVERBOUGHT"):
            _market = "btc" if signal_type.startswith("BTC_") else "total2"
            _label  = "BTC" if _market == "btc" else "Total2"
            if signal_type.endswith("_OVERSOLD"):
                _set_macro_extreme(_market, -1)
                _state_txt   = "OVERSOLD"
                _emoji       = "🔻"
                _premium_dir = "LONG"   # DOMINUS: Oversold = Long Premium
                _risk_dir    = "short"  # offene SHORTs sind jetzt gegen Reversal
            else:
                _set_macro_extreme(_market, +1)
                _state_txt   = "OVERBOUGHT"
                _emoji       = "🔺"
                _premium_dir = "SHORT"  # DOMINUS: Overbought = Short Premium
                _risk_dir    = "long"   # offene LONGs sind jetzt gegen Reversal
            save_state()

            _until_ts  = macro_extreme[_market]["until_ts"]
            _until_str = datetime.utcfromtimestamp(_until_ts).strftime("%d.%m.%Y %H:%M UTC")
            log(f"📡 {_label} → {_state_txt} (Premium-Zone bis {_until_str})")

            # Offene Gegenpositionen: Hinweis, dass der Makro-Kontext umgeschlagen hat.
            _warn_trades = []
            for pos in get_all_positions():
                if pos.get("holdSide", "").lower() == _risk_dir:
                    _sym  = pos.get("symbol", "")
                    _pnl  = float(pos.get("unrealizedPL", 0))
                    _warn_trades.append(f"  ⚠️ {_sym} {_risk_dir.upper()} | PnL={_pnl:+.2f} USDT")

            _msg_lines = [
                f"{_emoji} <b>{_label} Impuls → {_state_txt}</b>",
                "━" * 12,
                f"🎯 <b>{_premium_dir} Premium-Zone aktiv</b> (DOMINUS-Handbuch)",
                f"Zeitraum: {EXTREME_COOLDOWN_H}h · bis <b>{_until_str}</b>",
                "",
                f"→ Neue H2-Signale in {_premium_dir}-Richtung bekommen Premium-Tag.",
                "→ Kein Entry-Block — alle regulären Checks laufen normal.",
                "→ Bestehende Positionen laufen normal weiter (Trailing SL aktiv).",
            ]
            if _warn_trades:
                _msg_lines += [
                    "",
                    f"⚠️ <b>Offene {_risk_dir.upper()}-Positionen (gegen Reversal):</b>",
                    *_warn_trades,
                    "",
                    "🔔 SL/Trailing prüfen!",
                ]
            telegram("\n".join(_msg_lines))

            return jsonify({
                "status":   "ok",
                "market":   _market,
                "extreme":  _state_txt,
                "premium":  _premium_dir,
                "until":    _until_str,
            }), 200

        # ─────────────────────────────────────────────────────────────
        # BTC_DIR / T2_DIR — Makro-Kontext: DOM-DIR Impuls-Richtung geändert
        # Webhook von BTC H2 Chart (signal=BTC_DIR) oder Total2 (signal=T2_DIR)
        # zone="neutral"    → direction long/short ab Nulllinie (bestätigt)
        # zone="recovering" + long  → Impuls verlässt Oversold  (−10 aufwärts, noch −10..0)
        # zone="recovering" + short → Impuls verlässt Overbought (+10 abwärts, noch 0..+10)
        # ─────────────────────────────────────────────────────────────
        if signal_type in ("BTC_DIR", "T2_DIR"):
            label    = "BTC" if signal_type == "BTC_DIR" else "Total2"
            prev     = btc_dir if signal_type == "BTC_DIR" else t2_dir
            zone_val = data.get("zone", "").lower()

            # Zustand bestimmen: recovering (long) oder recovering_short oder normal
            if zone_val == "recovering":
                new_dir = "recovering" if direction == "long" else "recovering_short"
            else:
                new_dir = direction

            if signal_type == "BTC_DIR":
                btc_dir = new_dir
            else:
                t2_dir = new_dir
            save_state()

            if new_dir == "recovering":
                _anker = "sec-alarm5c" if signal_type == "BTC_DIR" else "sec-alarm5d"
                _albl  = ("Alarm 5c — BTC Long Recovery" if signal_type == "BTC_DIR"
                           else "Alarm 5d — Total2 Long Recovery")
                dir_label = "🟡 Recovering Long (−10→0)"
                dir_info  = (
                    "Impuls verlässt Oversold-Zone.\n"
                    "🟡 <b>Nur Premium-Longs</b> erlaubt bis Bestätigung bei 0.\n"
                    + _doc_link(_anker, _albl)
                )
            elif new_dir == "recovering_short":
                _anker = "sec-alarm5e" if signal_type == "BTC_DIR" else "sec-alarm5f"
                _albl  = ("Alarm 5e — BTC Short Recovery" if signal_type == "BTC_DIR"
                           else "Alarm 5f — Total2 Short Recovery")
                dir_label = "🟠 Recovering Short (0→+10)"
                dir_info  = (
                    "Impuls verlässt Overbought-Zone.\n"
                    "🟠 <b>Nur Premium-Shorts</b> erlaubt bis Bestätigung bei 0.\n"
                    + _doc_link(_anker, _albl)
                )
            elif direction == "long":
                _anker = "sec-alarm5" if signal_type == "BTC_DIR" else "sec-alarm5b"
                _albl  = "Alarm 5 — BTC Long" if signal_type == "BTC_DIR" else "Alarm 5b — Total2 Long"
                dir_label = "🟢 Grün (Bullish bestätigt)"
                dir_info  = "Impuls über Nulllinie bestätigt.\n" + _doc_link(_anker, _albl)
            else:
                _anker = "sec-alarm5" if signal_type == "BTC_DIR" else "sec-alarm5b"
                _albl  = "Alarm 5 — BTC Short" if signal_type == "BTC_DIR" else "Alarm 5b — Total2 Short"
                dir_label = "🔴 Rot (Bearish)"
                dir_info  = "Impuls unter Nulllinie.\n" + _doc_link(_anker, _albl)
            log(f"📡 {label} DOM-DIR geändert → {new_dir.upper()} (vorher: {prev or '?'})")

            # Offene Positionen prüfen: Positionen GEGEN neuen Impuls warnen
            positions   = get_all_positions()
            warn_trades = []
            for pos in positions:
                sym  = pos.get("symbol", "")
                side = pos.get("holdSide", "").lower()
                if side and side != direction:
                    pnl = float(pos.get("unrealizedPL", 0))
                    warn_trades.append(f"  ⚠️ {sym} {side.upper()} | PnL={pnl:+.2f} USDT")

            icon = ("🟡" if new_dir == "recovering"
                    else "🟠" if new_dir == "recovering_short"
                    else "🟢" if direction == "long" else "🔴")
            msg = (
                f"{icon} <b>{label} Impuls → {dir_label}</b>\n"
                f"━━━━━━━━━━━━\n"
                f"{dir_info}"
            )
            if warn_trades:
                msg += (
                    f"\n\n⚠️ <b>Offene Gegenpositionen:</b>\n"
                    + "\n".join(warn_trades)
                    + "\n\n🔔 SL & Exit-Strategie prüfen!"
                )
            else:
                msg += "\n✅ Keine offenen Gegenpositionen."
            telegram(msg)
            return jsonify({"status": "ok", "dir": new_dir, "label": label}), 200

        if signal_type == "HARSI_SL":
            # HARSI_SL: SL auf Harsi-Ausstiegslinie setzen (für offene Positionen)
            # Unterschied zu HARSI_EXIT: HARSI_EXIT = Entry-Signal (Alarm 3/3b)
            #                            HARSI_SL   = SL-Anpassung bei offenem Trade (Alarm 4/4b)
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

        if signal_type == "HARSI_EXIT":
            # ─────────────────────────────────────────────────────────────
            # HARSI_EXIT — Alarm 3/3b: HARSI verlässt Extremzone → Einstieg prüfen
            # Hier wird geprüft ob das 30-Min-Fenster nach dem letzten H2_SIGNAL
            # noch offen ist. Falls abgelaufen oder unbekannt → Warnung in Telegram.
            # ─────────────────────────────────────────────────────────────
            # v4.10: Makro-Extremzonen als DOMINUS-Premium-Info (kein Block).
            # Oversold + LONG bzw. Overbought + SHORT → 🎯 Premium-Hinweis;
            # Gegenrichtung → ⚠️ Warnung. In beiden Fällen wird der Entry-Fluss
            # regulär fortgesetzt — der User entscheidet.
            _xinfo = extreme_warn(direction)
            _xmsg  = format_extreme_info_msg(symbol, direction, _xinfo, "HARSI_EXIT")
            if _xmsg:
                telegram(_xmsg)

            sig_key = f"{symbol}_{direction}"
            h2_ts   = last_h2_signal_time.get(sig_key)
            icon    = dir_icon(direction)

            if h2_ts is None:
                # Kein H2_SIGNAL für dieses Symbol/Richtung bekannt
                warn_line = (
                    "⚠️ <b>Kein H2-Signal gespeichert</b> — Timing unbekannt.\n"
                    "Bitte manuell prüfen ob ein H2-Signal vorlag!"
                )
                timing_ok = False
                elapsed_min = None
            else:
                elapsed_sec = (datetime.utcnow() - h2_ts).total_seconds()
                elapsed_min = int(elapsed_sec // 60)
                if elapsed_sec > 1800:   # > 30 Minuten
                    warn_line = (
                        f"⛔ <b>30-Min-Fenster abgelaufen!</b>\n"
                        f"H2-Signal vor {elapsed_min} Min empfangen — "
                        f"Signal nicht mehr gültig laut DOMINUS-Regel."
                    )
                    timing_ok = False
                else:
                    remaining = 30 - elapsed_min
                    warn_line = f"✅ Fenster offen — noch ca. {remaining} Min gültig"
                    timing_ok = True

            log(f"  HARSI_EXIT {symbol} {direction} | {warn_line[:60]}")

            msg_parts = [
                f"{icon} <b>HARSI EXIT — {symbol} {direction.upper()}</b>",
                "━" * 12,
                f"Kurs: {entry}",
                "",
                warn_line,
                "",
            ]
            if timing_ok:
                _e_anker = "sec-alarm2" if direction == "long" else "sec-alarm2b"
                _e_lbl   = "Alarm 2 Long Entry" if direction == "long" else "Alarm 2b Short Entry"
                msg_parts += [
                    "📋 <b>Einstieg jetzt möglich:</b>",
                    f"/trade {symbol} {direction.upper()} [HEBEL] {entry:.5f} [SL]",
                    _doc_link(_e_anker, _e_lbl),
                ]
            else:
                msg_parts += [
                    "🚫 Kein Einstieg — Signal abgelaufen oder unbekannt.",
                    "Warte auf nächsten H2-Signal-Alarm.",
                ]

            telegram("\n".join(msg_parts))

            # Nach Eintritt: Zeitstempel löschen (verhindert Doppel-Warnungen)
            if sig_key in last_h2_signal_time:
                del last_h2_signal_time[sig_key]

            return jsonify({
                "status": "ok",
                "symbol": symbol,
                "timing_ok": timing_ok,
                "elapsed_min": elapsed_min,
            }), 200

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

        # v4.10: Makro-Extremzonen als DOMINUS-Premium-Info (kein Block).
        # Oversold + LONG bzw. Overbought + SHORT → 🎯 Premium-Hinweis;
        # Gegenrichtung → ⚠️ Warnung. Der H2-Signal-Fluss (inkl. 30-Min-Fenster)
        # läuft regulär weiter — der User entscheidet beim HARSI_EXIT.
        _xinfo = extreme_warn(direction)
        _xmsg  = format_extreme_info_msg(symbol, direction, _xinfo, "H2_SIGNAL")
        if _xmsg:
            telegram(_xmsg)

        # H2 Signal → H4 Puffer flushen dann sofort senden
        # 30-Min-Fenster starten: Zeitstempel für HARSI_EXIT-Prüfung speichern
        last_h2_signal_time[f"{symbol}_{direction}"] = datetime.utcnow()
        # Altes Einträge aufräumen (Memory-Leak verhindern): nur letzte 30 Min behalten
        _cutoff = datetime.utcnow() - timedelta(minutes=35)
        for _k in list(last_h2_signal_time.keys()):
            if last_h2_signal_time[_k] < _cutoff:
                del last_h2_signal_time[_k]

        # Makro-Kontext aus Webhook auslesen (vom DOM-ORC Plot-Werten)
        harsi_warn_val  = int(float(data.get("harsi_warn",  0) or 0))
        btc_t2_warn_val = int(float(data.get("btc_t2_warn", 0) or 0))
        premium_val     = int(float(data.get("premium",     0) or 0))

        # Makro-Richtung des Signals speichern (BTC/Total2 als letzten bekannten Stand)
        # btc_t2_warn=0 bedeutet: BTC & Total2 passten zur Signal-Richtung
        # WICHTIG: "recovering"/"recovering_short" NICHT überschreiben
        # → Zustand bleibt bis BTC_DIR 0-Kreuzungs-Alarm kommt (Alarm 5)
        if btc_t2_warn_val == 0:
            if btc_dir not in ("recovering", "recovering_short"):
                btc_dir = direction
            if t2_dir  not in ("recovering", "recovering_short"):
                t2_dir  = direction

        flush_h4_buffer()
        balance   = get_futures_balance()
        kelly     = kelly_recommendation(balance, WINRATE)
        links     = tv_chart_links(symbol)
        per_order = balance * 0.10 / 3
        icon      = dir_icon(direction)

        # Recovering-Prüfung: BTC/Total2 in Exit-Zone (Long: −10..0 / Short: 0..+10)
        # → nur Premium-Setups in der jeweiligen Richtung erlaubt
        _rec_long_btc  = (btc_dir == "recovering")
        _rec_long_t2   = (t2_dir  == "recovering")
        _rec_short_btc = (btc_dir == "recovering_short")
        _rec_short_t2  = (t2_dir  == "recovering_short")
        _rec_btc = _rec_long_btc or _rec_short_btc
        _rec_t2  = _rec_long_t2  or _rec_short_t2
        _recovering_long  = (_rec_long_btc  or _rec_long_t2)  and direction == "long"
        _recovering_short_trade = (_rec_short_btc or _rec_short_t2) and direction == "short"
        _recovering_active = _recovering_long or _recovering_short_trade

        # Checkliste: automatisch ausgefüllt wo möglich
        harsi_icon   = "✅" if harsi_warn_val  == 0 else "⚠️"
        if btc_t2_warn_val != 0:
            btc_t2_icon = "⚠️"
        elif _recovering_long:
            btc_t2_icon = "🟡"
        elif _recovering_short_trade:
            btc_t2_icon = "🟠"
        else:
            btc_t2_icon = "✅"
        premium_icon = "⭐" if premium_val     == 1 else "☐"
        harsi_txt    = "HARSI OK — Einstieg möglich"       if harsi_warn_val  == 0 else "HARSI Warnung — warten auf Alarm 3"
        if btc_t2_warn_val != 0:
            btc_t2_txt = "BTC/Total2 Abweichung — Trade prüfen!"
        elif _recovering_long:
            _rec_who = ("BTC + Total2" if (_rec_long_btc and _rec_long_t2)
                        else "BTC" if _rec_long_btc else "Total2")
            btc_t2_txt = f"{_rec_who} Recovery Long (−10→0) — nur Premium-Long!"
        elif _recovering_short_trade:
            _rec_who = ("BTC + Total2" if (_rec_short_btc and _rec_short_t2)
                        else "BTC" if _rec_short_btc else "Total2")
            btc_t2_txt = f"{_rec_who} Recovery Short (0→+10) — nur Premium-Short!"
        else:
            btc_t2_txt = "BTC + Total2 gleiche Richtung ✓"
        premium_txt  = "Premium-Setup (dunkelgrün/-rot)"   if premium_val     == 1 else "Kein Premium (höheres Risiko)"

        # Timer-Zeile abhängig von harsi_warn und recovering-Zustand
        if harsi_warn_val != 0:
            # Ablaufzeitpunkt: H2-Zeitstempel + 30 Min (oder "jetzt + 30 Min" als Fallback)
            _h2_ts      = last_h2_signal_time.get(f"{symbol}_{direction}", datetime.utcnow())
            _expiry_utc = _h2_ts + timedelta(minutes=30)
            _expiry_str = _expiry_utc.strftime("%d.%m.%Y %H:%M UTC")
            # Richtungsabhängiger HTML-Anker
            _anker  = "sec-alarm3" if direction == "long" else "sec-alarm3b"
            _anchor_label = "Alarm 3 Long HARSI Exit" if direction == "long" else "Alarm 3b Short HARSI Exit"
            if DOCS_URL:
                _docs_link = f'\n🔗 <a href="{DOCS_URL}#{_anker}">{_anchor_label} — Anleitung</a>'
            else:
                _docs_link = f"\n🔗 Anker: #{_anker} in Dominus_Alarm_Templates.html"
            timer_line = (
                f"⚠️ <b>HARSI in Extremzone — jetzt Alarm 3 erstellen!</b>\n"
                f"⏰ Signal läuft ab: <b>{_expiry_str}</b>"
                f"{_docs_link}"
            )
        elif _recovering_long and premium_val != 1:
            timer_line = (
                "🟡 <b>Long-Recovery — nur bei Premium-Long einsteigen!</b>\n"
                + _doc_link("sec-alarm2", "Alarm 2 Long Entry")
            )
        elif _recovering_short_trade and premium_val != 1:
            timer_line = (
                "🟠 <b>Short-Recovery — nur bei Premium-Short einsteigen!</b>\n"
                + _doc_link("sec-alarm2b", "Alarm 2b Short Entry")
            )
        else:
            _e_anker = "sec-alarm2" if direction == "long" else "sec-alarm2b"
            _e_lbl   = "Alarm 2 Long Entry" if direction == "long" else "Alarm 2b Short Entry"
            timer_line = (
                "⏱ <b>30 Min zum Einsteigen — jetzt /trade!</b>\n"
                + _doc_link(_e_anker, _e_lbl)
            )

        msg_parts = [
            f"{icon} <b>H2 Signal — {symbol} {direction.upper()}</b>",
            "━" * 12,
            f"Kurs: {entry}",
            "",
            "📋 <b>DOMINUS Checkliste:</b>",
            "☐ DOMINUS Impuls Extremzone erreicht?",
            "☐ H4 Trigger bestätigt?",
            f"{harsi_icon} {harsi_txt}",
            f"{btc_t2_icon} {btc_t2_txt}",
            f"{premium_icon} {premium_txt}",
            "",
            f"💰 {balance:.0f} USDT  |  Pro Order: {per_order:.0f} USDT",
            f"📊 Kelly: {kelly['kelly_pct']}%",
            "",
            timer_line,
            f"/trade {symbol} {direction.upper()} [HEBEL] {entry:.5f} [SL]",
            "",
            "📈 Charts:",
            f"H2 {symbol}: {links['coin_h2']}",
            f"H4 {symbol}: {links['coin_h4']}",
            f"BTC H2: {links['btc_h2']}",
            f"Total2: {links['total2']}",
        ]
        telegram("\n".join(msg_parts))

        # ── v4.8: HARSI in Extremzone → kopierbare Alarm-3/3b-Vorlage mitschicken ──
        # Spart dem User den Weg ins HTML-Handbuch: die Message-JSON, der Alarm-Name
        # und die Webhook-URL kommen hier schon fertig zum Copy-Paste in TV.
        # Die Fenster-Logik ist in build_alarm_harsi_exit() eingebaut und prüft
        # last_h2_signal_time[symbol_direction] (wurde oben in diesem Handler gesetzt).
        if harsi_warn_val == 1:
            try:
                telegram(build_alarm_harsi_exit(symbol, direction))
            except Exception as _alarm_err:
                log(f"[alarm-inline] Fehler: {_alarm_err}")
        return jsonify({"status": "ok", "symbol": symbol,
                        "direction": direction}), 200

    @app.route("/", methods=["GET"])
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "running", "version": "v4.35"}), 200

    port = int(os.environ.get("PORT", 8080))
    log(f"Webhook-Server gestartet auf Port {port}")
    log(f"Endpoint: /webhook?token={WEBHOOK_SECRET}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)



# ═══════════════════════════════════════════════════════════════
# STATE PERSISTENZ (speichern / laden)
# ═══════════════════════════════════════════════════════════════

STATE_FILE = os.environ.get("STATE_FILE", "/app/data/dominus_state.json")
# /app/data/ = Railway Volume (persistiert über Restarts und Redeploys).
# Falls kein Volume gemountet: Railway Variable STATE_FILE=/app/data/dominus_state.json setzen.
if not os.path.isdir("/app/data"):
    print("[WARN] /app/data/ nicht gefunden — Railway Volume korrekt gemountet?")
    print("[WARN] → Volume-Mount-Path: /app/data | STATE_FILE=/app/data/dominus_state.json")


def save_state():
    """Speichert den aktuellen In-Memory-State in eine JSON-Datei."""
    try:
        # last_h2_signal_time: datetime → ISO-String für JSON-Serialisierung
        h2_ts_serialized = {
            k: v.isoformat() for k, v in last_h2_signal_time.items()
        }
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
            "last_h2_signal_time":     h2_ts_serialized,
            "btc_dir":                 btc_dir,
            "t2_dir":                  t2_dir,
            # v4.9 Makro-Extremzonen persistieren (überlebt Railway-Restarts)
            "macro_extreme":           macro_extreme,
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
        # last_h2_signal_time: ISO-Strings → datetime (nur Einträge < 30 Min laden)
        now_utc = datetime.utcnow()
        for k, v in s.get("last_h2_signal_time", {}).items():
            try:
                ts = datetime.fromisoformat(v)
                if (now_utc - ts).total_seconds() < 1800:
                    last_h2_signal_time[k] = ts  # nur noch gültige Fenster laden
            except Exception:
                pass
        # Makro-Kontext laden (bleibt auch nach Neustart erhalten)
        global btc_dir, t2_dir
        btc_dir = s.get("btc_dir", "")
        t2_dir  = s.get("t2_dir",  "")
        # v4.9: Makro-Extremzonen laden — nur gültige (noch aktive) Cooldowns übernehmen
        _me_saved = s.get("macro_extreme", {})
        _now_ts   = time.time()
        for _mk in ("btc", "total2"):
            _saved = _me_saved.get(_mk, {})
            _until = float(_saved.get("until_ts", 0.0))
            if _until > _now_ts:
                macro_extreme[_mk]["state"]    = int(_saved.get("state", 0))
                macro_extreme[_mk]["until_ts"] = _until
            # sonst: abgelaufen oder leer → bleibt auf Defaults (0 / 0.0)
        _me_btc = macro_extreme["btc"]["state"]
        _me_t2  = macro_extreme["total2"]["state"]
        log(f"[load_state] State geladen: {len(last_known_avg)} Position(en) | "
            f"BTC={btc_dir or '?'} Total2={t2_dir or '?'} | "
            f"Makro-Extreme BTC={_me_btc} Total2={_me_t2}")
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

    # SL: erst direkt aus pos-Daten (all-position enthält stopLossPrice wenn
    # via place-pos-tpsl gesetzt), dann via Plan-Order-Endpoints
    sl_price = 0.0
    for _f in ("stopLossPrice", "stopLoss", "stopLossTriggerPrice", "slPrice", "sl"):
        _v = float(pos.get(_f, 0) or 0)
        if _v > 0:
            sl_price = _v
            log(f"  SL aus pos-Feld '{_f}': {_v}")
            break
    if sl_price == 0:
        sl_price = get_sl_price(symbol, direction)
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
    # Toleranz: max(0.05%, 2× Preise-Dezimalstellen) — skaliert mit Preis damit
    # Pennystocks (PEPE, SHIB) und grosse Kurse (BTC) beide korrekt erkannt werden.
    if sl_price > 0:
        sl_dist_pct_read = abs(avg - sl_price) / avg * 100
        _sl_entry_tol    = max(0.05, 2 * (10 ** -get_price_decimals(symbol)) / avg * 100)
        sl_is_entry      = sl_dist_pct_read <= _sl_entry_tol
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
        sl_dist_pct     = abs(avg - sl_price) / avg * 100
        _sl_entry_tol   = max(0.05, 2 * (10 ** -get_price_decimals(symbol)) / avg * 100)
        sl_is_entry     = sl_dist_pct <= _sl_entry_tol

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
        # Spezialfall: SL nicht lesbar (plan-order Endpoints defekt), aber
        # lokaler trailing_sl_level zeigt dass SL bereits gesetzt wurde.
        # → State vertrauen statt unnötig überschreiben.
        if trailing_sl_level.get(symbol, 0) >= 1:
            sl_is_entry = True
            log(f"  SL nicht lesbar via API, aber Trailing Level "
                f"{trailing_sl_level.get(symbol,0)} bekannt → als gesichert gewertet")
        elif tp1_done:
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
    # Erkennung ob TP2 / TP3 bereits ausgelöst wurden — zwei Methoden:
    #
    # Methode A — Fill-basiert (primär):
    #   detect_filled_tps() gleicht Close-Fills mit erwarteten TP-Preisen ab.
    #   Zuverlässig, aber auf 48h begrenzt.
    #
    # Methode B — Grössen-basiert (immer aktiv als Ergänzung):
    #   peak_size wird rekonstruiert aus: aktuelle Grösse + Summe aller Close-Fill-Grössen.
    #   Damit ist peak_size auch nach Neustart korrekt ohne gespeicherten State.
    #   TP2 ausgelöst: size < peak * 0.69  (35% geschlossen: TP1 15% + TP2 20% → 65% rest + 4% Puffer)
    #   TP3 ausgelöst: size < peak * 0.42  (60% geschlossen: TP1+TP2+TP3 = 60% → 40% rest + 2% Puffer)
    #
    # Kombination: Fill UND/ODER Grösse können TP2/TP3 bestätigen.
    # Grössen-Methode deaktiviert nur wenn peak_size = aktuelle size (kein Rückschluss möglich).

    fill_tp2_hit = any(t["roi"] == TP2_ROI for t in filled_tps)
    fill_tp3_hit = any(t["roi"] == TP3_ROI for t in filled_tps)

    # peak_size rekonstruieren: current_size + alle geschlossenen Fill-Mengen
    # Zuverlässiger als gespeicherter State allein — funktioniert auch nach Neustart
    _fill_closed_qty = sum(
        float(f.get("baseVolume", 0) or f.get("size", 0) or 0)
        for f in close_fills
    )
    _reconstructed_peak = size + _fill_closed_qty
    _state_peak         = trade_data.get(symbol, {}).get("peak_size", 0)
    # Grösstes der bekannten Werte nehmen — peak_size sinkt nie
    _ref = max(_state_peak, _reconstructed_peak, size)

    # Grössen-basierte Erkennung (unabhängig von Fill-Alter)
    # Nur aktiv wenn _ref echt grösser als aktuelle size (belastbare Info)
    _size_ratio    = size / _ref if _ref > size else 1.0
    # Schwellwerte: nach TP3 verbleiben 40% (TP_CLOSE_PCTS 15+20+25=60%), +2% Puffer → 0.42
    #               nach TP2 verbleiben 65% (TP_CLOSE_PCTS 15+20=35%),     +4% Puffer → 0.69
    size_tp3_hit   = _size_ratio < 0.42
    size_tp2_hit   = _size_ratio < 0.69

    # Kombination: Fill ODER Grösse bestätigen TP-Level
    tp3_confirmed = fill_tp3_hit or size_tp3_hit
    tp2_confirmed = fill_tp2_hit or size_tp2_hit

    log(f"  Phase 2b | fills={len(close_fills)} | "
        f"fill_tp2={fill_tp2_hit} fill_tp3={fill_tp3_hit} | "
        f"peak={_ref:.2f} cur={size:.2f} ratio={_size_ratio:.2f} | "
        f"size_tp2={size_tp2_hit} size_tp3={size_tp3_hit} | "
        f"tp2_confirmed={tp2_confirmed} tp3_confirmed={tp3_confirmed}")

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
            log(f"  {exp_tp_label} erkannt, "
                f"Trailing SL Level {exp_trail_lvl}: SL @ {sl_price} bereits korrekt ✓")
        elif current_trail < exp_trail_lvl:
            log(f"  {exp_tp_label} erkannt, "
                f"SL noch nicht auf Trailing-Niveau → nachziehen auf {exp_trail_sl:.5f}")
            set_sl_trailing(symbol, direction, exp_trail_sl,
                            level=exp_trail_lvl, cur_size=size)
            # Lokale sl_price-Variable nachführen, damit place_tp_orders den
            # richtigen (neuen) SL für TP4 mitschickt.
            # trailing_sl_level wird in set_sl_trailing() nur bei Erfolg gesetzt.
            if trailing_sl_level.get(symbol, 0) >= exp_trail_lvl:
                sl_price = exp_trail_sl
                log(f"  sl_price lokal aktualisiert → {exp_trail_sl:.5f} "
                    f"(für nachfolgende TP4-Platzierung)")
                # Kurze Pause — Bitget braucht einen Moment nach place-pos-tpsl
                # bevor plan-orders (place-tpsl-order) wieder akzeptiert werden.
                time.sleep(2)
    else:
        log(f"  Phase 2b: kein TP2/TP3 erkannt — SL bleibt unverändert")

    # Trade-Daten aktualisieren — peak_size NIE kleiner setzen als bekanntes Maximum
    _prev_td   = trade_data.get(symbol, {})
    _prev_peak = _prev_td.get("peak_size", 0)
    _prev_ts   = _prev_td.get("open_ts", int(time.time() * 1000))
    trade_data[symbol] = {
        "entry":        avg,
        "direction":    direction,
        "leverage":     leverage,
        "sl":           sl_price,
        "peak_size":    max(_prev_peak, _reconstructed_peak, size),   # nie reduzieren
        "open_ts":      _prev_ts,                                      # Öffnungszeit erhalten
        "tp_order_ids": _prev_td.get("tp_order_ids", []),
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
    # sl_at_entry nur zurücksetzen wenn trailing_sl_level NOCH NICHT gesetzt
    # (d.h. Script hat SL noch nie selbst gesetzt). Wenn trailing Level >= 1
    # bekannt ist, vertrauen wir dem lokalen State — SL wurde bereits korrekt
    # platziert, auch wenn er via API nicht lesbar ist.
    if tp1_done and trailing_sl_level.get(symbol, 0) == 0:
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

    log("DOMINUS Trade-Automatisierung v4.35 gestartet — mit finanzmathematischen Optimierungen")
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
            # Lock für den ganzen Check — verhindert Race mit Webhook-Thread
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

                        # ── Passive SL-Erkennung aus Position-Daten ──────────────
                        # Liest stopLossPrice direkt aus den Positionsdaten
                        # (funktioniert auch wenn plan-order Endpoints fehlschlagen).
                        # Aktualisiert sl_at_entry/trailing_sl_level wenn SL ≥ Entry.
                        if not sl_at_entry.get(sym, False):
                            _sl_pos = 0.0
                            for _sf in ("stopLossPrice", "stopLoss",
                                        "stopLossTriggerPrice", "slPrice", "sl"):
                                _sv = float(pos.get(_sf, 0) or 0)
                                if _sv > 0:
                                    _sl_pos = _sv
                                    break
                            if _sl_pos > 0:
                                _secured = ((_dir == "long"  and _sl_pos >= _avg) or
                                            (_dir == "short" and _sl_pos <= _avg))
                                if _secured:
                                    log(f"{sym}: SL {_sl_pos} aus Position-Daten "
                                        f"≥ Entry {_avg} → sl_at_entry = True")
                                    sl_at_entry[sym] = True
                                    trailing_sl_level[sym] = max(_trl, 1)
                                    save_state()

                        # ── TP-Kaskade: ALLE Levels in einem Tick prüfen ─────────
                        # Sequentielle ifs statt elif — damit TP1+TP2 (oder TP2+TP3)
                        # die gleichzeitig zwischen zwei Ticks auslösen in einem
                        # einzigen Check korrekt eskaliert werden.
                        _size_changed = False

                        if cur_size < _ref * 0.87 and not sl_at_entry.get(sym, False):
                            red = (_ref - cur_size) / _ref * 100
                            log(f"TP1 erkannt ({sym}): peak={_ref:.2f} → jetzt={cur_size:.2f} (-{red:.0f}%) → SL auf Entry")
                            trailing_sl_level[sym] = max(trailing_sl_level.get(sym, 0), 1)
                            set_sl_at_entry(sym, _dir, _avg, cur_size=cur_size)
                            _size_changed = True

                        if cur_size < _ref * 0.69 and trailing_sl_level.get(sym, 0) < 2:
                            red = (_ref - cur_size) / _ref * 100
                            tp1_price = calc_tp_price(_avg, TP1_ROI, _dir, _lev)
                            log(f"TP2 erkannt ({sym}): peak={_ref:.2f} → jetzt={cur_size:.2f} (-{red:.0f}%) → Trailing SL auf TP1 @ {tp1_price:.5f}")
                            set_sl_trailing(sym, _dir, tp1_price, level=2, cur_size=cur_size)
                            _size_changed = True

                        if cur_size < _ref * 0.42 and trailing_sl_level.get(sym, 0) < 3:
                            red = (_ref - cur_size) / _ref * 100
                            tp2_price = calc_tp_price(_avg, TP2_ROI, _dir, _lev)
                            log(f"TP3 erkannt ({sym}): peak={_ref:.2f} → jetzt={cur_size:.2f} (-{red:.0f}%) → Trailing SL auf TP2 @ {tp2_price:.5f}")
                            set_sl_trailing(sym, _dir, tp2_price, level=3, cur_size=cur_size)
                            _size_changed = True

                        if _size_changed or cur_size != kno_size:
                            last_known_size[sym] = cur_size

                # Geschlossene Positionen erkennen
                # (Position war bekannt, ist jetzt nicht mehr in get_all_positions)
                # Ursache: SL/TP4 automatisch ODER manuell geschlossen.
                # In beiden Fällen: TP-Orders + DCA-Orders stornieren → handle_position_closed.
                active_symbols = {p.get("symbol") for p in get_all_positions()}
                for sym in list(last_known_avg.keys()):
                    if sym not in active_symbols and last_known_avg.get(sym, 0) > 0:
                        # Trailing-Level bestimmt Ursache: 0 = vor TP1 → wahrscheinlich manuell
                        trl = trailing_sl_level.get(sym, 0)
                        if trl == 0:
                            reason = "Manuell geschlossen (vor TP1)"
                        elif trl >= 3:
                            reason = "TP3/TP4 oder Trailing SL ausgelöst"
                        else:
                            reason = "SL oder TP ausgelöst"
                        handle_position_closed(sym, reason)

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
