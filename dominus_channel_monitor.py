#!/usr/bin/env python3
"""
DOMINUS Channel Monitor  v1.3  (2026-04-22)
════════════════════════════════════════════════════════════════
Überwacht den Dominus Telegram-Kanal auf neue Coins UND optional
parallel eine öffentliche TradingView-Watchlist.
Prüft Verfügbarkeit auf Bitget (bevorzugt) oder Bybit.
Sendet bei neuen Coins eine aktualisierte TradingView-Watchlist
als importierbare .txt-Datei per Telegram.

Changelog v1.3:
  • BUG-FIX: _state_lock wurde referenziert aber nie definiert
    → Live-Updates crashten lautlos bei jeder Coin-Liste im Kanal.
    Lock ist jetzt eine lokale asyncio.Lock() in main().
  • BUG-FIX: STATE_FILE lag im /tmp (wird von Railway bei jedem
    Redeploy gewischt). Jetzt über MONITOR_STATE_FILE env var auf
    Railway-Volume /app/data/. Auto-Migration vom alten /tmp-Pfad.
  • BUG-FIX: Initial-Bootstrap hat den TV-Abgleich ignoriert und
    alle historischen Channel-Coins in die Watchlist gekippt.
    Jetzt wird zuerst die TV-Baseline (via API + Seed-Fallback)
    geladen, dann der Channel gegen diese Baseline gefiltert.
  • NEU: Seed-File master_watchlist.txt (Format: TV-Export,
    komma- oder zeilen-separiert) als Offline-Fallback, wenn die
    TV-API nicht erreichbar ist.
  • NEU: Channel-History auf 2000 Msgs erweitert (konfigurierbar).
  • NEU: MONITOR_REBUILD=1 erzwingt einen sauberen Re-Bootstrap
    (State wird gewipt, TV-Baseline + Full-Channel neu aufgebaut).

Benötigte Railway-Variablen:
  TELEGRAM_API_ID         → my.telegram.org API ID
  TELEGRAM_API_HASH       → my.telegram.org API Hash
  DOMINUS_SESSION_STRING  → Telethon StringSession
  DOMINUS_CHANNEL_LINK    → Kanal-Einladungslink (t.me/+...)
  TELEGRAM_TOKEN          → Bot-Token
  TELEGRAM_CHAT_ID        → Deine Chat-ID

Optionale Railway-Variablen:
  TV_WATCHLIST_URL                → Öffentliche TV-Watchlist (Abgleich-Filter)
  MONITOR_STATE_FILE              → /app/data/dominus_watchlist.json (Default)
  MONITOR_SEED_FILE               → Pfad zur TV-Export-Baseline-Datei
                                    Default: master_watchlist.txt im Skript-Ordner
  MONITOR_CHANNEL_HISTORY_LIMIT   → Max. Nachrichten beim initialen Walk (Default 2000)
  MONITOR_REBUILD                 → "1" erzwingt kompletten Re-Bootstrap
════════════════════════════════════════════════════════════════
"""

import os, json, re, time, asyncio, logging, io, shutil
import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("dominus_monitor")

# ── Konfiguration ────────────────────────────────────────────────
API_ID         = int(os.environ["TELEGRAM_API_ID"])
API_HASH       = os.environ["TELEGRAM_API_HASH"]
SESSION_STRING = os.environ["DOMINUS_SESSION_STRING"]
CHANNEL_LINK   = os.environ["DOMINUS_CHANNEL_LINK"]
BOT_TOKEN      = os.environ["TELEGRAM_TOKEN"]
CHAT_ID        = os.environ["TELEGRAM_CHAT_ID"]

# TV-Abgleich: öffentliche TradingView-Watchlist als "Ist-Stand"-Referenz
# Default-Link zentral hinterlegt — per Railway-Variable TV_WATCHLIST_URL überschreibbar.
# Leerer String = TV-Abgleich deaktiviert (altes Verhalten).
TV_WATCHLIST_URL_DEFAULT = "https://de.tradingview.com/watchlists/328392936/"
TV_WATCHLIST_URL = os.environ.get("TV_WATCHLIST_URL", TV_WATCHLIST_URL_DEFAULT).strip()

# Cache-Dauer: TV-Watchlist ändert sich selten. 12 h → max. 2 Fetches pro Tag.
# Jede Telegram-Meldung innerhalb dieses Fensters nutzt den gecachten Stand.
TV_CACHE_TTL = 12 * 3600  # 43'200 Sekunden = 12 h

# State auf Railway-Volume (/app/data/) — überlebt Redeploys.
# Alter Pfad /tmp/dominus_watchlist.json wird beim Start automatisch migriert.
STATE_FILE       = os.environ.get("MONITOR_STATE_FILE", "/app/data/dominus_watchlist.json")
STATE_FILE_LEGACY = "/tmp/dominus_watchlist.json"

# Seed-Datei (TV-Export als Offline-Baseline). Wird verwendet, wenn die TV-API
# beim Bootstrap nicht erreichbar ist. Default: master_watchlist.txt im Skript-Ordner.
SEED_FILE = os.environ.get(
    "MONITOR_SEED_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_watchlist.txt"),
)

# Wie viele Nachrichten aus dem Kanal beim initialen Bootstrap durchgehen?
# 100 war zu wenig (alter Default) — der volle Kanal hat deutlich mehr.
CHANNEL_HISTORY_LIMIT = int(os.environ.get("MONITOR_CHANNEL_HISTORY_LIMIT", "2000"))

# Sauberer Re-Bootstrap: State wipen, TV-Baseline + Full-Channel neu aufbauen.
# Setze MONITOR_REBUILD=1 in Railway und redeploye — nach dem Rebuild wieder auf 0.
MONITOR_REBUILD = os.environ.get("MONITOR_REBUILD", "0").strip().lower() in ("1", "true", "yes", "on")

SYMBOL_CACHE_TTL = 6 * 3600   # Exchange-Listen alle 6h neu laden

# ── Coin-Erkennung ────────────────────────────────────────────────
COIN_RE = re.compile(r'^[A-Z]{2,8}$')
EXCLUDE: set[str] = {
    "UND","DIE","DER","DAS","EIN","IST","AUF","MIT","VON","ZUR","ZUM",
    "BEI","WIE","WAS","ALS","OFT","NUR","GUT","NEU","ALT","HAT","WAR",
    "ABER","ODER","WENN","DANN","NOCH","SEHR","MEHR","NACH","AUCH",
    "EINE","KEIN","HALT","BEIM","ALLE","HIER","EUCH","VIEL","BALD",
    "HEUTE","IMMER","NICHT","SCHON","DAMIT","DURCH","HALLO","LIEBE",
    "DANKE","BITTE","GRUSS","GUTEN","ABEND","MORGEN","NACHT","VIDEO",
    "TEXT","INFO","TIPP","REGEL","MARKT","KURS","SWING","BITCOIN",
    "THE","AND","FOR","NOT","YOU","ARE","ALL","NEW","OLD","HAS","HAD",
    "HIS","HER","ITS","OUR","OUT","NOW","GET","SET","USE","TWO","WAY",
    "MAY","DAY","MAN","BACK","FROM","THAT","THIS","WITH","HAVE","BEEN",
    "WILL","WHEN","THEY","VERY","EVEN","ALSO","SUCH","THAN","THEN",
    "INTO","OVER","JUST","LIKE","SOME","WHAT","KNOW","TIME","WELL",
    "SELL","LONG","SHORT","OPEN","STOP","LOSS","TAKE","PROFIT","HOLD",
    "USD","USDT","USDC","BUSD","EUR","BTC","ETH","BNB",
}


def looks_like_coin(word: str) -> bool:
    return bool(COIN_RE.match(word)) and word not in EXCLUDE


def extract_coins(text: str) -> list[str]:
    """Gibt Coin-Liste zurück wenn ≥3 reine Grossbuchstaben-Zeilen gefunden."""
    coins = []
    for line in text.splitlines():
        word = line.strip()
        if looks_like_coin(word):
            coins.append(word)
    return list(dict.fromkeys(coins)) if len(coins) >= 3 else []  # dedup, Reihenfolge erhalten


# ── TradingView Watchlist-API ────────────────────────────────────
def _extract_watchlist_id(url: str) -> str | None:
    """Extrahiert die numerische ID aus einer TV-Watchlist-URL."""
    m = re.search(r"/watchlists/(\d+)", url or "")
    return m.group(1) if m else None


_TV_API_CANDIDATES = [
    "https://www.tradingview.com/api/v1/symbols_list/colored_lists/{id}/",
    "https://de.tradingview.com/api/v1/symbols_list/colored_lists/{id}/",
    "https://www.tradingview.com/api/v1/symbols_list/custom/{id}/",
]
_TV_UA = "Mozilla/5.0 (compatible; DominusMonitor/1.0; +railway)"


def fetch_tv_watchlist_symbols(url: str) -> list[str]:
    """
    Holt die Symbole einer öffentlichen TV-Watchlist (JSON-API).
    Rückgabe z.B. ["BITGET:BTCUSDT.P", "BINANCE:ETHUSDT", ...]
    """
    wl_id = _extract_watchlist_id(url)
    if not wl_id:
        raise ValueError(f"Keine Watchlist-ID in URL: {url}")

    last_err: Exception | None = None
    for tpl in _TV_API_CANDIDATES:
        api = tpl.format(id=wl_id)
        try:
            r = requests.get(
                api,
                headers={"User-Agent": _TV_UA, "Accept": "application/json"},
                timeout=10,
            )
            if r.status_code != 200:
                last_err = Exception(f"{api} → HTTP {r.status_code}")
                continue
            data = r.json()
            syms = data.get("symbols")
            if isinstance(syms, list) and syms:
                return [s for s in syms if isinstance(s, str)]
            # manche Varianten: {"list": {"symbols": [...]}}
            alt = (data.get("list") or {}).get("symbols") or []
            if isinstance(alt, list) and alt:
                return [s for s in alt if isinstance(s, str)]
            last_err = Exception(f"{api} → JSON ohne 'symbols'")
        except Exception as e:
            last_err = e
            continue
    raise last_err or Exception("Kein TV-Endpoint lieferte Daten")


def tv_symbol_to_coin(sym: str) -> str | None:
    """
    Reduziert 'BINANCE:BTCUSDT', 'BITGET:ETHUSDT.P' auf Basiswährung 'BTC'/'ETH'.
    Nicht-Crypto-Symbole (NASDAQ:AAPL etc.) geben None zurück.
    """
    ticker = sym.split(":", 1)[1] if ":" in sym else sym
    ticker = ticker.upper().strip()
    # Perp-Suffixe abschneiden
    if ticker.endswith(".P"):
        ticker = ticker[:-2]
    if ticker.endswith("PERP"):
        ticker = ticker[:-4]
    # Quote-Währung abschneiden
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if ticker.endswith(quote) and len(ticker) > len(quote):
            base = ticker[: -len(quote)]
            if re.fullmatch(r"[A-Z0-9]{2,10}", base) and base not in EXCLUDE:
                return base
    return None


# ── Exchange-Symbol-Listen ────────────────────────────────────────
_bitget_syms:  set[str] = set()
_bybit_syms:   set[str] = set()
_syms_loaded_at: float  = 0.0


def _load_exchange_symbols() -> None:
    """Lädt verfügbare USDT-Perpetual-Symbole von Bitget und Bybit."""
    global _bitget_syms, _bybit_syms, _syms_loaded_at

    # Bitget: /api/v2/mix/market/contracts?productType=usdt-futures
    try:
        r = requests.get(
            "https://api.bitget.com/api/v2/mix/market/contracts",
            params={"productType": "usdt-futures"},
            timeout=10,
        )
        data = r.json().get("data", [])
        _bitget_syms = {d["symbol"].replace("USDT", "").upper() for d in data if "symbol" in d}
        log.info(f"Bitget: {len(_bitget_syms)} USDT-Perp Symbole geladen")
    except Exception as e:
        log.warning(f"Bitget Symbol-Liste fehlgeschlagen: {e}")

    # Bybit: /v5/market/instruments-info?category=linear&status=Trading
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "linear", "status": "Trading", "limit": "1000"},
            timeout=10,
        )
        items = r.json().get("result", {}).get("list", [])
        _bybit_syms = {
            d["symbol"].replace("USDT", "").upper()
            for d in items
            if d.get("symbol", "").endswith("USDT") and d.get("quoteCoin") == "USDT"
        }
        log.info(f"Bybit: {len(_bybit_syms)} USDT-Perp Symbole geladen")
    except Exception as e:
        log.warning(f"Bybit Symbol-Liste fehlgeschlagen: {e}")

    _syms_loaded_at = time.time()


def _ensure_symbols_fresh() -> None:
    if time.time() - _syms_loaded_at > SYMBOL_CACHE_TTL:
        _load_exchange_symbols()


def resolve_tv_symbol(coin: str) -> tuple[str, str] | tuple[None, None]:
    """
    Gibt (TradingView-Symbol, Exchange) zurück.
    Priorität: Bitget → Bybit → None
    """
    _ensure_symbols_fresh()
    if coin in _bitget_syms:
        return f"BITGET:{coin}USDT.P", "Bitget"
    if coin in _bybit_syms:
        return f"BYBIT:{coin}USDT.P", "Bybit"
    return None, None


# ── State-Persistenz ─────────────────────────────────────────────
def _ensure_state_dir() -> None:
    """Stellt sicher, dass der Zielordner für STATE_FILE existiert."""
    d = os.path.dirname(os.path.abspath(STATE_FILE))
    if d and not os.path.exists(d):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            log.warning(f"State-Verzeichnis konnte nicht erstellt werden ({d}): {e}")


def _migrate_legacy_state() -> None:
    """Kopiert einen alten /tmp-State einmalig auf den neuen /app/data-Pfad."""
    if os.path.exists(STATE_FILE):
        return  # neuer Pfad bereits vorhanden — nichts zu tun
    if not os.path.exists(STATE_FILE_LEGACY):
        return  # kein alter State
    try:
        _ensure_state_dir()
        shutil.copy2(STATE_FILE_LEGACY, STATE_FILE)
        log.info(f"Legacy-State migriert: {STATE_FILE_LEGACY} → {STATE_FILE}")
    except Exception as e:
        log.warning(f"State-Migration fehlgeschlagen: {e}")


def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"known_coins": [], "watchlist": [], "skipped": []}


def save_state(state: dict) -> None:
    state["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _ensure_state_dir()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Seed-File (TV-Export als Offline-Baseline) ────────────────────
def load_seed_symbols() -> list[str]:
    """
    Liest die TV-Export-Datei und gibt die darin enthaltenen TV-Symbole
    (z.B. 'BITGET:BTCUSDT.P') als Liste zurück.
    Unterstützt sowohl komma-separiertes als auch zeilen-separiertes Format
    (TradingView exportiert standardmässig komma-separiert auf einer Zeile).
    Leere Liste → Datei nicht gefunden oder leer.
    """
    if not os.path.exists(SEED_FILE):
        return []
    try:
        with open(SEED_FILE, encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        log.warning(f"Seed-File nicht lesbar ({SEED_FILE}): {e}")
        return []
    # Komma oder Zeilenumbruch als Trenner akzeptieren
    parts = re.split(r"[,\n\r]+", content)
    return [p.strip() for p in parts if p.strip()]


def seed_tv_coins() -> set[str]:
    """Gibt die Basiswährungs-Menge aus der Seed-Datei zurück."""
    syms = load_seed_symbols()
    coins: set[str] = set()
    for s in syms:
        base = tv_symbol_to_coin(s)
        if base:
            coins.add(base)
    return coins


def get_tv_baseline_coins() -> set[str]:
    """
    Liefert die aktuelle TV-Baseline für den Initial-Bootstrap.
    Priorität:
      1) Live TV-API (via get_tv_watchlist_coins)
      2) Seed-File (master_watchlist.txt) als Offline-Fallback
      3) Leere Menge (Bootstrap läuft ohne Baseline)
    """
    try:
        live = get_tv_watchlist_coins()
        if live:
            log.info(f"[Baseline] TV-API: {len(live)} Coins")
            return live
    except Exception as e:
        log.warning(f"[Baseline] TV-API fehlgeschlagen: {e}")
    seed = seed_tv_coins()
    if seed:
        log.info(f"[Baseline] Seed-File {SEED_FILE}: {len(seed)} Coins (Offline-Fallback)")
    else:
        log.warning(f"[Baseline] Kein Seed-File gefunden ({SEED_FILE}) — Bootstrap ohne Baseline")
    return seed


# ── TradingView Watchlist .txt ────────────────────────────────────
def build_watchlist_txt(watchlist: list[str]) -> str:
    """Erzeugt den Inhalt der TradingView-importierbaren .txt-Datei."""
    return "\n".join(sorted(watchlist)) + "\n"


def send_watchlist_file(state: dict, new_coins: list[str], skipped: list[str]) -> None:
    """Sendet Zusammenfassung + .txt-Datei per Telegram."""
    watchlist = state["watchlist"]
    txt_content = build_watchlist_txt(watchlist)
    filename = f"dominus_watchlist_{time.strftime('%Y%m%d_%H%M')}.txt"

    # Zusammenfassung
    lines = [f"📋 <b>Watchlist aktualisiert — {len(new_coins)} neue Coin(s)</b>", ""]
    for sym, exch in [(s, "Bitget" if "BITGET" in s else "Bybit") for s in
                      sorted(state["watchlist"]) if any(c in s for c in new_coins)]:
        coin = sym.split(":")[1].replace("USDT.P", "")
        exch_icon = "🟠" if "BITGET" in sym else "🔵"
        lines.append(f"{exch_icon} <b>{coin}</b> → {sym}")
    if skipped:
        lines += ["", f"⚠️ Nicht gefunden (weder Bitget noch Bybit): {', '.join(skipped)}"]
    lines += ["", f"Gesamte Watchlist: <b>{len(watchlist)} Symbole</b>"]
    lines += ["↓ Datei importieren: TradingView → Watchlist → ⋮ → Import"]

    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID, "text": "\n".join(lines),
                "parse_mode": "HTML", "disable_web_page_preview": True,
            },
            timeout=10,
        )
        # .txt-Datei senden
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
            data={"chat_id": CHAT_ID},
            files={"document": (filename, io.BytesIO(txt_content.encode()), "text/plain")},
            timeout=15,
        )
    except Exception as e:
        log.warning(f"Telegram-Send fehlgeschlagen: {e}")


def send_text(text: str) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram-Send fehlgeschlagen: {e}")


# ── Neue Coins verarbeiten ────────────────────────────────────────
def process_new_coins(raw_coins: list[str], state: dict) -> tuple[list[str], list[str]]:
    """
    Löst neue Coins gegen Bitget/Bybit auf.
    Gibt (neue TV-Symbole, übersprungene Coins) zurück.
    """
    known   = set(state.get("known_coins", []))
    wl_set  = set(state.get("watchlist", []))
    new_syms, skipped = [], []

    for coin in raw_coins:
        if coin in known:
            continue
        tv_sym, exch = resolve_tv_symbol(coin)
        if tv_sym:
            if tv_sym not in wl_set:
                new_syms.append(tv_sym)
                wl_set.add(tv_sym)
                log.info(f"  + {coin} → {tv_sym} ({exch})")
        else:
            skipped.append(coin)
            log.info(f"  ? {coin} → weder Bitget noch Bybit gefunden")
        known.add(coin)

    state["known_coins"] = sorted(known)
    state["watchlist"]   = sorted(wl_set)
    state["skipped"]     = sorted(set(state.get("skipped", [])) | set(skipped))
    return new_syms, skipped


# ── TradingView On-Demand-Fetch (Kurzcache, kein Poll-Loop) ───────
_tv_coins_cache: set[str] = set()
_tv_cache_at: float       = 0.0


def get_tv_watchlist_coins() -> set[str]:
    """
    Gibt die Basiswährungs-Namen (z.B. {"BTC","ETH",…}) der aktuellen
    TV-Watchlist zurück. Ergebnis wird TV_CACHE_TTL Sekunden (12 h) zwischen-
    gespeichert — pro Tag fallen also maximal 2 HTTP-Requests an TV an.
    Bei Fehler → letzter bekannter Stand (oder leere Menge, wenn nie erfolgreich).
    """
    global _tv_coins_cache, _tv_cache_at

    if not TV_WATCHLIST_URL:
        return set()

    if time.time() - _tv_cache_at < TV_CACHE_TTL and _tv_coins_cache:
        return _tv_coins_cache

    try:
        tv_syms = fetch_tv_watchlist_symbols(TV_WATCHLIST_URL)
    except Exception as e:
        log.warning(f"[TV] Abgleich-Fetch fehlgeschlagen: {e}")
        return _tv_coins_cache  # bei Fehler: letzter bekannter Stand (ggf. leer)

    coins: set[str] = set()
    for s in tv_syms:
        base = tv_symbol_to_coin(s)
        if base:
            coins.add(base)

    _tv_coins_cache = coins
    _tv_cache_at    = time.time()
    log.info(f"[TV] Watchlist aktualisiert: {len(coins)} Coins geladen")
    return coins


# ── Bootstrap ─────────────────────────────────────────────────────
async def bootstrap_state(client, channel, state: dict) -> dict:
    """
    Baut den State aus TV-Baseline + voller Channel-History auf.
    Wird ausgeführt bei:
      - leerem State (erster Start)
      - MONITOR_REBUILD=1 (erzwungener Rebuild)

    Reihenfolge:
      1) TV-Baseline (Live-API → Seed-Fallback) als "bereits bekannt" markieren
         + alle in Exchange verfügbaren Coins in state["watchlist"] übernehmen.
      2) Volle Channel-History durchlaufen (CHANNEL_HISTORY_LIMIT).
      3) Für Channel-Coins NICHT in TV-Baseline → auflösen + zur Watchlist.
      4) Persistieren.
    """
    log.info("=== Bootstrap gestartet ===")

    # 1) TV-Baseline laden
    baseline_coins: set[str] = await asyncio.to_thread(get_tv_baseline_coins)
    baseline_known: set[str] = set()
    baseline_resolved: list[str] = []

    for coin in sorted(baseline_coins):
        baseline_known.add(coin)
        tv_sym, _ = resolve_tv_symbol(coin)
        if tv_sym:
            baseline_resolved.append(tv_sym)

    state["known_coins"] = sorted(baseline_known)
    state["watchlist"]   = sorted(set(baseline_resolved))
    state["skipped"]     = []  # wird gleich durch Channel-Walk ergänzt
    log.info(f"[Bootstrap] TV-Baseline: {len(baseline_known)} Coins "
             f"({len(baseline_resolved)} auf Bitget/Bybit verfügbar)")

    # 2) Volle Channel-History durchlaufen
    log.info(f"[Bootstrap] Lese letzte {CHANNEL_HISTORY_LIMIT} Nachrichten aus dem Kanal...")
    channel_raw: list[str] = []
    msg_count = 0
    coin_msg_count = 0
    async for msg in client.iter_messages(channel, limit=CHANNEL_HISTORY_LIMIT):
        msg_count += 1
        if not msg.text:
            continue
        coins = extract_coins(msg.text)
        if coins:
            coin_msg_count += 1
            for c in coins:
                if c not in channel_raw:
                    channel_raw.append(c)
    log.info(f"[Bootstrap] {msg_count} Nachrichten gelesen, "
             f"{coin_msg_count} Coin-Listen, {len(channel_raw)} Roh-Coins extrahiert")

    # 3) Channel-Coins verarbeiten (filter gegen Baseline passiert implizit via known_coins)
    new_syms, skipped = process_new_coins(channel_raw, state)

    # 4) Persistieren
    save_state(state)

    log.info(f"=== Bootstrap fertig: {len(state['watchlist'])} Watchlist-Symbole, "
             f"{len(new_syms)} neue aus Channel, {len(skipped)} nicht gefunden ===")

    # Telegram-Report
    lines = [
        "✅ <b>Dominus-Monitor Bootstrap abgeschlossen</b>",
        "",
        f"📊 TV-Baseline: <b>{len(baseline_known)}</b> Coins",
        f"📋 Watchlist gesamt: <b>{len(state['watchlist'])}</b> Symbole",
        f"➕ Aus Kanal-History neu: <b>{len(new_syms)}</b>",
    ]
    if skipped:
        lines.append(f"⚠️ Nicht auf Bitget/Bybit: {', '.join(skipped[:15])}"
                     + (f" …(+{len(skipped)-15})" if len(skipped) > 15 else ""))
    lines += ["", "Ab jetzt werden neue Coins automatisch erkannt."]
    send_text("\n".join(lines))

    return state


# ── Hauptprogramm ────────────────────────────────────────────────
async def main() -> None:
    log.info("════ Dominus Channel Monitor v1.3 ════")
    log.info(f"STATE_FILE = {STATE_FILE}")
    log.info(f"SEED_FILE  = {SEED_FILE} (exists={os.path.exists(SEED_FILE)})")
    log.info(f"CHANNEL_HISTORY_LIMIT = {CHANNEL_HISTORY_LIMIT}")
    log.info(f"MONITOR_REBUILD = {MONITOR_REBUILD}")

    # State-Migration & Directory-Setup
    _ensure_state_dir()
    _migrate_legacy_state()

    # Exchange-Listen beim Start laden
    _load_exchange_symbols()

    # Thread-safe State-Lock für concurrent Channel-Events
    state_lock = asyncio.Lock()

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    log.info("Telethon-Client gestartet")

    # Kanal beitreten / Entity holen
    try:
        channel = await client.get_entity(CHANNEL_LINK)
    except Exception:
        log.info("Kanal beitreten...")
        from telethon.tl.functions.channels import JoinChannelRequest
        await client(JoinChannelRequest(CHANNEL_LINK))
        channel = await client.get_entity(CHANNEL_LINK)
    log.info(f"Kanal verbunden: {getattr(channel, 'title', CHANNEL_LINK)}")

    # TV-Abgleich-Setup-Check (damit Config-Fehler früh im Log auftauchen)
    if TV_WATCHLIST_URL:
        if _extract_watchlist_id(TV_WATCHLIST_URL):
            log.info(f"TV-Abgleich aktiv: {TV_WATCHLIST_URL}")
        else:
            log.warning(f"TV_WATCHLIST_URL gesetzt, aber keine ID extrahierbar: {TV_WATCHLIST_URL}")
    else:
        log.info("TV_WATCHLIST_URL nicht gesetzt → kein TV-Abgleich")

    # State laden + ggf. Bootstrap erzwingen
    state = load_state()
    need_bootstrap = MONITOR_REBUILD or not state.get("known_coins")

    if need_bootstrap:
        if MONITOR_REBUILD:
            log.info("MONITOR_REBUILD=1 → State wird komplett neu aufgebaut")
            state = {"known_coins": [], "watchlist": [], "skipped": []}
        state = await bootstrap_state(client, channel, state)
    else:
        log.info(f"State geladen: {len(state['watchlist'])} Symbole bekannt")
        send_text(
            f"▶️ <b>Dominus-Monitor neugestartet</b>\n"
            f"Watchlist: {len(state['watchlist'])} Symbole — überwache auf Neuzugänge."
        )

    # Neue Nachrichten überwachen
    @client.on(events.NewMessage(chats=channel))
    async def on_new_message(event):
        text = event.message.text or ""
        raw_coins = extract_coins(text)

        if not raw_coins:
            log.info(f"Infonachricht ({len(text)} Zeichen) — ignoriert")
            return

        log.info(f"Coin-Liste erkannt: {raw_coins}")

        # TV-Abgleich: Coins, die schon in der öffentlichen TV-Watchlist sind,
        # nicht erneut melden.
        tv_coins: set[str] = set()
        already_in_tv: list[str] = []
        if TV_WATCHLIST_URL:
            tv_coins = await asyncio.to_thread(get_tv_watchlist_coins)
            if tv_coins:
                filtered: list[str] = []
                for c in raw_coins:
                    if c in tv_coins:
                        already_in_tv.append(c)
                    else:
                        filtered.append(c)
                if already_in_tv:
                    log.info(f"Bereits in TV-Watchlist (übersprungen): {already_in_tv}")
                raw_coins = filtered

        if not raw_coins:
            log.info("Alle Coins aus dieser Meldung sind bereits in der TV-Watchlist — kein Update")
            return

        async with state_lock:
            st = load_state()
            new_syms, skipped = process_new_coins(raw_coins, st)
            if new_syms:
                save_state(st)

        if new_syms:
            await asyncio.to_thread(send_watchlist_file, st, new_syms, skipped)
        elif skipped:
            log.info(f"Keine neuen Symbole, aber {len(skipped)} nicht gefunden: {skipped}")
        else:
            log.info("Alle Coins bereits bekannt — kein Update")

    log.info("Monitoring aktiv — warte auf neue Nachrichten...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
