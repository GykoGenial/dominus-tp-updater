#!/usr/bin/env python3
"""
DOMINUS Channel Monitor
════════════════════════════════════════════════════════════════
Überwacht den Dominus Telegram-Kanal auf neue Coins.
Prüft Verfügbarkeit auf Bitget (bevorzugt) oder Bybit.
Sendet bei neuen Coins eine aktualisierte TradingView-Watchlist
als importierbare .txt-Datei per Telegram.

Benötigte Railway-Variablen:
  TELEGRAM_API_ID         → my.telegram.org API ID
  TELEGRAM_API_HASH       → my.telegram.org API Hash
  DOMINUS_SESSION_STRING  → Telethon StringSession
  DOMINUS_CHANNEL_LINK    → Kanal-Einladungslink (t.me/+...)
  TELEGRAM_TOKEN          → Bot-Token
  TELEGRAM_CHAT_ID        → Deine Chat-ID
════════════════════════════════════════════════════════════════
"""

import os, json, re, time, asyncio, logging, io
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

STATE_FILE     = "/tmp/dominus_watchlist.json"
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
def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"known_coins": [], "watchlist": [], "skipped": []}


def save_state(state: dict) -> None:
    state["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


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


# ── Hauptprogramm ────────────────────────────────────────────────
async def main() -> None:
    # Exchange-Listen beim Start laden
    _load_exchange_symbols()

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

    # Initiale Watchlist aus Channel-Historie aufbauen
    state = load_state()
    if not state.get("known_coins"):
        log.info("Erstelle initiale Watchlist aus letzten 100 Nachrichten...")
        all_raw: list[str] = []
        async for msg in client.iter_messages(channel, limit=100):
            if msg.text:
                coins = extract_coins(msg.text)
                for c in coins:
                    if c not in all_raw:
                        all_raw.append(c)

        process_new_coins(all_raw, state)
        save_state(state)
        log.info(f"Initiale Watchlist: {len(state['watchlist'])} Symbole "
                 f"({len(state['skipped'])} nicht gefunden)")

        skip_note = ""
        if state["skipped"]:
            skip_note = f"\n⚠️ Nicht gefunden: {', '.join(state['skipped'])}"
        send_text(
            f"✅ <b>Dominus-Monitor gestartet</b>\n"
            f"Watchlist aufgebaut: <b>{len(state['watchlist'])} Symbole</b>"
            f" aus {len(state['known_coins'])} Kanal-Coins{skip_note}\n\n"
            f"Ab jetzt werden neue Coins automatisch erkannt und die\n"
            f"Watchlist-Datei automatisch zugestellt."
        )
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
        st = load_state()
        new_syms, skipped = process_new_coins(raw_coins, st)

        if new_syms:
            save_state(st)
            send_watchlist_file(st, new_syms, skipped)
        elif skipped:
            log.info(f"Keine neuen Symbole, aber {len(skipped)} nicht gefunden: {skipped}")
        else:
            log.info("Alle Coins bereits bekannt — kein Update")

    log.info("Monitoring aktiv — warte auf neue Nachrichten...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
