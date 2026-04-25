#!/usr/bin/env python3
"""
DOMINUS — Watchlist-Rebuild nach Bitget-First-Regel
═══════════════════════════════════════════════════════════════
Liest master_watchlist.txt, prüft jeden Coin gegen Bitgets Live-
Liste der USDT-Perpetual-Kontrakte und re-prefixt:

  ─ Symbol auf Bitget verfügbar  →  BITGET:<SYMBOL>.P
  ─ Symbol NICHT auf Bitget      →  BYBIT:<SYMBOL>.P
  ─ Symbol weder noch            →  in Report aufgelistet, NICHT in Output

Vorgehen
────────
1. Backup der aktuellen master_watchlist.txt → master_watchlist.bak_<timestamp>.txt
2. Bitget-Contracts-Liste fetchen (public endpoint, kein API-Key nötig)
3. (optional) Bybit-Contracts-Liste fetchen, um "auch nicht auf Bybit" zu
   markieren — falls Bybit-API auch erreichbar
4. Re-prefix jedes Symbol nach Bitget-First-Regel
5. Diff-Report in master_watchlist_audit_<timestamp>.txt schreiben
6. Neue master_watchlist.txt schreiben

Aufruf
──────
    python3 rebuild_watchlist_bitget_first.py
    python3 rebuild_watchlist_bitget_first.py --dry-run   # nur Report, keine Änderung
    python3 rebuild_watchlist_bitget_first.py --no-bybit  # Bybit-Verifikation überspringen
    python3 rebuild_watchlist_bitget_first.py --insecure  # macOS-SSL-Notfall-Fallback

Exit-Codes
──────────
    0  Erfolg
    1  Bitget-API nicht erreichbar
    2  master_watchlist.txt nicht gefunden
"""
import json
import os
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone

WATCHLIST_FILE = "master_watchlist.txt"
BITGET_URL = "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures"
BYBIT_URL  = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"

DRY_RUN  = "--dry-run"  in sys.argv
NO_BYBIT = "--no-bybit" in sys.argv
INSECURE = "--insecure" in sys.argv  # Notfall-Fallback bei macOS-SSL-Problem


# macOS-System-Python kommt oft ohne CA-Zertifikate — robusteren SSL-Context bauen:
def _build_ssl_context():
    if INSECURE:
        # Nur public market-data, keine API-Keys/sensiblen Daten — Notfall-OK
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        print("⚠ --insecure: SSL-Verifikation deaktiviert (nur Notfall)")
        return ctx
    # Bevorzugt: certifi-Bundle (üblich installiert via requests/pip)
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    # Fallback: System-Default
    return ssl.create_default_context()


SSL_CTX = _build_ssl_context()


def fetch_json(url: str, timeout: int = 20) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "dominus-watchlist-audit/1.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
        return json.loads(r.read())


def fetch_bitget_symbols() -> set:
    print(f"[1/4] Bitget USDT-Perp Kontrakte abrufen ...")
    data = fetch_json(BITGET_URL)
    if data.get("code") != "00000":
        raise RuntimeError(f"Bitget API Fehler: {data.get('msg', data)}")
    contracts = data.get("data", []) or []
    syms = {(c.get("symbol") or "").upper() for c in contracts if c.get("symbol")}
    print(f"      → {len(syms)} Bitget-Kontrakte verfügbar")
    return syms


def fetch_bybit_symbols() -> set:
    print(f"[2/4] Bybit USDT-Perp Kontrakte abrufen ...")
    cursor = ""
    out = set()
    while True:
        url = BYBIT_URL + (f"&cursor={cursor}" if cursor else "")
        data = fetch_json(url)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit API Fehler: {data.get('retMsg', data)}")
        result = data.get("result") or {}
        for it in (result.get("list") or []):
            s = (it.get("symbol") or "").upper()
            # Nur echte USDT-Perpetual (kein USDC, kein Inverse)
            if s.endswith("USDT") and (it.get("contractType") or "").lower() == "linearperpetual":
                out.add(s)
        cursor = result.get("nextPageCursor") or ""
        if not cursor:
            break
        time.sleep(0.2)  # rate-limit safety
    print(f"      → {len(out)} Bybit-Kontrakte verfügbar")
    return out


def parse_watchlist(path: str) -> list:
    """Gibt Liste von (current_prefix, base_symbol) zurück, in Reihenfolge des Files."""
    if not os.path.isfile(path):
        print(f"FEHLER: {path} nicht gefunden", file=sys.stderr)
        sys.exit(2)
    with open(path) as f:
        raw = f.read().strip()
    items = [t.strip() for t in raw.replace("\n", ",").split(",") if t.strip()]
    parsed = []
    for it in items:
        if ":" not in it:
            continue
        prefix, sym = it.split(":", 1)
        sym = sym.replace(".P", "").upper()
        if not sym.endswith("USDT"):
            sym = sym + "USDT"
        parsed.append((prefix.upper(), sym))
    return parsed


def main():
    bitget_syms = fetch_bitget_symbols()
    bybit_syms  = set() if NO_BYBIT else fetch_bybit_symbols()

    print(f"[3/4] master_watchlist.txt einlesen ...")
    items = parse_watchlist(WATCHLIST_FILE)
    print(f"      → {len(items)} Einträge gelesen")

    # Dedup auf Basis-Symbol — falls Doppel-Listing wie ARBUSDT, GRTUSDT,
    # CAKEUSDT, WIFUSDT vorkommt, behalten wir nur den ersten Eintrag.
    seen = set()
    unique_items = []
    duplicates = []
    for prefix, sym in items:
        if sym in seen:
            duplicates.append((prefix, sym))
        else:
            seen.add(sym)
            unique_items.append((prefix, sym))

    # Re-Prefix nach Bitget-First-Regel
    rebuilt = []
    moves_to_bitget = []
    moves_to_bybit  = []
    keeps_bitget    = []
    keeps_bybit     = []
    not_on_bitget_nor_bybit = []
    for old_prefix, sym in unique_items:
        if sym in bitget_syms:
            new_prefix = "BITGET"
            if old_prefix != "BITGET":
                moves_to_bitget.append(sym)
            else:
                keeps_bitget.append(sym)
            rebuilt.append((new_prefix, sym))
        else:
            # nicht auf Bitget → Bybit
            if (not NO_BYBIT) and (sym not in bybit_syms):
                not_on_bitget_nor_bybit.append(sym)
                continue  # rauswerfen — kein Exchange hat den Coin
            new_prefix = "BYBIT"
            if old_prefix != "BYBIT":
                moves_to_bybit.append(sym)
            else:
                keeps_bybit.append(sym)
            rebuilt.append((new_prefix, sym))

    # Write Audit-Report
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    audit_file = f"master_watchlist_audit_{ts}.txt"
    with open(audit_file, "w") as f:
        f.write(f"DOMINUS Watchlist-Audit — {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"{'═' * 70}\n\n")
        f.write(f"Eingelesen:                {len(items)} Einträge\n")
        f.write(f"Doppel-Listings entfernt:  {len(duplicates)}\n")
        f.write(f"In neuer Watchlist:        {len(rebuilt)}\n\n")
        f.write(f"BITGET (bleibt):           {len(keeps_bitget)}\n")
        f.write(f"BITGET (neu, war BYBIT):   {len(moves_to_bitget)}\n")
        f.write(f"BYBIT  (bleibt):           {len(keeps_bybit)}\n")
        f.write(f"BYBIT  (neu, war BITGET):  {len(moves_to_bybit)}\n")
        f.write(f"Nicht handelbar (raus):    {len(not_on_bitget_nor_bybit)}\n\n")

        if moves_to_bitget:
            f.write(f"┌─ Verschoben BYBIT → BITGET ({len(moves_to_bitget)}) ─\n")
            for s in moves_to_bitget:
                f.write(f"│  {s}\n")
            f.write(f"└─\n\n")

        if moves_to_bybit:
            f.write(f"┌─ Verschoben BITGET → BYBIT ({len(moves_to_bybit)}) ─\n")
            for s in moves_to_bybit:
                f.write(f"│  {s}\n")
            f.write(f"└─\n\n")

        if duplicates:
            f.write(f"┌─ Entfernte Doppel-Listings ({len(duplicates)}) ─\n")
            for prefix, sym in duplicates:
                f.write(f"│  {prefix}:{sym}.P (zweite Erwähnung)\n")
            f.write(f"└─\n\n")

        if not_on_bitget_nor_bybit:
            f.write(f"┌─ Auf KEINER Exchange — aus Watchlist entfernt ({len(not_on_bitget_nor_bybit)}) ─\n")
            for s in not_on_bitget_nor_bybit:
                f.write(f"│  {s} ⚠️ Pine-Symbol prüfen!\n")
            f.write(f"└─\n\n")

        f.write(f"┌─ Finale Watchlist ({len(rebuilt)}) ─\n")
        for prefix, sym in rebuilt:
            f.write(f"│  {prefix}:{sym}.P\n")
        f.write(f"└─\n")

    print(f"[4/4] Audit-Report: {audit_file}")
    print(f"      Bitget-Universum: {len(keeps_bitget) + len(moves_to_bitget)} Symbole")
    print(f"      Bybit-Universum:  {len(keeps_bybit) + len(moves_to_bybit)} Symbole")
    if duplicates:
        print(f"      ⚠ Doppel-Listings entfernt: {len(duplicates)} ({', '.join(s for _, s in duplicates)})")
    if not_on_bitget_nor_bybit:
        print(f"      ⚠ Coins ohne Exchange: {len(not_on_bitget_nor_bybit)} ({', '.join(not_on_bitget_nor_bybit)})")

    if DRY_RUN:
        print(f"\n--dry-run: master_watchlist.txt NICHT überschrieben.")
        return

    # Backup + Schreiben
    backup_file = f"master_watchlist.bak_{ts}.txt"
    if os.path.isfile(WATCHLIST_FILE):
        with open(WATCHLIST_FILE) as f_in, open(backup_file, "w") as f_out:
            f_out.write(f_in.read())
        print(f"      Backup:    {backup_file}")

    new_content = ",".join(f"{p}:{s}.P" for p, s in rebuilt)
    with open(WATCHLIST_FILE, "w") as f:
        f.write(new_content + "\n")
    print(f"      Geschrieben: {WATCHLIST_FILE} ({len(rebuilt)} Einträge)")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        print(f"NETZWERK-FEHLER: {e}", file=sys.stderr)
        print("Bitget/Bybit nicht erreichbar — VPN/Firewall prüfen.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"FEHLER: {e}", file=sys.stderr)
        sys.exit(1)
