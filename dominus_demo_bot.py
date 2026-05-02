"""
DOMINUS Demo-Bot v4.35.1 — PAPER TRADING
════════════════════════════════════
Vollautomatisches Paper Trading auf Bitget Demo-Account.
Kein echtes Kapital — identische Logik wie Live-Script.

Unterschied zum Live-Script:
  - GLEICHE API-Keys wie Live (API_KEY / SECRET_KEY / PASSPHRASE)
  - paperId="1" Header aktiviert Bitget Demo-Account
  - Separater Telegram-Kanal (TELEGRAM_CHAT_ID = Demo-Kanal ID)
  - Performance-Tracking in demo_trades.json

Railway Setup:
  1. Neuer Service → Start Command: python dominus_demo_bot.py
  2. Variables: gleiche API-Keys wie Live
  3. TELEGRAM_CHAT_ID = Chat-ID des Demo-Telegram-Kanals
  4. WEBHOOK_SECRET = eigenes Token z.B. dominus-demo-2026
════════════════════════════════════
DOMINUS Trade-Automatisierung v4.35.1 — ursprüngliches Script
══════════════════════════════════════════════════════════════
Vollautomatisches Setup nach DOMINUS-Strategie (Handbuch März 2026)
Finanzmathematische Optimierungen:
  ① Hebel-Empfehlung  — Hebel = 25 / SL-Abstand%
  ② R:R-Filter        — kein Trade unter 1.5 R:R
  ③ Kelly-Kriterium   — optimale Positionsgrösse
  ④ Asymm. TPs        — 15/20/25/40% statt 25/25/25/25%
  ⑤ Telegram Polling  — /berechnen /trade /status /hilfe /alarm
  ⑥ Sling-SL Trailing — Swing-Pivot-basierter SL (nur protektiv)
  ⑦ Exposure-Cap 25%  — max. Gesamt-Einsatz inkl. Hebel pro Trade
  ⑧ Entry-Queue       — H2_SIGNAL + HARSI_EXIT gemeinsam als Rangliste (v4.22)
  ⑨ Queue-Tracking    — entry_queue_log.csv + R-Multiple-Outcome (v4.20)
  ⑩ Klick-UX          — Button-Driven Rangliste mit adaptivem Keyboard (v4.25)
  ⑪ One-Click-Exec    — 🚀 Trade jetzt Button mit Two-Tap-Confirm (v4.28)
  ⑫ Inline-Berechnung — 🚀 1. Tap zeigt volle /trade-Vorschau (v4.35.1)

Changelog v4.35.1 — UX: 🚀 1. Tap zeigt volle /trade-Berechnung:
  U1: Erster Tap auf 🚀 Trade jetzt postet zusätzlich die komplette
      cmd_trade()-Vorschau als Chat-Message (Margin/Kelly/DCAs/TPs/R:R/
      Warnungen). Vor v4.35.1 zeigte das Confirm-Alert nur Symbol +
      Hebel + Entry + SL — Felix musste vorher 🎯 Berechnen separat
      tippen, um Margin und Kelly-Werte zu sehen. Jetzt ein Schritt
      weniger im Workflow. cmd_trade() ist in try/except gekapselt
      damit ein Vorschau-Fehler den Trade-Flow nie blockiert.
      Confirm-Alert vereinfacht auf "⚠️ Berechnung oben prüfen.
      Nochmals 🚀 zur Bestätigung — läuft in Ns ab." Two-Tap-Schutz
      bleibt unverändert (gleicher payload_sig, gleiche TTL).
      🎯 Berechnen-Button bleibt parallel als Backup für reine
      Vorschau ohne Trade-Absicht erhalten.
  U2: iOS-Optimierung Telegram-Bubble-Breite. Trennlinie in der
      DOMINUS-Entry-Rangliste (Zeile 4673) von 38 auf 24 Em-Dashes
      gekürzt — vorher überspannte sie auf iPhone-Telegram die
      sichtbare Bubble. Inline-Button-Divider "Weitere Signale"
      verkürzt von "━━━━━ Weitere Signale ━━━━━" (27 Zeichen) auf
      "━ Weitere Signale ━" (19 Zeichen) damit der Button auf
      iPhone-Breite nicht überstreckt wirkt. Bullet-Padding (Score-
      Spalte rechtsbündig) bewusst unverändert — würde sonst die
      Punkte-Ausrichtung brechen, die in v4.25 explizit gewollt war.
  U3: Long/Short-Emoji auf den Coin-Buttons in der Rangliste.
      _coin_button_label() prepended jetzt 🟢 (long) bzw. 🔴 (short)
      vor das Stern/Pfeil-Prefix, sodass Felix beim Scrollen sofort
      die Trade-Richtung sieht — ohne den Button erst tippen oder
      die Karte öffnen zu müssen. Fallback ⚪ falls direction leer.
      Format z.B. "🟢⭐1 AVAX 82" / "🔴 2 OP 78" / "🟢▾ AVAX 82".

Changelog v4.35 — DCA Auto-Void Bug-Fix + Lärm-Reduktion + Forensik:
  D1: BUG-FIX _void_passed_dcas() — falsche Bitget-API.
      Vor v4.35 fragte die Funktion /api/v2/mix/order/orders-plan-pending
      ab (TP/SL/Trigger-Plan-Orders). DCAs sind aber reguläre LIMIT-Orders
      und liegen unter /api/v2/mix/order/orders-pending. Konsequenz:
      Der Aufruf nach Sling-SL fand nichts und DCA-Orders unterhalb des
      neuen (long) bzw. oberhalb des neuen (short) SL blieben aktiv.
      Beobachtet bei QNTUSDT 2026-04-25 08:01 UTC: Sling-SL wurde gesetzt,
      die zwei DCA-Limits unterhalb des Long-SL blieben aber pending.
      Jetzt analog zu cancel_open_dca_orders implementiert: filter
      side ∈ {buy, sell}, tradeSide=="open", orderType=="limit"; Storno
      via /api/v2/mix/order/cancel-order statt cancel-plan-order.
  D2: Watchlist-Master Lärm-Reduktion. Pine sendet SLING_SL/HARSI_SL für
      jedes Watchlist-Symbol; ohne offene Position landeten Drops mit
      Per-Drop-Log im Railway-Stream (24h-Log: 240 SLING + 43 HARSI =
      12 Lärm-Zeilen/h). Neuer Helper _track_watchlist_drop() zählt
      stumm und loggt nur eine Summary alle WATCHLIST_DROP_SUMMARY_EVERY_N
      Drops (default 50, ≈ alle 4h bei aktueller Lautstärke). Env-Flag
      WATCHLIST_DROP_VERBOSE=1 stellt das alte Per-Drop-Log wieder her,
      falls für eine konkrete Forensik benötigt. Vier Call-Sites ersetzt:
      generic Auto-Direction-Drop, HARSI_SL kein-Preis, HARSI_SL keine-
      Position, SLING_SL kein-Pivot, SLING_SL keine-matching-Position.
  D3: Auto-SL -25% Fallback Forensik-Dump. Im 24h-Log feuerte das "Kein
      SL gefunden — Auto-SL bei -25%"-Fallback 2× (ENSUSDT, SOLUSDT)
      ohne Diagnose-Möglichkeit. Vor dem Auto-SL-Setzen wird jetzt ein
      🔬-Forensik-Block geloggt: get_sl_price-Result, Anzahl + planTypes
      der pending Plan-Orders, trade_data[symbol].sl-Snapshot, direction,
      leverage. Beim nächsten Auftritt direkt diagnostizierbar ob Webhook,
      Plan-Order-Persistenz oder eine andere Race-Condition die Ursache war.
  D4: Min-Qty Edge-Case bei place_tp_orders_after_dca + Reconciliation.
      Bisher: Qty<4 → kompletter Bail-Out ("manuell überwachen"), kein
      einziger TP gesetzt. SOLUSDT 2026-04-25: Nachkauf reduzierte die
      Position auf 3.7 Kontrakte → keine TPs, nur SL als Exit. v4.35:
      place_tp_orders() hat bereits eingebaute Carry-Forward-Logik (Sub-
      min-Qty-TPs werden auf den nächsten TP addiert) plus TP4 Full-Close
      — diese reichen oft auch bei kleinen Positionen. Wrapper versucht
      jetzt erst Setzen und warnt "manuell" nur wenn count==0
      zurückkommt. Telegram-Hinweis bei echtem Bail-Out (vorher nur Log).

Changelog v4.34 — trades.csv via Telegram transferieren:
  T1: Neuer Befehl /trades sendet die aktuelle trades.csv als Telegram-
      Dokument (via sendDocument + multipart-Upload). Caption enthält
      Zeilen-Anzahl (Header + Datenzeilen), Dateigrösse und Mtime, damit
      man vor dem Editieren lokal weiss, was man gerade runterlädt.
      Nutzen: Railway hat keinen File-Browser für Volumes — bisher war
      der einzige Weg `railway run cat /app/data/trades.csv`. Jetzt
      reicht ein Tap im Telegram-Chat.
  T2: Neuer Befehl /restore_trades zeigt Anleitung, wie man eine
      bereinigte CSV zurück aufs Volume bringt: Datei an den Bot
      anhängen und als Bildunterschrift /restore_trades setzen.
      poll_telegram_commands() prüft bei jedem Document-Message jetzt
      die Caption — nur mit exaktem /restore_trades-Caption wird der
      Upload verarbeitet (sonst Silent-Ignore, damit normale Media-
      Posts nicht aus Versehen die CSV überschreiben).
  T3: handle_trades_restore() validiert die hochgeladene Datei:
      → Filename muss auf .csv enden (getFile via Bot-API,
        Download-URL = api.telegram.org/file/bot<token>/<file_path>).
      → Erste Zeile muss den Semikolon-getrennten _CSV_HEADER enthalten
        (Datum;Zeit (UTC);Symbol;…;Jahr) — sonst Abbruch.
      → Vor dem Überschreiben wird die bestehende TRADES_CSV nach
        TRADES_CSV.backup_YYYYMMDD-HHMMSS.csv gesichert.
      → Danach wird atomar via os.replace() auf TRADES_CSV geschrieben.
      Bestätigungs-Telegram enthält: alte vs. neue Zeilenzahl,
      Backup-Pfad und Hinweis auf /report für Plausibilitäts-Check.
  T4: /hilfe-Block um den Abschnitt "Archiv-Transfer" ergänzt.
      /health + Startup-Banner auf v4.34. (v4.35 erbt diesen Stack;
      eigene Banner-Bumps siehe Zeilen oben.)

Changelog v4.33 — Daily-Report Break-even-Klassifikation:
  R1: build_daily_report() klassifiziert Trades jetzt rein P&L-basiert.
      Bisher galt alles mit net_pnl ≤ 0 als "Loss" (Zeile 2164: won =
      net_pnl > 0, plus Zeile 6281: losses = count - wins). Trades mit
      net_pnl == 0 (Break-even / Phantom-Close-Artefakte vor v4.32-Fix)
      landeten deshalb im 🔴-Bucket und verfälschten die Win-Rate.
      Neue Regel: pnl>0 = 🏆 Win, pnl<0 = 🔴 Loss, pnl==0 = ⚪ Break-even.
      Win-Rate-Nenner = wins+losses (Break-evens werden ignoriert, damit
      flache Trades die Quote nicht künstlich drücken). Break-even-Zähler
      erscheint in der Summary-Zeile nur wenn > 0 — an normalen Tagen
      ändert sich an der Optik nichts.
  R2: Trade-Liste im Report zeigt ⚪ statt 🔴 für +0.00-Einträge.
  R3: /health + Startup-Banner auf v4.33.

Changelog v4.32 — Phantom-Close-Guard (LDOUSDT-Bug-Fix):
  P1: Double-Check vor Close-Booking. Wenn ein Symbol plötzlich aus
      get_all_positions() verschwindet, wird jetzt zuerst der Cache
      invalidiert und ein frischer Raw-API-Call gemacht. Nur wenn der
      2. Read das Symbol ebenfalls nicht enthält, wird
      handle_position_closed() gefeuert. Grund: Am 2026-04-23 07:48:43
      UTC hat Bitget bei LDOUSDT (SHORT 7x @ 0.38963) für einen einzigen
      Tick keine Position gemeldet → Bot buchte einen falschen +0.00-
      Close, stornierte TPs und re-seedete bei 07:49:06 als "neuer Trade"
      mit identischem Entry, aber frischem -25 %-SL. Echter Profit von
      ~13 USDT ging als 0 in die CSV, Trailing-Fortschritt auf TP1 war
      weg. Der zweite API-Call kostet im Normalfall nichts (Cache), im
      Phantom-Fall ein einziger zusätzlicher REST-Roundtrip.
  P2: Setup-Guard gegen Phantom-Reopen. setup_new_trade() prüft jetzt
      vor jedem neuen Setup, ob recent_closes[symbol] einen Eintrag
      innerhalb der letzten PHANTOM_REOPEN_TTL_SEC (120s) enthält UND
      der neue Entry bit-identisch (±0.1 %) zum gemerkten Close-Entry
      ist UND die Richtung gleich. Trifft das zu → State rollback
      (trailing_sl_level, sl_at_entry, trade_data) statt Neu-Setup.
      Keine neuen DCAs, kein -25 %-Auto-SL, kein zerstörter Trailing-
      Progress. Telegram-Nachricht ♻️ "Phantom-Reopen abgefangen"
      macht das für den Operator sichtbar. Zweite Sicherheitsebene
      falls der Raw-API-Read aus P1 ausnahmsweise auch leer kommt.
  P3: Neuer In-Memory-State recent_closes + PHANTOM_REOPEN_TTL_SEC /
      PHANTOM_ENTRY_TOLERANCE Konstanten. Snapshot wird in
      handle_position_closed() VOR dem State-Reset geschrieben und
      überlebt Railway-Redeploys via save_state/load_state (Einträge
      ausserhalb des TTL-Fensters werden beim Speichern/Laden verworfen,
      um dominus_state.json klein zu halten).
  P4: /health + Startup-Banner auf v4.32.

Changelog v4.31 — Observability & SL-Status-Wahrheit:
  O1: Neuer Helper infer_trailing_level(symbol, direction, entry, leverage)
      liest den IST-SL auf Bitget (via get_sl_price) und leitet daraus den
      Trailing-Level (0=unter Entry · 1=Entry · 2=TP1 · 3=TP2) ab. Wird in
      /status, /berechnen und build_daily_report anstelle des blinden
      Dict-Lookups trailing_sl_level[sym] aufgerufen. Heilt den State-Dict
      nach oben, wenn die Wahrheit höher liegt (save_state). Rückstufungen
      bleiben dem regulären Flow vorbehalten — stale SL-Read → Dict-Fallback.
      Grund: Nach Railway-Redeploy oder manuellem SL-Move auf Bitget hatte
      das Dict den realen Fortschritt verpasst — /status zeigte z.B.
      "SL=Entry" obwohl der SL längst auf TP1 stand.
  O2: Werkzeug-Access-Log runtergedreht: werkzeug-Logger.setLevel(WARNING).
      Grund: 80 von 83 "error"-Zeilen im 6h-Railway-Log waren reine
      "POST /webhook 200"-Access-Log-Einträge, die Werkzeug auf stderr
      schreibt und die in Railway als error-Severity landen. Damit waren
      echte Fehler im Rauschen nicht sichtbar. Token-Redact-Filter bleibt
      defensiv drauf, falls Werkzeug mal auf WARNING-Level eine URL loggt.
  O3: Drop-Logging im Webhook-Dispatcher. Fünf Pfade, die vorher silent
      returnten, schreiben jetzt eine "⏭"-markierte Log-Zeile (Forensik
      ermöglicht ohne Telegram-Noise):
        · HARSI_SL/SLING_SL ohne offene Position (Auto-Direction leer)
        · HARSI_SL ohne Preis
        · HARSI_SL ohne offene Position
        · SLING_SL ohne Pivot
        · SLING_SL ohne matching Position
      Grund: 6h-Log-Analyse zeigte 191 Alerts aus 73 Symbolen, aber 0
      "ignoriert"-Einträge → jede "Warum hat Symbol X nicht getriggert?"-
      Frage endete bisher in Rätselraten.
  O4: /health-Endpoint liefert jetzt die korrekte Version v4.31 (war seit
      v4.25 hardkodiert stehen geblieben).
  O5: Startup-Banner auf v4.31 gebumpt.

Changelog v4.30 — Kosmetik: BTC/Total2 DOM-DIR-Meldung nur bei echtem Wechsel:
  K1: BTC_DIR/T2_DIR-Webhook-Handler sendet Telegram-Message + save_state()
      jetzt nur noch, wenn new_dir != prev (echter Richtungs-Wechsel). TV-
      Alarme können pro Bar mehrfach triggern; bisher kam jeder Tick als
      volle "BTC Impuls → 🟢 Grün (Bullish bestätigt)"-Nachricht inkl.
      Gegenpositions-Warnung durch — das hat sich angefühlt wie mehrere
      Flips pro Tag, war aber nur Re-Trigger. Bei Gleichstand wird noch
      eine knappe Log-Zeile geschrieben ("📡 BTC DOM-DIR Tick (unverändert:
      LONG) — ignoriert") damit der Alive-Check über die Logs sichtbar
      bleibt.
  K2: Erster-ever-Signal-Fall bleibt korrekt — prev="" und new_dir="long"
      unterscheiden sich, also feuert die volle Initial-Meldung wie gehabt.

Changelog v4.29 — datetime.utcnow() Deprecation-Fix (Python 3.12+ Futureproof):
  D1: Alle 8 Vorkommen von datetime.utcnow() auf datetime.now(timezone.utc)
      migriert. utcnow() ist seit Python 3.12 deprecated (gibt naive UTC
      zurück, was gegen die zwingende Aware-Empfehlung für alle Zeitvergleiche
      verstösst). Python 3.15 entfernt die Funktion komplett — Migration
      garantiert Zukunftssicherheit ohne Verhaltensänderung.
      Betroffene Stellen: cmd_makro Timestamp-String, harsi-window Status-
      Prüfung (_harsi_window_status), /watchlist-List-Helper
      (_list_active_harsi_windows), HARSI_EXIT-Pfad timing-elapsed,
      H2_SIGNAL-Write + 35-Min-Cleanup, Ablauf-Zeile im HARSI_EXIT-Render-
      Pfad, load_state() Migration-Pfad. Plus 1× Default-Fallback in
      dict.get(..., datetime.utcnow()).
  D2: Neuer Helper _ensure_aware_utc(dt). Defensiv angewandt an allen Stellen
      wo Werte aus last_h2_signal_time oder load_state() subtrahiert/
      verglichen werden. Grund: Beim ersten Deploy nach v4.29 kann die
      State-Datei auf Railway noch naive ISO-Strings enthalten ("2026-04-22
      T00:00:00" ohne +00:00). datetime.fromisoformat() liefert dann naive
      datetime. Subtraktion aware-minus-naive → TypeError. Der Shim setzt
      fehlende tzinfo auf UTC und lässt aware-Werte unverändert; nach dem
      ersten save_state() sind alle Serialisierungen aware-roundtrip-safe.
  D3: Kein externes Verhalten geändert — strftime-Output, Cutoff-Werte
      (30 Min / 35 Min), alle Vergleichsgrenzen bleiben identisch. Der Fix
      ist reine Python-API-Modernisierung plus ein-maliger Migrations-Shim.

Changelog v4.28 — Stufe B: One-Click-Execution (🚀 Trade jetzt-Button):
  E1: Neuer Button 🚀 Trade jetzt (Auto) in der Detail-Ansicht der
      Entry-Rangliste. callback_data = exec:SYM:DIR:LEV:ENTRY:SL (~41 Byte).
      Erscheint nur wenn AUTO_TRADE_ENABLED=true (Railway-Env) UND Setup-
      Suggestion komplett vorliegt. Baut direkt auf dem v4.25-Slot-Keyboard
      auf — keine neue Message, nur zusätzliche Reihe vor ❌ Schliessen.
  E2: Two-Tap-Confirmation als Fat-Finger-Schutz. Erster Tap: Callback-Alert
      "TRADE AUSFÜHREN? {SYM} {DIR} {LEV}x — Nochmals tippen zur Bestätigung
      — läuft in 10s ab". State wird in-memory in _exec_confirm[(msg_id,
      payload_sig)] gehalten mit TTL-Expiry. Zweiter Tap innerhalb TTL führt
      execute_trade_order aus; nach TTL = Reset zum ersten Tap. TTL
      konfigurierbar via AUTO_TRADE_CONFIRM_TTL_SEC (default 10).
  E3: Neue Core-Funktion execute_trade_order(symbol, direction, leverage,
      entry, sl). Pipeline:
        (1) Pre-Validate: AUTO_TRADE_ENABLED, SL-Seite korrekt, Hebel in
            [1, MAX_LEVERAGE], Balance > 0.
        (2) Sizing: Half-Kelly-Total (kelly_recommendation) / 5 = Initial-
            Margin (entspricht 20% im DOMINUS 20/30/50-Schema). Gekappt auf
            balance*0.10/3 (10%-Regel) und optional MAX_AUTO_TRADE_USDT.
        (3) set_leverage_on_bitget() — POST /account/set-leverage mit
            holdSide=direction. Non-fatal bei Fehler (Bitget fällt auf
            Account-Default zurück).
        (4) Market-Order via /order/place-order (orderType=market,
            tradeSide=open, marginMode=isolated).
        (5) 3s Sleep, danach _get_all_positions_raw() — verifiziert dass
            Position tatsächlich existiert (bypasst 5s-Cache). Kein Fill
            → Fail-Closed Return.
        (6) SL via /order/place-pos-tpsl mit stopLossTriggerType=mark_price.
            Fehler nach geöffneter Position → PANIC-Telegram + "partial"-
            Status, damit User manuell nachziehen kann.
      DCA + TP1–TP4 setzt der bestehende Main-Loop über setup_new_trade()
      automatisch im nächsten Position-Watcher-Tick.
  E4: ENV-Flags (alle neu in v4.28):
        • AUTO_TRADE_ENABLED (default "0") — Feature-Gate. Nur "1"/"true"/
          "yes"/"on" aktivieren den Button + execute_trade_order.
        • AUTO_TRADE_CONFIRM_TTL_SEC (default 10) — Sekunden zwischen erstem
          und zweitem Tap.
        • MAX_AUTO_TRADE_USDT (default 0) — Hard-Cap für Initial-Margin pro
          Auto-Trade. 0 = kein extra Cap (nur Kelly + 10%-Regel).
  E5: Neuer Helper set_leverage_on_bitget(symbol, direction, leverage) —
      POST /api/v2/mix/account/set-leverage. Bisher hat das Script nie den
      Hebel selber gesetzt (war immer manuell auf Bitget vorbelegt). Für
      Auto-Trade essenziell — Bitget lehnt sonst Orders ab die den aktuellen
      Hebel überschreiten.
  E6: Result-Reporting: ✅-Telegram mit Order-ID/Qty/Margin/Notional bei
      Erfolg, ❌-Telegram mit reason-String bei Abbruch, 🚨-Telegram bei
      "partial" (Position offen aber SL-Fehler) — mit explizitem "manuell
      SL setzen"-Hinweis + Bitget-Button.

Changelog v4.27 — Score-Formel Refactor (SL/Hebel entkoppelt + Gegen-Trend-Malus):
  S1: SL-Abstand Docstring an tatsächliche Formel angepasst. Alte Doku behauptete
      2%=7pt, 5%=1pt; die Formel `15 − (sl_pct − 0.5) * 3` liefert aber 2%=10pt,
      5%=2pt (Slope −3/% statt der Doku-impliziten −5/%). Formel bleibt —
      Doku-Strings im Score-Breakdown sind jetzt konsistent.
  S2: Hebel-Punkte von Linear (lev−2, Cap 10) auf asymmetrische Glockenkurve
      um 12x Peak umgestellt. Unten Slope −1.0 pro Hebel-Stufe (5x=3pt, 8x=6pt,
      10x=8pt, 12x=10pt), oben flacher Slope −0.7 (15x=8pt, 18x=6pt, 25x=1pt,
      30x+=0pt). Entkoppelt SL+Hebel: Da in DOMINUS Hebel=25/SL% gilt, gab es
      bei extremen Scalps (SL 0.5% ⇒ 50x) Double-Counting → früher 25pt, jetzt
      15pt. Sweet-Spot bei SL 2% ⇒ 12x bleibt 20pt kombiniert. Dadurch belohnt
      Score tatsächliches R:R-Gleichgewicht statt reiner Tightness.
  S3: Gegen-Trend-Malus −5 eingeführt (bisher nur Warnung). Wenn btc_dir ODER
      t2_dir bekannt sind, aber KEIN _dir_matches() greift (d.h. Makro-Trend
      ist die Gegenrichtung, kein Recovering-Fall), gibt es jetzt expliziten
      Punkt-Abzug. Recovering-Zustände werden via _dir_matches() als konform
      gewertet und bleiben malus-frei.
  S4: Korrelations-Malus linear gestaffelt. Bisher binary: ab 2 gleichartigen
      offenen Positionen pauschal −10. Neu: 2 offen = −10 (wie bisher), 3+ =
      −15 (Cap). Stärkerer Anti-Klumpen-Effekt ohne Score-Kollaps.

Changelog v4.26 — Entry-Queue Robustness (30-Min-Fenster + Logging):
  R1: save_state() wird jetzt unmittelbar nach jeder Mutation von
      last_h2_signal_time aufgerufen — beim Setzen neuer H2-Zeitstempel
      (H2_SIGNAL-Empfang mit anschliessendem 35-Min-Cleanup) UND beim
      Löschen nach Consume (HARSI_EXIT Queue-Pfad, HARSI_EXIT Fallback-
      Pfad, H2_SIGNAL harsi_warn=0 Queue-Pfad). Bisher wurde die
      Serialisierung nur implizit beim nächsten BTC_DIR/Trade-Event
      mitgeschrieben — bei Railway-Redeploy innerhalb des 30-Min-Fensters
      war das H2-TS damit verloren und der folgende HARSI_EXIT bekam
      "Kein H2-Signal gespeichert". Mit v4.26 überlebt das Fenster jeden
      Redeploy, solange < 30 Min alt (load_state() filtert automatisch).
  R2: Erweitertes ENTRY-QUEUE Dedup-Log. enqueue_entry() vergleicht bei
      Re-Trigger (gleicher symbol_direction) die Felder source/warn_line/
      timing_elapsed_min/harsi_warn zwischen alter und neuer Entry und
      loggt den Diff — z.B. "source: 'H2_SIGNAL'→'HARSI_EXIT' | elapsed:
      0→12". Hilft bei Forensik, welche Signal-Quelle den Batch zuletzt
      überschrieben hat.
  R3: HARSI_EXIT-ohne-H2-Warnung wird jetzt als strukturierter Log-
      Eintrag mit Dump aller aktuell offenen H2-Fenster-Keys geschrieben
      ("[HARSI_EXIT-ohne-H2] SYMBOL_DIR — Aktive H2-Fenster: [...]").
      Erleichtert Nachforschung, ob der fehlende H2-TS ein Bug ist oder
      eine echte TV-Out-of-Order-Lieferung.
  R4: HARSI_EXIT-Enqueue trägt jetzt expliziten "source": "HARSI_EXIT"-
      Marker (bisher implizit). H2_SIGNAL hatte "source": "H2_SIGNAL"
      bereits seit v4.22. Damit ist der R2-Dedup-Diff aussagekräftig.

Changelog v4.25 — Button-Driven Entry-Rangliste (One-Message-UX):
  B1: Die Entry-Rangliste ist keine seitenlange Text-Liste mehr, sondern
      eine KOMPAKTE Übersichts-Message mit Coin-Buttons (Top-10 im 3er-Grid,
      darunter "▾ N weitere"). Tap auf einen Coin-Button klappt dessen Detail
      per editMessageText in derselben Message auf — inkl. Trade-Vorschlag
      (Entry/SL/Hebel/DCA), Score-Breakdown mit rechtsbündiger Punkte-Spalte
      (✅ Faktor ... +XX) und Premium-Hinweis. Nur EIN Coin gleichzeitig offen.
  B2: Adaptives Inline-Keyboard im Detail-Zustand. Detail-relevante Buttons
      (🎯 Berechnen / 🟠 Bitget / 📊 TV / 📈 BTC H2 / 🔀 Total2 /
      ❌ Schliessen) oben, danach Divider (Label-Button, callback=noop), dann
      die Coin-Grid-Buttons. Bitget + TV-Links werden auf den offenen Coin
      umgeschaltet; BTC/Total2 bleiben gleich.
  B3: 1-Tap-Berechnung via Callback-Button "🎯 Berechnen {COIN}".
      callback_data = calc:SYM:DIR:LEV:ENTRY:SL (36 Byte, Limit 64).
      Callback-Handler delegiert an cmd_trade() — User bekommt die volle
      /trade-Antwort als separate Message in denselben Chat.
  B4: Score-Breakdown jetzt als ✅-Liste mit rechtsbündiger Punkte-Spalte
      plus "Score"-Summe unten. DOMINUS Impuls Extremzone + H4 Trigger sind
      als Pine-Gates (ohne Punkte) oben gelistet — sie sind bei ankommendem
      Signal per Definition erfüllt.
  B5: Farbkodiertes Score-Badge 🟢 ≥75 / 🟡 50–74 / 🔴 <50 in Buttons und
      Detail-Header.
  B6: Top-5-Kurzform im Übersichts-Header ("⚡ Top-5  AVAX 82 · OP 78 ...").
      Hilft beim Lock-Screen-Check ohne Message zu öffnen.
  B7: Low-Score-Bucket: alle Signale unter SLOT_LOWSCORE_CUT (default 50)
      wandern in einen separaten "▾ N weitere"-Block (einzeilig kompakt).
      Spart bei 30+ Signalen ~60% vertikalen Platz.
  B8: State-Management _slot_states[msg_id] → {ranked, balance, current_detail,
      view_mode, created_ts}. TTL 2h, Thread-safe via _slot_states_lock.
      Expired-Callbacks → Toast "Slot abgelaufen — neue H2-Welle abwarten".
  B9: Neue Helpers:
        • telegram(..., return_id=True) — gibt message_id zurück
        • telegram_edit_message(msg_id, text, reply_markup) — editMessageText
        • telegram_answer_callback(cb_id, text, show_alert) — answerCallbackQuery
        • format_slot_overview / format_slot_detail / format_slot_more
        • build_slot_keyboard(state, open_symbol, mode)
        • handle_callback_query(update) — Dispatcher
  B10: ENV-Flags SLOT_TOP_N (default 10), SLOT_LOWSCORE_CUT (default 50) —
      erlauben spätere Feinjustierung ohne Code-Änderung.

Changelog v4.24 — Ghost-Flag-Fix (kein fälschliches "♻️ TPs nach DCA" mehr):
  G1: Discriminator in main()-Polling-Loop gehärtet. Bisher entschied allein
      new_trade_done[sym] über "Neuer Trade vs. Nachkauf". Wenn eine
      vorherige Position via Bitget-GUI oder Race-Condition geschlossen wurde
      ohne dass handle_position_closed() das Flag zurücksetzen konnte, blieb
      new_trade_done=True im State hängen. Folge-Fill eines NEUEN Trades
      wurde dann als DCA behandelt → "♻️ TPs nach DCA"-Message, kein
      setup_new_trade, fehlende DCA-Limits, CSV-Log, trade_data, Trailing-
      SL-Init, Kelly/R:R-Check, Auto-SL-Fallback.
      Neu: is_known_pos = (last_known_size > 0 AND last_known_avg > 0 AND
      sym in trade_data). Nur wenn alle drei konsistent vorhanden sind,
      gilt's als Nachkauf. Sonst wird der Ghost-State gewarnt, weggeputzt
      und setup_new_trade() läuft sauber.
  G2: Startup-Cleanup — nach load_state() + get_all_positions() gleicht der
      Bot den persistent-state mit der Bitget-Live-Position-Liste ab. Alle
      Symbole mit new_trade_done=True aber ohne aktive Position werden als
      verwaiste Ghost-Flags erkannt und komplett entfernt (new_trade_done,
      last_known_avg/size, sl_at_entry, trailing_sl_level, harsi_sl,
      sling_sl, dca_void, trade_data) und der bereinigte State wird sofort
      persistiert. Damit kommt kein Ghost-State mehr nach einem Railway-
      Restart in den Polling-Loop.

Changelog v4.23 — TV-Coin-Button + ✅-Checkliste (Pine-geprüft):
  T1: build_setup_buttons(symbol) Row 1 um zweiten Button erweitert — neben
      🟠 Bitget {COIN} jetzt 📊 TV {COIN} H2. Link öffnet den Coin direkt im
      gespeicherten DOMINUS-Layout (lX5eDAis) auf H2-Timeframe. Greift
      automatisch in allen Signal-Messages (H2_SIGNAL, HARSI_EXIT, Extreme-
      Zone-Info, /berechnen, /trade, Close-Summary, Auto-SL).
  T2: DOMINUS-Checkliste in H2-Signal: die zwei hardcodierten ☐ (Impuls-
      Extremzone / H4-Trigger) sind jetzt ✅ "(Pine-geprüft)". Grund: Pine
      erzwingt diese Bedingungen bereits im Orchestrator (h4_imp_hit + h4_bar_
      confirmed → h4_long_active → h2_entry_long), bevor überhaupt ein
      H2_SIGNAL losgeschickt wird. Leere Kästchen waren irreführend, da sie
      bereits per Konstruktion true sind.

Changelog v4.22 — Unified Entry-Queue + Auto-HARSI_EXIT + Button-Konsistenz:
  V1: H2_SIGNAL mit harsi_warn=0 wird jetzt ebenfalls in die Entry-Queue
      eingespeist (bisher nur HARSI_EXIT). Damit erscheinen mehrere gleichzeitige
      Einstiegs-Signale — unabhängig davon ob sie über H2_SIGNAL oder HARSI_EXIT
      reinkommen — als EINE konsolidierte Rangliste im ENTRY_QUEUE_WINDOW_SEC-
      Fenster. H2_SIGNAL mit harsi_warn=1 bleibt Einzel-Message mit 30-Min-Timer
      (dort ist die Pro-Coin-Timer-Info relevant).
  V2: Der alte "Alarm 3 manuell anlegen"-Block beim H2-Signal mit harsi_warn=1
      wurde entfernt. Seit DOM-ORC v2.4.2 (Intrabar alert.freq_once_per_bar)
      feuert HARSI_EXIT automatisch über den Watchlist-Master-Alarm ("Any alert()
      function call"). Der User muss keinen separaten TV-Alarm mehr pro Coin
      anlegen — die timer_line verweist nur noch darauf, dass der Exit
      automatisch eintrudelt.
  V3: Button-Konsistenz — build_setup_buttons(symbol) (Bitget + BTC H2 + Total2)
      wird jetzt an ALLE Signal-Messages angehängt: H2_SIGNAL, HARSI_EXIT-
      Fallback, Extreme-Zone-Info, Entry-Rangliste (Top-1 Bitget). Damit ist
      der Klick-UX-Standard aus v4.21 durchgängig in der Signal-Pipeline.
  V4: Entry-Message enthält keine URL-Link-Zeilen mehr (H2/H4/BTC/Total2) —
      diese sind via Inline-Buttons abgedeckt. Weniger Text-Rauschen, bessere
      Mobile-Lesbarkeit.

Changelog v4.21 — Klick-UX, CSV-Dedup, Token-Redact, utcfromtimestamp-Fix:
  U1: Inline-Keyboard-Buttons für Trade-Setups — telegram() und reply() nehmen
      ein optionales reply_markup; build_setup_buttons(symbol) liefert das
      Standard-Layout (Row 1: 🟠 Bitget COIN, Row 2: 📈 BTC H2 / 🔀 Total2).
      Integriert in /trade-Setup, /berechnen-Style-Ausgabe, Auto-SL-Meldungen,
      SL-auf-Entry-Bestätigung und Close-Summary.
  U2: tv_chart_links(symbol) um bitget-Feld erweitert —
      https://www.bitget.com/futures/usdt/<SYMBOL> für Direkt-Sprung in den
      Futures-Chart des Coins.
  U3: Klickbare Alarm-Templates — /alarm-Overview zeigt Aliases wie
      /alarm_harsi_BTC_LONG. Router zerlegt den Unterstrich-Suffix in Tokens
      und dispatcht auf cmd_alarm(["/alarm", sub, SYMBOL, DIR]). Funktioniert
      analog für /alarm_harsi, /alarm_harsisl, /alarm_h2, /alarm_h4.
  U4: /dedup_trades-Command — Dry-Run zeigt Dubletten in trades.csv (Fingerprint
      symbol+direction+entry+close+pnl innerhalb 2h). /dedup_apply (oder
      /dedup_trades apply) legt Timestamp-Backup trades.csv.backup_YYYYMMDD-
      HHMMSS.csv an, schreibt dedup'te CSV atomar und bereinigt closed_trades[]
      im RAM. Dry-Run-Output enthält /dedup_apply als tappbaren Klick-Link.
  U5: Werkzeug-Log-Filter _TokenRedactFilter — scrubt WEBHOOK_SECRET aus
      Flask-Access-Logs, damit der Token in Railway-Logs nicht im Klartext
      auftaucht (Query-Param ?token=… und JSON-Body-Redaction).
  U6: datetime.utcfromtimestamp() Deprecation-Fix — auf
      datetime.fromtimestamp(ts, tz=timezone.utc) umgestellt (2 Stellen).
      OFFEN: 8 weitere datetime.utcnow()-Vorkommen werden separat migriert.

Changelog v4.20 — Entry-Queue-Outcome-Tracking (datengetriebene Optimierung):
  T1: entry_queue_log.csv persistiert JEDES bewertete Signal (auch Rank≥2 und
      nicht-getradete) — Schema: id, ts, symbol, direction, score, rank, total,
      premium_flag, taken (1/0), outcome (open/win/loss), r_multiple, duration_h.
  T2: Two-Phase-Logging: log_scored_entry() schreibt alle Queue-Kandidaten beim
      flush_entries(); mark_trade_taken() und später der Close-Handler annotieren
      Outcome und R-Multiple zurück in die CSV (atomic Read-Modify-Write mit Lock).
  T3: symbol_win_rate() ist jetzt aus trades.csv + entry_queue_log.csv gespeist —
      Queue-Log liefert zusätzlich die Gegenprobe "hätte funktioniert, aber nicht
      getradet" für künftige Parameter-Kalibrierung.
  T4: /queue_stats-Command — zeigt Win-Rate, durchschnittlicher R-Multiple,
      Wilson-Konfidenzintervall und optimaler Kelly über das Queue-Log.

Changelog v4.19 — Entry-Queue: Ranked Entry-Liste bei mehreren HARSI_EXIT:
  Q1: Mehrere HARSI_EXIT-Signale innerhalb ENTRY_QUEUE_WINDOW_SEC (Standard
      90s) werden gesammelt und als EINE konsolidierte Telegram-Rangliste
      gesendet — keine separaten Pop-ups mehr pro Signal.
  Q2: Zwei Buckets: 🎯 PREMIUM (Makro-Extremzone + BTC_DIR/T2_DIR konform,
      Handbuch-Hard-Gate) und 📋 REGULAR. Innerhalb jeder Gruppe absteigend
      nach Quality-Score (0-100).
  Q3: score_entry() bewertet: Makro-Premium (+30), BTC/T2-Richtung (+20/+10),
      SL-Abstand (+15), Hebel als ATR-Proxy (+10), historische Win-Rate aus
      trades.csv (+15), harsi_warn=0 (+5), Timing-Frische (+5), Korrelations-
      Malus (-10 bei ≥2 offenen Positionen gleicher Richtung).
  Q4: symbol_win_rate() liest die letzten 20 Trades pro Symbol aus trades.csv,
      cacht 1h und lernt damit über Zeit — je mehr Historie, desto präziser.
  Q5: Dedup im Fenster: identisches symbol+direction erhöht confirm_count
      (Pine re-trigger = Bestätigung), keine Doubletten in der Liste.
  Q6: ENTRY_QUEUE_ENABLED (Default 1) — per Railway-Variable abschaltbar
      für Rollback ohne Redeploy. Abgelaufene 30-Min-Fenster senden weiterhin
      die bisherige Einzel-Warnung (nur frische HARSI_EXITs werden gequeued).
  Q7: State-Persistenz NICHT nötig — die Queue ist transient (max. 90s).
      Ein Railway-Restart während eines offenen Fensters verliert höchstens
      einige noch nicht ausgelöste Einstiege.

Changelog v4.18 — /refresh Telegram-Anzeige inkl. TP4:
  R1: /refresh-Nachricht zeigte "TPs gesetzt: 3" obwohl 4 TPs auf Bitget
      gesetzt waren. Ursache: get_existing_tps() liest nur profit_plan-Orders
      (TP1/TP2/TP3). TP4 lebt als Full-Close via place-pos-tpsl in einem
      separaten Slot und wurde deshalb nicht mitgezählt.
  R2: Im /refresh-Report wird TP4-Preis zusätzlich via _get_pos_tp_price()
      ermittelt und zum Count addiert. Anzeige jetzt "TPs gesetzt: 4/4
      (inkl. TP4 @ <Preis>)" bzw. "(ohne TP4)" wenn nicht gesetzt —
      deckt sich mit dem, was Bitget tatsächlich im GUI zeigt.

Changelog v4.17 — Bitget-Feldname: takeProfit → stopSurplus (Root-Cause-Fix):
  F1: Root-Cause für TP4-Verschwinden gefunden — Bitget v2 `place-pos-tpsl`
      erwartet `stopSurplusTriggerPrice` / `stopSurplusTriggerType` (NICHT
      `takeProfitTriggerPrice`, das ist der Parameter von `place-tpsl-order`).
      Bitget akzeptiert den Request mit code=00000 (weil SL-Felder gültig
      sind), ignoriert aber das unbekannte TP-Feld → TP4 wurde nie persistiert.
  F2: Alle 6 place-pos-tpsl-Aufrufe umgestellt (place_tp_orders TP4-Initial,
      TP4-Retry mit reduzierten Dezimalen, set_sl_at_entry, set_sl_trailing,
      set_sl_harsi, set_sl_sling, Phase-0-SL-Nachzug im Monitor).
  F3: _get_pos_tp_price() liest TP4 jetzt aus erweiterter Feldliste
      (presetStopSurplusPrice, stopSurplusPrice, stopSurplusTriggerPrice +
      Legacy-Namen als Fallback) — Bitget retourniert den TP je nach Variante
      unterschiedlich.
  F4: Startup-Banner Version (hartkodiert "v4.35") auf dynamische Version
      umgestellt — zeigt ab jetzt korrekt die aktive Version im Log.

Changelog v4.16 — TP4-Persistenz (Hotfix: TP4 verschwindet nach SL-Update):
  K1: Ursache: Bitget `place-pos-tpsl` speichert SL im single-position-Response
      (Feld `stopLoss`), aber NICHT den TP (kein `takeProfitPrice`-Feld).
      Fallback via Plan-Orders (`pos_profit`) ist unzuverlässig — bei gerade
      fehlschlagenden Endpoints liefert `_get_pos_tp_price()` dann 0 → bei
      SL-Updates wird TP4 nicht mitgeschickt → Bitget löscht ihn still.
  K2: trade_data[symbol]["tp4"] speichert jede erfolgreiche TP4-Platzierung
      lokal. Wird bei Restart aus dominus_state.json rekonstruiert.
  K3: _get_pos_tp_price() prüft Quellen in Reihenfolge: (1) trade_data["tp4"],
      (2) Position-Felder (takeProfitPrice, …), (3) Plan-Orders (pos_profit).
      Damit ist TP4 immer verfügbar, auch wenn Bitget-API hängt.
  K4: cache_invalidate() wird nach jedem erfolgreichen SL+TP4-Update
      aufgerufen, damit der nächste Read garantiert frische Daten sieht.

Changelog v4.15 — TP-Stornierung: Einzelcall mit planType (Hotfix):
  T1: cancel_all_tp_orders() — Bitget v2 Batch-Mode (orderIdList) liefert
      für profit_plan-Orders silent `code=00000` mit leeren success/fail-
      Listen zurück → Orders werden NICHT storniert. v4.14 hat das als
      Erfolg interpretiert → 6 alte + 4 neue TPs = 10 Duplikate.
  T2: Migration auf Einzelstornierung mit explizitem `planType=profit_plan`
      (analog python-bitget `mix_cancel_plan_order`). Jeder Cancel-Response
      wird einzeln geprüft, Gesamtstatistik am Ende geloggt.
  T3: Robuster gegen Orders ohne orderId (clientOid-Fallback), jeder echte
      Fehler wird jetzt mit Code+Msg geloggt — kein stilles No-Op mehr.

Changelog v4.14 — Bitget-v2 Plan-Order-Endpoints (TP-Duplikate-Fix):
  P1: _get_plan_orders() — nutzt jetzt den v2-Pflichtparameter `planType`
      und queryt `profit_loss` / `normal_plan` / `track_plan` / `moving_plan`
      einzeln + mergt die Ergebnisse dedupliziert. Vorher:
      "Parameter verification failed" in allen 4 Fallback-Varianten, weil
      `orders-plan-pending` zwingend `planType` verlangt.
  P2: cancel_all_tp_orders() — der nicht existierende v2-Endpoint
      `/api/v2/mix/order/cancel-all-plan-order` ("Request URL NOT FOUND")
      ist komplett entfernt. Ersetzt durch ECHTEN Batch-Cancel via
      `cancel-plan-order` mit `orderIdList[]` (ein Call, alle TPs). Fehl-
      geschlagene orderIds werden automatisch einzeln nachversucht.
  P3: /refresh-Bug behoben — alte TPs werden jetzt zuverlässig storniert
      bevor neue gesetzt werden. Keine TP-Duplikate mehr auf Bitget.
  P4: SL (loss_plan / pos_loss) und TP4 (pos_profit) werden weiterhin
      nicht angefasst von cancel_all_tp_orders — saubere Rollenteilung:
      Einzel-TPs via cancel-plan-order, Positions-TPSL via place-pos-tpsl.

Changelog v4.13 — Webhook-Async + Bitget-Cache (TradingView-Timeout-Fix):
  W1: Webhook-Handler sofort 200 — Token-Prüfung + JSON-Parse im HTTP-Request,
      danach sofort ACK zurück. TradingView bekommt nie wieder einen Timeout
      (Root-Cause: Bitget-API-Aufrufe > 5s haben alle Deliveries blockiert).
  W2: _process_webhook_async() — komplette Signal-Verarbeitung (H2/HARSI_SL/
      SLING_SL/HARSI_EXIT/BTC_DIR/T2_DIR/BTC_OVERSOLD/...) läuft jetzt in
      einem Daemon-Thread. Exceptions werden geloggt + Telegram-Alert.
  W3: TTL-Cache für Bitget-API (get_mark_price 3s / get_futures_balance 10s /
      get_all_positions 5s) — bei parallelen Watchlist-Webhooks (z.B. 12
      H2-Close-Alerts zur gleichen Sekunde) wird Bitget nur noch einmal
      getroffen. Drastische Latenz-Reduktion.
  W4: cache_invalidate() — nach jedem erfolgreichen Order-Placement werden die
      Positions- + Balance-Caches geleert, damit Folge-Checks frische Daten sehen.
  W5: /health-Endpoint liefert jetzt korrekt "v4.13" (vorher hartkodiert v4.35).
  W6: Robuste Env-Parser _env_int/_env_float — Railway-Variablen mit Leerstring
      ("" statt fehlend) crashen nicht mehr den Container-Start (betraf
      H4_BUFFER_SEC, EXTREME_COOLDOWN_H, MAX_LEVERAGE, PORT, WINRATE, MIN_RR,
      MAX_EXPOSURE_PCT, SLING_ATR_MULT, SLING_PCT_FLOOR).

Changelog v4.12 — Sling-SL Universal + /trade-Vorschlag + Exposure-Cap:
  S1: SLING_SL-Webhook akzeptiert direction="auto" — Richtung wird aus
      offener Position abgeleitet (analog HARSI_SL v4.11).
  S2: set_sl_sling() — setzt SL auf Swing-Pivot-Preis (nur wenn näher
      am Markt als aktueller SL = protektiv). Short: Sling-High, Long: Sling-Low.
  S3: ATR(14)-Fallback — wenn Pivot zu nah am Markt (< max(0.8%, 0.5×ATR)),
      wird dieser Mindestpuffer gewahrt.
  S4: DCA Auto-Void — wenn neuer SL einen DCA-Level überschreitet,
      wird die DCA-Order storniert (DCA-im-Gewinn ist logisch unmöglich).
  S5: H2_SIGNAL / HARSI_EXIT — /trade-Vorschlag enthält jetzt vollständig
      berechneten Hebel + SL + Exposure-Check (aus sling_sl + atr).
  S6: MAX_EXPOSURE_PCT = 0.25 — Gesamt-Einsatz (Margin × Hebel) pro Trade
      darf 25% des Kontostands nicht überschreiten. Wird in setup_new_trade()
      und cmd_trade() geprüft; Hebel wird bei Überschreitung reduziert.
  S7: State-Persistenz — sling_sl + dca_void überleben Railway-Restart.

Changelog v4.11 — HARSI_SL Universal-Watchlist (Vollautomatik):
  U1: HARSI_SL-Webhook akzeptiert direction="auto" (oder fehlend) —
      Richtung wird automatisch aus der offenen Position abgeleitet.
  U2: Ein einziger "Any alert() function call"-Watchlist-Alarm ersetzt
      die bisherigen Alarm-4/4b Pro-Symbol-Einrichtungen. Pine feuert
      bei jedem H2-Close mit HARSI-Kreuzung, Railway filtert stumm
      Symbole ohne offene Position.
  U3: Signal wird nur ausgeführt, wenn eine offene Position für das
      Symbol existiert — alle anderen Events silent-ignoriert.
  U4: Backward-kompatibel — bestehende Alarm 4/4b mit direction="long"/
      "short" funktionieren weiter.
  U5: HTML-Doku um "Alarm 4c — HARSI SL Universal" erweitert.

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
import shutil
from datetime import datetime, timedelta, timezone
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

# Demo-Mode: dieselben Credentials wie Live
# Der Demo-Modus wird ausschliesslich via paperId="1" Header gesteuert
API_KEY    = os.environ.get("API_KEY",    "")
SECRET_KEY = os.environ.get("SECRET_KEY", "")
PASSPHRASE = os.environ.get("PASSPHRASE", "")
DEMO_MODE  = True   # aktiviert paperId="1" + separaten Telegram-Kanal + Trade-Logging

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
TELEGRAM_CHAT_ID = os.environ.get("DEMO_TELEGRAM_CHAT_ID",
                   os.environ.get("TELEGRAM_CHAT_ID", ""))
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "dominus")  # Token für TradingView
WEBHOOK_URL      = os.environ.get("WEBHOOK_URL", "")  # optional: vollständige Railway-URL inkl. ?token=… für /alarm-Vorlagen
DOCS_URL           = os.environ.get("DOCS_URL", "https://GykoGenial.github.io/dominus-tp-updater/Dominus_Alarm_Templates.html")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")  # Service Account JSON als String
GOOGLE_SHEET_ID    = os.environ.get("GOOGLE_SHEET_ID",    "")  # Spreadsheet ID aus URL
TRADES_CSV         = os.environ.get("TRADES_CSV", "/app/data/trades.csv")  # Persistentes Trade-Archiv auf Railway Volume
ENTRY_LOG_CSV      = os.environ.get("ENTRY_LOG_CSV", "/app/data/entry_queue_log.csv")  # Queue-Entscheidungen + Outcomes (v4.20)


# v4.13: Robuste Env-Parser — Railway-Variablen können "" statt fehlend sein,
# darum NICHT einfach int(os.environ.get(..., "300")) verwenden — ein leerer
# String wird sonst an int() weitergereicht und crasht den Container-Start.
def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    try:
        return int(raw) if raw.strip() else default
    except (ValueError, TypeError):
        print(f"[WARN] {name}='{raw}' ungültig, nutze Default {default}")
        return default

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    try:
        return float(raw) if raw.strip() else default
    except (ValueError, TypeError):
        print(f"[WARN] {name}='{raw}' ungültig, nutze Default {default}")
        return default


# Finanzmathematische Parameter
WINRATE = _env_float("WINRATE", 0.55)  # eigene Winrate (historisch)
MIN_RR  = _env_float("MIN_RR", 1.5)    # Mindest R:R Ratio

# v4.12: Max-Exposure-Cap pro Trade (Van Tharp 1% regulär → hier Gesamt-Einsatz
# inkl. Hebel bei vollem DCA-Setup). Default 25% = 10%-Margin × Hebel-Cap 25x /
# Aufteilung entspricht dem DOMINUS-25%-Max-Loss-Framework.
MAX_EXPOSURE_PCT = _env_float("MAX_EXPOSURE_PCT", 0.25)
MAX_LEVERAGE     = _env_int("MAX_LEVERAGE", 25)

# v4.12: Sling-SL Fallback-Puffer (wenn Pivot < pct_floor vom Preis entfernt,
# wird mindestens max(pct_floor%, atr_mult × ATR) als Puffer gewahrt).
SLING_ATR_MULT  = _env_float("SLING_ATR_MULT",  0.5)
SLING_PCT_FLOOR = _env_float("SLING_PCT_FLOOR", 0.8)  # % vom Preis

# v4.19: Entry-Queue — mehrere HARSI_EXIT-Signale als Rangliste sammeln
# ENTRY_QUEUE_ENABLED ("1"/"0"/"true"/"false") — default aktiv.
# ENTRY_QUEUE_WINDOW_SEC — Sammelfenster in Sekunden (default 90).
_raw_eq = os.environ.get("ENTRY_QUEUE_ENABLED", "1").strip().lower()
if not _raw_eq:
    _raw_eq = "1"
ENTRY_QUEUE_ENABLED    = _raw_eq not in ("0", "false", "no", "off")
ENTRY_QUEUE_WINDOW_SEC = _env_int("ENTRY_QUEUE_WINDOW_SEC", 90)

# v4.28: Stufe B — One-Click-Execution (🚀 Trade jetzt-Button)
# AUTO_TRADE_ENABLED ("1"/"0"/"true"/"false") — default AUS aus Safety-Gründen.
# Muss auf Railway explizit auf "1" gesetzt werden damit der Button erscheint
# und execute_trade_order() Orders tatsächlich platzieren darf.
# AUTO_TRADE_CONFIRM_TTL_SEC — Sekunden in denen der zweite Tap die Bestätigung
# darstellt (Two-Tap-Confirm). Default 10s. Nach Ablauf ist wieder erster Tap
# nötig, der den State frisch setzt.
# MAX_AUTO_TRADE_USDT — Hard-Cap für initiale Market-Order-Margin (pro Trade).
# 0 = kein Extra-Cap (nur Half-Kelly/3 und 10%-Regel greifen).
_raw_at = os.environ.get("AUTO_TRADE_ENABLED", "0").strip().lower()
AUTO_TRADE_ENABLED        = _raw_at in ("1", "true", "yes", "on")
AUTO_TRADE_CONFIRM_TTL_SEC = _env_int("AUTO_TRADE_CONFIRM_TTL_SEC", 10)
MAX_AUTO_TRADE_USDT       = _env_float("MAX_AUTO_TRADE_USDT", 0.0)

# v4.35: Watchlist-Master Drop-Logging — Lärm-Reduktion
# Pine sendet SLING_SL/HARSI_SL für jedes Watchlist-Symbol. Ohne offene Position
# wird stumm verworfen — bisher mit Per-Drop-Log-Zeile (240+43 Drops in 24h
# = 12/h Lärm). v4.35: Drops werden gezählt und nur jede N-te Meldung als
# Summary geloggt. WATCHLIST_DROP_VERBOSE=1 stellt Per-Drop-Logs wieder her.
_raw_wdv = os.environ.get("WATCHLIST_DROP_VERBOSE", "0").strip().lower()
WATCHLIST_DROP_VERBOSE          = _raw_wdv in ("1", "true", "yes", "on")
WATCHLIST_DROP_SUMMARY_EVERY_N  = _env_int("WATCHLIST_DROP_SUMMARY_EVERY_N", 50)
_watchlist_drop_counter = {"count": 0}

def _track_watchlist_drop(reason: str, symbol: str, signal_type: str, direction: str = ""):
    """v4.35: zählt Watchlist-Master-Drops und loggt nur eine Summary alle
    WATCHLIST_DROP_SUMMARY_EVERY_N Events. WATCHLIST_DROP_VERBOSE=1 → altes
    Per-Drop-Log-Verhalten (für Forensik in Einzel-Diagnosen)."""
    _watchlist_drop_counter["count"] += 1
    n = _watchlist_drop_counter["count"]
    dir_lbl = direction.upper() if direction else "?"
    if WATCHLIST_DROP_VERBOSE:
        log(f"  ⏭ {signal_type} ignoriert: {reason} ({symbol} {dir_lbl})")
    elif WATCHLIST_DROP_SUMMARY_EVERY_N > 0 and n % WATCHLIST_DROP_SUMMARY_EVERY_N == 0:
        log(f"  🔇 Watchlist-Master: {n} Drops bisher (zuletzt: {signal_type} "
            f"{symbol} {dir_lbl} — {reason})")

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

# v4.32 Phantom-Close-Guard: merkt sich kürzlich geschlossene Trades inkl.
# ihres letzten State-Snapshots. Wenn innerhalb des TTL-Fensters eine neue
# Position mit identischem Entry erkannt wird, ist das kein "neuer Trade",
# sondern der Phantom-Re-Open nach einem einzelnen stale Bitget-Tick.
# Schema:
#   {symbol: {
#       "ts_close":   float  (time.time()),
#       "entry":      float,
#       "direction":  str,
#       "leverage":   int,
#       "peak_size":  float,
#       "sl":         float,
#       "trailing_level": int,
#       "sl_at_entry":    bool,
#       "trade_data":     dict  (Kopie des trade_data[symbol] zum Zeitpunkt des Close),
#   }}
recent_closes: dict = {}
PHANTOM_REOPEN_TTL_SEC   = 120   # Zeitfenster für Phantom-Reopen-Detektion
PHANTOM_ENTRY_TOLERANCE  = 0.001  # 0.1 % — Bitget liefert bei Phantom-Ticks bit-identischen Entry

# H4 Trigger-Puffer: sammelt Alerts, sendet gebündelt nach Zeitfenster
h4_buffer:     list = []
h4_buffer_lock = __import__("threading").Lock()
H4_BUFFER_SEC  = _env_int("H4_BUFFER_SEC", 300)  # 5 Min

trailing_sl_level: dict = {}  # {symbol: int} — 0=initial, 1=Entry, 2=TP1-Preis, 3=TP2-Preis

# Daily P&L Report — Aufzeichnung abgeschlossener Trades
closed_trades: list = []
daily_report_sent_date: str = ""   # "2026-04-17" — verhindert Doppelversand pro Tag

# Harsi-Ausstiegslinie
harsi_sl: dict = {}

# v4.12: Sling-SL (Swing-Pivot-basiert) — letzter gesetzter Sling-SL pro Symbol
# {symbol: float}  — Short: Sling-High  |  Long: Sling-Low
sling_sl: dict = {}

# v4.12: DCA Auto-Void — markiert welche DCAs nach SL-Nachzug bereits
# storniert wurden, damit sie nicht erneut zu stornieren versucht werden.
# {symbol: {"dca1": bool, "dca2": bool}}
dca_void: dict = {}

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
EXTREME_COOLDOWN_H = _env_int("EXTREME_COOLDOWN_H", 4)

macro_extreme: dict = {
    "btc":    {"state": 0, "until_ts": 0.0},
    "total2": {"state": 0, "until_ts": 0.0},
}

# v4.19: Entry-Queue State — transient (nicht persistiert, max. 90s Lebensdauer)
pending_entries:      dict = {}           # {sig_key: entry_info_dict}
pending_entries_lock       = threading.Lock()
_entry_flush_timer         = None         # threading.Timer oder None
_entry_flush_started_ts    = 0.0          # time.time() beim Start des aktuellen Fensters

# v4.25: Slot-States — Button-Driven Entry-Rangliste (transient, 2h TTL)
# Nach flush_entries() wird EINE Übersichts-Message gesendet. Deren message_id
# dient als Slot-Key. Callbacks (detail:/close/more/calc:) mutieren diesen
# State und edit_message_text() rendert die Nachricht neu.
# Struktur: {slot_msg_id (int): {
#     "ranked":         list[entry_dict],     # sortiert nach Score absteigend
#     "balance":        float,
#     "slot_label":     str,                   # "16:00-Slot"
#     "current_detail": str | None,            # aktuell aufgeklappter Symbol oder None
#     "view_mode":      "overview"|"detail"|"more",
#     "created_ts":     float,
# }}
_slot_states: dict = {}
_slot_states_lock  = threading.Lock()
_SLOT_TTL_SEC      = 2 * 3600  # 2h — danach Callbacks mit "Slot abgelaufen"-Toast

# Top-N Coins mit Detail-Button (Score-absteigend). Rest geht in "Weitere"-Block.
SLOT_TOP_N         = _env_int("SLOT_TOP_N", 10)
# Score-Schwelle unter der Signale in den Low-Score-Block wandern.
SLOT_LOWSCORE_CUT  = _env_int("SLOT_LOWSCORE_CUT", 50)

# v4.19: Win-Rate Cache pro Symbol — {symbol: (wr, n_trades, expire_ts)}
_winrate_cache: dict = {}
_WINRATE_CACHE_TTL   = 3600               # 1h TTL

# v4.20: Entry-Log Lock — schützt CSV-Read-Modify-Write Operationen
_entry_log_lock = threading.Lock()
# Maximales Alter (Sekunden) für mark_trade_taken() Matching eines
# Queue-Eintrags zur tatsächlich eröffneten Position. User kann nach einem
# Signal bis zu ~60 Min brauchen um /trade auszuführen — darüber hinaus
# wird das Signal als "nicht genommen" gewertet.
_ENTRY_MATCH_WINDOW_SEC = 60 * 60


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
            dt       = datetime.fromtimestamp(ts, timezone.utc)
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
            dt       = datetime.fromtimestamp(ts, timezone.utc)
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


def telegram(msg: str, reply_markup: dict = None, return_id: bool = False):
    """
    Sendet Telegram-Nachricht wenn konfiguriert.

    reply_markup: optional inline keyboard (dict mit {"inline_keyboard": [[...]]})
                  — z.B. Ergebnis von build_setup_buttons(symbol).
    return_id:    v4.25 — wenn True, wird die message_id der gesendeten Nachricht
                  zurückgegeben (oder None bei Fehler). Default False behält
                  Backward-Compat mit allen bestehenden Aufrufern.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return None if return_id else None
    try:
        payload = {
            "chat_id":                  TELEGRAM_CHAT_ID,
            "text":                     msg,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload,
            timeout=5
        )
        if return_id:
            try:
                data = r.json()
                if data.get("ok"):
                    return data["result"]["message_id"]
            except Exception:
                pass
            return None
    except Exception:
        if return_id:
            return None
    return None if return_id else None


def telegram_edit_message(
    msg_id: int,
    text: str,
    reply_markup: dict = None,
) -> bool:
    """v4.25 — editMessageText für Button-Driven Entry-Rangliste.
    Returns True bei Erfolg, False sonst. Silent-Fail bei Netz-Problemen."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not msg_id:
        return False
    try:
        payload = {
            "chat_id":                  TELEGRAM_CHAT_ID,
            "message_id":               msg_id,
            "text":                     text,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText",
            json=payload,
            timeout=5
        )
        try:
            data = r.json()
            if data.get("ok"):
                return True
            # "message is not modified" ist kein echter Fehler (Callback hat
            # gar nichts verändert, z.B. Tap auf bereits geöffneten Coin)
            desc = (data.get("description") or "").lower()
            if "not modified" in desc:
                return True
        except Exception:
            pass
    except Exception:
        pass
    return False


def telegram_answer_callback(
    callback_id: str,
    text: str = "",
    show_alert: bool = False,
) -> None:
    """v4.25 — answerCallbackQuery: bestätigt den Tap-Event und zeigt
    optional einen Toast/Alert (z.B. 'Slot abgelaufen'). MUSS innerhalb
    von 15 Sekunden nach dem Tap ausgeführt werden, sonst zeigt Telegram
    eine lästige 'Konnte Callback nicht verarbeiten'-Meldung."""
    if not TELEGRAM_TOKEN or not callback_id:
        return
    try:
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"]       = text
            payload["show_alert"] = bool(show_alert)
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
            json=payload,
            timeout=5
        )
    except Exception:
        pass


def telegram_document(file_path: str, caption: str = "",
                      filename: str = None) -> bool:
    """v4.34 — Schickt eine Datei per sendDocument an TELEGRAM_CHAT_ID.
    Wird für /trades (trades.csv → Chat) verwendet. Rückgabe True bei
    Erfolg, False sonst (inkl. "Datei nicht vorhanden")."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    if not os.path.isfile(file_path):
        return False
    fname = filename or os.path.basename(file_path)
    try:
        with open(file_path, "rb") as fh:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                data={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "caption":    caption or "",
                    "parse_mode": "HTML",
                },
                files={"document": (fname, fh, "text/csv")},
                timeout=30,
            )
        try:
            return bool(r.json().get("ok"))
        except Exception:
            return False
    except Exception as ex:
        log(f"[telegram_document] Upload fehlgeschlagen: {ex}")
        return False


def sign(timestamp: str, method: str, path: str, body: str = "") -> str:
    msg = timestamp + method.upper() + path + body
    sig = hmac.new(SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()


def make_headers(method: str, path: str, body: str = "") -> dict:
    ts = str(int(time.time() * 1000))
    h = {
        "ACCESS-KEY":        API_KEY,
        "ACCESS-SIGN":       sign(ts, method, path, body),
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "ACCESS-TIMESTAMP":  ts,
        "Content-Type":      "application/json",
        "locale":            "en-US",
    }
    # Demo-Modus: paperId=1 schaltet Bitget auf Demo-Account um
    if DEMO_MODE:
        h["paperId"] = "1"
    return h


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

# v4.13 — TTL-Cache für Bitget-Read-API
# Ziel: Webhook-Bursts (z.B. 12 Watchlist-Alarme zur selben Sekunde) treffen
# Bitget nur einmal. TradingView-Timeouts werden dadurch eliminiert.
# WICHTIG: Nur für Read-Endpoints. Write-Operationen (Order-Placement)
# invalidieren die betroffenen Caches via cache_invalidate().
_API_CACHE: dict = {}          # key -> (timestamp, value)
_API_CACHE_LOCK = threading.Lock()

# TTL-Werte pro Endpoint — konservativ klein, damit SL-Updater & Monitor
# keine veralteten Positionsgrössen sehen.
CACHE_TTL_MARK_PRICE = 3.0     # Sekunden
CACHE_TTL_BALANCE    = 10.0
CACHE_TTL_POSITIONS  = 5.0


def _cached_read(key: str, ttl: float, fn, *args, **kwargs):
    """Führt fn(*args, **kwargs) aus, cached den Rückgabewert für ttl Sekunden.

    - Cache-Hit: Rückgabe ohne Bitget-Call.
    - Cache-Miss: Bitget-Call; Ergebnis wird nur gecached wenn "plausibel"
      (positive Balance / nicht-leere Position / positive MarkPrice).
      Fehler-Antworten (0, [], None) werden NICHT gecached — sonst
      würde ein einzelner API-Ausfall alle Folge-Calls kaputt machen.
    """
    now = time.time()
    with _API_CACHE_LOCK:
        entry = _API_CACHE.get(key)
        if entry and (now - entry[0]) < ttl:
            return entry[1]
    # Lock bewusst freigegeben — paralleler Bitget-Call ist OK,
    # der erste Rückläufer füllt den Cache und die anderen schreiben hinein
    # (idempotent, da Inhalt ~gleich ist).
    value = fn(*args, **kwargs)
    if _cache_is_valid(key, value):
        with _API_CACHE_LOCK:
            _API_CACHE[key] = (time.time(), value)
    return value


def _cache_is_valid(key: str, value) -> bool:
    """Entscheidet ob ein Rückgabewert cache-würdig ist."""
    if key.startswith("mark_price:"):
        return isinstance(value, (int, float)) and value > 0
    if key == "futures_balance":
        return isinstance(value, (int, float)) and value > 0
    if key == "all_positions":
        # Auch leere Liste ist gültig — "keine offenen Positionen" ist
        # ein legitimer Cache-Zustand, damit /status nicht pro Aufruf hämmert.
        return isinstance(value, list)
    return False


def cache_invalidate(*keys: str) -> None:
    """Entfernt genannte Keys (oder bei keinem Argument den ganzen Cache).

    Aufrufen nach jeder Order-Platzierung / SL-Anpassung / Position-
    Schliessung, damit Folge-Reads frische Daten sehen.
    """
    with _API_CACHE_LOCK:
        if not keys:
            _API_CACHE.clear()
            return
        for k in keys:
            _API_CACHE.pop(k, None)


def _get_mark_price_raw(symbol: str) -> float:
    """Aktuellen Mark Price von Bitget (ungecacht)."""
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


def get_mark_price(symbol: str) -> float:
    """Aktuellen Mark Price von Bitget (mit 3s-Cache)."""
    return _cached_read(
        f"mark_price:{symbol}",
        CACHE_TTL_MARK_PRICE,
        _get_mark_price_raw,
        symbol,
    )


def _get_futures_balance_raw() -> float:
    """Verfügbares USDT-Guthaben im Futures-Konto (ungecacht)."""
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


def get_futures_balance() -> float:
    """Verfügbares USDT-Guthaben im Futures-Konto (mit 10s-Cache)."""
    return _cached_read(
        "futures_balance",
        CACHE_TTL_BALANCE,
        _get_futures_balance_raw,
    )


# ═══════════════════════════════════════════════════════════════
# POSITIONEN & FILLS
# ═══════════════════════════════════════════════════════════════

def _get_all_positions_raw() -> list:
    """Alle offenen Futures-Positionen (ungecacht)."""
    result = api_get("/api/v2/mix/position/all-position", {
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") != "00000":
        return []
    return [p for p in (result.get("data") or [])
            if float(p.get("total", 0)) > 0]


def get_all_positions() -> list:
    """Alle offenen Futures-Positionen (mit 5s-Cache)."""
    return _cached_read(
        "all_positions",
        CACHE_TTL_POSITIONS,
        _get_all_positions_raw,
    )


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
    Liest alle offenen Plan-Orders (TPs, SL, Trigger) für ein Symbol.

    Bitget v2 /api/v2/mix/order/orders-plan-pending verlangt den Filter
    `planType` als Pflichtparameter (sonst "Parameter verification failed").
    Wir queryen jeden relevanten planType einzeln und mergen die Ergebnisse
    — so deckt die Funktion alle Order-Quellen ab:

      profit_plan / loss_plan   — TP/SL via place-tpsl-order (Einzel-TPs)
      pos_profit  / pos_loss    — TP4/SL via place-pos-tpsl (Positions-TPSL)
      normal_plan               — Standard-Trigger-Orders (Entry/Exit)
      track_plan  / moving_plan — Trailing / Moving Stops

    Bitget akzeptiert den kombinierten Filter `profit_loss`, der alle
    TP+SL-Varianten auf einen Schlag liefert — das wird zuerst versucht.
    """
    def _parse(raw) -> list:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("entrustedList", "planList", "orderList", "data"):
                lst = raw.get(key)
                if isinstance(lst, list):
                    return lst
            # leeres dict / kein bekanntes Feld → leere Liste
            return []
        return []

    def _dedupe_add(target: dict, orders: list) -> None:
        for o in orders:
            oid = o.get("orderId") or o.get("clientOid")
            if oid:
                target[oid] = o

    # planType-Varianten, die wir zusammentragen (in dieser Reihenfolge):
    # "profit_loss" liefert TP+SL in einem Call (decken profit_plan, loss_plan,
    # pos_profit, pos_loss ab). Die restlichen Typen gehören zu Triggern und
    # Trailing-Stops — separat abgefragt, damit wir nichts verpassen.
    plan_types = ["profit_loss", "normal_plan", "track_plan", "moving_plan"]

    collected: dict = {}
    errors: list = []
    any_success = False

    for pt in plan_types:
        r = api_get("/api/v2/mix/order/orders-plan-pending", {
            "productType": PRODUCT_TYPE,
            "symbol":      symbol,
            "planType":    pt,
        })
        if r.get("code") == "00000":
            any_success = True
            _dedupe_add(collected, _parse(r.get("data")))
        else:
            errors.append(f"{pt}={r.get('msg','?')}")

    # Fallback: wenn ALLE Queries mit symbol-Filter fehlschlugen (z.B. weil
    # Bitget an dem Symbol gerade nichts findet und 40XX zurückgibt), einmal
    # ohne symbol-Filter und dann clientside filtern.
    if not any_success:
        r_any = api_get("/api/v2/mix/order/orders-plan-pending", {
            "productType": PRODUCT_TYPE,
            "planType":    "profit_loss",
        })
        if r_any.get("code") == "00000":
            any_success = True
            filtered = [o for o in _parse(r_any.get("data"))
                        if o.get("symbol") == symbol]
            _dedupe_add(collected, filtered)
        else:
            errors.append(f"no-symbol/profit_loss={r_any.get('msg','?')}")

    if not any_success:
        log(f"  [WARN] Alle Plan-Order-Queries für {symbol} fehlgeschlagen: "
            f"{' | '.join(errors)}")

    return list(collected.values())


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
    Storniert alle Einzel-TP-Orders (profit_plan) für ein Symbol.

    v4.15 — Einzelstornierung mit explizitem planType
    ─────────────────────────────────────────────────
    Bitget v2 `/api/v2/mix/order/cancel-plan-order` akzeptiert zwar laut Doku
    einen `orderIdList`-Batch-Mode, aber für TP/SL-Orders (profit_plan) gibt
    Bitget in diesem Modus `code=00000` mit LEEREM successList+failureList
    zurück — die Orders werden tatsächlich NICHT storniert (stilles No-Op).
    Die python-bitget Referenz-Implementierung (`mix_cancel_plan_order`)
    nutzt deshalb die Einzel-Form MIT `planType` — das funktioniert für
    profit_plan zuverlässig.

    Ablauf:
      1. orders-plan-pending lesen (planType=profit_loss liefert TPs+SL)
      2. Nur profit_plan-Einträge auswählen (SL/TP4 nicht anfassen!)
      3. Pro Order: POST cancel-plan-order mit {orderId, planType, …}
      4. Jede Response einzeln prüfen — echte Fehler werden geloggt,
         der Gesamt-Status wird am Ende zusammengefasst.

    SL (loss_plan, pos_loss) und TP4 (pos_profit via place-pos-tpsl)
    werden NIE angefasst — diese werden per place-pos-tpsl überschrieben.
    """
    orders = _get_plan_orders(symbol)
    tp_orders = [o for o in orders if o.get("planType") == "profit_plan"]

    if not tp_orders:
        log(f"  Keine TP-Orders (profit_plan) gefunden")
        return

    log(f"  {len(tp_orders)} TP(s) einzeln stornieren (planType=profit_plan)...")

    ok = 0
    errs = 0
    for order in tp_orders:
        oid = order.get("orderId") or ""
        coid = order.get("clientOid") or ""
        price = order.get("triggerPrice", "?")
        pt = order.get("planType") or "profit_plan"

        body = {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "planType":    pt,
        }
        if oid:
            body["orderId"] = oid
        elif coid:
            body["clientOid"] = coid
        else:
            log(f"    ⏭ Order ohne ID übersprungen @ {price}")
            continue

        res = api_post("/api/v2/mix/order/cancel-plan-order", body)
        code = res.get("code")

        if code == "00000":
            log(f"    ✓ TP storniert @ {price}: {oid or coid}")
            ok += 1
        else:
            log(f"    ✗ TP @ {price} ({oid or coid}): "
                f"code={code} msg={res.get('msg','?')}")
            errs += 1

    log(f"  → {ok}/{len(tp_orders)} TPs storniert, {errs} Fehler")


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
            "stopSurplusTriggerPrice": tp4_str,
            "stopSurplusTriggerType":  "mark_price",
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
            # v4.16 — TP4-Preis im lokalen State speichern (Quelle 1 für
            # _get_pos_tp_price). Garantiert, dass folgende SL-Updates den
            # TP4 mitführen können, auch wenn Bitget-Read-API hängt.
            _td = trade_data.setdefault(symbol, {})
            _td["tp4"] = float(tp4_str)
            save_state()
            cache_invalidate()
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
                    "stopSurplusTriggerPrice": tp4_str2,
                    "stopSurplusTriggerType":  "mark_price",
                }
                if current_sl > 0:
                    body4b["stopLossTriggerPrice"] = sl_for_tp4
                    body4b["stopLossTriggerType"]  = "mark_price"
                res4b = api_post("/api/v2/mix/order/place-pos-tpsl", body4b)
                if res4b.get("code") == "00000":
                    log(f"    ✓ TP4 Full Close @ {tp4_str2} USDT [retry OK]")
                    count += 1
                    prices.append(f"TP4 Full Close: {tp4_str2}")
                    # v4.16 — auch im Retry-Pfad State speichern
                    _td = trade_data.setdefault(symbol, {})
                    _td["tp4"] = float(tp4_str2)
                    save_state()
                    cache_invalidate()

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

    # Haltedauer = Zeit zwischen Trade-Öffnung (since_ms) und letztem Close-Fill.
    # Bitget liefert cTime (ms) pro Fill. Falls nicht vorhanden, bleibt hold_time=0
    # und der Report zeigt "?" (altes Fallback-Verhalten).
    close_times = []
    for f in close_fills:
        ct = f.get("cTime") or f.get("ctime") or 0
        try:
            ct_int = int(ct)
        except (TypeError, ValueError):
            ct_int = 0
        if ct_int > 0:
            close_times.append(ct_int)
    last_close_ts = max(close_times) if close_times else 0
    hold_time_ms  = (last_close_ts - since_ms) if (last_close_ts and since_ms and last_close_ts > since_ms) else 0

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
        "hold_time":    hold_time_ms,
    }


# ═══════════════════════════════════════════════════════════════
# POSITION GESCHLOSSEN ERKENNEN
# ═══════════════════════════════════════════════════════════════

def _save_demo_trade(symbol: str, td: dict, pnl_data: dict):
    """Speichert abgeschlossenen Demo-Trade in demo_trades.json."""
    import json as _json
    fname = "demo_trades.json"
    try:
        with open(fname, "r") as f:
            trades = _json.load(f)
    except Exception:
        trades = []

    import datetime as _dt
    now = _dt.datetime.now()
    entry_price = float(td.get("entry", 0) or 0)
    sl_price    = float(td.get("sl", 0) or 0)
    lev         = int(td.get("leverage", 10) or 10)
    sl_dist_pct = abs(entry_price - sl_price) / entry_price * 100 if entry_price else 0

    # Score aus trade_data lesen (wurde beim Entry-Queue gespeichert)
    score_data  = td.get("score_data") or {}
    score       = int(score_data.get("score", 0))
    is_premium  = bool(score_data.get("is_premium", False))
    breakdown   = score_data.get("breakdown", [])
    score_range = (
        "A 75-100" if score >= 75 else
        "B 50-74"  if score >= 50 else
        "C 25-49"  if score >= 25 else
        "D 0-24"
    )

    net_pnl    = float(pnl_data.get("net_pnl", 0) or 0)
    tp_closes  = pnl_data.get("tp_closes", []) or []
    num_closes = int(pnl_data.get("num_closes", 0) or 0)

    # TP-Hit Analyse
    tp1_hit = num_closes >= 2
    tp2_hit = num_closes >= 3
    tp3_hit = num_closes >= 4
    tp4_hit = num_closes >= 4 and net_pnl > 0

    trades.append({
        # Identifikation
        "symbol":      symbol,
        "direction":   td.get("direction", "?"),
        "leverage":    lev,
        "entry":       entry_price,
        "sl":          sl_price,
        "sl_dist_pct": round(sl_dist_pct, 3),
        # Zeitstempel
        "open_dt":     td.get("open_dt", ""),
        "close_dt":    now.isoformat(),
        "weekday":     now.strftime("%A"),  # Monday, Tuesday, ...
        "hour":        now.hour,
        # P&L
        "net_pnl":     round(net_pnl, 4),
        "realized":    round(float(pnl_data.get("realized_pnl", 0) or 0), 4),
        "fee":         round(float(pnl_data.get("fee", 0) or 0), 4),
        "won":         net_pnl > 0,
        # TP-Analyse
        "num_closes":  num_closes,
        "tp1_hit":     tp1_hit,
        "tp2_hit":     tp2_hit,
        "tp3_hit":     tp3_hit,
        "tp4_hit":     tp4_hit,
        # Score
        "score":       score,
        "score_range": score_range,
        "is_premium":  is_premium,
        "score_breakdown": breakdown,
        # DCA
        "peak_size":   float(td.get("peak_size", 0) or 0),
        "dca_used":    float(td.get("peak_size", 0) or 0) > float(td.get("init_size", 0) or 0),
    })

    try:
        with open(fname, "w") as f:
            _json.dump(trades, f, indent=2)
        log(f"  Demo-Trade gespeichert ({len(trades)} total)")
    except Exception as e:
        log(f"  ⚠ Demo-Trade Speichern fehlgeschlagen: {e}")


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

    telegram("\n".join(msg_lines), reply_markup=build_setup_buttons(symbol))

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

    # Dedup: Nach Railway-Restart wird handle_position_closed() manchmal
    # mehrfach für denselben Trade ausgelöst. Wenn bereits ein Record mit
    # identischem (Symbol, peak_size, net_pnl, entry) innerhalb der letzten
    # 2 h existiert, ist es dieselbe Schliessung → Eintrag überspringen.
    _dedup_window = 2 * 3600  # Sekunden
    _now_ts = int(time.time())
    _duplicate = any(
        r.get("symbol") == symbol
        and abs(float(r.get("peak_size", 0) or 0) - float(peak_size or 0)) < 1e-9
        and round(float(r.get("net_pnl", 0) or 0), 4) == round(float(net_pnl or 0), 4)
        and abs(float(r.get("entry", 0) or 0) - float(entry or 0)) < 1e-12
        and (_now_ts - int(r.get("ts", 0))) < _dedup_window
        for r in closed_trades
    )
    if _duplicate:
        log(f"  ⚠ Doppelter Close erkannt ({symbol}, {net_pnl:+.2f} USDT) — Eintrag übersprungen")
    else:
        closed_trades.append(_trade_record)
        csv_log_trade(_trade_record)     # → Railway Volume CSV (non-blocking, immer aktiv)
        sheets_log_trade(_trade_record)  # → Google Sheets (non-blocking, optional)
    # v4.20: Queue-Log Outcome-Annotation — bindet R-Multiple, Duration etc.
    # an die ursprüngliche Queue-Entscheidung (falls vorhanden). Kein Eintrag
    # im Queue-Log → stillschweigendes No-Op (manuelle Trades ohne Signal).
    try:
        update_entry_log_outcome(
            symbol        = symbol,
            direction     = direction,
            exit_price    = close_px or 0,
            pnl_usdt      = net_pnl,
            ts_close      = time.time(),
            won           = won,
            close_reason  = (reason or "unknown").strip() or "unknown",
        )
    except Exception as ex:
        log(f"[update_entry_log_outcome] {ex}")
    # v4.32 Phantom-Close-Guard: kompletten State-Snapshot vor dem Reset
    # wegspeichern. Wird in setup_new_trade() als "wurde dieser Entry gerade
    # eben erst geschlossen?"-Marker gelesen. Bei echtem Re-Open mit neuem
    # Entry läuft der Normal-Pfad; bei Phantom (identischer Entry innerhalb
    # PHANTOM_REOPEN_TTL_SEC) wird der State zurückgerollt statt neu gesetzt.
    try:
        recent_closes[symbol] = {
            "ts_close":        time.time(),
            "entry":           entry,
            "direction":       direction,
            "leverage":        leverage,
            "peak_size":       peak_size,
            "sl":              sl_price,
            "trailing_level":  trailing_sl_level.get(symbol, 0),
            "sl_at_entry":     sl_at_entry.get(symbol, False),
            "trade_data":      dict(trade_data.get(symbol, {}) or {}),
        }
    except Exception as ex:
        log(f"[recent_closes snapshot] {ex}")

    save_state()

    # Internen Status zurücksetzen
    last_known_avg.pop(symbol, None)
    last_known_size.pop(symbol, None)
    new_trade_done.pop(symbol, None)
    sl_at_entry.pop(symbol, None)
    harsi_sl.pop(symbol, None)
    # v4.12: Sling-SL + DCA Auto-Void State für geschlossenen Trade löschen
    sling_sl.pop(symbol, None)
    dca_void.pop(symbol, None)
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
    Liest den TP4-Preis für eine Position.

    v4.16 — drei Quellen in Reihenfolge:
      Quelle 1: trade_data[symbol]["tp4"] (lokaler State, verlässlich)
      Quelle 2: takeProfitPrice etc. aus single-position (meist nicht populated)
      Quelle 3: pos_profit aus orders-plan-pending (unzuverlässig)

    Hintergrund: Bitget `place-pos-tpsl` speichert zwar den SL im
    single-position-Response (Feld `stopLoss`), aber KEINEN TP. Fällt
    Quelle 3 gleichzeitig aus (Plan-Order-Endpoints scheitern regelmässig
    mit "Parameter verification failed"), wurde TP4 fälschlich als "nicht
    vorhanden" gewertet — und bei SL-Updates dann gelöscht (weil
    place-pos-tpsl SL UND TP kombiniert verwaltet).
    """
    # Quelle 1: Lokaler State (wird in place_tp_orders() gesetzt)
    td = trade_data.get(symbol, {})
    if td.get("direction") == direction:
        tp4_state = float(td.get("tp4", 0) or 0)
        if tp4_state > 0:
            return tp4_state

    # Quelle 2: single-position (meistens leer für TP — trotzdem prüfen)
    result = api_get("/api/v2/mix/position/single-position", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
    })
    if result.get("code") == "00000":
        for pos in (result.get("data") or []):
            if pos.get("holdSide") == direction:
                # v4.17 — Bitget gibt den TP teils gar nicht, teils unter
                # diversen Namen zurück. Liste defensiv, alle Varianten prüfen.
                for field in ("presetStopSurplusPrice", "stopSurplusTriggerPrice",
                              "stopSurplusPrice", "takeProfitPrice", "takeProfit",
                              "tpPrice", "takeProfitTriggerPrice"):
                    tp = float(pos.get(field, 0) or 0)
                    if tp > 0:
                        log(f"  TP4 aus Position.{field}: {tp}")
                        return tp

    # Quelle 3: Plan-Orders (pos_profit). Kann fehlschlagen → dann 0.
    try:
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
    except Exception as _e:
        log(f"  _get_pos_tp_price: plan-orders Fehler: {_e}")

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
        body_sl["stopSurplusTriggerPrice"] = round_price(existing_tp4, decimals_sl)
        body_sl["stopSurplusTriggerType"]  = "mark_price"
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
        body_sl["stopSurplusTriggerPrice"] = round_price(existing_tp4, decimals)
        body_sl["stopSurplusTriggerType"]  = "mark_price"
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
        body_sl["stopSurplusTriggerPrice"] = round_price(existing_tp4, decimals)
        body_sl["stopSurplusTriggerType"]  = "mark_price"

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
# v4.12: SLING-SL + DCA AUTO-VOID
# ═══════════════════════════════════════════════════════════════

def _void_passed_dcas(symbol: str, direction: str, new_sl: float) -> list:
    """
    v4.35: Auto-Void: wenn der neue SL einen DCA-Level überschreitet (SL bei LONG
    über dem DCA-Preis bzw. bei SHORT unter dem DCA-Preis), wird die Order
    storniert. DCA-im-Gewinn ist per Definition unmöglich.

    BUG-FIX v4.35: Bisher wurde /api/v2/mix/order/orders-plan-pending abgefragt
    (Plan-Orders: TP/SL/Trigger). DCAs sind aber reguläre LIMIT-Orders die unter
    /api/v2/mix/order/orders-pending liegen. Konsequenz vor v4.35: Funktion
    fand nichts → DCAs blieben unter dem neuen SL aktiv (siehe QNTUSDT
    2026-04-25 08:01 UTC). Jetzt analog zu cancel_open_dca_orders implementiert.

    Rückgabe: Liste der stornierten DCA-Labels (für Telegram-Nachricht).
    """
    voided = []
    try:
        result = api_get("/api/v2/mix/order/orders-pending", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
        })
    except Exception as _e:
        log(f"  [_void_passed_dcas] orders-pending Fehler: {_e}")
        return voided

    if (result or {}).get("code") != "00000":
        log(f"  [_void_passed_dcas] orders-pending Code {result.get('code')}: {result.get('msg', result)}")
        return voided

    data   = result.get("data") or {}
    orders = data.get("entrustedList") or [] if isinstance(data, dict) else (data or [])

    # DCAs = Open-Side LIMIT-Orders in Trade-Richtung (LONG → buy, SHORT → sell)
    side = "buy" if direction == "long" else "sell"
    dca_orders = [
        o for o in orders
        if o.get("side") == side
        and o.get("tradeSide") == "open"
        and o.get("orderType") == "limit"
    ]

    for o in dca_orders:
        try:
            price = float(o.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue

        # Prüfen ob SL diesen DCA überschritten hat
        # LONG: SL über DCA-Preis  →  DCA wäre im Gewinn (unmöglich)
        # SHORT: SL unter DCA-Preis →  DCA wäre im Gewinn (unmöglich)
        overshoots = (direction == "long" and new_sl >= price) or \
                     (direction == "short" and new_sl <= price)
        if not overshoots:
            continue

        order_id = o.get("orderId") or o.get("clientOid") or ""
        if not order_id:
            continue
        res = api_post("/api/v2/mix/order/cancel-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     order_id,
        })
        if res.get("code") == "00000":
            label = f"DCA @ {price}"
            log(f"  ✓ {label} storniert (SL {new_sl} überschreitet DCA-Preis)")
            voided.append(label)
        else:
            log(f"  ✗ DCA-Stornierung fehlgeschlagen ({order_id}): {res.get('msg', res)}")

    if voided:
        dca_void.setdefault(symbol, {})["voided_at"] = time.time()
        dca_void[symbol]["last_voided"] = voided
        save_state()
    return voided


def set_sl_sling(symbol: str, direction: str, pivot_price: float,
                 cur_size: float = 0, atr_val: float = 0.0):
    """
    v4.12 Sling-SL: setzt den SL auf den letzten bestätigten Swing-Pivot.
      Long:   pivot = Sling-Low  → SL = pivot - fallback_buffer (wenn zu nah)
      Short:  pivot = Sling-High → SL = pivot + fallback_buffer (wenn zu nah)

    Fallback-Buffer: max(SLING_PCT_FLOOR%, SLING_ATR_MULT × ATR) — stellt sicher
    dass der SL nicht direkt am Entry liegt (Stop-Hunts vermeiden).

    Nur-protektiv: Der SL wird nur nachgezogen, wenn er NÄHER am aktuellen
    Markt liegt als der bestehende SL. Verhindert Verschlechterung.

    Nach Setzen: Auto-Void für DCA-Orders die im Gewinn liegen würden.
    """
    mark = get_mark_price(symbol)
    if mark <= 0 or pivot_price <= 0:
        log(f"  Sling-SL: ungültige Preise (mark={mark}, pivot={pivot_price}) — skip")
        return

    decimals = get_price_decimals(symbol)

    # Fallback-Puffer berechnen
    pct_buf = mark * (SLING_PCT_FLOOR / 100.0)
    atr_buf = atr_val * SLING_ATR_MULT if atr_val > 0 else 0.0
    min_buf = max(pct_buf, atr_buf)

    # Pivot zu nah am Markt? → Puffer aufschlagen
    if direction == "long":
        # SL unter dem Pivot — aber Puffer zum Markt mindestens min_buf
        candidate = pivot_price
        if mark - candidate < min_buf:
            candidate = mark - min_buf
            log(f"  Sling-SL (LONG): Pivot {pivot_price:.5f} zu nah an Mark {mark:.5f} "
                f"— ATR-Fallback aktiv: SL → {candidate:.5f} (Puffer {min_buf:.5f})")
    else:
        candidate = pivot_price
        if candidate - mark < min_buf:
            candidate = mark + min_buf
            log(f"  Sling-SL (SHORT): Pivot {pivot_price:.5f} zu nah an Mark {mark:.5f} "
                f"— ATR-Fallback aktiv: SL → {candidate:.5f} (Puffer {min_buf:.5f})")

    sl_str = round_price(candidate, decimals)
    new_sl = float(sl_str)

    # ── Nur-protektiv-Check: neuer SL muss näher am Markt sein ──
    current_sl = _get_pos_sl_price(symbol, direction)
    if current_sl > 0:
        if direction == "long" and new_sl <= current_sl:
            log(f"  Sling-SL {new_sl} nicht protektiver als SL {current_sl} — skip")
            return
        if direction == "short" and new_sl >= current_sl:
            log(f"  Sling-SL {new_sl} nicht protektiver als SL {current_sl} — skip")
            return

    # ── Mark-Price-Guard: SL muss auf richtiger Seite liegen ──
    if direction == "long" and new_sl >= mark:
        log(f"  ⚠ Sling-SL (LONG) {new_sl} >= Mark {mark} — Bitget würde ablehnen — skip")
        return
    if direction == "short" and new_sl <= mark:
        log(f"  ⚠ Sling-SL (SHORT) {new_sl} <= Mark {mark} — Bitget würde ablehnen — skip")
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
        body_sl["stopSurplusTriggerPrice"] = round_price(existing_tp4, decimals)
        body_sl["stopSurplusTriggerType"]  = "mark_price"

    result = api_post("/api/v2/mix/order/place-pos-tpsl", body_sl)
    if result.get("code") == "00000":
        sling_sl[symbol]  = new_sl
        sl_set_ts[symbol] = time.time()
        save_state()

        # DCA Auto-Void prüfen (DCAs im Gewinn sind logisch unmöglich)
        voided = _void_passed_dcas(symbol, direction, new_sl)

        td        = trade_data.get(symbol, {})
        entry     = float(td.get("entry", 0))
        base_coin = get_base_coin(symbol)
        qty_dec   = get_qty_decimals(symbol)
        if cur_size > 0 and entry > 0:
            guaranteed = (cur_size * (new_sl - entry) if direction == "long"
                          else cur_size * (entry - new_sl))
            pos_usdt       = cur_size * entry
            size_str       = f"{cur_size:.{qty_dec}f} {base_coin} (≈ {pos_usdt:.2f} USDT)"
            guaranteed_str = (f"+{guaranteed:.2f} USDT" if guaranteed >= 0
                              else f"{guaranteed:.2f} USDT")
        else:
            size_str       = "—"
            guaranteed_str = "—"

        prev_sl_str = f"{current_sl:.5f}" if current_sl > 0 else "—"
        pivot_label = "Sling-Low" if direction == "long" else "Sling-High"
        log(f"  ✓ Sling-SL gesetzt: {sl_str} USDT ({symbol})")

        msg_lines = [
            f"📉 <b>Sling-SL — {symbol}</b>",
            f"{pivot_label}: {pivot_price:.5f}  →  SL: {sl_str} USDT",
            f"Vorher: {prev_sl_str}",
            "━" * 10,
            f"📦 Restposition:     {size_str}",
            f"🛡 Min. Gewinn:      {guaranteed_str}",
            f"  (falls SL greift)",
        ]
        if voided:
            msg_lines += ["", "🧹 <b>DCA Auto-Void:</b>"] + [f"  • {v}" for v in voided]
            msg_lines.append("  (SL überschreitet DCA — DCA-im-Gewinn unmöglich)")
        telegram("\n".join(msg_lines))
    else:
        log(f"  ✗ Sling-SL fehlgeschlagen: {result.get('msg', result)}")
        telegram(f"❌ <b>Sling-SL fehlgeschlagen — {symbol}</b>\nBitte SL manuell prüfen!")


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

    # ── 10%-Kapitalregel: DCA-Grössen deckeln ──────────────────
    # Market + DCA1 + DCA2 dürfen zusammen max. 10% des Kapitals verbrauchen
    if balance > 0 and leverage > 0 and entry > 0:
        total_limit      = balance * 0.10          # max Margin total
        market_margin    = (base_size * entry) / leverage
        remaining        = total_limit - market_margin
        if remaining > 0:
            # Verbleibende Margin 30/50 aufteilen (aus 80% Rest)
            dca1_margin_max  = remaining * (0.30 / 0.80)
            dca2_margin_max  = remaining * (0.50 / 0.80)
            dca1_max = snap_qty(symbol, (dca1_margin_max * leverage) / entry)
            dca2_max = snap_qty(symbol, (dca2_margin_max * leverage) / entry)
            if dca1_max > 0 and dca1_size > dca1_max:
                log(f"  DCA1 gedeckelt: {dca1_size} → {dca1_max} (10%-Regel)")
                dca1_size = dca1_max
            if dca2_max > 0 and dca2_size > dca2_max:
                log(f"  DCA2 gedeckelt: {dca2_size} → {dca2_max} (10%-Regel)")
                dca2_size = dca2_max
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


# ═══════════════════════════════════════════════════════════════
# v4.28 STUFE B — ONE-CLICK-EXECUTION
# ═══════════════════════════════════════════════════════════════
# Zweck: Button 🚀 Trade jetzt in der Detail-Ansicht platziert Market-Order
# + SL direkt auf Bitget — ohne Browser-Umweg. DCA + TPs übernimmt der
# bestehende Main-Loop über setup_new_trade() sobald die Position gesehen wird.
#
# Sicherheits-Architektur (Fail-Closed):
#   1) AUTO_TRADE_ENABLED Gate — Feature kann Env-gesteuert deaktiviert werden
#   2) Two-Tap-Confirmation — erster Tap speichert Signatur, zweiter Tap führt aus
#   3) SL-Side-Validierung — Long SL < Entry, Short SL > Entry
#   4) Hebel-Cap auf MAX_LEVERAGE (default 25x)
#   5) Half-Kelly-Sizing mit 10%-Margin-Limit als Hard-Cap
#   6) Post-Order 3s-Verifikation via _get_all_positions_raw (kein Cache)
#   7) SL-Set-Fehler nach Position-Open → PANIC-Telegram mit Handlungsanweisung
# ═══════════════════════════════════════════════════════════════

def set_leverage_on_bitget(symbol: str, direction: str, leverage: int) -> dict:
    """v4.28 — Setzt den Hebel für ein Symbol auf Bitget.

    Bitget speichert Hebel pro Seite (long/short) getrennt in isolated-Modus.
    Wir setzen nur die relevante Seite. Bei Fehler nur loggen — Bitget
    verwendet dann den Account-Default. Die Funktion ist idempotent.

    Returns: Response-Dict von Bitget (code, msg, data).
    """
    res = api_post("/api/v2/mix/account/set-leverage", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin":  MARGIN_COIN,
        "leverage":    str(int(leverage)),
        "holdSide":    direction,
    })
    if res.get("code") != "00000":
        log(f"  ⚠ set-leverage({symbol} {direction} {leverage}x) "
            f"fehlgeschlagen: {res.get('msg', res)}")
    else:
        log(f"  ✓ Hebel {leverage}x gesetzt ({symbol} {direction})")
    return res


def execute_trade_order(symbol: str, direction: str, leverage: int,
                         entry: float, sl: float) -> dict:
    """v4.28 — Platziert Market-Order + SL für einen Trade-Vorschlag.

    Schritte:
      1. Pre-Validierung (Feature-Gate, SL-Seite, Hebel-Cap, Balance)
      2. Sizing: Half-Kelly-Total → Initial = Total / 5 (20/30/50-Schema).
         Gekappt auf max_margin/3 (10%-Regel) und optional MAX_AUTO_TRADE_USDT.
      3. Hebel auf Bitget setzen (nicht fatal bei Fehler).
      4. Market-Order platzieren (tradeSide=open, orderType=market).
      5. 3s warten, dann _get_all_positions_raw() → verifizieren dass Position
         tatsächlich existiert (bypasst 5s-Cache).
      6. SL via place-pos-tpsl setzen. Bei Fehler → PANIC-Status.

    Returns: {"ok": True|"partial"|False, "reason": str, "orderId": str,
              "qty": str, "leverage": int, "entry": float, "sl": str,
              "initial_margin": float}
    """
    # ── 1a. Feature-Gate ────────────────────────────────────────
    if not AUTO_TRADE_ENABLED:
        return {"ok": False,
                "reason": "AUTO_TRADE_ENABLED=false — Feature ist deaktiviert"}

    direction = direction.lower()
    if direction not in ("long", "short"):
        return {"ok": False, "reason": f"Ungültige Richtung: {direction!r}"}

    # ── 1b. SL-Seite validieren ─────────────────────────────────
    if direction == "long" and sl >= entry:
        return {"ok": False,
                "reason": f"SL {sl} >= Entry {entry} bei LONG — ungültig"}
    if direction == "short" and sl <= entry:
        return {"ok": False,
                "reason": f"SL {sl} <= Entry {entry} bei SHORT — ungültig"}

    # ── 1c. Hebel-Cap ───────────────────────────────────────────
    leverage = int(leverage)
    if leverage < 1:
        return {"ok": False, "reason": f"Hebel {leverage} < 1"}
    if leverage > MAX_LEVERAGE:
        log(f"  ⚠ Hebel {leverage}x > MAX_LEVERAGE {MAX_LEVERAGE}x — gecappt")
        leverage = MAX_LEVERAGE

    # ── 1d. Balance holen ───────────────────────────────────────
    balance = get_futures_balance()
    if balance <= 0:
        return {"ok": False,
                "reason": f"Balance {balance} — Konto leer oder API-Fehler"}

    # ── 2. Sizing: Half-Kelly / 5 (Initial in 20/30/50-Schema) ──
    kelly         = kelly_recommendation(balance, WINRATE)
    target_total  = kelly.get("half_kelly_usdt", 0) or 0
    initial_margin = target_total / 5.0

    # Hard-Cap 1: 10%-Margin-Regel (max_margin / 3 pro Order)
    max_margin = balance * 0.10
    cap_10pct  = max_margin / 3.0
    if initial_margin > cap_10pct:
        log(f"  Initial-Margin {initial_margin:.2f} USDT > 10%/3-Cap "
            f"{cap_10pct:.2f} — capped")
        initial_margin = cap_10pct

    # Hard-Cap 2: MAX_AUTO_TRADE_USDT (optional)
    if MAX_AUTO_TRADE_USDT > 0 and initial_margin > MAX_AUTO_TRADE_USDT:
        log(f"  Initial-Margin {initial_margin:.2f} > "
            f"MAX_AUTO_TRADE_USDT {MAX_AUTO_TRADE_USDT:.2f} — capped")
        initial_margin = MAX_AUTO_TRADE_USDT

    if initial_margin < 1.0:
        return {"ok": False,
                "reason": f"Initial-Margin {initial_margin:.2f} < 1 USDT — "
                          f"Half-Kelly liefert zu wenig (Balance {balance:.2f}, "
                          f"Winrate {WINRATE})"}

    # Kontrakt-Grösse (Quantität in Base-Coin)
    contracts_raw = (initial_margin * leverage) / entry
    qty           = snap_qty(symbol, contracts_raw)
    if qty <= 0:
        return {"ok": False,
                "reason": f"Qty {qty} nach snap_qty (raw {contracts_raw:.6f}) "
                          f"— zu klein für Symbol-Precision"}
    qty_str = round_qty(symbol, qty)
    notional = qty * entry

    log(f"══ AUTO-TRADE START: {symbol} ══")
    log(f"  {direction.upper()} | Entry={entry} | SL={sl} | Hebel={leverage}x")
    log(f"  Initial-Margin: {initial_margin:.2f} USDT | Qty: {qty_str} | "
        f"Notional: {notional:.2f} USDT")

    # ── 3. Hebel auf Bitget setzen ──────────────────────────────
    set_leverage_on_bitget(symbol, direction, leverage)

    # ── 4. Market-Order platzieren ──────────────────────────────
    side = "buy" if direction == "long" else "sell"
    ord_res = api_post("/api/v2/mix/order/place-order", {
        "symbol":      symbol,
        "productType": PRODUCT_TYPE,
        "marginMode":  "isolated",
        "marginCoin":  MARGIN_COIN,
        "size":        qty_str,
        "side":        side,
        "tradeSide":   "open",
        "orderType":   "market",
        "force":       "gtc",
    })
    if ord_res.get("code") != "00000":
        err = ord_res.get("msg", str(ord_res))
        log(f"  ✗ Market-Order Fehler: {err}")
        return {"ok": False, "reason": f"Market-Order rejected: {err}"}

    order_id = (ord_res.get("data") or {}).get("orderId") or "?"
    log(f"  ✓ Market-Order platziert — orderId={order_id} | "
        f"Qty={qty_str} | Side={side}")

    # ── 5. 3s Verifikation — Position muss sichtbar sein ───────
    time.sleep(3)
    try:
        positions = _get_all_positions_raw()   # bypasst Cache
    except Exception as ex:
        positions = []
        log(f"  ⚠ Position-Read nach Order fehlgeschlagen: {ex}")

    pos_match = next(
        (p for p in positions
         if p.get("symbol") == symbol
         and p.get("holdSide") == direction
         and float(p.get("total", 0)) > 0),
        None,
    )
    if not pos_match:
        log(f"  ✗ PANIC: Position nach 3s nicht sichtbar — "
            f"Order evtl. rejected oder Fill-Race")
        return {"ok": False,
                "reason": f"Position nicht verifiziert nach 3s — orderId "
                          f"{order_id}, Qty {qty_str} — manuell bei Bitget prüfen!",
                "orderId": order_id}

    filled_qty = float(pos_match.get("total", 0))
    avg_price  = float(pos_match.get("openPriceAvg", entry))
    log(f"  ✓ Position verifiziert: Qty={filled_qty} @ Avg={avg_price}")

    # ── 6. SL setzen ────────────────────────────────────────────
    decimals = get_price_decimals(symbol)
    sl_str   = round_price(sl, decimals)
    sl_res   = api_post("/api/v2/mix/order/place-pos-tpsl", {
        "symbol":               symbol,
        "productType":          PRODUCT_TYPE,
        "marginCoin":           MARGIN_COIN,
        "holdSide":             direction,
        "stopLossTriggerPrice": sl_str,
        "stopLossTriggerType":  "mark_price",
    })
    if sl_res.get("code") != "00000":
        err = sl_res.get("msg", str(sl_res))
        log(f"  ✗ PANIC: SL-Set fehlgeschlagen — Position offen ohne SL! {err}")
        try:
            telegram(
                f"🚨 <b>PANIC — {symbol}</b>\n"
                f"Market-Order wurde platziert, aber SL-Set "
                f"<b>fehlgeschlagen</b>!\n\n"
                f"Position: {direction.upper()} {filled_qty} @ {avg_price}\n"
                f"Geplanter SL: {sl_str} USDT\n"
                f"Fehler: {err}\n\n"
                f"⚠️ <b>SL jetzt manuell auf Bitget setzen!</b>",
                reply_markup=build_setup_buttons(symbol),
            )
        except Exception:
            pass
        return {"ok": "partial",
                "reason": f"Position offen, SL-Set-Fehler: {err}",
                "orderId": order_id, "qty": qty_str,
                "leverage": leverage, "entry": avg_price}

    log(f"  ✓ SL @ {sl_str} gesetzt — Main-Loop übernimmt DCA+TPs")
    return {"ok": True,
            "orderId":        order_id,
            "qty":            qty_str,
            "leverage":       leverage,
            "entry":          avg_price,
            "sl":             sl_str,
            "initial_margin": round(initial_margin, 2),
            "notional":       round(notional, 2)}


# ═══════════════════════════════════════════════════════════════
# v4.28 — Two-Tap-Confirm State (in-memory, 10s TTL)
# ═══════════════════════════════════════════════════════════════
# Key: (msg_id, payload_sig) — Wert: expires_ts (float Unix)
# Erster Tap eines "exec:"-Buttons legt (msg_id, sig) an mit expires = now + TTL.
# Zweiter Tap innerhalb TTL führt aus und cleart den Eintrag.
# Abgelaufene Einträge werden beim nächsten Zugriff gecleant (lazy).
_exec_confirm: dict = {}
_exec_confirm_lock = threading.Lock()


def _exec_confirm_check_and_consume(msg_id: int, payload_sig: str) -> bool:
    """Prüft ob derselbe payload_sig bereits im Fenster liegt.
    True → zweiter Tap, ausführen + State cleanen.
    False → erster Tap oder abgelaufen; State neu setzen."""
    now = time.time()
    key = (int(msg_id), payload_sig)
    with _exec_confirm_lock:
        # Lazy-Cleanup abgelaufener Einträge
        for _k in list(_exec_confirm.keys()):
            if _exec_confirm[_k] < now:
                del _exec_confirm[_k]
        exp = _exec_confirm.get(key)
        if exp is not None and exp >= now:
            # Bestätigung — State cleanen und ausführen
            del _exec_confirm[key]
            return True
        # Erster Tap (oder Re-Tap nach TTL) — State setzen
        _exec_confirm[key] = now + AUTO_TRADE_CONFIRM_TTL_SEC
        return False


def tv_chart_links(symbol: str) -> dict:
    """
    Generiert TradingView Chart-Links mit gespeichertem Layout lX5eDAis
    + direkten Bitget-Trading-Link. Symbol wird automatisch getauscht,
    Timeframe angepasst.
    """
    tv_sym = symbol.upper()
    if not tv_sym.endswith(".P"):
        tv_sym = tv_sym + ".P"

    # Bitget akzeptiert den Plain-USDT-Pair im /futures/usdt/-Pfad.
    bitget_sym = symbol.upper().replace(".P", "")
    if not bitget_sym.endswith("USDT"):
        bitget_sym += "USDT"

    base = "https://www.tradingview.com/chart/lX5eDAis"
    return {
        "coin_h2":  f"{base}/?symbol=BITGET:{tv_sym}&interval=120",
        "coin_h4":  f"{base}/?symbol=BITGET:{tv_sym}&interval=240",
        "btc_h2":   f"{base}/?symbol=BITGET:BTCUSDT.P&interval=120",
        "total2":   f"{base}/?symbol=CRYPTOCAP:TOTAL2&interval=120",
        "bitget":   f"https://www.bitget.com/futures/usdt/{bitget_sym}",
    }


def build_setup_buttons(symbol: str = "") -> dict:
    """
    Baut das Telegram Inline-Keyboard für Trade-Setups und Auto-SL-Meldungen.

    Layout (Wunsch Felix, 2026-04-22):
      Row 1:  🟠 Bitget {COIN}   📊 TV {COIN} H2   ← primäre Aktionen
      Row 2:  📈 BTC H2          🔀 Total2        ← Makro-Kontext

    Der TV-Button öffnet den Coin direkt im gespeicherten DOMINUS-Layout
    (lX5eDAis) auf H2 — Timeframe lässt sich in TradingView mit einem Klick
    auf H4 umschalten. Wenn kein Symbol übergeben wird, fehlt die Coin-Zeile
    (z.B. für Macro-only-Reports).
    """
    links = tv_chart_links(symbol if symbol else "BTCUSDT")
    rows = []
    if symbol:
        base_coin = symbol.upper().replace("USDT", "").replace(".P", "")
        rows.append([
            {"text": f"🟠 Bitget {base_coin}",  "url": links["bitget"]},
            {"text": f"📊 TV {base_coin} H2",   "url": links["coin_h2"]},
        ])
    rows.append([
        {"text": "📈 BTC H2",  "url": links["btc_h2"]},
        {"text": "🔀 Total2",  "url": links["total2"]},
    ])
    return {"inline_keyboard": rows}


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

    v4.32 Fix C — Phantom-Reopen-Guard:
    Bevor ein Neu-Setup läuft, wird geprüft, ob innerhalb der letzten
    PHANTOM_REOPEN_TTL_SEC ein Close für dieses Symbol gebucht wurde UND
    der aktuelle Entry bit-identisch (±PHANTOM_ENTRY_TOLERANCE) zum
    gemerkten Entry ist. Trifft das zu, ist es kein neuer Trade, sondern
    ein Phantom-Re-Open: Bitget hatte einen Tick lang die Position
    versehentlich weggelassen, ist jetzt wieder da. In diesem Fall wird
    der State aus recent_closes[sym] zurückgerollt (trailing_sl_level,
    sl_at_entry, trade_data) — keine neuen DCAs, keine neuen TPs, kein
    zerschossener -25 %-SL.
    """
    symbol    = pos.get("symbol", "?")
    direction = pos.get("holdSide", "long")
    entry     = float(pos.get("openPriceAvg", 0))
    size      = float(pos.get("total", 0))
    leverage  = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
    mark      = get_mark_price(symbol)

    # ── v4.32 Phantom-Reopen-Guard ─────────────────────────────────────
    _prev_close = recent_closes.get(symbol)
    if _prev_close and entry > 0:
        try:
            _age = time.time() - float(_prev_close.get("ts_close", 0))
            _prev_entry = float(_prev_close.get("entry", 0) or 0)
            if _prev_entry > 0 and _age < PHANTOM_REOPEN_TTL_SEC:
                _entry_diff = abs(entry - _prev_entry) / _prev_entry
                _same_dir   = (_prev_close.get("direction", "") == direction)
                if _same_dir and _entry_diff <= PHANTOM_ENTRY_TOLERANCE:
                    # Phantom erkannt → State restaurieren, NICHT neu aufsetzen.
                    log(f"══ PHANTOM-REOPEN IGNORIERT: {symbol} ══")
                    log(f"  Close-Alter: {_age:.1f}s | Entry-Diff: {_entry_diff*100:.4f}% "
                        f"(Tol {PHANTOM_ENTRY_TOLERANCE*100:.1f}%) | same_dir={_same_dir}")
                    log(f"  → Rolle State zurück auf letzten bekannten Stand, "
                        f"überspringe Setup (kein neuer Auto-SL, keine DCA-Neuanlage)")

                    _trl_prev = int(_prev_close.get("trailing_level", 0) or 0)
                    _sle_prev = bool(_prev_close.get("sl_at_entry", False))
                    _td_prev  = dict(_prev_close.get("trade_data", {}) or {})

                    last_known_avg[symbol]  = entry
                    last_known_size[symbol] = max(size, float(_prev_close.get("peak_size", 0) or 0))
                    trailing_sl_level[symbol] = _trl_prev
                    sl_at_entry[symbol]       = _sle_prev
                    if _td_prev:
                        trade_data[symbol] = _td_prev
                    new_trade_done[symbol]  = True   # verhindert dass der nächste Tick
                                                     # gleich wieder als neuer Trade triggert

                    # Close-Eintrag invalidieren: wenn derselbe Trade in kurzer Zeit
                    # erneut schliesst, soll das KEIN weiterer Phantom-Guard-Treffer
                    # werden — dann ist es ein echter Close. Aber wir behalten den
                    # Snapshot für den seltenen Fall "Phantom-Reopen unmittelbar
                    # nach Phantom-Reopen" — einfach den Timer zurücksetzen.
                    recent_closes.pop(symbol, None)

                    # Telegram-Benachrichtigung: Operator soll sehen, dass der Bot
                    # hier korrigierend eingegriffen hat (damit der falsche +0.00-
                    # Close-Report nicht als "echter Verlust" missverstanden wird).
                    try:
                        telegram(
                            f"♻️ <b>Phantom-Reopen abgefangen — {symbol}</b>\n"
                            f"━━━━━━━━━━━━\n"
                            f"Bitget hatte {symbol} für einen Tick aus der Position-"
                            f"Liste weggelassen, ist jetzt wieder aktiv. Der vorige "
                            f"'Close' war eine API-Phantom-Meldung.\n\n"
                            f"Rollback: SL-Level {_trl_prev}, sl_at_entry={_sle_prev} "
                            f"wiederhergestellt. Kein neuer Auto-SL, keine DCA-Neuanlage.\n"
                            f"Entry: {entry} (±{_entry_diff*100:.4f}% zum letzten bekannten)"
                        )
                    except Exception as _tex:
                        log(f"[phantom-guard telegram] {_tex}")

                    save_state()
                    return
        except Exception as ex:
            log(f"[setup_new_trade phantom-guard] {ex} — fahre mit normalem Setup fort")

    # Phantom-Snapshot nach Ablauf entsorgen (damit er nicht ewig im State hängt)
    if _prev_close:
        try:
            if time.time() - float(_prev_close.get("ts_close", 0)) >= PHANTOM_REOPEN_TTL_SEC:
                recent_closes.pop(symbol, None)
        except Exception:
            pass

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

        # v4.35: Forensik-Dump — warum wurde kein SL gefunden?
        # 24h-Log zeigte 2× Auto-SL-Fallback (ENSUSDT, SOLUSDT). Ursache war
        # vorher unklar weil der Log nur "Kein SL gefunden" sagte. Jetzt
        # dumpen wir Plan-Order-Count + trade_data-Snapshot, damit der
        # nächste Auftritt direkt diagnostizierbar ist.
        try:
            _td_snap     = trade_data.get(symbol, {}) or {}
            _td_sl       = _td_snap.get("sl", "—")
            _plan_orders = _get_plan_orders(symbol) or []
            _plan_kinds  = [(o.get("planType") or "—") for o in _plan_orders]
            log(f"  🔬 Auto-SL-Forensik [{symbol}]: get_sl_price=0, "
                f"plan_orders={len(_plan_orders)} ({_plan_kinds}), "
                f"trade_data.sl={_td_sl}, direction={direction}, leverage={leverage}")
        except Exception as _fe:
            log(f"  🔬 Auto-SL-Forensik Fehler: {_fe}")

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
                f"Ausstiegslinie \u00fcbereinstimmt!",
                reply_markup=build_setup_buttons(symbol),
            )
            sl_price = sl_auto
        else:
            log(f"  ✗ Auto-SL fehlgeschlagen: {res.get('msg', res)}")
            telegram(
                f"\u274c <b>Kein SL \u2014 {symbol}</b>\n"
                f"Auto-SL fehlgeschlagen. Bitte manuell setzen!\n"
                f"Empfehlung: {sl_str} USDT ({sl_dist:.1f}%)",
                reply_markup=build_setup_buttons(symbol),
            )
            cancel_all_tp_orders(symbol)
            time.sleep(1)
            place_tp_orders(symbol, entry, size, direction, leverage, mark,
                            known_sl=sl_price)
            last_known_avg[symbol]  = entry
            last_known_size[symbol] = size
            new_trade_done[symbol]  = True
            # v4.20: Queue-Log Outcome-Bindung (Fail-Pfad, ohne valides SL)
            try:
                mark_trade_taken(symbol, direction, entry, sl_price or 0, leverage)
            except Exception as ex:
                log(f"[mark_trade_taken] {ex}")
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
    # v4.20: Queue-Log Outcome-Bindung — matched die jüngste passende
    # Queue-Zeile mit taken=1 + Open-Preisen. Wenn kein Queue-Eintrag
    # vorhanden (manueller Trade ohne vorheriges Signal), ist das okay —
    # mark_trade_taken() macht dann einen No-Op.
    try:
        mark_trade_taken(symbol, direction, entry, sl_price, leverage)
    except Exception as ex:
        log(f"[mark_trade_taken] {ex}")
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
    # v4.12: Sling-SL + DCA Auto-Void für neuen Trade zurücksetzen
    sling_sl.pop(symbol, None)
    dca_void.pop(symbol, None)
    # Trade-Daten für spätere Auswertung inkl. Score
    _sugg_for_score = build_trade_suggestion(symbol, direction, entry, sl_price, None)
    _score_data = score_entry({
        "symbol":             symbol,
        "direction":          direction,
        "sugg":               _sugg_for_score,
        "harsi_warn":         0,
        "timing_elapsed_min": 0,
        "xinfo":              {},
    }) if _sugg_for_score else {}

    trade_data[symbol] = {
        "entry":      entry,
        "direction":  direction,
        "leverage":   leverage,
        "sl":         sl_price,
        "peak_size":  size,
        "init_size":  size,
        "open_ts":    int(time.time() * 1000),
        "open_dt":    __import__("datetime").datetime.now().isoformat(),
        "symbol":     symbol,
        "demo":       DEMO_MODE,
        "score_data": _score_data,
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
        f"({order_margin*3/balance*100:.1f}% Kapital)"
        if balance > 0 else ""
    )
    telegram(msg, reply_markup=build_setup_buttons(symbol))

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
    leverage  = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
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

    # v4.35: Min-Qty Edge-Case verbessern. Bisher: Qty<4 → komplettes Bail-Out
    # ("manuell überwachen"). Ergebnis im Log war SOLUSDT mit Qty=3.7: kein
    # einziger TP gesetzt, nur SL als Exit. place_tp_orders() hat aber bereits
    # eingebaute Carry-Forward-Logik (überspringt Sub-min-Qty TPs und addiert
    # auf den nächsten) plus TP4=Full-Close. Jetzt: erst Versuch starten,
    # nur bei count==0 als "manuell" warnen.
    cancel_all_tp_orders(symbol)
    time.sleep(1)
    count, prices = place_tp_orders(
        symbol, avg, total, direction, leverage, mark, known_sl=known_sl
    )

    if count == 0 and total < 4:
        # Echtes Bail-Out nur wenn place_tp_orders auch nichts hingekriegt hat
        log(f"  ⚠ Position zu klein (Qty={total}, min. 4) — kein TP setzbar — "
            f"TPs manuell überwachen")
        telegram(
            f"⚠️ <b>Position zu klein — {symbol}</b>\n"
            f"Qty={total} ({direction.upper()}) — keine TPs setzbar.\n"
            f"SL: {known_sl if known_sl > 0 else '—'} USDT\n"
            f"Bitte TP manuell überwachen oder Position glattstellen."
        )
        last_known_avg[symbol]  = avg
        last_known_size[symbol] = total
        return

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


def reply(text: str, reply_markup: dict = None):
    """Sendet Antwort an den Telegram Chat (optional mit Inline-Keyboard)."""
    telegram(text, reply_markup=reply_markup)


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
            lev  = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
            drct = pos.get("holdSide", "?").upper()
            pnl  = float(pos.get("unrealizedPL", 0))

            # v4.31: Trailing-Level aus IST-SL auf Bitget ableiten (heilt
            # State nach Redeploy oder manuellem SL-Move auf Bitget).
            trl  = infer_trailing_level(sym, drct.lower(), avg, lev)
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


def build_trade_suggestion(symbol: str, direction: str, entry: float,
                            sling_sl_raw, atr_raw) -> dict:
    """
    v4.12 Trade-Vorschlag-Generator für /trade-Auto-Fill.
    Berechnet Hebel + SL aus (entry, sling_sl, ATR) unter Berücksichtigung:
      - MAX_LEVERAGE (25x Hard-Cap)
      - MAX_EXPOSURE_PCT (25% Equity Gesamt-Einsatz)
      - Sling-Low (LONG) / Sling-High (SHORT) als primärer SL-Preis
      - Fallback auf max(SLING_PCT_FLOOR%, SLING_ATR_MULT × ATR) wenn Pivot
        zu nah am Entry liegt.
    Gibt ein Dict mit allen Werten zurück — oder {} wenn nicht berechenbar.
    """
    try:
        pivot = float(sling_sl_raw) if sling_sl_raw not in (None, "", "null") else 0.0
    except (TypeError, ValueError):
        pivot = 0.0
    try:
        atr = float(atr_raw) if atr_raw not in (None, "") else 0.0
    except (TypeError, ValueError):
        atr = 0.0

    if entry <= 0:
        return {}

    # Fallback-Puffer bestimmen
    pct_buf = entry * (SLING_PCT_FLOOR / 100.0)
    atr_buf = atr * SLING_ATR_MULT if atr > 0 else 0.0
    min_buf = max(pct_buf, atr_buf)

    if direction == "long":
        sl_candidate = pivot if pivot > 0 else entry - min_buf
        if entry - sl_candidate < min_buf:
            sl_candidate = entry - min_buf
    else:
        sl_candidate = pivot if pivot > 0 else entry + min_buf
        if sl_candidate - entry < min_buf:
            sl_candidate = entry + min_buf

    sl_dist_pct = abs(entry - sl_candidate) / entry * 100
    if sl_dist_pct <= 0:
        return {}

    # Hebel-Berechnung: 25 / SL-Abstand% — hart capped bei MAX_LEVERAGE
    opt_lev = max(1, min(MAX_LEVERAGE, round(25.0 / sl_dist_pct)))

    # Exposure-Cap prüfen: Margin × Hebel ≤ MAX_EXPOSURE_PCT × Balance
    # Gesamt-Einsatz = Per-Order × 3 (Market + 2 DCAs mit 1.5x + 2.5x Multiplier) × Hebel
    # Wir orientieren uns am Standard-Setup: per_order = balance × 0.10 / 3,
    # also total notional = balance × 0.10 × Hebel. Für Hebel 25 sind das 2.5x Equity.
    # Exposure-Cap = Notional ≤ MAX_EXPOSURE_PCT × Equity × 10 (d.h. Hebel ≤ 25
    # bei 10%-Margin entspricht 2.5x Equity-Notional).
    # → In Praxis: wir prüfen Margin × Hebel / Balance ≤ MAX_EXPOSURE_PCT × 10.
    try:
        balance = get_futures_balance()
    except Exception:
        balance = 0.0
    max_notional = balance * MAX_EXPOSURE_PCT if balance > 0 else 0.0
    per_order_margin = balance * 0.10 / 3 if balance > 0 else 0.0
    total_notional   = per_order_margin * 3 * opt_lev  # Market + 2 DCAs (Margin)
    exposure_ok      = (max_notional == 0) or (per_order_margin * opt_lev * 3 <= balance * MAX_EXPOSURE_PCT * 10)

    # Wenn Hebel × 3-Tranche > 25% der Equity (bei 10% Margin) → Hebel reduzieren
    # 10% × Hebel × 3 ≤ 0.25 × 10 → Hebel ≤ 8.33 reicht nicht, wir rechnen
    # stattdessen gegen MAX_EXPOSURE_PCT × 10 (bei 25% → 2.5x Equity-Notional).
    # Der Hebel = 25 / SL% ist schon durch MAX_LEVERAGE gedeckelt.

    return {
        "entry":           entry,
        "sl":              round(sl_candidate, 8),
        "sl_dist_pct":     round(sl_dist_pct, 2),
        "leverage":        int(opt_lev),
        "pivot_used":      pivot if pivot > 0 else 0.0,
        "atr":             atr,
        "min_buf":         round(min_buf, 8),
        "balance":         round(balance, 2),
        "max_notional":    round(max_notional, 2),
        "per_order":       round(per_order_margin, 2),
        "exposure_ok":     bool(exposure_ok),
        "exposure_cap_pct":round(MAX_EXPOSURE_PCT * 100, 1),
    }


def format_trade_suggestion(symbol: str, direction: str, sugg: dict) -> str:
    """Baut eine kopierbare /trade-Zeile + Erläuterung aus build_trade_suggestion()."""
    if not sugg:
        return f"/trade {symbol} {direction.upper()} [HEBEL] [ENTRY] [SL]"

    entry   = sugg["entry"]
    sl      = sugg["sl"]
    lev     = sugg["leverage"]
    sl_pct  = sugg["sl_dist_pct"]
    pivot   = sugg.get("pivot_used", 0)
    atr_v   = sugg.get("atr", 0)
    per_ord = sugg.get("per_order", 0)
    cap_pct = sugg.get("exposure_cap_pct", 25)

    cmd_line = f"/trade {symbol} {direction.upper()} {lev} {entry:.5f} {sl:.5f}"
    lines = [cmd_line]
    src = ("Sling-Low" if direction == "long" else "Sling-High") if pivot > 0 else "ATR-Fallback"
    lines.append(f"↳ SL-Basis: {src} | SL-Abstand: {sl_pct:.2f}% | Hebel: {lev}x (Max {MAX_LEVERAGE}x)")
    if atr_v > 0:
        lines.append(f"↳ ATR(14): {atr_v:.5f} | Puffer: {sugg['min_buf']:.5f}")
    if per_ord > 0:
        lines.append(f"↳ Pro Order ≈ {per_ord:.2f} USDT | Exposure-Cap {cap_pct}%")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# v4.19: ENTRY-QUEUE — Ranked Entry-Liste bei mehreren HARSI_EXIT
# ═══════════════════════════════════════════════════════════════
# Statt jedes HARSI_EXIT-Signal sofort an Telegram zu schicken, werden
# Signale in einem ENTRY_QUEUE_WINDOW_SEC-Fenster gesammelt und danach
# als eine einzige, nach Score sortierte Rangliste gesendet:
#   🎯 PREMIUM  (Makro-Extremzone + BTC/T2-Richtung konform — Hard-Gate)
#   📋 REGULAR  (alles andere)
# innerhalb jeder Gruppe absteigend nach Quality-Score (0-100).

def symbol_win_rate(symbol: str, n_last: int = 20) -> tuple:
    """Liest TRADES_CSV (Railway Volume) und gibt (win_rate, n_trades) für die
    letzten n_last Trades des Symbols zurück. Gecacht 1h. Rückgabe (0.5, 0)
    wenn CSV leer/fehlt oder zu wenige Daten."""
    now = time.time()
    cached = _winrate_cache.get(symbol)
    if cached and cached[2] > now:
        return (cached[0], cached[1])

    wr, n = 0.5, 0
    try:
        if not TRADES_CSV or not os.path.isfile(TRADES_CSV):
            _winrate_cache[symbol] = (wr, n, now + _WINRATE_CACHE_TTL)
            return (wr, n)

        matched_rows: list = []
        with open(TRADES_CSV, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            next(reader, None)  # Header überspringen
            for row in reader:
                if len(row) < 12:
                    continue
                if row[2].strip().upper() == symbol.upper():
                    matched_rows.append(row)

        subset = matched_rows[-n_last:]
        total  = len(subset)
        wins   = sum(1 for r in subset
                     if r[11].strip().lower() in ("ja", "yes", "true", "1"))
        if total > 0:
            wr = wins / total
            n  = total
    except Exception as e:
        log(f"[symbol_win_rate] {symbol} Fehler: {e}")

    _winrate_cache[symbol] = (wr, n, now + _WINRATE_CACHE_TTL)
    return (wr, n)


def score_entry(e: dict) -> dict:
    """Quality-Score (0-100) für ein Entry-Signal. Rückgabe:
      {"score": int, "is_premium": bool, "breakdown": [...], "warnings": [...]}

    Gewichte (v4.27 — Formel-Fix + Hebel-Glockenkurve + Gegen-Trend-Malus):
      +30  Makro-Premium aktiv (Extremzone richtungskonform)
      +20  BTC_DIR + T2_DIR beide konform (nur eine: +10)
      +15  enger SL-Abstand (Slope −3/%; 0.5%=15pt, 2%=10pt, 5%=2pt)
      +10  Hebel-Sweet-Spot (Glockenkurve um 12x Peak;
                            Unten steiler: 5x=3pt, 8x=6pt;
                            Oben flacher: 18x=6pt, 25x=1pt, 30x+=0pt)
      +15  historische Win-Rate (0.40=0pt, 0.55=8pt, 0.70+=15pt)
       +5  harsi_warn=0 (keine HARSI-Divergenz)
       +5  Timing (<5 min ab H2 = 5pt, >30 min = 0pt)
       -5  Gegen Makro-Trend (BTC/T2 in Gegenrichtung, ohne Recovering-Ausnahme)
    −10/−15 Korrelations-Malus (≥2 offen=−10, ≥3=−15)

    SL+Hebel-Entkopplung: Da Hebel = 25/SL% in DOMINUS gilt, messen beide
    Kriterien im Linear-Modell dasselbe. Die Glockenkurve auf Hebel behebt das:
    extreme Scalps (SL 0.5%, 50x) bekommen 15pt statt 25pt, der Sweet-Spot
    (SL 2%, 12x) bleibt bei 20pt kombiniert. Dadurch echte R:R-Qualität.

    Premium-Gate: Makro-Premium UND BTC/T2 mindestens einmal konform.
    """
    direction = e.get("direction", "")
    xinfo     = e.get("xinfo") or {}
    premium_zones = xinfo.get("premium",  []) or []
    warn_zones    = xinfo.get("warnings", []) or []
    sugg      = e.get("sugg") or {}
    sl_pct    = float(sugg.get("sl_dist_pct", 0) or 0)
    leverage  = int(sugg.get("leverage", 0) or 0)
    harsi_w   = int(e.get("harsi_warn", 0) or 0)
    elapsed_m = int(e.get("timing_elapsed_min", 0) or 0)
    symbol    = e.get("symbol", "")

    score: int = 0
    breakdown: list = []
    warnings:  list = []

    # 1) Makro-Premium-Zonen (+30 / Warnung)
    has_premium = len(premium_zones) > 0
    if has_premium:
        score += 30
        breakdown.append(f"+30 Makro-Premium ({len(premium_zones)}x)")
    if warn_zones:
        warnings.append(f"Makro-Gegensignal ({len(warn_zones)}x)")

    # 2) BTC_DIR / T2_DIR Richtungs-Konsistenz
    _btc = (btc_dir or "").lower()
    _t2  = (t2_dir  or "").lower()

    def _dir_matches(d: str) -> bool:
        if direction == "long":
            return d in ("long", "recovering")
        if direction == "short":
            return d in ("short", "recovering_short")
        return False

    m_btc = _dir_matches(_btc)
    m_t2  = _dir_matches(_t2)
    if m_btc and m_t2:
        score += 20
        breakdown.append("+20 BTC+T2 konform")
    elif m_btc or m_t2:
        score += 10
        breakdown.append(f"+10 {'BTC' if m_btc else 'T2'} konform")
    elif _btc or _t2:
        # v4.27: Gegen-Trend-Malus (−5) — bisher nur Warnung ohne Punkt-Abzug.
        # Recovering-Zustände werden durch _dir_matches() bereits als "konform"
        # gewertet; dieser Zweig feuert nur bei echtem Gegen-Trend.
        score -= 5
        breakdown.append("−5 Gegen Makro-Trend")
        warnings.append("Makro-Richtung nicht konform")

    # 3) SL-Abstand → enger = bessere R:R (+15 bis 0)
    if sl_pct > 0:
        sl_pts = max(0, min(15, int(round(15 - (sl_pct - 0.5) * 3))))
        score += sl_pts
        breakdown.append(f"+{sl_pts} SL {sl_pct:.2f}%")

    # 4) Hebel-Sweet-Spot — Glockenkurve um 12x (v4.27)
    #    Unten steiler (niedriger Hebel = wider SL = schlechteres R:R):
    #      Faktor 1.0 pro Stufe, 5x=3pt, 8x=6pt, 10x=8pt, 12x=10pt (Peak)
    #    Oben flacher (hoher Hebel = tight SL, aber Noise-Risiko):
    #      Faktor 0.7 pro Stufe, 15x=8pt, 18x=6pt, 25x=1pt, 30x+=0pt
    #    Entkoppelt SL+Hebel, weil DOMINUS-Formel Hebel=25/SL% sonst doppelt zählt.
    if leverage > 0:
        if leverage >= 12:
            _raw = 10 - (leverage - 12) * 0.7
        else:
            _raw = 10 - (12 - leverage) * 1.0
        lev_pts = max(0, min(10, int(round(_raw))))
        score += lev_pts
        breakdown.append(f"+{lev_pts} {leverage}x Hebel")

    # 5) Historische Win-Rate (+15 bis 0) — nur ab 3 Trades relevant
    if symbol:
        wr, n_tr = symbol_win_rate(symbol)
        if n_tr >= 3:
            wr_pts = max(0, min(15, int(round((wr - 0.40) / 0.30 * 15))))
            score += wr_pts
            breakdown.append(f"+{wr_pts} WR {wr*100:.0f}% ({n_tr}T)")
        else:
            breakdown.append(f"±0 WR n/a ({n_tr}T)")

    # 6) HARSI-Divergenz-Flag (+5 / Warnung)
    if harsi_w == 0:
        score += 5
        breakdown.append("+5 kein HARSI-warn")
    else:
        warnings.append("HARSI-Divergenz aktiv")

    # 7) Timing-Frische (+5 bis 0)
    if elapsed_m <= 30:
        t_pts = max(0, min(5, int(round((30 - elapsed_m) / 30.0 * 5))))
        score += t_pts
        if t_pts > 0:
            breakdown.append(f"+{t_pts} Timing {elapsed_m}min")

    # 8) Korrelations-Malus — linear gestaffelt (v4.27)
    #    2 offene gleichartige Positionen = −10 (wie bisher)
    #    3+ offene = −15 (Cap)
    #    Verhindert Klumpen-Risiko stärker als Binary-Malus.
    try:
        same_dir = 0
        for pos in get_all_positions():
            if float(pos.get("total", 0) or 0) <= 0:
                continue
            if (pos.get("holdSide") or "").lower() == direction:
                same_dir += 1
        if same_dir >= 2:
            _corr_malus = min(15, 10 + (same_dir - 2) * 5)
            score -= _corr_malus
            breakdown.append(f"−{_corr_malus} {same_dir}x {direction.upper()} offen")
    except Exception:
        pass

    # Clamp auf 0..100
    score = max(0, min(100, score))

    # Premium-Bucket Hard-Gate
    is_premium = has_premium and (m_btc or m_t2)

    return {
        "score":      score,
        "is_premium": is_premium,
        "breakdown":  breakdown,
        "warnings":   warnings,
    }


def enqueue_entry(entry: dict) -> None:
    """Fügt ein Entry-Signal zur Queue hinzu und startet den Flush-Timer
    beim ersten Eintrag jeder Batch. Dubletten (gleicher sig_key) erhöhen
    confirm_count und aktualisieren die Daten, ohne eine 2. Zeile zu erzeugen.

    v4.26: Erweitertes Logging — bei Dedup werden geänderte Felder sichtbar
    gemacht (source/warn_line/elapsed_min), damit man im Log erkennt, welche
    Signal-Quelle den Batch zuletzt überschrieben hat."""
    global _entry_flush_timer, _entry_flush_started_ts
    sig_key = f"{entry['symbol']}_{entry['direction']}"
    with pending_entries_lock:
        existing = pending_entries.get(sig_key)
        if existing:
            entry["confirm_count"] = existing.get("confirm_count", 1) + 1
            # v4.26: Dedup-Diff anzeigen (welche Felder ändern sich durchs Re-Trigger)
            _diff_fields = []
            for _f in ("source", "warn_line", "timing_elapsed_min", "harsi_warn"):
                _ov, _nv = existing.get(_f), entry.get(_f)
                if _ov != _nv:
                    _diff_fields.append(f"{_f}: {_ov!r}→{_nv!r}")
            _diff_str = " | ".join(_diff_fields) if _diff_fields else "keine Änderung"
            log(f"  ENTRY-QUEUE: {sig_key} bestätigt ({entry['confirm_count']}x) "
                f"— dedup-diff: {_diff_str}")
        else:
            entry["confirm_count"] = 1
        pending_entries[sig_key] = entry

        timer_running = _entry_flush_timer is not None and _entry_flush_timer.is_alive()
        if not timer_running:
            _entry_flush_started_ts = time.time()
            _entry_flush_timer = threading.Timer(ENTRY_QUEUE_WINDOW_SEC, flush_entries)
            _entry_flush_timer.daemon = True
            _entry_flush_timer.start()
            log(f"  ENTRY-QUEUE: Fenster gestartet ({ENTRY_QUEUE_WINDOW_SEC}s) "
                f"— 1. Signal: {sig_key} (source={entry.get('source','HARSI_EXIT')})")


def flush_entries() -> None:
    """Timer-Callback nach Ablauf des Sammelfensters. Scored alle Signale,
    sortiert in einer einzigen Liste rein nach Score absteigend und sendet
    konsolidierte Nachricht. Premium wird per Stern-Badge markiert."""
    global _entry_flush_timer
    with pending_entries_lock:
        if not pending_entries:
            _entry_flush_timer = None
            return
        batch = list(pending_entries.values())
        pending_entries.clear()
        _entry_flush_timer = None

    try:
        for e in batch:
            e["_scored"] = score_entry(e)

        # Eine einzige Liste — rein nach Score absteigend.
        # Premium-Faktoren (Makro +30, BTC/T2 je +20) sind bereits
        # im Score enthalten; ein Stern-Badge markiert Premium visuell.
        ranked = sorted(
            batch,
            key=lambda x: x["_scored"]["score"],
            reverse=True,
        )
        n_premium = sum(1 for e in ranked if e["_scored"]["is_premium"])

        # v4.20: Persistiert JEDES bewertete Signal (auch nicht-getradete)
        # — essenziell für Survivorship-Bias-freie Score-Kalibrierung.
        for e in ranked:
            try:
                log_scored_entry(e)
            except Exception as ex:
                log(f"[log_scored_entry] {ex}")

        try:
            balance = get_futures_balance()
        except Exception:
            balance = 0.0

        # v4.25: Button-Driven Rangliste — initialen Slot-State bauen und
        # als Übersichts-Message senden. message_id dient als Slot-Key für
        # spätere editMessageText-Updates bei Detail-Taps.
        _now = __import__("datetime").datetime.now()
        slot_label = f"{_now.strftime('%H:%M')}-Slot"

        _slot_purge_expired()

        slot_state = {
            "ranked":         ranked,
            "balance":        balance,
            "slot_label":     slot_label,
            "current_detail": None,
            "view_mode":      "overview",
            "created_ts":     time.time(),
        }
        overview_text = format_slot_overview(slot_state)
        keyboard      = build_slot_keyboard(slot_state, mode="overview")
        msg_id = telegram(overview_text, reply_markup=keyboard, return_id=True)

        if msg_id:
            with _slot_states_lock:
                _slot_states[int(msg_id)] = slot_state
            log(f"  ENTRY-QUEUE: Slot-Overview gesendet (msg_id={msg_id}) "
                f"— {len(ranked)} Signale ({n_premium} Premium)")
        else:
            # Fallback — Callbacks funktionieren nicht, aber User sieht
            # zumindest die Übersicht als normale Telegram-Message.
            log("  ENTRY-QUEUE: WARNUNG — keine message_id erhalten, "
                "Callback-Navigation deaktiviert für diesen Slot")
    except Exception as ex:
        log(f"[flush_entries] Fehler: {ex}")
        try:
            telegram(f"❌ <b>Entry-Queue Fehler</b>\n<code>{html.escape(str(ex))}</code>")
        except Exception:
            pass


def format_ranked_list(ranked: list, balance: float, total: int) -> str:
    """Baut die konsolidierte Telegram-Rangliste als EINE einzige Liste
    (Premium zuerst, dann nach Score absteigend) inkl. kopierbarer
    /trade-Befehle. Premium-Kandidaten tragen ein 🎯-Badge."""

    def _render(idx: int, e: dict) -> list:
        s        = e["_scored"]
        sugg     = e.get("sugg") or {}
        sym      = e["symbol"]
        dr       = e["direction"].upper()
        icon     = "🟢" if e["direction"] == "long" else "🔴"
        entry_px = e.get("entry", 0) or 0
        sl       = sugg.get("sl", 0) or 0
        sl_pct   = sugg.get("sl_dist_pct", 0) or 0
        lev      = sugg.get("leverage", 0) or 0
        per_ord  = sugg.get("per_order", 0) or 0
        confirms = e.get("confirm_count", 1)
        is_prem  = bool(s.get("is_premium"))

        badge    = "⭐ " if is_prem else ""
        conf_str = f" ⚡{confirms}x" if confirms > 1 else ""
        out = [f"<b>{idx}. {badge}{icon} {sym} {dr}{conf_str}</b>  ·  "
               f"Score <b>{s['score']}/100</b>"]

        if entry_px and sl and lev:
            out.append(f"   Entry {entry_px:.5f} | SL {sl:.5f} "
                       f"({sl_pct:.2f}%) | {lev}x")
        if per_ord > 0:
            out.append(f"   ≈ {per_ord:.2f} USDT/Order")

        bd_top = [b for b in (s.get("breakdown") or [])
                  if b.startswith(("+", "-"))][:3]
        if bd_top:
            out.append(f"   {' · '.join(bd_top)}")

        wn = s.get("warnings") or []
        if wn:
            out.append(f"   ⚠️ {'; '.join(wn)}")

        if entry_px and sl and lev:
            out.append(f"   <code>/trade {sym} {dr} {lev} "
                       f"{entry_px:.5f} {sl:.5f}</code>")
        return out

    n_premium = sum(1 for e in ranked if e["_scored"].get("is_premium"))

    lines = [
        "🏁 <b>DOMINUS — Entry-Rangliste</b>",
        "━━━━━━━━━━━━",
        f"{total} Signal{'e' if total != 1 else ''} "
        f"im {ENTRY_QUEUE_WINDOW_SEC}s-Fenster"
        + (f"  ·  ⭐ {n_premium} Premium" if n_premium else ""),
    ]
    if balance > 0:
        lines.append(f"💰 Balance: {balance:.2f} USDT | "
                     f"Exposure-Cap {MAX_EXPOSURE_PCT*100:.0f}%")
    lines.append("")

    if ranked:
        for i, e in enumerate(ranked, 1):
            lines.extend(_render(i, e))
            lines.append("")
    else:
        lines.append("<i>Keine gültigen Kandidaten.</i>")
        lines.append("")

    lines.append("━━━━━━━━━━━━")
    lines.append("💡 <i>Top-Eintrag zuerst traden. ⭐ = Macro-Premium "
                 "(Makro + BTC/T2 bestätigt). Kelly + Exposure-Cap "
                 "bestimmen Slot-Anzahl.</i>")
    _anker = "sec-alarm3"
    lines.append(_doc_link(_anker, "Alarm 3/3b — HARSI Exit"))
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# v4.25: BUTTON-DRIVEN ENTRY-RANGLISTE
#
# Neue UX statt seitenlanger Rangliste:
#   • Übersichts-Message mit Coin-Button-Grid
#   • Tap auf Coin → Message wird zu Detail-View (nur EIN Coin offen)
#   • Adaptives Keyboard (🎯 Berechnen / 🟠 Bitget / 📊 TV / BTC H2 /
#     🔀 Total2 / ❌ Schliessen, darunter die restlichen Coin-Buttons)
#   • Tap auf 🎯 Berechnen → 1-Tap /trade-Berechnung (Callback-Handler)
# ═══════════════════════════════════════════════════════════════

def _slot_purge_expired() -> None:
    """Entfernt abgelaufene Slots aus _slot_states (TTL 2h)."""
    cutoff = time.time() - _SLOT_TTL_SEC
    with _slot_states_lock:
        expired = [mid for mid, st in _slot_states.items()
                   if st.get("created_ts", 0) < cutoff]
        for mid in expired:
            _slot_states.pop(mid, None)


def _score_color_badge(score: int) -> str:
    """Farbkodiertes Score-Badge: 🟢 ≥75 | 🟡 50–74 | 🔴 <50."""
    if score >= 75:
        return "🟢"
    if score >= 50:
        return "🟡"
    return "🔴"


def format_slot_overview(state: dict) -> str:
    """Rendert die Übersichts-Message (kompakt) für einen Slot.
    Zeigt: Header mit Slot-Label, Signal-Summary, Top-5-Kurzform, Hinweis."""
    ranked = state.get("ranked") or []
    balance = state.get("balance", 0) or 0
    slot_label = state.get("slot_label", "")
    total = len(ranked)
    n_premium = sum(1 for e in ranked
                    if (e.get("_scored") or {}).get("is_premium"))

    lines = [
        f"🏁 <b>DOMINUS Entry-Rangliste</b> · {html.escape(slot_label)}",
        "━━━━━━━━━━━━",
    ]
    summary_bits = [f"{total} Signal{'e' if total != 1 else ''}"]
    if n_premium:
        summary_bits.append(f"⭐ {n_premium} Premium")
    summary_bits.append(f"Fenster {ENTRY_QUEUE_WINDOW_SEC}s")
    lines.append("  ·  ".join(summary_bits))
    if balance > 0:
        lines.append(f"💰 {balance:.2f} USDT  ·  Cap {MAX_EXPOSURE_PCT*100:.0f}%")
    lines.append("")

    # Top-5-Kurzform (extra 3 des Changelogs)
    top5 = ranked[:5]
    if top5:
        top5_bits = [
            f"{e['symbol'].replace('USDT','')} {(e.get('_scored') or {}).get('score', 0)}"
            for e in top5
        ]
        lines.append("⚡ <b>Top-5</b>")
        lines.append("  " + "  ·  ".join(top5_bits))
        lines.append("")

    lines.append("💡 <i>Coin antippen für Details</i>")
    return "\n".join(lines)


def format_slot_detail(state: dict, symbol: str) -> str:
    """Rendert die Detail-View für genau einen Coin. Score-Breakdown
    mit rechtsbündiger Punkte-Spalte + Checkliste-Gates oben."""
    ranked = state.get("ranked") or []
    slot_label = state.get("slot_label", "")
    # Rang ermitteln
    rank = None
    entry = None
    for i, e in enumerate(ranked, 1):
        if e.get("symbol") == symbol:
            rank = i
            entry = e
            break
    if entry is None:
        return format_slot_overview(state)

    s        = entry.get("_scored") or {}
    sugg     = entry.get("sugg") or {}
    dr       = entry.get("direction", "").upper()
    icon     = "🟢" if dr == "LONG" else "🔴"
    score    = int(s.get("score", 0))
    is_prem  = bool(s.get("is_premium"))
    badge    = "⭐" if is_prem else "  "
    entry_px = entry.get("entry", 0) or 0
    sl       = sugg.get("sl", 0) or 0
    sl_pct   = sugg.get("sl_dist_pct", 0) or 0
    lev      = sugg.get("leverage", 0) or 0
    per_ord  = sugg.get("per_order", 0) or 0

    lines = [
        f"🏁 <b>DOMINUS Entry-Rangliste</b> · {html.escape(slot_label)}",
        "━━━━━━━━━━━━",
        f"{badge}  <b>{rank}. {icon} {symbol} {dr}</b>  ·  "
        f"{_score_color_badge(score)} {score}/100",
        "",
    ]

    # Trade-Vorschlag (Entry/SL/Hebel/DCA)
    if entry_px and sl and lev:
        lines.append("<pre>"
                     f"Entry   {entry_px:.5f}\n"
                     f"SL      {sl:.5f}  ({sl_pct:.2f}%)\n"
                     f"Hebel   {lev}x  ·  {per_ord:.2f} USDT/Order"
                     + (f"\nDCA     {per_ord * DCA1_MULTIPLIER:.2f} / "
                        f"{per_ord * DCA2_MULTIPLIER:.2f} USDT"
                        if per_ord > 0 else "")
                     + "</pre>")
    lines.append("")

    # Score-Breakdown — Gates oben (ohne Punkte), dann Faktoren mit +XX
    breakdown = list(s.get("breakdown") or [])
    # Pine-Gates (implizit bei ankommendem Signal)
    lines.append("✅ DOMINUS Impuls Extremzone")
    lines.append("✅ H4 Trigger bestätigt")
    # Score-Faktoren aus breakdown (Format: "+XX Label" oder "-YY Label" oder "±0 Label")
    for bd in breakdown:
        bd = str(bd).strip()
        if not bd:
            continue
        # Split in Punkte-Token (erste Gruppe) und Label (Rest)
        parts = bd.split(" ", 1)
        pts_tok = parts[0] if parts else ""
        label = parts[1] if len(parts) > 1 else ""
        # Zeichen für die Zeile: ✅ (positiv), ⚠️ (negativ), ➖ (±0)
        if pts_tok.startswith("+") and pts_tok != "+0":
            row_icon = "✅"
        elif pts_tok.startswith("-"):
            row_icon = "⚠️"
        else:
            row_icon = "➖"
        # Punkte-Spalte rechtsbündig auf 4 Zeichen
        lines.append(f"{row_icon} {label:<28} {pts_tok:>5}")

    # Warnungen (falls vorhanden)
    for w in (s.get("warnings") or []):
        lines.append(f"⚠️ {html.escape(str(w))}")

    # v4.35.1: Trennlinie auf iOS-Telegram-Breite gekürzt (38 → 24 Zeichen)
    lines.append("────────────────────────")
    lines.append(f"<b>Score{'':<27}{score:>5}</b>")

    # Premium-Hinweis aus Extreme-Info
    xinfo = entry.get("xinfo") or {}
    if xinfo.get("premium"):
        lines.append("")
        lines.append("🎯 <i>DOMINUS-Premium aktiv (Extremzone konform)</i>")

    lines.append("━━━━━━━━━━━━")
    lines.append("💡 <i>Anderen Coin antippen, oder ❌ Schliessen</i>")
    return "\n".join(lines)


def format_slot_more(state: dict) -> str:
    """Rendert den 'Weitere X Signale'-Block als einzeilige Low-Score-Liste."""
    ranked = state.get("ranked") or []
    slot_label = state.get("slot_label", "")
    low = [e for e in ranked if (e.get("_scored") or {}).get("score", 0)
           < SLOT_LOWSCORE_CUT]
    top = [e for e in ranked if (e.get("_scored") or {}).get("score", 0)
           >= SLOT_LOWSCORE_CUT][SLOT_TOP_N:]
    # "Weitere" = alle jenseits Top-N (Score ≥ cut) + alle unter cut
    extra = top + low

    lines = [
        f"🏁 <b>DOMINUS Entry-Rangliste</b> · {html.escape(slot_label)}",
        "━━━━━━━━━━━━",
        f"<b>Weitere {len(extra)} Signale</b>",
        "",
    ]
    if not extra:
        lines.append("<i>Keine weiteren Signale.</i>")
    else:
        # Pro Zeile: "11 MEMEUSDT SHORT 47" (einzeilig kompakt)
        for i, e in enumerate(extra, SLOT_TOP_N + 1):
            s = e.get("_scored") or {}
            sym = e.get("symbol", "").replace("USDT", "")
            dr  = e.get("direction", "").upper()
            sc  = int(s.get("score", 0))
            pr  = "⭐" if s.get("is_premium") else " "
            lines.append(f"{pr} {i:>2}. {_score_color_badge(sc)} "
                         f"<code>{sym:<10}</code> {dr:<5} {sc:>3}")
    lines.append("")
    lines.append("━━━━━━━━━━━━")
    lines.append("💡 <i>Score &lt; 50 = kein Entry empfohlen</i>")
    return "\n".join(lines)


def _encode_calc_payload(entry: dict) -> str:
    """Kodiert /trade-Parameter für callback_data (Limit 64 Bytes).
    Format: calc:SYM:DIR:LEV:ENTRY:SL  (SL/ENTRY als Float, max ~5 Nachkommastellen).
    Beispiel: calc:AVAXUSDT:short:25:9.5420:9.8700 (36 Byte) — gut unter 64."""
    sugg     = entry.get("sugg") or {}
    sym      = entry.get("symbol", "")
    dr       = entry.get("direction", "")
    lev      = int(sugg.get("leverage", 0) or 0)
    entry_px = float(entry.get("entry", 0) or 0)
    sl       = float(sugg.get("sl", 0) or 0)
    return f"calc:{sym}:{dr}:{lev}:{entry_px:.5f}:{sl:.5f}"


def _encode_exec_payload(entry: dict) -> str:
    """v4.28 — callback_data für 🚀 Trade jetzt Button.
    Format: exec:SYM:DIR:LEV:ENTRY:SL. Maximal-Länge ≈ 41 Byte (Limit 64).
    Identisches Payload-Format wie _encode_calc_payload, nur Prefix unterschied
    — die Signatur hinter dem Prefix wird für Two-Tap-Confirm-Dedup genutzt."""
    sugg     = entry.get("sugg") or {}
    sym      = entry.get("symbol", "")
    dr       = entry.get("direction", "")
    lev      = int(sugg.get("leverage", 0) or 0)
    entry_px = float(entry.get("entry", 0) or 0)
    sl       = float(sugg.get("sl", 0) or 0)
    return f"exec:{sym}:{dr}:{lev}:{entry_px:.5f}:{sl:.5f}"


def _coin_button_label(idx: int, entry: dict, is_open: bool) -> str:
    """Kompakter Button-Label mit Long/Short-Indikator.

    Format: '🟢⭐1 AVAX 82' (Long Premium) / '🔴 2 OP 78' (Short normal)
            '🟢▾ AVAX 82' (offen Long, aktiv aufgeklappt)
    v4.35.1: Direction-Emoji 🟢=Long / 🔴=Short als erstes Zeichen
             damit Felix beim Scrollen sofort sieht, in welche
             Richtung der Setup geht — ohne den Button erst öffnen
             zu müssen.
    """
    s  = entry.get("_scored") or {}
    sc = int(s.get("score", 0))
    sym_short = entry.get("symbol", "").replace("USDT", "")
    direction = (entry.get("direction") or "").lower()
    dir_emoji = "🟢" if direction == "long" else ("🔴" if direction == "short" else "⚪")
    prefix = "▾" if is_open else ("⭐" if s.get("is_premium") else "")
    # Rang-Nr nur wenn nicht offen (der aufgeklappte Coin zeigt ▾)
    if is_open:
        return f"{dir_emoji}{prefix} {sym_short} {sc}"
    # Kompakt: "🟢⭐1 AVAX 82" / "🔴 2 OP 78"
    if prefix:
        return f"{dir_emoji}{prefix}{idx} {sym_short} {sc}"
    return f"{dir_emoji} {idx} {sym_short} {sc}"


def build_slot_keyboard(state: dict, open_symbol: str = None,
                        mode: str = "overview") -> dict:
    """v4.25 — Adaptives Inline-Keyboard für die Slot-Message.

    mode="overview": nur Coin-Grid (Top-N) + Weitere-Button.
    mode="detail":   Detail-Aktionen oben (Berechnen / Bitget / TV / BTC /
                     Total2 / Schliessen) + Divider + Coin-Grid.
    mode="more":     einfacher Zurück-Button.
    """
    ranked = state.get("ranked") or []
    rows: list = []

    if mode == "more":
        rows.append([
            {"text": "⬅ Zurück zur Rangliste", "callback_data": "back"},
        ])
        return {"inline_keyboard": rows}

    # Detail-Zustand: Aktions-Buttons zuerst
    if mode == "detail" and open_symbol:
        open_entry = next((e for e in ranked
                           if e.get("symbol") == open_symbol), None)
        if open_entry:
            sugg = open_entry.get("sugg") or {}
            base_coin = open_symbol.replace("USDT", "").replace(".P", "")
            links = tv_chart_links(open_symbol)
            # Reihe 1: 🎯 Berechnen (Callback für 1-Tap /trade)
            if (sugg.get("leverage") and open_entry.get("entry")
                    and sugg.get("sl")):
                rows.append([{
                    "text": f"🎯 Berechnen {base_coin} (1-Tap)",
                    "callback_data": _encode_calc_payload(open_entry),
                }])
            # Reihe 2: Bitget + TV (URL-Buttons)
            rows.append([
                {"text": f"🟠 Bitget {base_coin}", "url": links["bitget"]},
                {"text": f"📊 TV {base_coin} H2", "url": links["coin_h2"]},
            ])
            # Reihe 3: BTC H2 + Total2
            rows.append([
                {"text": "📈 BTC H2", "url": links["btc_h2"]},
                {"text": "🔀 Total2", "url": links["total2"]},
            ])
            # Reihe 4: 🚀 Trade jetzt (nur wenn v4.28 Auto-Trade aktiv ist +
            # komplettes Setup vorliegt). Two-Tap-Confirm schützt vor Fat-Finger.
            if (AUTO_TRADE_ENABLED and sugg.get("leverage")
                    and open_entry.get("entry") and sugg.get("sl")):
                rows.append([{
                    "text": f"🚀 Trade jetzt {base_coin} (Auto)",
                    "callback_data": _encode_exec_payload(open_entry),
                }])
            # Reihe 5: Schliessen
            rows.append([
                {"text": "❌ Schliessen", "callback_data": "close"},
            ])
            # Reihe 5: Divider (noop)
            rows.append([
                {"text": "━ Weitere Signale ━",
                 "callback_data": "noop"},
            ])

    # Coin-Grid (Top-N, sortiert nach Score absteigend)
    top = ranked[:SLOT_TOP_N]
    # 3 Buttons pro Reihe
    grid_row: list = []
    for i, e in enumerate(top, 1):
        sym = e.get("symbol", "")
        is_open = (mode == "detail" and sym == open_symbol)
        grid_row.append({
            "text": _coin_button_label(i, e, is_open),
            "callback_data": f"detail:{sym}",
        })
        if len(grid_row) == 3:
            rows.append(grid_row)
            grid_row = []
    if grid_row:
        rows.append(grid_row)

    # "Weitere N Signale"-Button (nur wenn Rest existiert)
    rest_count = max(0, len(ranked) - SLOT_TOP_N)
    if rest_count > 0:
        rows.append([
            {"text": f"▾ {rest_count} weitere", "callback_data": "more"},
        ])

    return {"inline_keyboard": rows}


def handle_callback_query(update: dict) -> None:
    """v4.25 — Dispatcher für Inline-Button-Taps in der Slot-Rangliste.

    Erwartete callback_data-Formate:
      detail:{SYMBOL}                        → Coin aufklappen
      close                                  → zurück zur Übersicht
      more                                   → Weitere-Block zeigen
      back                                   → aus more/detail zurück zur Übersicht
      calc:{SYM}:{DIR}:{LEV}:{ENTRY}:{SL}    → 1-Tap /trade-Berechnung
      noop                                   → Divider / Label-Button (ignoriert)

    Slot wird über msg_id identifiziert (eindeutig pro flush_entries()-Lauf).
    Expired/unbekannte Slots → Toast "Slot abgelaufen — neue H2-Welle abwarten".
    """
    cbq = update.get("callback_query")
    if not cbq:
        return
    callback_id = cbq.get("id", "")
    data        = (cbq.get("data") or "").strip()
    msg         = cbq.get("message") or {}
    msg_id      = int(msg.get("message_id") or 0)
    # Sicherheit: nur eigene Chat-ID
    chat_id_cb = str((msg.get("chat") or {}).get("id", ""))
    if chat_id_cb and chat_id_cb != str(TELEGRAM_CHAT_ID):
        telegram_answer_callback(callback_id, "⛔ Nicht erlaubt")
        return

    log(f"Callback empfangen: data={data!r} msg_id={msg_id}")

    # Noop / Divider — nur Toast unterdrücken
    if data == "noop" or not data:
        telegram_answer_callback(callback_id)
        return

    # Slot-State laden
    with _slot_states_lock:
        state = _slot_states.get(msg_id)

    if state is None:
        telegram_answer_callback(
            callback_id,
            "⏳ Slot abgelaufen — neue H2-Welle abwarten.",
            show_alert=True,
        )
        return

    # 1-Tap-Berechnung — delegiert an cmd_trade() mit den encodierten Parametern
    if data.startswith("calc:"):
        try:
            _, sym, dr, lev, entry_px, sl = data.split(":", 5)
            # cmd_trade erwartet: parts = ["/trade", SYMBOL, DIR, LEV, ENTRY, SL]
            cmd_trade(["/trade", sym, dr.upper(), lev, entry_px, sl])
            telegram_answer_callback(callback_id, "🎯 Berechne...")
        except Exception as ex:
            log(f"[callback calc] Fehler: {ex}")
            telegram_answer_callback(
                callback_id,
                f"❌ Berechnung fehlgeschlagen: {ex}",
                show_alert=True,
            )
        return

    # v4.28 — Two-Tap-Confirm + One-Click-Execution via Bitget-API
    if data.startswith("exec:"):
        if not AUTO_TRADE_ENABLED:
            telegram_answer_callback(
                callback_id,
                "⛔ Auto-Trade ist in den Railway-Env auf 'False' gesetzt.",
                show_alert=True,
            )
            return
        try:
            _, sym, dr, lev_s, entry_s, sl_s = data.split(":", 5)
            lev      = int(lev_s)
            entry_px = float(entry_s)
            sl_px    = float(sl_s)
        except Exception as ex:
            log(f"[callback exec] Parse-Fehler: {ex} | data={data!r}")
            telegram_answer_callback(
                callback_id,
                f"❌ Payload-Parse-Fehler: {ex}",
                show_alert=True,
            )
            return

        # payload_sig — identifiziert denselben Trade-Vorschlag für Two-Tap
        payload_sig = f"{sym}|{dr}|{lev}|{entry_px:.5f}|{sl_px:.5f}"
        confirmed   = _exec_confirm_check_and_consume(msg_id, payload_sig)

        if not confirmed:
            # v4.35.1 — Erster Tap zeigt zusätzlich die volle /trade-
            # Berechnung als Chat-Message (Margin, Kelly, DCAs, TPs, R:R,
            # Warnungen). Felix muss nicht mehr 🎯 Berechnen separat tippen.
            try:
                cmd_trade(["/trade", sym, dr.upper(), str(lev),
                           str(entry_px), str(sl_px)])
            except Exception as ex:
                log(f"[exec] cmd_trade-Vorschau fehlgeschlagen "
                    f"(Trade-Flow nicht beeinträchtigt): {ex}")
            # Erster Tap — State gesetzt, Alert anzeigen
            telegram_answer_callback(
                callback_id,
                f"⚠️ Berechnung oben prüfen.\n"
                f"Nochmals 🚀 zur Bestätigung — "
                f"läuft in {AUTO_TRADE_CONFIRM_TTL_SEC}s ab.",
                show_alert=True,
            )
            log(f"[exec] First-Tap {payload_sig} — Berechnung gepostet, "
                f"wartet auf Confirm")
            return

        # Zweiter Tap innerhalb TTL — ausführen
        log(f"[exec] Confirmed — starte execute_trade_order {payload_sig}")
        telegram_answer_callback(
            callback_id,
            f"🚀 Führe aus: {sym} {dr.upper()} {lev}x ...",
        )
        # execute_trade_order ist blocking (~3s für Verifikation) — ok, da im
        # Callback-Handler-Thread. Telegram hat das Alert schon gezeigt.
        try:
            result = execute_trade_order(sym, dr, lev, entry_px, sl_px)
        except Exception as ex:
            log(f"[exec] Exception in execute_trade_order: {ex}")
            try:
                telegram(
                    f"❌ <b>Auto-Trade Fehler — {sym}</b>\n"
                    f"Unbekannte Exception: <code>{ex}</code>",
                    reply_markup=build_setup_buttons(sym),
                )
            except Exception:
                pass
            return

        # Ergebnis an den User melden
        try:
            if result.get("ok") is True:
                telegram(
                    f"✅ <b>Auto-Trade ausgeführt — {sym}</b>\n"
                    f"Richtung: {dir_icon(dr)} {dr.upper()} | "
                    f"Hebel: {result.get('leverage')}x\n"
                    f"Entry: {result.get('entry')} | SL: {result.get('sl')}\n"
                    f"Qty: {result.get('qty')} | "
                    f"Margin: {result.get('initial_margin')} USDT | "
                    f"Notional: {result.get('notional')} USDT\n"
                    f"Order-ID: <code>{result.get('orderId')}</code>\n\n"
                    f"<i>DCA + TPs setzt der Main-Loop automatisch.</i>",
                    reply_markup=build_setup_buttons(sym),
                )
            elif result.get("ok") == "partial":
                # PANIC wurde bereits aus execute_trade_order geschickt
                log(f"[exec] Partial success — PANIC schon gesendet: {result}")
            else:
                telegram(
                    f"❌ <b>Auto-Trade abgebrochen — {sym}</b>\n"
                    f"Grund: <code>{result.get('reason')}</code>",
                    reply_markup=build_setup_buttons(sym),
                )
        except Exception as ex:
            log(f"[exec] Telegram-Report-Fehler: {ex}")
        return

    # Navigations-Callbacks — alle mutieren state + editen Message
    if data == "close" or data == "back":
        new_mode = "overview"
        open_sym = None
    elif data == "more":
        new_mode = "more"
        open_sym = None
    elif data.startswith("detail:"):
        new_mode = "detail"
        open_sym = data.split(":", 1)[1]
        # Sicherheit: Symbol muss in ranked sein
        if not any(e.get("symbol") == open_sym for e in (state.get("ranked") or [])):
            telegram_answer_callback(callback_id, "Coin nicht im Slot.")
            return
    else:
        telegram_answer_callback(callback_id)
        return

    # State mutieren (unter Lock)
    with _slot_states_lock:
        current_state = _slot_states.get(msg_id)
        if current_state is None:
            telegram_answer_callback(callback_id, "⏳ Slot abgelaufen.",
                                     show_alert=True)
            return
        current_state["view_mode"]      = new_mode
        current_state["current_detail"] = open_sym
        state_snapshot = dict(current_state)  # für Rendering ausserhalb Lock

    # Rendern + editMessageText
    if new_mode == "overview":
        new_text = format_slot_overview(state_snapshot)
        new_kb   = build_slot_keyboard(state_snapshot, mode="overview")
    elif new_mode == "more":
        new_text = format_slot_more(state_snapshot)
        new_kb   = build_slot_keyboard(state_snapshot, mode="more")
    else:  # detail
        new_text = format_slot_detail(state_snapshot, open_sym)
        new_kb   = build_slot_keyboard(
            state_snapshot, open_symbol=open_sym, mode="detail"
        )

    ok = telegram_edit_message(msg_id, new_text, reply_markup=new_kb)
    if ok:
        telegram_answer_callback(callback_id)
    else:
        telegram_answer_callback(
            callback_id, "⚠️ Update fehlgeschlagen", show_alert=False
        )


# ═══════════════════════════════════════════════════════════════
# v4.20: ENTRY-QUEUE TRACKING
#
# Zweck: Jede vom Scorer bewertete Entscheidung (samt vollem Kontext)
# wird in entry_queue_log.csv persistiert. Bei tatsächlicher Trade-
# Eröffnung wird die zugehörige Zeile mit 'taken=1' + trade_id annotiert.
# Beim Close wird der Outcome (exit_price, pnl, r_multiple, duration,
# close_reason) zurückgeschrieben.
#
# Finanzmathematisches Fundament:
#   • R-Multiple (R) = (exit − entry) / (entry − sl) für Long,
#     negiert für Short. Macht Trades über Symbole/Hebel vergleichbar.
#   • Expected Value: E[R] = Σ R_i / N — die eigentliche Metrik die
#     maximiert werden soll (nicht bloße Win-Rate).
#   • Wilson-LCB: ehrliche untere 95%-Schätzung der Win-Rate, robust
#     gegen kleines N (verhindert z.B. 3/3 = "100% WR" Fehlinterpretation).
#   • Kelly-Fraktion: f* = (p·b − (1−p)) / b wobei b = avg_R_win /
#     |avg_R_loss|. Zeigt optimale Position-Gewichtung pro Setup-Klasse.
#   • Survivorship-Bias-Control: auch NICHT getradete Kandidaten werden
#     geloggt → Basis um zu erkennen ob die Auswahl-Entscheidung richtig
#     war (Score 70 getraded vs. Score 50 übergangen → hätte Score 50
#     profitabler abgeschnitten?).
# ═══════════════════════════════════════════════════════════════

_ENTRY_LOG_HEADER = [
    # Identifikation
    "ts_queue",          # ISO UTC — wann das Signal gescored wurde
    "symbol",
    "direction",
    "trade_id",          # <symbol>_<direction>_<ts_queue_int>  (generiert bei enqueue)
    # Score + Kontext
    "score",             # 0–100
    "is_premium",        # 0/1
    "entry",
    "sl",
    "leverage",
    "sl_pct",
    "per_order",
    "confirm_count",
    "macro_ok",          # 0/1
    "m_btc",             # 0/1 — BTC-Richtung matching
    "m_t2",              # 0/1 — Total2-Richtung matching
    "harsi_confirm",     # 0/1
    "fresh_signal",      # 0/1
    "wr",                # Win-Rate Lookup (0.0–1.0) zum Scoring-Zeitpunkt
    "wr_n",              # Anzahl Trades hinter dieser WR
    "breakdown",         # Komma-getrennte Score-Komponenten
    # Ausführung (wird bei mark_trade_taken() gefüllt)
    "taken",             # 0 wenn nicht getradet, 1 wenn Position eröffnet
    "ts_open",           # ISO UTC der Positionseröffnung
    "open_entry",        # Tatsächlicher Entry-Preis (kann vom vorgeschlagenen leicht abweichen)
    "open_sl",           # Tatsächlich gesetzter SL
    "open_leverage",     # Tatsächlicher Hebel
    # Outcome (wird bei update_entry_log_outcome() gefüllt)
    "ts_close",          # ISO UTC des Close
    "duration_sec",      # Dauer zwischen Open und Close
    "exit_price",        # Avg Close-Preis
    "pnl_usdt",
    "r_multiple",        # (exit − entry) / (entry − sl), Long-Konvention
    "won",               # 0/1
    "close_reason",      # TP1/TP2/TP3/TP4/SL/HARSI/manual/unknown
]


def _parse_iso_utc(s: str) -> float:
    """Parst ISO-UTC-String (z.B. '2026-04-21T00:00:00Z') zu Unix-Timestamp.
    Wichtig: .timestamp() auf naiver datetime interpretiert als LOKAL —
    darum explizit UTC-TZ setzen, damit Railway-Container (UTC) und lokale
    Dev-Umgebung dasselbe Ergebnis liefern. Raises bei invalidem Format."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc).timestamp()


def _ensure_aware_utc(dt):
    """v4.29 — Gibt dt als timezone-aware UTC zurück.

    Seit v4.29 werden alle neu erzeugten datetimes mit timezone=UTC erstellt
    (datetime.now(timezone.utc) statt datetime.utcnow()). Der In-Memory-Store
    last_h2_signal_time kann aber nach einem Railway-Redeploy mit einem alten
    State-File geladen werden, in dem Werte noch als naive ISO-Strings ("...
    T00:00:00" ohne +00:00) serialisiert waren — fromisoformat() liefert
    dann naive datetimes. Subtraktion aware-minus-naive raised TypeError.

    Dieser Shim setzt bei naive-Werten tzinfo=UTC und lässt aware-Werte
    unverändert. Der Aufruf ist günstig (Typ-Check + optional .replace) und
    kann defensiv überall gebraucht werden, wo ein Wert aus dem Dict (oder
    ein extern reingereichter dt) subtrahiert/verglichen wird.
    """
    if dt is None:
        return datetime.now(timezone.utc)
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _entry_log_ensure_file() -> None:
    """Legt die Log-CSV an falls nicht vorhanden (mit Header)."""
    if not ENTRY_LOG_CSV:
        return
    try:
        os.makedirs(os.path.dirname(ENTRY_LOG_CSV), exist_ok=True)
    except Exception:
        pass
    if not os.path.isfile(ENTRY_LOG_CSV):
        try:
            with open(ENTRY_LOG_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(_ENTRY_LOG_HEADER)
        except Exception as ex:
            log(f"[entry_log_ensure_file] {ex}")


def _entry_log_read_all() -> list:
    """Liest die Queue-Log-CSV als list[dict]. Gibt [] zurück wenn Datei fehlt."""
    if not ENTRY_LOG_CSV or not os.path.isfile(ENTRY_LOG_CSV):
        return []
    try:
        with open(ENTRY_LOG_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            return list(reader)
    except Exception as ex:
        log(f"[entry_log_read_all] {ex}")
        return []


def _entry_log_write_all(rows: list) -> None:
    """Schreibt alle Zeilen zurück (überschreibt). Caller MUSS _entry_log_lock halten."""
    if not ENTRY_LOG_CSV:
        return
    try:
        os.makedirs(os.path.dirname(ENTRY_LOG_CSV), exist_ok=True)
        tmp_path = ENTRY_LOG_CSV + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_ENTRY_LOG_HEADER, delimiter=";",
                                     extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, ENTRY_LOG_CSV)
    except Exception as ex:
        log(f"[entry_log_write_all] {ex}")


def log_scored_entry(entry: dict) -> None:
    """Persistiert ein gescortes Queue-Signal (non-blocking via Thread).
    Wird aus flush_entries() für jeden Kandidaten gerufen — auch für die,
    die nicht getradet werden (→ Survivorship-Bias-Kontrolle)."""

    def _write():
        with _entry_log_lock:
            _entry_log_ensure_file()
            try:
                s      = entry.get("_scored") or {}
                sugg   = entry.get("sugg") or {}
                tags   = entry.get("tags") or {}
                ts     = entry.get("timestamp") or time.time()
                sym    = entry.get("symbol", "?")
                dr     = entry.get("direction", "?")
                tid    = f"{sym}_{dr}_{int(ts)}"

                dt_iso = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                btc_d  = (entry.get("btc_dir") or "").lower()
                t2_d   = (entry.get("t2_dir")  or "").lower()

                def _dir_ok(d: str) -> int:
                    if dr == "long":  return 1 if d in ("long", "recovering") else 0
                    if dr == "short": return 1 if d in ("short", "recovering_short") else 0
                    return 0

                row = {
                    "ts_queue":       dt_iso,
                    "symbol":         sym,
                    "direction":      dr,
                    "trade_id":       tid,
                    "score":          s.get("score", 0),
                    "is_premium":     1 if s.get("is_premium") else 0,
                    "entry":          entry.get("entry", 0),
                    "sl":             sugg.get("sl", 0),
                    "leverage":       sugg.get("leverage", 0),
                    "sl_pct":         round(float(sugg.get("sl_dist_pct", 0) or 0), 3),
                    "per_order":      round(float(sugg.get("per_order", 0) or 0), 2),
                    "confirm_count":  entry.get("confirm_count", 1),
                    "macro_ok":       1 if tags.get("macro_ok") else 0,
                    "m_btc":          _dir_ok(btc_d),
                    "m_t2":           _dir_ok(t2_d),
                    "harsi_confirm":  1 if tags.get("harsi_confirm") else 0,
                    "fresh_signal":   1 if tags.get("fresh_signal") else 0,
                    "wr":             round(float(s.get("wr", 0) or 0), 4),
                    "wr_n":           int(s.get("wr_n", 0) or 0),
                    "breakdown":      " | ".join(s.get("breakdown") or []),
                    # Ausführung + Outcome bleiben leer bis mark_trade_taken /
                    # update_entry_log_outcome angerufen werden.
                    "taken": 0, "ts_open": "", "open_entry": "", "open_sl": "",
                    "open_leverage": "", "ts_close": "", "duration_sec": "",
                    "exit_price": "", "pnl_usdt": "", "r_multiple": "",
                    "won": "", "close_reason": "",
                }

                with open(ENTRY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=_ENTRY_LOG_HEADER,
                                             delimiter=";", extrasaction="ignore")
                    writer.writerow(row)
            except Exception as ex:
                log(f"[log_scored_entry] {ex}")

    threading.Thread(target=_write, daemon=True).start()


def mark_trade_taken(symbol: str, direction: str,
                      open_entry: float, open_sl: float,
                      open_leverage: int) -> None:
    """Wenn eine Position aufmacht, wird die jüngste passende
    (symbol+direction, taken=0, score innerhalb _ENTRY_MATCH_WINDOW_SEC)
    Zeile im Queue-Log auf taken=1 gesetzt und mit Open-Preisen annotiert.
    Gibt es keinen passenden Queue-Eintrag (z.B. manueller Trade ohne
    vorheriges Signal), passiert nichts — der Trade wird nur im trades.csv
    archiviert, nicht im Queue-Log."""
    if not ENTRY_LOG_CSV:
        return
    with _entry_log_lock:
        rows = _entry_log_read_all()
        if not rows:
            return
        now = time.time()
        cutoff_ts_ns = now - _ENTRY_MATCH_WINDOW_SEC

        # jüngste matchende pending Zeile finden (rückwärts iterieren)
        target_idx = -1
        for i in range(len(rows) - 1, -1, -1):
            r = rows[i]
            if r.get("symbol") != symbol:          continue
            if r.get("direction") != direction:    continue
            if str(r.get("taken", "0")) == "1":    continue
            # Timestamp parsen
            try:
                ts_q = _parse_iso_utc(r.get("ts_queue", ""))
            except Exception:
                continue
            if ts_q < cutoff_ts_ns:
                break  # älter als Fenster → abbrechen
            target_idx = i
            break

        if target_idx < 0:
            log(f"[mark_trade_taken] kein Queue-Eintrag für "
                f"{symbol} {direction} in {_ENTRY_MATCH_WINDOW_SEC//60} Min gefunden")
            return

        r = rows[target_idx]
        r["taken"]         = 1
        r["ts_open"]       = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        r["open_entry"]    = open_entry
        r["open_sl"]       = open_sl
        r["open_leverage"] = open_leverage
        _entry_log_write_all(rows)
        log(f"[mark_trade_taken] {symbol} {direction} → Queue-Eintrag markiert (score={r.get('score')})")


def calc_r_multiple(entry: float, sl: float, exit_price: float,
                     direction: str) -> float:
    """R-Multiple = Profit/Loss in Einheiten des initialen Risikos.
    Long:  R = (exit - entry) / (entry - sl)   [sl < entry → entry-sl > 0]
    Short: R = (entry - exit) / (sl - entry)   [sl > entry → sl-entry > 0]
    Macht Trades über Symbole, Hebel und Positionsgrößen vergleichbar.
    Unabhängig vom Hebel — R misst den SL-Abstand als Risiko-Einheit."""
    try:
        entry = float(entry); sl = float(sl); exit_price = float(exit_price)
    except Exception:
        return 0.0
    if direction == "long":
        risk = entry - sl
        return (exit_price - entry) / risk if risk > 0 else 0.0
    else:
        risk = sl - entry
        return (entry - exit_price) / risk if risk > 0 else 0.0


def update_entry_log_outcome(symbol: str, direction: str,
                              exit_price: float, pnl_usdt: float,
                              ts_close: float, won: bool,
                              close_reason: str = "unknown") -> None:
    """Annotiert die jüngste getakte (taken=1, exit_price="") Zeile im
    Queue-Log mit Close-Daten. Wird direkt nach csv_log_trade() in der
    Close-Routine aufgerufen."""
    if not ENTRY_LOG_CSV:
        return
    with _entry_log_lock:
        rows = _entry_log_read_all()
        if not rows:
            return

        target_idx = -1
        for i in range(len(rows) - 1, -1, -1):
            r = rows[i]
            if r.get("symbol") != symbol:          continue
            if r.get("direction") != direction:    continue
            if str(r.get("taken", "0")) != "1":    continue
            if r.get("exit_price", "") != "":      continue
            target_idx = i
            break

        if target_idx < 0:
            log(f"[update_entry_log_outcome] kein offener Queue-Eintrag "
                f"für {symbol} {direction} gefunden — Close nicht im Queue-Log annotiert")
            return

        r = rows[target_idx]
        try:
            open_entry = float(r.get("open_entry") or r.get("entry") or 0)
            open_sl    = float(r.get("open_sl")    or r.get("sl")    or 0)
            r_mult     = calc_r_multiple(open_entry, open_sl, float(exit_price),
                                           direction)
        except Exception:
            r_mult = 0.0

        try:
            ts_open = _parse_iso_utc(r.get("ts_open", ""))
            duration_sec = int(ts_close - ts_open)
        except Exception:
            duration_sec = ""

        r["ts_close"]     = datetime.fromtimestamp(ts_close, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        r["duration_sec"] = duration_sec
        r["exit_price"]   = exit_price
        r["pnl_usdt"]     = round(float(pnl_usdt), 2)
        r["r_multiple"]   = round(r_mult, 3)
        r["won"]          = 1 if won else 0
        r["close_reason"] = close_reason
        _entry_log_write_all(rows)
        log(f"[update_entry_log_outcome] {symbol} {direction} → R={r_mult:+.2f} "
            f"({close_reason})")


# ── Statistik-Helfer ───────────────────────────────────────────

def _wilson_lower_bound(successes: int, n: int, z: float = 1.96) -> float:
    """Untere Grenze des 95%-Wilson-Konfidenzintervalls für eine Erfolgsrate.
    Robust bei kleinem N — verhindert Fehlschlüsse aus wenigen Beobachtungen.
    n=0 → 0.0; 3/3 → ≈0.29 (nicht 1.0); 30/40 → ≈0.60 (nicht 0.75)."""
    if n <= 0:
        return 0.0
    p = successes / n
    denom = 1 + (z**2) / n
    centre = p + (z**2) / (2 * n)
    adj = z * math.sqrt((p * (1 - p) + (z**2) / (4 * n)) / n)
    return max(0.0, (centre - adj) / denom)


def _kelly_fraction(p_win: float, avg_r_win: float, avg_r_loss_abs: float) -> float:
    """Kelly-Fraktion für ein Setup mit gegebener Win-Prob und R-Verteilung.
    f* = (p·b − (1−p)) / b   mit   b = avg_R_win / |avg_R_loss|
    Negativer Kelly → Setup ist finanzmathematisch unrentabel (nicht traden).
    Cap bei 0.25 (25%) um Fat-Tail-Risiken abzufedern."""
    if avg_r_loss_abs <= 0 or avg_r_win <= 0:
        return 0.0
    b = avg_r_win / avg_r_loss_abs
    f = (p_win * b - (1 - p_win)) / b
    return max(-1.0, min(0.25, f))


def _queue_rows_completed(rows: list, days: int = 30) -> list:
    """Filtert Zeilen: nur getradete mit vorhandenem Outcome, innerhalb letzter N Tage."""
    cutoff = time.time() - days * 86400
    out = []
    for r in rows:
        if str(r.get("taken", "0")) != "1":
            continue
        if r.get("r_multiple", "") in ("", None):
            continue
        try:
            ts_q = _parse_iso_utc(r.get("ts_queue", ""))
        except Exception:
            continue
        if ts_q < cutoff:
            continue
        out.append(r)
    return out


def _bucket_stats(rows: list, bucket_fn) -> dict:
    """Gruppiert Zeilen nach bucket_fn(row) und berechnet Stats pro Bucket:
    {bucket: {n, wins, wr, wilson_lcb, avg_r, e_r, kelly}}"""
    buckets = {}
    for r in rows:
        try:
            key = bucket_fn(r)
        except Exception:
            continue
        if key is None:
            continue
        buckets.setdefault(key, []).append(r)

    out = {}
    for key, brows in buckets.items():
        n = len(brows)
        try:
            rs = [float(x.get("r_multiple", 0) or 0) for x in brows]
        except Exception:
            rs = []
        wins    = sum(1 for x in brows if str(x.get("won", "0")) == "1")
        avg_r   = sum(rs) / n if n else 0.0
        wr      = wins / n if n else 0.0
        wilson  = _wilson_lower_bound(wins, n)
        wins_r  = [r for r in rs if r > 0]
        losses_r = [abs(r) for r in rs if r < 0]
        avg_win  = sum(wins_r) / len(wins_r) if wins_r else 0.0
        avg_loss = sum(losses_r) / len(losses_r) if losses_r else 0.0
        kelly   = _kelly_fraction(wr, avg_win, avg_loss)
        out[key] = {"n": n, "wins": wins, "wr": wr, "wilson": wilson,
                     "avg_r": avg_r, "e_r": avg_r, "kelly": kelly,
                     "avg_win": avg_win, "avg_loss": avg_loss}
    return out


def _score_bucket(r: dict) -> str:
    """Score-Bucket für Attribution."""
    try:
        s = int(r.get("score", 0) or 0)
    except Exception:
        return "?"
    if s <= 20:  return "00–20"
    if s <= 40:  return "21–40"
    if s <= 60:  return "41–60"
    if s <= 80:  return "61–80"
    return "81–100"


def queue_stats_report(days: int = 30) -> str:
    """Baut den HTML-Report für /queue_stats — liest entry_queue_log.csv,
    aggregiert nach R-Multiple und gibt die wichtigsten Kennzahlen aus."""
    if not ENTRY_LOG_CSV or not os.path.isfile(ENTRY_LOG_CSV):
        return ("📊 <b>Queue-Stats</b>\n\n"
                "<i>Noch keine Daten — Queue-Log-CSV existiert nicht oder ist leer.</i>")

    with _entry_log_lock:
        rows = _entry_log_read_all()

    if not rows:
        return "📊 <b>Queue-Stats</b>\n\n<i>Noch keine Queue-Einträge.</i>"

    # Alle Signale der letzten N Tage
    cutoff = time.time() - days * 86400
    recent = []
    for r in rows:
        try:
            ts_q = _parse_iso_utc(r.get("ts_queue", ""))
        except Exception:
            continue
        if ts_q >= cutoff:
            recent.append(r)

    n_total  = len(recent)
    n_taken  = sum(1 for r in recent if str(r.get("taken", "0")) == "1")
    n_prem   = sum(1 for r in recent if str(r.get("is_premium", "0")) == "1")
    take_rate = (n_taken / n_total * 100) if n_total else 0.0

    completed = _queue_rows_completed(rows, days=days)
    n_c = len(completed)

    lines = [
        f"📊 <b>Queue-Stats — letzte {days} Tage</b>",
        "━━━━━━━━━━━━",
        f"Signale: <b>{n_total}</b>  ·  getradet: <b>{n_taken}</b> "
        f"({take_rate:.0f}%)  ·  ⭐ Premium: <b>{n_prem}</b>",
    ]

    if n_c == 0:
        lines.append("")
        lines.append("<i>Noch keine geschlossenen Trades im Zeitraum — "
                     "Stats erscheinen sobald die ersten Positionen schließen.</i>")
        return "\n".join(lines)

    # ── Gesamt-Performance ────────────────────────────────
    rs      = [float(r.get("r_multiple", 0) or 0) for r in completed]
    wins    = sum(1 for r in completed if str(r.get("won", "0")) == "1")
    wr      = wins / n_c
    wilson  = _wilson_lower_bound(wins, n_c)
    avg_r   = sum(rs) / n_c
    wins_r  = [r for r in rs if r > 0]
    loss_r  = [abs(r) for r in rs if r < 0]
    avg_win = sum(wins_r) / len(wins_r) if wins_r else 0.0
    avg_los = sum(loss_r) / len(loss_r) if loss_r else 0.0
    kelly   = _kelly_fraction(wr, avg_win, avg_los)
    total_pnl = sum(float(r.get("pnl_usdt", 0) or 0) for r in completed)

    lines.extend([
        "",
        f"<b>Geschlossene Trades: {n_c}</b>",
        f"  Win-Rate   : <b>{wr*100:.1f}%</b>  "
        f"(Wilson 95% LCB: {wilson*100:.1f}%)",
        f"  E[R]       : <b>{avg_r:+.2f}R</b>  "
        f"(pro Trade, risiko-normalisiert)",
        f"  Avg Win/Loss: +{avg_win:.2f}R / −{avg_los:.2f}R",
        f"  Kelly f*   : <b>{kelly*100:.1f}%</b> "
        f"({'traden' if kelly > 0 else '⚠ unprofitabel'})",
        f"  Netto PnL  : {total_pnl:+.2f} USDT",
    ])

    # ── Score-Bucket Attribution ──────────────────────────
    by_score = _bucket_stats(completed, _score_bucket)
    if by_score:
        lines.append("")
        lines.append("<b>Score-Buckets</b> (Attribution der Gewichte)")
        for key in ["81–100", "61–80", "41–60", "21–40", "00–20"]:
            b = by_score.get(key)
            if not b:
                continue
            marker = " ⚠" if b["n"] < 5 else ""
            lines.append(
                f"  {key}: n={b['n']}{marker} · WR {b['wr']*100:.0f}% · "
                f"E[R] <b>{b['e_r']:+.2f}</b>"
            )
        if any(b["n"] < 5 for b in by_score.values()):
            lines.append("  <i>⚠ n&lt;5 — statistisch nicht belastbar</i>")

    # ── Premium vs. Regular ───────────────────────────────
    by_prem = _bucket_stats(completed, lambda r: "Premium" if str(r.get("is_premium", "0")) == "1" else "Regular")
    if len(by_prem) >= 1:
        lines.append("")
        lines.append("<b>Premium vs. Regular</b>")
        for key in ["Premium", "Regular"]:
            b = by_prem.get(key)
            if not b:
                continue
            marker = " ⚠" if b["n"] < 5 else ""
            lines.append(
                f"  {key}: n={b['n']}{marker} · WR {b['wr']*100:.0f}% · "
                f"E[R] <b>{b['e_r']:+.2f}</b> · Kelly {b['kelly']*100:.0f}%"
            )

    # ── Close-Reason Breakdown ─────────────────────────────
    by_reason = _bucket_stats(completed, lambda r: (r.get("close_reason") or "unknown").upper())
    if by_reason:
        lines.append("")
        lines.append("<b>Close-Gründe</b>")
        for key, b in sorted(by_reason.items(), key=lambda kv: -kv[1]["n"]):
            lines.append(f"  {key}: n={b['n']} · E[R] {b['e_r']:+.2f}")

    lines.append("")
    lines.append("━━━━━━━━━━━━")
    lines.append("💡 <i>E[R] ist die Leitmetrik. Score-Buckets mit "
                 "negativem E[R] → Gewichte reviewen.</i>")

    return "\n".join(lines)


def cmd_queue_stats(parts: list = None):
    """Telegram-Command /queue_stats [tage]  — Default: 30 Tage."""
    days = 30
    if parts and len(parts) >= 2:
        try:
            days = max(1, min(365, int(parts[1])))
        except Exception:
            pass
    try:
        msg = queue_stats_report(days=days)
    except Exception as ex:
        msg = f"❌ <b>Queue-Stats Fehler</b>\n<code>{html.escape(str(ex))}</code>"
    telegram(msg)


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
    # v4.12: Hard-Cap Hebel
    if leverage > MAX_LEVERAGE:
        warnings.append(
            f"⛔ Hebel {leverage}x > MAX_LEVERAGE {MAX_LEVERAGE}x — "
            f"Setup wird von Bitget auf {MAX_LEVERAGE}x gecappt!"
        )
    # v4.12: Exposure-Cap — Gesamt-Einsatz (3× Margin × Hebel) ≤ MAX_EXPOSURE_PCT × 10 × Balance
    # Bei 25%-Cap und 10%-Margin-Regel darf Hebel × 3 ≤ 2.5 × 10 = 25 sein.
    if balance > 0:
        total_notional = per_order * 3 * leverage
        cap_notional   = balance * MAX_EXPOSURE_PCT * 10
        if total_notional > cap_notional * 1.02:   # 2% Toleranz
            warnings.append(
                f"⛔ Exposure-Cap überschritten: "
                f"Gesamt-Einsatz {total_notional:.0f} USDT > "
                f"Cap {cap_notional:.0f} USDT "
                f"({MAX_EXPOSURE_PCT*100:.0f}% Equity × 10)\n"
                f"   → Hebel oder Margin reduzieren"
            )

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

    lines += [
        "",
        "✔︎ HARSI nicht in Extremzone?",
        "✔︎ DOMINUS Impuls im Premium-Bereich?",
        "✔︎ BTC + Total2 gleiche Richtung?",
        "",
        "✅ Wenn ja: Market Order + SL auf Bitget setzen.",
        "Script setzt DCA + TPs automatisch.",
    ]
    reply("\n".join(lines), reply_markup=build_setup_buttons(symbol))


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

    # v4.29: datetime.utcnow() Deprecation-Fix — timezone-aware UTC
    ts = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

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
    # v4.29: timezone-aware; _ensure_aware_utc(ts) schützt vor Legacy-Dict-Werten
    elapsed_sec = (datetime.now(timezone.utc) - _ensure_aware_utc(ts)).total_seconds()
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
    # v4.29: timezone-aware UTC — Werte aus Dict bei Bedarf shim'en
    now = datetime.now(timezone.utc)
    for key, ts in list(last_h2_signal_time.items()):
        ts = _ensure_aware_utc(ts)
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


def _alarm_click_cmd(sub: str, symbol: str = "", direction: str = "") -> str:
    """
    Erzeugt einen Telegram-klickbaren Alias-Command für eine Alarm-Vorlage.
    Telegram macht nur /wort_ohne_leerzeichen automatisch tippbar — daher
    kodieren wir die Args mit Unterstrichen:

        /alarm_harsi_BTCUSDT_LONG
        /alarm_harsisl_ETHUSDT_SHORT
        /alarm_h2_SOLUSDT_LONG
        /alarm_h4_LONG
    """
    parts = ["/alarm", sub]
    if symbol:
        parts.append(symbol.upper())
    if direction:
        parts.append(direction.upper())
    return "_".join(parts).replace("/_", "/")


def cmd_alarm(parts: list):
    """
    Copy-Paste ready Alarm-Vorlagen für TradingView.

    /alarm                             → Übersicht & aktive HARSI-Fenster
    /alarm SYMBOL LONG|SHORT           → Kurzform → Alarm 3/3b (HARSI Exit)
    /alarm harsi  SYMBOL LONG|SHORT    → Alarm 3/3b (HARSI_EXIT) inkl. 30-Min-Status
    /alarm harsisl SYMBOL LONG|SHORT   → Alarm 4/4b (HARSI_SL, für offene Trades)
    /alarm h2     SYMBOL LONG|SHORT    → Alarm 2/2b (H2_SIGNAL Entry)
    /alarm h4     LONG|SHORT           → Alarm 1/1b (H4_TRIGGER Watchlist)

    Tippbare Aliasse (Telegram-Klick):
      /alarm_harsi_SYMBOL_DIR  /alarm_harsisl_SYMBOL_DIR
      /alarm_h2_SYMBOL_DIR     /alarm_h4_DIR
    """
    # 1) Keine Args → Übersicht
    if len(parts) == 1:
        # Offene Positionen für Symbol-spezifische Klick-Aliasse ermitteln
        try:
            _open_syms = [p.get("symbol", "") for p in (get_all_positions() or []) if p.get("symbol")]
        except Exception:
            _open_syms = []

        lines = [
            "🔔 <b>Alarm-Vorlagen — Copy &amp; Paste für TradingView</b>",
            "━" * 12,
            "",
            "<b>Syntax (mit Platzhaltern — Copy &amp; edit):</b>",
            "<code>/alarm SYMBOL LONG|SHORT</code> — Kurzform → HARSI Exit (Alarm 3/3b)",
            "<code>/alarm harsi SYMBOL LONG|SHORT</code> — Alarm 3/3b (HARSI Exit)",
            "<code>/alarm harsisl SYMBOL LONG|SHORT</code> — Alarm 4/4b (HARSI SL)",
            "<code>/alarm h2 SYMBOL LONG|SHORT</code> — Alarm 2/2b (H2 Entry)",
            "<code>/alarm h4 LONG|SHORT</code> — Alarm 1/1b (H4 Watchlist)",
            "",
            "<b>⚡ Klickbar (H4 Watchlist — keine Args nötig):</b>",
            f"  {_alarm_click_cmd('h4', direction='LONG')}",
            f"  {_alarm_click_cmd('h4', direction='SHORT')}",
        ]

        # Symbol-spezifische Klick-Aliasse für alle offenen Positionen
        if _open_syms:
            lines += ["", "<b>⚡ Klickbar für offene Positionen (HARSI SL):</b>"]
            for _sym in _open_syms:
                lines.append(f"  {_alarm_click_cmd('harsisl', _sym, 'LONG')}")
                lines.append(f"  {_alarm_click_cmd('harsisl', _sym, 'SHORT')}")

        lines.append("")
        active = _list_active_harsi_windows()
        if active:
            lines.append("<b>🟢 Aktive HARSI-Fenster (30-Min-Timer läuft):</b>")
            for sym, drct, rem, exp in active:
                icon = dir_icon(drct)
                lines.append(f"  {icon} <b>{sym}</b> {drct.upper()} — noch {rem} Min (bis {exp})")
                # Tippbare Alias-Commands (Telegram erkennt /xxx als Link)
                lines.append(
                    f"     ⚡ {_alarm_click_cmd('harsi', sym, drct)}  "
                    f"| {_alarm_click_cmd('harsisl', sym, drct)}  "
                    f"| {_alarm_click_cmd('h2', sym, drct)}"
                )
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
        "/queue_stats [tage] — Entry-Queue Auswertung (E[R], Kelly, Score-Buckets)\n"
        "\n"
        "⚙️ <b>Aktionen:</b>\n"
        "/trade SYMBOL LONG|SHORT HEBEL ENTRY SL\n"
        "   → Setup berechnen + alle Chart-Links\n"
        "   Beispiel: /trade ETHUSDT LONG 10 2850 2700\n"
        "/refresh [SYMBOL] — SL/TP/DCA sofort prüfen &amp; reparieren\n"
        "   Beispiel: /refresh BTCUSDT  (oder /refresh für alle)\n"
        "/dedup_trades [apply] — Duplikate im Trade-Archiv suchen\n"
        "   ohne Argument: Dry-Run | mit <code>apply</code>: bereinigen\n"
        "\n"
        "📦 <b>Archiv-Transfer (v4.34):</b>\n"
        "/trades — trades.csv als Telegram-Dokument herunterladen\n"
        "/restore_trades — Anleitung: bereinigte CSV wieder hochladen\n"
        "   (Datei anhängen + Caption <code>/restore_trades</code>)\n"
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


def infer_trailing_level(symbol: str, direction: str,
                         entry: float, leverage: int) -> int:
    """
    v4.31 — Leitet den Trailing-SL-Level aus dem IST-SL auf Bitget ab und
    heilt den State-Dict `trailing_sl_level[symbol]` falls er hinterherhinkt.

    Hintergrund: Nach einem Railway-Redeploy, nach manuellem SL-Move auf
    Bitget oder bei einem TP-Cascade-Miss (z.B. das Script war kurz offline)
    kann `trailing_sl_level[symbol]` den tatsächlichen SL-Stand unter-
    schätzen. `/status` und `/berechnen` bauten ihre Anzeige auf dem Dict
    auf — Resultat: "SL=Entry", obwohl der reale SL bereits auf TP1 stand.

    Rückgabe-Levels:
      0 = SL unter Entry (ungesichert)     — noch kein TP gehittet
      1 = SL @ Entry (Break-even)          — TP1 erreicht
      2 = SL @ TP1   (Gewinn gesichert)    — TP2 erreicht
      3 = SL @ TP2   (fett gesichert)      — TP3 erreicht

    Heilung erfolgt nur **nach oben**: ein kurzzeitig fehlender SL (z.B.
    während einer Neusetzung) darf nicht zu Rückstufung führen.

    Fallback: Ist der SL nicht lesbar (Bitget-API-Fehler, keine Order),
    wird der bisherige State-Dict-Wert zurückgegeben — Status-Anzeige
    bleibt dann "stale aber bekannt", nicht "falsch konservativ".
    """
    stored = trailing_sl_level.get(symbol, 0)
    if entry <= 0 or leverage <= 0 or direction not in ("long", "short"):
        return stored
    try:
        sl = get_sl_price(symbol, direction)
    except Exception as _e:
        log(f"  ⚠ infer_trailing_level({symbol}): get_sl_price fehlgeschlagen: {_e}")
        return stored
    if sl <= 0:
        # Kein lesbarer SL → Dict-Wert behalten (nicht auf 0 zurückfallen).
        return stored

    tp1 = calc_tp_price(entry, TP1_ROI, direction, leverage)
    tp2 = calc_tp_price(entry, TP2_ROI, direction, leverage)

    # Toleranz: Bitget rundet TP-Preise auf Tick-Decimals und der SL wird
    # oft in round_price() getrimmt → 0.25% Toleranz deckt das ab.
    TOL_ENTRY = 0.0015
    TOL_TP    = 0.0025

    if direction == "long":
        # Je höher der SL, desto weiter getrailt.
        if sl >= tp2 * (1 - TOL_TP):
            level = 3
        elif sl >= tp1 * (1 - TOL_TP):
            level = 2
        elif sl >= entry * (1 - TOL_ENTRY):
            level = 1
        else:
            level = 0
    else:  # short
        if sl <= tp2 * (1 + TOL_TP):
            level = 3
        elif sl <= tp1 * (1 + TOL_TP):
            level = 2
        elif sl <= entry * (1 + TOL_ENTRY):
            level = 1
        else:
            level = 0

    if level > stored:
        trailing_sl_level[symbol] = level
        if level >= 1:
            sl_at_entry[symbol] = True
        try:
            save_state()
        except Exception:
            pass
        log(f"  🔧 SL-Level geheilt: {symbol} → Level {level} "
            f"(SL={sl} · Entry={entry:.6g} · TP1={tp1:.6g} · TP2={tp2:.6g})")
        return level
    return stored


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
        lev      = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
        pnl      = float(pos.get("unrealizedPL", 0))
        mark     = get_mark_price(sym)
        # v4.31: Trailing-Level aus IST-SL auf Bitget ableiten (heilt State
        # nach Railway-Redeploy oder manuellem SL-Move direkt auf Bitget).
        _entry_avg = float(pos.get("openPriceAvg", 0) or 0)
        trl_lvl  = infer_trailing_level(sym, drct.lower(), _entry_avg, lev)
        secured  = sl_at_entry.get(sym, False) or trl_lvl >= 1
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
        lev      = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
        avg      = float(pos.get("openPriceAvg", 0))
        qty      = float(pos.get("total", 0))
        mark     = get_mark_price(sym)
        sl       = get_sl_price(sym, pos.get("holdSide", "long"))
        tps      = get_existing_tps(sym)
        n_tp     = len(tps)
        # v4.18 — TP4 (Full-Close via place-pos-tpsl) ist KEIN profit_plan
        # und erscheint NICHT in get_existing_tps(). Wir ermitteln ihn separat
        # und zählen ihn zur Anzeige hinzu, damit Telegram die tatsächliche
        # Anzahl TPs auf Bitget reflektiert (erwartet max. 4: TP1+TP2+TP3+TP4).
        tp4_price = _get_pos_tp_price(sym, pos.get("holdSide", "long"))
        tp4_set   = tp4_price > 0
        n_tp_total = n_tp + (1 if tp4_set else 0)
        tp4_info  = f" (inkl. TP4 @ {tp4_price})" if tp4_set else " (ohne TP4)"
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
            f"   {lock} | TPs gesetzt: {n_tp_total}/4{tp4_info}"
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
        # v4.33: Break-even (net_pnl == 0) zählt nicht mehr als Verlust.
        # Bisher wurde losses = len - wins gerechnet → Phantom-Close-Artefakte
        # (net_pnl=0) landeten fälschlich im Loss-Bucket und verfälschten die
        # Win-Rate. Jetzt wird rein P&L-basiert klassifiziert: >0 = Win,
        # <0 = Loss, =0 = Break-even (extra ausgewiesen).
        # Win-Rate ignoriert Break-evens im Nenner, damit flache Trades die
        # Quote nicht künstlich drücken.
        if not trades:
            return {"count": 0, "wins": 0, "losses": 0, "breakeven": 0,
                    "total_pnl": 0.0, "win_rate": 0.0, "best": 0.0, "worst": 0.0}
        pnls      = [float(t.get("net_pnl", 0)) for t in trades]
        wins      = sum(1 for p in pnls if p > 0)
        losses    = sum(1 for p in pnls if p < 0)
        breakeven = sum(1 for p in pnls if p == 0)
        decided   = wins + losses   # nur Trades mit echtem Outcome
        total     = sum(pnls)
        best      = max(pnls) if pnls else 0.0
        worst     = min(pnls) if pnls else 0.0
        return {
            "count":     len(trades),
            "wins":      wins,
            "losses":    losses,
            "breakeven": breakeven,
            "total_pnl": total,
            "win_rate":  wins / decided * 100 if decided else 0.0,
            "best":      best,
            "worst":     worst,
        }

    day_s   = _summary(day_trades)
    month_s = _summary(month_trades)

    # Offene Positionen
    open_positions = get_all_positions()

    # v4.33: Summary-Zeile fügt Break-even nur hinzu wenn > 0, damit normale
    # Tage (alle entschieden) visuell unverändert bleiben.
    def _summary_line(s: dict) -> str:
        base = f"Trades: {s['count']}  |  🏆 {s['wins']} / 🔴 {s['losses']}"
        if s.get("breakeven", 0) > 0:
            base += f" / ⚪ {s['breakeven']} Break-even"
        return base

    lines = [
        f"📊 <b>DOMINUS Daily Report — {date_str}</b>",
        "━━━━━━━━━━━━",
        f"",
        f"📅 <b>Heute ({date_str}):</b>",
        _summary_line(day_s),
        f"Win-Rate: {day_s['win_rate']:.0f}%",
        f"Netto P&L: {day_s['total_pnl']:+.2f} USDT",
        f"Bester: {day_s['best']:+.2f} USDT  |  Schlechtester: {day_s['worst']:+.2f} USDT",
    ]

    if day_trades:
        lines.append("")
        lines.append("📋 <b>Trades heute:</b>")
        for t in day_trades:
            # v4.33: Icon rein P&L-basiert. Break-even (0.00) = ⚪,
            # damit Phantom-Artefakte nicht mehr fälschlich als 🔴 erscheinen.
            _pnl = float(t.get('net_pnl', 0))
            if _pnl > 0:
                icon = "🏆"
            elif _pnl < 0:
                icon = "🔴"
            else:
                icon = "⚪"
            lines.append(
                f"  {icon} {t['symbol']} {t.get('direction','?').upper()} "
                f"| {_pnl:+.2f} USDT "
                f"| {t.get('hold_str','?')}"
            )

    lines += [
        "",
        f"📆 <b>Monat ({month_str}):</b>",
        _summary_line(month_s),
        f"Win-Rate: {month_s['win_rate']:.0f}%",
        f"Netto P&L: {month_s['total_pnl']:+.2f} USDT",
    ]

    if open_positions:
        lines.append("")
        lines.append(f"📈 <b>Offene Positionen ({len(open_positions)}):</b>")
        for pos in open_positions:
            sym  = pos.get("symbol", "?")
            drct = pos.get("holdSide", "?").upper()
            lev  = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
            pnl  = float(pos.get("unrealizedPL", 0))
            # v4.31: IST-SL live abfragen statt Dict blind zu glauben
            _avg = float(pos.get("openPriceAvg", 0) or 0)
            trl  = infer_trailing_level(sym, drct.lower(), _avg, lev)
            trl_tag = {0: "", 1: " SL=Entry", 2: " Trail→TP1", 3: " Trail→TP2"}.get(trl, "")
            lines.append(f"  • {sym} {drct} {lev}x | PnL={pnl:+.2f}{trl_tag}")
    else:
        lines.append("")
        lines.append("✅ Keine offenen Positionen")

    return "\n".join(lines)


def cmd_dedup_trades(parts: list = None):
    """
    /dedup_trades [apply] — Räumt Duplikate aus dem Trade-Archiv.

    Ohne Argument: DRY-RUN — zeigt gefundene Duplikate an, ändert nichts.
    Mit `apply`:   Bereinigt trades.csv + In-Memory closed_trades[].

    Duplikat-Definition: Gleicher Symbol + Richtung + Entry + Close + PnL
    innerhalb eines 2-Stunden-Fensters (gleiche Schliessung, mehrfach durch
    Railway-Restarts erfasst). Bei Match wird der **älteste** Eintrag behalten.
    """
    apply = bool(parts and len(parts) > 1 and parts[1].lower() in ("apply", "yes", "ja", "--apply"))

    if not TRADES_CSV or not os.path.isfile(TRADES_CSV):
        reply(f"❌ Kein Trade-Archiv gefunden unter <code>{TRADES_CSV}</code>")
        return

    # CSV einlesen
    try:
        with open(TRADES_CSV, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)
    except Exception as e:
        reply(f"❌ CSV-Lesefehler: {e}")
        return

    if len(rows) < 2:
        reply("ℹ️ Trade-Archiv ist leer — nichts zu bereinigen.")
        return

    header = rows[0]
    data_rows = rows[1:]

    # Spaltenindizes bestimmen
    try:
        idx_datum  = header.index("Datum")
        idx_zeit   = header.index("Zeit (UTC)")
        idx_sym    = header.index("Symbol")
        idx_dir    = header.index("Richtung")
        idx_entry  = header.index("Entry")
        idx_close  = header.index("Close")
        idx_pnl    = header.index("PnL USDT")
    except ValueError as e:
        reply(f"❌ CSV-Header unvollständig: {e}")
        return

    def _parse_ts(r):
        try:
            dt = datetime.strptime(
                f"{r[idx_datum]} {r[idx_zeit]}", "%d.%m.%Y %H:%M"
            ).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    # Stabil nach Timestamp sortieren, Ursprungs-Index als Sekundärschlüssel
    indexed = list(enumerate(data_rows))
    indexed.sort(key=lambda x: (_parse_ts(x[1]), x[0]))

    kept_rows      = []
    duplicate_info = []   # Liste von (original_idx, kept_key, kept_ts, dup_ts)
    seen           = {}   # key → (ts, list_index_in_kept_rows)
    dedup_window   = 2 * 3600  # 2 h

    def _key(r):
        try:
            return (
                r[idx_sym].strip(),
                r[idx_dir].strip().upper(),
                round(float(r[idx_entry] or 0), 8),
                round(float(r[idx_close] or 0), 8),
                round(float(r[idx_pnl]  or 0), 4),
            )
        except Exception:
            return (r[idx_sym].strip(), r[idx_dir].strip().upper(), r[idx_entry], r[idx_close], r[idx_pnl])

    for orig_idx, r in indexed:
        k   = _key(r)
        ts  = _parse_ts(r)
        prev = seen.get(k)
        if prev and (ts - prev[0]) < dedup_window:
            # Duplikat → verwerfen
            duplicate_info.append({
                "symbol":   r[idx_sym],
                "direction": r[idx_dir],
                "pnl":      r[idx_pnl],
                "datum":    r[idx_datum],
                "zeit":     r[idx_zeit],
                "orig_ts":  datetime.fromtimestamp(prev[0], timezone.utc).strftime("%d.%m %H:%M"),
            })
            continue
        seen[k] = (ts, len(kept_rows))
        kept_rows.append(r)

    removed = len(data_rows) - len(kept_rows)

    # ── In-Memory closed_trades ebenfalls deduplizieren (gleiche Logik)
    mem_removed = 0
    global closed_trades
    try:
        mem_sorted = sorted(enumerate(closed_trades), key=lambda x: int(x[1].get("ts", 0) or 0))
        mem_kept   = []
        mem_seen   = {}
        for orig_i, t in mem_sorted:
            try:
                mk = (
                    str(t.get("symbol", "")),
                    str(t.get("direction", "")).upper(),
                    round(float(t.get("entry", 0) or 0), 8),
                    round(float(t.get("close_price", 0) or 0), 8),
                    round(float(t.get("net_pnl", 0) or 0), 4),
                )
            except Exception:
                mk = (str(t.get("symbol", "")), str(t.get("direction", "")))
            mts  = int(t.get("ts", 0) or 0)
            prev = mem_seen.get(mk)
            if prev and (mts - prev) < dedup_window:
                mem_removed += 1
                continue
            mem_seen[mk] = mts
            mem_kept.append(t)
    except Exception as e:
        log(f"[DEDUP] Memory-Dedup Fehler: {e}")
        mem_kept = closed_trades

    # DRY-RUN: nur Rapport
    if not apply:
        if removed == 0 and mem_removed == 0:
            reply(
                "✅ <b>Keine Duplikate gefunden.</b>\n"
                f"• CSV-Zeilen:        {len(data_rows)}\n"
                f"• Memory-Trades:     {len(closed_trades)}\n"
                "\n"
                "📋 /status | /report"
            )
            return
        lines = [
            "🔍 <b>DEDUP Dry-Run</b> (keine Änderungen geschrieben)",
            "━━━━━━━━━━━━",
            f"CSV:     {len(data_rows)} Zeilen → {len(kept_rows)} behalten, <b>{removed} Duplikate</b>",
            f"Memory:  {len(closed_trades)} Trades → {len(mem_kept)} behalten, <b>{mem_removed} Duplikate</b>",
        ]
        if duplicate_info:
            lines.append("")
            lines.append("<b>Gefundene CSV-Duplikate:</b>")
            for d in duplicate_info[:20]:
                lines.append(
                    f"  • {d['symbol']} {d['direction']} {d['pnl']} USDT "
                    f"@ {d['datum']} {d['zeit']} "
                    f"(Original: {d['orig_ts']})"
                )
            if len(duplicate_info) > 20:
                lines.append(f"  … und {len(duplicate_info) - 20} weitere")
        lines += [
            "",
            "⚙️ Zum Bereinigen: /dedup_apply  (tippen/klicken)",
            "   <i>oder Langform:</i> <code>/dedup_trades apply</code>",
            "📋 /status | /report",
        ]
        reply("\n".join(lines))
        return

    # APPLY: Backup + bereinigte CSV schreiben
    ts_tag     = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = f"{TRADES_CSV}.backup_{ts_tag}.csv"
    try:
        import shutil as _shutil
        _shutil.copy2(TRADES_CSV, backup_path)
    except Exception as e:
        reply(f"❌ Backup fehlgeschlagen, Abbruch: {e}")
        return

    try:
        with open(TRADES_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(header)
            # Original-Reihenfolge wiederherstellen (nach Timestamp sortieren)
            kept_rows_sorted = sorted(kept_rows, key=_parse_ts)
            writer.writerows(kept_rows_sorted)
    except Exception as e:
        reply(f"❌ CSV-Schreibfehler: {e}\nBackup liegt unter <code>{backup_path}</code>")
        return

    # Memory-Liste ersetzen
    closed_trades = mem_kept

    log(f"[DEDUP] apply: CSV {removed} entfernt, Memory {mem_removed} entfernt. Backup: {backup_path}")

    reply(
        "🧹 <b>Trade-Archiv bereinigt</b>\n"
        "━━━━━━━━━━━━\n"
        f"CSV:     {len(data_rows)} → {len(kept_rows)} Zeilen ({removed} Duplikate entfernt)\n"
        f"Memory:  {len(data_rows)} → {len(mem_kept)} Trades ({mem_removed} Duplikate entfernt)\n"
        "\n"
        f"💾 Backup: <code>{backup_path}</code>\n"
        "\n"
        "📋 /status | /report"
    )


def cmd_report():
    """Sendet den täglichen P&L Report auf Telegram-Anfrage."""
    try:
        report = build_daily_report()
        report += "\n\n📋 /status | /makro | /berechnen"
        telegram(report)
    except Exception as e:
        reply(f"❌ Report Fehler: {e}")


def cmd_trades():
    """v4.34 — /trades: schickt die aktuelle trades.csv als Telegram-Dokument
    zurück. Caption enthält Zeilenzahl, Dateigrösse und Mtime, damit man
    vor dem Edit lokal weiss, welche Version man in den Händen hält."""
    if not TRADES_CSV:
        reply("❌ TRADES_CSV ist nicht konfiguriert.")
        return
    if not os.path.isfile(TRADES_CSV):
        reply(f"❌ trades.csv nicht gefunden: <code>{TRADES_CSV}</code>")
        return
    try:
        size_bytes = os.path.getsize(TRADES_CSV)
        mtime_str  = datetime.fromtimestamp(os.path.getmtime(TRADES_CSV)) \
                        .strftime("%Y-%m-%d %H:%M:%S")
        # Zeilen zählen (ohne ganze Datei in den RAM zu laden)
        line_count = 0
        with open(TRADES_CSV, "r", encoding="utf-8") as fh:
            for _ in fh:
                line_count += 1
        data_rows = max(0, line_count - 1)  # erste Zeile = Header

        # Human-readable Grösse
        if size_bytes < 1024:
            size_str = f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            size_str = f"{size_bytes / 1024:.1f} KB"
        else:
            size_str = f"{size_bytes / (1024 * 1024):.2f} MB"

        fname   = f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        caption = (
            "📦 <b>Trade-Archiv</b>\n"
            f"━━━━━━━━━━━━\n"
            f"Datei:   <code>{os.path.basename(TRADES_CSV)}</code>\n"
            f"Trades:  <b>{data_rows}</b> Zeile(n) + Header\n"
            f"Grösse:  {size_str}\n"
            f"Mtime:   {mtime_str} (lokal)\n"
            "\n"
            "Zurückspielen: Datei anhängen, Bildunterschrift "
            "<code>/restore_trades</code>."
        )
        ok = telegram_document(TRADES_CSV, caption=caption, filename=fname)
        if not ok:
            reply("❌ sendDocument fehlgeschlagen — siehe Railway-Logs.")
    except Exception as e:
        reply(f"❌ /trades Fehler: {e}")


def handle_trades_restore(document: dict) -> None:
    """v4.34 — /restore_trades: nimmt die an die Nachricht angehängte CSV,
    validiert Header + Endung, legt Backup an und ersetzt TRADES_CSV atomar.
    Aufrufer garantiert, dass `document` gesetzt ist UND die Caption
    genau /restore_trades (case-insensitive, erstes Token) lautet."""
    if not TRADES_CSV:
        reply("❌ TRADES_CSV ist nicht konfiguriert — Restore abgebrochen.")
        return

    file_id  = document.get("file_id") or ""
    fname_in = (document.get("file_name") or "").strip()
    mime     = (document.get("mime_type") or "").lower()
    size_in  = int(document.get("file_size") or 0)

    if not file_id:
        reply("❌ Kein file_id in der Nachricht — Telegram-API-Fehler?")
        return
    # Endung prüfen — Telegram gibt mime_type z.T. 'text/csv' oder 'application/vnd.ms-excel'.
    # Wir verlassen uns primär auf den Dateinamen, weil der verlässlich ist.
    if not fname_in.lower().endswith(".csv"):
        reply(f"❌ Datei muss auf <code>.csv</code> enden — erhalten: "
              f"<code>{fname_in or '(ohne Name)'}</code> ({mime or 'kein MIME'})")
        return
    # Telegram-Bot-getFile hat 20 MB Download-Limit. Wir warnen defensiv ab 18 MB.
    if size_in > 18 * 1024 * 1024:
        reply(f"❌ Datei zu gross für Bot-Download ({size_in/1024/1024:.1f} MB, "
              "Limit ≈ 20 MB). Nutze `railway run` für grosse CSVs.")
        return

    # 1) getFile → file_path holen
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getFile",
            params={"file_id": file_id}, timeout=10,
        )
        data = r.json()
        if not data.get("ok"):
            reply(f"❌ getFile fehlgeschlagen: "
                  f"{data.get('description') or 'unbekannt'}")
            return
        tg_file_path = data["result"]["file_path"]
    except Exception as ex:
        reply(f"❌ getFile-Call fehlgeschlagen: {ex}")
        return

    # 2) Datei herunterladen
    try:
        r = requests.get(
            f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{tg_file_path}",
            timeout=30,
        )
        if r.status_code != 200:
            reply(f"❌ Download HTTP {r.status_code}")
            return
        content_bytes = r.content
    except Exception as ex:
        reply(f"❌ Download fehlgeschlagen: {ex}")
        return

    # 3) Header-Validierung (erste Zeile muss _CSV_HEADER mit ';' matchen)
    try:
        first_line = content_bytes.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    except Exception as ex:
        reply(f"❌ UTF-8-Decode fehlgeschlagen: {ex}")
        return
    first_line = first_line.strip().lstrip("\ufeff")  # BOM toleranter
    expected_header = ";".join(_CSV_HEADER)
    if first_line != expected_header:
        reply(
            "❌ <b>Header-Mismatch — Restore abgebrochen</b>\n"
            f"Erwartet: <code>{expected_header}</code>\n"
            f"Bekommen: <code>{first_line[:200]}</code>"
        )
        return

    # Zeilen der neuen CSV
    new_line_count = content_bytes.count(b"\n")
    if content_bytes and not content_bytes.endswith(b"\n"):
        new_line_count += 1
    new_data_rows = max(0, new_line_count - 1)

    # 4) Backup der existierenden Datei
    backup_path = None
    old_data_rows = 0
    if os.path.isfile(TRADES_CSV):
        try:
            with open(TRADES_CSV, "r", encoding="utf-8") as fh:
                old_data_rows = max(0, sum(1 for _ in fh) - 1)
        except Exception:
            pass
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = f"{TRADES_CSV}.backup_{ts}.csv"
        try:
            shutil.copy2(TRADES_CSV, backup_path)
        except Exception as ex:
            reply(f"❌ Backup fehlgeschlagen ({ex}) — Restore abgebrochen.")
            return

    # 5) Atomar schreiben (tmp + os.replace)
    tmp_path = f"{TRADES_CSV}.tmp_restore"
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(content_bytes)
        os.replace(tmp_path, TRADES_CSV)
    except Exception as ex:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        reply(f"❌ Schreiben fehlgeschlagen: {ex}")
        return

    # 6) Bestätigung
    delta = new_data_rows - old_data_rows
    delta_str = (f"+{delta}" if delta > 0 else str(delta)) if delta != 0 else "±0"
    msg_lines = [
        "✅ <b>trades.csv restored</b>",
        "━━━━━━━━━━━━",
        f"Alte Version:   <b>{old_data_rows}</b> Zeilen",
        f"Neue Version:   <b>{new_data_rows}</b> Zeilen ({delta_str})",
        f"Quelle:         <code>{fname_in}</code>",
    ]
    if backup_path:
        msg_lines.append(f"Backup:         <code>{backup_path}</code>")
    msg_lines += [
        "",
        "📋 Jetzt /report prüfen — Win-Rate und PnL müssen passen.",
    ]
    telegram("\n".join(msg_lines))


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
        # v4.25: Callback-Query für Button-Driven Entry-Rangliste
        if update.get("callback_query"):
            try:
                handle_callback_query(update)
            except Exception as ex:
                log(f"[handle_callback_query] Fehler: {ex}")
            continue

        msg = update.get("message")
        if not msg:
            continue

        # Sicherheit: nur eigene Chat-ID
        chat_id = str(msg.get("chat", {}).get("id", ""))
        if chat_id != str(TELEGRAM_CHAT_ID):
            continue

        # v4.34 — Document-Upload: nur mit Caption /restore_trades verarbeiten.
        # So kippt ein versehentlich an den Bot gesendetes PDF/Bild NICHT
        # die trades.csv um. Das erste Token der Caption muss exakt
        # /restore_trades (case-insensitive) sein.
        document = msg.get("document")
        caption  = (msg.get("caption") or "").strip()
        if document:
            cap_first = caption.split()[0].lower() if caption else ""
            if cap_first in ("/restore_trades", "/restoretrades"):
                log(f"Telegram Restore-Upload empfangen: "
                    f"{document.get('file_name')} ({document.get('file_size')} bytes)")
                try:
                    handle_trades_restore(document)
                except Exception as ex:
                    log(f"[handle_trades_restore] Fehler: {ex}")
                    reply(f"❌ Restore fehlgeschlagen: {ex}")
                continue
            # anderer Document-Upload → ignorieren (kein Match)

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
        elif cmd == "/trades":
            cmd_trades()
        elif cmd == "/restore_trades" or cmd == "/restoretrades":
            # ohne angehängte Datei — nur Anleitung zeigen
            reply(
                "♻️ <b>trades.csv zurückspielen</b>\n"
                "━━━━━━━━━━━━\n"
                "1. Bereinigte <code>trades.csv</code> hier an den Bot anhängen\n"
                "2. Als Bildunterschrift genau <code>/restore_trades</code>\n"
                "3. Bot validiert Header, legt Backup an und ersetzt die Datei\n"
                "\n"
                "Ohne Caption → Upload wird ignoriert (Unfallschutz)."
            )
        elif cmd == "/alarm":
            cmd_alarm(parts)
        elif cmd == "/queue_stats" or cmd == "/queuestats":
            cmd_queue_stats(parts)
        elif cmd == "/dedup_trades" or cmd == "/deduptrades":
            cmd_dedup_trades(parts)
        elif cmd == "/dedup_apply" or cmd == "/dedupapply":
            # Klick-Alias für /dedup_trades apply (Telegram erkennt /xxx
            # automatisch als tippbaren Befehl, /xxx yyy aber nicht mehr).
            cmd_dedup_trades(["/dedup_trades", "apply"])
        elif cmd.startswith("/alarm_") or cmd.startswith("/harsi_") \
             or cmd.startswith("/harsisl_") or cmd.startswith("/h2_") \
             or cmd.startswith("/h4_"):
            # Klick-Aliasse für cmd_alarm() — siehe _alarm_click_cmd() für
            # das Format. Beispiele (alles lowercase nach Normalisierung):
            #   /alarm_harsi_btcusdt_long  → /alarm harsi BTCUSDT LONG
            #   /alarm_harsisl_ethusdt_short
            #   /alarm_h2_solusdt_long
            #   /alarm_h4_long             → /alarm h4 LONG
            #   /harsi_btcusdt_long        → Kurzform (dasselbe)
            if cmd.startswith("/alarm_"):
                _suffix = cmd[len("/alarm_"):]
            else:
                _suffix = cmd[1:]  # führenden Slash entfernen
            _tokens = [t for t in _suffix.split("_") if t]
            if _tokens:
                cmd_alarm(["/alarm"] + _tokens)
            else:
                cmd_alarm(["/alarm"])
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


def _build_dashboard_html(trades_json: str) -> str:
    """Rendert das Performance Dashboard als HTML-String."""
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DOMINUS Demo Performance</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Fraunces:ital,wght@0,300;0,700;1,300&display=swap');
:root{{
  --bg:#0a0c0e;--bg2:#111417;--bg3:#181c20;--border:#252a2f;
  --text:#e8e4dc;--muted:#6b7280;--green:#2ecc71;--red:#e74c3c;
  --amber:#f39c12;--blue:#3498db;--accent:#c9a84c;
  --font:'Fraunces',serif;--mono:'DM Mono',monospace;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:var(--font);min-height:100vh}}
.header{{padding:2.5rem 3rem 2rem;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:flex-end}}
.logo{{font-size:1.8rem;font-weight:700;color:var(--accent);letter-spacing:-.02em}}
.logo span{{color:var(--muted);font-weight:300;font-size:1rem;display:block;letter-spacing:.1em;font-family:var(--mono);margin-top:.2rem}}
.refresh{{font-family:var(--mono);font-size:11px;color:var(--muted);cursor:pointer;border:1px solid var(--border);padding:.4rem .8rem;border-radius:4px;background:transparent;color:var(--muted)}}
.refresh:hover{{border-color:var(--accent);color:var(--accent)}}
.main{{padding:2rem 3rem;max-width:1400px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;margin-bottom:2rem}}
.kpi{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:1.25rem 1.5rem}}
.kpi-label{{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.5rem}}
.kpi-value{{font-size:2rem;font-weight:700;line-height:1}}
.kpi-sub{{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:.3rem}}
.green{{color:var(--green)}}.red{{color:var(--red)}}.amber{{color:var(--amber)}}.blue{{color:var(--blue)}}
.section{{margin-bottom:2.5rem}}
.section-title{{font-family:var(--mono);font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border);padding-bottom:.6rem;margin-bottom:1.2rem}}
.score-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem}}
.score-card{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:1.2rem}}
.score-badge{{font-family:var(--mono);font-size:20px;font-weight:500;margin-bottom:.4rem}}
.score-label{{font-size:.8rem;color:var(--muted);margin-bottom:.6rem}}
.score-bar{{height:4px;border-radius:2px;background:var(--border);margin-bottom:.6rem;overflow:hidden}}
.score-fill{{height:100%;border-radius:2px}}
.score-stats{{font-family:var(--mono);font-size:11px;color:var(--muted)}}
.table-wrap{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;overflow:hidden}}
table{{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:12px}}
th{{padding:.8rem 1rem;text-align:left;border-bottom:1px solid var(--border);color:var(--muted);font-weight:500;font-size:10px;letter-spacing:.08em;text-transform:uppercase}}
td{{padding:.7rem 1rem;border-bottom:1px solid var(--border)}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:var(--bg3)}}
.pill{{display:inline-block;padding:.15rem .5rem;border-radius:3px;font-size:10px;font-weight:500}}
.pill-green{{background:rgba(46,204,113,.15);color:var(--green)}}
.pill-red{{background:rgba(231,76,60,.15);color:var(--red)}}
.pill-amber{{background:rgba(243,156,18,.15);color:var(--amber)}}
.pill-blue{{background:rgba(52,152,219,.15);color:var(--blue)}}
.bar-row{{display:grid;grid-template-columns:100px 1fr 60px;gap:.5rem;align-items:center;padding:.4rem 0}}
.bar-bg{{background:var(--border);border-radius:2px;height:8px;overflow:hidden}}
.bar-val{{height:100%;border-radius:2px;background:var(--green)}}
.empty{{padding:3rem;text-align:center;color:var(--muted);font-style:italic}}
</style>
</head>
<body>
<div class="header">
  <div>
    <div class="logo">DOMINUS <span>DEMO PERFORMANCE DASHBOARD</span></div>
  </div>
  <button class="refresh" onclick="location.reload()">↻ Aktualisieren</button>
</div>
<div class="main">
  <div id="content"><div class="empty">Lade Daten...</div></div>
</div>
<script>
const RAW = {trades_json};

function fmt(n, d=2) {{ return n == null ? '—' : n.toFixed(d); }}
function pct(n) {{ return n == null ? '—' : (n*100).toFixed(1)+'%'; }}
function pill(txt, cls) {{ return `<span class="pill pill-${{cls}}">${{txt}}</span>`; }}

function render(trades) {{
  if (!trades.length) {{
    document.getElementById('content').innerHTML = '<div class="empty">Noch keine Demo-Trades gespeichert.<br>Warte auf das erste Signal.</div>';
    return;
  }}

  const wins   = trades.filter(t => t.won);
  const losses = trades.filter(t => !t.won);
  const wr     = wins.length / trades.length;
  const totalPnl = trades.reduce((s,t) => s + (t.net_pnl||0), 0);
  const avgWin   = wins.length   ? wins.reduce((s,t)=>s+(t.net_pnl||0),0)/wins.length : 0;
  const avgLoss  = losses.length ? losses.reduce((s,t)=>s+(t.net_pnl||0),0)/losses.length : 0;
  const tp1Rate  = trades.filter(t=>t.tp1_hit).length/trades.length;
  const tp4Rate  = trades.filter(t=>t.tp4_hit).length/trades.length;
  const premTrades = trades.filter(t=>t.is_premium);
  const premWr   = premTrades.length ? premTrades.filter(t=>t.won).length/premTrades.length : null;

  // Score Buckets
  const buckets = {{'A 75-100':[],'B 50-74':[],'C 25-49':[],'D 0-24':[]}};
  trades.forEach(t => {{ if (t.score_range && buckets[t.score_range]) buckets[t.score_range].push(t); }});

  // Wochentag Winrate
  const days = {{}};
  trades.forEach(t => {{
    if (!t.weekday) return;
    if (!days[t.weekday]) days[t.weekday] = {{wins:0,total:0}};
    days[t.weekday].total++;
    if (t.won) days[t.weekday].wins++;
  }});

  let html = '';

  // KPIs
  html += `<div class="kpi-grid section">
    <div class="kpi"><div class="kpi-label">Trades Total</div><div class="kpi-value">${{trades.length}}</div></div>
    <div class="kpi"><div class="kpi-label">Winrate</div><div class="kpi-value ${{wr>=0.55?'green':wr>=0.45?'amber':'red'}}">${{pct(wr)}}</div><div class="kpi-sub">${{wins.length}}W / ${{losses.length}}L</div></div>
    <div class="kpi"><div class="kpi-label">Net P&L</div><div class="kpi-value ${{totalPnl>=0?'green':'red'}}">${{totalPnl>=0?'+':''}}${{fmt(totalPnl)}} USDT</div></div>
    <div class="kpi"><div class="kpi-label">Ø Gewinn</div><div class="kpi-value green">+${{fmt(avgWin)}}</div></div>
    <div class="kpi"><div class="kpi-label">Ø Verlust</div><div class="kpi-value red">${{fmt(avgLoss)}}</div></div>
    <div class="kpi"><div class="kpi-label">TP1 Hit-Rate</div><div class="kpi-value blue">${{pct(tp1Rate)}}</div></div>
    <div class="kpi"><div class="kpi-label">TP4 Hit-Rate</div><div class="kpi-value amber">${{pct(tp4Rate)}}</div></div>
    <div class="kpi"><div class="kpi-label">Premium WR</div><div class="kpi-value ${{premWr!=null?(premWr>=0.6?'green':'amber'):''}}">${{premWr!=null?pct(premWr):'n/a'}}</div><div class="kpi-sub">${{premTrades.length}} Trades</div></div>
  </div>`;

  // Score-Analyse
  html += `<div class="section"><div class="section-title">Score-Analyse — Welcher Score-Bereich ist profitabelsten?</div>
  <div class="score-grid">`;
  const bucketColors = {{'A 75-100':'#2ecc71','B 50-74':'#3498db','C 25-49':'#f39c12','D 0-24':'#e74c3c'}};
  Object.entries(buckets).forEach(([range, ts]) => {{
    if (!ts.length) {{
      html += `<div class="score-card"><div class="score-badge" style="color:${{bucketColors[range]}}">${{range}}</div><div class="score-stats">Keine Trades</div></div>`;
      return;
    }}
    const bWr  = ts.filter(t=>t.won).length/ts.length;
    const bPnl = ts.reduce((s,t)=>s+(t.net_pnl||0),0);
    const bAvg = bPnl/ts.length;
    html += `<div class="score-card">
      <div class="score-badge" style="color:${{bucketColors[range]}}">${{range}}</div>
      <div class="score-label">${{ts.length}} Trades</div>
      <div class="score-bar"><div class="score-fill" style="width:${{bWr*100}}%;background:${{bucketColors[range]}}"></div></div>
      <div class="score-stats">
        WR: ${{pct(bWr)}} &nbsp;|&nbsp; Ø P&L: ${{bAvg>=0?'+':''}}${{fmt(bAvg)}} USDT<br>
        Net: ${{bPnl>=0?'+':''}}${{fmt(bPnl)}} USDT
      </div>
    </div>`;
  }});
  html += `</div></div>`;

  // Wochentag Analyse
  const dayOrder = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
  const dayDE    = {{'Monday':'Mo','Tuesday':'Di','Wednesday':'Mi','Thursday':'Do','Friday':'Fr','Saturday':'Sa','Sunday':'So'}};
  html += `<div class="section"><div class="section-title">Winrate nach Wochentag</div>`;
  dayOrder.forEach(d => {{
    if (!days[d]) return;
    const dwr = days[d].wins/days[d].total;
    html += `<div class="bar-row">
      <span style="font-family:var(--mono);font-size:12px;color:var(--muted)">${{dayDE[d]}} (${{days[d].total}}T)</span>
      <div class="bar-bg"><div class="bar-val" style="width:${{dwr*100}}%;background:${{dwr>=0.55?'var(--green)':dwr>=0.45?'var(--amber)':'var(--red)'}}"></div></div>
      <span style="font-family:var(--mono);font-size:12px;color:${{dwr>=0.55?'var(--green)':dwr>=0.45?'var(--amber)':'var(--red)'}}">${{pct(dwr)}}</span>
    </div>`;
  }});
  html += `</div>`;

  // Letzte Trades
  const recent = [...trades].reverse().slice(0, 30);
  html += `<div class="section"><div class="section-title">Letzte Trades (max. 30)</div>
  <div class="table-wrap"><table>
  <thead><tr>
    <th>Datum</th><th>Symbol</th><th>Dir</th><th>Score</th><th>Premium</th>
    <th>Hebel</th><th>SL%</th><th>TP1</th><th>TP4</th><th>Net P&L</th><th>Ergebnis</th>
  </tr></thead><tbody>`;

  recent.forEach(t => {{
    const d = t.close_dt ? t.close_dt.substring(0,10) : '—';
    const scoreRange = t.score_range || '—';
    const scoreColor = {{'A 75-100':'green','B 50-74':'blue','C 25-49':'amber','D 0-24':'red'}}[scoreRange] || '';
    html += `<tr>
      <td style="color:var(--muted)">${{d}}</td>
      <td style="font-weight:600">${{t.symbol||'—'}}</td>
      <td>${{t.direction==='long'?pill('LONG','green'):pill('SHORT','red')}}</td>
      <td>${{pill(scoreRange, scoreColor)}}</td>
      <td>${{t.is_premium?pill('✓ Premium','amber'):'—'}}</td>
      <td style="color:var(--muted)">${{t.leverage||'—'}}x</td>
      <td style="color:var(--muted)">${{fmt(t.sl_dist_pct,2)}}%</td>
      <td>${{t.tp1_hit?'✓':'—'}}</td>
      <td>${{t.tp4_hit?'✓':'—'}}</td>
      <td class="${{(t.net_pnl||0)>=0?'green':'red'}}">${{(t.net_pnl||0)>=0?'+':''}}${{fmt(t.net_pnl)}} USDT</td>
      <td>${{t.won?pill('Gewinn','green'):pill('Verlust','red')}}</td>
    </tr>`;
  }});
  html += `</tbody></table></div></div>`;

  document.getElementById('content').innerHTML = html;
}}

render(RAW);
</script>
</body>
</html>"""



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

    # ── Token-Redaction im Werkzeug-Access-Log ──────────────────
    # Werkzeug loggt jedes eingehende POST /webhook?token=XYZ im Access-Log.
    # Das landet in Railway → Token leakt jedes Mal wenn Logs geteilt/exportiert
    # werden. Wir hängen einen Filter an den werkzeug-Logger, der jedes
    # ?token=... durch ?token=*** ersetzt BEVOR die Zeile rausgeht.
    # Defense-in-Depth: auch wenn TradingView-Alerts noch URL-Token schicken,
    # wird der Token nie mehr geloggt.
    import logging as _logging
    import re as _re_log

    class _TokenRedactFilter(_logging.Filter):
        _TOKEN_PAT = _re_log.compile(r'(\?|&)token=[^\s"&]+', _re_log.IGNORECASE)

        def filter(self, record: _logging.LogRecord) -> bool:
            try:
                # Werkzeug baut die Message aus record.msg + record.args zusammen
                msg = record.getMessage()
                # Case-insensitive Pre-Check (TradingView sendet immer klein,
                # aber falls doch mal "Token=" o.ä. → trotzdem fangen)
                if "token=" in msg.lower():
                    redacted = self._TOKEN_PAT.sub(r'\1token=***REDACTED***', msg)
                    # Wir überschreiben .msg und leeren .args, damit getMessage()
                    # beim nächsten Aufruf die geänderte Version liefert.
                    record.msg = redacted
                    record.args = ()
            except Exception:
                pass
            return True  # Zeile nicht verwerfen, nur modifizieren

    _wz_logger = _logging.getLogger("werkzeug")
    _wz_logger.addFilter(_TokenRedactFilter())
    # v4.31: Access-Log-Spam entfernen. Flask/Werkzeug schreibt jede "POST
    # /webhook 200"-Zeile auf stderr — die landet in Railway im error-Bucket
    # und maskiert echte Fehler (bei 6h-Log-Analyse: 80 von 83 "error"-
    # Einträgen waren reine 200-OK-Access-Logs). WARNING schluckt Access-
    # Log-INFO-Zeilen, liefert aber Fehler/Exceptions weiter.
    _wz_logger.setLevel(_logging.WARNING)

    app = Flask(__name__)

    @app.route("/webhook", methods=["POST"])
    def webhook():
        """Webhook-Entry-Point — NUR Token-Check + Parse, dann 200 ACK.

        v4.13: Die komplette Signal-Verarbeitung (Bitget-Calls, SL-Sets,
        Telegram-Messages) läuft in _process_webhook_async() in einem
        Daemon-Thread. So wird TradingView garantiert in < 100 ms bedient
        und ein Bitget-Hänger blockiert keine weiteren Webhooks mehr.
        """
        # ── Body & Token lesen ────────────────────────────────
        # Token aus URL-Parameter ODER aus JSON-Body akzeptieren.
        # WICHTIG: Wir geben bei ungültigem Token TROTZDEM 200 zurück —
        # ein 401/403 würde TradingView als Fehler anzeigen und Retries auslösen.
        token_url = flask_request.args.get("token", "")
        raw_body  = flask_request.get_data(as_text=True) or ""

        # ── Body-Bereinigung vor JSON-Parse ───────────────────
        import re as _re
        # Fix 1: Unaufgelöste TradingView-Platzhalter → 0 (Watchlist-Alarme)
        raw_clean = _re.sub(r'\{\{[^}]+\}\}', '0', raw_body)
        # Fix 2: NaN / Infinity (Pine 'na' → TradingView NaN)
        raw_clean = _re.sub(r'\bNaN\b',       'null', raw_clean)
        raw_clean = _re.sub(r'\bInfinity\b',  'null', raw_clean)
        raw_clean = _re.sub(r'\b-Infinity\b', 'null', raw_clean)

        # ── Payload parsen ────────────────────────────────────
        # IMMER 200 zurückgeben — nie 400/401/500 an TradingView senden.
        try:
            data = json.loads(raw_clean) if raw_clean.strip() else {}
        except Exception as _e:
            log(f"⚠ Webhook: JSON-Parse-Fehler: {_e} | Body (erste 200 Z.): {raw_body[:200]}")
            # Telegram-Alert im Thread, damit der Request sofort 200 liefert
            threading.Thread(
                target=telegram,
                args=(f"⚠️ <b>Webhook Parse-Fehler</b>\n{_e}\nBody: <code>{raw_body[:150]}</code>",),
                daemon=True,
            ).start()
            return jsonify({"status": "ignored", "reason": "parse_error"}), 200

        # Token-Prüfung (nach Parse, damit body-Token auch funktioniert)
        token_body = str(data.get("token", ""))
        token      = token_url or token_body
        if WEBHOOK_SECRET and token != WEBHOOK_SECRET:
            # Token NICHT ins Log — nur Länge + Quelle, damit man debuggen kann
            # ohne den (womöglich echten) falschen Token zu persistieren.
            _src = "url" if token_url else ("body" if token_body else "none")
            _len = len(token) if token else 0
            # Diagnose: bei quelle=none strukturelle Body-Info loggen.
            # Token wird niemals geloggt, aber wir brauchen genug Struktur
            # um zu sehen, welcher Alarm ohne Token sendet.
            if _src == "none":
                try:
                    _keys = sorted((data or {}).keys()) if isinstance(data, dict) else []
                except Exception:
                    _keys = []
                _sym = ""
                if isinstance(data, dict):
                    _sym = str(data.get("symbol", "") or data.get("ticker", ""))
                _sig = str(data.get("signal", "")) if isinstance(data, dict) else ""
                _body_len = len(raw_body)
                _body_snip = raw_body[:200].replace("\n", " ")
                log(f"⚠ Webhook: Ungültiger Token (quelle=none, len=0) | "
                    f"body_len={_body_len} keys={_keys} symbol={_sym!r} signal={_sig!r} | "
                    f"snippet={_body_snip!r}")
            else:
                log(f"⚠ Webhook: Ungültiger Token (quelle={_src}, len={_len})")
            return jsonify({"status": "ignored", "reason": "unauthorized"}), 200

        # ── Verarbeitung im Hintergrund-Thread ────────────────
        # TradingView bekommt sofort 200 — keine Timeouts mehr, auch wenn
        # Bitget-APIs im Worker 10+ Sekunden brauchen.
        threading.Thread(
            target=_process_webhook_async,
            args=(data,),
            daemon=True,
            name="dominus-webhook-worker",
        ).start()
        return jsonify({"status": "accepted"}), 200

    def _process_webhook_async(data: dict) -> None:
        """Verarbeitet den Webhook-Payload asynchron.

        Darf beliebig lange dauern (Bitget-Calls, SL-Sets, Telegram).
        Exceptions werden geloggt + via Telegram gemeldet, damit nichts
        lautlos verschwindet.
        """
        global btc_dir, t2_dir
        try:
            _webhook_dispatch(data)
        except Exception as _exc:
            import traceback as _tb
            log(f"⚠ Webhook-Worker-Exception: {_exc}")
            log(_tb.format_exc())
            try:
                telegram(
                    "⚠️ <b>Webhook-Worker-Fehler</b>\n"
                    f"<code>{html.escape(str(_exc))}</code>"
                )
            except Exception:
                pass

    def _webhook_dispatch(data: dict) -> None:
        """Kern-Dispatcher für den Webhook-Payload (ehemals Inhalt von webhook())."""
        global btc_dir, t2_dir

        raw_symbol = data.get("symbol", "").upper()
        entry      = float(data.get("entry", 0) or 0)
        timeframe  = data.get("timeframe", "H2").upper()

        # v4.11: signal_type früh parsen — HARSI_SL darf direction="auto" haben.
        signal_type = data.get("signal", "").upper()

        # Richtung: "direction" ODER "side" (beide Feldnamen akzeptieren)
        # Alarm-Templates senden "side":"long"/"short" — Script liest beide.
        # direction kann String ("long"/"short") oder Int (1/-1 vom Liquidity Filter) sein
    _dir_raw  = data.get("direction", "")
    if isinstance(_dir_raw, (int, float)):
        direction = "long" if int(_dir_raw) == 1 else "short" if int(_dir_raw) == -1 else ""
    else:
        direction = str(_dir_raw).lower()
        if direction not in ("long", "short"):
            direction = data.get("side", "").lower()
        # "auto" → leer (Auto-Direction kommt unten, nur für HARSI_SL)
        if direction == "auto":
            direction = ""
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

        # v4.11: HARSI_SL Universal — falls direction fehlt/auto, aus offener
        # Position ableiten. Ein einziger Watchlist-Alarm feuert dann für alle
        # DOMINUS-Symbole und Railway filtert stumm Symbole ohne offene Position.
        # v4.12: gleiche Auto-Direction-Logik für SLING_SL.
        if signal_type in ("HARSI_SL", "SLING_SL") and direction not in ("long", "short") and symbol:
            for _p in get_all_positions():
                _psym = (_p.get("symbol") or "").upper()
                _psize = float(_p.get("total", 0) or 0)
                if _psym == symbol and _psize > 0:
                    direction = (_p.get("holdSide") or "").lower()
                    if direction in ("long", "short"):
                        log(f"🔎 {signal_type} Auto-Direction: {symbol} → {direction.upper()} (aus offener Position)")
                        break
            if direction not in ("long", "short"):
                # Kein offener Trade → silent ignore, Watchlist-Rauschen unterdrücken.
                # v4.31: aus "silent" → "dezent loggen" (Forensik via "⏭"-Marker).
                # v4.35: Drop-Counter mit Summary statt Per-Drop-Log (Lärm-Reduktion).
                _track_watchlist_drop("keine offene Position", symbol, signal_type)
                return  # ignored: no open position

        if not symbol or direction not in ("long", "short"):
            log(f"⚠ Webhook ignoriert: kein Signal "
                f"(buy={data.get('buy',0)} sell={data.get('sell',0)} "
                f"dir={data.get('direction','')} side={data.get('side','')} "
                f"sym={raw_symbol})")
            return  # ignored: no signal

        log(f"📡 TradingView Alert: {symbol} {direction.upper()} "
            f"@ {entry} [{timeframe}]")

        if entry == 0:
            entry = get_mark_price(symbol)

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
            _until_str = datetime.fromtimestamp(_until_ts, timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
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
            return  # ok: macro extreme handled

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

            # v4.30: Nur bei echtem Wechsel loggen, state speichern und Telegram
            # senden. TV-Alarm kann mehrfach pro Bar feuern (z.B. "once_per_bar"
            # triggert bei jedem Tick innerhalb der Bar). Bisher kam jeder
            # Tick als eigene Telegram-Message + vollständige Positions-Warnung
            # durch — das klang wie ein Flip, war aber nur ein Re-Trigger.
            if new_dir == prev:
                log(f"📡 {label} DOM-DIR Tick (unverändert: {new_dir.upper()}) — ignoriert")
                return  # ok: identisch zu vorher, keine Aktion
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
            return  # ok: BTC_DIR/T2_DIR handled

        if signal_type == "HARSI_SL":
            # HARSI_SL: SL auf Harsi-Ausstiegslinie setzen (für offene Positionen)
            # Unterschied zu HARSI_EXIT: HARSI_EXIT = Entry-Signal (Alarm 3/3b)
            #                            HARSI_SL   = SL-Anpassung bei offenem Trade (Alarm 4/4b)
            harsi_price = float(data.get("price", 0) or data.get("sl", 0) or 0)
            if harsi_price == 0:
                # v4.35: Drop-Counter (Lärm-Reduktion, vorher Per-Drop-Log)
                _track_watchlist_drop("kein Preis im Payload", symbol, "HARSI_SL", direction)
                return  # ignored: no price
            cur_size = 0
            for pos in get_all_positions():
                if pos.get("symbol") == symbol and pos.get("holdSide") == direction:
                    cur_size = float(pos.get("total", 0))
                    break
            if cur_size == 0:
                # v4.35: Drop-Counter (Lärm-Reduktion, vorher Per-Drop-Log)
                _track_watchlist_drop("keine offene Position", symbol, "HARSI_SL", direction)
                return  # ignored: no open position
            set_sl_harsi(symbol, direction, harsi_price, cur_size=cur_size)
            cache_invalidate("all_positions")
            return  # ok: HARSI_SL set

        # ─────────────────────────────────────────────────────────────
        # v4.12: SLING_SL — Swing-Pivot-basierter Trailing-SL
        # Pine feuert bei jedem bestätigten Pivot (3/3 H2).
        # Sling-Low  (side=long)  → SL-Kandidat für offenen LONG
        # Sling-High (side=short) → SL-Kandidat für offenen SHORT
        # Richtungs-Mismatch = stumm ignorieren (Pine ist stateless).
        # ─────────────────────────────────────────────────────────────
        if signal_type == "SLING_SL":
            pivot_price = float(data.get("pivot", 0) or data.get("price", 0) or 0)
            atr_raw     = float(data.get("atr", 0) or 0)
            if pivot_price == 0:
                # v4.35: Drop-Counter (Lärm-Reduktion, vorher Per-Drop-Log)
                _track_watchlist_drop("kein Pivot im Payload", symbol, "SLING_SL", direction)
                return  # ignored: no pivot

            # Offene Position suchen — Richtung MUSS zur Pine-side passen
            cur_size = 0
            for pos in get_all_positions():
                if (pos.get("symbol") or "").upper() == symbol \
                        and (pos.get("holdSide") or "").lower() == direction:
                    cur_size = float(pos.get("total", 0) or 0)
                    break
            if cur_size == 0:
                # Pine sendet Pivot für jedes Symbol — Watchlist-Rauschen.
                # v4.35: Drop-Counter (Lärm-Reduktion, vorher Per-Drop-Log)
                _track_watchlist_drop("keine matching Position", symbol, "SLING_SL", direction)
                return  # ignored: no matching position

            set_sl_sling(symbol, direction, pivot_price,
                         cur_size=cur_size, atr_val=atr_raw)
            cache_invalidate("all_positions")
            return  # ok: SLING_SL set

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
                # v4.26: deutlich sichtbarer Log-Eintrag + aktiver H2-Fenster-Bestand
                #        für Forensik (welche Symbole HABEN ein offenes H2-Fenster?)
                _other_keys = sorted(last_h2_signal_time.keys())
                log(f"[HARSI_EXIT-ohne-H2] {sig_key} — KEIN H2-TS gespeichert. "
                    f"Aktive H2-Fenster ({len(_other_keys)}): {_other_keys[:10]}"
                    + (" ..." if len(_other_keys) > 10 else ""))
                warn_line = (
                    "⚠️ <b>Kein H2-Signal gespeichert</b> — Timing unbekannt.\n"
                    "Bitte manuell prüfen ob ein H2-Signal vorlag!"
                )
                timing_ok = False
                elapsed_min = None
            else:
                # v4.29: timezone-aware Subtraktion; h2_ts aus Dict kann legacy-naive sein
                elapsed_sec = (datetime.now(timezone.utc)
                               - _ensure_aware_utc(h2_ts)).total_seconds()
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

            # v4.19: Wenn Entry-Queue aktiv UND Timing OK → in Queue statt
            # direkt senden. Abgelaufene/unbekannte Timings liefern weiterhin
            # die bisherige Einzel-Warnung, damit der User die Info sofort sieht.
            if timing_ok and ENTRY_QUEUE_ENABLED:
                _sugg = build_trade_suggestion(
                    symbol, direction, entry,
                    data.get("sling_sl"), data.get("atr"),
                )
                enqueue_entry({
                    "symbol":             symbol,
                    "direction":          direction,
                    "entry":              entry,
                    "warn_line":          warn_line,
                    "timing_elapsed_min": elapsed_min or 0,
                    "sugg":                _sugg,
                    "harsi_warn":         int(data.get("harsi_warn", 0) or 0),
                    "sling_sl":           data.get("sling_sl"),
                    "atr":                data.get("atr"),
                    "xinfo":              _xinfo,
                    "source":             "HARSI_EXIT",   # v4.26: explizite Quelle für Dedup-Diff-Log
                    "ts":                 time.time(),
                })
                # Timestamp löschen — verhindert Doppel-Enqueue beim nächsten Re-Trigger
                # v4.26: save_state() nach Mutation → überlebt Railway-Redeploys
                if sig_key in last_h2_signal_time:
                    del last_h2_signal_time[sig_key]
                    save_state()
                return  # ok: HARSI_EXIT in Queue

            # Fallback-Pfad (Queue disabled ODER Timing abgelaufen) → wie bisher
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
                _sugg = build_trade_suggestion(
                    symbol, direction, entry,
                    data.get("sling_sl"), data.get("atr"),
                )
                msg_parts += [
                    "📋 <b>Einstieg jetzt möglich:</b>",
                    format_trade_suggestion(symbol, direction, _sugg),
                    _doc_link(_e_anker, _e_lbl),
                ]
            else:
                msg_parts += [
                    "🚫 Kein Einstieg — Signal abgelaufen oder unbekannt.",
                    "Warte auf nächsten H2-Signal-Alarm.",
                ]

            telegram("\n".join(msg_parts), reply_markup=build_setup_buttons(symbol))

            # Nach Eintritt: Zeitstempel löschen (verhindert Doppel-Warnungen)
            # v4.26: save_state() nach Mutation → überlebt Railway-Redeploys
            if sig_key in last_h2_signal_time:
                del last_h2_signal_time[sig_key]
                save_state()
            return  # ok: HARSI_EXIT handled

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
            return  # buffered: H4 trigger queued

        # v4.10: Makro-Extremzonen als DOMINUS-Premium-Info (kein Block).
        # Oversold + LONG bzw. Overbought + SHORT → 🎯 Premium-Hinweis;
        # Gegenrichtung → ⚠️ Warnung. Der H2-Signal-Fluss (inkl. 30-Min-Fenster)
        # läuft regulär weiter — der User entscheidet beim HARSI_EXIT.
        _xinfo = extreme_warn(direction)
        _xmsg  = format_extreme_info_msg(symbol, direction, _xinfo, "H2_SIGNAL")
        if _xmsg:
            telegram(_xmsg, reply_markup=build_setup_buttons(symbol))

        # H2 Signal → H4 Puffer flushen dann sofort senden
        # 30-Min-Fenster starten: Zeitstempel für HARSI_EXIT-Prüfung speichern
        # v4.29: timezone-aware UTC
        last_h2_signal_time[f"{symbol}_{direction}"] = datetime.now(timezone.utc)
        # Altes Einträge aufräumen (Memory-Leak verhindern): nur letzte 30 Min behalten
        _cutoff = datetime.now(timezone.utc) - timedelta(minutes=35)
        for _k in list(last_h2_signal_time.keys()):
            if _ensure_aware_utc(last_h2_signal_time[_k]) < _cutoff:
                del last_h2_signal_time[_k]
        # v4.26: State persistieren → H2-Fenster überlebt Railway-Redeploys
        save_state()

        # Makro-Kontext aus Webhook auslesen (vom DOM-ORC Plot-Werten)
        harsi_warn_val  = int(float(data.get("harsi_warn",  0) or 0))
        btc_t2_warn_val = int(float(data.get("btc_t2_warn", 0) or 0))
        premium_val     = int(float(data.get("premium",     0) or 0))

        # ─────────────────────────────────────────────────────────────
        # v4.22: H2_SIGNAL mit harsi_warn=0 → in Entry-Queue (Option 1)
        # ─────────────────────────────────────────────────────────────
        # Bei harsi_warn=0 ist HARSI bereits außerhalb der Extremzone —
        # das H2_SIGNAL IST der Einstiegs-Trigger (nicht mehr nur Vorwarnung).
        # Damit mehrere gleichzeitige H2-Einstiege als Rangliste erscheinen
        # (statt einzeln pro Coin), wird das Signal hier in dieselbe Queue
        # eingespeist wie HARSI_EXIT. flush_entries() scored & rendert beide
        # Signal-Quellen zusammen nach Premium-Bucket + Score absteigend.
        #
        # WICHTIG: btc_dir / t2_dir werden vorher gesetzt (unten), damit die
        # Queue-Scoring-Funktion aktuelle Makro-Richtungen sieht.
        if btc_t2_warn_val == 0:
            if btc_dir not in ("recovering", "recovering_short"):
                btc_dir = direction
            if t2_dir  not in ("recovering", "recovering_short"):
                t2_dir  = direction

        if harsi_warn_val == 0 and ENTRY_QUEUE_ENABLED:
            _sugg_q = build_trade_suggestion(
                symbol, direction, entry,
                data.get("sling_sl"), data.get("atr"),
            )
            enqueue_entry({
                "symbol":             symbol,
                "direction":          direction,
                "entry":              entry,
                "warn_line":          "✅ H2 ready — HARSI OK, sofort einsteigbar",
                "timing_elapsed_min": 0,
                "sugg":               _sugg_q,
                "harsi_warn":         0,
                "sling_sl":           data.get("sling_sl"),
                "atr":                data.get("atr"),
                "xinfo":              _xinfo,
                "source":             "H2_SIGNAL",
                "ts":                 time.time(),
            })
            # H4-Puffer trotzdem flushen (gebündelte H4-Trigger-Nachricht)
            flush_h4_buffer()
            # Fenster-Marker entfernen — Queue-Flush übernimmt die Darstellung
            # v4.26: save_state() nach Mutation → überlebt Railway-Redeploys
            if f"{symbol}_{direction}" in last_h2_signal_time:
                del last_h2_signal_time[f"{symbol}_{direction}"]
                save_state()
            log(f"  H2_SIGNAL {symbol} {direction} → Entry-Queue (harsi_warn=0)")
            return  # ok: H2_SIGNAL queued

        # v4.12: Sling-SL + ATR(14) für berechneten /trade-Vorschlag
        # (btc_dir / t2_dir wurden bereits oben v4.22-Block gesetzt)
        _trade_sugg = build_trade_suggestion(
            symbol, direction, entry,
            data.get("sling_sl"), data.get("atr"),
        )

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
            # v4.29: timezone-aware Default + Shim für Legacy-Dict-Werte
            _h2_ts      = _ensure_aware_utc(last_h2_signal_time.get(
                f"{symbol}_{direction}", datetime.now(timezone.utc)))
            _expiry_utc = _h2_ts + timedelta(minutes=30)
            _expiry_str = _expiry_utc.strftime("%d.%m.%Y %H:%M UTC")
            # v4.22: HARSI_EXIT kommt automatisch über DOM-ORC v2.4.2 Intrabar-
            # Alert (Pine alert.freq_once_per_bar) → Watchlist-Master-Alarm.
            # Kein manuelles Alarm-3-Anlegen mehr nötig. Stattdessen: Timer +
            # kurzer Hinweis, dass der HARSI_EXIT automatisch eintrudeln wird.
            _anker  = "sec-alarm3" if direction == "long" else "sec-alarm3b"
            _anchor_label = "Alarm 3 — Doku" if direction == "long" else "Alarm 3b — Doku"
            if DOCS_URL:
                _docs_link = f'\n🔗 <a href="{DOCS_URL}#{_anker}">{_anchor_label}</a>'
            else:
                _docs_link = ""
            timer_line = (
                f"⏳ <b>HARSI noch in Extremzone — warten auf Intrabar-Exit</b>\n"
                f"⏰ Fenster läuft ab: <b>{_expiry_str}</b>\n"
                f"🤖 HARSI_EXIT-Alarm kommt automatisch (Pine v2.4.2 Intrabar)"
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
            "✅ DOMINUS Impuls Extremzone erreicht (Pine-geprüft)",
            "✅ H4 Trigger bestätigt (Pine-geprüft)",
            f"{harsi_icon} {harsi_txt}",
            f"{btc_t2_icon} {btc_t2_txt}",
            f"{premium_icon} {premium_txt}",
            "",
            f"💰 {balance:.0f} USDT  |  Pro Order: {per_order:.0f} USDT",
            f"📊 Kelly: {kelly['kelly_pct']}%",
            "",
            timer_line,
            format_trade_suggestion(symbol, direction, _trade_sugg),
        ]
        # v4.22: Charts werden via Inline-Buttons angeboten — keine Link-URLs
        # mehr im Message-Text (weniger Rauschen, bessere Mobile-UX). Buttons
        # liefert build_setup_buttons(symbol) als Bitget + BTC H2 + Total2.
        telegram("\n".join(msg_parts), reply_markup=build_setup_buttons(symbol))

        # v4.22: Der frühere "Alarm 3 manuell anlegen"-Block (build_alarm_harsi_exit)
        # ist obsolet: Pine v2.4.2 feuert HARSI_EXIT intrabar, der Watchlist-Master-
        # Alarm ("Any alert() function call") fängt ihn automatisch ab und Railway
        # leitet ihn in die Entry-Queue. Kein User-Action nötig.
        return  # ok: H2_SIGNAL processed

    @app.route("/", methods=["GET"])
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "running", "version": "v4.35.1"}), 200

    @app.route("/dashboard", methods=["GET"])
    def dashboard():
        """Performance Dashboard — liest demo_trades.json live."""
        import json as _j, os as _os
        from flask import Response
        fname = "demo_trades.json"
        trades = []
        if _os.path.exists(fname):
            try:
                with open(fname) as f:
                    trades = _j.load(f)
            except Exception:
                trades = []
        return Response(_build_dashboard_html(_j.dumps(trades)), mimetype="text/html")

    port = _env_int("PORT", 8080)
    # WICHTIG: Token NICHT ins Log schreiben — er landet sonst in Railway-Logs.
    _tok_hint = (
        f"gesetzt (len={len(WEBHOOK_SECRET)})"
        if WEBHOOK_SECRET and WEBHOOK_SECRET != "dominus"
        else "⚠ NICHT gesetzt (Default 'dominus' — bitte WEBHOOK_SECRET env-var setzen!)"
    )
    log(f"Webhook-Server gestartet auf Port {port}")
    log(f"Endpoint: POST /webhook  (Token: {_tok_hint})")
    log("Token-Übergabe: Query-Param ?token=… ODER JSON-Body-Feld \"token\" — beides wird akzeptiert.")
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
            # v4.12: Sling-SL (Swing-Pivot-basiert) + DCA Auto-Void State
            "sling_sl":                sling_sl,
            "dca_void":                dca_void,
            "last_h2_signal_time":     h2_ts_serialized,
            "btc_dir":                 btc_dir,
            "t2_dir":                  t2_dir,
            # v4.9 Makro-Extremzonen persistieren (überlebt Railway-Restarts)
            "macro_extreme":           macro_extreme,
            # v4.32 Phantom-Close-Guard: überlebt Railway-Redeploy.
            # Nur Einträge innerhalb des TTL-Fensters behalten, sonst wächst
            # der State-File unnötig.
            "recent_closes":           {
                _s: _v for _s, _v in recent_closes.items()
                if time.time() - float(_v.get("ts_close", 0)) < PHANTOM_REOPEN_TTL_SEC
            },
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
        # v4.12: Sling-SL + DCA Auto-Void State wiederherstellen
        sling_sl.update(s.get("sling_sl", {}))
        dca_void.update(s.get("dca_void", {}))
        # last_h2_signal_time: ISO-Strings → datetime (nur Einträge < 30 Min laden)
        # v4.29: Migrations-kompatibel — alte State-Files enthalten naive ISO-Strings
        # ("2026-04-22T00:00:00"), neue enthalten aware ("...+00:00"). Shim über
        # _ensure_aware_utc setzt bei naive-Werten tzinfo=UTC, damit die Subtraktion
        # mit now_utc nicht mit TypeError crasht.
        now_utc = datetime.now(timezone.utc)
        for k, v in s.get("last_h2_signal_time", {}).items():
            try:
                ts = _ensure_aware_utc(datetime.fromisoformat(v))
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
        # v4.32 Phantom-Close-Guard laden — nur noch gültige Fenster übernehmen
        for _sym, _snap in (s.get("recent_closes", {}) or {}).items():
            try:
                _ts_close = float(_snap.get("ts_close", 0) or 0)
                if _now_ts - _ts_close < PHANTOM_REOPEN_TTL_SEC:
                    recent_closes[_sym] = _snap
            except Exception:
                pass
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
    leverage  = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
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
                    body_sl["stopSurplusTriggerPrice"] = round_price(existing_tp4, decimals)
                    body_sl["stopSurplusTriggerType"]  = "mark_price"
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
                        f"⚠️ Mit Ausstiegslinie abgleichen!",
                        reply_markup=build_setup_buttons(symbol),
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
                telegram(
                    f"⚠️ <b>{symbol}</b>: Position im Verlust, SL manuell setzen!",
                    reply_markup=build_setup_buttons(symbol),
                )

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
                    f"⚠️ Bitte mit Ausstiegslinie abgleichen!",
                    reply_markup=build_setup_buttons(symbol),
                )
            else:
                log(f"  ✗ Auto-SL fehlgeschlagen: {res.get('msg', res)}")
                telegram(
                    f"❌ <b>Kein SL — {symbol}</b>\n"
                    f"Auto-SL fehlgeschlagen. Bitte manuell setzen!\n"
                    f"Empfehlung: {sl_str} USDT ({sl_dist:.1f}%)",
                    reply_markup=build_setup_buttons(symbol),
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
    _prev_tp4  = _prev_td.get("tp4", 0)                                 # v4.16 — TP4 erhalten
    trade_data[symbol] = {
        "entry":        avg,
        "direction":    direction,
        "leverage":     leverage,
        "sl":           sl_price,
        "peak_size":    max(_prev_peak, _reconstructed_peak, size),   # nie reduzieren
        "open_ts":      _prev_ts,                                      # Öffnungszeit erhalten
        "tp_order_ids": _prev_td.get("tp_order_ids", []),
        "tp4":          _prev_tp4,                                     # v4.16 — TP4-Preis persistent
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

        # v4.35: Versuche TP-Setzen auch bei Qty<4 — place_tp_orders carry-forward
        # + TP4 (Full-Close) reichen oft auch bei kleinen Positionen.
        cancel_all_tp_orders(symbol)
        time.sleep(1)
        count, tp_prices = place_tp_orders(
            symbol, avg, size, direction, leverage, mark, known_sl=sl_price
        )
        if count == 0 and size < 4:
            log(f"  ⚠ Position zu klein (Qty={size}, min. 4) — kein TP setzbar")
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
    leverage  = (int(trade_data.get(symbol, {}).get("leverage", 0)) or int(float(pos.get("leverage", 10))))
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
    _prev_td_p = trade_data.get(symbol, {})
    trade_data[symbol] = {
        "entry":     avg,
        "direction": direction,
        "leverage":  leverage,
        "sl":        0,   # nicht lesbar via API
        "peak_size": size,
        "open_ts":   int(time.time() * 1000),
        "tp4":       _prev_td_p.get("tp4", 0),   # v4.16 — TP4 erhalten
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

    log("DOMINUS Demo-Bot v4.35.1 gestartet [DEMO-MODUS — Paper Trading]")
    log("  paperId=1 aktiv → alle Orders gehen auf Bitget Demo-Account")
    log("  Dieselben API-Keys wie Live — kein separater Demo-Key nötig")
    log(f"  Telegram Demo-Kanal: {TELEGRAM_CHAT_ID}")
    log("  Demo-Trades werden in demo_trades.json gespeichert")
    log(f"Intervall: {POLL_INTERVAL}s")
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

    # v4.24: Verwaiste State-Einträge aufräumen.
    # Symbole mit new_trade_done=True im State-File, die aber laut Bitget
    # keine offene Position haben, waren die Ursache des "♻️ TPs nach DCA"-
    # Bugs (neue Trades wurden als Nachkauf interpretiert). Beim Startup
    # gleichen wir state-File mit Bitget-Live-State ab und entfernen Ghosts.
    _active_syms = {p.get("symbol") for p in positions if p.get("symbol")}
    _orphans = [s for s in list(new_trade_done.keys()) if s not in _active_syms]
    if _orphans:
        log(f"  ⚠ {len(_orphans)} verwaiste(s) Ghost-Flag(s) gefunden: "
            f"{', '.join(_orphans)} — wird bereinigt")
        for _o in _orphans:
            new_trade_done.pop(_o, None)
            last_known_avg.pop(_o, None)
            last_known_size.pop(_o, None)
            sl_at_entry.pop(_o, None)
            trailing_sl_level.pop(_o, None)
            harsi_sl.pop(_o, None)
            sling_sl.pop(_o, None)
            dca_void.pop(_o, None)
            trade_data.pop(_o, None)
        save_state()
        log(f"  ✓ Ghost-State bereinigt und persistiert")

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

                    # v4.24: Neuer Trade oder Nachkauf?
                    # Discriminator basiert NICHT mehr nur auf new_trade_done —
                    # das Flag blieb in v4.23 nach Bitget-GUI-Closes als Ghost
                    # hängen und liess danach neue Trades fälschlich als DCA
                    # durchlaufen (→ "♻️ TPs nach DCA" statt "Neuer Trade",
                    # kein setup_new_trade → fehlende DCA-Limits, CSV-Log,
                    # Trade-Data, Trailing-SL-Init). Jetzt müssen zusätzlich
                    # last_known_size + last_known_avg + trade_data konsistent
                    # vorhanden sein, sonst wird der Fill als neuer Trade
                    # behandelt — auch wenn new_trade_done noch True ist.
                    is_known_pos = (
                        last_known_size.get(sym, 0) > 0
                        and last_known_avg.get(sym, 0) > 0
                        and sym in trade_data
                    )
                    if not is_known_pos:
                        if new_trade_done.get(sym, False):
                            log(f"  ⚠ Ghost-Flag entdeckt: new_trade_done[{sym}]=True "
                                f"ohne last_known_size/avg/trade_data — wird als "
                                f"neuer Trade behandelt")
                            # Verwaisten State wegputzen bevor setup_new_trade läuft
                            new_trade_done.pop(sym, None)
                            sl_at_entry.pop(sym, None)
                            trailing_sl_level.pop(sym, None)
                            harsi_sl.pop(sym, None)
                            sling_sl.pop(sym, None)
                            dca_void.pop(sym, None)
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
                #
                # v4.32 Fix A — Phantom-Close-Guard via Double-Check:
                # Ein einzelner stale Bitget-Tick (Position fehlt in einem API-Response,
                # kommt beim nächsten zurück) darf nicht zu einem False-Close führen.
                # Deshalb: erst Kandidaten sammeln, dann Cache invalidieren und mit
                # frischem Raw-API-Call gegenprüfen. Nur wenn BEIDE Reads leer sind,
                # wird handle_position_closed() gefeuert.
                active_symbols = {p.get("symbol") for p in get_all_positions()}
                _close_candidates = [
                    sym for sym in list(last_known_avg.keys())
                    if sym not in active_symbols and last_known_avg.get(sym, 0) > 0
                ]
                if _close_candidates:
                    try:
                        cache_invalidate("all_positions")
                        _raw_positions = _get_all_positions_raw()
                        _raw_symbols = {p.get("symbol") for p in _raw_positions}
                    except Exception as _ex:
                        # Kein belastbarer 2. Read → konservativ auf active_symbols
                        # zurückfallen (Verhalten wie vor v4.32: Close wird gebucht).
                        # Fix C in setup_new_trade() ist dann die zweite Sicherheitsnetz-
                        # Ebene falls gleich darauf ein Phantom-Reopen kommt.
                        log(f"[phantom-guard raw-check] {_ex} — fahre mit normaler Close-Logik fort")
                        _raw_symbols = active_symbols

                    for sym in _close_candidates:
                        if sym in _raw_symbols:
                            # Phantom — Position ist beim 2. Read wieder sichtbar.
                            log(f"  ♻ Phantom-Close verhindert: {sym} nach Cache-Refresh wieder aktiv")
                            continue
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
