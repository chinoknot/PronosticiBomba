import requests
import sys
import os
import http.server
import socketserver

# =========================================================
# CONFIGURAZIONE
# =========================================================

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"  # puoi spostarla in ENV se vuoi
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

# Data target: oggi per te è 26/11/2025
# Se in futuro vuoi la data dinamica: usa date.today().isoformat()
TARGET_DATE = "2025-11-26"

# File di marker per sapere se abbiamo già girato per questa data
RUN_MARKER_PATH = "/tmp/last_run_marker.txt"


# =========================================================
# FUNZIONI DI BASE PER LA CHIAMATA API
# =========================================================

def api_get(path, params=None):
    """Wrapper semplice per chiamare l'API-FOOTBALL e tornare solo 'response'."""
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=15)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print(f"# ERRORE HTTP su {url}: {e}", file=sys.stderr)
        return []

    data = r.json()
    if data.get("errors"):
        print(f"# ERRORI API su {url}: {data['errors']}", file=sys.stderr)
    return data.get("response", [])


# =========================================================
# 1) FIXTURES DI OGGI (NESSUN FILTRO)
# =========================================================

def get_fixtures_for_date(target_date):
    """
    Prende tutti i fixtures del giorno target_date in tutte le nazioni/leghe.
    """
    fixtures = api_get(
        "/fixtures",
        {
            "date": target_date,
            "timezone": "Europe/Dublin",  # per avere orario coerente con te
        },
    )

    print(
        f"# Partite totali trovate per {target_date}: {len(fixtures)}",
        file=sys.stderr,
    )
    return fixtures


# =========================================================
# 2) PREDICTIONS PER FIXTURE
# =========================================================

def get_prediction_for_fixture(fixture_id):
    """
    Chiama /predictions?fixture={id} e torna un dict con info essenziali,
    incluse le percentuali home/draw/away se disponibili.
    """
    preds = api_get("/predictions", {"fixture": fixture_id})
    if not preds:
        return {}

    block = preds[0]  # di solito c'è un solo elemento
    p_block = block.get("predictions") or {}

    winner = p_block.get("winner") or {}
    goals = p_block.get("goals") or {}
    percent = p_block.get("percent") or {}

    return {
        "pred_winner_id": winner.get("id"),
        "pred_winner_name": winner.get("name"),
        "pred_winner_comment": winner.get("comment"),
        "win_or_draw": p_block.get("win_or_draw"),
        "under_over": p_block.get("under_over"),
        "advice": p_block.get("advice"),
        "goals_home": goals.get("home"),
        "goals_away": goals.get("away"),
        "prob_home": percent.get("home"),
        "prob_draw": percent.get("draw"),
        "prob_away": percent.get("away"),
    }


# =========================================================
# 3) ODDS PER FIXTURE (PRIORITÀ BET365)
# =========================================================

PREFERRED_BOOKMAKER_NAMES = {"Bet365", "bet365", "bet365.com", "Bet 365"}

def extract_match_winner(bets):
    """
    Estrae le quote 1X2 dal mercato 'Match Winner'.
    """
    result = {
        "odd_home": None,
        "odd_draw": None,
        "odd_away": None,
    }

    match_winner_bet = next(
        (b for b in bets if b.get("name") == "Match Winner"),
        None,
    )
    if not match_winner_bet:
        return result

    values = match_winner_bet.get("values") or []
    for v in values:
        value = v.get("value")
        odd = v.get("odd")
        if value == "Home":
            result["odd_home"] = odd
        elif value == "Draw":
            result["odd_draw"] = odd
        elif value == "Away":
            result["odd_away"] = odd

    return result


def extract_over_under(bets):
    """
    Estrae le quote Over/Under dal mercato 'Goals Over/Under'.
    Ritorna:
      - odd_ou_1_5_over
      - odd_ou_2_5_over
      - odd_ou_2_5_under
      - odd_ou_3_5_over
    """
    result = {
        "odd_ou_1_5_over": None,
        "odd_ou_2_5_over": None,
        "odd_ou_2_5_under": None,
        "odd_ou_3_5_over": None,
    }

    ou_bet = next(
        (b for b in bets if b.get("name") == "Goals Over/Under"),
        None,
    )
    if not ou_bet:
        return result

    values = ou_bet.get("values") or []
    for v in values:
        label = v.get("value") or ""
        odd = v.get("odd")
        if not label or odd is None:
            continue

        label = label.strip()
        # es: "Over 2.5", "Under 2.5"
        if label.startswith("Over "):
            line = label[5:].strip()  # dopo "Over "
            if line == "1.5":
                result["odd_ou_1_5_over"] = odd
            elif line == "2.5":
                result["odd_ou_2_5_over"] = odd
            elif line == "3.5":
                result["odd_ou_3_5_over"] = odd
        elif label.startswith("Under "):
            line = label[6:].strip()  # dopo "Under "
            if line == "2.5":
                result["odd_ou_2_5_under"] = odd

    return result


def extract_btts(bets):
    """
    Estrae le quote BTTS dal mercato 'Both Teams To Score'.
    """
    result = {
        "odd_btts_yes": None,
        "odd_btts_no": None,
    }

    btts_bet = next(
        (b for b in bets if b.get("name") == "Both Teams To Score"),
        None,
    )
    if not btts_bet:
        return result

    values = btts_bet.get("values") or []
    for v in values:
        value = v.get("value")
        odd = v.get("odd")
        if value == "Yes":
            result["odd_btts_yes"] = odd
        elif value == "No":
            result["odd_btts_no"] = odd

    return result


def get_odds_for_fixture(fixture_id):
    """
    Chiama /odds?fixture={id}, sceglie il bookmaker (preferendo Bet365)
    e ritorna tutte le quote richieste.
    """
    odds_list = api_get("/odds", {"fixture": fixture_id})
    if not odds_list:
        return {}

    o = odds_list[0]

    bookmakers = o.get("bookmakers") or []
    if not bookmakers:
        return {}

    # Scelta del bookmaker: prima Bet365 se presente, altrimenti il primo
    chosen_bookmaker = None
    fallback_bookmaker = None

    for bookmaker in bookmakers:
        name = (bookmaker.get("name") or "").strip()
        if not fallback_bookmaker:
            fallback_bookmaker = bookmaker
        if name in PREFERRED_BOOKMAKER_NAMES:
            chosen_bookmaker = bookmaker
            break

    if not chosen_bookmaker:
        chosen_bookmaker = fallback_bookmaker

    bets = chosen_bookmaker.get("bets") or []

    mw = extract_match_winner(bets)
    ou = extract_over_under(bets)
    btts = extract_btts(bets)

    result = {
        "bookmaker": chosen_bookmaker.get("name"),
    }
    result.update(mw)
    result.update(ou)
    result.update(btts)

    return result


# =========================================================
# 4) STAMPA CSV NEI LOG
# =========================================================

def sanitize_field(value):
    """
    Converte in stringa ed elimina eventuali ';' sostituendole con ','.
    Evita di rompere il CSV.
    """
    if value is None:
        return ""
    s = str(value)
    return s.replace(";", ",")


def main():
    fixtures = get_fixtures_for_date(TARGET_DATE)

    # Header CSV
    print("### CSV_INIZIO ###")
    print(
        "fixture_id;"
        "date;"
        "time;"
        "league_id;"
        "league_name;"
        "country;"
        "season;"
        "round;"
        "status_short;"
        "status_long;"
        "venue_name;"
        "venue_city;"
        "home_team;"
        "away_team;"
        "prediction_winner_name;"
        "prediction_winner_comment;"
        "prediction_win_or_draw;"
        "prediction_under_over;"
        "prediction_advice;"
        "prediction_goals_home;"
        "prediction_goals_away;"
        "prob_home;"
        "prob_draw;"
        "prob_away;"
        "bookmaker;"
        "odd_home;"
        "odd_draw;"
        "odd_away;"
        "odd_ou_1_5_over;"
        "odd_ou_2_5_over;"
        "odd_ou_2_5_under;"
        "odd_ou_3_5_over;"
        "odd_btts_yes;"
        "odd_btts_no"
    )

    for f in fixtures:
        fixture_info = f.get("fixture", {})
        league = f.get("league", {})
        teams = f.get("teams", {})

        fixture_id = fixture_info.get("id", "")
        date_time = fixture_info.get("date", "")  # ISO, es. 2025-11-26T19:45:00+00:00

        date_part = ""
        time_part = ""
        if isinstance(date_time, str) and len(date_time) >= 16:
            date_part = date_time[:10]
            time_part = date_time[11:16]  # HH:MM

        status = fixture_info.get("status") or {}
        venue = fixture_info.get("venue") or {}

        # Prediction
        pred = get_prediction_for_fixture(fixture_id) or {}

        # Odds
        odds = get_odds_for_fixture(fixture_id) or {}

        row = [
            fixture_id,
            date_part,
            time_part,
            league.get("id", ""),
            league.get("name", ""),
            league.get("country", ""),
            league.get("season", ""),
            league.get("round", ""),
            status.get("short", ""),
            status.get("long", ""),
            venue.get("name", ""),
            venue.get("city", ""),
            (teams.get("home") or {}).get("name", ""),
            (teams.get("away") or {}).get("name", ""),
            pred.get("pred_winner_name", ""),
            pred.get("pred_winner_comment", ""),
            pred.get("win_or_draw", ""),
            pred.get("under_over", ""),
            pred.get("advice", ""),
            pred.get("goals_home", ""),
            pred.get("goals_away", ""),
            pred.get("prob_home", ""),
            pred.get("prob_draw", ""),
            pred.get("prob_away", ""),
            odds.get("bookmaker", ""),
            odds.get("odd_home", ""),
            odds.get("odd_draw", ""),
            odds.get("odd_away", ""),
            odds.get("odd_ou_1_5_over", ""),
            odds.get("odd_ou_2_5_over", ""),
            odds.get("odd_ou_2_5_under", ""),
            odds.get("odd_ou_3_5_over", ""),
            odds.get("odd_btts_yes", ""),
            odds.get("odd_btts_no", ""),
        ]

        print(";".join(sanitize_field(v) for v in row))

    print("### CSV_FINE ###")


# =========================================================
# 5) GESTIONE "UNA SOLA ESECUZIONE" + MINI WEB SERVER
# =========================================================

def already_ran_for_target_date():
    """
    Ritorna True se il marker indica che abbiamo già eseguito lo scraping
    per la TARGET_DATE in questa istanza.
    """
    try:
        with open(RUN_MARKER_PATH, "r") as f:
            content = f.read().strip()
            return content == TARGET_DATE
    except FileNotFoundError:
        return False
    except Exception as e:
        print(f"# Impossibile leggere marker: {e}", file=sys.stderr)
        return False


def set_run_marker():
    """
    Scrive il marker per la TARGET_DATE.
    """
    try:
        with open(RUN_MARKER_PATH, "w") as f:
            f.write(TARGET_DATE)
    except Exception as e:
        print(f"# Impossibile scrivere marker: {e}", file=sys.stderr)


def run_http_server():
    """
    Avvia un mini HTTP server per tenere vivo il Web Service su Render.
    Risponde semplicemente 'OK'.
    """
    port = int(os.environ.get("PORT", "10000"))

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK\n")

        def log_message(self, format, *args):
            # Evita log rumorosi per ogni richiesta
            return

    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"# HTTP server in ascolto sulla porta {port}", file=sys.stderr)
        httpd.serve_forever()


if __name__ == "__main__":
    if already_ran_for_target_date():
        print("# Scraping già eseguito per questa data, salto le chiamate API.", file=sys.stderr)
    else:
        print("# Avvio scraping per la data target.", file=sys.stderr)
        main()
        set_run_marker()
        print("# Scraping completato, marker aggiornato.", file=sys.stderr)

    # In ogni caso avvia il mini server per tenere vivo il Web Service
    run_http_server()
