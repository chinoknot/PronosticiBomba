import requests
from datetime import date
import sys

# =========================================================
# CONFIGURAZIONE
# =========================================================

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"  # puoi lasciarla così o mettere in env var
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

# Coppe UEFA segnate come "World" nell'API ma che vogliamo includere comunque
# (ID da documentazione/comunità: UCL=2, UEL=3, Conference=4, Super Cup=5) :contentReference[oaicite:5]{index=5}
EUROPEAN_UEFA_LEAGUE_IDS = {2, 3, 4, 5}

# Data target: oggi per te è 26/11/2025
TARGET_DATE = "2025-11-26"  # cambia a date.today().isoformat() se poi vuoi dinamico


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
# 1) PAESI EUROPEI
# =========================================================

def get_european_country_names():
    """
    Scarica tutti i paesi da /countries e ritorna l'insieme dei nomi
    con continent == 'Europe'.
    """
    countries = api_get("/countries")
    europe = set()

    for c in countries:
        continent = c.get("continent")
        name = c.get("name")
        if continent == "Europe" and name:
            europe.add(name)

    print(f"# Paesi europei trovati: {len(europe)}", file=sys.stderr)
    return europe


# =========================================================
# 2) FIXTURES DI OGGI IN EUROPA (+ COPPE UEFA)
# =========================================================

def get_european_fixtures_for_date(target_date):
    """
    Prende tutti i fixtures della data indicata e filtra:
      - tutte le leghe con league.country in Europa
      - più le coppe UEFA (league.id in EUROPEAN_UEFA_LEAGUE_IDS)
    """
    europe_countries = get_european_country_names()

    fixtures = api_get(
        "/fixtures",
        {
            "date": target_date,
            "timezone": "Europe/Dublin",
        },
    )

    selected = []
    for f in fixtures:
        league = f.get("league", {})
        league_id = league.get("id")
        league_country = league.get("country")

        if league_country in europe_countries or league_id in EUROPEAN_UEFA_LEAGUE_IDS:
            selected.append(f)

    print(
        f"# Partite trovate nel mondo per {target_date}: {len(fixtures)}",
        file=sys.stderr,
    )
    print(
        f"# Partite filtrate per Europa (+coppe UEFA) per {target_date}: {len(selected)}",
        file=sys.stderr,
    )
    return selected


# =========================================================
# 3) PREDICTIONS PER FIXTURE
# =========================================================

def get_prediction_for_fixture(fixture_id):
    """
    Chiama /predictions?fixture={id} e torna un dict con info essenziali.
    """
    preds = api_get("/predictions", {"fixture": fixture_id})
    if not preds:
        return {}

    # La risposta è una lista, ci interessa il primo elemento
    p_block = preds[0].get("predictions") or {}

    winner = p_block.get("winner") or {}
    goals = p_block.get("goals") or {}

    return {
        "pred_winner_id": winner.get("id"),
        "pred_winner_name": winner.get("name"),
        "pred_winner_comment": winner.get("comment"),
        "win_or_draw": p_block.get("win_or_draw"),
        "under_over": p_block.get("under_over"),
        "advice": p_block.get("advice"),
        "goals_home": goals.get("home"),
        "goals_away": goals.get("away"),
    }


# =========================================================
# 4) ODDS PRINCIPALI (MATCH WINNER 1X2) PER FIXTURE
# =========================================================

def get_main_odds_for_fixture(fixture_id):
    """
    Chiama /odds?fixture={id} e cerca il mercato 'Match Winner'.
    Ritorna bookmaker usato e quote 1X2 se trovate.
    """
    odds_list = api_get("/odds", {"fixture": fixture_id})
    if not odds_list:
        return {}

    # odds_list è per league/fixture. Prendiamo il primo item.
    o = odds_list[0]

    bookmakers = o.get("bookmakers") or []
    for bookmaker in bookmakers:
        bets = bookmaker.get("bets") or []

        match_winner_bet = next(
            (b for b in bets if b.get("name") == "Match Winner"),
            None,
        )
        if not match_winner_bet:
            continue

        values = match_winner_bet.get("values") or []
        value_map = {v.get("value"): v.get("odd") for v in values}

        return {
            "bookmaker": bookmaker.get("name"),
            "odd_home": value_map.get("Home"),
            "odd_draw": value_map.get("Draw"),
            "odd_away": value_map.get("Away"),
        }

    # Nessun mercato 'Match Winner'
    return {}


# =========================================================
# 5) STAMPA CSV NEI LOG
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
    fixtures = get_european_fixtures_for_date(TARGET_DATE)

    # Header CSV
    print("### CSV_INIZIO ###")
    print(
        "fixture_id;"
        "date;"
        "time;"
        "league_id;"
        "league_name;"
        "country;"
        "home_team;"
        "away_team;"
        "prediction_winner_name;"
        "prediction_winner_comment;"
        "prediction_win_or_draw;"
        "prediction_under_over;"
        "prediction_advice;"
        "prediction_goals_home;"
        "prediction_goals_away;"
        "bookmaker;"
        "odd_home;"
        "odd_draw;"
        "odd_away"
    )

    for f in fixtures:
        fixture_info = f.get("fixture", {})
        league = f.get("league", {})
        teams = f.get("teams", {})

        fixture_id = fixture_info.get("id", "")
        date_time = fixture_info.get("date", "")  # formato ISO, es. 2025-11-26T19:45:00+00:00

        date_part = ""
        time_part = ""
        if isinstance(date_time, str) and len(date_time) >= 16:
            date_part = date_time[:10]
            time_part = date_time[11:16]  # HH:MM

        # Prediction
        pred = get_prediction_for_fixture(fixture_id) or {}

        # Odds
        odds = get_main_odds_for_fixture(fixture_id) or {}

        row = [
            fixture_id,
            date_part,
            time_part,
            league.get("id", ""),
            league.get("name", ""),
            league.get("country", ""),
            (teams.get("home") or {}).get("name", ""),
            (teams.get("away") or {}).get("name", ""),
            pred.get("pred_winner_name", ""),
            pred.get("pred_winner_comment", ""),
            pred.get("win_or_draw", ""),
            pred.get("under_over", ""),
            pred.get("advice", ""),
            pred.get("goals_home", ""),
            pred.get("goals_away", ""),
            odds.get("bookmaker", ""),
            odds.get("odd_home", ""),
            odds.get("odd_draw", ""),
            odds.get("odd_away", ""),
        ]

        print(";".join(sanitize_field(v) for v in row))

    print("### CSV_FINE ###")


if __name__ == "__main__":
    main()
