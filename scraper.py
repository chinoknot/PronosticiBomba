import requests
import sys
import os
import http.server
import socketserver

# =========================================================
# CONFIGURAZIONE
# =========================================================

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

TARGET_DATE = "2025-11-26"

RUN_MARKER_PATH = "/tmp/last_run_marker.txt"


# =========================================================
# FUNZIONI DI BASE
# =========================================================

def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("response", [])


# =========================================================
# FIXTURES DELLA GIORNATA
# =========================================================

def get_fixtures_for_date(target_date):
    params = {"date": target_date, "timezone": "Europe/Dublin"}
    r = requests.get(f"{BASE_URL}/fixtures", headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    resp = data.get("response", [])
    print(f"# Partite totali trovate per {target_date}: {len(resp)}", file=sys.stderr)
    return resp


# =========================================================
# PREDICTIONS
# =========================================================

def get_prediction_for_fixture(fixture_id):
    preds = api_get("/predictions", {"fixture": fixture_id})
    if not preds:
        return {}

    block = preds[0]
    p_block = block.get("predictions") or {}
    winner = p_block.get("winner") or {}
    goals = p_block.get("goals") or {}
    percent = p_block.get("percent") or {}

    return {
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
# ODDS
# =========================================================

PREFERRED_BOOKMAKER_NAMES = {"Bet365", "bet365", "bet365.com", "Bet 365"}


def extract_match_winner(bets):
    result = {"odd_home": "", "odd_draw": "", "odd_away": ""}
    for b in bets:
        if b.get("name") == "Match Winner":
            for v in b.get("values", []):
                if v.get("value") == "Home":
                    result["odd_home"] = v.get("odd", "")
                elif v.get("value") == "Draw":
                    result["odd_draw"] = v.get("odd", "")
                elif v.get("value") == "Away":
                    result["odd_away"] = v.get("odd", "")
    return result


def extract_over_under(bets):
    result = {
        "odd_ou_1_5_over": "",
        "odd_ou_2_5_over": "",
        "odd_ou_2_5_under": "",
        "odd_ou_3_5_over": "",
    }
    for b in bets:
        if b.get("name") == "Goals Over/Under":
            for v in b.get("values", []):
                val = str(v.get("value", "")).strip()
                odd = v.get("odd", "")
                if val == "Over 1.5":
                    result["odd_ou_1_5_over"] = odd
                elif val == "Over 2.5":
                    result["odd_ou_2_5_over"] = odd
                elif val == "Under 2.5":
                    result["odd_ou_2_5_under"] = odd
                elif val == "Over 3.5":
                    result["odd_ou_3_5_over"] = odd
    return result


def extract_btts(bets):
    result = {"odd_btts_yes": "", "odd_btts_no": ""}
    for b in bets:
        if b.get("name") == "Both Teams To Score":
            for v in b.get("values", []):
                if v.get("value") == "Yes":
                    result["odd_btts_yes"] = v.get("odd", "")
                elif v.get("value") == "No":
                    result["odd_btts_no"] = v.get("odd", "")
    return result


def get_odds_for_fixture(fixture_id):
    odds_list = api_get("/odds", {"fixture": fixture_id})
    if not odds_list:
        return {}

    bookmakers = odds_list[0].get("bookmakers", [])
    if not bookmakers:
        return {}

    chosen = None
    fallback = bookmakers[0]

    for b in bookmakers:
        if b.get("name") in PREFERRED_BOOKMAKER_NAMES:
            chosen = b
            break

    chosen = chosen or fallback
    bets = chosen.get("bets", [])

    result = {
        "bookmaker": chosen.get("name"),
    }
    result.update(extract_match_winner(bets))
    result.update(extract_over_under(bets))
    result.update(extract_btts(bets))

    return result


# =========================================================
# CSV
# =========================================================

def sanitize_field(v):
    if v is None:
        return ""
    return str(v).replace(";", ",")


def main():
    fixtures = get_fixtures_for_date(TARGET_DATE)

    print("### CSV_INIZIO ###")
    print(
        "fixture_id;date;time;league_id;league_name;country;season;round;"
        "status_short;status_long;venue_name;venue_city;home_team;away_team;"
        "prediction_winner_name;prediction_winner_comment;prediction_win_or_draw;"
        "prediction_under_over;prediction_advice;prediction_goals_home;prediction_goals_away;"
        "prob_home;prob_draw;prob_away;bookmaker;odd_home;odd_draw;odd_away;"
        "odd_ou_1_5_over;odd_ou_2_5_over;odd_ou_2_5_under;odd_ou_3_5_over;odd_btts_yes;odd_btts_no"
    )

    for f in fixtures:
        fixture_id = ""
        try:
            fixture = f.get("fixture", {})
            league = f.get("league", {})
            teams = f.get("teams", {})
            fixture_id = fixture.get("id", "")

            # DATA
            date_iso = fixture.get("date", "")
            date_part = date_iso[:10] if len(date_iso) >= 10 else ""
            time_part = date_iso[11:16] if len(date_iso) >= 16 else ""

            status = fixture.get("status", {})
            venue = fixture.get("venue", {})

            # Prediction
            pred = get_prediction_for_fixture(fixture_id)

            # Odds
            odds = get_odds_for_fixture(fixture_id)

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
                teams.get("home", {}).get("name", ""),
                teams.get("away", {}).get("name", ""),
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

        except Exception as e:
            print(f"# ERRORE SU FIXTURE {fixture_id}: {e}", file=sys.stderr)
            continue

    print("### CSV_FINE ###")


# =========================================================
# MARCATORE & SERVER HTTP
# =========================================================

def already_ran_for_target_date():
    try:
        with open(RUN_MARKER_PATH, "r") as f:
            return f.read().strip() == TARGET_DATE
    except:
        return False


def set_run_marker():
    try:
        with open(RUN_MARKER_PATH, "w") as f:
            f.write(TARGET_DATE)
    except:
        pass


def run_http_server():
    port = int(os.environ.get("PORT", "10000"))

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK\n")

        def log_message(self, format, *args):
            return

    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"# HTTP server in ascolto sulla porta {port}", file=sys.stderr)
        httpd.serve_forever()


if __name__ == "__main__":
    if already_ran_for_target_date():
        print("# Scraping già eseguito per questa data, niente API calls.", file=sys.stderr)
    else:
        print("# Avvio scraping per questa data.", file=sys.stderr)
        main()
        set_run_marker()
        print("# Scraping completato, marker aggiornato.", file=sys.stderr)

    run_http_server()
