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

# Data target (per ora fissa, poi la potrai rendere dinamica)
TARGET_DATE = "2025-11-26"

# Marker per evitare doppia esecuzione sulla stessa istanza
RUN_MARKER_PATH = "/tmp/last_run_marker.txt"


# =========================================================
# FUNZIONI DI BASE
# =========================================================

def api_get(path, params=None, timeout=15):
    """Wrapper semplice: torna solo 'response'."""
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json().get("response", [])


def get_fixtures_for_date(target_date):
    """Tutti i fixtures del giorno, nessun filtro su leghe/zone."""
    r = requests.get(
        f"{BASE_URL}/fixtures",
        headers=HEADERS,
        params={"date": target_date, "timezone": "Europe/Dublin"},
        timeout=20,
    )
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

    b = preds[0].get("predictions") or {}

    return {
        "pred_winner_name": (b.get("winner") or {}).get("name"),
        "pred_winner_comment": (b.get("winner") or {}).get("comment"),
        "win_or_draw": b.get("win_or_draw"),
        "under_over": b.get("under_over"),
        "advice": b.get("advice"),
        "goals_home": (b.get("goals") or {}).get("home"),
        "goals_away": (b.get("goals") or {}).get("away"),
        "prob_home": (b.get("percent") or {}).get("home"),
        "prob_draw": (b.get("percent") or {}).get("draw"),
        "prob_away": (b.get("percent") or {}).get("away"),
    }


# =========================================================
# ODDS (1X2, O/U, BTTS)
# =========================================================

PREFERRED_BOOKMAKER_NAMES = {"Bet365", "bet365", "bet365.com", "Bet 365"}


def extract_match_winner(bets):
    res = {"odd_home": "", "odd_draw": "", "odd_away": ""}
    for b in bets:
        if b.get("name") == "Match Winner":
            for v in b.get("values", []):
                val = v.get("value")
                odd = v.get("odd", "")
                if val == "Home":
                    res["odd_home"] = odd
                elif val == "Draw":
                    res["odd_draw"] = odd
                elif val == "Away":
                    res["odd_away"] = odd
    return res


def extract_over_under(bets):
    res = {
        "odd_ou_1_5_over": "",
        "odd_ou_2_5_over": "",
        "odd_ou_2_5_under": "",
        "odd_ou_3_5_over": "",
    }
    for b in bets:
        if b.get("name") == "Goals Over/Under":
            for v in b.get("values", []):
                label = str(v.get("value", "")).strip()
                odd = v.get("odd", "")
                if label == "Over 1.5":
                    res["odd_ou_1_5_over"] = odd
                elif label == "Over 2.5":
                    res["odd_ou_2_5_over"] = odd
                elif label == "Under 2.5":
                    res["odd_ou_2_5_under"] = odd
                elif label == "Over 3.5":
                    res["odd_ou_3_5_over"] = odd
    return res


def extract_btts(bets):
    res = {"odd_btts_yes": "", "odd_btts_no": ""}
    for b in bets:
        if b.get("name") == "Both Teams To Score":
            for v in b.get("values", []):
                val = v.get("value")
                odd = v.get("odd", "")
                if val == "Yes":
                    res["odd_btts_yes"] = odd
                elif val == "No":
                    res["odd_btts_no"] = odd
    return res


def get_odds_for_fixture(fixture_id):
    """Odds per fixture: Bet365 prioritario per 1X2 e O/U, BTTS anche da altri book."""
    data = api_get("/odds", {"fixture": fixture_id})
    if not data:
        return {}

    bookmakers = data[0].get("bookmakers", [])
    if not bookmakers:
        return {}

    # 1) bookmaker principale (Bet365 se c'è)
    chosen = None
    for b in bookmakers:
        if b.get("name") in PREFERRED_BOOKMAKER_NAMES:
            chosen = b
            break
    if chosen is None:
        chosen = bookmakers[0]

    bets_main = chosen.get("bets", [])

    res = {"bookmaker": chosen.get("name")}
    res.update(extract_match_winner(bets_main))
    res.update(extract_over_under(bets_main))

    # BTTS: prima proviamo sul principale
    btts = extract_btts(bets_main)

    # Se il principale non ce l'ha, cerchiamo negli altri bookmaker (stessa chiamata)
    if not btts["odd_btts_yes"] and not btts["odd_btts_no"]:
        for b in bookmakers:
            bets_b = b.get("bets", [])
            alt = extract_btts(bets_b)
            if alt["odd_btts_yes"] or alt["odd_btts_no"]:
                btts = alt
                break

    res.update(btts)
    return res


# =========================================================
# STATISTICHE FIXTURE (CORNERS, CARDS)
# =========================================================

def get_statistics_for_fixture(fixture_id, home_team_id, away_team_id):
    """
    /fixtures/statistics?fixture={fixture_id}
    Estrae corners e gialli/rossi per team home/away.
    """
    stats_list = api_get("/fixtures/statistics", {"fixture": fixture_id}, timeout=20)

    # valori di default vuoti (stringhe, poi sanitize li gestisce)
    result = {
        "corners_home": "",
        "corners_away": "",
        "yellow_cards_home": "",
        "yellow_cards_away": "",
        "red_cards_home": "",
        "red_cards_away": "",
    }

    # costruiamo dizionario: team_id -> { type: value }
    per_team = {}

    for entry in stats_list:
        team = entry.get("team") or {}
        team_id = team.get("id")
        if team_id is None:
            continue

        stat_map = {}
        for s in entry.get("statistics", []):
            stype = s.get("type")
            value = s.get("value")
            stat_map[stype] = value
        per_team[team_id] = stat_map

    def fill_for_team(team_id, side_prefix):
        stat_map = per_team.get(team_id) or {}
        corners = stat_map.get("Corner Kicks")
        if corners is None:
            corners = stat_map.get("Corners")
        yellow = stat_map.get("Yellow Cards")
        red = stat_map.get("Red Cards")

        result[f"corners_{side_prefix}"] = corners if corners is not None else ""
        result[f"yellow_cards_{side_prefix}"] = yellow if yellow is not None else ""
        result[f"red_cards_{side_prefix}"] = red if red is not None else ""

    if home_team_id is not None:
        fill_for_team(home_team_id, "home")
    if away_team_id is not None:
        fill_for_team(away_team_id, "away")

    return result


# =========================================================
# CSV
# =========================================================

def sanitize(v):
    return "" if v is None else str(v).replace(";", ",")


def main():
    fixtures = get_fixtures_for_date(TARGET_DATE)

    csv_rows = []
    header = (
        "fixture_id;date;time;league_id;league_name;country;season;round;"
        "status_short;status_long;venue_name;venue_city;referee_name;"
        "home_team;away_team;corners_home;corners_away;"
        "yellow_cards_home;yellow_cards_away;red_cards_home;red_cards_away;"
        "prediction_winner_name;prediction_winner_comment;prediction_win_or_draw;"
        "prediction_under_over;prediction_advice;prediction_goals_home;prediction_goals_away;"
        "prob_home;prob_draw;prob_away;bookmaker;odd_home;odd_draw;odd_away;"
        "odd_ou_1_5_over;odd_ou_2_5_over;odd_ou_2_5_under;odd_ou_3_5_over;odd_btts_yes;odd_btts_no"
    )
    csv_rows.append(header)

    for f in fixtures:
        fixture_id = ""
        try:
            fx = f.get("fixture", {})
            league = f.get("league", {})
            teams = f.get("teams", {})
            fixture_id = fx.get("id", "")

            dateiso = fx.get("date", "")
            d = dateiso[:10] if len(dateiso) >= 10 else ""
            t = dateiso[11:16] if len(dateiso) >= 16 else ""

            status = fx.get("status", {}) or {}
            venue = fx.get("venue", {}) or {}
            referee_name = fx.get("referee", "")

            home_team = teams.get("home", {}) or {}
            away_team = teams.get("away", {}) or {}
            home_team_id = home_team.get("id")
            away_team_id = away_team.get("id")

            # Predictions
            pred = get_prediction_for_fixture(fixture_id)

            # Odds
            odds = get_odds_for_fixture(fixture_id)

            # Statistiche (corners + cards)
            stats = get_statistics_for_fixture(fixture_id, home_team_id, away_team_id)

            row = [
                fixture_id,
                d,
                t,
                league.get("id", ""),
                league.get("name", ""),
                league.get("country", ""),
                league.get("season", ""),
                league.get("round", ""),
                status.get("short", ""),
                status.get("long", ""),
                venue.get("name", ""),
                venue.get("city", ""),
                referee_name,
                home_team.get("name", ""),
                away_team.get("name", ""),
                stats.get("corners_home", ""),
                stats.get("corners_away", ""),
                stats.get("yellow_cards_home", ""),
                stats.get("yellow_cards_away", ""),
                stats.get("red_cards_home", ""),
                stats.get("red_cards_away", ""),
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

            csv_rows.append(";".join(sanitize(v) for v in row))

        except Exception as e:
            print(f"# ERRORE fixture {fixture_id}: {e}", file=sys.stderr)
            continue

    print("### CSV_INIZIO ###")
    print("\n".join(csv_rows))
    print("### CSV_FINE ###")


# =========================================================
# MARCATORE & MINI WEB SERVER PER RENDER
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
            pass

    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"# HTTP server running on port {port}", file=sys.stderr)
        httpd.serve_forever()


if __name__ == "__main__":
    if not already_ran_for_target_date():
        print("# Avvio scraping", file=sys.stderr)
        main()
        set_run_marker()
        print("# Scraping completato", file=sys.stderr)
    else:
        print("# Scraping già fatto oggi", file=sys.stderr)

    run_http_server()
