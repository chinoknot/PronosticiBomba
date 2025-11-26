import requests
import sys
import os
import http.server
import socketserver

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

TARGET_DATE = "2025-11-26"
RUN_MARKER_PATH = "/tmp/last_run_marker.txt"


def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json().get("response", [])


def get_fixtures_for_date(target_date):
    r = requests.get(
        f"{BASE_URL}/fixtures",
        headers=HEADERS,
        params={"date": target_date, "timezone": "Europe/Dublin"},
        timeout=20
    )
    r.raise_for_status()
    data = r.json()
    resp = data.get("response", [])
    print(f"# Partite totali trovate per {target_date}: {len(resp)}", file=sys.stderr)
    return resp


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


PREFERRED_BOOKMAKER_NAMES = {"Bet365", "bet365", "bet365.com", "Bet 365"}


def extract_match_winner(bets):
    res = {"odd_home": "", "odd_draw": "", "odd_away": ""}
    for b in bets:
        if b.get("name") == "Match Winner":
            for v in b.get("values", []):
                if v.get("value") == "Home": res["odd_home"] = v.get("odd", "")
                if v.get("value") == "Draw": res["odd_draw"] = v.get("odd", "")
                if v.get("value") == "Away": res["odd_away"] = v.get("odd", "")
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
                if label == "Over 1.5": res["odd_ou_1_5_over"] = odd
                if label == "Over 2.5": res["odd_ou_2_5_over"] = odd
                if label == "Under 2.5": res["odd_ou_2_5_under"] = odd
                if label == "Over 3.5": res["odd_ou_3_5_over"] = odd
    return res


def extract_btts(bets):
    res = {"odd_btts_yes": "", "odd_btts_no": ""}
    for b in bets:
        if b.get("name") == "Both Teams To Score":
            for v in b.get("values", []):
                if v.get("value") == "Yes": res["odd_btts_yes"] = v.get("odd", "")
                if v.get("value") == "No": res["odd_btts_no"] = v.get("odd", "")
    return res


def get_odds_for_fixture(fixture_id):
    data = api_get("/odds", {"fixture": fixture_id})
    if not data:
        return {}
    books = data[0].get("bookmakers", [])
    if not books:
        return {}

    chosen = None
    for b in books:
        if b.get("name") in PREFERRED_BOOKMAKER_NAMES:
            chosen = b
            break
    chosen = chosen or books[0]

    bets = chosen.get("bets", [])

    res = {"bookmaker": chosen.get("name")}
    res.update(extract_match_winner(bets))
    res.update(extract_over_under(bets))
    res.update(extract_btts(bets))
    return res


def sanitize(v):
    return "" if v is None else str(v).replace(";", ",")


def main():
    fixtures = get_fixtures_for_date(TARGET_DATE)

    csv_rows = []
    header = (
        "fixture_id;date;time;league_id;league_name;country;season;round;"
        "status_short;status_long;venue_name;venue_city;home_team;away_team;"
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

            status = fx.get("status", {})
            venue = fx.get("venue", {})

            pred = get_prediction_for_fixture(fixture_id)
            odds = get_odds_for_fixture(fixture_id)

            row = [
                fixture_id, d, t,
                league.get("id", ""), league.get("name", ""), league.get("country", ""),
                league.get("season", ""), league.get("round", ""),
                status.get("short", ""), status.get("long", ""),
                venue.get("name", ""), venue.get("city", ""),
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
                odds.get("bookmaker",""),
                odds.get("odd_home",""),
                odds.get("odd_draw",""),
                odds.get("odd_away",""),
                odds.get("odd_ou_1_5_over",""),
                odds.get("odd_ou_2_5_over",""),
                odds.get("odd_ou_2_5_under",""),
                odds.get("odd_ou_3_5_over",""),
                odds.get("odd_btts_yes",""),
                odds.get("odd_btts_no",""),
            ]

            csv_rows.append(";".join(sanitize(v) for v in row))

        except Exception as e:
            print(f"# ERRORE fixture {fixture_id}: {e}", file=sys.stderr)
            continue

    # STAMPA TUTTO IN UNA VOLTA → NIENTE BLOCCO OUTPUT
    print("### CSV_INIZIO ###")
    print("\n".join(csv_rows))
    print("### CSV_FINE ###")


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
