import requests
import sys
import os
import http.server
import socketserver
import time

# =========================================================
# CONFIGURAZIONE
# =========================================================

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

# Data target (per ora fissa, poi la puoi rendere dinamica)
TARGET_DATE = "2025-11-26"

# Marker per evitare doppia esecuzione sulla stessa istanza
RUN_MARKER_PATH = "/tmp/last_run_marker.txt"

# Cache per le statistiche di squadra (league, season, team)
TEAM_STATS_CACHE = {}


# =========================================================
# FUNZIONI DI BASE
# =========================================================

def api_get(path, params=None, timeout=15):
    """Wrapper semplice: torna solo 'response' (lista) per gli endpoint standard."""
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

    # bookmaker principale (Bet365 se c'è)
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
# STATISTICHE FIXTURE (CORNERS, CARDS) - /fixtures/statistics
# =========================================================

def get_statistics_for_fixture(fixture_id, home_team_id, away_team_id):
    """
    /fixtures/statistics?fixture={fixture_id}
    Estrae corners e gialli/rossi per team home/away.
    """
    stats_list = api_get("/fixtures/statistics", {"fixture": fixture_id}, timeout=20)

    result = {
        "corners_home": "",
        "corners_away": "",
        "yellow_cards_home": "",
        "yellow_cards_away": "",
        "red_cards_home": "",
        "red_cards_away": "",
    }

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
# TEAM STATISTICS - /teams/statistics (per squadra, league, season)
# =========================================================

def get_team_statistics_raw(league_id, season, team_id):
    """
    Ritorna il 'response' grezzo di /teams/statistics (dict).
    Usa cache per non ripetere le chiamate.
    """
    if not league_id or not season or not team_id:
        return {}

    key = (league_id, season, team_id)
    if key in TEAM_STATS_CACHE:
        return TEAM_STATS_CACHE[key]

    url = f"{BASE_URL}/teams/statistics"
    params = {"league": league_id, "season": season, "team": team_id}
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        ts = data.get("response") or {}
    except Exception as e:
        print(f"# ERRORE /teams/statistics league={league_id} season={season} team={team_id}: {e}", file=sys.stderr)
        ts = {}

    TEAM_STATS_CACHE[key] = ts

    # piccolo throttling per non bombardare l'API
    time.sleep(0.2)

    return ts


def flatten_team_stats(ts, prefix):
    """
    'ts' è il dict 'response' di /teams/statistics.
    Ritorna un dict con chiavi prefissate (home_/away_).
    """
    out = {}

    if not isinstance(ts, dict) or not ts:
        # riempiamo con vuoti, tanto poi .get(..., "") in main copre i None
        keys = [
            "form",
            "fixtures_played_home", "fixtures_played_away",
            "fixtures_wins_home", "fixtures_wins_away",
            "fixtures_draws_home", "fixtures_draws_away",
            "fixtures_loses_home", "fixtures_loses_away",
            "goals_for_total_home", "goals_for_total_away",
            "goals_against_total_home", "goals_against_total_away",
            "goals_for_avg_home", "goals_for_avg_away",
            "goals_against_avg_home", "goals_against_avg_away",
            "clean_sheet_home", "clean_sheet_away",
            "failed_to_score_home", "failed_to_score_away",
            "streak_wins", "streak_draws", "streak_loses",
        ]
        for k in keys:
            out[prefix + k] = ""
        # OU keys dinamiche, li definiamo dopo comunque
    else:
        form = ts.get("form")

        fixtures = ts.get("fixtures") or {}
        played = fixtures.get("played") or {}
        wins = fixtures.get("wins") or {}
        draws = fixtures.get("draws") or {}
        loses = fixtures.get("loses") or {}

        goals = ts.get("goals") or {}
        g_for_total = (goals.get("for") or {}).get("total") or {}
        g_against_total = (goals.get("against") or {}).get("total") or {}
        g_for_avg = (goals.get("for") or {}).get("average") or {}
        g_against_avg = (goals.get("against") or {}).get("average") or {}

        clean_sheet = ts.get("clean_sheet") or {}
        failed_to_score = ts.get("failed_to_score") or {}
        biggest = ts.get("biggest") or {}
        streak = biggest.get("streak") or {}

        out[prefix + "form"] = form or ""

        out[prefix + "fixtures_played_home"] = played.get("home", "")
        out[prefix + "fixtures_played_away"] = played.get("away", "")
        out[prefix + "fixtures_wins_home"] = wins.get("home", "")
        out[prefix + "fixtures_wins_away"] = wins.get("away", "")
        out[prefix + "fixtures_draws_home"] = draws.get("home", "")
        out[prefix + "fixtures_draws_away"] = draws.get("away", "")
        out[prefix + "fixtures_loses_home"] = loses.get("home", "")
        out[prefix + "fixtures_loses_away"] = loses.get("away", "")

        out[prefix + "goals_for_total_home"] = g_for_total.get("home", "")
        out[prefix + "goals_for_total_away"] = g_for_total.get("away", "")
        out[prefix + "goals_against_total_home"] = g_against_total.get("home", "")
        out[prefix + "goals_against_total_away"] = g_against_total.get("away", "")

        out[prefix + "goals_for_avg_home"] = g_for_avg.get("home", "")
        out[prefix + "goals_for_avg_away"] = g_for_avg.get("away", "")
        out[prefix + "goals_against_avg_home"] = g_against_avg.get("home", "")
        out[prefix + "goals_against_avg_away"] = g_against_avg.get("away", "")

        out[prefix + "clean_sheet_home"] = clean_sheet.get("home", "")
        out[prefix + "clean_sheet_away"] = clean_sheet.get("away", "")
        out[prefix + "failed_to_score_home"] = failed_to_score.get("home", "")
        out[prefix + "failed_to_score_away"] = failed_to_score.get("away", "")

        out[prefix + "streak_wins"] = streak.get("wins", "")
        out[prefix + "streak_draws"] = streak.get("draws", "")
        out[prefix + "streak_loses"] = streak.get("loses", "")

        # Under/Over per for/against alle soglie 0.5, 1.5, 2.5, 3.5
        gf_uo = (goals.get("for") or {}).get("under_over") or {}
        ga_uo = (goals.get("against") or {}).get("under_over") or {}

        def get_ou(uo_dict, line):
            d = uo_dict.get(str(line)) or {}
            return d.get("over", ""), d.get("under", "")

        for line in [0.5, 1.5, 2.5, 3.5]:
            label = str(line).replace(".", "_")
            over_for, under_for = get_ou(gf_uo, line)
            over_against, under_against = get_ou(ga_uo, line)

            out[f"{prefix}ou_{label}_for_over"] = over_for
            out[f"{prefix}ou_{label}_for_under"] = under_for
            out[f"{prefix}ou_{label}_against_over"] = over_against
            out[f"{prefix}ou_{label}_against_under"] = under_against

    # Se per qualche motivo mancano alcune chiavi OU, le assicuriamo vuote
    for line in [0.5, 1.5, 2.5, 3.5]:
        label = str(line).replace(".", "_")
        for part in ["for_over", "for_under", "against_over", "against_under"]:
            key = f"{prefix}ou_{label}_{part}"
            if key not in out:
                out[key] = ""

    return out


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
        "odd_ou_1_5_over;odd_ou_2_5_over;odd_ou_2_5_under;odd_ou_3_5_over;odd_btts_yes;odd_btts_no;"
        # TEAM STATS HOME
        "home_form;"
        "home_fixtures_played_home;home_fixtures_played_away;"
        "home_fixtures_wins_home;home_fixtures_wins_away;"
        "home_fixtures_draws_home;home_fixtures_draws_away;"
        "home_fixtures_loses_home;home_fixtures_loses_away;"
        "home_goals_for_total_home;home_goals_for_total_away;"
        "home_goals_against_total_home;home_goals_against_total_away;"
        "home_goals_for_avg_home;home_goals_for_avg_away;"
        "home_goals_against_avg_home;home_goals_against_avg_away;"
        "home_clean_sheet_home;home_clean_sheet_away;"
        "home_failed_to_score_home;home_failed_to_score_away;"
        "home_streak_wins;home_streak_draws;home_streak_loses;"
        "home_ou_0_5_for_over;home_ou_0_5_for_under;home_ou_0_5_against_over;home_ou_0_5_against_under;"
        "home_ou_1_5_for_over;home_ou_1_5_for_under;home_ou_1_5_against_over;home_ou_1_5_against_under;"
        "home_ou_2_5_for_over;home_ou_2_5_for_under;home_ou_2_5_against_over;home_ou_2_5_against_under;"
        "home_ou_3_5_for_over;home_ou_3_5_for_under;home_ou_3_5_against_over;home_ou_3_5_against_under;"
        # TEAM STATS AWAY
        "away_form;"
        "away_fixtures_played_home;away_fixtures_played_away;"
        "away_fixtures_wins_home;away_fixtures_wins_away;"
        "away_fixtures_draws_home;away_fixtures_draws_away;"
        "away_fixtures_loses_home;away_fixtures_loses_away;"
        "away_goals_for_total_home;away_goals_for_total_away;"
        "away_goals_against_total_home;away_goals_against_total_away;"
        "away_goals_for_avg_home;away_goals_for_avg_away;"
        "away_goals_against_avg_home;away_goals_against_avg_away;"
        "away_clean_sheet_home;away_clean_sheet_away;"
        "away_failed_to_score_home;away_failed_to_score_away;"
        "away_streak_wins;away_streak_draws;away_streak_loses;"
        "away_ou_0_5_for_over;away_ou_0_5_for_under;away_ou_0_5_against_over;away_ou_0_5_against_under;"
        "away_ou_1_5_for_over;away_ou_1_5_for_under;away_ou_1_5_against_over;away_ou_1_5_against_under;"
        "away_ou_2_5_for_over;away_ou_2_5_for_under;away_ou_2_5_against_over;away_ou_2_5_against_under;"
        "away_ou_3_5_for_over;away_ou_3_5_for_under;away_ou_3_5_against_over;away_ou_3_5_against_under"
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

            league_id = league.get("id")
            season = league.get("season")

            # Predictions
            pred = get_prediction_for_fixture(fixture_id)

            # Odds
            odds = get_odds_for_fixture(fixture_id)

            # Statistiche match (corners + cards)
            stats = get_statistics_for_fixture(fixture_id, home_team_id, away_team_id)

            # Team statistics home/away (con cache + throttling)
            home_ts_raw = get_team_statistics_raw(league_id, season, home_team_id)
            away_ts_raw = get_team_statistics_raw(league_id, season, away_team_id)

            home_ts = flatten_team_stats(home_ts_raw, "home_")
            away_ts = flatten_team_stats(away_ts_raw, "away_")

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
                # HOME TEAM STATS
                home_ts.get("home_form", ""),
                home_ts.get("home_fixtures_played_home", ""),
                home_ts.get("home_fixtures_played_away", ""),
                home_ts.get("home_fixtures_wins_home", ""),
                home_ts.get("home_fixtures_wins_away", ""),
                home_ts.get("home_fixtures_draws_home", ""),
                home_ts.get("home_fixtures_draws_away", ""),
                home_ts.get("home_fixtures_loses_home", ""),
                home_ts.get("home_fixtures_loses_away", ""),
                home_ts.get("home_goals_for_total_home", ""),
                home_ts.get("home_goals_for_total_away", ""),
                home_ts.get("home_goals_against_total_home", ""),
                home_ts.get("home_goals_against_total_away", ""),
                home_ts.get("home_goals_for_avg_home", ""),
                home_ts.get("home_goals_for_avg_away", ""),
                home_ts.get("home_goals_against_avg_home", ""),
                home_ts.get("home_goals_against_avg_away", ""),
                home_ts.get("home_clean_sheet_home", ""),
                home_ts.get("home_clean_sheet_away", ""),
                home_ts.get("home_failed_to_score_home", ""),
                home_ts.get("home_failed_to_score_away", ""),
                home_ts.get("home_streak_wins", ""),
                home_ts.get("home_streak_draws", ""),
                home_ts.get("home_streak_loses", ""),
                home_ts.get("home_ou_0_5_for_over", ""),
                home_ts.get("home_ou_0_5_for_under", ""),
                home_ts.get("home_ou_0_5_against_over", ""),
                home_ts.get("home_ou_0_5_against_under", ""),
                home_ts.get("home_ou_1_5_for_over", ""),
                home_ts.get("home_ou_1_5_for_under", ""),
                home_ts.get("home_ou_1_5_against_over", ""),
                home_ts.get("home_ou_1_5_against_under", ""),
                home_ts.get("home_ou_2_5_for_over", ""),
                home_ts.get("home_ou_2_5_for_under", ""),
                home_ts.get("home_ou_2_5_against_over", ""),
                home_ts.get("home_ou_2_5_against_under", ""),
                home_ts.get("home_ou_3_5_for_over", ""),
                home_ts.get("home_ou_3_5_for_under", ""),
                home_ts.get("home_ou_3_5_against_over", ""),
                home_ts.get("home_ou_3_5_against_under", ""),
                # AWAY TEAM STATS
                away_ts.get("away_form", ""),
                away_ts.get("away_fixtures_played_home", ""),
                away_ts.get("away_fixtures_played_away", ""),
                away_ts.get("away_fixtures_wins_home", ""),
                away_ts.get("away_fixtures_wins_away", ""),
                away_ts.get("away_fixtures_draws_home", ""),
                away_ts.get("away_fixtures_draws_away", ""),
                away_ts.get("away_fixtures_loses_home", ""),
                away_ts.get("away_fixtures_loses_away", ""),
                away_ts.get("away_goals_for_total_home", ""),
                away_ts.get("away_goals_for_total_away", ""),
                away_ts.get("away_goals_against_total_home", ""),
                away_ts.get("away_goals_against_total_away", ""),
                away_ts.get("away_goals_for_avg_home", ""),
                away_ts.get("away_goals_for_avg_away", ""),
                away_ts.get("away_goals_against_avg_home", ""),
                away_ts.get("away_goals_against_avg_away", ""),
                away_ts.get("away_clean_sheet_home", ""),
                away_ts.get("away_clean_sheet_away", ""),
                away_ts.get("away_failed_to_score_home", ""),
                away_ts.get("away_failed_to_score_away", ""),
                away_ts.get("away_streak_wins", ""),
                away_ts.get("away_streak_draws", ""),
                away_ts.get("away_streak_loses", ""),
                away_ts.get("away_ou_0_5_for_over", ""),
                away_ts.get("away_ou_0_5_for_under", ""),
                away_ts.get("away_ou_0_5_against_over", ""),
                away_ts.get("away_ou_0_5_against_under", ""),
                away_ts.get("away_ou_1_5_for_over", ""),
                away_ts.get("away_ou_1_5_for_under", ""),
                away_ts.get("away_ou_1_5_against_over", ""),
                away_ts.get("away_ou_1_5_against_under", ""),
                away_ts.get("away_ou_2_5_for_over", ""),
                away_ts.get("away_ou_2_5_for_under", ""),
                away_ts.get("away_ou_2_5_against_over", ""),
                away_ts.get("away_ou_2_5_against_under", ""),
                away_ts.get("away_ou_3_5_for_over", ""),
                away_ts.get("away_ou_3_5_for_under", ""),
                away_ts.get("away_ou_3_5_against_over", ""),
                away_ts.get("away_ou_3_5_against_under", ""),
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
