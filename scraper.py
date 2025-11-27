import os
import sys
import time
import json
import math
import http.server
import socketserver
import urllib.parse
import threading
from datetime import datetime, timezone

import requests

# ==========================
# CONFIG
# ==========================

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json",
}

SHEETDB_URL = os.environ.get("SHEETDB_URL", "https://sheetdb.io/api/v1/ou6vl5uzwgsda")

TZ = timezone.utc

RUN_MARKER_PATH = "/tmp/last_run_marker.txt"
TEAM_STATS_CACHE = {}


# ==========================
# UTILS
# ==========================

def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")


def api_get(path, params=None, timeout=20):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data.get("response", [])


def to_float(x):
    """
    Converte stringhe tipo '1.35', '45%', '1,8', ' 2.10 ' in float.
    Ritorna None se non convertibile.
    """
    if x is None:
        return None
    try:
        s = str(x).strip()
        if not s:
            return None
        s = s.replace("%", "")       # toglie il simbolo %
        s = s.replace(",", ".")      # virgola -> punto
        return float(s)
    except Exception:
        return None



def implied_prob(odd):
    if odd is None or odd <= 1:
        return None
    return 100.0 / odd


def safe_div(a, b):
    try:
        return a / b if b not in (0, None) and a is not None else 0.0
    except ZeroDivisionError:
        return 0.0


# ==========================
# SCRAPING FIXTURES + DATA
# ==========================

def get_fixtures_for_date(target_date):
    r = requests.get(
        f"{BASE_URL}/fixtures",
        headers=HEADERS,
        params={"date": target_date, "timezone": "Europe/Dublin"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    resp = data.get("response", [])
    print(f"# Fixtures trovati per {target_date}: {len(resp)}", file=sys.stderr)
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
    data = api_get("/odds", {"fixture": fixture_id})
    if not data:
        return {}

    bookmakers = data[0].get("bookmakers", [])
    if not bookmakers:
        return {}

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

    btts = extract_btts(bets_main)
    if not btts["odd_btts_yes"] and not btts["odd_btts_no"]:
        for b in bookmakers:
            alt = extract_btts(b.get("bets", []))
            if alt["odd_btts_yes"] or alt["odd_btts_no"]:
                btts = alt
                break

    res.update(btts)
    return res


def get_statistics_for_fixture(fixture_id, home_team_id, away_team_id):
    stats_list = api_get("/fixtures/statistics", {"fixture": fixture_id}, timeout=30)
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

    def fill(team_id, side_prefix):
        stat_map = per_team.get(team_id) or {}
        corners = stat_map.get("Corner Kicks")
        if corners is None:
            corners = stat_map.get("Corners")
        yellow = stat_map.get("Yellow Cards")
        red = stat_map.get("Red Cards")

        result[f"corners_{side_prefix}"] = corners or ""
        result[f"yellow_cards_{side_prefix}"] = yellow or ""
        result[f"red_cards_{side_prefix}"] = red or ""

    if home_team_id:
        fill(home_team_id, "home")
    if away_team_id:
        fill(away_team_id, "away")
    return result


def get_team_statistics_raw(league_id, season, team_id):
    if not league_id or not season or not team_id:
        return {}

    key = (league_id, season, team_id)
    if key in TEAM_STATS_CACHE:
        return TEAM_STATS_CACHE[key]

    url = f"{BASE_URL}/teams/statistics"
    params = {"league": league_id, "season": season, "team": team_id}
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        ts = data.get("response") or {}
    except Exception as e:
        print(f"# ERRORE /teams/statistics {key}: {e}", file=sys.stderr)
        ts = {}
    TEAM_STATS_CACHE[key] = ts
    time.sleep(0.2)
    return ts


def flatten_team_stats(ts, prefix):
    out = {}

    if not isinstance(ts, dict) or not ts:
        base_keys = [
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
        for k in base_keys:
            out[prefix + k] = ""
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

    for line in [0.5, 1.5, 2.5, 3.5]:
        label = str(line).replace(".", "_")
        for part in ["for_over", "for_under", "against_over", "against_under"]:
            key = f"{prefix}ou_{label}_{part}"
            if key not in out:
                out[key] = ""
    return out


def build_rows_for_date(target_date):
    fixtures = get_fixtures_for_date(target_date)
    rows = []

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

            pred = get_prediction_for_fixture(fixture_id)
            odds = get_odds_for_fixture(fixture_id)
            stats = get_statistics_for_fixture(fixture_id, home_team_id, away_team_id)

            home_ts_raw = get_team_statistics_raw(league_id, season, home_team_id)
            away_ts_raw = get_team_statistics_raw(league_id, season, away_team_id)
            home_ts = flatten_team_stats(home_ts_raw, "home_")
            away_ts = flatten_team_stats(away_ts_raw, "away_")

            row = {
                "fixture_id": fixture_id,
                "date": d,
                "time": t,
                "league_id": league.get("id", ""),
                "league_name": league.get("name", ""),
                "country": league.get("country", ""),
                "season": league.get("season", ""),
                "round": league.get("round", ""),
                "status_short": status.get("short", ""),
                "status_long": status.get("long", ""),
                "venue_name": venue.get("name", ""),
                "venue_city": venue.get("city", ""),
                "referee_name": referee_name,
                "home_team": home_team.get("name", ""),
                "away_team": away_team.get("name", ""),
                "corners_home": stats.get("corners_home", ""),
                "corners_away": stats.get("corners_away", ""),
                "yellow_cards_home": stats.get("yellow_cards_home", ""),
                "yellow_cards_away": stats.get("yellow_cards_away", ""),
                "red_cards_home": stats.get("red_cards_home", ""),
                "red_cards_away": stats.get("red_cards_away", ""),
                "prediction_winner_name": pred.get("pred_winner_name", ""),
                "prediction_winner_comment": pred.get("pred_winner_comment", ""),
                "prediction_win_or_draw": pred.get("win_or_draw", ""),
                "prediction_under_over": pred.get("under_over", ""),
                "prediction_advice": pred.get("advice", ""),
                "prediction_goals_home": pred.get("goals_home", ""),
                "prediction_goals_away": pred.get("goals_away", ""),
                "prob_home": pred.get("prob_home", ""),
                "prob_draw": pred.get("prob_draw", ""),
                "prob_away": pred.get("prob_away", ""),
                "bookmaker": odds.get("bookmaker", ""),
                "odd_home": odds.get("odd_home", ""),
                "odd_draw": odds.get("odd_draw", ""),
                "odd_away": odds.get("odd_away", ""),
                "odd_ou_1_5_over": odds.get("odd_ou_1_5_over", ""),
                "odd_ou_2_5_over": odds.get("odd_ou_2_5_over", ""),
                "odd_ou_2_5_under": odds.get("odd_ou_2_5_under", ""),
                "odd_ou_3_5_over": odds.get("odd_ou_3_5_over", ""),
                "odd_btts_yes": odds.get("odd_btts_yes", ""),
                "odd_btts_no": odds.get("odd_btts_no", ""),
            }
            row.update(home_ts)
            row.update(away_ts)

            rows.append(row)

        except Exception as e:
            print(f"# ERRORE fixture {fixture_id}: {e}", file=sys.stderr)
            continue

    return rows


# ==========================
# MODELLI & PICKS
# ==========================

def generate_picks(rows):
    picks = []

    MIN_ODD = 1.20
    VALUE_THRESHOLD_PCT = 1.5  # soglia value in punti percentuali (es. 1.5 = 1.5%)

    def compute_value(model_prob_pct, odd):
        """
        model_prob_pct: probabilità stimata dal modello (0-100)
        odd: quota decimale
        Ritorna (value_pct, value_float) dove:
          - value_pct = model_prob_pct - implied_prob_pct
          - value_float = value_pct / 100
        """
        if model_prob_pct is None:
            return None, None
        ip = implied_prob(odd)
        if ip is None:
            return None, None
        value_pct = model_prob_pct - ip
        return value_pct, value_pct / 100.0

    for r in rows:
        try:
            fixture_id = r["fixture_id"]
            league = r["league_name"]
            home = r["home_team"]
            away = r["away_team"]

            # Probabilità API (%)
            prob_home = to_float(r["prob_home"])
            prob_draw = to_float(r["prob_draw"])
            prob_away = to_float(r["prob_away"])

            # Gol previsti da prediction
            gh = to_float(r["prediction_goals_home"]) or 0.0
            ga = to_float(r["prediction_goals_away"]) or 0.0
            exp_goals_pred = gh + ga

            # Odds mercato
            oh = to_float(r["odd_home"])
            od = to_float(r["odd_draw"])
            oa = to_float(r["odd_away"])
            o_o15 = to_float(r["odd_ou_1_5_over"])
            o_o25 = to_float(r["odd_ou_2_5_over"])
            o_u25 = to_float(r["odd_ou_2_5_under"])
            o_o35 = to_float(r["odd_ou_3_5_over"])
            o_btts_y = to_float(r["odd_btts_yes"])
            o_btts_n = to_float(r["odd_btts_no"])

            # Under/over storici squadre
            home_ou15_for = to_float(r.get("home_ou_1_5_for_over")) or 0.0
            home_ou15_against = to_float(r.get("home_ou_1_5_against_over")) or 0.0
            away_ou15_for = to_float(r.get("away_ou_1_5_for_over")) or 0.0
            away_ou15_against = to_float(r.get("away_ou_1_5_against_over")) or 0.0

            home_ou25_for = to_float(r.get("home_ou_2_5_for_over")) or 0.0
            home_ou25_against = to_float(r.get("home_ou_2_5_against_over")) or 0.0
            away_ou25_for = to_float(r.get("away_ou_2_5_for_over")) or 0.0
            away_ou25_against = to_float(r.get("away_ou_2_5_against_over")) or 0.0

            # Partite giocate
            home_games = (to_float(r["home_fixtures_played_home"]) or 0) + (to_float(r["home_fixtures_played_away"]) or 0)
            away_games = (to_float(r["away_fixtures_played_home"]) or 0) + (to_float(r["away_fixtures_played_away"]) or 0)
            tot_games = home_games + away_games if home_games and away_games else max(home_games, away_games, 1)

            # Rate over 1.5 / 2.5
            over15_cnt = home_ou15_for + home_ou15_against + away_ou15_for + away_ou15_against
            over25_cnt = home_ou25_for + home_ou25_against + away_ou25_for + away_ou25_against
            over15_rate = safe_div(over15_cnt, tot_games)
            over25_rate = safe_div(over25_cnt, tot_games)

            # Medie gol fatti/subiti
            def avg2(a, b):
                xs = [v for v in [a, b] if v is not None]
                return sum(xs) / len(xs) if xs else None

            h_for_h = to_float(r.get("home_goals_for_avg_home"))
            h_for_a = to_float(r.get("home_goals_for_avg_away"))
            a_for_h = to_float(r.get("away_goals_for_avg_home"))
            a_for_a = to_float(r.get("away_goals_for_avg_away"))

            h_ag_h = to_float(r.get("home_goals_against_avg_home"))
            h_ag_a = to_float(r.get("home_goals_against_avg_away"))
            a_ag_h = to_float(r.get("away_goals_against_avg_home"))
            a_ag_a = to_float(r.get("away_goals_against_avg_away"))

            home_for_avg = avg2(h_for_h, h_for_a) or 0.0
            away_for_avg = avg2(a_for_h, a_for_a) or 0.0
            home_against_avg = avg2(h_ag_h, h_ag_a) or 0.0
            away_against_avg = avg2(a_ag_h, a_ag_a) or 0.0

            # clean sheets / failed to score
            home_cs_home = to_float(r.get("home_clean_sheet_home")) or 0.0
            home_cs_away = to_float(r.get("home_clean_sheet_away")) or 0.0
            away_cs_home = to_float(r.get("away_clean_sheet_home")) or 0.0
            away_cs_away = to_float(r.get("away_clean_sheet_away")) or 0.0

            home_fts_home = to_float(r.get("home_failed_to_score_home")) or 0.0
            home_fts_away = to_float(r.get("home_failed_to_score_away")) or 0.0
            away_fts_home = to_float(r.get("away_failed_to_score_home")) or 0.0
            away_fts_away = to_float(r.get("away_failed_to_score_away")) or 0.0

            # streaks
            home_streak_wins = to_float(r.get("home_streak_wins")) or 0.0
            home_streak_loses = to_float(r.get("home_streak_loses")) or 0.0
            away_streak_wins = to_float(r.get("away_streak_wins")) or 0.0
            away_streak_loses = to_float(r.get("away_streak_loses")) or 0.0

            # Exp_goals generico (prediction + medie)
            exp_goals = exp_goals_pred
            if exp_goals <= 0:
                exp_goals = (home_for_avg + away_for_avg + home_against_avg + away_against_avg) / 2.0

            # ==========================
            # 1) MODELLI ESISTENTI
            # ==========================

            # O1.5_SAFE
            if o_o15 and MIN_ODD <= o_o15 <= 1.60 and exp_goals >= 1.9 and over15_rate >= 0.6:
                p_imp = implied_prob(o_o15) or 0
                prob_model = min(97, over15_rate * 100 + max(0, exp_goals - 2.0) * 15)
                value_pct, value = compute_value(prob_model, o_o15)
                score = (value or 0) + (over15_rate - 0.6) + max(0, exp_goals - 1.9)
                picks.append({
                    "model": "O1_5_SAFE",
                    "category": "OVER_UNDER_TIPS",
                    "fixture_id": fixture_id,
                    "league": league,
                    "home": home,
                    "away": away,
                    "pick": "Over 1.5 goals",
                    "odd": o_o15,
                    "score": score,
                })

            # BTTS_YES_STRONG
            if o_btts_y and 1.30 <= o_btts_y <= 2.10 and gh >= 0.8 and ga >= 0.8:
                prob_model = min(96, (gh + ga) * 30 + (home_for_avg + away_for_avg) * 10)
                value_pct, value = compute_value(prob_model, o_btts_y)
                score = (value or 0) + max(0, gh - 0.8) + max(0, ga - 0.8)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    picks.append({
                        "model": "BTTS_YES_STRONG",
                        "category": "OVER_UNDER_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Both teams score YES",
                        "odd": o_btts_y,
                        "score": score,
                    })

            # BTTS_NO_STRONG
            if o_btts_n and 1.30 <= o_btts_n <= 2.10 and exp_goals <= 2.3:
                prob_model = max(40, 100 - exp_goals * 25 - (home_for_avg + away_for_avg) * 10)
                value_pct, value = compute_value(prob_model, o_btts_n)
                score = (value or 0) + max(0, 2.3 - exp_goals)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    picks.append({
                        "model": "BTTS_NO_STRONG",
                        "category": "OVER_UNDER_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Both teams score NO",
                        "odd": o_btts_n,
                        "score": score,
                    })

            # HOME_WIN_STRONG
            if oh and 1.30 <= oh <= 2.10 and prob_home:
                value_pct, value = compute_value(prob_home, oh)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT and prob_home >= 55:
                    score = (value or 0) + (prob_home - 55) / 100.0 + max(0, home_for_avg - 1.2)
                    picks.append({
                        "model": "HOME_WIN_STRONG",
                        "category": "BEST_TIPS_OF_DAY",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Home wins",
                        "odd": oh,
                        "score": score,
                    })

            # DC1X_SAFE
            if oh and MIN_ODD <= oh <= 1.90 and prob_home and prob_draw:
                prob1x = prob_home + prob_draw
                if prob1x >= 70:
                    value_pct, value = compute_value(prob1x, oh)
                    q1x = max(1.25, min(1.65, oh * 0.65))
                    score = (value or 0) + (prob1x - 70) / 100.0
                    picks.append({
                        "model": "DC1X_SAFE",
                        "category": "SAFE_PICKS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "1X",
                        "odd": q1x,
                        "score": score,
                    })

            # DCX2_SAFE
            if oa and MIN_ODD <= oa <= 2.00 and prob_away and prob_draw:
                probx2 = prob_away + prob_draw
                if probx2 >= 70:
                    value_pct, value = compute_value(probx2, oa)
                    qx2 = max(1.25, min(1.75, oa * 0.65))
                    score = (value or 0) + (probx2 - 70) / 100.0
                    picks.append({
                        "model": "DCX2_SAFE",
                        "category": "SAFE_PICKS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "X2",
                        "odd": qx2,
                        "score": score,
                    })

            # O2.5_STRONG
            if o_o25 and 1.40 <= o_o25 <= 2.40 and exp_goals >= 2.5:
                prob_model = min(96, exp_goals * 28 + (over25_rate * 30))
                value_pct, value = compute_value(prob_model, o_o25)
                score = (value or 0) + max(0, exp_goals - 2.5) + max(0, over25_rate - 0.5)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    picks.append({
                        "model": "O2_5_STRONG",
                        "category": "OVER_UNDER_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Over 2.5 goals",
                        "odd": o_o25,
                        "score": score,
                    })

            # O3.5_STRONG
            if o_o35 and 1.60 <= o_o35 <= 3.50 and exp_goals >= 3.0:
                prob_model = min(96, exp_goals * 25 + over25_rate * 20)
                value_pct, value = compute_value(prob_model, o_o35)
                score = (value or 0) + max(0, exp_goals - 3.0)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    picks.append({
                        "model": "O3_5_STRONG",
                        "category": "OVER_UNDER_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Over 3.5 goals",
                        "odd": o_o35,
                        "score": score,
                    })

            # ==========================
            # 2) VALUE BET 1X2 / GOALS
            # ==========================

            # VALUE_HOME
            if oh and 1.40 <= oh <= 3.50 and prob_home:
                value_pct, value = compute_value(prob_home, oh)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    score = (value or 0) + (prob_home - 50) / 100.0
                    picks.append({
                        "model": "VALUE_HOME",
                        "category": "VALUE_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Home wins (value)",
                        "odd": oh,
                        "score": score,
                    })

            # VALUE_AWAY
            if oa and 1.40 <= oa <= 3.50 and prob_away:
                value_pct, value = compute_value(prob_away, oa)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    score = (value or 0) + (prob_away - 50) / 100.0
                    picks.append({
                        "model": "VALUE_AWAY",
                        "category": "VALUE_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Away wins (value)",
                        "odd": oa,
                        "score": score,
                    })

            # VALUE_O2_5
            if o_o25 and 1.40 <= o_o25 <= 2.60:
                prob_model = min(96, exp_goals * 28 + over25_rate * 40)
                value_pct, value = compute_value(prob_model, o_o25)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    score = (value or 0) + max(0, exp_goals - 2.4) + max(0, over25_rate - 0.5)
                    picks.append({
                        "model": "VALUE_O2_5",
                        "category": "VALUE_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Over 2.5 goals (value)",
                        "odd": o_o25,
                        "score": score,
                    })

            # VALUE_BTTS_YES
            if o_btts_y and 1.40 <= o_btts_y <= 2.50:
                prob_model = min(
                    96,
                    (home_for_avg + away_for_avg) * 20
                    + (home_against_avg + away_against_avg) * 15
                    + exp_goals * 10,
                )
                value_pct, value = compute_value(prob_model, o_btts_y)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    score = (value or 0) + max(0, exp_goals - 2.4)
                    picks.append({
                        "model": "VALUE_BTTS_YES",
                        "category": "VALUE_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "BTTS YES (value)",
                        "odd": o_btts_y,
                        "score": score,
                    })

            # VALUE_BTTS_NO
            if o_btts_n and 1.40 <= o_btts_n <= 2.50:
                prob_model = max(
                    35,
                    100
                    - (home_for_avg + away_for_avg) * 20
                    - (home_against_avg + away_against_avg) * 15
                    - exp_goals * 10,
                )
                value_pct, value = compute_value(prob_model, o_btts_n)
                if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                    score = (value or 0) + max(0, 2.3 - exp_goals)
                    picks.append({
                        "model": "VALUE_BTTS_NO",
                        "category": "VALUE_TIPS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "BTTS NO (value)",
                        "odd": o_btts_n,
                        "score": score,
                    })

            # ==========================
            # 3) MIXED PICKS (combo)
            # ==========================

            # MIX_1X_O1_5
            if o_o15 and oh and prob_home and prob_draw:
                prob1x = prob_home + prob_draw
                if prob1x >= 70 and exp_goals >= 1.9:
                    # quota combo stimata
                    q1x = max(1.25, min(1.65, oh * 0.65))
                    q_mix = max(1.40, min(2.20, q1x * o_o15 * 0.7))
                    prob_model = min(96, prob1x + (over15_rate * 20))
                    value_pct, value = compute_value(prob_model, q_mix)
                    if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                        score = (value or 0) + (prob1x - 70) / 100.0 + max(0, exp_goals - 2.0)
                        picks.append({
                            "model": "MIX_1X_O1_5",
                            "category": "MIXED_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "1X + Over 1.5 goals",
                            "odd": q_mix,
                            "score": score,
                        })

            # MIX_X2_O1_5
            if o_o15 and oa and prob_away and prob_draw:
                probx2 = prob_away + prob_draw
                if probx2 >= 70 and exp_goals >= 1.9:
                    qx2 = max(1.25, min(1.75, oa * 0.65))
                    q_mix = max(1.40, min(2.30, qx2 * o_o15 * 0.7))
                    prob_model = min(96, probx2 + (over15_rate * 20))
                    value_pct, value = compute_value(prob_model, q_mix)
                    if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                        score = (value or 0) + (probx2 - 70) / 100.0 + max(0, exp_goals - 2.0)
                        picks.append({
                            "model": "MIX_X2_O1_5",
                            "category": "MIXED_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "X2 + Over 1.5 goals",
                            "odd": q_mix,
                            "score": score,
                        })

            # ==========================
            # 4) LOW-SCORING / UNDER
            # ==========================

            # DEFENSIVE_UNDER_2_5
            if o_u25 and 1.30 <= o_u25 <= 2.20:
                cond_attack = (home_for_avg <= 1.4 and away_for_avg <= 1.4)
                cond_def = (home_against_avg <= 1.0 and away_against_avg <= 1.0)
                if cond_attack and cond_def and exp_goals <= 2.4:
                    prob_model = max(40, 100 - exp_goals * 25)
                    value_pct, value = compute_value(prob_model, o_u25)
                    if value_pct is not None and value_pct >= VALUE_THRESHOLD_PCT:
                        score = (value or 0) + max(0, 2.4 - exp_goals)
                        picks.append({
                            "model": "DEFENSIVE_UNDER_2_5",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Under 2.5 goals",
                            "odd": o_u25,
                            "score": score,
                        })

        except Exception as e:
            print("# ERR PICK", e, file=sys.stderr)
            continue

    # Ordina tutte le picks per score decrescente
    picks.sort(key=lambda x: x["score"], reverse=True)

    # Limite massimo di picks per non esplodere
    MAX_PICKS = 120
    if len(picks) > MAX_PICKS:
        picks = picks[:MAX_PICKS]

    print(f"# TOT picks candidate: {len(picks)}", file=sys.stderr)
    return picks



def build_categories(picks):
    cats = {
        "SAFE_PICKS": [],
        "SINGLE_GAME": [],
        "TOP_5_TIPS": [],
        "BEST_TIPS_OF_DAY": [],
        "OVER_UNDER_TIPS": [],
        "DAILY_2PLUS": [],
        "DAILY_10PLUS": [],
    }

    cats["TOP_5_TIPS"] = picks[:5]
    cats["BEST_TIPS_OF_DAY"] = picks[:15]

    safe = [p for p in picks if p["category"] == "SAFE_PICKS" and 1.20 <= (p["odd"] or 0) <= 1.55]
    cats["SAFE_PICKS"] = safe[:10]

    ou = [p for p in picks if p["model"].startswith("O") or p["model"].startswith("U") or "BTTS" in p["model"]]
    cats["OVER_UNDER_TIPS"] = ou[:15]

    cats["SINGLE_GAME"] = picks[:1]

    ticket = []
    prod = 1.0
    for p in safe:
        if p["odd"] and prod * p["odd"] <= 3.0:
            ticket.append(p)
            prod *= p["odd"]
        if prod >= 2.0:
            break
    cats["DAILY_2PLUS"] = ticket

    ticket10 = []
    prod10 = 1.0
    for p in picks:
        if 1.30 <= (p["odd"] or 0) <= 1.80:
            ticket10.append(p)
            prod10 *= p["odd"]
        if prod10 >= 10.0:
            break
    cats["DAILY_10PLUS"] = ticket10

    return cats


# ==========================
# SHEETDB (DEBUG)
# ==========================

def sheetdb_clear_sheet(sheet_name):
    """
    Per ora NON cancelliamo niente, stampiamo solo nei log.
    Così non facciamo DELETE strane su SheetDB.
    """
    print(f"# SheetDB: clear DISABLED for sheet={sheet_name}", file=sys.stderr)


def sheetdb_append_rows(sheet_name, rows):
    if not rows:
        print(f"# SheetDB: nessuna riga da inviare per sheet={sheet_name}", file=sys.stderr)
        return

    try:
        # Usiamo l'endpoint base + parametro sheet
        params = {"sheet": sheet_name}
        payload = {"data": rows}

        print(f"# SheetDB: POST -> {SHEETDB_URL} sheet={sheet_name} rows={len(rows)}", file=sys.stderr)
        r = requests.post(SHEETDB_URL, params=params, json=payload, timeout=30)

        print(
            f"# SheetDB: risposta sheet={sheet_name} "
            f"status={r.status_code} body={r.text[:300]}",
            file=sys.stderr,
        )
    except Exception as e:
        print(f"# ERRORE SheetDB append sheet={sheet_name}: {e}", file=sys.stderr)



def push_raw_and_picks_to_sheetdb(rows, categories):
    sheetdb_clear_sheet("RAW")
    sheetdb_append_rows("RAW", rows)

    out = []
    for cat_name, plist in categories.items():
        for p in plist:
            out.append({
                "date": today_str(),
                "category": cat_name,
                "model": p["model"],
                "fixture_id": p["fixture_id"],
                "league": p["league"],
                "home": p["home"],
                "away": p["away"],
                "pick": p["pick"],
                "odd": p["odd"],
                "score": round(p["score"], 3),
            })

    sheetdb_clear_sheet("PICKS")
    sheetdb_append_rows("PICKS", out)


# ==========================
# RUN MARKER
# ==========================

def already_ran_for_today():
    try:
        with open(RUN_MARKER_PATH, "r") as f:
            return f.read().strip() == today_str()
    except:
        return False


def set_run_marker():
    try:
        with open(RUN_MARKER_PATH, "w") as f:
            f.write(today_str())
    except:
        pass


def run_pipeline():
    target = today_str()
    print(f"# PIPELINE START {target}", file=sys.stderr)
    rows = build_rows_for_date(target)
    print(f"# Rows raccolte: {len(rows)}", file=sys.stderr)
    picks = generate_picks(rows)
    print(f"# Picks generate: {len(picks)}", file=sys.stderr)
    cats = build_categories(picks)
    push_raw_and_picks_to_sheetdb(rows, cats)
    print("# PIPELINE END", file=sys.stderr)
    return rows, picks, cats


# ==========================
# PIPELINE ASINCRONA
# ==========================

pipeline_running = False
pipeline_lock = threading.Lock()

def start_pipeline_async():
    global pipeline_running

    if already_ran_for_today():
        return False

    with pipeline_lock:
        if pipeline_running:
            return False
        pipeline_running = True

    def _worker():
        global pipeline_running
        try:
            run_pipeline()
            set_run_marker()
        finally:
            with pipeline_lock:
                pipeline_running = False

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return True


# ==========================
# HTTP SERVER PER RENDER
# ==========================

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/run":
            started = start_pipeline_async()
            if started:
                text = "Pipeline avviata in background"
            else:
                if already_ran_for_today():
                    text = "Già eseguito oggi"
                else:
                    text = "Pipeline già in esecuzione"

            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(text.encode("utf-8"))
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK\n")

    def log_message(self, format, *args):
        pass


class ReuseTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def run_http_server():
    port = int(os.environ.get("PORT", "10000"))
    with ReuseTCPServer(("", port), Handler) as httpd:
        print(f"# HTTP server running on port {port}", file=sys.stderr)
        httpd.serve_forever()


if __name__ == "__main__":
    run_http_server()




