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

def is_valid_competition(row):
    """
    Filtra fuori:
    - calcio femminile
    - U17
    - amichevoli
    - coppe nazionali (FA Cup, Coppa Italia, ecc.)
      ma NON esclude Champions / Europa / Libertadores ecc.
    """
    lname = (row.get("league_name") or "").lower()
    rnd = (row.get("round") or "").lower()

    # Femminile
    women_tokens = ["women", "feminine", "femenina", "femminile", "fem."]
    if any(tok in lname for tok in women_tokens):
        return False

    # U17
    u17_tokens = ["u17", "u-17", "under 17"]
    if any(tok in lname for tok in u17_tokens) or any(tok in rnd for tok in u17_tokens):
        return False

    # Amichevoli
    friendly_tokens = ["friendly", "friendlies", "amic", "club friend"]
    if any(tok in lname for tok in friendly_tokens):
        return False

    # Coppe nazionali
    # Esempi: FA Cup, Coppa Italia, Copa del Rey, Taça, Pokal...
    cup_tokens = ["cup", "coppa", "copa", "taça", "pokal", "coupe", "taça", "杯"]
    # Competizioni che NON vogliamo escludere anche se contengono "cup"
    keep_if_contains = [
        "champions",         # Champions League
        "libertadores",
        "sudamericana",
        "confederations",
        "afc champions",
        "ucl", "uel", "uecl"
    ]
    if any(tok in lname for tok in cup_tokens) and not any(k in lname for k in keep_if_contains):
        return False

    return True


def form_score(form_str, window=5):
    """
    Converte la stringa di forma (es. 'WWLWD') in un punteggio.
    W=3, D=1, L=0 sugli ultimi N match.
    """
    if not form_str:
        return 0
    s = str(form_str).strip().upper()
    s = s[-window:]
    score = 0
    for ch in s:
        if ch == "W":
            score += 3
        elif ch == "D":
            score += 1
    return score


def poisson_pmf(k, lam):
    if lam is None or lam <= 0:
        return 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def prob_goals_at_least(lam, min_goals, max_goals=10):
    """
    P(TotGol >= min_goals) con Poisson(lam), tagliata a max_goals.
    """
    if lam is None or lam <= 0:
        return 0.0
    p_leq = 0.0
    for k in range(0, min_goals):
        p_leq += poisson_pmf(k, lam)
    return max(0.0, 1.0 - p_leq)


def estimate_lambdas(row):
    """
    Stima λ_home e λ_away usando:
    - prediction_goals_home/away
    - medie gol fatte/subite (home/away)
    """
    gh_pred = to_float(row.get("prediction_goals_home")) or 0.0
    ga_pred = to_float(row.get("prediction_goals_away")) or 0.0

    vals_h = []
    vals_a = []

    if gh_pred > 0:
        vals_h.append(gh_pred)
    if ga_pred > 0:
        vals_a.append(ga_pred)

    # medie gol a favore / contro
    h_gf_home = to_float(row.get("home_goals_for_avg_home"))
    h_ga_home = to_float(row.get("home_goals_against_avg_home"))
    a_gf_away = to_float(row.get("away_goals_for_avg_away"))
    a_ga_away = to_float(row.get("away_goals_against_avg_away"))

    if h_gf_home:
        vals_h.append(h_gf_home)
    if a_ga_away:
        vals_h.append(a_ga_away)   # gol concessi in trasferta dall'avversaria

    if a_gf_away:
        vals_a.append(a_gf_away)
    if h_ga_home:
        vals_a.append(h_ga_home)   # gol concessi in casa dall'avversaria

    lam_h = sum(vals_h) / len(vals_h) if vals_h else 1.2
    lam_a = sum(vals_a) / len(vals_a) if vals_a else 1.0
    return lam_h, lam_a


def generate_picks(rows):
    picks = []
    MIN_ODD = 1.20

    for r in rows:
        try:
            if not is_valid_competition(r):
                continue

            fixture_id = r["fixture_id"]
            league = r["league_name"]
            home = r["home_team"]
            away = r["away_team"]

            # Probabilità previste API-Football (in %)
            prob_home_pct = to_float(r.get("prob_home"))
            prob_draw_pct = to_float(r.get("prob_draw"))
            prob_away_pct = to_float(r.get("prob_away"))

            prob_home = (prob_home_pct or 0.0) / 100.0
            prob_draw = (prob_draw_pct or 0.0) / 100.0
            prob_away = (prob_away_pct or 0.0) / 100.0

            # λ stimati + exp_goals
            lam_h, lam_a = estimate_lambdas(r)
            lam_total = lam_h + lam_a

            # Odds principali
            oh = to_float(r.get("odd_home"))
            od = to_float(r.get("odd_draw"))
            oa = to_float(r.get("odd_away"))

            o_o15 = to_float(r.get("odd_ou_1_5_over"))
            o_o25 = to_float(r.get("odd_ou_2_5_over"))
            o_u25 = to_float(r.get("odd_ou_2_5_under"))  # non lo usiamo, ma lo leggo comunque
            o_o35 = to_float(r.get("odd_ou_3_5_over"))
            o_btts_y = to_float(r.get("odd_btts_yes"))
            o_btts_n = to_float(r.get("odd_btts_no"))

            # Over 1.5 rate dai dati under/over
            home_over15_for = to_float(r.get("home_ou_1_5_for_over")) or 0.0
            home_over15_against = to_float(r.get("home_ou_1_5_against_over")) or 0.0
            away_over15_for = to_float(r.get("away_ou_1_5_for_over")) or 0.0
            away_over15_against = to_float(r.get("away_ou_1_5_against_over")) or 0.0

            home_games = (to_float(r.get("home_fixtures_played_home")) or 0.0) + \
                         (to_float(r.get("home_fixtures_played_away")) or 0.0)
            away_games = (to_float(r.get("away_fixtures_played_home")) or 0.0) + \
                         (to_float(r.get("away_fixtures_played_away")) or 0.0)
            tot_games = home_games + away_games if home_games and away_games else max(home_games, away_games, 1.0)

            over15_count = home_over15_for + home_over15_against + away_over15_for + away_over15_against
            over15_rate = safe_div(over15_count, tot_games)  # tra 0 e >1, lo clampiamo dopo

            if over15_rate > 1:
                over15_rate = 1.0

            # Forma squadre
            home_form = form_score(r.get("home_form"))
            away_form = form_score(r.get("away_form"))

            # Medie gol
            h_gf_home = to_float(r.get("home_goals_for_avg_home")) or 0.0
            h_ga_home = to_float(r.get("home_goals_against_avg_home")) or 0.0
            a_gf_away = to_float(r.get("away_goals_for_avg_away")) or 0.0
            a_ga_away = to_float(r.get("away_goals_against_avg_away")) or 0.0

            # Poisson: probabilità sugli over
            p_over15 = prob_goals_at_least(lam_total, 2)
            p_over25 = prob_goals_at_least(lam_total, 3)
            p_over35 = prob_goals_at_least(lam_total, 4)

            # Probabilità BTTS con Poisson
            # P(BTTS) = 1 - P(H=0) - P(A=0) + P(H=0)*P(A=0)
            p_h0 = math.exp(-lam_h) if lam_h > 0 else 1.0
            p_a0 = math.exp(-lam_a) if lam_a > 0 else 1.0
            p_btts = 1.0 - p_h0 - p_a0 + p_h0 * p_a0
            p_btts = max(0.0, min(1.0, p_btts))

            # ---------- MODELLI OVER / BTTS ----------

            # O1.5 SAFE (rimane la base "sicura")
            if o_o15 and 1.20 <= o_o15 <= 1.40:
                if lam_total >= 1.9 and over15_rate >= 0.70 and p_over15 >= 0.80:
                    p_imp = 1.0 / o_o15
                    prob_model = max(p_over15, over15_rate)
                    value = max(0.0, prob_model - p_imp)
                    score = value + (prob_model - 0.80) + (over15_rate - 0.70)

                    picks.append({
                        "model": "O1_5_SAFE",
                        "category": "SAFE_PICKS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Over 1.5 goals",
                        "odd": o_o15,
                        "score": score,
                    })

            # O1.5 VALUE (quote un po' più alte, 1.35–1.60)
            if o_o15 and 1.35 <= o_o15 <= 1.60:
                if lam_total >= 2.1 and over15_rate >= 0.65 and p_over15 >= 0.75:
                    p_imp = 1.0 / o_o15
                    prob_model = max(p_over15, over15_rate)
                    value = prob_model - p_imp
                    if value > 0.05:  # almeno +5 punti di value
                        score = value + (prob_model - 0.75) + (lam_total - 2.1) * 0.1

                        picks.append({
                            "model": "O1_5_VALUE",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Over 1.5 goals",
                            "odd": o_o15,
                            "score": score,
                        })

            # O2.5 VALUE
            if o_o25 and 1.60 <= o_o25 <= 2.20:
                if lam_total >= 2.5 and p_over25 >= 0.55:
                    p_imp = 1.0 / o_o25
                    prob_model = p_over25
                    value = prob_model - p_imp
                    if value > 0.04:
                        score = value + (prob_model - 0.55) + (lam_total - 2.5) * 0.1
                        picks.append({
                            "model": "O2_5_VALUE",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Over 2.5 goals",
                            "odd": o_o25,
                            "score": score,
                        })

            # O3.5 HIGH RISK
            if o_o35 and 1.90 <= o_o35 <= 3.00:
                if lam_total >= 3.1 and p_over35 >= 0.35:
                    p_imp = 1.0 / o_o35
                    prob_model = p_over35
                    value = prob_model - p_imp
                    if value > 0.0:
                        score = value + (prob_model - 0.35) + (lam_total - 3.1) * 0.1
                        picks.append({
                            "model": "O3_5_HIGH_RISK",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Over 3.5 goals",
                            "odd": o_o35,
                            "score": score,
                        })

            # BTTS YES STRONG
            if o_btts_y and 1.50 <= o_btts_y <= 2.00:
                if p_btts >= 0.60 and lam_h >= 0.9 and lam_a >= 0.9:
                    p_imp = 1.0 / o_btts_y
                    prob_model = p_btts
                    value = prob_model - p_imp
                    if value > 0.03:
                        score = value + (prob_model - 0.60) + (lam_h - 0.9) * 0.05 + (lam_a - 0.9) * 0.05
                        picks.append({
                            "model": "BTTS_YES_STRONG",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Both teams to score YES",
                            "odd": o_btts_y,
                            "score": score,
                        })

            # BTTS YES VALUE (quote un po' più alte)
            if o_btts_y and 1.70 <= o_btts_y <= 2.20:
                if p_btts >= 0.55 and lam_total >= 2.4:
                    p_imp = 1.0 / o_btts_y
                    prob_model = p_btts
                    value = prob_model - p_imp
                    if value > 0.04:
                        score = value + (prob_model - 0.55) + (lam_total - 2.4) * 0.1
                        picks.append({
                            "model": "BTTS_YES_VALUE",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Both teams to score YES",
                            "odd": o_btts_y,
                            "score": score,
                        })

            # Eventuale BTTS NO strong (solo se proprio chiuso)
            if o_btts_n and 1.50 <= o_btts_n <= 2.20:
                if p_btts <= 0.35 and lam_total <= 2.0:
                    p_imp = 1.0 / o_btts_n
                    prob_model = 1.0 - p_btts
                    value = prob_model - p_imp
                    if value > 0.03:
                        score = value + ((1.0 - p_btts) - 0.65) + (2.0 - lam_total) * 0.1
                        picks.append({
                            "model": "BTTS_NO_STATS",
                            "category": "OVER_UNDER_TIPS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Both teams to score NO",
                            "odd": o_btts_n,
                            "score": score,
                        })

            # ---------- MODELLI 1X2 / COMBO ----------

            # Indici "forza"
            home_strength = (h_gf_home - h_ga_home)
            away_strength = (a_gf_away - a_ga_away)

            # HOME WIN STRONG basato su stats+forma
            if oh and 1.40 <= oh <= 1.90 and prob_home > 0:
                if prob_home >= 0.55 and (home_form - away_form) >= 3 and (home_strength - away_strength) >= 0.5 and lam_h > lam_a:
                    p_imp = 1.0 / oh
                    prob_model = prob_home
                    value = prob_model - p_imp
                    if value > 0.02:
                        score = value + (prob_home - 0.55) + (home_form - away_form) * 0.02 + (home_strength - away_strength) * 0.1
                        picks.append({
                            "model": "HOME_WIN_STRONG_STATS",
                            "category": "BEST_TIPS_OF_DAY",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Home wins",
                            "odd": oh,
                            "score": score,
                        })

            # AWAY WIN STRONG (versione speculare)
            if oa and 1.40 <= oa <= 2.20 and prob_away > 0:
                if prob_away >= 0.55 and (away_form - home_form) >= 3 and (away_strength - home_strength) >= 0.5 and lam_a > lam_h:
                    p_imp = 1.0 / oa
                    prob_model = prob_away
                    value = prob_model - p_imp
                    if value > 0.02:
                        score = value + (prob_away - 0.55) + (away_form - home_form) * 0.02 + (away_strength - home_strength) * 0.1
                        picks.append({
                            "model": "AWAY_WIN_STRONG_STATS",
                            "category": "BEST_TIPS_OF_DAY",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Away wins",
                            "odd": oa,
                            "score": score,
                        })

            # VALUE BET HOME (quote 1.60–2.20 con buon value)
            if oh and 1.60 <= oh <= 2.20 and prob_home > 0:
                p_imp = 1.0 / oh
                if prob_home - p_imp >= 0.07:  # almeno +7 punti percentuali
                    value = prob_home - p_imp
                    score = value + (prob_home - 0.55)
                    picks.append({
                        "model": "HOME_VALUE_BET",
                        "category": "VALUE_PICKS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Home wins",
                        "odd": oh,
                        "score": score,
                    })

            # VALUE BET AWAY
            if oa and 1.70 <= oa <= 2.40 and prob_away > 0:
                p_imp = 1.0 / oa
                if prob_away - p_imp >= 0.07:
                    value = prob_away - p_imp
                    score = value + (prob_away - 0.55)
                    picks.append({
                        "model": "AWAY_VALUE_BET",
                        "category": "VALUE_PICKS",
                        "fixture_id": fixture_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "pick": "Away wins",
                        "odd": oa,
                        "score": score,
                    })

            # Double chance 1X + Over 1.5 (stima quota)
            if prob_home > 0 and prob_draw > 0 and o_o15:
                prob1x = prob_home + prob_draw
                if prob1x >= 0.75 and p_over15 >= 0.75 and o_o15 >= MIN_ODD:
                    # quota DC 1X stimata (più bassa dell'1)
                    q1x_est = max(1.25, min(1.70, (oh or 1.50) * 0.65))
                    # combo con O1.5 leggermente più alta
                    q_combo = max(1.35, min(1.90, q1x_est * 1.10))
                    p_imp = 1.0 / q_combo
                    prob_model = min(0.99, prob1x * p_over15)
                    value = prob_model - p_imp
                    if value > 0.04:
                        score = value + (prob_model - 0.70)
                        picks.append({
                            "model": "DC1X_O1_5_COMBO",
                            "category": "SAFE_PICKS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "1X & Over 1.5 goals",
                            "odd": q_combo,
                            "score": score,
                        })

            # Double chance X2 + Over 1.5
            if prob_away > 0 and prob_draw > 0 and o_o15:
                probx2 = prob_away + prob_draw
                if probx2 >= 0.75 and p_over15 >= 0.75 and o_o15 >= MIN_ODD:
                    qx2_est = max(1.25, min(1.80, (oa or 1.70) * 0.65))
                    q_combo = max(1.35, min(2.00, qx2_est * 1.10))
                    p_imp = 1.0 / q_combo
                    prob_model = min(0.99, probx2 * p_over15)
                    value = prob_model - p_imp
                    if value > 0.04:
                        score = value + (prob_model - 0.70)
                        picks.append({
                            "model": "DCX2_O1_5_COMBO",
                            "category": "SAFE_PICKS",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "X2 & Over 1.5 goals",
                            "odd": q_combo,
                            "score": score,
                        })

            # Winner + Over 1.5 (combo "virtuale")
            if oh and o_o15 and prob_home > 0:
                if oh >= 1.35 and p_over15 >= 0.75:
                    # quota combo stimata (non reale, ma serve per ranking)
                    q_combo = max(1.50, min(2.40, oh * 1.10))
                    p_imp = 1.0 / q_combo
                    prob_model = min(0.99, prob_home * p_over15)
                    value = prob_model - p_imp
                    if value > 0.03:
                        score = value + (prob_model - 0.60)
                        picks.append({
                            "model": "HOME_WIN_O1_5_COMBO",
                            "category": "BEST_TIPS_OF_DAY",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Home wins & Over 1.5 goals",
                            "odd": q_combo,
                            "score": score,
                        })

            if oa and o_o15 and prob_away > 0:
                if oa >= 1.50 and p_over15 >= 0.75:
                    q_combo = max(1.60, min(2.60, oa * 1.10))
                    p_imp = 1.0 / q_combo
                    prob_model = min(0.99, prob_away * p_over15)
                    value = prob_model - p_imp
                    if value > 0.03:
                        score = value + (prob_model - 0.60)
                        picks.append({
                            "model": "AWAY_WIN_O1_5_COMBO",
                            "category": "BEST_TIPS_OF_DAY",
                            "fixture_id": fixture_id,
                            "league": league,
                            "home": home,
                            "away": away,
                            "pick": "Away wins & Over 1.5 goals",
                            "odd": q_combo,
                            "score": score,
                        })

        except Exception as e:
            print("# ERR PICK", e, file=sys.stderr)
            continue

    # ordiniamo per score decrescente
    picks.sort(key=lambda x: x["score"], reverse=True)
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
        "VALUE_PICKS": [],
    }

    # TOP 5 e BEST TIPS usano il ranking globale
    cats["TOP_5_TIPS"] = picks[:5]
    cats["BEST_TIPS_OF_DAY"] = picks[:15]

    # SAFE_PICKS: modelli marcati come SAFE_PICKS + quota contenuta
    safe = [p for p in picks if p["category"] == "SAFE_PICKS" and 1.20 <= (p["odd"] or 0) <= 1.65]
    cats["SAFE_PICKS"] = safe[:15]

    # OVER/UNDER TIPS
    ou = [p for p in picks if p["category"] == "OVER_UNDER_TIPS"]
    cats["OVER_UNDER_TIPS"] = ou[:25]

    # VALUE_PICKS
    val = [p for p in picks if p["category"] == "VALUE_PICKS"]
    cats["VALUE_PICKS"] = val[:20]

    # SINGLE GAME: la miglior pick in assoluto
    cats["SINGLE_GAME"] = picks[:1]

    # DAILY 2+ODDS: selezione di SAFE con prodotto ~2
    ticket = []
    prod = 1.0
    for p in safe:
        if p["odd"] and prod * p["odd"] <= 3.0:
            ticket.append(p)
            prod *= p["odd"]
        if prod >= 2.0:
            break
    cats["DAILY_2PLUS"] = ticket

    # DAILY 10+ODDS: picks a quota medio-bassa, ma da combinare
    ticket10 = []
    prod10 = 1.0
    for p in picks:
        if 1.30 <= (p["odd"] or 0) <= 1.90:
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

    BATCH_SIZE = 60  # puoi alzare/abbassare se vuoi

    try:
        total = len(rows)
        print(f"# SheetDB: invio {total} righe in batch su sheet={sheet_name}", file=sys.stderr)

        for i in range(0, total, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            params = {"sheet": sheet_name}
            payload = {"data": batch}

            print(
                f"# SheetDB: POST -> {SHEETDB_URL} sheet={sheet_name} "
                f"batch {i}–{i+len(batch)-1} (size={len(batch)})",
                file=sys.stderr,
            )

            r = requests.post(SHEETDB_URL, params=params, json=payload, timeout=30)

            print(
                f"# SheetDB: risposta sheet={sheet_name} "
                f"status={r.status_code} body={r.text[:200]}",
                file=sys.stderr,
            )

            # piccolo delay per non martellare
            time.sleep(0.2)

    except Exception as e:
        print(f"# ERRORE SheetDB append sheet={sheet_name}: {e}", file=sys.stderr)




def push_raw_and_picks_to_sheetdb(rows, categories):
    # 1) RAW: appendiamo tutte le righe grezze
    sheetdb_clear_sheet("RAW")
    sheetdb_append_rows("RAW", rows)

    # 2) Costruiamo una mappa fixture_id -> info base (data/ora/lega/paese)
    fixture_info = {}
    for r in rows:
        fid = r.get("fixture_id")
        if not fid:
            continue
        fixture_info[fid] = {
            "match_date": r.get("date", ""),
            "match_time": r.get("time", ""),
            "league": r.get("league_name", ""),
            "country": r.get("country", ""),
        }

    # 3) Prepariamo le righe per il foglio PICKS
    out = []
    run_date = today_str()

    for cat_name, plist in categories.items():
        for p in plist:
            info = fixture_info.get(p["fixture_id"], {})
            out.append({
                # quando abbiamo lanciato la pipeline
                "run_date": run_date,
                # data/ora della partita
                "match_date": info.get("match_date", ""),
                "match_time": info.get("match_time", ""),
                # info torneo
                "league": info.get("league", p.get("league", "")),
                "country": info.get("country", ""),
                # classificazione interna
                "category": cat_name,
                "model": p["model"],
                # match
                "fixture_id": p["fixture_id"],
                "home": p["home"],
                "away": p["away"],
                # pick
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

def start_results_checker_async(date_str=None):
    """
    Lancia il result checker (results_checker.py) in background,
    importando la funzione run_results_checker dal file separato.
    """
    def _worker():
        try:
            from results_checker import run_results_checker
            print(f"# Avvio results_checker per data={date_str}", file=sys.stderr)
            run_results_checker(date_str)
            print("# results_checker completato", file=sys.stderr)
        except Exception as e:
            print(f"# ERRORE in results_checker: {e}", file=sys.stderr)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return True



# ==========================
# HTTP SERVER PER RENDER
# ==========================

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query or "")

        secret_conf = os.environ.get("RUN_SECRET", "")

        # ------------------------------
        # /run -> avvia pipeline scraper
        # ------------------------------
        if parsed.path == "/run":
            if secret_conf:
                key = qs.get("key", [""])[0]
                if key != secret_conf:
                    self.send_response(403)
                    self.send_header("Content-type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(b"Forbidden\n")
                    return

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
            return

        # -----------------------------------
        # /check_results -> lancia result checker
        # -----------------------------------
        elif parsed.path == "/check_results":
            if secret_conf:
                key = qs.get("key", [""])[0]
                if key != secret_conf:
                    self.send_response(403)
                    self.send_header("Content-type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(b"Forbidden\n")
                    return

            # opzionale: ?date=YYYY-MM-DD
            date_param = qs.get("date", [None])[0]
            start_results_checker_async(date_param)

            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Results checker avviato in background")
            return

        # ------------------------------
        # qualsiasi altra path -> OK
        # ------------------------------
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK\n")

    def log_message(self, format, *args):
        # niente log HTTP rumorosi
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










