import os
import sys
import time
from datetime import datetime, timedelta, timezone

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

TZ = timezone.utc

# Stessa env del scraper
SHEETDB_URL = os.environ.get("SHEETDB_URL", "https://sheetdb.io/api/v1/ou6vl5uzwgsda")


# ==========================
# UTILS
# ==========================

def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")


def yesterday_str():
    return (datetime.now(TZ) - timedelta(days=1)).strftime("%Y-%m-%d")


def api_get(path, params=None, timeout=20):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data.get("response", [])


def to_float(x):
    if x is None:
        return None
    try:
        s = str(x).strip()
        if not s:
            return None
        s = s.replace("%", "")
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None


# ==========================
# SHEETDB
# ==========================

def sheetdb_get_picks_for_date(date_str):
    """
    Legge le righe da sheet=PICKS filtrando per date=YYYY-MM-DD
    """
    url = f"{SHEETDB_URL}/search"
    params = {
        "sheet": "PICKS",
        "date": date_str,
    }
    print(f"# SheetDB: GET picks date={date_str}", file=sys.stderr)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"# SheetDB: trovate {len(data)} righe in PICKS per {date_str}", file=sys.stderr)
    return data


def sheetdb_append_rows(sheet_name, rows):
    if not rows:
        print(f"# SheetDB: nessuna riga da inviare per sheet={sheet_name}", file=sys.stderr)
        return

    try:
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


# ==========================
# FIXTURE RESULTS
# ==========================

FINAL_STATUS = {"FT", "AET", "PEN", "AWD", "WO"}


def get_fixtures_results_by_ids(fixture_ids):
    """
    Ritorna un dict fixture_id -> {
        "goals_home": int,
        "goals_away": int,
        "status_short": str,
        "status_long": str
    }
    usando /fixtures?ids=
    """
    results = {}
    ids_list = list({str(fid) for fid in fixture_ids if fid})

    if not ids_list:
        return results

    print(f"# Fetch risultati per {len(ids_list)} fixtures", file=sys.stderr)

    # API docs: max 20 ids per chiamata
    CHUNK = 20
    for i in range(0, len(ids_list), CHUNK):
        chunk = ids_list[i:i + CHUNK]
        ids_param = "-".join(chunk)
        params = {"ids": ids_param, "timezone": "Europe/Dublin"}

        try:
            resp = api_get("/fixtures", params=params, timeout=30)
            print(f"# /fixtures?ids=... -> {len(resp)} risultati", file=sys.stderr)

            for item in resp:
                fx = item.get("fixture", {}) or {}
                goals = item.get("goals", {}) or {}
                fid = fx.get("id")
                if fid is None:
                    continue

                status = fx.get("status", {}) or {}
                status_short = status.get("short", "")
                status_long = status.get("long", "")

                gh = goals.get("home", 0)
                ga = goals.get("away", 0)

                results[str(fid)] = {
                    "goals_home": gh,
                    "goals_away": ga,
                    "status_short": status_short,
                    "status_long": status_long,
                }

        except Exception as e:
            print(f"# ERRORE /fixtures?ids= chunk {chunk}: {e}", file=sys.stderr)

        time.sleep(0.2)  # piccola pausa, tanto sono poche chiamate

    return results


# ==========================
# EVALUATION LOGIC
# ==========================

def evaluate_pick(pick_row, match_info):
    """
    Ritorna dict con:
    - result: WIN / LOSE / PENDING / UNKNOWN
    - goals_home, goals_away, final_score, status_short
    """
    if not match_info:
        return {
            "result": "PENDING",
            "goals_home": "",
            "goals_away": "",
            "final_score": "",
            "status_short": "",
        }

    status_short = (match_info.get("status_short") or "").upper()
    gh = match_info.get("goals_home", 0)
    ga = match_info.get("goals_away", 0)
    final_score = f"{gh}-{ga}"

    if status_short not in FINAL_STATUS:
        return {
            "result": "PENDING",
            "goals_home": gh,
            "goals_away": ga,
            "final_score": final_score,
            "status_short": status_short,
        }

    pick_text = (pick_row.get("pick") or "").lower()
    model = (pick_row.get("model") or "").upper()

    # Regole basate sul testo del pick (robuste)
    res = None

    # Over/Under generici
    if "over 1.5" in pick_text:
        res = "WIN" if (gh + ga) >= 2 else "LOSE"
    elif "over 2.5" in pick_text:
        res = "WIN" if (gh + ga) >= 3 else "LOSE"
    elif "over 3.5" in pick_text:
        res = "WIN" if (gh + ga) >= 4 else "LOSE"
    elif "under 2.5" in pick_text:
        res = "WIN" if (gh + ga) <= 2 else "LOSE"

    # BTTS
    elif "both teams score yes" in pick_text or "btts yes" in pick_text:
        res = "WIN" if (gh >= 1 and ga >= 1) else "LOSE"
    elif "both teams score no" in pick_text or "btts no" in pick_text:
        res = "WIN" if (gh == 0 or ga == 0) else "LOSE"

    # Esiti 1X, X2, 1, 2
    elif pick_text.strip() == "home wins" or model == "HOME_WIN_STRONG":
        res = "WIN" if gh > ga else "LOSE"
    elif pick_text.strip() == "1x" or model == "DC1X_SAFE":
        res = "WIN" if gh >= ga else "LOSE"
    elif pick_text.strip() == "x2" or model == "DCX2_SAFE":
        res = "WIN" if ga >= gh else "LOSE"

    # Se non abbiamo capito il tipo di pick
    if res is None:
        res = "UNKNOWN"

    return {
        "result": res,
        "goals_home": gh,
        "goals_away": ga,
        "final_score": final_score,
        "status_short": status_short,
    }


# ==========================
# MAIN PIPELINE
# ==========================

def run_results_checker(target_date=None):
    if not target_date:
        target_date = yesterday_str()

    print(f"# RESULTS CHECKER START per picks_date={target_date}", file=sys.stderr)

    # 1) Leggi picks da SheetDB
    picks = sheetdb_get_picks_for_date(target_date)
    if not picks:
        print("# Nessun pick trovato, esco.", file=sys.stderr)
        return

    # 2) Colleziona fixture_ids
    fixture_ids = [p.get("fixture_id") for p in picks if p.get("fixture_id")]
    results_map = get_fixtures_results_by_ids(fixture_ids)

    # 3) Valuta ogni pick
    rows_out = []
    run_date = today_str()

    for p in picks:
        fixture_id = str(p.get("fixture_id", "")).strip()
        league = p.get("league", "")
        home = p.get("home", "")
        away = p.get("away", "")
        model = p.get("model", "")
        category = p.get("category", "")
        pick_txt = p.get("pick", "")
        odd = p.get("odd", "")
        score_model = p.get("score", "")

        match_info = results_map.get(fixture_id) or {}
        eval_res = evaluate_pick(p, match_info)

        rows_out.append({
            "run_date": run_date,
            "picks_date": target_date,
            "fixture_id": fixture_id,
            "league": league,
            "home": home,
            "away": away,
            "model": model,
            "category": category,
            "pick": pick_txt,
            "odd": odd,
            "score_model": score_model,
            "goals_home": eval_res["goals_home"],
            "goals_away": eval_res["goals_away"],
            "final_score": eval_res["final_score"],
            "status_short": eval_res["status_short"],
            "result": eval_res["result"],
        })

    # 4) Scrivi su SheetDB (sheet RESULTS)
    sheetdb_append_rows("RESULTS", rows_out)

    print(f"# RESULTS CHECKER END, righe scritte: {len(rows_out)}", file=sys.stderr)


# ==========================
# ENTRYPOINT
# ==========================

if __name__ == "__main__":
    # Uso: python results_checker.py           -> controlla ieri
    #      python results_checker.py 2025-11-26 -> controlla quella data
    if len(sys.argv) >= 2:
        date_arg = sys.argv[1]
    else:
        date_arg = None

    run_results_checker(date_arg)
