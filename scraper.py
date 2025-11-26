import requests
import csv
from datetime import datetime
import time

# ===================== CONFIG =====================
API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key": API_KEY
}

# Bookmaker che ci interessano (più stabili e con odds alte)
BOOKMAKERS = {
    "Bet365": 8,
    "Pinnacle": 2,
    "1xBet": 12
}

TODAY = datetime.now().strftime("%Y-%-m-%d")  # es. 2025-11-26
CSV_FILE = f"europe_fixtures_full_{TODAY}.csv"

# Paesi europei (tutti i principali + "Europe" per le coppe)
EUROPEAN_COUNTRIES = {
    "Europe", "England", "Italy", "Spain", "Germany", "France", "Portugal", "Netherlands",
    "Belgium", "Scotland", "Turkey", "Russia", "Ukraine", "Greece", "Austria", "Switzerland",
    "Croatia", "Czech Republic", "Denmark", "Sweden", "Norway", "Poland", "Romania", "Serbia"
}

# =================================================

def get_fixtures_today():
    print(f"Recupero tutte le partite di oggi ({TODAY})...")
    url = f"{BASE_URL}/fixtures"
    params = {"date": TODAY, "timezone": "Europe/Rome"}
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code != 200:
        print(f"Errore fixtures: {r.status_code} {r.text}")
        return []
    data = r.json().get("response", [])
    european = [f for f in data if f["league"]["country"] in EUROPEAN_COUNTRIES]
    print(f"Trovate {len(european)} partite in Europa oggi")
    return european

def get_predictions(fixture_id):
    url = f"{BASE_URL}/predictions"
    r = requests.get(url, headers=HEADERS, params={"fixture": fixture_id})
    if r.status_code != 200 or not r.json().get("response"):
        return {}
    pred = r.json()["response"][0]
    return {
        "Prediction_Winner": pred.get("predictions", {}).get("winner", {}).get("name", ""),
        "Prediction_WinOrDraw": pred.get("predictions", {}).get("win_or_draw", ""),
        "Prediction_UnderOver": pred.get("predictions", {}).get("under_over", ""),
        "Prediction_Goals_Home": pred.get("predictions", {}).get("goals_home", ""),
        "Prediction_Goals_Away": pred.get("predictions", {}).get("goals_away", ""),
        "Prediction_Advice": pred.get("advice", ""),
        "Percent_Home": pred.get("comparison", {}).get("form", {}).get("home", ""),
        "Percent_Away": pred.get("comparison", {}).get("form", {}).get("away", ""),
        "Percent_Att_Home": pred.get("comparison", {}).get("att", {}).get("home", ""),
        "Percent_Def_Away": pred.get("comparison", {}).get("def", {}).get("away", ""),
    }

def get_odds(fixture_id):
    url = f"{BASE_URL}/odds"
    params = {"fixture": fixture_id, "timezone": "Europe/Rome"}
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code != 200 or not r.json().get("response"):
        return {}
    
    odds_dict = {}
    for book in r.json()["response"][0].get("bookmakers", []):
        name = book["name"]
        if name not in BOOKMAKERS.values():
            continue
        book_name = [k for k, v in BOOKMAKERS.items() if v == book["id"]][0]
        
        for market in book.get("bets", []):
            label = market["label"]
            if label not in ["Match Winner", "Over/Under", "Both Teams To Score"]:
                continue
            for value in market["values"]:
                key = f"{book_name}_{label}_{value['value'].replace(' ', '')}"
                odds_dict[key] = value["odd"]
    return odds_dict

# ===================== MAIN =====================
fixtures = get_fixtures_today()
if not fixtures:
    print("Nessuna partita trovata. Esco.")
    exit()

results = []
total_calls = 2  # 1 fixtures + 1 leagues (se serve)

print("Inizio recupero predictions + odds...")
for i, fix in enumerate(fixtures, 1):
    fixture_id = fix["fixture"]["id"]
    league = fix["league"]["name"]
    home = fix["teams"]["home"]["name"]
    away = fix["teams"]["away"]["name"]
    kickoff = fix["fixture"]["date"]
    status = fix["fixture"]["status"]["long"]
    
    print(f"{i}/{len(fixtures)} - {home} vs {away} ({league})")
    
    pred = get_predictions(fixture_id)
    total_calls += 1
    time.sleep(0.15)  # gentile con l'API
    
    odds = get_odds(fixture_id)
    total_calls += 1
    time.sleep(0.15)
    
    row = {
        "Date": TODAY,
        "Kickoff_UTC": kickoff,
        "League": league,
        "Country": fix["league"]["country"],
        "Home_Team": home,
        "Away_Team": away,
        "Status": status,
        **pred,
        **odds
    }
    results.append(row)

# ===================== CSV =====================
keys = set()
for r in results:
    keys.update(r.keys())
keys = sorted(keys)

with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=keys)
    writer.writeheader()
    writer.writerows(results)

print(f"\nFINITO!")
print(f"Partite elaborate: {len(results)}")
print(f"Chiamate API stimate: ~{total_calls}")
print(f"CSV salvato: {CSV_FILE}")
print(f"Puoi scaricarlo e caricarlo subito su Google Sheets")
