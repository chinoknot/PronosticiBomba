# FILE: print_full_csv_in_logs.py
import requests
from datetime import datetime
import time

API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-rapidapi-host": "v3.football.api-sports.io", "x-rapidapi-key": API_KEY}
TODAY = "2025-11-26"

EUROPE = {"Europe","England","Italy","Spain","Germany","France","Portugal","Netherlands","Belgium","Scotland","Turkey","Greece","Austria","Switzerland","Croatia","Czech Republic","Denmark","Sweden","Norway","Poland","Romania","Serbia","Ukraine","Russia"}

# 1. Fixtures
fixtures = requests.get(f"{BASE_URL}/fixtures", headers=HEADERS, params={"date": TODAY, "timezone": "Europe/Rome"}).json().get("response", [])
europe_fixtures = [f for f in fixtures if f["league"]["country"] in EUROPE]
print(f"Partite europee oggi: {len(europe_fixtures)}\n")

# 2. Iniziamo il CSV
print("DATA,ORARIO,LEGA,CASA,FUORI,STATUS,PRED_WINNER,PRED_ADVICE,PRED_UNDER_OVER,BET365_HOME,BET365_DRAW,BET365_AWAY,BET365_OVER25,BET365_UNDER25,PINNACLE_HOME,PINNACLE_DRAW,PINNACLE_AWAY,1XBET_HOME,1XBET_DRAW,1XBET_AWAY")

for f in europe_fixtures:
    fid = f["fixture"]["id"]
    home = f["teams"]["home"]["name"]
    away = f["teams"]["away"]["name"]
    league = f["league"]["name"]
    kickoff = f["fixture"]["date"][11:16]  # solo HH:MM
    status = f["fixture"]["status"]["short"]
    
    # Predictions
    pred = requests.get(f"{BASE_URL}/predictions", headers=HEADERS, params={"fixture": fid})
    time.sleep(0.2)
    winner = advice = underover = "N/D"
    if pred.status_code == 200 and pred.json().get("response"):
        p = pred.json()["response"][0]
        winner = p.get("predictions", {}).get("winner", {}).get("name", "N/D")
        advice = p.get("advice", "N/D")
        underover = p.get("predictions", {}).get("under_over", "N/D") or "N/D"
    
    # Odds (solo i 3 bookmaker più stabili)
    odds_resp = requests.get(f"{BASE_URL}/odds", headers=HEADERS, params={"fixture": fid, "bookmaker": "8,2,12", "bet": "1,3"})
    time.sleep(0.2)
    
    b365_1 = b365_x = b365_2 = b365_o25 = b365_u25 = "-"
    pin_1 = pin_x = pin_2 = "-"
    x1_1 = x1_x = x1_2 = "-"
    
    if odds_resp.status_code == 200 and odds_resp.json().get("response"):
        for book in odds_resp.json()["response"][0]["bookmakers"]:
            name = book["name"]
            for bet in book["bets"]:
                if bet["label"] == "Match Winner":
                    vals = {v["value"]: v["odd"] for v in bet["values"]}
                    if name == "Bet365":
                        b365_1, b365_x, b365_2 = vals.get("Home",""), vals.get("Draw",""), vals.get("Away","")
                    elif name == "Pinnacle":
                        pin_1, pin_x, pin_2 = vals.get("Home",""), vals.get("Draw",""), vals.get("Away","")
                    elif name == "1xBet":
                        x1_1, x1_x, x1_2 = vals.get("Home",""), vals.get("Draw",""), vals.get("Away","")
                if bet["label"] == "Over/Under" and "2.5" in bet["name"]:
                    for v in bet["values"]:
                        if v["value"] == "Over 2.5":  b365_o25 = v["odd"] if name=="Bet365" else b365_o25
                        if v["value"] == "Under 2.5": b365_u25 = v["odd"] if name=="Bet365" else b365_u25
    
    # Stampa la riga completa
    row = f'{TODAY},{kickoff},{league},{home},{away},{status},{winner},"{advice}",{underover},{b365_1},{b365_x},{b365_2},{b365_o25},{b365_u25},{pin_1},{pin_x},{pin_2},{x1_1},{x1_x},{x1_2}'
    print(row)

print("\nFINE CSV – seleziona tutto sopra e incolla in Google Sheets/Excel")
