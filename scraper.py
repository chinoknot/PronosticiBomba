import requests
from datetime import date
import random  # <--- questa riga mancava, ora c'è

SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"
API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"

headers = {"x-apisports-key": API_KEY}

def inserisci(partita, pronostico, quota, tipo, stake, prob):
    payload = [{"data": date.today().isoformat(), "partita": partita, "pronostico": pronostico, "quota": quota, "tipo": tipo, "stake_suggerito": stake, "prob_calcolata": round(prob,3)}]
    requests.post(SHEETDB_URL, json=payload, timeout=10)

# Fixture di oggi
fixtures = requests.get(f"https://v3.football.api-sports.io/fixtures?date={date.today()}", headers=headers).json()["response"]

print(f"Partite di oggi: {len(fixtures)}")

high_prob = []

for match in fixtures[:120]:
    fid = match["fixture"]["id"]
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    league = match["league"]["name"]

    if any(x in f"{home} {away} {league}" for x in ["U19", "Youth", "U21", "Women"]):
        continue

    pred = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}", headers=headers).json()["response"][0]["predictions"]

    try:
        over15 = float(pred["over_1_5"].replace("%","")) / 100
        over25 = float(pred["over_2_5"].replace("%","")) / 100
        btts = float(pred.get("btts_yes", "0%").replace("%","")) / 100
        corners = float(pred.get("corners_over_9_5", "0%").replace("%","")) / 100
        cards = float(pred.get("cards_over_4_5", "0%").replace("%","")) / 100

        partita = f"{home} - {away}"

        if over15 > 0.90:
            high_prob.append((partita, "Over 1.5", round(1/over15*1.03,2), "over15_safe", 10, over15))
        if over25 > 0.75:
            high_prob.append((partita, "Over 2.5", round(1/over25*1.05,2), "raddoppio", 5, over25))
        if btts > 0.70:
            high_prob.append((partita, "BTTS Yes", round(1/btts*1.05,2), "multipla10", 0.5, btts))
        if corners > 0.75:
            high_prob.append((partita, "Over 9.5 Corners", round(1/corners*1.05,2), "multipla10", 0.5, corners))
        if cards > 0.70:
            high_prob.append((partita, "Over 4.5 Cards", round(1/cards*1.05,2), "multipla10", 0.5, cards))
    except:
        continue

# Ordina per probabilità più alta
high_prob = sorted(high_prob, key=lambda x: x[5], reverse=True)[:50]

for p in high_prob:
    inserisci(*p)

# Bomba
if fixtures:
    match = random.choice(fixtures[:50])
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    inserisci(f"{home} - {away}", "Exact Score 3-1", round(random.uniform(14,22),1), "bomba", 1, 0.11)

print(f"{date.today()} – {len(high_prob)} pronostici alta probabilità inseriti – Pro plan")
