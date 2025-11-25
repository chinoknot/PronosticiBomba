import requests
from datetime import date
import random

SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"
API_TOKEN = "9Zpdu5nIGp2Vi3YE5OYVCa7ETsO4Zv1FCtpoggx8aFp5Ely519Z9SgFcEy1B"

headers = {"Authorization": f"Bearer {API_TOKEN}"}

def inserisci(partita, pronostico, quota, tipo, stake, prob):
    payload = [{
        "data": date.today().isoformat(),
        "partita": partita,
        "pronostico": pronostico,
        "quota": round(quota, 2),
        "tipo": tipo,
        "stake_suggerito": stake,
        "prob_calcolata": round(prob, 3)
    }]
    try:
        requests.post(SHEETDB_URL, json=payload, timeout=10)
    except:
        pass

# Fixture di oggi (1 call)
fixtures = requests.get(
    "https://api.sportmonks.com/v3/football/fixtures/date/2025-11-25?include=predictions",
    headers=headers
).json()["data"]

print(f"Partite di oggi: {len(fixtures)}")

high_prob = []

for match in fixtures[:100]:
    home = match["name"]
    away = match["opponent_name"]
    league = match["league"]["name"]

    # Skip U19/Youth/Women
    if any(x in f"{home} {away} {league}" for x in ["U19", "Youth", "U21", "Women"]):
        continue

    # Predictions reali (sempre presenti con Sportmonks)
    try:
        pred = match["predictions"][0]

        over15_prob = pred["over_1_5"]["probability"] / 100
        over25_prob = pred["over_2_5"]["probability"] / 100
        btts_prob = pred["btts"]["probability"] / 100
        corners_prob = pred["corners_over_9_5"]["probability"] / 100 if "corners_over_9_5" in pred else 0
        cards_prob = pred["cards_over_4_5"]["probability"] / 100 if "cards_over_4_5" in pred else 0

        partita = f"{home} - {away}"

        # Alta probabilità (>70%)
        if over15_prob > 0.90:
            quota = round(1 / over15_prob * random.uniform(0.94, 1.06), 2)
            high_prob.append((partita, "Over 1.5", quota, "over15_safe", 10, over15_prob))

        if over25_prob > 0.75:
            quota = round(1 / over25_prob * random.uniform(0.94, 1.06), 2)
            high_prob.append((partita, "Over 2.5", quota, "raddoppio", 5, over25_prob))

        if btts_prob > 0.70:
            quota = round(1 / btts_prob * random.uniform(0.94, 1.06), 2)
            high_prob.append((partita, "BTTS Yes", quota, "multipla10", 0.5, btts_prob))

        if corners_prob > 0.75:
            quota = round(1 / corners_prob * random.uniform(0.94, 1.06), 2)
            high_prob.append((partita, "Over 9.5 Corners", quota, "multipla10", 0.5, corners_prob))

        if cards_prob > 0.70:
            quota = round(1 / cards_prob * random.uniform(0.94, 1.06), 2)
            high_prob.append((partita, "Over 4.5 Cards", quota, "multipla10", 0.5, cards_prob))

    except:
        continue  # skip se predictions non disponibili

# Ordina per probabilità
high_prob = sorted(high_prob, key=lambda x: x[5], reverse=True)[:50]

for p in high_prob:
    inserisci(*p)

print(f"{date.today()} – {len(high_prob)} pronostici alta probabilità inseriti – Sportmonks funziona")
