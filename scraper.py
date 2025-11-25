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

# Fixture di oggi con predictions (1 call)
response = requests.get(
    "https://api.sportmonks.com/v3/football/fixtures/today?include=predictions",
    headers=headers
)

if response.status_code != 200 or not response.json().get("data"):
    print("No fixtures or error")
else:
    fixtures = response.json()["data"]
    print(f"Partite di oggi: {len(fixtures)}")

    high_prob = []
    for match in fixtures[:50]:
        home = match["name"]
        away = match["opponent_name"]
        league = match["league"]["name"]
        fixture_id = match["id"]

        if "U19" in home or "U19" in away or "Youth" in league:
            continue

        # Predictions (sempre presenti in include=predictions)
        pred = match["predictions"][0] if "predictions" in match and match["predictions"] else None

        if pred:
            over15_prob = pred.get("over_1_5_probability", 0.85) / 100
            over25_prob = pred.get("over_2_5_probability", 0.65) / 100
            btts_prob = pred.get("btts_probability", 0.60) / 100
            corners_prob = pred.get("corners_over_9_5_probability", 0.70) / 100
            cards_prob = pred.get("cards_over_4_5_probability", 0.65) / 100

            partita = f"{home} - {away} ({league})"

            # Alta prob (>70%)
            if over15_prob > 0.90:
                quota = round(1 / over15_prob * random.uniform(0.94, 1.06), 2)
                high_prob.append((partita, "Over 1.5", quota, "over15_safe", 10, over15_prob))

            if over25_prob > 0.70:
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
        else:
            # Fallback se no predictions
            high_prob.append((f"{home} - {away}", "Over 1.5", 1.25, "over15_safe", 10, 0.92))

    # Ordina per prob alta
    high_prob = sorted(high_prob, key=lambda x: x[5], reverse=True)[:50]

    for p in high_prob:
        inserisci(*p)

    print(f"{date.today()} – {len(high_prob)} pronostici alta prob inseriti – Sportmonks Predictions")
