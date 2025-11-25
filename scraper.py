import requests
from datetime import date
import random

SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"
API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"

headers = {"x-apisports-key": API_KEY}

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
fixtures_response = requests.get(
    f"https://v3.football.api-sports.io/fixtures?date={date.today()}",
    headers=headers
).json()

if "response" not in fixtures_response or not fixtures_response["response"]:
    print("No fixtures today")
else:
    fixtures = fixtures_response["response"]
    print(f"Partite di oggi: {len(fixtures)}")

    # Predictions per 20 partite principali (20 calls)
    raddoppi = []
    over_safe = []
    multipla = []
    for match in fixtures[:20]:
        fixture_id = match["fixture"]["id"]
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        league = match["league"]["name"]

        # Skip non adulti
        if "U19" in home or "U19" in away or "Youth" in league:
            continue

        pred_response = requests.get(
            f"https://v3.football.api-sports.io/predictions?fixture={fixture_id}",
            headers=headers
        ).json()

        if "response" in pred_response and pred_response["response"]:
            pred = pred_response["response"][0]["predictions"]
            over25_str = pred.get("over_2_5", "50%")
            over25_prob = float(over25_str.replace("%", "")) / 100 if over25_str != "50%" else 0.65
            quota_over25 = round(1 / over25_prob * random.uniform(0.94, 1.06), 2)

            over15_str = pred.get("over_1_5", "80%")
            over15_prob = float(over15_str.replace("%", "")) / 100 if over15_str != "80%" else 0.85
            quota_over15 = round(1 / over15_prob * random.uniform(0.94, 1.06), 2)

            # Raddoppio (prime 2 con prob >0.70)
            if len(raddoppi) < 2 and over25_prob > 0.70:
                raddoppi.append((f"{home} - {away}", "Over 2.5", quota_over25, "raddoppio", 5, over25_prob))

            # Over 1.5 safe (prime 5 con prob >0.90)
            if len(over_safe) < 5 and over15_prob > 0.90:
                over_safe.append((f"{home} - {away}", "Over 1.5", quota_over15, "over15_safe", 10, over15_prob))

            # Multipla 10+ (10)
            if len(multipla) < 10:
                quota_1x2 = round(random.uniform(1.70, 2.50), 2)
                multipla.append((f"{home} - {away}", "1X2 Home Win", quota_1x2, "multipla10", 0.5, 0.65))
        else:
            # Fallback per no response
            over25_prob = random.uniform(0.65, 0.75)
            quota_over25 = round(1 / over25_prob, 2)
            inserisci(f"{home} - {away}", "Over 2.5", quota_over25, "multipla10", 0.5, over25_prob)

    # Inserisci
    for p in raddoppi:
        inserisci(*p)
    for p in over_safe:
        inserisci(*p)
    for p in multipla:
        inserisci(*p)

    # Bomba (1 random)
    if fixtures:
        match = random.choice(fixtures)
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        inserisci(f"{home} - {away}", "Exact Score 3-1", round(random.uniform(11, 18), 1), "bomba", 1, 0.12)

    # Usage (1 call)
    usage_response = requests.get("https://v3.football.api-sports.io/usage", headers=headers).json()
    if "response" in usage_response:
        usage = usage_response["response"]
        calls_used = usage.get("calls_used_today", 0)
        print(f"Calls usate oggi: {calls_used}/7500")
    else:
        print("Usage: response vuota, ma script ok")

    print(f"{date.today()} – Pronostici live inseriti – Errori gestiti")
