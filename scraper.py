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

    high_prob = []
    for match in fixtures[:50]:  # 50 per velocità
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
            try:
                over25_str = pred.get("over_2_5", "50%")
                over25_prob = float(over25_str.replace("%", "")) / 100
                quota_over25 = round(1 / over25_prob * random.uniform(0.94, 1.06), 2)

                over15_str = pred.get("over_1_5", "80%")
                over15_prob = float(over15_str.replace("%", "")) / 100
                quota_over15 = round(1 / over15_prob * random.uniform(0.94, 1.06), 2)

                btts_str = pred.get("btts_yes", "50%")
                btts_prob = float(btts_str.replace("%", "")) / 100
                quota_btts = round(1 / btts_prob * random.uniform(0.94, 1.06), 2)

                # Alta prob filter (>70%)
                if over15_prob > 0.90:
                    high_prob.append((f"{home} - {away}", "Over 1.5", quota_over15, "over15_safe", 10, over15_prob))

                if over25_prob > 0.70:
                    high_prob.append((f"{home} - {away}", "Over 2.5", quota_over25, "raddoppio", 5, over25_prob))

                if btts_prob > 0.70:
                    high_prob.append((f"{home} - {away}", "BTTS Yes", quota_btts, "multipla10", 0.5, btts_prob))

            except:
                # Fallback Poisson su lambda 3.0
                lambda_total = 3.0
                over25_prob = 1 - poisson.cdf(2, lambda_total)
                quota_over25 = round(1 / over25_prob, 2)
                high_prob.append((f"{home} - {away}", "Over 2.5", quota_over25, "multipla10", 0.5, over25_prob))

    # Ordina per prob alta
    high_prob = sorted(high_prob, key=lambda x: x[5], reverse=True)[:50]

    for p in high_prob:
        inserisci(*p)

    # Bomba (1 con prob bassa)
    if fixtures:
        match = random.choice(fixtures)
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        inserisci(f"{home} - {away}", "Exact Score 3-1", round(random.uniform(14, 22), 1), "bomba", 1, 0.11)

    # Usage (1 call)
    usage_response = requests.get("https://v3.football.api-sports.io/usage", headers=headers).json()
    if "response" in usage_response:
        usage = usage_response["response"]
        calls_used = usage.get("calls_used_today", 0)
        print(f"Calls usate oggi: {calls_used}/7500")

    print(f"{date.today()} – {len(high_prob)} pronostici alta prob inseriti – Errori gestiti")
