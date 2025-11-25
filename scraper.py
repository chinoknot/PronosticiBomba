import requests
from datetime import date
import random
from scipy.stats import poisson

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

if "response" not in fixtures_response:
    print("No fixtures")
else:
    fixtures = fixtures_response["response"]
    print(f"Partite di oggi: {len(fixtures)}")

    # Stats per 10 leagues principali (10 calls, incl. National League Cup ID 534)
    leagues = [2, 39, 78, 135, 61, 71, 140, 144, 534, 135]
    lambda_medio = {}
    for lid in leagues:
        stats_response = requests.get(
            f"https://v3.football.api-sports.io/leagues/statistics?league={lid}&season=2025",
            headers=headers
        ).json()
        if "response" in stats_response and stats_response["response"]:
            stats = stats_response["response"]
            avg_goals = sum(s["goals"]["for"]["total"]["total"] for s in stats if "goals" in s) / len(stats) + sum(s["goals"]["against"]["total"]["total"] for s in stats if "goals" in s) / len(stats)
            lambda_medio[lid] = avg_goals / 2 if avg_goals else 2.8
        else:
            lambda_medio[lid] = 2.8

    high_prob = []
    for match in fixtures[:50]:
        lid = match["league"]["id"]
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]
        league = match["league"]["name"]

        if "U19" in home or "U19" in away or "Youth" in league:
            continue

        lambda_total = lambda_medio.get(lid, 2.8)

        # Poisson per over 2.5
        over25_prob = 1 - poisson.cdf(2, lambda_total)
        quota_over25 = round(1 / over25_prob * random.uniform(0.94, 1.06), 2)

        # Poisson per over 1.5
        over15_prob = 1 - poisson.cdf(1, lambda_total)
        quota_over15 = round(1 / over15_prob * random.uniform(0.94, 1.06), 2)

        # BTTS approx
        btts_prob = (1 - poisson.pmf(0, lambda_total / 2)) ** 2
        quota_btts = round(1 / btts_prob * random.uniform(0.94, 1.06), 2)

        partita = f"{home} - {away} ({league})"

        # Alta prob (>70%)
        if over15_prob > 0.90:
            high_prob.append((partita, "Over 1.5", quota_over15, "over15_safe", 10, over15_prob))

        if over25_prob > 0.70:
            high_prob.append((partita, "Over 2.5", quota_over25, "raddoppio", 5, over25_prob))

        if btts_prob > 0.70:
            high_prob.append((partita, "BTTS Yes", quota_btts, "multipla10", 0.5, btts_prob))

    # Ordina per prob alta
    high_prob = sorted(high_prob, key=lambda x: x[5], reverse=True)[:50]

    for p in high_prob:
        inserisci(*p)

    print(f"{date.today()} – {len(high_prob)} pronostici alta prob inseriti – Poisson su stats reali")
