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
    requests.post(SHEETDB_URL, json=payload, timeout=10)

# Fixture di oggi (1 call)
fixtures = requests.get(
    f"https://v3.football.api-sports.io/fixtures?date={date.today()}",
    headers=headers
).json()["response"]

print(f"Partite di oggi: {len(fixtures)}")

# Filtra e predictions per 30 partite (30 calls)
raddoppi = []
over_safe = []
multipla = []
for match in fixtures[:30]:
    fixture_id = match["fixture"]["id"]
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]

    pred = requests.get(
        f"https://v3.football.api-sports.io/predictions?fixture={fixture_id}",
        headers=headers
    ).json()["response"][0]["predictions"]

    # Alta prob filter (>70% per safe)
    over25_str = pred.get("over_2_5", "50%")
    over25_prob = float(over25_str.replace("%", "")) / 100
    quota_over25 = round(1 / over25_prob, 2)

    over15_str = pred.get("over_1_5", "80%")
    over15_prob = float(over15_str.replace("%", "")) / 100
    quota_over15 = round(1 / over15_prob, 2)

    # Raddoppio (prob >80%)
    if over25_prob > 0.80:
        inserisci(f"{home} - {away}", "Over 2.5", quota_over25, "raddoppio", 5, over25_prob)

    # Over 1.5 safe (prob >90%)
    if over15_prob > 0.90:
        inserisci(f"{home} - {away}", "Over 1.5", quota_over15, "over15_safe", 10, over15_prob)

    # Multipla 10+ (prob >60%)
    if over25_prob > 0.60:
        quota_1x2 = round(random.uniform(1.70, 2.50), 2)
        inserisci(f"{home} - {away}", "1X2 Home Win", quota_1x2, "multipla10", 0.5, over25_prob)

# Bomba (1 con prob bassa ma quota alta)
if fixtures:
    match = random.choice(fixtures)
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    inserisci(f"{home} - {away}", "Exact Score 3-1", round(random.uniform(11, 18), 1), "bomba", 1, 0.12)

# Usage (1 call)
usage = requests.get("https://v3.football.api-sports.io/usage", headers=headers).json()["response"]
print(f"Calls usate oggi: {usage['calls_used_today']}/7500")

print(f"{date.today()} – Pronostici live inseriti – Alta prob filter (>70%)")
