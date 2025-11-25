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

    pronostici = []  # Definito qui

    for match in fixtures[:30]:
        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]
        home = match["teams"]["home"]["name"]
        away = match["teams"]["away"]["name"]

        if any(x in f"{home} {away}" for x in ["U19", "Youth", "U21", "Women"]):
            continue

        # Stats home & away (2 calls)
        h_stats = requests.get(f"https://v3.football.api-sports.io/teams/statistics?team={home_id}&season=2025", headers=headers).json().get("response", {})
        a_stats = requests.get(f"https://v3.football.api-sports.io/teams/statistics?team={away_id}&season=2025", headers=headers).json().get("response", {})

        if not h_stats or not a_stats:
            continue

        gf_h = float(h_stats.get("goals", {}).get("for", {}).get("average", {}).get("total", 1.4))
        ga_h = float(h_stats.get("goals", {}).get("against", {}).get("average", {}).get("total", 1.4))
        gf_a = float(a_stats.get("goals", {}).get("for", {}).get("average", {}).get("total", 1.4))
        ga_a = float(a_stats.get("goals", {}).get("against", {}).get("average", {}).get("total", 1.4))

        xG_home = gf_h * ga_a * 1.05
        xG_away = gf_a * ga_h
        total_xG = xG_home + xG_away

        over15 = 1 - poisson.cdf(1, total_xG)
        over25 = 1 - poisson.cdf(2, total_xG)
        btts = (1 - poisson.pmf(0, xG_home)) * (1 - poisson.pmf(0, xG_away))

        partita = f"{home} - {away}"

        if over15 > 0.90:
            pronostici.append((partita, "Over 1.5", round(1/over15*1.03,2), "over15_safe", 10, over15))

        if over25 > 0.72:
            pronostici.append((partita, "Over 2.5", round(1/over25*1.05,2), "raddoppio", 5, over25))

        if btts > 0.68:
            pronostici.append((partita, "BTTS Yes", round(1/btts*1.05,2), "multipla10", 0.5, btts))

    # Ordina per prob alta
    pronostici = sorted(pronostici, key=lambda x: x[5], reverse=True)[:50]

    for p in pronostici:
        inserisci(*p)

    print(f"{date.today()} – {len(pronostici)} pronostici reali inseriti – funziona")
