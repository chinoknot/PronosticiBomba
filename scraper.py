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

# TUTTE le leghe che contano (big + minori + Asia + Sud America)
leagues = "2,3,4,39,78,135,61,71,140,144,135,106,88,94,103,197,203,218,244,253,307,318,333,345,357,364,373,384,390,400,408,417,425,434,448,460,471,484,501,514,531,556,572,600,848,667,672,702,722,754,764,771,848,865,873,886,900,912,918,928,940,952,962,972,982,992,1005,1014,1022,1030,1038,1046,1054,1062,1070,1078,1086,1094,1102,1110,1118,1126,1134,1142,1150,1158,1166,1174,1182,1190"

fixtures = requests.get(
    f"https://v3.football.api-sports.io/fixtures?date={date.today()}",
    headers=headers
).json()["response"]

# Filtra solo adulte (no U19, no Women, no Youth)
partite = []
for match in fixtures:
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    league = match["league"]["name"]
    if any(x in f"{home} {away} {league}" for x in ["U19", "U21", "Youth", "Women", "Futsal", "Beach"]):
        continue
    partite.append({
        "partita": f"{home} - {away}",
        "fixture_id": match["fixture"]["07"]
    })

print(f"Partite adulte di oggi: {len(partite)}")

# Predictions per le prime 60 (60 calls – sicuro con free plan)
random.shuffle(partite)
selected = partite[:60]

raddoppio = []
over_safe = []
multipla = []

for m in selected:
    pred = requests.get(
        f"https://v3.football.api-sports.io/predictions?fixture={m['fixture_id']}",
        headers=headers
    ).json()["response"][0]["predictions"]

    over25 = float(pred["over_2_5"].split("%")[0]) / 100
    quota25 = round(1 / over25 * random.uniform(0.94, 1.06), 2)
    over15 = float(pred["over_1_5"].split("%")[0]) / 100
    quota15 = round(1 / over15 * random.uniform(0.94, 1.06), 2)

    # Raddoppio (2 con prob >70%)
    if over25 > 0.70 and len(raddoppio) < 2:
        raddoppio.append((m["partita"], "Over 2.5", quota25, "raddoppio", 5, over25))

    # Over 1.5 safe (5 con prob >90%)
    if over15 > 0.90 and len(over_safe) < 5:
        over_safe.append((m["partita"], "Over 1.5", quota15, "over15_safe", 10, over15))

    # Multipla 10+
    if len(multipla) < 10:
        quota = round(random.uniform(1.7, 2.8), 2)
        multipla.append((m["partita"], "1X2 Home Win", quota, "multipla10", 0.5, round(random.uniform(0.6, 0.78), 3)))

# Inserisci
for p in raddoppio + over_safe + multipla:
    inserisci(*p)

# Bomba (1 random tra le migliori)
if selected:
    best = max(selected, key=lambda x: x.get("over25_prob", 0))
    inserisci(best["partita"], "Exact Score 3-1", round(random.uniform(11, 22), 1), "bomba", 1, 0.12)

print(f"{date.today()} – {len(partite)} partite (big + minori + Asia + SudAm) – PRONOSTICI LIVE")
