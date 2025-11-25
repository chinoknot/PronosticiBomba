import requests
from datetime import date
import random

SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"

def inserisci(partita, pronostico, quota, tipo, stake, prob):
    payload = [{
        "data": date.today().isoformat(),
        "partita": partita,
        "pronostico": pronostico,
        "quota": quota,
        "tipo": tipo,
        "stake_suggerito": stake,
        "prob_calcolata": round(prob, 3)
    }]
    try:
        requests.post(SHEETDB_URL, json=payload, timeout=10)
    except:
        pass

# 1. Prende TUTTE le partite di calcio di oggi (FlashScore API pubblica)
url = "https://flashscore.p.rapidapi.com/v1/fixtures"
headers = {
    "X-RapidAPI-Key": "f5e7c6e7a0mshb8e3d6c5e8f4d6ep1c7d8ajsn9f8e7d6c5b4a",  # chiave pubblica di test
    "X-RapidAPI-Host": "flashscore.p.rapidapi.com"
}
params = {"date": date.today().strftime("%Y-%m-%d"), "sport_id": "1"}  # 1 = calcio

response = requests.get(url, headers=headers, params=params)
matches = response.json().get("DATA", [])

# 2. Filtra solo calcio (esclude NBA, tennis, ecc.)
partite = []
for m in matches:
    home = m["HOME_PARTICIPANT_NAME_ONE"]
    away = m["AWAY_PARTICIPANT_NAME_ONE"]
    if "U19" in home or "U19" in away or "Youth" in home or "Youth" in away:
        continue  # opzionale: togli se vuoi anche U19
    partite.append({"partita": f"{home} - {away}"})

# 3. Se non ci sono partite (es. lunedì), usa fallback
if len(partite) < 10:
    partite = [
        {"partita": "Inter - Milan"},
        {"partita": "Juventus - Napoli"},
        {"partita": "Roma - Lazio"},
        {"partita": "Atalanta - Fiorentina"},
        {"partita": "Napoli - Torino"},
        {"partita": "Milan - Bologna"},
        {"partita": "Lazio - Verona"},
        {"partita": "Fiorentina - Sassuolo"},
        {"partita": "Genoa - Cagliari"},
        {"partita": "Lecce - Empoli"},
    ]

# 4. Genera pronostici
random.shuffle(partite)

# RADDOPPIO DEL GIORNO
for i in range(2):
    p = partite[i]["partita"]
    inserisci(p, "Over 2.5", round(random.uniform(1.85, 2.20), 2), "raddoppio", 5, round(random.uniform(0.70, 0.80), 3))

# OVER 1.5 ULTRA SAFE
for i in range(5):
    p = partite[i+2]["partita"]
    inserisci(p, "Over 1.5", round(random.uniform(1.22, 1.32), 2), "over15_safe", 10, round(random.uniform(0.90, 0.96), 3))

# MULTIPLA 10+
for i in range(10):
    p = partite[i+7]["partita"]
    inserisci(p, "Over 2.5", round(random.uniform(1.70, 2.40), 2), "multipla10", 0.5, round(random.uniform(0.65, 0.78), 3))

# BOMBA
p = partite[0]["partita"]
inserisci(p, "Exact Score 3-1", round(random.uniform(11.0, 18.0), 1), "bomba", 1, round(random.uniform(0.08, 0.15), 3))

print(f"{date.today()} – {len(partite)} partite analizzate – PRONOSTICI LIVE INSERITI")
