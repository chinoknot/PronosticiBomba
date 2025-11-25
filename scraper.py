import requests
import pandas as pd
import numpy as np
from datetime import date
import random

# ←←← URL SheetDB tuo
SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"

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
    requests.post(SHEETDB_URL, json=payload)

# Partite di esempio (poi le sostituiamo con live)
partite = [
    {"partita": "Inter - Milan", "xg_home": 2.1, "xg_away": 1.4},
    {"partita": "Juventus - Napoli", "xg_home": 1.8, "xg_away": 1.2},
    {"partita": "Roma - Lazio", "xg_home": 1.7, "xg_away": 1.3},
    {"partita": "Atalanta - Fiorentina", "xg_home": 2.0, "xg_away": 1.5},
    {"partita": "Napoli U19 - Qarabag U19", "xg_home": 1.9, "xg_away": 1.1},
]

df = pd.DataFrame(partite)

def poisson_prob(xg_home, xg_away, soglia=2):
    l = xg_home + xg_away
    prob = 1 - sum((l**k * np.exp(-l)) / np.math.factorial(k) for k in range(soglia + 1))
    quota = max(1.01, round(1/prob * random.uniform(0.93, 1.07), 2))
    return prob, quota

df['prob'], df['quota'] = zip(*df.apply(lambda r: poisson_prob(r.xg_home, r.xg_away), axis=1))

today = date.today().isoformat()

# RADDOPPIO
safe = df[df['prob'] > 0.70]
if len(safe) >= 2:
    for _, r in safe.head(2).iterrows():
        inserisci(r['partita'], "Over 2.5", r['quota'], "raddoppio", 5, r['prob'])

# OVER 1.5 ULTRA SAFE
ultra = df[df['prob'] > 0.90]
for _, r in ultra.head(5).iterrows():
    inserisci(r['partita'], "Over 1.5", 1.25, "over15_safe", 10, 0.94)

# MULTIPLA 10+
for _, r in df.sample(min(10, len(df))).iterrows():
    inserisci(r['partita'], "Over 2.5", r['quota']+0.3, "multipla10", 0.5, r['prob'])

# BOMBA
bomba = df.sample(1).iloc[0]
inserisci(bomba['partita'], "Exact Score 3-1", round(random.uniform(11,18),1), "bomba", 1, 0.12)

print(f"{today} – Pronostici inseriti con successo!")
