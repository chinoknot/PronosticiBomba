import requests
import pandas as pd
import numpy as np
import math
from datetime import date

SUPABASE_URL = "https://oiudaxsyvhjpjjhglejd.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9pdWRheHN5dmhqcGpqaGdsZWpkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQwMDk0OTcsImV4cCI6MjA3OTU4NTQ5N30.r7kz3FdijAhsJLz1DcEtobJLaPCqygrQGgCPpSc-05A"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

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
    requests.post(f"{SUPABASE_URL}/rest/v1/pronostici", json=payload, headers=headers)

partite_oggi = [
    {"partita": "Napoli U19 - Qarabag U19", "xg_home": 1.8, "xg_away": 1.1},
    {"partita": "Ajax U19 - Benfica U19", "xg_home": 1.6, "xg_away": 1.4},
    {"partita": "Slavia Praga U19 - Athletic Bilbao U19", "xg_home": 1.9, "xg_away": 1.3},
    {"partita": "Barcelona U19 - Bayern U19", "xg_home": 2.0, "xg_away": 1.5},
    {"partita": "Chengdu Rongcheng - Sanfrecce Hiroshima", "xg_home": 1.7, "xg_away": 1.5},
    {"partita": "Buriram Utd - Vissel Kobe", "xg_home": 1.4, "xg_away": 1.6},
    {"partita": "Inter U19 - Arsenal U19", "xg_home": 1.9, "xg_away": 1.5},
]

df = pd.DataFrame(partite_oggi)

def calcola_prob(xg_home, xg_away, soglia=2):
    lambda_total = xg_home + xg_away
    prob = 1 - sum((lambda_total**k * np.exp(-lambda_total)) / math.factorial(k) for k in range(soglia + 1))
    quota = max(1.01, round(1 / prob * np.random.uniform(0.93, 1.07), 2))
    return prob, quota

df['prob_over25'], df['quota_over25'] = zip(*df.apply(lambda row: calcola_prob(row['xg_home'], row['xg_away']), axis=1))

today = date.today().isoformat()

# RADDOPPIO
safe = df[df['prob_over25'] > 0.7]
if len(safe) >= 2:
    due = safe.sample(2)
    for _, row in due.iterrows():
        inserisci(row['partita'], "Over 2.5", row['quota_over25'], "raddoppio", 5, row['prob_over25'])

# OVER 1.5 SAFE
ultra = df[df['prob_over25'] > 0.9]
for _, row in ultra.head(5).iterrows():
    inserisci(row['partita'], "Over 1.5", 1.25, "over15_safe", 10, row['prob_over25'])

# MULTIPLA 10+
multi = df.sample(min(10, len(df)))
for _, row in multi.iterrows():
    inserisci(row['partita'], "Over 1.5", row['quota_over25'] + 0.3, "multipla10", 0.5, row['prob_over25'])

# BOMBA
bomba = df.sample(1).iloc[0]
inserisci(bomba['partita'], "Exact Score 3-1", round(np.random.uniform(11, 18), 1), "bomba", 1, 0.12)

print(f"{today} – {len(df)} partite analizzate – PRONOSTICI INSERITI")
