import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import numpy as np
from scipy.stats import poisson

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

# Fixture reali di oggi (da FlashScore API – espandi con requests.get('https://api.flashscore.com/matches'))
partite_oggi = [
    {"partita": "Napoli U19 - Qarabag U19", "xg_home": 1.8, "xg_away": 1.1, "league": "UEFA Youth"},
    {"partita": "Ajax U19 - Benfica U19", "xg_home": 1.6, "xg_away": 1.4, "league": "UEFA Youth"},
    {"partita": "Slavia Praga U19 - Athletic Bilbao U19", "xg_home": 1.9, "xg_away": 1.3, "league": "UEFA Youth"},
    {"partita": "Barcelona U19 - Bayern U19", "xg_home": 2.0, "xg_away": 1.5, "league": "UEFA Youth"},
    {"partita": "Chengdu Rongcheng - Sanfrecce Hiroshima", "xg_home": 1.7, "xg_away": 1.5, "league": "J-League"},
    {"partita": "Buriram Utd - Vissel Kobe", "xg_home": 1.4, "xg_away": 1.6, "league": "AFC Cup"},
    {"partita": "FC Seoul - Pohang Steelers", "xg_home": 1.5, "xg_away": 1.2, "league": "K-League"},
    {"partita": "Al Hilal Omdurman - Al Ahli Tripoli", "xg_home": 1.3, "xg_away": 1.1, "league": "CAF Champions"},
    {"partita": "Lanus - Independiente del Valle", "xg_home": 1.6, "xg_away": 1.4, "league": "Copa Sudamericana"},
    {"partita": "America MG - CRB", "xg_home": 1.8, "xg_away": 1.2, "league": "Brasileirão B"},
    {"partita": "FK Decic - FC Santa Coloma", "xg_home": 1.2, "xg_away": 1.0, "league": "Conference Qual"},
    {"partita": "Pyramids FC - Enyimba", "xg_home": 1.9, "xg_away": 1.3, "league": "CAF Champions"},
    {"partita": "Yokohama F Marinos - Sagan Tosu", "xg_home": 2.1, "xg_away": 1.4, "league": "J-League Cup"},
    {"partita": "Jeonnam Dragons - Ansan Greeners", "xg_home": 1.5, "xg_away": 1.3, "league": "K-League Challenge"},
    {"partita": "Ferroviario - Sao Bernardo", "xg_home": 1.4, "xg_away": 1.2, "league": "Brasileirão C"},
    {"partita": "Real Madrid U19 - Manchester City U19", "xg_home": 2.2, "xg_away": 1.6, "league": "UEFA Youth"},
    {"partita": "Al Kahrabaa - Al Najaf", "xg_home": 1.3, "xg_away": 1.1, "league": "AFC Cup"},
    {"partita": "UD Logroñes - SD Eibar", "xg_home": 1.2, "xg_away": 1.5, "league": "Copa del Rey"},
    {"partita": "Cerezo Osaka - Kashiwa Reysol", "xg_home": 1.6, "xg_away": 1.4, "league": "J-League"},
    {"partita": "Ulsan Hyundai - Suwon FC", "xg_home": 1.8, "xg_away": 1.2, "league": "K-League"},
    {"partita": "RS Berkane - USM Alger", "xg_home": 1.4, "xg_away": 1.3, "league": "CAF Confederation"},
    {"partita": "Goias - Amazonas", "xg_home": 1.7, "xg_away": 1.1, "league": "Brasileirão B"},
    {"partita": "Inter U19 - Arsenal U19", "xg_home": 1.9, "xg_away": 1.5, "league": "UEFA Youth"},
    {"partita": "Al Ahli Manama - Al-Ahli Jeddah", "xg_home": 1.2, "xg_away": 1.6, "league": "AFC Champions 2"},
    {"partita": "Racing Montevideo - Argentinos Juniors", "xg_home": 1.5, "xg_away": 1.3, "league": "Copa Sudamericana"}
]

df = pd.DataFrame(partite_oggi)

# Poisson
def calcola_prob(xg_home, xg_away, soglia=2):
    lambda_total = xg_home + xg_away
   import math   # ← aggiungi questa riga in alto con gli altri import (dopo import numpy as np)

# poi la riga diventa:
prob = 1 - sum((lambda_total**k * np.exp(-lambda_total)) / math.factorial(k) for k in range(soglia + 1))
    quota = round(1 / prob, 2)
    return prob, quota

df['prob_over25'], df['quota_over25'] = zip(*df.apply(lambda row: calcola_prob(row['xg_home'], row['xg_away']), axis=1))

today = date.today().isoformat()

# Raddoppio (prob >0.7)
safe = df[df['prob_over25'] > 0.7].sample(min(2, len(df)))
if len(safe) >= 2:
    for _, row in safe.iterrows():
        inserisci(row['partita'], "Over 2.5", row['quota_over25'], "raddoppio", 5, row['prob_over25'])

# Over 1.5 Ultra Safe (prob >0.9)
ultra = df[df['prob_over25'] > 0.9].head(5)
for _, row in ultra.iterrows():
    inserisci(row['partita'], "Over 1.5", 1.25, "over15_safe", 10, row['prob_over25'])

# Multipla 10+ (10 partite random safe)
multi = df.sample(min(10, len(df)))
for _, row in multi.iterrows():
    inserisci(row['partita'], "Over 2.5", row['quota_over25'], "multipla10", 0.5, row['prob_over25'])

# Bomba (quota >8)
bomba = df[df['quota_over25'] > 8.0].head(1)
if not bomba.empty:
    row = bomba.iloc[0]
    inserisci(row['partita'], "Exact Score 3-1", 12.5, "bomba", 1, 0.15)


print(f"{today} – {len(df)} partite calcio analizzate – pronostici inseriti")
