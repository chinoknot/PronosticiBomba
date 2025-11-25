import requests
from datetime import date

SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"
API_KEY = "daaf29bc97d50f28aa64816c7cc203bc"

headers = {"x-apisports-key": API_KEY}

def inserisci(partita, pronostico, quota, tipo, stake, prob):
    payload = [{"data": date.today().isoformat(), "partita": partita, "pronostico": pronostico, "quota": quota, "tipo": tipo, "stake_suggerito": stake, "prob_calcolata": round(prob,3)}]
    requests.post(SHEETDB_URL, json=payload, timeout=10)

# Fixture di oggi (1 call)
fixtures = requests.get(f"https://v3.football.api-sports.io/fixtures?date={date.today()}", headers=headers).json()["response"]

print(f"Partite di oggi: {len(fixtures)}")

# Prendi predictions per 120 partite (120 calls – con Pro è un cazzo di niente)
high_prob = []

for match in fixtures[:120]:
    fid = match["fixture"]["id"]
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    league = match["league"]["name"]

    # Skip U19/Youth
    if any(x in f"{home} {away} {league}" for x in ["U19", "Youth", "U21", "Women"]):
        continue

    pred = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}", headers=headers).json()["response"][0]["predictions"]

    # Estraggo TUTTE le probabilità reali
    try:
        over15 = float(pred["over_1_5"].replace("%","")) / 100
        over25 = float(pred["over_2_5"].replace("%","")) / 100
        btts = float(pred["btts_yes"].replace("%","")) / 100 if "btts_yes" in pred else 0
        home_win = float(pred["win_home"].replace("%","")) / 100 if "win_home" in pred else 0
        corners = float(pred["corners_over_9_5"].replace("%","")) / 100 if "corners_over_9_5" in pred else 0
        cards = float(pred["cards_over_4_5"].replace("%","")) / 100 if "cards_over_4_5" in pred else 0

        partita = f"{home} - {away}"

        # ALTA PROBABILITÀ (>75%)
        if over15 > 0.90:
            quota = round(1 / over15 * 1.03, 2)
            high_prob.append((partita, "Over 1.5", quota, "over15_safe", 10, over15))

        if over25 > 0.75:
            quota = round(1 / over25 * 1.05, 2)
            high_prob.append((partita, "Over 2.5", quota, "raddoppio", 5, over25))

        if btts > 0.70:
            quota = round(1 / btts * 1.05, 2)
            high_prob.append((partita, "BTTS Yes", quota, "multipla10", 0.5, btts))

        if corners > 0.75:
            quota = round(1 / corners * 1.05, 2)
            high_prob.append((partita, "Over 9.5 Corners", quota, "multipla10", 0.5, corners))

        if cards > 0.70:
            quota = round(1 / cards * 1.05, 2)
            high_prob.append((partita, "Over 4.5 Cards", quota, "multipla10", 0.5, cards))

    except:
        continue  # skip se qualche campo manca

# Inserisci le 40/50 migliori (ordinato per prob)
high_prob = sorted(high_prob, key=lambda x: x[5], reverse=True)[:50]

for p in high_prob:
    inserisci(*p)

# Bomba (1 con prob bassa ma quota alta)
if fixtures:
    match = random.choice(fixtures[:50])
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    inserisci(f"{home} - {away}", "Exact Score 3-1", round(random.uniform(14, 22), 1), "bomba", 1, 0.11)

print(f"{date.today()} – {len(high_prob)} pronostici alta probabilità inseriti (7500 calls disponibili)")
