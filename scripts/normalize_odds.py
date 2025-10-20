import pandas as pd, glob, math, os

def money_to_prob(american):
    a = float(american)
    return 100/(a+100) if a>0 else (-a)/(-a+100)

files = sorted(glob.glob("data/odds_api/*.csv"))
if not files:
    raise SystemExit("No odds snapshots found.")
latest = files[-1]
df = pd.read_csv(latest)

df["selection"] = df["name"].str.lower().map(
    {"home":"home","away":"away","over":"over","under":"under"}
).fillna(df["name"])

if "point" in df.columns and "line" not in df.columns:
    df["line"] = df["point"]

if "price" in df.columns:
    df["implied_prob"] = df["price"].apply(money_to_prob)

keep = [c for c in [
    "id","commence_time","home_team","away_team","book","market",
    "selection","price","line","implied_prob"
] if c in df.columns]

os.makedirs("data/odds_api", exist_ok=True)
df[keep].to_csv("data/odds_api/latest_normalized.csv", index=False)
print("✅ normalized -> data/odds_api/latest_normalized.csv")
