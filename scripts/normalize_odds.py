# Normalize aggregator-API odds into a consistent schema with implied probabilities.
# Expects latest file in data/odds_api/*.csv (created by your API workflow step).

import pandas as pd, glob, math, os

def money_to_prob(american):
    a = float(american)
    return 100/(a+100) if a > 0 else (-a)/(-a+100)

files = sorted(glob.glob("data/odds_api/*.csv"))
if not files:
    raise SystemExit("No odds snapshots found in data/odds_api/")
latest = files[-1]
df = pd.read_csv(latest)

# Columns from The Odds API normalization (adjust if you changed the fetch step)
# We expect: id, commence_time, home_team, away_team, book, market, name (selection), price, point (optional)
rename = {
    "bookmakers.key":"book_key",
    "bookmakers.title":"book",
    "bookmakers.markets.key":"market"
}
for k,v in rename.items():
    if k in df.columns and v not in df.columns:
        df[v] = df[k]

# Normalize selection naming
df["selection"] = df["name"].str.lower().map({
    "home":"home","away":"away","over":"over","under":"under"
}).fillna(df["name"])

# Lines & prices
if "point" in df.columns and "line" not in df.columns:
    df["line"] = df["point"]

if "price" in df.columns:
    df["implied_prob"] = df["price"].apply(money_to_prob)

keep = [c for c in [
    "id","commence_time","home_team","away_team","book","book_key","market",
    "selection","price","line","implied_prob"
] if c in df.columns]

out = "data/odds_api/latest_normalized.csv"
os.makedirs("data/odds_api", exist_ok=True)
df[keep].to_csv(out, index=False)
print(f"normalized -> {out} (rows={len(df)})")
