# Merge normalized odds with MLB Savant team metrics into one training-ready table.
# Output: data/model_inputs/odds_savant_merged.csv

import os, glob, pandas as pd

# --- 1) Load odds ---
odds_path = "data/odds_api/latest_normalized.csv"
odds = pd.read_csv(odds_path)
print(f"Loaded odds -> {odds_path} ({len(odds)} rows)")

# --- 2) Load team metrics ---
bat_q = pd.read_csv("data/mlb_savant/team_batter_quality.csv")   # team, bat_barrel_rate, bat_hard_hit_rate
pit_q = pd.read_csv("data/mlb_savant/team_pitching_allowed.csv") # team, pit_barrels_allowed_rate, pit_hard_hit_allowed_rate

# --- 3) Map book team names to Savant abbreviations ---
TEAM_MAP = {
    # AL East
    "New York Yankees":"NYY","Boston Red Sox":"BOS","Tampa Bay Rays":"TB","Toronto Blue Jays":"TOR","Baltimore Orioles":"BAL",
    # AL Central
    "Detroit Tigers":"DET","Cleveland Guardians":"CLE","Chicago White Sox":"CWS","Minnesota Twins":"MIN","Kansas City Royals":"KC",
    # AL West
    "Houston Astros":"HOU","Texas Rangers":"TEX","Seattle Mariners":"SEA","Los Angeles Angels":"LAA","Oakland Athletics":"OAK",
    # NL East
    "New York Mets":"NYM","Philadelphia Phillies":"PHI","Atlanta Braves":"ATL","Miami Marlins":"MIA","Washington Nationals":"WSH",
    # NL Central
    "Chicago Cubs":"CHC","St. Louis Cardinals":"STL","Milwaukee Brewers":"MIL","Cincinnati Reds":"CIN","Pittsburgh Pirates":"PIT",
    # NL West
    "Los Angeles Dodgers":"LAD","San Francisco Giants":"SF","San Diego Padres":"SD","Arizona Diamondbacks":"ARI","Colorado Rockies":"COL",
    # Common alternates
    "LA Dodgers":"LAD","SF Giants":"SF","SD Padres":"SD","D-backs":"ARI","NY Yankees":"NYY","NY Mets":"NYM"
}

def to_abbr(name):
    return TEAM_MAP.get(name, name)  # leave as-is if already abbr

odds["home_abbr"] = odds["home_team"].apply(to_abbr)
odds["away_abbr"] = odds["away_team"].apply(to_abbr)

# --- 4) Attach team batting (own offense) ---
odds = odds.merge(bat_q.rename(columns={"team":"home_abbr"}), on="home_abbr", how="left")
odds = odds.rename(columns={
    "bat_barrel_rate":"home_bat_barrel_rate",
    "bat_hard_hit_rate":"home_bat_hard_hit_rate"
})
odds = odds.merge(bat_q.rename(columns={"team":"away_abbr"}), on="away_abbr", how="left")
odds = odds.rename(columns={
    "bat_barrel_rate":"away_bat_barrel_rate",
    "bat_hard_hit_rate":"away_bat_hard_hit_rate"
})

# --- 5) Attach team pitching allowed (defense) ---
odds = odds.merge(pit_q.rename(columns={"team":"home_abbr"}), on="home_abbr", how="left")
odds = odds.rename(columns={
    "pit_barrels_allowed_rate":"home_pit_barrels_allowed_rate",
    "pit_hard_hit_allowed_rate":"home_pit_hard_hit_allowed_rate"
})
odds = odds.merge(pit_q.rename(columns={"team":"away_abbr"}), on="away_abbr", how="left")
odds = odds.rename(columns={
    "pit_barrels_allowed_rate":"away_pit_barrels_allowed_rate",
    "pit_hard_hit_allowed_rate":"away_pit_hard_hit_allowed_rate"
})

# --- 6) Save ---
os.makedirs("data/model_inputs", exist_ok=True)
out = "data/model_inputs/odds_savant_merged.csv"
odds.to_csv(out, index=False)
print(f"✅ merged -> {out} (rows={len(odds)})")
