import pandas as pd, os

odds = pd.read_csv("data/odds_api/latest_normalized.csv")
bat = pd.read_csv("data/mlb_savant/team_batter_quality.csv")
pit = pd.read_csv("data/mlb_savant/team_pitching_allowed.csv")

TEAM_MAP = {
 "New York Yankees":"NYY","Boston Red Sox":"BOS","Tampa Bay Rays":"TB",
 "Toronto Blue Jays":"TOR","Baltimore Orioles":"BAL","Detroit Tigers":"DET",
 "Cleveland Guardians":"CLE","Chicago White Sox":"CWS","Minnesota Twins":"MIN",
 "Kansas City Royals":"KC","Houston Astros":"HOU","Texas Rangers":"TEX",
 "Seattle Mariners":"SEA","Los Angeles Angels":"LAA","Oakland Athletics":"OAK",
 "New York Mets":"NYM","Philadelphia Phillies":"PHI","Atlanta Braves":"ATL",
 "Miami Marlins":"MIA","Washington Nationals":"WSH","Chicago Cubs":"CHC",
 "St. Louis Cardinals":"STL","Milwaukee Brewers":"MIL","Cincinnati Reds":"CIN",
 "Pittsburgh Pirates":"PIT","Los Angeles Dodgers":"LAD","San Francisco Giants":"SF",
 "San Diego Padres":"SD","Arizona Diamondbacks":"ARI","Colorado Rockies":"COL"
}
odds["home_abbr"] = odds["home_team"].map(TEAM_MAP).fillna(odds["home_team"])
odds["away_abbr"] = odds["away_team"].map(TEAM_MAP).fillna(odds["away_team"])

# merge batting & pitching metrics
odds = odds.merge(bat.rename(columns={"team":"home_abbr"}), on="home_abbr", how="left")\
           .rename(columns={"bat_barrel_rate":"home_bat_barrel_rate",
                            "bat_hard_hit_rate":"home_bat_hard_hit_rate"})
odds = odds.merge(bat.rename(columns={"team":"away_abbr"}), on="away_abbr", how="left")\
           .rename(columns={"bat_barrel_rate":"away_bat_barrel_rate",
                            "bat_hard_hit_rate":"away_bat_hard_hit_rate"})
odds = odds.merge(pit.rename(columns={"team":"home_abbr"}), on="home_abbr", how="left")\
           .rename(columns={"pit_barrels_allowed_rate":"home_pit_barrels_allowed_rate",
                            "pit_hard_hit_allowed_rate":"home_pit_hard_hit_allowed_rate"})
odds = odds.merge(pit.rename(columns={"team":"away_abbr"}), on="away_abbr", how="left")\
           .rename(columns={"pit_barrels_allowed_rate":"away_pit_barrels_allowed_rate",
                            "pit_hard_hit_allowed_rate":"away_pit_hard_hit_allowed_rate"})

os.makedirs("data/model_inputs", exist_ok=True)
out = "data/model_inputs/odds_savant_merged.csv"
odds.to_csv(out, index=False)
print(f"✅ merged -> {out}")
