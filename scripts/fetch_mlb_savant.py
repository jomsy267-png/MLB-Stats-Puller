from datetime import date, timedelta
import os
import pandas as pd
from pybaseball import statcast

DAYS = int(os.getenv("DAYS", "7"))
OUT_DIR = os.getenv("OUT_DIR", "data/mlb_savant") or "data/mlb_savant"

def main():
    start = (date.today() - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    end   = date.today().strftime("%Y-%m-%d")
    print(f"Fetching Statcast data from {start} to {end}")

    df = statcast(start_dt=start, end_dt=end)
    if df is None or len(df) == 0:
        raise SystemExit("No data returned from statcast(). Try widening the date window.")

    os.makedirs(OUT_DIR, exist_ok=True)
    raw_path = f"{OUT_DIR}/statcast_{start}_to_{end}.csv"
    df.to_csv(raw_path, index=False)
    print(f"✅ Raw saved -> {raw_path} ({len(df)} rows)")

    keep = [
        "pitcher","player_name","game_date","home_team","away_team","inning_topbot",
        "release_speed","launch_speed","launch_angle","events","description"
    ]
    df_trim = df[[c for c in keep if c in df.columns]].copy()

    # numeric coercion (avoid NA->int cast failures)
    for c in ("launch_speed", "launch_angle", "release_speed"):
        if c in df_trim.columns:
            df_trim[c] = pd.to_numeric(df_trim[c], errors="coerce")

    # flags
    if {"launch_speed","launch_angle"}.issubset(df_trim.columns):
        barrel_bool = (df_trim["launch_speed"].ge(98) &
                       df_trim["launch_angle"].between(26, 30)).fillna(False)
        hard_bool = df_trim["launch_speed"].ge(95).fillna(False)
        df_trim["barrel_flag"] = barrel_bool.astype("Int64").fillna(0).astype(int)
        df_trim["hard_hit"]    = hard_bool.astype("Int64").fillna(0).astype(int)
    else:
        df_trim["barrel_flag"] = 0
        df_trim["hard_hit"] = 0

    trim_path = f"{OUT_DIR}/latest_pitch_data.csv"
    df_trim.to_csv(trim_path, index=False)
    print(f"✅ Trim saved -> {trim_path} ({len(df_trim)} rows)")

    # per-pitcher aggregates
    if "pitcher" in df_trim.columns:
        agg = (df_trim.groupby("pitcher")[["barrel_flag","hard_hit"]]
               .mean()
               .rename(columns={"barrel_flag":"barrel_rate","hard_hit":"hard_hit_rate"})
               .reset_index())
        agg.to_csv(f"{OUT_DIR}/pitcher_quality.csv", index=False)
        print(f"✅ Pitcher aggregates -> {OUT_DIR}/pitcher_quality.csv ({len(agg)} pitchers)")

    # team batting/pitching
    def batting_team(row):
        return row.get("away_team") if row.get("inning_topbot") == "Top" else row.get("home_team")
    def pitching_team(row):
        return row.get("home_team") if row.get("inning_topbot") == "Top" else row.get("away_team")

    if {"home_team","away_team","inning_topbot"}.issubset(df_trim.columns):
        df_trim["bat_team"] = df_trim.apply(batting_team, axis=1)
        df_trim["pitch_team"] = df_trim.apply(pitching_team, axis=1)
        bat_cols = ["barrel_flag","hard_hit"]

        team_bat = (df_trim.groupby("bat_team")[bat_cols]
                    .mean()
                    .rename(columns={"barrel_flag":"bat_barrel_rate",
                                     "hard_hit":"bat_hard_hit_rate"})
                    .reset_index()
                    .rename(columns={"bat_team":"team"}))
        team_bat.to_csv(f"{OUT_DIR}/team_batter_quality.csv", index=False)
        print(f"✅ Team batting -> {OUT_DIR}/team_batter_quality.csv ({len(team_bat)} teams)")

        team_pit = (df_trim.groupby("pitch_team")[bat_cols]
                    .mean()
                    .rename(columns={"barrel_flag":"pit_barrels_allowed_rate",
                                     "hard_hit":"pit_hard_hit_allowed_rate"})
                    .reset_index()
                    .rename(columns={"pitch_team":"team"}))
        team_pit.to_csv(f"{OUT_DIR}/team_pitching_allowed.csv", index=False)
        print(f"✅ Team pitching -> {OUT_DIR}/team_pitching_allowed.csv ({len(team_pit)} teams)")

if __name__ == "__main__":
    main()
