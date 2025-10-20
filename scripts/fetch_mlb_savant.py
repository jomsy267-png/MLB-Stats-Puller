# scripts/fetch_mlb_savant.py
from pybaseball import statcast_pitcher
from datetime import date, timedelta
import pandas as pd, os

def main():
    # Pull last 7 days of Statcast data
    start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    end = date.today().strftime("%Y-%m-%d")

    print(f"Fetching Statcast data from {start} to {end}")
    df = statcast_pitcher(start_dt=start, end_dt=end)

    # Select core features for model
    keep = [
        "pitcher","player_name","release_speed","launch_speed","launch_angle",
        "events","description","home_team","away_team","game_date"
    ]
    df = df[keep]
    os.makedirs("data/mlb_savant", exist_ok=True)
    out_path = f"data/mlb_savant/latest_pitch_data.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main()
