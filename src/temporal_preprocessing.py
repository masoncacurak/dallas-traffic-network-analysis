import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")
LINKS_FILE = "processed_links.csv"

# factor = 0.0  -> free-flow traffic
# factor = 1.0  -> fully congested traffic
CONGESTION_FACTORS = {
    "AM": 0.80,     # heavy congestion
    "Midday": 0.40, # light congestion
    "PM": 0.90,     # heaviest congestion
    "Evening": 0.20 # mostly free-flow
}

def load_links():
    path = os.path.join(PROCESSED_DIR, LINKS_FILE)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} not found. Make sure preprocessing.py has been run"
        )

    print(f"Loading {path}...")
    df = pd.read_csv(path)
    return df, path

# If congested_time exists but has NaNs, fill them from Max_Time or AB_Time
# If it does not exist at all, create it from Max_Time/AB_Time/freeflow_time
def ensure_congested_time(df):
    if "congested_time" not in df.columns:
        print("congested_time column missing. Trying to synthesize it...")
        if "Max_Time" in df.columns:
            df["congested_time"] = df["Max_Time"]
        elif "AB_Time" in df.columns:
            df["congested_time"] = df["AB_Time"]
        else:
            # fall back to free-flow
            df["congested_time"] = df["freeflow_time"]

    # Fill remaining NaNs
    if df["congested_time"].isna().any():
        filled_from = False
        if "Max_Time" in df.columns:
            df["congested_time"] = df["congested_time"].fillna(df["Max_Time"])
            filled_from = True
        if "AB_Time" in df.columns:
            df["congested_time"] = df["congested_time"].fillna(df["AB_Time"])
            filled_from = True

        df["congested_time"] = df["congested_time"].fillna(df["freeflow_time"])
        print(f"Filled NaNs in congested_time "
              f"{'from Max_Time/AB_Time and ' if filled_from else ''}from freeflow_time as last resort")

    return df

# Add travel_time_AM, travel_time_Midday, travel_time_PM, travel_time_Evening
# Formula: travel_time_T = freeflow_time + (congested_time - freeflow_time) * factor_T
#          where factor_T in [0, 1] controls how close we are to congested conditions
def add_temporal_travel_times(df):
    print("Creating temporal travel time columns...")
    if "freeflow_time" not in df.columns:
        raise KeyError("Column freeflow_time missing from processed_links.csv")

    df = ensure_congested_time(df)

    for period, factor in CONGESTION_FACTORS.items():
        col_name = f"travel_time_{period}"
        df[col_name] = (
            df["freeflow_time"]
            + (df["congested_time"] - df["freeflow_time"]) * factor
        )
        print(f"Added column '{col_name}' (factor={factor})")

    return df

def save_links(df, path):
    # Save a backup with temporal info
    backup_path = os.path.join(PROCESSED_DIR, "processed_links_temporal.csv")
    df.to_csv(backup_path, index=False)
    print(f"Saved temporal links to {backup_path}")

    # Overwrite main processed_links.csv so the rest of the pipeline can see the new columns
    df.to_csv(path, index=False)
    print(f"Overwrote {path} with temporal travel times")

def run_temporal_preprocessing():
    print("Temporal preprocessing starting...")
    df, path = load_links()
    print(f"Original columns: {list(df.columns)[:10]} ... (+{len(df.columns) - 10} more)")
    print(f"Original shape: {df.shape}")

    df = add_temporal_travel_times(df)

    print(f"New columns added:")
    for period in CONGESTION_FACTORS.keys():
        print(f"  - travel_time_{period}")

    save_links(df, path)
    print("Temporal preprocessing done!")

if __name__ == "__main__":
    run_temporal_preprocessing()
