import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "../data/raw/")
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")

# Load CSV from the raw folder
def load_csv(name):
    path = os.path.join(RAW_DIR, name)
    if not os.path.exists(path):
        print(f"[WARNING] {name} not found in data/raw/. Skipping.")
        return None

    print(f"Loading {name}...")
    return pd.read_csv(path)

# Clean the nodes file
def preprocess_nodes(nodes_df):
    print("Cleaning nodes...")

    nodes_df = nodes_df.copy()
    nodes_df["Node_ID"] = nodes_df["Node_ID"].astype(int)
    nodes_df["Lon"] = nodes_df["Lon"].astype(float)
    nodes_df["Lat"] = nodes_df["Lat"].astype(float)

    # Tract_Node is optional; keep for now
    return nodes_df

# Clean links and merge with LinkFlows if available
def preprocess_links(links_df, nodes_df, linkflows_df=None):
    print("Cleaning links...")

    links_df = links_df.copy()

    # Fix types
    int_cols = ["Link_ID", "From_Node_ID", "To_Node_ID", "Lanes"]
    for col in int_cols:
        if col in links_df:
            links_df[col] = links_df[col].astype(int)

    float_cols = ["Length", "FreeFlow_Speed", "Free_Speed", "Capacity"]
    for col in float_cols:
        if col in links_df:
            links_df[col] = links_df[col].astype(float)

    # Drop invalid rows
    before = len(links_df)
    links_df = links_df[links_df["Length"] > 0]
    print(f"Removed {before - len(links_df)} links with zero length")

    # Remove edges pointing to missing nodes
    valid_nodes = set(nodes_df["Node_ID"])

    before = len(links_df)
    links_df = links_df[
        links_df["From_Node_ID"].isin(valid_nodes)
        & links_df["To_Node_ID"].isin(valid_nodes)
    ]
    print(f"Removed {before - len(links_df)} links with missing node references")

    # Compute free-flow travel time using the first matching speed column
    speed_col = next((c for c in ["FreeFlow_Speed", "Free_Speed", "Speed", "speed"] if c in links_df), None)
    if speed_col is None:
        raise KeyError("No free-flow speed column found in links data.")
    links_df["freeflow_time"] = links_df["Length"] / links_df[speed_col]


    # Merge congested traffic data if available
    if linkflows_df is not None:
        print("Merging LinkFlows (congested speeds)...")

        if "Link_ID" not in linkflows_df and "ID1" in linkflows_df:
            linkflows_df = linkflows_df.rename(columns={"ID1": "Link_ID"})

        if "Link_ID" not in linkflows_df:
            print("Warning: LinkFlows format unexpected")
        else:
            # Ensure proper types
            linkflows_df["Link_ID"] = linkflows_df["Link_ID"].astype(int)

            # Merge
            links_df = links_df.merge(linkflows_df, on="Link_ID", how="left", suffixes=("", "_flow"))

            # If traversal_time available use it
            if "traversal_time" in links_df.columns:
                links_df["congested_time"] = links_df["traversal_time"]
            else:
                # Prefer explicit time columns, fall back to speed columns
                time_col = next((c for c in ["Max_Time", "AB_Time", "BA_Time"] if c in links_df), None)
                if time_col:
                    links_df["congested_time"] = links_df[time_col]
                else:
                    congested_col = next((c for c in linkflows_df.columns if "Speed" in c or "speed" in c), None)
                    if congested_col:
                        links_df["congested_time"] = links_df["Length"] / links_df[congested_col]
                    else:
                        print("Warning: No congested time column found in LinkFlows --> only free-flow times will be used")

    else:
        print("LinkFlows.csv not provided --> only free flow weights will be available")
        links_df["congested_time"] = links_df["freeflow_time"]

    return links_df

# OD cleaning
def preprocess_od(od_df, nodes_df):
    if od_df is None:
        return None

    print("Cleaning OD matrix...")

    # Map common alternative column names
    col_map = {
        "origin": ["origin", "O_ID", "O", "Origin"],
        "destination": ["destination", "D_ID", "D", "Destination"],
        "trips": ["trips", "OD_Number", "Trips", "Trip"]
    }

    resolved = {}
    for target, options in col_map.items():
        match = next((c for c in options if c in od_df.columns), None)
        if match is None:
            raise KeyError(f"Missing expected OD column for {target}.")
        resolved[target] = match

    # Ensure proper types
    od_df["origin"] = od_df[resolved["origin"]].astype(int)
    od_df["destination"] = od_df[resolved["destination"]].astype(int)
    od_df["trips"] = od_df[resolved["trips"]].astype(float)

    valid_nodes = set(nodes_df["Node_ID"])

    # Remove OD pairs pointing to non-existent nodes
    before = len(od_df)
    od_df = od_df[
        od_df["origin"].isin(valid_nodes)
        & od_df["destination"].isin(valid_nodes)
    ]
    print(f"Removed {before - len(od_df)} OD rows with missing nodes")

    return od_df

# Save cleaned datasets into processed folder
def save_processed(nodes_df, links_df, od_df):
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    nodes_df.to_csv(os.path.join(PROCESSED_DIR, "processed_nodes.csv"), index=False)
    links_df.to_csv(os.path.join(PROCESSED_DIR, "processed_links.csv"), index=False)
    
    if od_df is not None:
        od_df.to_csv(os.path.join(PROCESSED_DIR, "processed_od.csv"), index=False)

    print(f"Saved processed files to {PROCESSED_DIR}")

def run_preprocessing():
    print("Running data preprocessing...")

    # Load datasets
    nodes_df = load_csv("Dallas_node.csv")
    links_df = load_csv("Dallas_link.csv")
    linkflows_df = load_csv("LinkFlows.csv")
    od_df = load_csv("Dallas_od.csv")

    if nodes_df is None or links_df is None:
        print("Error: Missing required node/link files")
        return

    # Process datasets
    nodes_df = preprocess_nodes(nodes_df)
    links_df = preprocess_links(links_df, nodes_df, linkflows_df)
    od_df = preprocess_od(od_df, nodes_df)

    # Save
    save_processed(nodes_df, links_df, od_df)

    print("Preprocessing complete!")

if __name__ == "__main__":
    run_preprocessing()
