import os
import pandas as pd
import networkx as nx

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")

# Load the cleaned node and link datasets
def load_processed():
    nodes_path = os.path.join(PROCESSED_DIR, "processed_nodes.csv")
    links_path = os.path.join(PROCESSED_DIR, "processed_links.csv")

    if not os.path.exists(nodes_path) or not os.path.exists(links_path):
        raise FileNotFoundError(
            f"Missing processed files --> run preprocessing.py and temporal_preprocessing.py first"
        )

    print("Loading processed_nodes.csv...")
    nodes_df = pd.read_csv(nodes_path)

    print("Loading processed_links.csv...")
    links_df = pd.read_csv(links_path)

    return nodes_df, links_df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Construct directed weighted graph from nodes + links
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
    nodes_df : DataFrame
        Processed nodes containing Node_ID, Lon, Lat

    links_df : DataFrame
        Processed links containing:
        Link_ID, From_Node_ID, To_Node_ID, freeflow_time, congested_time, travel_time_<Period>

    weight_type : str
        "congested" --> use congested_time
        "freeflow" --> use freeflow_time
        Which base weight to use when period is None
    
    period : {"AM", "Midday", "PM", "Evening", None}, default None
        If not None --> use the column `travel_time_<period>` as the weight
        For example: period="AM" -> weight column "travel_time_AM"

    Returns --> G : networkx.DiGraph
"""
def build_graph(nodes_df, links_df, weight_type="congested", period=None):
    
    # Decide which column to use as the edge weight
    if period is not None:
        weight_col = f"travel_time_{period}"
        if weight_col not in links_df.columns:
            raise KeyError(
                f"Requested period='{period}', but column '{weight_col}' was not found in processed_links.csv. "
                "Run temporal_preprocessing.py first or check the period name."
            )
        print(f"Building graph using temporal weight column '{weight_col}'")
    else:
        if weight_type not in {"freeflow", "congested"}:
            raise ValueError("weight_type must be 'freeflow' or 'congested'")

        weight_col = "freeflow_time" if weight_type == "freeflow" else "congested_time"
        print(f"Building graph using {weight_col} as weight")

    G = nx.DiGraph()

    # Add nodes with coordinates
    print(f"Adding {len(nodes_df)} nodes...")
    for _, row in nodes_df.iterrows():
        node_id = int(row["Node_ID"])
        G.add_node(
            node_id,
            lon=float(row["Lon"]),
            lat=float(row["Lat"])
        )

    # Add edges with weights
    print(f"Adding {len(links_df)} edges...")
    missing_weights = 0
    for _, row in links_df.iterrows():
        u = int(row["From_Node_ID"])
        v = int(row["To_Node_ID"])

        # Skip edges whose endpoints are missing just in case
        if u not in G or v not in G:
            continue

        weight = row.get(weight_col, None)

        if weight is None or pd.isna(weight) or weight <= 0:
            missing_weights += 1
            continue

        # Always keep core attributes; weight used by NX algorithms
        G.add_edge(
            u,
            v,
            link_id=int(row["Link_ID"]),
            length=float(row["Length"]),
            freeflow_time=float(row["freeflow_time"]),
            congested_time=float(row["congested_time"]),
            weight=float(weight),
        )

    print(f"Graph build complete: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    if missing_weights > 0:
        print(f"Warning: {missing_weights} edges skipped due to missing/invalid weights")

    return G

# Load processed data -> build graph -> return G
"""
    weight_type : {"freeflow", "congested"}
        Used only when period is None.

    period : {"AM", "Midday", "PM", "Evening", None}
        selects travel_time_<period> as the weight column when not None
    """
def load_and_build(weight_type="congested", period=None):
    nodes_df, links_df = load_processed()
    return build_graph(nodes_df, links_df, weight_type=weight_type, period=period)


if __name__ == "__main__":
    print("Building network graph...")
    G = load_and_build(period="AM") # or period=None, weight_type="congested"
    print("Done!")
