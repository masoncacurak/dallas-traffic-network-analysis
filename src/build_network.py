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
            f"Missing processed files --> run preprocessing.py first."
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
        Link_ID, From_Node_ID, To_Node_ID,
        freeflow_time, congested_time

    weight_type : str
        "congested" --> use congested_time
        "freeflow" --> use freeflow_time

    Returns --> G : networkx.DiGraph
"""
def build_graph(nodes_df, links_df, weight_type="congested"):
    print(f"Building NetworkX graph using {weight_type} weight...")

    if weight_type not in ["freeflow", "congested"]:
        raise ValueError("weight_type must be 'freeflow' or 'congested'")

    weight_col = "congested_time" if weight_type == "congested" else "freeflow_time"

    G = nx.DiGraph()

    # Add nodes with coordinates
    print(f"Adding {len(nodes_df)} nodes...")
    for _, row in nodes_df.iterrows():
        G.add_node(
            int(row["Node_ID"]),
            lon=float(row["Lon"]),
            lat=float(row["Lat"])
        )

    # Add edges with weights
    print(f"Adding {len(links_df)} edges...")

    missing_weights = 0

    for _, row in links_df.iterrows():
        u = int(row["From_Node_ID"])
        v = int(row["To_Node_ID"])

        weight = row.get(weight_col, None)

        if weight is None or weight <= 0:
            missing_weights += 1
            continue

        G.add_edge(
            u,
            v,
            link_id=int(row["Link_ID"]),
            length=float(row["Length"]),
            freeflow_time=float(row["freeflow_time"]),
            congested_time=float(row["congested_time"]),
            weight=float(weight), # used by NX algorithms
        )

    print(f"Graph build complete: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    if missing_weights > 0:
        print(f"Warning: {missing_weights} edges skipped due to missing/invalid weights")

    return G

# Load processed data -> build graph -> return G
def load_and_build(weight_type="congested"):
    nodes_df, links_df = load_processed()
    return build_graph(nodes_df, links_df, weight_type=weight_type)


if __name__ == "__main__":
    print("Building network graph...")
    G = load_and_build(weight_type="congested")
    print("Done!")
