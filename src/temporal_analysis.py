
import os
import pandas as pd
import networkx as nx

from build_network import load_and_build

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")


def _largest_weak_component(G: nx.DiGraph) -> nx.DiGraph:
    """Return largest weakly connected component as a subgraph."""
    if G.number_of_nodes() == 0:
        return G
    wcc = max(nx.weakly_connected_components(G), key=len)
    return G.subgraph(wcc).copy()


def compute_temporal_metrics(period: str) -> dict:
    """
    Build graph for a given time period and compute summary metrics.
    """
    # uses travel_time_<period> as edge weight
    G = load_and_build(period=period)
    print(f"[{period}] Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    n = G.number_of_nodes()
    m = G.number_of_edges()
    avg_out_degree = m / n if n > 0 else 0.0

    # undirected view for clustering etc.
    und = G.to_undirected()

    # largest weakly connected component for path metrics
    gcc = _largest_weak_component(G).to_undirected()

    try:
        avg_clustering = nx.average_clustering(und, weight=None)
    except Exception:
        avg_clustering = float("nan")

    try:
        if gcc.number_of_nodes() > 1:
            avg_shortest_path = nx.average_shortest_path_length(gcc, weight="weight")
        else:
            avg_shortest_path = float("nan")
    except Exception:
        avg_shortest_path = float("nan")

    try:
        if gcc.number_of_nodes() > 1:
            diameter = nx.diameter(gcc)
        else:
            diameter = float("nan")
    except Exception:
        diameter = float("nan")

    metrics = {
        "period": period,
        "num_nodes": n,
        "num_edges": m,
        "avg_out_degree": avg_out_degree,
        "avg_clustering": avg_clustering,
        "avg_shortest_path_weighted": avg_shortest_path,
        "diameter_gcc": diameter,
    }

    print(f"[{period}] metrics: {metrics}")
    return metrics


def main():
    periods = ["AM", "Midday", "PM", "Evening"]

    rows = []
    for p in periods:
        rows.append(compute_temporal_metrics(p))

    df = pd.DataFrame(rows)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    out_path = os.path.join(PROCESSED_DIR, "temporal_metrics_summary.csv")
    df.to_csv(out_path, index=False)
    print(f"Temporal metrics summary saved to {out_path}")


if __name__ == "__main__":
    main()
