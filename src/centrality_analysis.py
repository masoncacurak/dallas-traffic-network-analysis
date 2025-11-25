import os
import pandas as pd
import networkx as nx
from build_network import load_and_build

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")

def _print_top_scores(name, scores, n=10):
    top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    print(f"\nTop {n} nodes by {name} centrality:")
    for rank, (node, value) in enumerate(top, start=1):
        print(f"{rank:2d}. Node {node}: {value:.6f}")

def _print_stats(name, scores):
    values = list(scores.values())
    if not values:
        print(f"No values available for {name} centrality")
        return
    print(
        f"{name.capitalize()} stats -> "
        f"min: {min(values):.6f}, max: {max(values):.6f}, mean: {sum(values)/len(values):.6f}"
    )

# Return degree, betweenness, & eigenvector centrality dictionaries
def compute_centrality_measures(G):
    if G is None or G.number_of_nodes() == 0:
        raise ValueError("Graph is empty or None")

    print("Computing degree centrality...")
    degree_centrality = nx.degree_centrality(G)
    _print_top_scores("degree", degree_centrality)
    _print_stats("degree", degree_centrality)

    print("\nComputing betweenness centrality by weight...")
    betweenness_centrality = nx.betweenness_centrality(G, weight="weight", normalized=True)
    _print_top_scores("betweenness", betweenness_centrality)
    _print_stats("betweenness", betweenness_centrality)

    print("\nComputing eigenvector centrality on largest connected component...")
    # Prefer strongly connected --> fall back on weakly connected
    if nx.is_strongly_connected(G):
        base_component = G
        component_type = "strongly"
    else:
        scc = max(nx.strongly_connected_components(G), key=len)
        if len(scc) > 1:
            base_component = G.subgraph(scc).copy()
            component_type = "strongly"
        else:
            wcc = max(nx.weakly_connected_components(G), key=len)
            base_component = G.subgraph(wcc).copy()
            component_type = "weakly"

    print(f"Using {component_type} connected component with {base_component.number_of_nodes()} nodes")

    # Try a few variants to improve convergence (including unweighted)
    attempts = [
        (base_component, f"{component_type} component (directed)", True),
        (base_component.to_undirected(), f"{component_type} component (undirected)", True),
        (base_component.to_undirected(), f"{component_type} component (undirected, unweighted)", False),
    ]
    if component_type == "weakly":
        attempts.insert(0, (base_component.to_undirected(), "weakly component (undirected)", True))

    eigenvector_centrality = None
    for subgraph, label, use_weight in attempts:
        try:
            eigenvector_centrality = nx.eigenvector_centrality(
                subgraph,
                weight="weight" if use_weight else None,
                max_iter=10000,
                tol=1e-04,
            )
            print(f"Eigenvector converged on {label}{' (weighted)' if use_weight else ''}")
            break
        except nx.PowerIterationFailedConvergence:
            print(f"Eigenvector failed to converge on {label} (max_iter reached)")

    if eigenvector_centrality is None:
        # Final relaxed attempt on undirected graph with higher tolerance
        try:
            print("Final attempt with relaxed tolerance on undirected component...")
            eigenvector_centrality = nx.eigenvector_centrality(
                base_component.to_undirected(), weight=None, max_iter=20000, tol=1e-03
            )
            print("Eigenvector converged with relaxed parameters")
        except nx.PowerIterationFailedConvergence:
            raise RuntimeError("Eigenvector centrality failed to converge after multiple attempts")

    # Map eigenvector scores back to full node set (zeros for excluded nodes)
    full_eigen = {node: 0.0 for node in G.nodes()}
    full_eigen.update(eigenvector_centrality)

    _print_top_scores("eigenvector", full_eigen)
    _print_stats("eigenvector", full_eigen)

    return {
        "degree": degree_centrality,
        "betweenness": betweenness_centrality,
        "eigenvector": full_eigen,
    }

def save_centrality_rankings(centrality_dict, output_path=None):
    if output_path is None:
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        output_path = os.path.join(PROCESSED_DIR, "centrality_rankings.csv")

    df = pd.DataFrame({
        "node_id": list(centrality_dict["degree"].keys()),
        "degree": list(centrality_dict["degree"].values()),
        "betweenness": [centrality_dict["betweenness"].get(n, 0.0) for n in centrality_dict["degree"]],
        "eigenvector": [centrality_dict["eigenvector"].get(n, 0.0) for n in centrality_dict["degree"]],
    })

    df.sort_values(by=["eigenvector", "betweenness", "degree"], ascending=False, inplace=True)
    df.to_csv(output_path, index=False)
    print(f"Centrality rankings saved to {output_path}")
    return output_path

if __name__ == "__main__":
    print("Building graph and computing centrality measures...")
    G = load_and_build(weight_type="congested")
    centrality = compute_centrality_measures(G)
    save_centrality_rankings(centrality)
    print("Centrality analysis complete!")
