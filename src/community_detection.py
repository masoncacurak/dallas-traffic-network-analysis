import os
import networkx as nx

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")

try:
    import community as community_louvain # python-louvain package
except ImportError:
    community_louvain = None

# Count nodes per community id and return sorted sizes
def _calc_sizes(partition):
    sizes = {}
    for node, cid in partition.items():
        sizes[cid] = sizes.get(cid, 0) + 1
    size_list = sorted(sizes.values(), reverse=True)
    return sizes, size_list

# Run Louvain on G.to_undirected() and optionally run Girvan–Newman on sampled subgraph
def detect_communities(G, run_gn_sample=False, gn_max_nodes=1500):
    if G is None or G.number_of_nodes() == 0:
        raise ValueError("Graph is empty or None")

    undirected = G.to_undirected() # Louvain expects an undirected graph

    # Louvain (primary)
    if community_louvain is None:
        raise ImportError("python-louvain is required for Louvain community detection")

    print("Running Louvain community detection...")
    partition = community_louvain.best_partition(undirected, weight="weight")

    sizes, size_list = _calc_sizes(partition)
    num_communities = len(sizes)

    print(f"Detected {num_communities} communities via Louvain")
    print(f"Largest communities (by size): {size_list[:10]}")

    # Optional Girvan–Newman on sampled subgraph for comparison when graph is large
    if run_gn_sample and undirected.number_of_nodes() > gn_max_nodes:
        print(f"Running Girvan–Newman on a sampled subgraph (top {gn_max_nodes} by degree)...")
        # Sample top-degree nodes to keep GN tractable
        top_nodes = sorted(undirected.degree, key=lambda x: x[1], reverse=True)[:gn_max_nodes]
        sub_nodes = [n for n, _ in top_nodes]
        subgraph = undirected.subgraph(sub_nodes).copy()

        gn_generator = nx.algorithms.community.girvan_newman(subgraph)
        try:
            first_split = next(gn_generator)
            gn_partition = {node: idx for idx, comm in enumerate(first_split) for node in comm}
            _, gn_sizes = _calc_sizes(gn_partition)
            print(f"Girvan–Newman split produced {len(gn_sizes)} communities on the sample. Sizes: {gn_sizes}")
        except StopIteration:
            print("Girvan–Newman didn't produce a split on the sampled subgraph")

    return partition, num_communities, size_list

def save_communities(partition, output_path=None):
    if output_path is None:
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        output_path = os.path.join(PROCESSED_DIR, "communities.csv")

    with open(output_path, "w") as f:
        f.write("node_id,community_id\n")
        for node, cid in partition.items():
            f.write(f"{node},{cid}\n")

    print(f"Community assignments saved to {output_path}")
    return output_path
