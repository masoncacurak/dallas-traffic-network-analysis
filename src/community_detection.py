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

# Run Louvain on G.to_undirected()
def detect_communities(G):
    if G is None or G.number_of_nodes() == 0:
        raise ValueError("Graph is empty or None")

    undirected = G.to_undirected() # Louvain expects an undirected graph

    # Louvain
    if community_louvain is None:
        raise ImportError("python-louvain is required for Louvain community detection")

    print("Running Louvain community detection...")
    partition = community_louvain.best_partition(undirected, weight="weight")

    sizes, size_list = _calc_sizes(partition)
    num_communities = len(sizes)

    print(f"Detected {num_communities} communities via Louvain")
    print(f"Total communities: {num_communities} --> top sizes: {size_list[:10]}")

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
