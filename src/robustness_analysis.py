#!/usr/bin/env python3
"""
robustness_analysis.py

Simulate random failures and targeted attacks on the Dallas traffic network.

Outputs:
  - robustness_random.csv
  - robustness_targeted_betweenness.csv
  - robustness_targeted_degree.csv
  - robustness_robustness_curves.png
"""

import os
import random
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

from build_network import load_and_build  # uses processed_nodes/links.csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")

# ------------- helpers -------------

def _largest_weakly_component(G):
    if G.number_of_nodes() == 0:
        return G
    comps = list(nx.weakly_connected_components(G))
    largest = max(comps, key=len)
    return G.subgraph(largest).copy()


def _compute_robustness_metrics(G):
    """
    Metrics:
      - frac_nodes_in_GCC: |GCC| / |V|
      - efficiency: global efficiency on undirected graph
      - avg_shortest_path: on GCC (if possible)
    """
    n = G.number_of_nodes()
    if n == 0:
        return {
            "frac_nodes_in_GCC": 0.0,
            "efficiency": 0.0,
            "avg_shortest_path": np.nan,
        }

    gcc = _largest_weakly_component(G)
    frac_gcc = gcc.number_of_nodes() / n

    # Use undirected for efficiency & path length
    und = G.to_undirected()
    und_gcc = gcc.to_undirected()

    try:
        eff = nx.global_efficiency(und_gcc)
    except Exception:
        eff = 0.0

    try:
        if und_gcc.number_of_nodes() > 1:
            asp = nx.average_shortest_path_length(und_gcc, weight="weight")
        else:
            asp = np.nan
    except Exception:
        asp = np.nan

    return {
        "frac_nodes_in_GCC": frac_gcc,
        "efficiency": eff,
        "avg_shortest_path": asp,
    }


# ------------- random failure -------------

def simulate_random_failure(G, fractions=None, seed=0):
    if fractions is None:
        fractions = np.linspace(0, 1.0, 21)  # 0%, 5%, ..., 100%

    rng = random.Random(seed)

    all_nodes = list(G.nodes())
    n_total = len(all_nodes)
    rng.shuffle(all_nodes)

    results = []
    G_work = G.copy()

    remove_order = list(all_nodes)
    current_removed = set()

    for f in fractions:
        target_remove = int(round(f * n_total))
        while len(current_removed) < target_remove and remove_order:
            node = remove_order.pop()
            if node in G_work:
                G_work.remove_node(node)
                current_removed.add(node)

        metrics = _compute_robustness_metrics(G_work)
        metrics.update({
            "fraction_removed": f,
            "num_removed": len(current_removed)
        })
        results.append(metrics)

        print(f"[Random] removed {len(current_removed)}/{n_total} nodes "
              f"({f:.2f}) -> GCC={metrics['frac_nodes_in_GCC']:.3f}, "
              f"eff={metrics['efficiency']:.3f}")

    df = pd.DataFrame(results)
    out_path = os.path.join(PROCESSED_DIR, "robustness_random.csv")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Random failure robustness saved to {out_path}")
    return df


# ------------- targeted attack -------------

def _rank_nodes(G, strategy="betweenness"):
    if strategy == "betweenness":
        scores = nx.betweenness_centrality(G, weight="weight", normalized=True)
    elif strategy == "degree":
        scores = dict(G.degree())
    else:
        raise ValueError("strategy must be 'betweenness' or 'degree'")

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [n for n, _ in ranked]


def simulate_targeted_attack(G, strategy="betweenness", fractions=None):
    if fractions is None:
        fractions = np.linspace(0, 1.0, 21)

    all_nodes = _rank_nodes(G, strategy=strategy)
    n_total = len(all_nodes)

    results = []
    G_work = G.copy()
    current_removed = set()

    for f in fractions:
        target_remove = int(round(f * n_total))
        while len(current_removed) < target_remove and all_nodes:
            node = all_nodes.pop(0)
            if node in G_work:
                G_work.remove_node(node)
                current_removed.add(node)

        metrics = _compute_robustness_metrics(G_work)
        metrics.update({
            "fraction_removed": f,
            "num_removed": len(current_removed)
        })
        results.append(metrics)

        print(f"[Targeted-{strategy}] removed {len(current_removed)}/{n_total} nodes "
              f"({f:.2f}) -> GCC={metrics['frac_nodes_in_GCC']:.3f}, "
              f"eff={metrics['efficiency']:.3f}")

    df = pd.DataFrame(results)
    out_path = os.path.join(PROCESSED_DIR, f"robustness_targeted_{strategy}.csv")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Targeted attack robustness ({strategy}) saved to {out_path}")
    return df


# ------------- plotting -------------

def plot_robustness_curves(df_random, df_target_betw, df_target_deg,
                           output_path=None):
    if output_path is None:
        output_path = os.path.join(PROCESSED_DIR, "robustness_curves.png")

    plt.figure(figsize=(8, 6))

    for df, label, style in [
        (df_random, "Random failure", "-o"),
        (df_target_betw, "Targeted (betweenness)", "-s"),
        (df_target_deg, "Targeted (degree)", "-^"),
    ]:
        plt.plot(
            df["fraction_removed"],
            df["frac_nodes_in_GCC"],
            style,
            label=label,
            linewidth=1.5,
            markersize=5,
        )

    plt.xlabel("Fraction of nodes removed")
    plt.ylabel("Fraction of nodes in GCC")
    plt.title("Network robustness: GCC size vs node removal")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    print(f"Robustness curves figure saved to {output_path}")


# ------------- main -------------

def main():
    print("Loading congested-time graph for robustness analysis...")
    G = load_and_build(weight_type="congested", period=None)
    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    fractions = np.linspace(0, 1.0, 21)

    df_random = simulate_random_failure(G, fractions=fractions, seed=0)
    df_target_b = simulate_targeted_attack(G, strategy="betweenness",
                                           fractions=fractions)
    df_target_d = simulate_targeted_attack(G, strategy="degree",
                                           fractions=fractions)

    plot_robustness_curves(df_random, df_target_b, df_target_d)


if __name__ == "__main__":
    main()
