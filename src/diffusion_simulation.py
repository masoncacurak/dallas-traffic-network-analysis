import os
import random
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

from build_network import load_and_build

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "../data/processed/")


def choose_seed_nodes(G, k=10, strategy="betweenness"):
    if strategy == "betweenness":
        scores = nx.betweenness_centrality(G, weight="weight", normalized=True)
    elif strategy == "degree":
        scores = dict(G.degree())
    else:
        raise ValueError("strategy must be 'betweenness' or 'degree'")

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    seeds = [n for n, _ in ranked[:k]]
    print(f"Chosen {len(seeds)} seed nodes via {strategy} centrality")
    return seeds


def simulate_sir(G, beta=0.2, gamma=0.05, max_steps=50, seed_nodes=None,
                 rng_seed=0):
    rng = random.Random(rng_seed)

    nodes = list(G.nodes())
    state = {n: "S" for n in nodes}

    if not seed_nodes:
        seed_nodes = rng.sample(nodes, k=min(5, len(nodes)))

    for n in seed_nodes:
        state[n] = "I"

    history = []
    for t in range(max_steps + 1):
        counts = {
            "t": t,
            "S": sum(1 for v in state.values() if v == "S"),
            "I": sum(1 for v in state.values() if v == "I"),
            "R": sum(1 for v in state.values() if v == "R"),
        }
        history.append(counts)

        if counts["I"] == 0:
            print(f"No infected nodes at t={t}, stopping simulation")
            break

        new_state = state.copy()
        # infection
        for u in nodes:
            if state[u] != "I":
                continue
            for v in G.successors(u):
                if state[v] == "S":
                    if rng.random() < beta:
                        new_state[v] = "I"

        # recovery
        for u in nodes:
            if state[u] == "I":
                if rng.random() < gamma:
                    new_state[u] = "R"

        state = new_state

    df = pd.DataFrame(history)
    out_path = os.path.join(PROCESSED_DIR, "diffusion_SIR_timeseries.csv")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"SIR timeseries saved to {out_path}")
    return df


def plot_sir_timeseries(df, output_path=None):
    if output_path is None:
        output_path = os.path.join(PROCESSED_DIR, "diffusion_SIR_example_plot.png")

    plt.figure(figsize=(8, 6))
    plt.plot(df["t"], df["S"], "-o", label="Susceptible")
    plt.plot(df["t"], df["I"], "-s", label="Infected (congested)")
    plt.plot(df["t"], df["R"], "-^", label="Recovered")

    plt.xlabel("Time step")
    plt.ylabel("Number of nodes")
    plt.title("SIR-style congestion diffusion on Dallas network")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    print(f"SIR plot saved to {output_path}")


def main():
    print("Loading AM-period graph for diffusion simulation...")
    G = load_and_build(period="AM")  # uses travel_time_AM as weights
    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    seeds = choose_seed_nodes(G, k=15, strategy="betweenness")
    df = simulate_sir(G, beta=0.15, gamma=0.05, max_steps=60,
                      seed_nodes=seeds, rng_seed=0)
    plot_sir_timeseries(df)


if __name__ == "__main__":
    main()
