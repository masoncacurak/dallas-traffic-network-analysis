# Optimizing Urban Commute Efficiency Through Network Analysis of Dallas Traffic Flows 

## Team Members
- Rahul Juluru
- Mason Cacurak
- Saketh Reddy Aredla

## Motivation and Problem Statement
Effective traffic control is critical in cutting down congestion, time on the road, and pollution in city centrality. In this project we will analyze a real-world traffic dataset that is comprised of floating car data across thousands of road segments in Dallas, TX. We will model this dataset as a directed weighted transportation network where each road segment is an edge between intersections. In looking at Dallas’s traffic as a complex network, we will aim to identify key road segments and congestion communities with the goal of optimizing travel times. Note: As of now, we have not been able to find a sufficient dataset containing temporal data allowing us to center the project around optimizing commute duration based on the time of day. This work examines the questions: "How does traffic congestion spread through a network of highways.” "Which intersections are most critical for maintaining smooth traffic flow.” “Which road segments act as high centrality connectors that most effectively reduce overall commute times?" “Can we identify the main congestion bottlenecks in the traffic network based on the flow and speed data?”

## How to Run
- Prereqs: Run preprocessing.py so data/processed/processed_nodes.csv and processed_links.csv exist
- Run cells in order:
  - Setup/imports
  - Build graph
  - Centrality metrics
  - Community detection
- If you restart the kernel you need to rerun setup and build graph cells before centrality/community cells

## Project Structure
```
.
├── data
│   ├── data_copy
│   ├── processed
│   │   ├── centrality_rankings.csv
│   │   ├── communities.csv
│   │   ├── processed_links.csv
│   │   ├── processed_nodes.csv
│   │   └── processed_od.csv
│   └── raw
├── notebooks
│   └── network_analysis.ipynb
├── src
│   ├── preprocessing.py
│   ├── temporal_preprocessing.py
│   ├── build_network.py
│   ├── centrality_analysis.py
│   └── community_detection.py
├── visuals
│   ├── centrality_maps
│   │   └── ...
│   ├── community_plots
│   │   └── ...
└── README.md
```

- preprocessing.py: Cleans raw Dallas node/link/OD CSVs, computes free flow and congested times, and writes processed datasets to data/processed/
- temporal_preprocessing.py: Synthesize 4 different time of day travel times (am/midday/pm/evening) by interpolating between free flow and congested times then updates processed_links.csv and saves a backup
- build_network.py: Loads processed nodes/links and constructs a directed NetworkX graph, then you can choose base weights (freeflow/congested) or TOD weights via period (am, midday, pm, evening)
- centrality_analysis.py: Computes degree, weighted betweenness, and eigenvector centrality on the graph, prints top node stats, and saves rankings to data/processed/centrality_rankings.csv
- community_detection.py: Runs Louvain community detection (CURRENTLY FAILING) (optional WORKING Girvan–Newman sample), summarizes community sizes, and saves assignments to data/processed/communities.csv
- network_analysis.ipynb: E2E driver notebook that does preprocessing, builds the graph, runs centrality metrics, detects communities, and saves outputs to the processed data folder