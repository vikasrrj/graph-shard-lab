#!/usr/bin/env python3

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"
IMAGES_DIR = ROOT / "docs" / "images"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def generate_locality_chart() -> None:
    rows = read_csv(RESULTS_DIR / "locality_sweep.csv")

    local_edges = [int(row["local_edges"]) for row in rows]
    hash_hops = [float(row["hash_hops"]) for row in rows]
    community_hops = [float(row["community_hops"]) for row in rows]

    plt.figure(figsize=(9, 5.5))

    plt.plot(
        local_edges,
        hash_hops,
        marker="o",
        label="Hash placement",
    )

    plt.plot(
        local_edges,
        community_hops,
        marker="o",
        label="Community placement",
    )

    plt.title("Community Strength vs Cross-Shard Traversal")
    plt.xlabel("Local edges per user out of 8 total edges")
    plt.ylabel("Average cross-shard hops per two-hop query")

    plt.xticks(local_edges)
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = IMAGES_DIR / "locality_sweep.svg"

    plt.savefig(output_path, format="svg")
    plt.close()

    print(f"Created {output_path}")


def generate_batching_chart() -> None:
    rows = read_csv(RESULTS_DIR / "locality_sweep.csv")

    local_edges = [int(row["local_edges"]) for row in rows]

    direct_requests = [float(row["direct_shard_requests"]) for row in rows]

    batched_requests = [float(row["batched_shard_requests"]) for row in rows]

    plt.figure(figsize=(9, 5.5))

    plt.plot(
        local_edges,
        direct_requests,
        marker="o",
        label="Direct execution",
    )

    plt.plot(
        local_edges,
        batched_requests,
        marker="o",
        label="Batched execution",
    )

    plt.title("Direct vs Batched Shard Requests")
    plt.xlabel("Local edges per user out of 8 total edges")
    plt.ylabel("Average logical shard requests per query")

    plt.xticks(local_edges)
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = IMAGES_DIR / "batching_requests.svg"

    plt.savefig(output_path, format="svg")
    plt.close()

    print(f"Created {output_path}")


def generate_batching_by_shards_chart() -> None:
    rows = read_csv(RESULTS_DIR / "batching_sweep.csv")

    series: dict[int, list[tuple[int, float]]] = {}

    for row in rows:
        local_edges = int(row["local_edges_per_user"])
        shard_count = int(row["shard_count"])
        reduction = float(row["request_reduction_percent"])

        series.setdefault(local_edges, []).append((shard_count, reduction))

    plt.figure(figsize=(9, 5.5))

    for local_edges in sorted(series.keys()):
        points = sorted(series[local_edges])
        shard_counts = [shard_count for shard_count, _ in points]
        reductions = [reduction for _, reduction in points]

        plt.plot(
            shard_counts,
            reductions,
            marker="o",
            label=f"{local_edges} local edges",
        )

    plt.title("Batching Benefit vs Shard Count")
    plt.xlabel("Shard count")
    plt.ylabel("Logical shard-request reduction (%)")

    plt.xticks(sorted({int(row["shard_count"]) for row in rows}))
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = IMAGES_DIR / "batching_by_shards.svg"

    plt.savefig(output_path, format="svg")
    plt.close()

    print(f"Created {output_path}")


def generate_cache_baseline_chart() -> None:
    rows = read_csv(RESULTS_DIR / "cache_baseline.csv")

    capacities = [int(row["capacity"]) for row in rows]

    overall_hit_rates = [float(row["hit_rate_percent"]) for row in rows]

    hub_hit_rates = [float(row["hub_hit_rate_percent"]) for row in rows]

    normal_hit_rates = [float(row["normal_hit_rate_percent"]) for row in rows]

    plt.figure(figsize=(9, 5.5))

    plt.plot(
        capacities,
        overall_hit_rates,
        marker="o",
        label="Overall hit rate",
    )

    plt.plot(
        capacities,
        hub_hit_rates,
        marker="o",
        label="Hub hit rate",
    )

    plt.plot(
        capacities,
        normal_hit_rates,
        marker="o",
        label="Normal-user hit rate",
    )

    plt.title("Cold LRU Cache on a Hub-Heavy Workload")
    plt.xlabel("Cache capacity in adjacency lists")
    plt.ylabel("Cache hit rate (%)")

    plt.xticks(capacities)
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = IMAGES_DIR / "cache_baseline.svg"

    plt.savefig(output_path, format="svg")
    plt.close()

    print(f"Created {output_path}")


def generate_cache_warming_chart() -> None:
    rows = read_csv(RESULTS_DIR / "cache_warming.csv")

    capacities = [int(row["capacity"]) for row in rows]

    cold_startup_rates = [float(row["cold_startup_hit_rate_percent"]) for row in rows]

    warmed_startup_rates = [
        float(row["warmed_startup_hit_rate_percent"]) for row in rows
    ]

    plt.figure(figsize=(9, 5.5))

    plt.plot(
        capacities,
        cold_startup_rates,
        marker="o",
        label="Cold cache",
    )

    plt.plot(
        capacities,
        warmed_startup_rates,
        marker="o",
        label="Degree-warmed cache",
    )

    plt.title("Cache Warming During the First 1,000 Reads")
    plt.xlabel("Cache capacity in adjacency lists")
    plt.ylabel("Startup cache hit rate (%)")

    plt.xticks(capacities)
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = IMAGES_DIR / "cache_warming.svg"

    plt.savefig(output_path, format="svg")
    plt.close()

    print(f"Created {output_path}")


def generate_tradeoff_chart() -> None:
    rows = read_csv(RESULTS_DIR / "uneven_communities.csv")

    labels = []
    imbalances = []
    hops = []

    display_names = {
        "hash": "Hash",
        "naive_community": "Naive community",
        "balanced_community": "Balanced community",
    }

    for row in rows:
        labels.append(
            display_names.get(
                row["strategy"],
                row["strategy"],
            )
        )

        imbalances.append(float(row["user_imbalance_percent"]))

        hops.append(float(row["average_cross_shard_hops"]))

    plt.figure(figsize=(9, 5.5))

    plt.scatter(
        imbalances,
        hops,
        s=110,
    )

    for label, imbalance, hop_count in zip(
        labels,
        imbalances,
        hops,
    ):
        plt.annotate(
            label,
            (imbalance, hop_count),
            xytext=(8, 8),
            textcoords="offset points",
        )

    plt.title("Shard Balance vs Graph Locality")
    plt.xlabel("Maximum user imbalance (%)")
    plt.ylabel("Average cross-shard hops per two-hop query")

    plt.grid(alpha=0.3)
    plt.tight_layout()

    output_path = IMAGES_DIR / "uneven_tradeoff.svg"

    plt.savefig(output_path, format="svg")
    plt.close()

    print(f"Created {output_path}")


def main() -> None:
    IMAGES_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    generate_locality_chart()
    generate_batching_chart()
    generate_batching_by_shards_chart()
    generate_cache_baseline_chart()
    generate_cache_warming_chart()
    generate_tradeoff_chart()


if __name__ == "__main__":
    main()
