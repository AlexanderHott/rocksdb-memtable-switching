import os
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

NS_PER_S = 1_000_000_000

# Step 1: Extract and parse data
def parse_results(directory: str):
    results = defaultdict(dict)  # {<workload>: {<options>: <total_time>}}

    for filename in os.listdir(directory):
        if filename.endswith(".results.json"):
            # Extract workload and options from the filename
            parts = filename.split("--")
            workload = parts[0]
            options = parts[1].replace(".results.json", "").replace("options","")

            # Read JSON data and sum the durations
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r') as file:
                data = json.load(file)
                total_time = sum(entry["data"]["duration"] for entry in data if "duration" in entry["data"])

            # Store results
            results[workload][options] = total_time / NS_PER_S
    return results

# Step 2: Plot grouped bar chart
def plot_grouped_bar_chart(results):
    workloads = list(results.keys())
    options = sorted({opt for opts in results.values() for opt in opts})  # All unique options
    x = np.arange(len(workloads))  # Group positions
    bar_width = 0.8 / len(options)  # Width of each bar

    patterns = ['\\\\\\\\', '||||', '----', '....']
    fig, ax = plt.subplots(figsize=(10, 6))

    for i, option in enumerate(options):
        # Gather data for this option across workloads
        times = [results[workload].get(option, 0) for workload in workloads]
        # Bar positions for this option
        bar_positions = x + i * bar_width
        print("times", times)
        bars = ax.bar(bar_positions, times, bar_width, label=option, color='white', edgecolor='black')
        for bar in bars:
            # Ensure that the patterns loop if there are more bars than patterns
            bar.set_hatch(patterns[i])
    # Labels and legends
    ax.set_xlabel("Workloads")
    ax.set_ylabel("Total Latency (s)")
    # ax.set_yscale('log')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2}"))
    ax.set_title("Workload latencies per Memtable")
    ax.set_xticks(x + (len(options) - 1) * bar_width / 2)
    ax.set_xticklabels(workloads)
    ax.legend(title="Memtables")

    plt.tight_layout()
    plt.savefig("workloads.png")
    plt.show()

# Main execution
if __name__ == "__main__":
    directory = "../benchmark-runs/sandbox"  # Replace with the path to your files
    results = parse_results(directory)
    plot_grouped_bar_chart(results)
