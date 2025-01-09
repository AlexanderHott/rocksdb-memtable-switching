import json
import matplotlib.pyplot as plt


def graph_db_operation_results(json_data):
    # Parse the JSON data
    results = [entry for entry in json_data if entry.get("type") == "DBOperationResult"]
    print("got results", len(results))

    # Extract operations and durations
    # operations = [result["data"]["operation"] for result in results]
    durations = [result["data"]["duration"] for result in results]
    print("got operations and durations", len(durations))

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.scatter([i for i in range(len(durations))], durations)

    # plt.bar(operations, durations, color='skyblue')
    print("plotted")

    # Add labels and title
    plt.xlabel("Operation")
    plt.ylabel("Duration (ns)")
    plt.yscale('log')
    plt.title("Database Operation Durations")
    plt.xticks(rotation=45)
    print("showing")

    # Show the plot
    plt.tight_layout()
    plt.show()

print("loading data")
data = json.load(open("../benchmark/..-workload-gen-workloads-10m_i.txt-.-options.json.json"))
print("plotting data")
graph_db_operation_results(data)