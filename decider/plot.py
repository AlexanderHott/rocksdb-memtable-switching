import matplotlib.pyplot as plt
from statistics import mean

vector_insert = [
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113620.0,
    10113630.0,
]

vector_50_50 = [
    724.0,
    743.0,
    683.0,
    710.0,
    714.0,
    682.0,
    717.0,
    713.0,
    703.0,
    668.0,
    706.0,
    704.0,
]

hash_linklist_insert = [
    862223.0,
    872680.0,
    862534.0,
    862534.0,
]

hash_linklist_50_50 = [
    1961857.0,
    1921236.0,
    2022743.0,
    1927309.0,
]

switching_insert = [
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113630.0,
    10113620.0,
    10113630.0,
]

switching_50_50 = [
    1961857.0,
    1921236.0,
    2022743.0,
    1927309.0,
]

fig, axs = plt.subplots(1, 3, figsize=(12, 4), sharey=True)

values_inserts = [
    mean(vector_insert),
    mean(hash_linklist_insert),
    mean(switching_insert)
]

values_50_50 = [
    mean(vector_50_50),
    mean(hash_linklist_50_50),
    mean(switching_50_50)
]
categories = [
    "Vector",
    "Hash Linklist",
    "Dynamic",
]

for i, ax in enumerate(axs):
    ax.set_title(categories[i])
    if i == 0:
        ax.set_ylabel("Operations/30s")
    ax.bar(['50% Inserts 50% Point Queries', '100% Inserts'], [values_50_50[i], values_inserts[i]], color=['blue', 'green'])
    ax.semilogy()
    # ax.set_ylim(0, 40)  # Set the y-axis limit

fig.suptitle("Mean Memory Buffer Throughput for a Dynamic Workload")

plt.tight_layout()
plt.savefig("plot.png", dpi=800, transparent=True)
# plt.show()