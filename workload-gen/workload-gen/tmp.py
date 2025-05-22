from itertools import combinations
lst = [
"Inserts",
"Updates",
"Deletes",
"Point Queries",
"Range Queries",
"Empty Point Queries"
]

for i in range(1, 7):
    for comb in combinations(lst, i):
        print("| ", end="")
        print(", ".join(comb), end=" ")
        print("| |")
