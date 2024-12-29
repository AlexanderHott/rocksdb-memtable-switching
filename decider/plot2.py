import matplotlib.pyplot as plt

def read_file(path: str) -> list[float]:
    nums = []
    with open (path) as f:
        lines = f.readlines()
        for line in lines:
            nums.append(float(line.strip())/5000.0)
    return nums

plt.semilogy(read_file("./presentation/1m_i-vector_16-1.csv"), label="vector 2^16",color="purple")
plt.semilogy(read_file("./presentation/1m_i-vector_16-2.csv"), label="vector 2^16 (2)",color="purple")

plt.semilogy(read_file("./presentation/1m_i-vector_20-1.csv"), label="vector 2^20",color="green")
plt.semilogy(read_file("./presentation/1m_i-vector_20-2.csv"), label="vector 2^20 (2)",color="green")

plt.semilogy(read_file("./presentation/1m_i-vector_24-1.csv"), label="vector 2^24",color="blue")
plt.semilogy(read_file("./presentation/1m_i-vector_24-2.csv"), label="vector 2^24 (2)",color="blue")


plt.semilogy(read_file("./presentation/1m_i-skiplist_20-1.csv"), label="skiplist 2^20",color="orange")
plt.semilogy(read_file("./presentation/1m_i-skiplist_24-1.csv"), label="skiplist 2^24",color="yellow")
# plt.semilogy(read_file("./presentation/out.csv"), label="out",color="red")


# 900k insert, 100k point query
# plt.semilogy(read_file("./presentation/900k_i-100k_pq-vector_24-1.csv"), label="vector 2^24 (2)",color="blue")


# plt.plot(read_file("./presentation/1m_i-skiplist_20.csv"), label="skiplist 2^20",color="orange")
# plt.plot(read_file("./presentation/1m_i-skiplist_20-2.csv"), label="skiplist 2^20",color="orange")

# plt.plot(read_file("./presentation/1m_i-vector_24.csv"), label="vector 2^24",color="red")
# plt.plot(read_file("./presentation/1m_i-vector_24-2.csv"), label="vector 2^24 (2)",color="red")


plt.ylim(bottom=1)
plt.legend()
plt.show()

plt.clf()
plt.cla()

