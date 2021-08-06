import random
import time

import kinbaku as kn
from tqdm import tqdm

# create random edges
N = 2000  # number of nodes
d = 1000  # average degree

# G = kn.Graph("test.db", flag="n")
# for _ in tqdm(range(N * d)):
#     u = str(random.randint(0, N - 1))
#     v = str(random.randint(0, N - 1))
#     G.add_edge(u, v)
# del G

G = kn.Graph("test.db", flag="r")
# get neighbors sequentially
for node in tqdm(range(N)):
    print(len(list(G.neighbors(str(node)))))
del G

G = kn.Graph("test.db", flag="r")
start_time = time.time()
# multiprocessing neighbors: useful when degree is high
G.neighbors_from([str(i) for i in range(N)])
elapsed = time.time() - start_time
print(f"{elapsed:.2f}s", "multi")
del G
