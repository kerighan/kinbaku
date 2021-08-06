import random

import kinbaku as kn
import time
from tqdm import tqdm

# number of nodes and edges
N = 2000
M = 500 * N

# create graph
G = kn.Graph("test.db", flag="n")
for _ in tqdm(range(M), desc="inserting edges"):
    u = str(random.randint(0, N-1))
    v = str(random.randint(0, N-1))
    G.add_edge(u, v)
del G


start = time.time()
G = kn.Graph("test.db", flag="r")
A, index_to_node = G.adjacency_matrix()
print(time.time() - start)
