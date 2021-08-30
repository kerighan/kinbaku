import random
import time

import kinbaku as kn
from tqdm import tqdm

# number of nodes and edges
N = 2000
M = 100 * N

# create graph
G = kn.Graph("test.db", flag="n")
for _ in tqdm(range(M), desc="inserting edges"):
    u = str(random.randint(0, N-1))
    v = str(random.randint(0, N-1))
    G.add_edge(u, v)
del G

# adjacency matrix of the whole graph
start = time.time()
G = kn.Graph("test.db", flag="r")
A, index_to_node = G.adjacency_matrix()
print(time.time() - start)

# adjacency matrix of a subgraph
start = time.time()
G = kn.Graph("test.db", flag="r")
A, index_to_node = G.subgraph([str(i) for i in range(100)])
print(time.time() - start)
