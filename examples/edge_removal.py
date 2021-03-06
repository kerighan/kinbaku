import random

import kinbaku as kn
from tqdm import tqdm

# create graph
G = kn.Graph("test.db", flag="n")

# create random connections
N = 10000
M = 500 * N
for _ in tqdm(range(M), desc="inserting edges"):
    u = str(random.randint(0, N-1))
    v = str(random.randint(0, N-1))
    G.add_edge(u, v)

# remove edges in a random order
edges = list(G.edges)
random.shuffle(edges)
for u, v in tqdm(edges, total=G.n_edges, desc="removing edges"):
    G.remove_edge(u, v)

# check that everything works as expected
assert G.n_edges == 0
len(list(G.edges)) == 0
for node in G.nodes:
    assert len(list(G.neighbors(node))) == 0
