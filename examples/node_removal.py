import kinbaku as kn
from tqdm import tqdm
import random


# create graph
G = kn.Graph("test.db", flag="n")

# create random connections
N = 100
M = 500 * N
for _ in tqdm(range(M), desc="inserting edges"):
    u = str(random.randint(0, N-1))
    v = str(random.randint(0, N-1))
    G.add_edge(u, v)

# remove edges in a random order
nodes = list(G.nodes)
random.shuffle(nodes)
for u in tqdm(nodes, total=G.n_nodes, desc="removing nodes"):
    G.remove_node(u)

# check that everything works as expected
assert G.n_nodes == 0
