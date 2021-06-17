import random

import kinbaku as kn
from tqdm import tqdm

# create graph
G = kn.Graph("test.db", flag="n")

# create random connections
N = 20000000
M = 1000 * N
for i in tqdm(range(N), desc="inserting edges"):
    G.add_node(str(i))

# for _ in tqdm(range(M), desc="inserting edges"):
#     u = str(random.randint(0, N-1))
#     v = str(random.randint(0, N-1))
#     G.add_edge(u, v)

# # remove edges in a random order
# nodes = list(G.nodes)
# random.shuffle(nodes)
# for u in tqdm(nodes[:-2], total=G.n_nodes, desc="removing nodes"):
#     G.remove_node(u)

# # check that everything works as expected
# assert len(list(G.nodes)) == 2
# assert G.n_nodes == 2
