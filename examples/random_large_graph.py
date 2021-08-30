import random

import kinbaku as kn
from tqdm import tqdm

N = 5000  # number of nodes
d = 50  # average degree

# create random edges
G = kn.Graph("test.db", flag="n")
for _ in tqdm(range(N * d)):
    u = str(random.randint(0, N - 1))
    v = str(random.randint(0, N - 1))
    G.add_edge(u, v)
