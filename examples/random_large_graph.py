import random

import kinbaku as kn
from tqdm import tqdm

G = kn.Graph("test.db", flag="n")

# create random edges
N = 10000  # number of nodes
d = 100  # average degree
for _ in tqdm(range(N * d)):
    u = random.randint(0, N - 1)
    v = random.randint(0, N - 1)
    G.add_edge(str(u), str(v))
