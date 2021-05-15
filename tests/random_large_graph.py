import kinbaku as kn
from tqdm import tqdm
import random


G = kn.Graph("test.db", overwrite=True)

# create random edges
N = 1000000  # number of nodes
d = 50  # average degree
for _ in tqdm(range(N * d)):
    u = random.randint(0, N - 1)
    v = random.randint(0, N - 1)
    G.add_edge(u, v)
