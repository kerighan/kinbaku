import kinbaku as kn
from tqdm import tqdm

G = kn.Graph("test.db", flag="n")

# create random edges
N = 200000  # number of nodes
for i in tqdm(range(N)):
    G.add_node(str(i))

for i in tqdm(range(N)):
    G.add_edge("0", str(i))

for i in tqdm(range(1)):
    list(G.neighbors("0"))
