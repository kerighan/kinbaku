import kinbaku as kn
from tqdm import tqdm
import random


G = kn.Graph("test.db", overwrite=True)

# create random edges
N = 5000  # number of nodes
d = 50  # average degree
for _ in tqdm(range(N * d)):
    u = random.randint(0, N - 1)
    v = random.randint(0, N - 1)
    G.add_edge(u, v)

for key in G.nodes:
    node = G.node(key)
    if node.parent == 0:
        continue
    parent = G._get_node_at(node.parent)
    cond_left = parent.left == node.position
    cond_right = parent.right == node.position
    assert cond_left or cond_right

# nodes = list(G.nodes)
# for node in tqdm(nodes):
#     G.remove_node(node)
