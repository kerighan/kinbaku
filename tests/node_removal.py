import kinbaku as kn
from tqdm import tqdm
import networkx as nx
import random


# create graph
G = kn.Graph("test.db", overwrite=True)
G_nx = nx.DiGraph()

# create random connections
N = 100
M = 50 * N
for _ in tqdm(range(M)):
    u = str(random.randint(0, N-1))
    v = str(random.randint(0, N-1))
    G.add_edge(u, v)
    G_nx.add_edge(u, v)

# # print()
print(len(list(G.neighbors("0"))))

G.remove_node("0")


print(list(G.neighbors("0")))
print(len(list(G_nx.neighbors("0"))))

# G.add_edge("A", "B")
# print(list(G.neighbors("A")))
# G.remove_edge("A", "B")
# print(list(G.neighbors("A")))


# nodes = list(G.nodes)
# random.shuffle(nodes)
# for u in nodes:
#     G.remove_node(u)
# print(G.n_nodes)
