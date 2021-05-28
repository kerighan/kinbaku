import kinbaku as kn
from tqdm import tqdm
import networkx as nx
import random


G = kn.Graph("test.db", flag="n")

G.add_edge("A", "B")
# G.add_edge("A", "C")
# G.add_edge("B", "C")
# print(G.n_nodes)
print(list(G.nodes))
# print(list(G.neighbors("A")))
# print(list(G.predecessors("B")))
# print()
# G.remove_node("A")
# print(G.n_nodes)
# print(list(G.predecessors("B")))
# print(list(G.predecessors("C")))
# print(list(G.neighbors("A")))
print(G._get_node_at(0))
