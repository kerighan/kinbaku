import kinbaku as kn
from tqdm import tqdm
import networkx as nx
import random


G = kn.Graph("test.db", overwrite=True)

G.add_node("A")
G.add_node("B")
G.add_node("C")
print(G.n_nodes)
print(list(G.nodes))
