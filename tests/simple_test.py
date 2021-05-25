import kinbaku as kn
from tqdm import tqdm
import random


G = kn.Graph("test.db", overwrite=True)

G.add_edge("A", "B")
for edge in G.edges:
    print(edge)
