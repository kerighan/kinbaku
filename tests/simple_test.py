import kinbaku as kn
from tqdm import tqdm
import networkx as nx
import random


G = kn.Graph("test.db", overwrite=True)
G_nx = nx.DiGraph()

N = 100
M = 10 * N

# G.add_edge("0", "1")
# print(G.n_edges)
# G.add_edge("0", "1")
# print(G.n_edges)
edges = []


for _ in tqdm(range(M)):
    u = str(random.randint(0, N-1))
    v = str(random.randint(0, N-1))
    print(G.n_edges, len(G_nx.edges))

    if G_nx.has_edge(u, v):
        nns = list(G_nx.predecessors(v))
        print(nns)
        nns = list(G.predecessors(v))
        print(nns)
        print(u, v)
        assert u in nns
    # print((u, v) in edges)

    G.add_edge(u, v)
    G_nx.add_edge(u, v)
    edges.append((u, v))

    print(G.n_edges, len(G_nx.edges))
    print()

    assert len(G_nx.nodes) == G.n_nodes
    assert len(G_nx.edges) == G.n_edges
