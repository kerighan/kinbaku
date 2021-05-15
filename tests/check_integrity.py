from .utils import EdgeList
import kinbaku as kn
from tqdm import tqdm
import random
import networkx as nx
import mmh3
import os


G = kn.Graph("test.db", overwrite=True)
graph = nx.DiGraph()

N = 10000
iterations = 10000
edges = EdgeList()  # not efficient

# =============================================================================
# ADD AND REMOVE EDGES RANDOMLY

# probability of a deletion happening
p_del = .3
for _ in tqdm(range(iterations), desc="random edge insertion and deletion"):
    assert len(graph.edges) == G.n_edges
    assert len(graph.nodes) == G.n_nodes

    draw = random.random()
    if draw > p_del:
        # create a random edge
        u = str(random.randint(0, N-1))
        v = str(random.randint(0, N-1))

        edges.add((u, v))  # this is the reason of the performance drop
        G.add_edge(u, v)
        graph.add_edge(u, v)
    else:
        if len(graph.edges) >= 1:
            u, v = edges.sample()
            G.remove_edge(u, v)
            graph.remove_edge(u, v)
            edges.remove((u, v))


# =============================================================================
# CHECK THAT GRAPHS HAVE SAME NODES AND EDGES

# assert that graphs have the same nodes
assert set(G.nodes) == set(graph.nodes)
assert set(G.edges) == set(graph.edges)

# =============================================================================
# CHECK THAT GRAPHS RETURN SAME NEIGHBORS AND PREDECESSORS

for node in tqdm(G.nodes, total=G.n_nodes):
    G_neighbors = set(G.neighbors(node))
    graph_neighbors = set(graph.neighbors(node))
    assert G_neighbors == graph_neighbors
        
    G_predecessors = set(G.predecessors(node))
    graph_predecessors = set(graph.predecessors(node))
    assert G_predecessors == graph_predecessors
