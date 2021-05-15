import kinbaku as kn
import networkx as nx
from tqdm import tqdm
import random


# utility class for checkups
class EdgeList(object):
    def __init__(self):
        self.item_to_position = {}
        self.items = []

    def add(self, item):
        if item in self.item_to_position:
            return
        self.items.append(item)
        self.item_to_position[item] = len(self.items)-1

    def remove(self, item):
        position = self.item_to_position.pop(item)
        last_item = self.items.pop()
        if position != len(self.items):
            self.items[position] = last_item
            self.item_to_position[last_item] = position

    def sample(self):
        return random.choice(self.items)


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
# CHECK THAT TOMBSTONES CAN BE FOUND
del G
G = kn.Graph("test.db")
G.find_tombstones()
assert len(G.edge_tombstone) != 0

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
