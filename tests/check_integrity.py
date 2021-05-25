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


# create graphs
G_kn = kn.Graph("test.db", overwrite=True)
G_nx = nx.DiGraph()

N = 10000
iterations = 30000
edges = EdgeList()  # not efficient

# =============================================================================
# ADD AND REMOVE EDGES RANDOMLY
# probability of a deletion happening
p_edge_del = .3
for _ in tqdm(range(iterations), desc="edge insertion & deletion"):
    assert len(G_nx.edges) == G_kn.n_edges
    assert len(G_nx.nodes) == G_kn.n_nodes

    draw = random.random()
    if draw < p_edge_del and len(G_nx.edges) > 1:
        u, v = edges.sample()
        edges.remove((u, v))

        try:
            G_kn.remove_edge(u, v)
            G_nx.remove_edge(u, v)
        except KeyError:
            assert (u, v) in list(G_kn.edges)
            nns = list(G_kn.neighbors(u))
            assert v in nns
            raise KeyError
    else:
        # create a random edge
        u = str(random.randint(0, N-1))
        v = str(random.randint(0, N-1))

        edges.add((u, v))  # this is the reason of the performance drop
        G_kn.add_edge(u, v)
        G_nx.add_edge(u, v)

# =============================================================================
# CHECK THAT TOMBSTONES CAN BE FOUND
del G_kn
G_kn = kn.Graph("test.db")
G_kn.find_tombstones()
assert len(G_kn.edge_tombstone) != 0

# =============================================================================
# CHECK THAT GRAPHS HAVE SAME NODES AND EDGES
# assert that graphs have the same nodes
assert set(G_kn.nodes) == set(G_nx.nodes)
assert set(G_kn.edges) == set(G_nx.edges)

# =============================================================================
# CHECK THAT GRAPHS RETURN SAME NEIGHBORS AND PREDECESSORS
for node in tqdm(G_kn.nodes, total=G_kn.n_nodes, desc="check neighbors"):
    G_neighbors = set(G_kn.neighbors(node))
    graph_neighbors = set(G_nx.neighbors(node))
    try:
        assert G_neighbors == graph_neighbors
    except AssertionError:
        print(G_neighbors)
        print(graph_neighbors)
        raise AssertionError

    G_predecessors = set(G_kn.predecessors(node))
    graph_predecessors = set(G_nx.predecessors(node))
    assert G_predecessors == graph_predecessors

# =============================================================================
# CHECK THAT PARENTING IN NODES AND EDGES WORK
for key in tqdm(G_kn.nodes, total=G_kn.n_nodes, desc="check node parenting"):
    node = G_kn.node(key)
    if node.parent == 0:
        continue
    parent = G_kn._get_node_at(node.parent)
    cond_left = parent.left == node.position
    cond_right = parent.right == node.position
    assert cond_left or cond_right

for edge in tqdm(
    G_kn._iter_edges(),
    total=G_kn.n_edges, desc="check edge parenting"
):
    parent = G_kn._get_edge_at(edge.in_edge_parent)
    cond_left = parent.in_edge_left == edge.position
    cond_right = parent.in_edge_right == edge.position
    try:
        assert cond_left or cond_right
    except AssertionError:
        print(parent)
        print(edge)

for _ in tqdm(G_kn.edges, total=G_kn.n_edges):
    pass
