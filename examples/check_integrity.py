import kinbaku as kn
import networkx as nx
from tqdm import tqdm
import random


# create graphs
G_kn = kn.Graph("test.db", flag="n")
G_nx = nx.DiGraph()

N = 10000
iterations = 100000
nodes = set()
edges = set()

# =============================================================================
# ADD AND REMOVE EDGES RANDOMLY
# probability of a deletion happening
p_edge_del = .2
p_node_del = .25
for _ in tqdm(range(iterations), desc="edge insertion & deletion"):
    assert len(G_nx.edges) == G_kn.n_edges
    assert len(G_nx.nodes) == G_kn.n_nodes

    draw = random.random()
    if draw < p_edge_del and len(G_nx.edges) > 1:
        u, v = random.choice(list(edges))
        edges.remove((u, v))

        try:
            G_nx.remove_edge(u, v)
            G_kn.remove_edge(u, v)
        except nx.exception.NetworkXError:
            pass
    elif draw < p_node_del + p_edge_del and len(G_nx.nodes) > 1:
        u = random.choice(list(nodes))
        G_nx.remove_node(u)
        G_kn.remove_node(u)
        nodes.remove(u)
    else:
        # create a random edge
        u = str(random.randint(0, N-1))
        v = str(random.randint(0, N-1))

        nodes.update({u, v})
        edges.add((u, v))
        G_nx.add_edge(u, v)
        G_kn.add_edge(u, v)

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

G_kn.empty_cache()
for _ in tqdm(G_kn.edges, total=G_kn.n_edges, desc="iterating through edges"):
    pass

for _ in tqdm(G_kn.nodes, total=G_kn.n_nodes, desc="iterating through nodes"):
    pass