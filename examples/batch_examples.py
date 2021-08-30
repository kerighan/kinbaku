import kinbaku as kn
import networkx as nx

G = kn.Graph("test.db", flag="n")
G_nx = nx.cycle_graph(100)

for u, v in G_nx.edges:
    u = str(u)
    v = str(v)
    G.add_edge(u, v)

batch_size = 100
edges, cursor = G.batch_get_edges(batch_size=batch_size, cursor=0)
while cursor != -1:
    batch_edges, cursor = G.batch_get_edges(
        batch_size=batch_size, cursor=cursor)
    edges.extend(batch_edges)

edges = {(int(u), int(v)) for u, v in edges}
true_edges = set(G_nx.edges)
print(true_edges == edges)


nodes, cursor = G.batch_get_nodes(batch_size=batch_size, cursor=0)
while cursor != -1:
    batch_nodes, cursor = G.batch_get_nodes(
        batch_size=batch_size, cursor=cursor)
    nodes.extend(batch_nodes)

nodes = {int(node.key) for node in nodes}
true_nodes = set(G_nx.nodes)
print(nodes == true_nodes)
