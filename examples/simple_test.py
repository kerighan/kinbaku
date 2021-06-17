import kinbaku as kn

G = kn.Graph("test.db", flag="n")

G.add_edge("A", "B")
G.add_edge("A", "C")
G.add_edge("B", "C")

print(G._get_node_tree_info_at(1))
