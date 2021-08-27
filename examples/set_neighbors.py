import kinbaku as kn

G = kn.Graph("test.db", flag="n")

G.set_neighbors("A", ["B", "C"])
G.set_neighbors("A", ["A", "E", "D"])
G.set_predecessors("B", ["A", "Z", "W"])

print(list(G.neighbors("A")))
print(list(G.predecessors("E")))
print(list(G.predecessors("B")))
