from dataclasses import dataclass

import kinbaku as kn


@dataclass(repr=False)
class User(kn.structure.Node):
    name: str = ""
    email: str = ""


@dataclass(repr=False)
class Friendship(kn.structure.Edge):
    love: float = 0.


G = kn.Graph("test.db", node_class=User, edge_class=Friendship, flag="n")

# create nodes
G.add_node("john", attr={"name": "John B.", "email": "john@john.com"})
G["jack"] = {"name": "Jack H."}  # alternatively

# create edge
G.add_edge("john", "jack", attr={"love": .5})
# G["john", "jack"] = {"love": .5} alternatively

# update data
G["jack"] = {"name": "Jack C."}

# see results
print(G["john"])
print(G["jack"])
print(G["john", "jack"])
