# A graph database in a single file

_Kinbaku_ is the japanese art of bondage. In a graph, ropes are replaced by edges between nodes. This library allows you to manage large graphs on disk, using only one file and without having to load the whole graph in memory. The library is written in pure Python.

- **Source:** https://github.com/kerighan/kinbaku
- **Documentation:** https://kinbaku.readthedocs.io
- **Tutorial:** https://kinbaku.readthedocs.io/en/latest/tutorial.html

## Installation

It is recommended to install the cityhash package:
```
pip install cityhash
```
To install Kinbaku:
```
pip install kinbaku
```

## Basic usage

```python
import kinbaku as kn

# create graph if the file doesn't already exist
G = kn.Graph("graph.db")

# add nodes
G.add_node("A")  # keys must be strings
G.add_node("B")
G.add_node("C")

# add edges
G.add_edge("A", "B")
G.add_edge("A", "C")

# get a node
print(G.node("A"))
print(G["A"])

# get out neighbors
print(list(G.neighbors("A")))

# get incoming nodes
print(list(G.predecessors("B")))

# iterating through the nodes
for node in G.nodes:
    print(node)

# iterating through the edges
for edge in G.edges:
    print(edge)
```

Node keys must imperatively be strings with a maximum length. The maximum length can be set *before* the graph is created using the _max_key_len_ keyword argument.

## Using custom attributes

With Kinbaku, nodes and edges can have custom attributes. The way to proceed is to create Python dataclasses that inherit from Kinbaku structures.

```python
from dataclasses import dataclass
import kinbaku as kn


@dataclass
class User(kn.structure.Node):
    age: int = 0
    bio: str = ""


@dataclass
class Relation(kn.structure.Edge):
    weight: float = 0


G = kn.Graph("graph_with_attributes.db",
             node_class=User,
             edge_class=Relation,
             max_str_len=40)  # max string length

# using the 'add_node' method:
G.add_node("Mark", {"age": 25, "bio": "first text"})
# or using '__setitem__':
G["Mary"] = {"age": 32, "bio": "second text"}

# adding an edge with custom attributes:
G.add_edge("Mark", "Mary", {"weight": .1})
```
