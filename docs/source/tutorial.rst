Tutorial
========

This guide can help you start working with Kinbaku.

Creating a graph
----------------

A :class:`Graph` is a collection of nodes (vertices) along with
ordered pairs of nodes called edges. The current version of Kinbaku only support directed graph.

Create an empty graph with no nodes and no edges.

.. nbplot::

    >>> import kinbaku as kn
    >>> G = kn.Graph("test.db", flag="n")

You should see a `test.db` file in your current folder.
The flag parameter can be "r" (read), "w" (write) and "n" (new/empty).

In Kinbaku, nodes can be any `str` object of `max_key_len` length: for efficiency reasons, Kinbaku requires you to specify the maximum key length ahead of time.

.. nbplot::

    >>> import kinbaku as kn
    >>> G = kn.Graph("test.db", max_key_len=20, flag="n")
    >>> G.add_node("a_very_long_node_key")

.. note:: the maximum key length cannot be changed once the graph is created!

Creating nodes and edges
------------------------

Let's start with some simple manipulations.
You can add one node using the following code:

.. nbplot::

    >>> import kinbaku as kn
    >>> G = kn.Graph("test.db", flag="n")
    >>> G.add_node("A")
    >>> G.add_node("B")
    >>> G.add_node("C")

.. note:: The key cannot be changed once the node is created. Keep in mind that strings are the only accepted type for node keys. 

Add an edge from A to B using:

.. nbplot::

    >>> G.add_edge("A", "B")

If the nodes do not exist, they are created.

Accessing nodes and edges
-------------------------

You can iterate through the nodes:

.. nbplot::

    >>> for node in G.nodes:
    >>>     print(node)

and through the edges:

.. nbplot::

    >>> for edge in G.edges:
    >>>     print(edge)

You can get a node using its key:

.. nbplot::

    >>> print(G.node("A"))
    >>> print(G["A"])  # alternatively

and an edge using its endpoints:

.. nbplot::

    >>> print(G.edge("A", "B"))
    >>> print(G["A", "B"])  # alternatively

Checking if a node or an edge exists:

.. nbplot::

    >>> print(G.has_node("A"))  # True
    >>> print(G.has_node("D"))  # False
    >>> print(G.has_edge("A", "B"))  # True

Removing nodes and edges
------------------------

Removing a node can be done in one line:

.. nbplot::

    >>> G.remove_node("C")

This will remove all edges that link to or from C.

Likewise,

.. nbplot::

    >>> G.remove_edge("A", "B")

to remove an edge.
Removing a non-existing edge will throw an exception.


Neighbors and predecessors
--------------------------

Kinbaku uses Networkx conventions: the neighbors of a node A are all the nodes X where A -> X. Predecessors are all the X where X -> A.

.. nbplot::

    >>> G.add_edge("A", "B")
    >>> G.add_edge("A", "C")
    >>> for node in G.neighbors("A"):
    >>>     print(node)  # should print B and C
    >>> for node in G.predecessors("B"):
    >>>     print(node)  # should print A


Custom attributes
-----------------

Each edge and vertex can have custom attributes by inheriting from the :class:`Node` and :class:`Edge` dataclasses.

::

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

    # update data
    G["jack"] = {"name": "Jack C."}
    G["john", "jack"] = {"love": .8}

    # see results
    print(G["john"])
    print(G["jack"])
    print(G["john", "jack"])


.. attention:: updating data using *add_node*, *add_edge* or the dictionary syntax will not update individual fields, but replace the whole content.


Algorithms
----------

Coming soon.
