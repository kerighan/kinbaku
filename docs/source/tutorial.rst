Tutorial
========

This guide can help you start working with NetworkX.

Creating a graph
----------------

Create an empty graph with no nodes and no edges.

.. nbplot::

    >>> import kinbaku as kn
    >>> G = kn.Graph("test.db")

You should see a `test.db` file in your current folder.
A :class:`Graph` is a collection of nodes (vertices) along with
ordered pairs of nodes called edges.  In Kinbaku, nodes can
be any `str` object of `max_key_len` length: for efficiency reasons, Kinbaku
requires you to specify the maximum key length ahead of time.
The current version of Kinbaku only support directed graph.

.. nbplot::

    >>> import kinbaku as kn
    >>> G = kn.Graph("test.db", max_key_len=20)
    >>> G.add_node("a_very_long_node_key")

.. note:: the maximum key length cannot be changed once the graph is created!

Nodes
-----

The graph ``G`` can be grown in several ways.  NetworkX includes many graph
generator functions and facilities to read and write graphs in many formats.
To get started though we'll look at simple manipulations.  You can add one node
at a time.


.. automodule:: kinbaku
   :imported-members:
   :members:
   :undoc-members:
   :show-inheritance: