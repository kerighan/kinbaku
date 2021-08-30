=========================================
Graph
=========================================

Overview
========
.. currentmodule:: kinbaku
.. autoclass:: Graph

Methods
=======

Adding and removing nodes and edges
-----------------------------------

.. autosummary::
   :toctree: generated/

   Graph.__init__
   Graph.add_node
   Graph.add_edge
   Graph.remove_node
   Graph.remove_edge

Accessing nodes, edges and neighbors
------------------------------------

.. autosummary::
   :toctree: generated/

   Graph.node
   Graph.edge
   Graph.__getitem__
   Graph.__setitem__
   Graph.nodes
   Graph.edges
   Graph.has_node
   Graph.has_edge
   Graph.batch_get_nodes
   Graph.batch_get_edges
   Graph.__contains__
   Graph.neighbors
   Graph.predecessors
   Graph.set_neighbors
   Graph.set_predecessors
   Graph.neighbors_from
   Graph.predecessors_from
   Graph.common_neighbors
   Graph.common_predecessors
   Graph.close


Linear Algebra
--------------

.. autosummary::
   :toctree: generated/

   Graph.adjacency_matrix
   Graph.subgraph
