.. _graph:

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
   Graph.remove_node
   Graph.add_edge
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
   Graph.__contains__
   Graph.neighbors
   Graph.predecessors
   Graph.neighbors_from
   Graph.predecessors_from