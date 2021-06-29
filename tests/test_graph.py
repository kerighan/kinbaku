import os
import random

import kinbaku as kn
import networkx as nx
import pytest


@pytest.fixture()
def G():
    graph = kn.Graph("test.db", flag="n")
    yield graph

    graph.close()
    del graph
    os.remove("test.db")


@pytest.fixture()
def nodes(N):
    return [f"node_{i}" for i in range(N)]


@pytest.fixture()
def G_nx(N, M, nodes):
    graph = nx.DiGraph()
    for _ in range(M):
        u = nodes[random.randint(0, N-1)]
        v = nodes[random.randint(0, N-1)]
        graph.add_edge(u, v)
    return graph


def test_add_nodes(G, N, nodes):
    for node in nodes:
        G.add_node(node)

    assert N == G.n_nodes
    for i in range(N):
        assert G.has_node(nodes[i])
        assert nodes[i] in G


def test_neighbors(G, G_nx, nodes):
    for u, v in G_nx.edges:
        G.add_edge(u, v)

    for u in G_nx.nodes:
        assert u in G

    for u, v in G.edges:
        assert G_nx.has_edge(u, v)
        assert G.has_edge(u, v)
        assert (u, v) in G

    for node in nodes:
        neighbors_found = set(G.neighbors(node))
        neighbors_true = set(G_nx.neighbors(node))
        assert neighbors_found == neighbors_true

        predecessors_found = set(G.predecessors(node))
        predecessors_true = set(G_nx.predecessors(node))
        assert predecessors_found == predecessors_true
