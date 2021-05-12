from dataclasses import dataclass


@dataclass
class Header:
    n_nodes: int
    n_edges: int
    node_id: int
    next_table_position: int
    table_size: int


@dataclass
class Edge:
    is_node: bool = False
    source: int = 0
    target: int = 0
    hash: int = 0
    out_edge_left: int = 0
    out_edge_right: int = 0
    in_edge_left: int = 0
    in_edge_right: int = 0
    type: int = 0


@dataclass
class Node:
    is_node: bool = True
    index: int = 0
    position: int = 0
    left: int = 0
    right: int = 0
    hash: int = 0
    edge_start: int = 0
    key: str = ""
