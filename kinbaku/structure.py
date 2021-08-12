from dataclasses import dataclass, field


@dataclass
class Header:
    n_nodes: int
    n_edges: int
    node_id: int
    next_table_position: int
    table_size: int
    class_length: int


@dataclass
class Edge:
    is_node: bool = False
    exists: bool = True
    is_edge_start: bool = False
    position: int = field(default=0, compare=False)
    source_position: int = field(default=0, compare=False)
    target_position: int = field(default=0, compare=False)
    hash: int = 0
    out_edge_left: int = field(default=0, compare=False)
    out_edge_right: int = field(default=0, compare=False)
    out_edge_parent: int = field(default=0, compare=False)
    in_edge_left: int = field(default=0, compare=False)
    in_edge_right: int = field(default=0, compare=False)
    in_edge_parent: int = field(default=0, compare=False)
    type: int = 0

    def __repr__(self):
        txt = f"{self.__class__.__name__}("
        attr = []
        for key, val in vars(self).items():
            if key in {
                "is_node", "exists", "is_edge_start", "position",
                "source_position", "target_position", "hash", "out_edge_left",
                "out_edge_parent", "out_edge_right", "in_edge_left",
                "in_edge_right", "in_edge_parent", "type"
            }:
                continue
            if isinstance(val, str) and len(val) == 0:
                continue
            attr.append(f"{key}={val}")
        txt += ", ".join(attr)
        txt += ")"
        return txt

    def data(self):
        res = {}
        for key, val in vars(self).items():
            if key in {
                "is_node", "exists", "is_edge_start", "position",
                "source_position", "target_position", "hash", "out_edge_left",
                "out_edge_parent", "out_edge_right", "in_edge_left",
                "in_edge_right", "in_edge_parent", "type"
            }:
                continue
            if isinstance(val, str) and len(val) == 0:
                continue
            res[key] =val
        return res

@dataclass
class Node:
    is_node: bool = True
    exists: bool = True
    hash: int = 0
    left: int = field(default=0, compare=False)
    right: int = field(default=0, compare=False)
    index: int = field(default=0, compare=False)
    position: int = field(default=0, compare=False)
    parent: int = field(default=0, compare=False)
    edge_start: int = field(default=0, compare=False)
    key: str = ""

    def __repr__(self):
        txt = f"{self.__class__.__name__}("
        attr = []
        for key, val in vars(self).items():
            if key in {
                "is_node", "exists", "hash", "left", "right", "index",
                "position", "parent", "edge_start"
            }:
                continue
            if isinstance(val, str) and len(val) == 0:
                continue
            attr.append(f"{key}={val}")
        txt += ", ".join(attr)
        txt += ")"
        return txt
    
    def data(self):
        res = {}
        for key, val in vars(self).items():
            if key in {
                "is_node", "exists", "hash", "left", "right", "index",
                "position", "parent", "edge_start"
            }:
                continue
            if isinstance(val, str) and len(val) == 0:
                continue
            res[key] =val
        return res


@dataclass
class text:
    length: int = 0

    def __init__(self, length: int):
        self.length = length
