from functools import wraps


def compare_nodes(node_A_hash, node_A_key, node_B):
    node_B_key = node_B.key
    if node_B_key < node_A_key:
        return -1
    elif node_B_key > node_A_key:
        return 1
    else:
        return 0


def compare_edges(edge_A, edge_B):
    edge_A_hash = edge_A.hash
    edge_A_source = edge_A.source_position
    edge_A_target = edge_A.target_position
    edge_A_type = edge_A.type

    edge_B_hash = edge_B.hash
    edge_B_source = edge_B.source_position
    edge_B_target = edge_B.target_position
    edge_B_type = edge_B.type

    # edges are equal, return 0
    if (
        edge_A_hash == edge_B_hash and
        edge_A_source == edge_B_source and
        edge_A_target == edge_B_target and
        edge_A_type == edge_B_type
    ):
        return 0

    if edge_B_hash < edge_A_hash:
        return -1
    elif edge_B_hash > edge_A_hash:
        return 1
    else:  # hashes are equal
        if edge_A_source == edge_B_source:
            # case where sources are equal
            if edge_B_target < edge_A_target:
                return -1
            elif edge_B_target > edge_A_target:
                return 1
            elif edge_B_type < edge_A_type:
                return -1
            elif edge_B_type > edge_A_type:
                return 1
            else:
                return 0
        elif edge_B_target < edge_A_target:
            # case where targets are equal
            if edge_B_source < edge_A_source:
                return -1
            elif edge_B_source > edge_A_source:
                return 1
            elif edge_B_type < edge_A_type:
                return -1
            else:
                return 1
        elif edge_B_type < edge_A_type:
            return -1
        else:
            return 1


def to_string(data):
    return u"".join(chr(c) for c in data if c != 0)


def lock(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with args[0].lock:
            return f(*args, **kwargs)
    return wrapper
