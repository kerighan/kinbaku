def compare_nodes(node_A_hash, node_A_key, node_B):
    node_B_hash = node_B.hash
    if node_B_hash < node_A_hash:
        return -1
    elif node_B_hash > node_A_hash:
        return 1
    else:  # hashes are equal
        node_B_key = node_B.key
        if node_B_key < node_A_key:
            return -1
        elif node_B_key > node_A_key:
            return 1
        else:
            return 0


def compare_edges(A, B):
    A_hash = A.hash
    B_hash = B.hash
    if B_hash < A_hash:
        return -1
    elif B_hash > A_hash:
        return 1
    else:  # hashes are equal
        A_source, A_target, A_type = (
            A.source_position, A.target_position, A.type)
        B_source, B_target, B_type = (
            B.source_position, B.target_position, B.type)

        # edges are equal, return 0
        if (
            A_source == B_source and
            A_target == B_target and
            A_type == B_type
        ):
            return 0
        if A_source == B_source:
            # case where sources are equal
            if B_target < A_target:
                return -1
            elif B_target > A_target:
                return 1
            elif B_type < A_type:
                return -1
            elif B_type > A_type:
                return 1
            else:
                return 0
        elif B_target < A_target:
            # case where targets are equal
            if B_source < A_source:
                return -1
            elif B_source > A_source:
                return 1
            elif B_type < A_type:
                return -1
            else:
                return 1
        elif B_type < A_type:
            return -1
        else:
            return 1


def to_string(data):
    return u"".join(chr(c) for c in data if c != 0)
