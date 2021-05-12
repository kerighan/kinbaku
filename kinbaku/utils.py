from collections import OrderedDict


def compare_nodes(key_hash, key_tuple, leaf):
    if key_hash < leaf.hash:
        return -1
    elif key_hash == leaf.hash:
        if key_tuple < leaf.key:
            return -1
        elif key_tuple == leaf.key:
            return 0
        return 1
    return 1


def compare_edge(edge, target_index, target_hash, new_edge_type):
    edge_hash = edge.hash
    edge_source = edge.source

    if edge.target == target_index:
        return 0

    if target_hash < edge_hash:
        return -1
    elif target_hash == edge_hash:
        if target_index < edge_source:
            return -1
        elif target_index == edge_source:
            if new_edge_type == edge.type:
                return 0
            elif new_edge_type < edge.type:
                return -1
            return 1
        return 1
    return 1


def to_string(data):
    return u"".join(chr(c) for c in data if c != 0)


def stringify_key(func):
    def wrapper(*args, **kwargs):
        if not isinstance(args[1], str):
            args = list(args)
            args[1] = str(args[1])
        return func(*args, **kwargs)
    return wrapper


class CacheDict(OrderedDict):
    """Dict with a limited length, ejecting LRUs as needed."""

    def __init__(self, *args, cache_len: int = 10, **kwargs):
        assert cache_len > 0
        self.cache_len = cache_len

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.cache_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)
        return val
