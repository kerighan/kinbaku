"""Base class for directed graphs.
The Graph class allows any string as a node
Self-loops are allowed but multiple edges of a same edge type are not.
"""
import math
import mmap
import os
from struct import error, pack, unpack

from cachetools import LRUCache

from .exception import (EdgeNotFound, KeyTooLong, KinbakuError,
                        KinbakuException, NodeNotFound)
from .structure import Edge, Header, Node, text
from .utils import compare_edges, compare_nodes, to_string


class Graph:
    def __init__(
        self,
        filename,
        hash_func=None,
        max_str_len=15,
        max_key_len=15,
        int_format="l",
        char_format="h",
        bool_format="?",
        hash_format="I",
        cache_len=1000000,
        table_increment=100000,
        preload=False,
        node_class=None,
        edge_class=None,
        flag="w",
    ):
        """Initialize a directed graph. A file is automatically created
        if the path provided by the filename argument does not exist.

        Args:
            filename (str): path to database. File created if it does not exist
            hash_func (function, optional): hashing function. None means that
                                            Google's CityHash will be used.
                                            Defaults to None.
            max_str_len (int, optional): max length of a string field.
                                         Defaults to 15.
            max_key_len (int, optional): max length of node keys.
                                         Defaults to 15.
            int_format (str, optional): format for integers as described in
                                        struct package. Defaults to "l".
            char_format (str, optional): format for characters.
                                         Defaults to "h".
            bool_format (str, optional): format for booleans.
                                         Defaults to "?".
            hash_format (str, optional): format for containing hashes.
                                         Defaults to "I".
            cache_len (int, optional): maximum size of the dictionary.
                                       Defaults to 10000000.
            table_increment (int, optional): [description]. Defaults to 100000.
            preload (bool, optional): if True, all nodes attributes are loaded
                                      on initialization. Defaults to False.
            node_class (dataclass, optional): the dataclass that defines
                                              custom node attributes.
                                              Defaults to None.
            edge_class (dataclass, optional): the dataclass that defines
                                              custom edge attributes.
                                              Defaults to None.
            flag (str, optional): "w" for reading and writing,
                                  "r" for reading only,
                                  "n" for new (overwrite).
                                  Defaults to "w".

        Examples
        --------
        >>> G = kn.Graph("test.db")
        >>> G = kn.Graph("test.db", flag="r")
        >>> G = kn.Graph("test.db", hash_func=lambda x: math.abs(hash(x)))
        """
        self.filename = filename
        self.table_increment = table_increment
        self.flag = flag

        if hash_func is None:
            try:
                from cityhash import CityHash32
                self.hash_func = CityHash32
            except ImportError:
                import mmh3
                self.hash_func = lambda x: mmh3.hash(x, signed=False)
        else:
            self.hash_func = hash_func

        # sizes and formats
        self.max_str_len = max_str_len
        self.max_key_len = max_key_len
        self.int_format = int_format
        self.char_format = char_format
        self.bool_format = bool_format
        self.hash_format = hash_format

        # initialize cache
        self.preload = preload
        self.cache_len = cache_len
        self.cache_id_to_key = LRUCache(cache_len)
        self.cache_key_to_pos = LRUCache(cache_len)
        self.cache_pos_to_node = LRUCache(cache_len)
        self.cache_pos_to_node_tree = LRUCache(cache_len)

        self.edge_tombstone = []
        self.node_tombstone = []

        # custom dataclasses
        if node_class is not None:
            self.node_class = node_class
        else:
            self.node_class = Node
        if edge_class is not None:
            self.edge_class = edge_class
        else:
            self.edge_class = Edge

        # initialize sizes
        self._init_edge_size()
        self._init_node_size()
        self._init_header_size()

        # mmap file
        self._load_file()

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def n_nodes(self):
        """Number of nodes in graph

        Returns:
            int: number of nodes
        """
        return self.header.n_nodes

    @property
    def n_edges(self):
        """Number of edges in graph

        Returns:
            int: number of edges
        """
        return self.header.n_edges - self.header.n_nodes

    @property
    def nodes(self):
        """Iterate over all nodes

        Yields:
            iterator: an iterator over all nodes
        """
        position = 0
        leaf = self._get_node_at(position)
        for node in self._node_dfs(leaf):
            yield node.key

    @property
    def edges(self):
        """Iterate over all edges

        Yields:
            iterator: an iterator over all edges
        """
        for edge in self._iter_edges():
            yield self._get_keys_from_edge(edge)

    # =========================================================================
    # Parsers
    # =========================================================================

    def _parse_fields(self, data):
        DATA_FORMAT = ""
        DATA_VALUES = []
        for field in data.__dataclass_fields__.values():
            if field.name == "hash":
                DATA_FORMAT += self.hash_format
                DATA_VALUES.append(0)
            elif field.type == text:
                DATA_FORMAT += field.default.length * self.char_format
                DATA_VALUES += (0,) * field.default.length
            elif field.type == int:
                DATA_FORMAT += self.int_format
                DATA_VALUES.append(0)
            elif field.name == "key":
                DATA_FORMAT += self.max_key_len * self.char_format
                DATA_VALUES += (0,) * self.max_key_len
            elif field.type == str:
                DATA_FORMAT += self.max_str_len * self.char_format
                DATA_VALUES += (0,) * self.max_str_len
            elif field.type == bool:
                DATA_FORMAT += self.bool_format
                DATA_VALUES.append(False)
            elif field.type == float:
                DATA_FORMAT += "f"
                DATA_VALUES.append(0.0)
        return DATA_FORMAT, DATA_VALUES

    def _parse_values(self, data):
        values = []
        extend = values.extend
        append = values.append
        for field in data.__dataclass_fields__.keys():
            value = getattr(data, field)
            if isinstance(value, (int, bool)):
                append(value)
            elif isinstance(value, str):
                if field != "key":
                    extend(self._str_to_list(value))
                else:
                    extend(self._key_to_list(value))
            elif isinstance(value, tuple):
                extend(list(value))
            else:
                append(value)
        return values

    def _parse_attributes(self, leaf, attr):
        if attr is None:
            return
        for attribute, value in attr.items():
            if isinstance(value, str):
                assert len(value) <= self.max_str_len
            setattr(leaf, attribute, value)

    # =========================================================================
    # Initializers
    # =========================================================================

    def _init_edge_size(self):
        self.EDGE_FORMAT, VALUES = self._parse_fields(self.edge_class)
        self.EDGE = pack(self.EDGE_FORMAT, *VALUES)
        self.EDGE_SIZE = len(self.EDGE)

    def _init_node_size(self):
        self.NODE_FORMAT, VALUES = self._parse_fields(self.node_class)
        VALUES[0] = 1  # boolean that indicates that item is node
        self.NODE = pack(self.NODE_FORMAT, *VALUES)
        self.NODE_SIZE = len(self.NODE)

        self.NODE_TO_EDGE_RATIO = math.ceil(self.NODE_SIZE / self.EDGE_SIZE)
        self.NODE_PLACEHOLDER_SIZE = self.NODE_TO_EDGE_RATIO * self.EDGE_SIZE
        # pad placeholder with 0s
        self.NODE_PLACEHOLDER = self.NODE + b"\x00" * (
            self.NODE_PLACEHOLDER_SIZE - self.NODE_SIZE)

        # get node_tree_info  format
        self.NODE_TREE_FORMAT = (
            2 * self.bool_format + self.hash_format + 2 * self.int_format)
        self.NODE_TREE = pack(self.NODE_TREE_FORMAT, 0, 0, 0, 0, 0)
        self.NODE_TREE_SIZE = len(self.NODE_TREE)

    def _init_header_size(self):
        HEADER_FORMAT = ""
        HEADER_VALUES = []
        for name, field in Header.__dataclass_fields__.items():
            if field.type == int:
                HEADER_FORMAT += self.int_format
            if name == "table_size":
                HEADER_VALUES.append(
                    self.table_increment + self.NODE_TO_EDGE_RATIO
                )
            elif name == "node_id":
                HEADER_VALUES.append(1)
            elif name == "next_table_position":
                HEADER_VALUES.append(self.NODE_TO_EDGE_RATIO)
            else:
                HEADER_VALUES.append(0)
        self.HEADER_FORMAT = HEADER_FORMAT
        self.HEADER = pack(HEADER_FORMAT, *HEADER_VALUES)
        self.HEADER_SIZE = len(self.HEADER)

    # =========================================================================
    # File & memory management
    # =========================================================================

    def _load_file(self):
        # create folder if it doesn't exist
        if self.flag in {"n", "w"}:
            folder = os.path.dirname(self.filename)
            if folder != "" and not os.path.exists(folder):
                os.makedirs(folder)

        # initialize new file if it doesn't exist or flag is set to NEW
        if not os.path.exists(self.filename) or self.flag == "n":
            with open(self.filename, "wb") as f:
                f.write(self.HEADER)
                f.write(self.NODE_PLACEHOLDER)
                f.write(self.EDGE * self.table_increment)
            self._map_to_memory()
            self._get_sizes()

            # insert immovable root node
            root = self.node_class(hash=2147483648)
            self._set_node_at(root, 0)
        else:
            self._map_to_memory()
            self._get_sizes()
            if self.preload:
                for _ in self.nodes:
                    pass

    def _map_to_memory(self):
        with open(self.filename, "r+b") as f:
            if self.flag == "w" or self.flag == "n":
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)
            else:
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

    def _expand(self):
        if (
            self.header.next_table_position
            <= self.header.table_size - 0.1 * self.table_increment
        ):
            self.mm[: self.HEADER_SIZE] = pack(
                self.HEADER_FORMAT, *self._parse_values(self.header))
            return

        with open(self.filename, "ab") as f:
            f.write(self.EDGE * self.table_increment)

        # add increment to table_size
        self.header.table_size += self.table_increment
        self.mm[:self.HEADER_SIZE] = pack(
            self.HEADER_FORMAT, *self._parse_values(self.header))
        self._map_to_memory()

    def _increment_node(self, recycled):
        self.header.n_nodes += 1
        self.header.node_id += 1
        if not recycled:
            self.header.next_table_position += self.NODE_TO_EDGE_RATIO
        self._expand()

    def _increment_edge(self, recycled):
        self.header.n_edges += 1
        if not recycled:
            self.header.next_table_position += 1
        self._expand()

    def _decrement_edge(self):
        self.header.n_edges -= 1
        self._expand()

    def _decrement_node(self):
        self.header.n_nodes -= 1
        self._expand()

    def _cache_node(self, node):
        nkey, npos, nindex, nhash, nleft, nright = (
            node.key, node.position, node.index,
            node.hash, node.left, node.right)

        self.cache_key_to_pos[nkey] = npos
        self.cache_id_to_key[nindex] = nkey
        self.cache_pos_to_node[npos] = node
        self.cache_pos_to_node_tree[npos] = (nhash, nleft, nright)

    def _uncache_node(self, node):
        npos = node.position
        try:
            del self.cache_key_to_pos[node.key]
        except KeyError:
            pass
        try:
            del self.cache_id_to_key[node.index]
        except KeyError:
            pass
        try:
            del self.cache_pos_to_node[npos]
        except KeyError:
            pass
        try:
            del self.cache_pos_to_node_tree[npos]
        except KeyError:
            pass

    def empty_cache(self):
        self.cache_id_to_key = LRUCache(self.cache_len)
        self.cache_key_to_pos = LRUCache(self.cache_len)
        self.cache_pos_to_node = LRUCache(self.cache_len)
        self.cache_pos_to_node_tree = LRUCache(self.cache_len)
        self._get_sizes()

    def find_tombstones(self):
        EDGE_SIZE = self.EDGE_SIZE
        HEADER_SIZE = self.HEADER_SIZE
        NODE_TO_EDGE_RATIO = self.NODE_TO_EDGE_RATIO
        position = 0
        size = len(pack("?" + self.int_format, 0, 0))
        while position <= self.header.next_table_position:
            ind = position * EDGE_SIZE + HEADER_SIZE
            is_node, index = unpack("?" + self.int_format,
                                    self.mm[ind: ind + size])
            if is_node:
                if index == 0:
                    self.node_tombstone.append(position)
                position += NODE_TO_EDGE_RATIO
            else:
                if index == 0:
                    self.edge_tombstone.append(position)
                position += 1

    # =========================================================================
    # Tree traversal
    # =========================================================================

    def _iter_edges(self):
        EDGE_SIZE = self.EDGE_SIZE
        HEADER_SIZE = self.HEADER_SIZE
        NODE_TO_EDGE_RATIO = self.NODE_TO_EDGE_RATIO

        position = 0
        while position <= self.header.next_table_position:
            ind = position * EDGE_SIZE + HEADER_SIZE
            is_node, exists = unpack("??", self.mm[ind: ind + 2])
            if is_node:
                position += NODE_TO_EDGE_RATIO
            else:
                if not exists:
                    position += 1
                    continue

                edge = self._get_edge_at(position)
                position += 1
                if edge.is_edge_start:
                    continue
                yield edge

    def _find_node_pos(self, position, new_node):
        _get_node_at = self._get_node_at
        _get_node_tree_info_at = self._get_node_tree_info_at

        current_hash, current_left, current_right = (
            _get_node_tree_info_at(position))
        new_node_hash = new_node.hash

        while 1:
            if new_node_hash < current_hash:
                state = -1
            elif new_node_hash > current_hash:
                state = 1
            else:
                current_node = _get_node_at(position)
                state = compare_nodes(current_node.hash,
                                      current_node.key, new_node)

            if state == -1:
                if current_left != 0:
                    position = current_left
                    current_hash, current_left, current_right = (
                        _get_node_tree_info_at(position))
                    continue
                break
            elif state == 1:
                if current_right != 0:
                    position = current_right
                    current_hash, current_left, current_right = (
                        _get_node_tree_info_at(position))
                    continue
                break
            else:  # is equal
                break
        current_node = _get_node_at(position)
        return current_node, state

    def _find_edge_out_pos(self, position, new_edge):
        _get_edge_at = self._get_edge_at

        current_edge = _get_edge_at(position)
        while 1:
            state = compare_edges(current_edge, new_edge)
            if state == -1:  # go left
                if current_edge.out_edge_left != 0:
                    position = current_edge.out_edge_left
                    current_edge = _get_edge_at(position)
                    continue
                break
            elif state == 1:  # go right
                if current_edge.out_edge_right != 0:
                    position = current_edge.out_edge_right
                    current_edge = _get_edge_at(position)
                    continue
                break
            else:  # is equal
                break
        return current_edge, state

    def _find_edge_in_pos(self, position, new_edge):
        _get_edge_at = self._get_edge_at

        current_edge = _get_edge_at(position)
        while 1:
            state = compare_edges(current_edge, new_edge)
            if state == -1:  # go left
                if current_edge.in_edge_left != 0:
                    position = current_edge.in_edge_left
                    current_edge = _get_edge_at(position)
                    continue
                break
            elif state == 1:  # go right
                if current_edge.in_edge_right != 0:
                    position = current_edge.in_edge_right
                    current_edge = _get_edge_at(position)
                    continue
                break
            else:  # is equal
                break
        return current_edge, state

    def _find_inorder_successor_edge(self, edge, out=True):
        _get_edge_at = self._get_edge_at

        edge_right = edge.out_edge_right if out else edge.in_edge_right
        left = "out_edge_left" if out else "in_edge_left"
        successor = _get_edge_at(edge_right)
        antecedent = edge
        while getattr(successor, left) != 0:
            antecedent = successor
            successor = _get_edge_at(getattr(successor, left))
        return (successor, antecedent)

    def _find_inorder_successor_node(self, node):
        _get_node_at = self._get_node_at

        successor = _get_node_at(node.right)
        antecedent = node
        while successor.left != 0:
            antecedent = successor
            successor = _get_node_at(successor.left)
        return (successor, antecedent)

    def _node_dfs(self, node):
        if node.index != 0:
            yield node

        # store in cache
        self._cache_node(node)

        if node.left != 0:
            yield from self._node_dfs(self._get_node_at(node.left))
        if node.right != 0:
            yield from self._node_dfs(self._get_node_at(node.right))

    def _edge_out_dfs(self, edge):
        if edge.out_edge_left != 0:
            yield from self._edge_out_dfs(
                self._get_edge_at(edge.out_edge_left))
        if edge.out_edge_right != 0:
            yield from self._edge_out_dfs(
                self._get_edge_at(edge.out_edge_right))
        if not edge.is_edge_start:
            yield edge

    def _edge_in_dfs(self, edge):
        if edge.in_edge_left != 0:
            yield from self._edge_in_dfs(self._get_edge_at(edge.in_edge_left))
        if edge.in_edge_right != 0:
            yield from self._edge_in_dfs(self._get_edge_at(edge.in_edge_right))
        if not edge.is_edge_start:
            yield edge

    def _unplug_edge(self, parent, state, out=True):
        if state == -1:  # edge came from left
            if out:
                parent.out_edge_left = 0
            else:
                parent.in_edge_left = 0
        else:
            if out:
                parent.out_edge_right = 0
            else:
                parent.in_edge_right = 0
        self._set_edge_at(parent, parent.position)

    def _unplug_node(self, parent, state):
        if state == -1:
            parent.left = 0
        else:
            parent.right = 0
        self._set_node_at(parent, parent.position)

    def _rewire_edge(self, parent, child, state, out=True):
        if state == -1:
            if out:
                parent.out_edge_left = child.position
            else:
                parent.in_edge_left = child.position
        else:
            if out:
                parent.out_edge_right = child.position
            else:
                parent.in_edge_right = child.position
        if out:
            child.out_edge_parent = parent.position
        else:
            child.in_edge_parent = parent.position
        self._set_edge_at(parent, parent.position)
        self._set_edge_at(child, child.position)

    def _rewire_node(self, parent, child, state):
        if state == -1:
            parent.left = child.position
        else:
            parent.right = child.position
        child.parent = parent.position
        self._set_node_at(parent, parent.position)
        self._set_node_at(child, child.position)

    def _remove_node_from_tree(self, node):
        parent = self._get_node_at(node.parent)
        # find state
        if parent.left == node.position:
            state = -1
        elif parent.right == node.position:
            state = 1
        else:
            raise KinbakuError("state == 0")

        node_left = node.left
        node_right = node.right

        # case 1: node to remove has no child
        if node_left == 0 and node_right == 0:
            self._unplug_node(parent, state)
        # case 2: node to remove has only one child
        elif node_left == 0:
            child = self._get_node_at(node_right)
            self._rewire_node(parent, child, state)
        elif node_right == 0:
            child = self._get_node_at(node_left)
            self._rewire_node(parent, child, state)
        # case 3: node to remove has two children
        else:
            successor, antecedent = self._find_inorder_successor_node(node)
            # remove antecedent link to successor
            antecedent.left = 0
            # set successor's parent to parent
            successor.parent = parent.position
            if state == -1:
                parent.left = successor.position
            else:
                parent.right = successor.position

            # case a: antecedent happens to be the node to remove
            if antecedent.position == node.position:
                successor.left = node_left
                node_left_item = self._get_node_at(node_left)
                node_left_item.parent = successor.position

                self._set_node_at(node_left_item, node_left)
                self._set_node_at(successor, successor.position)
                self._set_node_at(parent, parent.position)
            # case b: antecedent is further down
            else:
                # put left tree in left child of successor
                successor.left = node_left
                node_left_item = self._get_node_at(node_left)
                node_left_item.parent = successor.position
                self._set_node_at(node_left_item, node_left)

                if node_right == antecedent.position:
                    antecedent.parent = successor.position
                else:
                    node_right_item = self._get_node_at(node_right)
                    node_right_item.parent = successor.position
                    self._set_node_at(node_right_item, node_right)

                # put right tree of successor to antecdent left tree
                successor_right_pos = successor.right
                antecedent.left = successor_right_pos
                self._set_node_at(antecedent, antecedent.position)
                if successor_right_pos != 0:
                    successor_right = self._get_node_at(successor_right_pos)
                    successor_right.parent = antecedent.position
                    self._set_node_at(successor_right, successor_right_pos)

                successor.right = node_right
                self._set_node_at(successor, successor.position)
                self._set_node_at(parent, parent.position)

    def _remove_edge_from_tree(self, edge, out=True):
        # utilitary variables
        if out:
            edge_left = edge.out_edge_left
            edge_right = edge.out_edge_right
            parent = self._get_edge_at(edge.out_edge_parent)
        else:
            edge_left = edge.in_edge_left
            edge_right = edge.in_edge_right
            parent = self._get_edge_at(edge.in_edge_parent)
        state = compare_edges(parent, edge)

        # case 1: edge to remove has no child
        if edge_left == 0 and edge_right == 0:
            if parent.position < 0:
                print(edge)
                print(parent)
                raise ValueError
            self._unplug_edge(parent, state, out)
        # case 2: edge to remove has only one child
        elif edge_left == 0:
            child = self._get_edge_at(edge_right)
            self._rewire_edge(parent, child, state, out)
        elif edge_right == 0:
            child = self._get_edge_at(edge_left)
            self._rewire_edge(parent, child, state, out)
        # case 3: edge to remove has two children
        else:
            successor, antecedent = self._find_inorder_successor_edge(
                edge, out=out)
            right = "out_edge_right" if out else "in_edge_right"
            left = "out_edge_left" if out else "in_edge_left"
            up = "out_edge_parent" if out else "in_edge_parent"

            # remove antecedent link to successor
            setattr(antecedent, left, 0)

            # set successor's parent to parent
            setattr(successor, up, parent.position)
            if state == -1:
                setattr(parent, left, successor.position)
            else:
                setattr(parent, right, successor.position)

            # case a: antecedent happens to be the edge to remove
            if antecedent.position == edge.position:
                setattr(successor, left, edge_left)
                edge_left_item = self._get_edge_at(edge_left)
                setattr(edge_left_item, up, successor.position)

                self._set_edge_at(edge_left_item, edge_left)
                self._set_edge_at(successor, successor.position)
                self._set_edge_at(parent, parent.position)
            # case b: antecedent is further down
            else:
                # put left tree in left child of successor
                setattr(successor, left, edge_left)
                edge_left_item = self._get_edge_at(edge_left)
                setattr(edge_left_item, up, successor.position)
                self._set_edge_at(edge_left_item, edge_left)

                if edge_right == antecedent.position:
                    setattr(antecedent, up, successor.position)
                else:
                    edge_right_item = self._get_edge_at(edge_right)
                    setattr(edge_right_item, up, successor.position)
                    self._set_edge_at(edge_right_item, edge_right)

                # put right tree of successor to antecdent left tree
                successor_right_pos = getattr(successor, right)
                setattr(antecedent, left, successor_right_pos)
                self._set_edge_at(antecedent, antecedent.position)
                if successor_right_pos != 0:
                    successor_right = self._get_edge_at(successor_right_pos)
                    setattr(successor_right, up, antecedent.position)
                    self._set_edge_at(successor_right, successor_right_pos)

                setattr(successor, right, edge_right)
                self._set_edge_at(successor, successor.position)
                self._set_edge_at(parent, parent.position)

    # =========================================================================
    # Getters
    # =========================================================================

    def _get_next_node_position(self):
        if len(self.node_tombstone) == 0:
            return self.header.next_table_position, False
        return self.node_tombstone.pop(0), True

    def _get_next_edge_position(self):
        if len(self.edge_tombstone) == 0:
            recycled = False
            return self.header.next_table_position, recycled
        recycled = True
        return self.edge_tombstone.pop(0), recycled

    def _get_sizes(self):
        data = unpack(self.HEADER_FORMAT, self.mm[:self.HEADER_SIZE])
        self.header = Header(*data)

    def _get_node_tree_info_at(self, position):
        data = self.cache_pos_to_node_tree.get(position)
        if data is not None:
            return data

        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        _, _, hash, left, right = unpack(
            self.NODE_TREE_FORMAT, self.mm[ind: ind + self.NODE_TREE_SIZE])
        self.cache_pos_to_node_tree[position] = hash, left, right
        return hash, left, right

    def _get_node_at(self, position):
        node = self.cache_pos_to_node.get(position)
        if node is not None:
            return node

        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        try:
            data = unpack(self.NODE_FORMAT, self.mm[ind: ind + self.NODE_SIZE])
        except error:
            self._map_to_memory()
            data = unpack(self.NODE_FORMAT, self.mm[ind: ind + self.NODE_SIZE])

        i = 0
        res = []
        append = res.append
        for field in self.node_class.__dataclass_fields__.values():
            if field.type != tuple and field.type != str:
                append(data[i])
                i += 1
            elif field.name == "key":
                append(to_string(data[i: i + self.max_key_len]))
                i += self.max_key_len
            elif field.type == str:
                append(to_string(data[i: i + self.max_str_len]))
                i += self.max_str_len
        node = self.node_class(*res)

        if node.index not in self.cache_id_to_key:
            self._cache_node(node)
        return node

    def _get_edge_at(self, position):
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        data = unpack(self.EDGE_FORMAT, self.mm[ind: ind + self.EDGE_SIZE])
        edge = self.edge_class(*data)
        return edge

    def _get_edge_hash(self, source, target, edge_type):
        return self.hash_func(
            "_".join((str(source.hash), str(edge_type), str(target.hash))))

    def _get_keys_from_edge(self, edge):
        src_pos = edge.source_position
        u = self._get_node_at(src_pos).key

        tgt_pos = edge.target_position
        v = self._get_node_at(tgt_pos).key
        return u, v

    # =========================================================================
    # Public methods
    # =========================================================================

    def close(self):
        """Closes database file."""
        self.mm.close()

    def neighbors(self, u):
        """Iterate over all nodes v such that (u, v) is an edge

        Args:
            u (str): key of the source node

        Yields:
            iterator: iterator of node keys
        """
        leaf = self.node(u)
        start = self._get_edge_at(leaf.edge_start)
        for edge in self._edge_out_dfs(start):
            res = self._get_node_at(edge.target_position)
            yield res.key

    def predecessors(self, v):
        """Iterate over all nodes u such that (u, v) is an edge

        Args:
            v ([type]): key of the target node

        Yields:
            iterator: iterator of node keys
        """
        leaf = self.node(v)
        start = self._get_edge_at(leaf.edge_start)
        for edge in self._edge_in_dfs(start):
            res = self._get_node_at(edge.source_position)
            yield res.key

    def set_neighbors(self, u, new_neighbors):
        """Strictly assign predecessors to a node

        Args:
            v ([type]): key of the target node
            new_predecessors: list or set of the sources key
        """
        remove_edge = self.remove_edge
        add_edge = self.add_edge

        self.add_node(u)
        new_neighbors = set(new_neighbors)
        old_neighbors = set(self.neighbors(u))

        to_add = new_neighbors.difference(old_neighbors)
        to_remove = old_neighbors.difference(new_neighbors)
        for v in to_remove:
            remove_edge(u, v)
        for v in to_add:
            add_edge(u, v)

    def set_predecessors(self, v, new_predecessors):
        """Strictly assign predecessors to a node

        Args:
            v ([type]): key of the target node
            new_predecessors: list or set of the sources key
        """
        remove_edge = self.remove_edge
        add_edge = self.add_edge

        self.add_node(v)
        new_predecessors = set(new_predecessors)
        old_predecessors = set(self.predecessors(v))

        to_add = new_predecessors.difference(old_predecessors)
        to_remove = old_predecessors.difference(new_predecessors)
        for u in to_remove:
            remove_edge(u, v)
        for u in to_add:
            add_edge(u, v)

    def neighbors_from(self, nodes):
        """Returns the list of neighbors for all given nodes

        Args:
            nodes (list): list of node keys
        Returns:
            dict: a dict mapping node keys to the list of their neighbors
        """
        nbs = []
        # NOTE: not a oneliner as it would block multithreading
        for node in nodes:
            nbs.append(self.neighbors(node))
        return nbs

    def predecessors_from(self, nodes, n_jobs=-1):
        """Returns the list of predecessors for all given nodes

        Args:
            nodes (list): list of node keys
            n_jobs (int, optional): The number of cpus to use. All available
                                    cpus are used if n_jobs==-1.
                                    Defaults to -1.
        Returns:
            dict: a dict mapping node keys to the list of their predecessors
        """
        nbs = []
        # NOTE: not a oneliner as it would block multithreading
        for node in nodes:
            nbs.append(self.predecessors(node))
        return nbs

    def common_neighbors(self, u, v):
        """Returns the set of common neighbors between two nodes

        Args:
            u (str): key of the first node
            v (str): key of the second node
        Returns:
            set: the set of all common neighbors
        """
        u_nbs = set(self.neighbors(u))
        v_nbs = set(self.neighbors(v))
        return u_nbs.intersection(v_nbs)

    def common_predecessors(self, u, v):
        """Returns the set of common predecessors between two nodes

        Args:
            u (str): key of the first node
            v (str): key of the second node
        Returns:
            set: the set of all common predecessors
        """
        u_nbs = set(self.predecessors(u))
        v_nbs = set(self.predecessors(v))
        return u_nbs.intersection(v_nbs)

    def out_degree(self, key):
        # returns out-degree
        count = 0
        for _ in self.neighbors(key):
            count += 1
        return count

    def in_degree(self, key):
        # returns in-degree
        count = 0
        for _ in self.predecessors(key):
            count += 1
        return count

    def node(self, key):
        """Get node from key

        Args:
            key (str): unique string identifier of the node

        Raises:
            NodeNotFound: the key does not match any node in the graph

        Returns:
            node_class: node
        """
        # if key is already a node object
        if isinstance(key, self.node_class):
            return key

        if len(key) > self.max_key_len:
            raise KeyTooLong

        # if key is in cache
        pos = self.cache_key_to_pos.get(key)
        if pos is not None:
            node = self.cache_pos_to_node.get(pos)
            if node is not None:
                return node

        key_hash = self.hash_func(key)

        node = self.node_class(hash=key_hash, key=key)

        # unroll tree
        position = 0
        prev_node, state = self._find_node_pos(position, node)
        if state == 0:
            self._cache_node(prev_node)
            return prev_node
        else:
            raise NodeNotFound

    def edge(self, source, target, edge_type=0):
        """Get edge from source, target and edge type

        Args:
            source (str): key of the source node
            target (str): key of the target node
            edge_type (int): type of edge to match

        Raises:
            EdgeNotFound: the arguments do not match any edge in the graph

        Returns:
            edge_class: edge
        """
        # get source and target nodes
        source = self.node(source)
        target = self.node(target)

        # new edge to create
        new_edge = self.edge_class(source_position=source.position,
                                   target_position=target.position,
                                   hash=self._get_edge_hash(
                                       source, target, edge_type),
                                   type=edge_type)
        edge, state = self._find_edge_out_pos(source.edge_start, new_edge)

        # edge already exists
        if state == 0:
            return edge
        raise EdgeNotFound(f"Edge {source.key} -> {target.key} not found")

    def has_node(self, node):
        """Returns True if node exists

        Args:
            node (str): string key of the node

        Returns:
            bool: True if node exists, False otherwise
        """
        try:
            self.node(node)
            return True
        except NodeNotFound:
            return False

    def has_edge(self, source, target, edge_type=0):
        """Returns True if (source, target[, edge_type]) exists

        Args:
            source (str): key of source node
            target (str): key of target node
            edge_type (int, optional): edge type to match. Defaults to 0.

        Returns:
            bool: True if edge exists, False otherwise
        """
        try:
            self.edge(source, target, edge_type)
            return True
        except EdgeNotFound:
            return False

    def batch_get_nodes(self, batch_size=100, cursor=0):
        """Get a batch of nodes starting from a given table position

        Args:
            batch_size (int): number of nodes to return per batch
            cursor (int): starting position in the table

        Returns:
            list[node_class]: list of nodes
            int: cursor for the next batch. Equals -1 if the end is reached
        """
        _get_node_at = self._get_node_at
        EDGE_SIZE = self.EDGE_SIZE
        HEADER_SIZE = self.HEADER_SIZE
        NODE_TO_EDGE_RATIO = self.NODE_TO_EDGE_RATIO

        position = cursor
        n_nodes = 0
        nodes = []
        append = nodes.append

        next_table_position = self.header.next_table_position
        while (
            position <= next_table_position and
            n_nodes < batch_size
        ):
            ind = position * EDGE_SIZE + HEADER_SIZE
            is_node, exists = unpack("??", self.mm[ind: ind + 2])
            if is_node:
                if not exists or position == 0:
                    position += NODE_TO_EDGE_RATIO
                    continue
                node = _get_node_at(position)
                append(node)
                position += NODE_TO_EDGE_RATIO
                n_nodes += 1
            else:
                position += 1
        if position > next_table_position:
            position = -1
        return nodes, position

    def batch_get_edges(self, batch_size=100, cursor=0):
        """Get a batch of edges starting from a given table position

        Args:
            batch_size (int): number of edges to return per batch
            cursor (int): starting position in the table

        Returns:
            list[tuple]: list of edges (tuple of str)
            int: cursor for the next batch. Equals -1 if the end is reached
        """
        _get_edge_at = self._get_edge_at
        _get_keys_from_edge = self._get_keys_from_edge
        EDGE_SIZE = self.EDGE_SIZE
        HEADER_SIZE = self.HEADER_SIZE
        NODE_TO_EDGE_RATIO = self.NODE_TO_EDGE_RATIO

        position = cursor
        n_edges = 0
        edges = []
        append = edges.append
        next_table_position = self.header.next_table_position
        while (
            position <= next_table_position and
            n_edges < batch_size
        ):
            ind = position * EDGE_SIZE + HEADER_SIZE
            is_node, exists = unpack("??", self.mm[ind: ind + 2])
            if is_node:
                position += NODE_TO_EDGE_RATIO
            else:
                if not exists:
                    position += 1
                    continue

                edge = _get_edge_at(position)
                position += 1
                if edge.is_edge_start:
                    continue
                append(_get_keys_from_edge(edge))
                n_edges += 1
        if position > next_table_position:
            position = -1
        return edges, position

    def adjacency_matrix(self, weight=None):
        """Return adjacency matrix of the graph

        Args:
            weight (str): NOT IMPLEMENTED! Weight attribute

        Returns:
            scipy.sparse.csr_matrix: sparse matrix representing the graph
        """
        import numpy as np
        from scipy.sparse import csr_matrix

        node_to_index = {}
        index_to_node = {}
        index = 0
        xs = []
        ys = []
        data = []
        for source, target in self.edges:
            source_id = node_to_index.get(source)
            if source_id is None:
                node_to_index[source] = index
                index_to_node[index] = source
                source_id = index
                index += 1

            target_id = node_to_index.get(target)
            if target_id is None:
                node_to_index[target] = index
                index_to_node[index] = target
                target_id = index
                index += 1

            xs.append(source_id)
            ys.append(target_id)
            if weight is None:
                data.append(1)

        if weight is None:
            dtype = np.bool_
        A = csr_matrix((data, (xs, ys)), shape=(index, index), dtype=dtype)
        return A, index_to_node

    def subgraph(self, nodes, weight=None):
        """Return adjacency matrix of a subgraph

        Args:
            nodes (list): subset of nodes to consider for the subgraph
            weight (str): NOT IMPLEMENTED! Weight attribute

        Returns:
            scipy.sparse.csr_matrix: sparse matrix representing the subgraph
        """
        import numpy as np
        from scipy.sparse import csr_matrix

        index_to_node = dict(enumerate(set(nodes)))
        node_to_index = {v: k for k, v in index_to_node.items()}
        n_nodes = len(index_to_node)

        xs = []
        ys = []
        data = []
        for source in nodes:
            source_id = node_to_index[source]
            for target in self.neighbors(source):
                target_id = node_to_index.get(target, None)
                if target_id is None:
                    continue
                xs.append(source_id)
                ys.append(target_id)
                if weight is None:
                    data.append(1)

        if weight is None:
            dtype = np.bool_
        A = csr_matrix((data, (xs, ys)), shape=(n_nodes, n_nodes), dtype=dtype)
        return A, index_to_node

    # =========================================================================
    # Overload
    # =========================================================================

    def __getitem__(self, item):
        """Get node or edge

        Args:
            item (str or tuple): if one string is provided, returns
                                 corresponding node.
                                 If two strings are provided (+ an optional
                                 edge_type), return the corresponding edge.

        Returns:
            node_class or edge_class: node or edge
        """
        if isinstance(item, str):
            return self.node(item)
        elif isinstance(item, tuple):
            return self.edge(*item)

    def __contains__(self, item):
        """Returns True if node or edge exists

        Args:
            item (str or tuple): if item is a string, return has_node(item),
                                 if item is a tuple, return has_edge(...)

        Raises:
            KinbakuException: query malformed

        Returns:
            bool: True if edge or node exists, False otherwise
        """
        if isinstance(item, tuple):
            if 2 <= len(item) <= 3:
                return self.has_edge(*item)
        elif isinstance(item, str):
            return self.has_node(item)
        raise KinbakuException("argument not understood")

    def __del__(self):
        self.close()

    # =========================================================================
    # Setters
    # =========================================================================

    def _set_node_at(self, leaf, position):
        values = self._parse_values(leaf)
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind: ind + self.NODE_SIZE] = pack(self.NODE_FORMAT, *values)
        self._cache_node(leaf)

    def _set_edge_at(self, edge, position):
        values = self._parse_values(edge)
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        try:
            self.mm[ind: ind + self.EDGE_SIZE] = pack(
                self.EDGE_FORMAT, *values)
        except IndexError:
            print(self.header.table_size, ind + self.EDGE_SIZE, ind)
            raise IndexError

    def _erase_edge_at(self, position):
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind: ind + self.EDGE_SIZE] = self.EDGE
        self.edge_tombstone.append(position)
        self._decrement_edge()

    def _erase_node(self, node):
        self._uncache_node(node)
        ind = node.position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind: ind + self.NODE_SIZE] = self.NODE
        self.node_tombstone.append(node.position)
        self._decrement_node()
        # also remove edge start
        ind = node.edge_start * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind: ind + self.EDGE_SIZE] = self.EDGE
        self.edge_tombstone.append(node.edge_start)
        self._decrement_edge()

    # =========================================================================
    # Create & delete Nodes & Edges
    # =========================================================================

    def add_node(self, key, attr=None):
        """Add a single node to graph, with optional attributes.

        Args:
            key (str): string key uniquely identifying a node
            attr (dict, optional): custom attributes. Must match the
                                   additional attributes provided in the
                                   `node_class` parameter. Defaults to None.

        Returns:
            node_class: returns node as an instance of Graph:`node_class`
        """
        # key must be of appropriate size
        if len(key) > self.max_key_len:
            raise KeyTooLong

        key_hash = self.hash_func(key)

        # new node
        new_node = self.node_class(
            hash=key_hash, index=self.header.node_id, key=key)
        self._parse_attributes(new_node, attr)

        # initialize position from cache
        position = self.cache_key_to_pos.get(key)
        if position is None:
            position = 0

        # unroll tree
        prev_node, state = self._find_node_pos(position, new_node)

        # node already exists
        if state == 0:
            if new_node == prev_node:
                return prev_node

            (
                new_node.left,
                new_node.right,
                new_node.index,
                new_node.position,
                new_node.parent,
                new_node.edge_start
            ) = (
                prev_node.left,
                prev_node.right,
                prev_node.index,
                prev_node.position,
                prev_node.parent,
                prev_node.edge_start
            )
            self._set_node_at(new_node, new_node.position)
            return new_node

        # new node and edge positions
        new_node_position, node_recycled = self._get_next_node_position()
        self._increment_node(node_recycled)
        new_edge_position, edge_recycled = self._get_next_edge_position()
        self._increment_edge(edge_recycled)

        # new node
        new_node.position, new_node.edge_start, new_node.parent = (
            new_node_position, new_edge_position, prev_node.position)
        self._set_node_at(new_node, new_node_position)

        # new 'dummy' edge
        edge = self.edge_class(source_position=new_node_position,
                               hash=new_node.hash,
                               is_edge_start=True,
                               position=new_node.edge_start)
        self._set_edge_at(edge, new_node.edge_start)

        # update parent node
        if state == -1:
            prev_node.left = new_node_position
        else:
            prev_node.right = new_node_position
        self._set_node_at(prev_node, prev_node.position)
        return new_node

    def add_edge(self, source_key, target_key, attr=None, edge_type=0):
        """Add a single edge with custom attributes to graph

        Args:
            source_key (str): string key of the source node
            target_key (str): string key of the target node
            attr (dict, optional): not yet implemented. Defaults to None.
            edge_type (int, optional): integer identifier of the edge type.
                                       Defaults to 0.

        Returns:
            edge_class: returns edge as an instance of Graph:`edge_class`
        """
        try:
            source = self.node(source_key)
        except NodeNotFound:
            source = self.add_node(source_key)
        try:
            target = self.node(target_key)
        except NodeNotFound:
            target = self.add_node(target_key)

        # new edge to create
        new_edge = self.edge_class(source_position=source.position,
                                   target_position=target.position,
                                   hash=self._get_edge_hash(
                                       source, target, edge_type),
                                   type=edge_type)
        self._parse_attributes(new_edge, attr)

        # =====================================================================
        # OUT direction
        prev_out, state = self._find_edge_out_pos(source.edge_start, new_edge)
        if state == 0:  # edge already exists
            if prev_out == new_edge:
                return prev_out
            new_edge_position = prev_out.position
            (
                new_edge.position,
                new_edge.source_position,
                new_edge.target_position,
                new_edge.out_edge_left,
                new_edge.out_edge_right,
                new_edge.out_edge_parent,
                new_edge.in_edge_left,
                new_edge.in_edge_right,
                new_edge.in_edge_parent
            ) = (
                new_edge_position,
                prev_out.source_position,
                prev_out.target_position,
                prev_out.out_edge_left,
                prev_out.out_edge_right,
                prev_out.out_edge_parent,
                prev_out.in_edge_left,
                prev_out.in_edge_right,
                prev_out.in_edge_parent
            )
            self._set_edge_at(new_edge, new_edge_position)
            return new_edge
        else:
            self._parse_attributes(new_edge, attr)
            new_edge_position, recycled = self._get_next_edge_position()
            new_edge.position = new_edge_position

        if state == -1:  # must insert left
            prev_out.out_edge_left = new_edge_position
        else:  # must insert right
            prev_out.out_edge_right = new_edge_position
        # update previous out-edge
        self._set_edge_at(prev_out, prev_out.position)

        # =====================================================================
        # IN direction
        prev_in, state = self._find_edge_in_pos(target.edge_start, new_edge)
        if state == -1:
            prev_in.in_edge_left = new_edge_position
        elif state == 1:
            prev_in.in_edge_right = new_edge_position
        else:  # edge shouldn't exist
            raise KinbakuError("serious integrity error")

        # update previous in-edge
        self._set_edge_at(prev_in, prev_in.position)

        # =====================================================================
        # insert new edge
        new_edge.out_edge_parent = prev_out.position
        new_edge.in_edge_parent = prev_in.position
        self._set_edge_at(new_edge, new_edge_position)
        self._increment_edge(recycled)
        return new_edge

    def remove_edge(self, source_key, target_key, edge_type=0):
        """Remove the edge linking source to target, with the given edge_type

        Args:
            source_key (str): string key of the source node
            target_key (str): string key of the target node
            edge_type (int, optional): edge type of the edge to remove.
                                       Defaults to 0.
        """
        edge = self.edge(source_key, target_key, edge_type)
        self._remove_edge(edge)

    def _remove_edge(self, edge):
        self._remove_edge_from_tree(edge, out=True)
        self._remove_edge_from_tree(edge, out=False)
        self._erase_edge_at(edge.position)

    def remove_node(self, key):
        """Remove node and incident edges from graph

        Args:
            key (str): string key of the node to remove
        """
        source = self.node(key)
        edge_start = self._get_edge_at(source.edge_start)

        existing_edges = (
            list(self._edge_out_dfs(edge_start)) +
            list(self._edge_in_dfs(edge_start)))
        for edge in existing_edges:
            edge = self._get_edge_at(edge.position)
            if not edge.exists:
                continue
            self._remove_edge(edge)

        # erase node from file
        self._remove_node_from_tree(source)
        self._erase_node(source)

    def __setitem__(self, key, attr):
        """Create/update node with custom attributes

        Args:
            key (str): node key
            attr (dict): attributes as provided in node_class

        Returns:
            node_class: inserted node
        """
        if isinstance(key, tuple):
            return self.add_edge(key[0], key[1], attr=attr)
        return self.add_node(key, attr=attr)

    # =========================================================================
    # Utils
    # =========================================================================

    def _str_to_list(self, key):
        res = [ord(c) for c in key]
        res += [0] * (self.max_str_len - len(res))
        return res

    def _key_to_list(self, key):
        res = [ord(c) for c in key]
        res += [0] * (self.max_key_len - len(res))
        return res
