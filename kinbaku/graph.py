"""Base class for directed graphs.
The Graph class allows any string as a node
Self-loops are allowed but multiple edges of a same edge type are not.
"""
from .utils import CacheDict, compare_nodes, compare_edge
from .utils import to_string, stringify_key
from .structure import Edge, Node, Header
from struct import unpack, pack, error
import math
import mmap
import os


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
        cache_len=10000000,
        table_increment=100000,
        preload=False,
        node_class=None,
        edge_class=None,
        overwrite=False,
        flag="w"
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
            overwrite (bool, optional): if True, the file is emptyed at
                                        initialization. Defaults to False.
            flag (str, optional): "w" for reading and writing and "r" for
                                  reading only. Defaults to "w".

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
            from cityhash import CityHash32
            self.hash_func = CityHash32
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
        self.cache_id_to_key = {}  # important
        self.cache_key_to_node = CacheDict(cache_len=cache_len)
        self.cache_pos_to_node = CacheDict(cache_len=cache_len)
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
        self._load_file(overwrite)

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
        return self.header.n_edges - self.header.n_nodes

    @property
    def nodes(self):
        position = 0
        leaf = self._get_node_at(position)
        yield from self._node_dfs(leaf, as_key=True)

    @property
    def edges(self):
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
                DATA_VALUES.append(0.)
        return DATA_FORMAT, DATA_VALUES

    def _parse_values(self, data):
        values = []
        for field in data.__dataclass_fields__.keys():
            value = getattr(data, field)
            if field == "key":
                values += self._key_to_tuple(value)
            elif isinstance(value, str):
                values += self._str_to_tuple(value)
            elif isinstance(value, tuple):
                values += list(value)
            else:
                values.append(value)
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
        self.NODE_PLACEHOLDER_SIZE = (
            self.NODE_TO_EDGE_RATIO * self.EDGE_SIZE)
        # pad placeholder with 0s
        self.NODE_PLACEHOLDER = (
            self.NODE +
            b'\x00' * (self.NODE_PLACEHOLDER_SIZE - self.NODE_SIZE))

    def _init_header_size(self):
        HEADER_FORMAT = ""
        HEADER_VALUES = []
        for name, field in Header.__dataclass_fields__.items():
            if field.type == int:
                HEADER_FORMAT += self.int_format
            if name == "table_size":
                HEADER_VALUES.append(self.table_increment +
                                     self.NODE_TO_EDGE_RATIO)
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

    def _load_file(self, overwrite):
        if not os.path.exists(self.filename) or overwrite:
            with open(self.filename, "wb") as f:
                f.write(self.HEADER)
                f.write(self.NODE_PLACEHOLDER)
                f.write(self.EDGE * self.table_increment)
            self._map_to_memory()
            self._get_sizes()
        else:
            self._map_to_memory()
            self._get_sizes()
            if self.preload:
                for _ in self.nodes:
                    pass

    def _map_to_memory(self):
        with open(self.filename, "r+b") as f:
            if self.flag == "w":
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)
            else:
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

    def _expand(self):
        if (
            self.header.next_table_position <=
            self.header.table_size - .1 * self.table_increment
        ):
            self.mm[:self.HEADER_SIZE] = pack(
                self.HEADER_FORMAT, *self._parse_values(self.header))
            return

        with open(self.filename, "ab") as f:
            f.write(self.EDGE * self.table_increment)

        # add increment to table_size
        self.header.table_size += self.table_increment
        self.mm[:self.HEADER_SIZE] = pack(
            self.HEADER_FORMAT,
            *self._parse_values(self.header))
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
        self.cache_key_to_node[node.key] = node
        self.cache_id_to_key[node.index] = node.key
        self.cache_pos_to_node[node.position] = node

    def _uncache_node(self, node):
        try:
            del self.cache_key_to_node[node.key]
            del self.cache_id_to_key[node.index]
            del self.cache_pos_to_node[node.position]
        except KeyError:
            pass

    def empty_cache(self):
        self.cache_id_to_key = {}  # important
        self.cache_key_to_node = CacheDict(cache_len=self.cache_len)
        self.cache_pos_to_node = CacheDict(cache_len=self.cache_len)
        self._get_sizes()

    def find_tombstones(self):
        position = 0
        size = len(pack("?" + self.int_format, 0, 0))
        while position <= self.header.next_table_position:
            ind = position * self.EDGE_SIZE + self.HEADER_SIZE
            is_node, index = unpack("?" + self.int_format,
                                    self.mm[ind:ind+size])
            if is_node:
                if index == 0:
                    self.node_tombstone.append(position)
                position += self.NODE_TO_EDGE_RATIO
            else:
                if index == 0:
                    self.edge_tombstone.append(position)
                position += 1

    # =========================================================================
    # Tree traversal
    # =========================================================================

    def _iter_edges(self):
        position = 0
        while position <= self.header.next_table_position:
            ind = position * self.EDGE_SIZE + self.HEADER_SIZE
            is_node, = unpack("?", self.mm[ind:ind+1])
            if is_node:
                position += self.NODE_TO_EDGE_RATIO
            else:
                edge = self._get_edge_at(position)
                position += 1
                if not edge.exists or edge.is_edge_start:
                    continue
                yield edge

    def _find_edge_out_pos(self, position, new_edge):
        current_edge = self._get_edge_at(position)
        while True:
            state = compare_edge(current_edge, new_edge)
            if state == -1:  # go left
                if current_edge.out_edge_left == 0:
                    break
                else:
                    position = current_edge.out_edge_left
                    current_edge = self._get_edge_at(position)
                    continue
            elif state == 1:  # go right
                if current_edge.out_edge_right == 0:
                    break
                else:
                    position = current_edge.out_edge_right
                    current_edge = self._get_edge_at(position)
                    continue
            else:  # is equal
                break
        return current_edge, state

    def _find_edge_in_pos(self, position, new_edge):
        current_edge = self._get_edge_at(position)
        while True:
            state = compare_edge(current_edge, new_edge)
            if state == -1:  # go left
                if current_edge.in_edge_left == 0:
                    break
                else:
                    position = current_edge.in_edge_left
                    current_edge = self._get_edge_at(position)
                    continue
            elif state == 1:  # go right
                if current_edge.in_edge_right == 0:
                    break
                else:
                    position = current_edge.in_edge_right
                    current_edge = self._get_edge_at(position)
                    continue
            else:  # is equal
                break
        return current_edge, state

    def _find_inorder_successor(self, edge, out=True):
        if out:
            edge_right = edge.out_edge_right
            edge_left_name = "out_edge_left"
        else:
            edge_right = edge.in_edge_right
            edge_left_name = "in_edge_left"

        successor = self._get_edge_at(edge_right)
        antecedent = edge

        while getattr(successor, edge_left_name) != 0:
            antecedent = successor
            successor_position = getattr(successor, edge_left_name)
            successor = self._get_edge_at(successor_position)
        return (successor, antecedent)

    def _find_edge_out_antecedent(self, position, edge):
        current_edge = self._get_edge_at(position)
        while True:
            state = compare_edge(current_edge, edge)
            if state == -1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.out_edge_left
                current_edge = self._get_edge_at(position)
                previous_state = -1
            elif state == 1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.out_edge_right
                current_edge = self._get_edge_at(position)
                previous_state = 1
            else:
                break
            if current_edge.target == 0:
                raise KeyError("Edge does not exist")
        return (
            current_edge, position,
            previous_edge, previous_pos, previous_state)

    def _find_edge_in_antecedent(self, position, edge):
        current_edge = self._get_edge_at(position)
        while True:
            state = compare_edge(current_edge, edge)
            if state == -1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.in_edge_left
                current_edge = self._get_edge_at(position)
                previous_state = -1
            elif state == 1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.in_edge_right
                current_edge = self._get_edge_at(position)
                previous_state = 1
            else:
                break
            if position == 0:
                raise KeyError("Edge does not exist")
        return (
            current_edge, position,
            previous_edge, previous_pos, previous_state)

    def _node_dfs(self, node, as_key=False):
        if node.index != 0:
            if as_key:
                yield node.key
            else:
                yield node

        # store in cache
        self._cache_node(node)

        if node.left != 0:
            yield from self._node_dfs(self._get_node_at(node.left), as_key)
        if node.right != 0:
            yield from self._node_dfs(self._get_node_at(node.right), as_key)

    def _edge_out_dfs(self, edge, edge_position=None, as_key=True):
        if not edge.is_edge_start:
            if as_key:
                res = self._get_node_at(edge.target_position)
                yield res.key
            else:
                yield edge_position, edge

        if edge.out_edge_left != 0:
            yield from self._edge_out_dfs(
                self._get_edge_at(edge.out_edge_left),
                edge_position=edge.out_edge_left,
                as_key=as_key)
        if edge.out_edge_right != 0:
            yield from self._edge_out_dfs(
                self._get_edge_at(edge.out_edge_right),
                edge_position=edge.out_edge_right,
                as_key=as_key)

    def _edge_in_dfs(self, edge):
        if not edge.is_edge_start:
            res = self._get_node_at(edge.source_position)
            yield res.key

        if edge.in_edge_left != 0:
            yield from self._edge_in_dfs(self._get_edge_at(edge.in_edge_left))
        if edge.in_edge_right != 0:
            yield from self._edge_in_dfs(self._get_edge_at(edge.in_edge_right))

    def _rewire_right(self, parent, child_position, out=True):
        if child_position == 0:
            if out:
                parent.out_edge_right = 0
            else:
                parent.in_edge_right = 0
            self._set_edge_at(parent, parent.position)
        else:
            child = self._get_edge_at(child_position)
            if out:
                parent.out_edge_right = child_position
                child.out_edge_parent = parent.position
            else:
                parent.in_edge_right = child_position
                child.in_edge_parent = parent.position
            self._set_edge_at(child, child_position)
            self._set_edge_at(parent, parent.position)

    def _rewire_left(self, parent, child_position, out=True):
        if child_position == 0:
            if out:
                parent.out_edge_left = 0
            else:
                parent.in_edge_left = 0
            self._set_edge_at(parent, parent.position)
        else:
            child = self._get_edge_at(child_position)
            if out:
                parent.out_edge_left = child_position
                child.out_edge_parent = parent.position
            else:
                parent.in_edge_left = child_position
                child.in_edge_parent = parent.position
            self._set_edge_at(parent, parent.position)
            self._set_edge_at(child, child_position)

    def _remove_edge_from_tree(self, edge, out=True):
        # utilitary variables
        if out:
            edge_left = edge.out_edge_left
            edge_right = edge.out_edge_right
            parent = self._get_edge_at(edge.out_edge_parent)

            cond_1 = parent.out_edge_left == edge.position
            cond_2 = parent.out_edge_right == edge.position
            assert cond_1 or cond_2
        else:
            edge_left = edge.in_edge_left
            edge_right = edge.in_edge_right
            parent = self._get_edge_at(edge.in_edge_parent)

            cond_1 = parent.in_edge_left == edge.position
            cond_2 = parent.in_edge_right == edge.position
            assert cond_1 or cond_2
        state = compare_edge(parent, edge)


        # case 1: edge to remove has no child
        if edge_left == 0 and edge_right == 0:
            if state == -1:  # edge came from left
                self._rewire_left(parent, 0)
            else:
                self._rewire_right(parent, 0)
        # case 2: edge to remove has only one child
        elif edge_left == 0:
            if state == -1:
                self._rewire_left(parent, edge_right)
            else:
                self._rewire_right(parent, edge_right)
        elif edge_right == 0:
            if state == -1:
                self._rewire_left(parent, edge_left)
            else:
                self._rewire_right(parent, edge_left)
        # case 3: edge to remove has two children
        else:
            print("\n\nhere\n")
            successor, antecedent = self._find_inorder_successor(edge, out=out)
            print(successor, antecedent)

    # def _remove_edge_from_tree2(
    #     self, edge, edge_pos, antecedent, antecedent_pos, state,
    #     out=True
    # ):
    #     # utils
    #     direction = "out" if out else "in"
    #     edge_left_name = f"{direction}_edge_left"
    #     edge_right_name = f"{direction}_edge_right"
    #     edge_parent_name = f"{direction}_edge_parent"
    #     edge_left = getattr(edge, edge_left_name)
    #     edge_right = getattr(edge, edge_right_name)

    #     # case 1: edge has no children - just remove link to it
    #     if edge_left == 0 and edge_right == 0:
    #         if state == -1:
    #             setattr(antecedent, edge_left_name, 0)
    #         else:
    #             setattr(antecedent, edge_right_name, 0)
    #         self._set_edge_at(antecedent, antecedent_pos)
    #     # case 2: edge has a right child
    #     elif edge_left == 0:
    #         if state == -1:
    #             setattr(antecedent, edge_left_name, edge_right)
    #         else:
    #             setattr(antecedent, edge_right_name, edge_right)
    #         child = self._get_edge_at(edge_right)
    #         setattr(child, edge_parent_name, antecedent_pos)

    #         self._set_edge_at(antecedent, antecedent_pos)
    #         self._set_edge_at(child, edge_right)
    #     # case 2: edge has a left child
    #     elif edge_right == 0:
    #         if state == -1:
    #             setattr(antecedent, edge_left_name, edge_left)
    #         else:
    #             setattr(antecedent, edge_right_name, edge_left)
    #         child = self._get_edge_at(edge_left)
    #         setattr(child, edge_parent_name, antecedent_pos)

    #         self._set_edge_at(antecedent, antecedent_pos)
    #         self._set_edge_at(child, edge_left)
    #     # case 3: edge has two children
    #     else:
    #         successor, successor_pos, successor_ant, successor_ant_pos =    \
    #             self._find_inorder_successor(edge_pos, edge, out=out)

    #         # if successor has no children
    #         if getattr(successor, edge_right_name) == 0:
    #             # replace successor's links
    #             setattr(successor, edge_left_name, edge_left)
    #             # update left child
    #             child = self._get_edge_at(edge_left)
    #             setattr(child, edge_parent_name, successor_pos)
    #             self._set_edge_at(child, edge_left)

    #             # avoid self-loops
    #             if edge_right != successor_pos:
    #                 setattr(successor, edge_right_name, edge_right)
    #                 # update right child
    #                 child = self._get_edge_at(edge_right)
    #                 setattr(child, edge_parent_name, successor_pos)
    #                 self._set_edge_at(child, edge_right)

    #             self._set_edge_at(successor, successor_pos)

    #             # replace successor antecedent's link
    #             if successor_ant_pos != edge_pos:
    #                 setattr(successor_ant, edge_left_name, 0)
    #                 self._set_edge_at(successor_ant, successor_ant_pos)
    #         # if successor antecedent is the original edge
    #         elif successor_ant_pos == edge_pos:
    #             print("1")
    #             # replace successor's links
    #             setattr(successor, edge_left_name, edge_left)
    #             self._set_edge_at(successor, successor_pos)
    #             # update left child
    #             child = self._get_edge_at(edge_left)
    #             setattr(child, edge_parent_name, successor_pos)
    #             self._set_edge_at(child, edge_left)
    #         else:
    #             print("2")
    #             successor_right = getattr(successor, edge_right_name)
    #             setattr(successor_ant, edge_left_name, successor_right)
    #             self._set_edge_at(successor_ant, successor_ant_pos)
    #             # update right successor child
    #             child = self._get_edge_at(successor_right)
    #             setattr(child, edge_parent_name, successor_ant_pos)
    #             self._set_edge_at(child, successor_right)

    #             setattr(successor, edge_right_name, edge_right)
    #             setattr(successor, edge_left_name, edge_left)
    #             self._set_edge_at(successor, successor_pos)

    #             # update right successor child
    #             child = self._get_edge_at(edge_right)
    #             setattr(child, edge_parent_name, successor_pos)
    #             self._set_edge_at(child, edge_right)
    #             # update left successor child
    #             child = self._get_edge_at(edge_left)
    #             setattr(child, edge_parent_name, successor_pos)
    #             self._set_edge_at(child, edge_left)

    #         if state == -1:
    #             setattr(antecedent, edge_left_name, successor_pos)
    #         else:
    #             setattr(antecedent, edge_right_name, successor_pos)
    #         child = self._get_edge_at(successor_pos)
    #         setattr(child, edge_parent_name, antecedent_pos)
    #         self._set_edge_at(child, successor_pos)

    #         self._set_edge_at(antecedent, antecedent_pos)

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

    def _get_node_at(self, position):
        node = self.cache_pos_to_node.get(position)
        if node is not None:
            return node

        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        try:
            data = unpack(self.NODE_FORMAT, self.mm[ind:ind+self.NODE_SIZE])
        except error:
            self._map_to_memory()
            data = unpack(self.NODE_FORMAT, self.mm[ind:ind+self.NODE_SIZE])

        i = 0
        res = []
        for field in self.node_class.__dataclass_fields__.values():
            if field.type != tuple and field.type != str:
                res.append(data[i])
                i += 1
            elif field.name == "key":
                res.append(to_string(data[i:i+self.max_key_len]))
                i += self.max_key_len
            elif field.type == str:
                res.append(to_string(data[i:i+self.max_str_len]))
                i += self.max_str_len
        node = self.node_class(*res)

        if node.index not in self.cache_id_to_key:
            self._cache_node(node)
        return node

    def _get_edge_at(self, position):
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        data = unpack(self.EDGE_FORMAT, self.mm[ind:ind+self.EDGE_SIZE])
        edge = self.edge_class(*data)
        return edge

    def _get_edge_hash(self, source, target, edge_type):
        return self.hash_func(
            str(source.hash) + "_" + str(edge_type) + "_" + str(target.hash))

    def _get_keys_from_edge(self, edge):
        src_pos = edge.source_position
        u = self._get_node_at(src_pos).key

        tgt_pos = edge.target_position
        v = self._get_node_at(tgt_pos).key
        return u, v

    @stringify_key
    def neighbors(self, key):
        leaf = self.node(key)
        start = self._get_edge_at(leaf.edge_start)
        yield from self._edge_out_dfs(start)

    @stringify_key
    def predecessors(self, key):
        leaf = self.node(key)
        start = self._get_edge_at(leaf.edge_start)
        yield from self._edge_in_dfs(start)

    @stringify_key
    def degree(self, key):
        # returns out-degree
        return len(self.neighbors(key))

    @stringify_key
    def in_degree(self, key):
        # returns in-degree
        return len(self.predecessors(key))

    @stringify_key
    def node(self, key):
        # if key is already a node object
        if isinstance(key, self.node_class):
            return key

        # if key is in cache
        result = self.cache_key_to_node.get(key)
        if result is not None:
            return result

        key_hash = self.hash_func(key)

        # if root is empty, set first value
        position = 0
        leaf = self._get_node_at(position)
        state = compare_nodes(key_hash, key, leaf)

        while state != 0:
            if state == -1:
                if leaf.left == 0:
                    raise KeyError(f"Node {key} does not exist")
                leaf = self._get_node_at(leaf.left)
            else:
                if leaf.right == 0:
                    raise KeyError(f"Node {key} does not exist")
                leaf = self._get_node_at(leaf.right)
            state = compare_nodes(key_hash, key, leaf)
        self._cache_node(leaf)
        return leaf

    def edge(self, source, target, edge_type=0):
        # stringify inputs
        if not isinstance(source, str):
            source = str(source)
        if not isinstance(target, str):
            target = str(target)

        # get source and target nodes
        source = self.node(source)
        target = self.node(target)

        # new edge to create
        new_edge = self.edge_class()
        new_edge.source_position = source.position
        new_edge.target_position = target.position
        new_edge.hash = self._get_edge_hash(source, target, edge_type)
        new_edge.type = edge_type

        edge, state = self._find_edge_out_pos(
            source.edge_start, new_edge)

        # edge already exists
        if state == 0:
            return edge
        raise KeyError(f"Edge {source.key} -> {target.key} not found")

    def __getitem__(self, key):
        return self.node(key)

    # =========================================================================
    # Setters
    # =========================================================================

    def _set_node_at(self, leaf, position):
        values = self._parse_values(leaf)
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.NODE_SIZE] = pack(self.NODE_FORMAT, *values)
        self._cache_node(leaf)

    def _set_edge_at(self, edge, position):
        values = self._parse_values(edge)
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.EDGE_SIZE] = pack(self.EDGE_FORMAT, *values)

    def _erase_edge_at(self, position):
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.EDGE_SIZE] = self.EDGE
        self.edge_tombstone.append(position)
        self._decrement_edge()

    def _erase_node(self, node):
        self._uncache_node(node)
        ind = node.position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.NODE_SIZE] = self.NODE
        self.node_tombstone.append(node.position)
        self._decrement_node()
        # also remove edge start
        ind = node.edge_start * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.EDGE_SIZE] = self.EDGE
        self.edge_tombstone.append(node.edge_start)
        self._decrement_edge()

    # =========================================================================
    # Create & delete Nodes & Edges
    # =========================================================================

    @stringify_key
    def add_node(self, key, attr=None):
        key_hash = self.hash_func(key)

        # check if root node is empty
        position = 0
        leaf = self._get_node_at(position)
        if leaf.index == 0:
            leaf.index = self.header.node_id
            leaf.key = key
            leaf.hash = key_hash
            leaf.edge_start, recycled = self._get_next_edge_position()
            self._parse_attributes(leaf, attr)

            self._set_node_at(leaf, position)
            self._increment_node(False)

            # create self-loop edge for new nodes
            edge = self.edge_class()
            edge.source_position = 0
            edge.hash = key_hash
            edge.is_edge_start = True
            edge.position = leaf.edge_start
            self._set_edge_at(edge, edge.position)
            self._increment_edge(recycled)
            return leaf

        # next node insertion position
        new_position, node_recycled = self._get_next_node_position()

        # new node
        node = self.node_class()
        node.index = self.header.node_id
        node.position = new_position
        node.key = key
        node.hash = key_hash
        self._parse_attributes(node, attr)

        # new edge
        edge = self.edge_class()
        edge.source_position = new_position
        edge.hash = node.hash
        edge.is_edge_start = True

        if key in self.cache_key_to_node:
            leaf = self.cache_key_to_node[key]
            position = leaf.position

        # unroll tree
        while True:
            state = compare_nodes(key_hash, key, leaf)
            if state == -1:  # go left
                if leaf.left == 0:  # and leaf is empty
                    left = True
                    break
                else:
                    position = leaf.left
                    leaf = self._get_node_at(position)
                    continue
            elif state == 1:  # go right
                if leaf.right == 0:  # and leaf is empty
                    left = False
                    break
                else:
                    position = leaf.right
                    leaf = self._get_node_at(position)
                    continue
            else:  # is equal
                if node_recycled:
                    self.node_tombstone.append(new_position)
                node.index = leaf.index
                node.left = leaf.left
                node.right = leaf.right
                node.edge_start = leaf.edge_start
                node.position = leaf.position
                node.parent = leaf.parent
                if node == leaf:
                    return leaf
                self._set_node_at(node, position)
                return node

        # update sizes
        self._increment_node(node_recycled)

        # add new node
        node.edge_start, recycled = self._get_next_edge_position()
        node.parent = leaf.position
        self._set_node_at(node, new_position)

        # add new edge for the node
        edge.position = node.edge_start
        self._set_edge_at(edge, node.edge_start)
        self._increment_edge(recycled)

        # update the node before: link to left or right tree
        if left:
            leaf.left = new_position
        else:
            leaf.right = new_position
        self._set_node_at(leaf, position)
        return node

    def add_edge(self, source_key, target_key, attr=None, edge_type=0):
        # get source and target
        source = self.cache_key_to_node.get(source_key)
        target = self.cache_key_to_node.get(target_key)

        # create nodes if necessary
        if source is None:
            source = self.add_node(source_key)
        if target is None:
            target = self.add_node(target_key)

        # new edge to create
        new_edge = self.edge_class()
        new_edge.source_position = source.position
        new_edge.target_position = target.position
        new_edge.hash = self._get_edge_hash(source, target, edge_type)
        new_edge.type = edge_type

        # =====================================================================
        # OUT direction
        prev_out, state = self._find_edge_out_pos(source.edge_start, new_edge)

        if state == 0:  # edge already exists
            return prev_out
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
            raise ValueError("strange")
        # update previous in-edge
        self._set_edge_at(prev_in, prev_in.position)

        # =====================================================================
        # insert new edge
        new_edge.out_edge_parent = prev_out.position
        new_edge.in_edge_parent = prev_in.position
        self._set_edge_at(new_edge, new_edge_position)
        self._increment_edge(recycled)

        return new_edge

    def remove_edge(self, source, target, edge_type=0):
        edge = self.edge(source, target, edge_type)
        self._remove_edge(edge)

    def _remove_edge(self, edge):
        self._remove_edge_from_tree(edge, out=True)
        self._remove_edge_from_tree(edge, out=False)
        self._erase_edge_at(edge.position)

    def remove_node(self, source):
        source = self.node(source)
        start = self._get_edge_at(source.edge_start)
        for edge_pos, edge in self._edge_out_dfs(start, as_key=False):
            target = self.cache_pos_to_node.get(edge.target_position)
            if target is None:
                target = self._get_node_at(edge.target_position)

            # =================================================================
            # OUT direction
            _, _, edge_out_ant, edge_out_ant_pos, state =   \
                self._find_edge_out_antecedent(source.edge_start, edge)
            self._remove_edge_from_tree(edge, out=True)

            # =================================================================
            # IN direction
            _, _, edge_in_ant, edge_in_ant_pos, state = \
                self._find_edge_in_antecedent(target.edge_start, edge)
            self._remove_edge_from_tree(edge, out=False)

            # erase edge from file
            self._erase_edge_at(edge_pos)

        # erase node from file
        self._erase_node(source)

    def __setitem__(self, key, attr):
        return self.add_node(key, attr)

    # =========================================================================
    # Utils
    # =========================================================================

    def _str_to_tuple(self, key):
        res = tuple(ord(c) for c in key)
        res += (0,) * (self.max_str_len - len(res))
        return res

    def _key_to_tuple(self, key):
        res = tuple(ord(c) for c in key)
        res += (0,) * (self.max_key_len - len(res))
        return res
