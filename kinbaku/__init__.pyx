from .utils import CacheDict, compare_nodes, compare_edge
from .utils import to_string, stringify_key
from .structure import Edge, Node, Header
from struct import unpack, pack
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
        preload=True,
        node_class=None,
        edge_class=None
    ):
        self.filename = filename
        self.table_increment = table_increment

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
        self.cache_id_to_key = {}  # important
        self.cache_key_to_node = CacheDict(cache_len=cache_len)
        self.cache_pos_to_node = CacheDict(cache_len=cache_len)
        self.cache_pos_to_edge = CacheDict(cache_len=cache_len)

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
        self.init_edge_size()
        self.init_node_size()
        self.init_header_size()

        # mmap file
        self.load_file()

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def n_nodes(self):
        return self.header.n_nodes

    @property
    def n_edges(self):
        return self.header.n_edges - self.header.n_nodes

    @property
    def nodes(self):
        position = 0
        leaf = self.get_node_at(position)
        yield from self.node_dfs(leaf)

    @property
    def edges(self):
        position = 0
        while position <= self.header.next_table_position:
            ind = position * self.EDGE_SIZE + self.HEADER_SIZE
            is_node, = unpack("?", self.mm[ind:ind+1])
            if is_node:
                position += self.NODE_TO_EDGE_RATIO
            else:
                edge = self.get_edge_at(position)
                position += 1
                if edge.target == 0:
                    continue
                yield edge

    def node_dfs(self, node):
        if node.index != 0:
            yield node

        # store in cache
        self.cache_node(node.key, node)

        if node.left != 0:
            yield from self.node_dfs(self.get_node_at(node.left))
        if node.right != 0:
            yield from self.node_dfs(self.get_node_at(node.right))

    # =========================================================================
    # Parsers
    # =========================================================================

    def parse_fields(self, data):
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

    def parse_values(self, data):
        values = []
        for field in data.__dataclass_fields__.keys():
            value = getattr(data, field)
            if field == "key":
                values += self.key_to_tuple(value)
            elif isinstance(value, str):
                values += self.str_to_tuple(value)
            elif isinstance(value, tuple):
                values += list(value)
            else:
                values.append(value)
        return values

    def parse_attributes(self, leaf, attr):
        if attr is None:
            return
        for attribute, value in attr.items():
            if isinstance(value, str):
                assert len(value) <= self.max_str_len
            setattr(leaf, attribute, value)

    # =========================================================================
    # Initializers
    # =========================================================================

    def init_edge_size(self):
        self.EDGE_FORMAT, VALUES = self.parse_fields(self.edge_class)
        self.EDGE = pack(self.EDGE_FORMAT, *VALUES)
        self.EDGE_SIZE = len(self.EDGE)

    def init_node_size(self):
        self.NODE_FORMAT, VALUES = self.parse_fields(self.node_class)
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

    def init_header_size(self):
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

    def load_file(self):
        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as f:
                f.write(self.HEADER)
                f.write(self.NODE_PLACEHOLDER)
                f.write(self.EDGE * self.table_increment)
            self.map_to_memory()
            self.get_sizes()
        else:
            self.map_to_memory()
            self.get_sizes()
            if self.preload:
                for _ in self.nodes:
                    pass

    def map_to_memory(self):
        with open(self.filename, "r+b") as f:
            self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)

    def expand(self):
        if self.header.next_table_position <= self.header.table_size - 4:
            self.mm[:self.HEADER_SIZE] = pack(
                self.HEADER_FORMAT,
                *self.parse_values(self.header))
            return

        with open(self.filename, "ab") as f:
            f.write(self.EDGE * self.table_increment)

        # add increment to table_size
        self.header.table_size += self.table_increment
        self.mm[:self.HEADER_SIZE] = pack(
            self.HEADER_FORMAT,
            *self.parse_values(self.header))
        self.map_to_memory()

    def increment_node(self):
        self.header.n_nodes += 1
        self.header.node_id += 1
        self.header.next_table_position += self.NODE_TO_EDGE_RATIO
        self.expand()

    def increment_edge(self):
        self.header.n_edges += 1
        self.header.next_table_position += 1
        self.expand()

    def cache_node(self, key, node):
        self.cache_key_to_node[key] = node
        self.cache_id_to_key[node.index] = key
        self.cache_pos_to_node[node.position] = node

    def cache_edge(self, edge, pos):
        self.cache_pos_to_edge[pos] = edge

    # =========================================================================
    # Getters
    # =========================================================================

    def get_next_position(self):
        return self.header.next_table_position

    def get_sizes(self):
        data = unpack(self.HEADER_FORMAT, self.mm[:self.HEADER_SIZE])
        self.header = Header(*data)

    def get_node_at(self, position):
        node = self.cache_pos_to_node.get(position)
        if node is not None:
            return node

        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
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
            self.cache_node(node.key, node)
        return node

    def get_edge_at(self, position):
        # edge = self.cache_pos_to_edge.get(position)
        # if edge is not None:
        #     return edge
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        data = unpack(self.EDGE_FORMAT, self.mm[ind:ind+self.EDGE_SIZE])
        edge = self.edge_class(*data)
        # self.cache_edge(edge, position)
        return edge

    @stringify_key
    def neighbors(self, key):
        leaf = self.node(key)
        start = self.get_edge_at(leaf.edge_start)
        return self.edge_dfs(start)

    @stringify_key
    def predecessors(self, key):
        leaf = self.node(key)
        start = self.get_edge_at(leaf.edge_start)
        return self.edge_in_dfs(start)

    def edge_dfs(self, edge):
        if edge.target == 0:
            this = []
        elif self.preload:
            this = [self.cache_id_to_key[edge.target]]
        else:
            this = [edge.target]

        # terminal node
        if edge.out_edge_left == 0 and edge.out_edge_right == 0:
            return this

        if edge.out_edge_left != 0:
            this += self.edge_dfs(self.get_edge_at(edge.out_edge_left))
        if edge.out_edge_right != 0:
            this += self.edge_dfs(self.get_edge_at(edge.out_edge_right))
        return this

    def edge_in_dfs(self, edge):
        if edge.target == 0:
            this = []
        elif self.preload:
            this = [self.cache_id_to_key[edge.source]]
        else:
            this = [edge.source]

        # terminal node
        if edge.in_edge_left == 0 and edge.in_edge_right == 0:
            return this

        if edge.in_edge_left != 0:
            this += self.edge_in_dfs(self.get_edge_at(edge.in_edge_left))
        if edge.in_edge_right != 0:
            this += self.edge_in_dfs(self.get_edge_at(edge.in_edge_right))
        return this

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
        # if key is in cache
        result = self.cache_key_to_node.get(key)
        if result is not None:
            return result

        key_hash = self.hash_func(key)

        # if root is empty, set first value
        position = 0
        leaf = self.get_node_at(position)
        state = compare_nodes(key_hash, key, leaf)

        while state != 0:
            if state == -1:
                if leaf.left == 0:
                    raise KeyError(f"{key} do no exist")
                leaf = self.get_node_at(leaf.left)
            else:
                if leaf.right == 0:
                    raise KeyError(f"{key} do no exist")
                leaf = self.get_node_at(leaf.right)
            state = compare_nodes(key_hash, key, leaf)
        self.cache_node(key, leaf)
        return leaf

    def __getitem__(self, key):
        return self.node(key)

    # =========================================================================
    # Setters
    # =========================================================================

    def set_node_at(self, leaf, position):
        values = self.parse_values(leaf)
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.NODE_SIZE] = pack(self.NODE_FORMAT, *values)
        self.cache_node(leaf.key, leaf)

    def set_edge_at(self, edge, position):
        values = self.parse_values(edge)
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.EDGE_SIZE] = pack(self.EDGE_FORMAT, *values)
        # self.cache_edge(edge, position)

    # =========================================================================
    # Create & delete
    # =========================================================================

    def add_edge(self, source, target, attr=None, edge_type=0):
        # stringify inputs
        if not isinstance(source, str):
            source = str(source)
        if not isinstance(target, str):
            target = str(target)

        # get source and target nodes
        try:
            source = self.cache_key_to_node[source]
        except KeyError:
            source = self.add_node(source)
        try:
            target = self.cache_key_to_node[target]
        except KeyError:
            target = self.add_node(target)

        # out edge hash
        edge_hash = self.hash_func(
            str(source.hash) + str(edge_type) + str(target.hash))

        # new edge to create
        target_index = target.index
        new_edge = self.edge_class()
        new_edge.source = source.index
        new_edge.target = target_index
        new_edge.hash = edge_hash
        self.parse_attributes(new_edge, attr)

        # =====================================================================
        # OUT direction
        position = source.edge_start
        current_edge = self.get_edge_at(position)
        while True:
            state = compare_edge(
                current_edge, target_index, edge_hash, edge_type)
            if state == -1:  # go left
                # if left out edge is empty
                if current_edge.out_edge_left == 0:
                    left = True
                    break
                else:
                    position = current_edge.out_edge_left
                    current_edge = self.get_edge_at(position)
                    continue
            elif state == 1:  # go right
                # if right out edge is empty
                if current_edge.out_edge_right == 0:
                    left = False
                    break
                else:
                    position = current_edge.out_edge_right
                    current_edge = self.get_edge_at(position)
                    continue
            else:  # is equal
                return current_edge

        new_edge_position = self.get_next_position()
        if left:
            current_edge.out_edge_left = new_edge_position
        else:
            current_edge.out_edge_right = new_edge_position
        self.set_edge_at(current_edge, position)
        self.set_edge_at(new_edge, new_edge_position)
        self.increment_edge()

        # =====================================================================
        # IN direction
        source_index = source.index
        position = target.edge_start
        current_edge = self.get_edge_at(position)
        while True:
            state = compare_edge(
                current_edge, source_index, edge_hash, edge_type)
            if state == -1:  # go left
                # if left out edge is empty
                if current_edge.in_edge_left == 0:
                    left = True
                    break
                else:
                    position = current_edge.in_edge_left
                    current_edge = self.get_edge_at(position)
                    continue
            elif state == 1:  # go right
                # if right out edge is empty
                if current_edge.in_edge_right == 0:
                    left = False
                    break
                else:
                    position = current_edge.in_edge_right
                    current_edge = self.get_edge_at(position)
                    continue
        if left:
            current_edge.in_edge_left = new_edge_position
        else:
            current_edge.in_edge_right = new_edge_position
        self.set_edge_at(current_edge, position)

        return new_edge

    @stringify_key
    def add_node(self, key, attr=None):
        key_hash = self.hash_func(key)

        # check if root node is empty
        position = 0
        leaf = self.get_node_at(position)
        if leaf.index == 0:
            leaf.index = self.header.node_id
            leaf.key = key
            leaf.hash = key_hash
            leaf.position = position
            leaf.edge_start = self.get_next_position()
            self.parse_attributes(leaf, attr)

            self.set_node_at(leaf, position)
            self.increment_node()

            # create self-loop edge for new nodes
            edge = self.edge_class()
            edge.source = leaf.index
            edge.target = 0
            edge.hash = key_hash
            self.set_edge_at(edge, leaf.edge_start)
            self.increment_edge()
            return leaf

        # next node insertion position
        new_position = self.get_next_position()

        # new node
        node = self.node_class()
        node.index = self.header.node_id
        node.position = new_position
        node.key = key
        node.hash = key_hash
        self.parse_attributes(node, attr)

        # new edge
        edge = self.edge_class()
        edge.source = node.index
        edge.target = 0
        edge.hash = node.hash

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
                    leaf = self.get_node_at(position)
                    continue
            elif state == 1:  # go right
                if leaf.right == 0:  # and leaf is empty
                    left = False
                    break
                else:
                    position = leaf.right
                    leaf = self.get_node_at(position)
                    continue
            else:  # is equal
                node.index = leaf.index
                node.left = leaf.left
                node.right = leaf.right
                node.edge_start = leaf.edge_start
                if node == leaf:
                    return leaf
                self.set_node_at(node, position)
                return node

        # update sizes
        self.increment_node()

        # add new node
        node.edge_start = self.get_next_position()
        self.set_node_at(node, new_position)

        # add new edge for the node
        self.set_edge_at(edge, node.edge_start)
        self.increment_edge()

        # update the node before: link to left or right tree
        if left:
            leaf.left = new_position
        else:
            leaf.right = new_position
        self.set_node_at(leaf, position)
        return node

    def __setitem__(self, key, attr):
        return self.add_node(key, attr)

    # =========================================================================
    # Utils
    # =========================================================================

    def str_to_tuple(self, key):
        res = tuple(ord(c) for c in key)
        res += (0,) * (self.max_str_len - len(res))
        return res

    def key_to_tuple(self, key):
        res = tuple(ord(c) for c in key)
        res += (0,) * (self.max_key_len - len(res))
        return res
