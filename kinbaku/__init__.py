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
        preload=False,
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

    def increment_node(self, recycled):
        self.header.n_nodes += 1
        self.header.node_id += 1
        if recycled:
            self.header.next_table_position += self.NODE_TO_EDGE_RATIO
        self.expand()

    def increment_edge(self, recycled):
        self.header.n_edges += 1
        if not recycled:
            self.header.next_table_position += 1
        self.expand()

    def decrement_edge(self):
        self.header.n_edges -= 1
        self.expand()

    def cache_node(self, key, node):
        self.cache_key_to_node[key] = node
        self.cache_id_to_key[node.index] = key
        self.cache_pos_to_node[node.position] = node

    # =========================================================================
    # Tree traversal
    # =========================================================================

    def find_edge_out_pos(self, position, new_edge):
        current_edge = self.get_edge_at(position)
        while True:
            state = compare_edge(current_edge, new_edge)
            if state == -1:  # go left
                # if left out edge is empty
                if current_edge.out_edge_left == 0:
                    break
                else:
                    position = current_edge.out_edge_left
                    current_edge = self.get_edge_at(position)
                    continue
            elif state == 1:  # go right
                # if right out edge is empty
                if current_edge.out_edge_right == 0:
                    break
                else:
                    position = current_edge.out_edge_right
                    current_edge = self.get_edge_at(position)
                    continue
            else:  # is equal
                break
        return current_edge, position, state

    def find_edge_in_pos(self, position, new_edge):
        current_edge = self.get_edge_at(position)
        while True:
            state = compare_edge(current_edge, new_edge)
            if state == -1:  # go left
                if current_edge.in_edge_left != 0:
                    position = current_edge.in_edge_left
                    current_edge = self.get_edge_at(position)
                    continue
                else:
                    break
            elif state == 1:  # go right
                # if right in edge is empty
                if current_edge.in_edge_right != 0:
                    position = current_edge.in_edge_right
                    current_edge = self.get_edge_at(position)
                    continue
                else:
                    break
            else:  # is equal
                break
        return current_edge, position, state
    
    def find_inorder_successor(self, position, edge, out=True):
        if out:
            edge_right = edge.out_edge_right
            edge_left_name = "out_edge_left"
        else:
            edge_right = edge.in_edge_right
            edge_left_name = "in_edge_left"

        successor_position = edge_right
        successor_edge = self.get_edge_at(successor_position)

        antecedent_position = position
        antecedent = edge
        while getattr(successor_edge, edge_left_name) != 0:
            antecedent = successor_edge
            antecedent_position = successor_position

            successor_position = getattr(successor_edge, edge_left_name)
            successor_edge = self.get_edge_at(successor_position)
        return (
            successor_edge, successor_position,
            antecedent, antecedent_position)

    def find_edge_out_antecedent(self, position, edge):
        current_edge = self.get_edge_at(position)
        while True:
            state = compare_edge(current_edge, edge)
            if state == -1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.out_edge_left
                current_edge = self.get_edge_at(position)
                previous_state = -1
            elif state == 1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.out_edge_right
                current_edge = self.get_edge_at(position)
                previous_state = 1
            else:
                break
            if current_edge.target == 0:
                raise KeyError("Edge does not exist")
        return (
            current_edge, position,
            previous_edge, previous_pos, previous_state)

    def find_edge_in_antecedent(self, position, edge):
        current_edge = self.get_edge_at(position)
        while True:
            state = compare_edge(current_edge, edge)
            if state == -1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.in_edge_left
                current_edge = self.get_edge_at(position)
                previous_state = -1
            elif state == 1:
                previous_edge = current_edge
                previous_pos = position
                position = current_edge.in_edge_right
                current_edge = self.get_edge_at(position)
                previous_state = 1
            else:
                break
            if position == 0:
                raise KeyError("Edge does not exist")
        return (
            current_edge, position,
            previous_edge, previous_pos, previous_state)


    def node_dfs(self, node):
        if node.index != 0:
            yield node

        # store in cache
        self.cache_node(node.key, node)

        if node.left != 0:
            yield from self.node_dfs(self.get_node_at(node.left))
        if node.right != 0:
            yield from self.node_dfs(self.get_node_at(node.right))

    def edge_dfs(self, edge):
        if edge.target != 0:
            res = self.cache_id_to_key.get(edge.target)
            if res is None:
                res = self.get_node_at(edge.target_position).key
            yield res
            # yield edge

        if edge.out_edge_left != 0:
            yield from self.edge_dfs(self.get_edge_at(edge.out_edge_left))
        if edge.out_edge_right != 0:
            yield from self.edge_dfs(self.get_edge_at(edge.out_edge_right))

    def edge_in_dfs(self, edge):
        if edge.target != 0:
            res = self.cache_id_to_key.get(edge.source)
            if res is None:
                res = self.get_node_at(edge.source_position).key
            yield res
            # yield edge

        if edge.in_edge_left != 0:
            yield from self.edge_in_dfs(self.get_edge_at(edge.in_edge_left))
        if edge.in_edge_right != 0:
            yield from self.edge_in_dfs(self.get_edge_at(edge.in_edge_right))

    # =========================================================================
    # Getters
    # =========================================================================

    def get_next_node_position(self):
        if len(self.node_tombstone) == 0:
            return self.header.next_table_position, False
        return self.node_tombstone.pop(0), True

    def get_next_edge_position(self):
        if len(self.edge_tombstone) == 0:
            recycled = False
            return self.header.next_table_position, recycled
        recycled = True
        return self.edge_tombstone.pop(0), recycled

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
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        data = unpack(self.EDGE_FORMAT, self.mm[ind:ind+self.EDGE_SIZE])
        edge = self.edge_class(*data)
        return edge

    def get_edge_hash(self, source, target, edge_type):
        return self.hash_func(
            str(source.hash) + "_" + str(edge_type) + "_" + str(target.hash))

    @stringify_key
    def neighbors(self, key):
        leaf = self.node(key)
        start = self.get_edge_at(leaf.edge_start)
        yield from self.edge_dfs(start)

    @stringify_key
    def predecessors(self, key):
        leaf = self.node(key)
        start = self.get_edge_at(leaf.edge_start)
        yield from self.edge_in_dfs(start)

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
                    raise KeyError(f"Node {key} does not exist")
                leaf = self.get_node_at(leaf.left)
            else:
                if leaf.right == 0:
                    raise KeyError(f"Node {key} does not exist")
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

    def erase_edge_at(self, position):
        ind = position * self.EDGE_SIZE + self.HEADER_SIZE
        self.mm[ind:ind+self.EDGE_SIZE] = self.EDGE
        self.edge_tombstone.append(position)
        self.decrement_edge()

    # =========================================================================
    # Create & delete Nodes & Edges
    # =========================================================================

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
            leaf.edge_start, recycled = self.get_next_edge_position()
            self.parse_attributes(leaf, attr)

            self.set_node_at(leaf, position)
            self.increment_node(False)

            # create self-loop edge for new nodes
            edge = self.edge_class()
            edge.source = leaf.index
            # edge.source_position = leaf.position
            edge.hash = key_hash
            self.set_edge_at(edge, leaf.edge_start)
            self.increment_edge(recycled)
            return leaf

        # next node insertion position
        new_position, node_recycled = self.get_next_node_position()

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
                if node_recycled:
                    self.node_tombstone.append(new_position)
                node.index = leaf.index
                node.left = leaf.left
                node.right = leaf.right
                node.edge_start = leaf.edge_start
                node.position = leaf.position
                if node == leaf:
                    return leaf
                self.set_node_at(node, position)
                return node

        # update sizes
        self.increment_node(node_recycled)

        # add new node
        node.edge_start, recycled = self.get_next_edge_position()
        self.set_node_at(node, new_position)

        # add new edge for the node
        self.set_edge_at(edge, node.edge_start)
        self.increment_edge(recycled)

        # update the node before: link to left or right tree
        if left:
            leaf.left = new_position
        else:
            leaf.right = new_position
        self.set_node_at(leaf, position)
        return node

    def add_edge(self, source, target, attr=None, edge_type=0):
        # stringify inputs
        if not isinstance(source, str):
            source = str(source)
        if not isinstance(target, str):
            target = str(target)

        # get source and target nodes
        if source in self.cache_key_to_node:
            source = self.cache_key_to_node[source]
        else:
            source = self.add_node(source)
        if target in self.cache_key_to_node:
            target = self.cache_key_to_node[target]
        else:
            target = self.add_node(target)

        # new edge to create
        new_edge = self.edge_class()
        new_edge.source = source.index
        new_edge.target = target.index
        new_edge.source_position = source.position
        new_edge.target_position = target.position
        new_edge.hash = self.get_edge_hash(source, target, edge_type)
        new_edge.type = edge_type
        self.parse_attributes(new_edge, attr)

        # =====================================================================
        # OUT direction
        previous_out_edge, previous_out_pos, state = self.find_edge_out_pos(
            source.edge_start, new_edge)
        
        # edge already exists
        if state == 0:
            return previous_out_edge

        new_edge_position, recycled = self.get_next_edge_position()
        if state == -1:  # must insert left
            previous_out_edge.out_edge_left = new_edge_position
        elif state == 1:  # must insert right
            previous_out_edge.out_edge_right = new_edge_position
        # add OUT edge
        self.set_edge_at(previous_out_edge, previous_out_pos)

        # =====================================================================
        # IN direction
        previous_in_edge, previous_in_pos, state = self.find_edge_in_pos(
            target.edge_start, new_edge)

        if state == -1:
            previous_in_edge.in_edge_left = new_edge_position
        elif state == 1:
            previous_in_edge.in_edge_right = new_edge_position
        else:  # edge shouldn't exist
            print(previous_in_edge)
            print(new_edge)
            raise ValueError("strange")
        # add IN edge
        self.set_edge_at(previous_in_edge, previous_in_pos)

        # add new edge
        self.set_edge_at(new_edge, new_edge_position)
        self.increment_edge(recycled)
        return new_edge
    
    def remove_edge_tree(
        self, edge, edge_pos, antecedent, antecedent_pos, state,
        out=True
    ):
        if out:
            edge_left = edge.out_edge_left
            edge_right = edge.out_edge_right
            edge_left_name = "out_edge_left"
            edge_right_name = "out_edge_right"
        else:
            edge_left = edge.in_edge_left
            edge_right = edge.in_edge_right
            edge_left_name = "in_edge_left"
            edge_right_name = "in_edge_right"

        # case 1: edge has no children - just remove link to it
        if edge_left == 0 and edge_right == 0:
            if state == -1:
                setattr(antecedent, edge_left_name, 0)
            else:
                setattr(antecedent, edge_right_name, 0)
            self.set_edge_at(antecedent, antecedent_pos)
        # case 2: edge has no left child or no right child
        elif edge_left == 0:
            if state == -1:
                setattr(antecedent, edge_left_name, edge_right)
            else:
                setattr(antecedent, edge_right_name, edge_right)
            self.set_edge_at(antecedent, antecedent_pos)
        elif edge_right == 0:
            if state == -1:
                setattr(antecedent, edge_left_name, edge_left)
            else:
                setattr(antecedent, edge_right_name, edge_left)
            self.set_edge_at(antecedent, antecedent_pos)
        # case 3: edge has two children
        else:
            successor, successor_pos, successor_ant, successor_ant_pos =    \
                self.find_inorder_successor(edge_pos, edge, out=out)

            # if successor has no children
            if getattr(successor, edge_right_name) == 0:
                # replace successor's links
                setattr(successor, edge_left_name, edge_left)
                # avoid self-loops
                if edge_right != successor_pos:
                    setattr(successor, edge_right_name, edge_right)
                self.set_edge_at(successor, successor_pos)
                # replace successor antecedent's link
                if successor_ant_pos != edge_pos:
                    setattr(successor_ant, edge_left_name, 0)
                    self.set_edge_at(successor_ant, successor_ant_pos)
            # if successor antecedent is the original edge
            elif successor_ant_pos == edge_pos:
                # replace successor's links
                setattr(successor, edge_left_name, edge_left)
                self.set_edge_at(successor, successor_pos)
            else:                
                successor_right = getattr(successor, edge_right_name)
                setattr(successor_ant, edge_left_name, successor_right)
                self.set_edge_at(successor_ant, successor_ant_pos)
                setattr(successor, edge_right_name, edge_right)
                setattr(successor, edge_left_name, edge_left)
                self.set_edge_at(successor, successor_pos)

            if state == -1:
                setattr(antecedent, edge_left_name, successor_pos)
            else:
                setattr(antecedent, edge_right_name, successor_pos)
            self.set_edge_at(antecedent, antecedent_pos)

    def remove_edge(self, source, target, attr=None, edge_type=0):
        # get source and target nodes
        source = self.node(source)
        target = self.node(target)

        # edge to find
        edge = self.edge_class()
        edge.source = source.index
        edge.target = target.index
        edge.hash = self.get_edge_hash(source, target, edge_type)
        edge.type = edge_type

        # =====================================================================
        # OUT direction
        edge, edge_pos, edge_out_ant, edge_out_ant_pos, state =   \
            self.find_edge_out_antecedent(source.edge_start, edge)
        self.remove_edge_tree(
            edge, edge_pos, edge_out_ant, edge_out_ant_pos, state, out=True)

        # =====================================================================
        # IN direction
        _, _, edge_in_ant, edge_in_ant_pos, state = self.find_edge_in_antecedent(
            target.edge_start, edge)
        self.remove_edge_tree(
            edge, edge_pos, edge_in_ant, edge_in_ant_pos, state, out=False)
        
        # erase edge from file
        self.erase_edge_at(edge_pos)

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
