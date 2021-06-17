import kinbaku as kn
from tqdm import tqdm

G = kn.Graph("test.db", flag="n", cache_len=100000)

# create random edges
N = 20000000  # number of nodes
for i in tqdm(range(N)):
    G.add_node(str(i))


# i = 0
# for node in tqdm(G.nodes, total=G.n_nodes):
#     # print(node)
#     i += 1
#     # if i > 100:
#     #     break
# print(i)

# print(G.node("1501540"))
