import threading
from dataclasses import asdict
from queue import Queue

import kinbaku as kn
from flask import Flask, request

DB_FILENAME = "test.db"


def write_job(G, q):
    while True:
        instruction, data = q.get()
        if instruction == "node":
            G.add_node(data)
        elif instruction == "edge":
            G.add_edge(*data)
        elif instruction == "remove_node":
            G.remove_node(data)
        elif instruction == "remove_edge":
            G.removed_edge(*data)
        q.task_done()


# create database
G = kn.Graph(DB_FILENAME)

# create writer thread
write_queue = Queue()
thread = threading.Thread(target=write_job, args=(G, write_queue))
thread.start()

# create Flask app
app = Flask(__name__)


@app.route("/node/<u>", methods=["GET", "POST", "PUT", "DELETE"])
def node(u):
    # get node data
    if request.method == "GET":
        try:
            return G.node(u)
        except kn.exception.NodeNotFound:
            return f"node {u} not found", 404
    # create node
    elif request.method == "POST":
        write_queue.put(("node", u))
        return f"node {u} created", 200
    elif request.method == "DELETE":
        write_queue.put(("remove_node", u))
        return f"node {u} removed", 200


@app.route("/edge/<u>/<v>", methods=["GET", "POST", "PUT", "DELETE"])
def edge(u, v):
    # get edge data
    if request.method == "GET":
        try:
            return asdict(G.edge(u, v))
        except kn.exception.EdgeNotFound:
            return f"edge {u}->{v} not found", 404
    # create edge
    elif request.method == "POST":
        write_queue.put(("edge", (u, v)))
        return f"edge {u}->{v} created", 200
    # delete edge
    elif request.method == "DELETE":
        write_queue.put(("remove_edge", (u, v)))
        return f"edge {u}->{v} removed", 200


@app.route("/neighbors/<u>", methods=["GET"])
def neighbors(u):
    return {u: list(G.neighbors(u))}


@app.route("/predecessors/<u>", methods=["GET"])
def predecessors(u):
    return {u: list(G.predecessors(u))}


if __name__ == "__main__":
    app.run("0.0.0.0", port=9200, debug=True)
