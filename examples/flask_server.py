from dataclasses import asdict

import kinbaku as kn
from flask import Flask, request, jsonify


# create thread-safe database
DB_FILENAME = "test.db"
G = kn.Graph(DB_FILENAME, flag="n")

# create Flask app
app = Flask(__name__)


@app.route("/node")
def n_nodes():
    return jsonify({"n_nodes": G.n_nodes })


@app.route("/edge")
def n_edges():
    return jsonify({"n_nodes": G.n_edges })


@app.route("/node/<u>", methods=["GET", "POST", "PUT", "DELETE"])
def node(u):
    # get node data
    if request.method == "GET":
        try:
            return jsonify(G.node(u).data())
        except kn.exception.NodeNotFound:
            return f"node {u} not found", 404
    # create node
    elif request.method == "POST":
        G.add_node(u)
        return f"node {u} created", 200
    elif request.method == "DELETE":
        G.remove_node(u)
        return f"node {u} removed", 200


@app.route("/edge/<u>/<v>", methods=["GET", "POST", "DELETE"])
def edge(u, v):
    # get edge data
    if request.method == "GET":
        try:
            return asdict(G.edge(u, v))
        except kn.exception.EdgeNotFound:
            return f"edge {u}->{v} not found", 404
    # create edge
    elif request.method == "POST":
        G.add_edge(u, v)
        return f"edge {u}->{v} created", 200
    # delete edge
    elif request.method == "DELETE":
        G.remove_edge(u, v)
        return f"edge {u}->{v} removed", 200


@app.route("/neighbors/<u>", methods=["GET"])
def neighbors(u):
    res = G.neighbors_list(u)
    return jsonify({
        "key": u,
        "degree": len(res),
        "neighbors": res
    })


@app.route("/predecessors/<u>", methods=["GET"])
def predecessors(u):
    res = G.predecessors_list(u)
    return jsonify({
        "key": u,
        "degree": len(res),
        "predecessors": res
    })


if __name__ == "__main__":
    app.run("0.0.0.0", port=9200, debug=True)
