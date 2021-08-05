import queue
import threading


def parallel_task(G, nodes, task, n_jobs=-1):
    q = queue.Queue()

    if n_jobs == -1:
        import multiprocessing as mp
        n_jobs = mp.cpu_count() * 5

    d = {}
    for _ in range(n_jobs):
        if task == "neighbors":
            thread = threading.Thread(
                target=target_neighbors, args=(q, d, G))
        else:
            thread = threading.Thread(
                target=target_predecessors, args=(q, d, G))
        thread.daemon = True
        thread.start()

    for node in nodes:
        q.put_nowait(node)
    q.join()
    return {}


def target_neighbors(q, d, G):
    while True:
        try:
            item = q.get()
        except queue.Empty:
            return
        d[item] = list(G.neighbors(item))
        q.task_done()


def target_predecessors(q, d, G):
    while True:
        try:
            item = q.get()
        except queue.Empty:
            return
        d[item] = list(G.predecessors(item))
        q.task_done()
