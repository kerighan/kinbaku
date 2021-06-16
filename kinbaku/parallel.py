def parallel_task(G, nodes, task, n_jobs=-1):
    import multiprocessing as mp

    if n_jobs == -1:
        n_jobs = mp.cpu_count()

    with mp.Manager() as manager:
        q = manager.Queue()
        d = manager.dict()
        for node in nodes:
            q.put(node)

        processes = []
        for _ in range(n_jobs):
            q.put(None)
            if task == "neighbors":
                p = mp.Process(target=target_neighbors,
                               args=(q, d, G), daemon=True)
            elif task == "predecessors":
                p = mp.Process(target=target_predecessors,
                               args=(q, d, G), daemon=True)
            else:
                raise ValueError(f"Unknown task {task}")
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        data = dict(d)
    return data


def target_neighbors(q, d, G):
    while True:
        item = q.get()
        if item is None:
            return
        d[item] = list(G.neighbors(item))


def target_predecessors(q, d, G):
    while True:
        item = q.get()
        if item is None:
            return
        d[item] = list(G.predecessors(item))
