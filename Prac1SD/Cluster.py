from xmlrpc.server import SimpleXMLRPCServer
# from multiprocessing import Process
import Worker


# import redis


class Cluster:
    def __init__(self):
        self.cluster = SimpleXMLRPCServer(('localhost', 8000), logRequests=True)
        self.workers = {}

        self.cluster.register_function(self.add_worker)
        self.cluster.register_function(self.remove_worker)
        self.cluster.register_function(self.list_workers)

    def add_worker(self, id_worker):
        if id_worker is None:
            id_worker = len(self.workers)
        worker = Worker(id_worker)
        self.workers[id_worker] = worker
        worker.start()
        return True

    def remove_worker(self, id_worker):
        try:
            self.workers.pop(id_worker).stop()
        except KeyError:
            print("This worker doesn't exists!")
        return True

    def list_workers(self):
        return list(self.workers.keys())
