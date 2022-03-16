# from xmlrpc.server import SimpleXMLRPCServer
# from xmlrpc.server import SimpleXMLRPCRequestHandler
# # from multiprocessing import Process
# import Worker
#
#
# # import redis
#
# class RequestHandler(SimpleXMLRPCRequestHandler):
#     rpc_paths = ('/',)
#
#
# class Cluster:
#     # def __init__(self):
#
#     with SimpleXMLRPCServer(('localhost', 8000), requestHandler=RequestHandler) as cluster:
#         workers = {}
#
#         def add_worker(id_worker, workers=None):
#             # if id_worker is None:
#             #     id_worker = len(workers)
#             # worker = Worker(id_worker)
#             workers[id_worker] = id_worker
#             # worker.start()
#             return True
#
#         cluster.register_function(cluster.add_worker)
#
#         def remove_worker(id_worker, workers=None):
#             try:
#                 workers.pop(id_worker).stop()
#             except KeyError:
#                  print("This worker doesn't exists!")
#             return True
#
#         cluster.register_function(cluster.remove_worker)
#
#         def list_workers(workers=None):
#           return list(workers.keys())
#
#         cluster.register_function(cluster.list_workers)
#
#
#         cluster.serve_forever()
