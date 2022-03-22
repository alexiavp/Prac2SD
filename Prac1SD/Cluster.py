from xmlrpc.server import SimpleXMLRPCServer
# # from multiprocessing import Process
# import Worker
#
#
# # import redis

workers = {}
cont_workers = 0

with SimpleXMLRPCServer(('localhost', 9000)) as cluster:

    def add_worker(url):
        global cont_workers
        workers[cont_workers] = url
        cont_workers = cont_workers + 1
        return "Worker added successfully!"
    cluster.register_function(add_worker, 'add')

    # Run the server's main loop
    cluster.serve_forever()

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
