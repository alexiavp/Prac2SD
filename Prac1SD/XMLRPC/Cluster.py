import sys
from xmlrpc.server import SimpleXMLRPCServer

workers = {}
cont_workers = 0

with SimpleXMLRPCServer(('localhost', 9000)) as cluster:
    #####################
    # Cluster functions #
    #####################
    def add_worker(url):
        global cont_workers, workers
        workers.append(url)
        return "Worker added successfully!"

    cluster.register_function(add_worker, 'add')

    def delete_worker(url):
        global cont_workers, workers
        workers.remove(url)
        return "Worker deleted successfully!"


    cluster.register_function(delete_worker, 'delete')

    def get_workers():
        return str(workers)

    cluster.register_function(get_workers, 'get')

    #######################
    # Cluster's main loop #
    #######################
    try:
        print("Ctrl+C to exit!")
        cluster.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
