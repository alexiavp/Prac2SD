import sys
from xmlrpc.server import SimpleXMLRPCServer

workers = []

with SimpleXMLRPCServer(('localhost', 9000)) as cluster:
    #####################
    # Cluster functions #
    #####################
    def add_worker(url):
        global workers
        workers.append(url)
        return "Worker added successfully!"

    cluster.register_function(add_worker, 'add')

    def delete_worker(url):
        global workers
        workers.remove(url)
        return "Worker deleted successfully!"


    cluster.register_function(delete_worker, 'delete')

    def get_workers():
        return str(workers.copy())

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
