import sys
from xmlrpc.server import SimpleXMLRPCServer

import redis

workers = {}
cont_workers = 0

r = redis.Redis('localhost')
r.flushdb()
with SimpleXMLRPCServer(('localhost', 9000)) as cluster:
    def close_connexion():
        sys.exit(0)


    def add_worker(url):
        global cont_workers
        workers[cont_workers] = url
        cont_workers = cont_workers + 1
        r.set("worker:"+str(cont_workers), url)
        return "Worker added successfully!"

    cluster.register_function(add_worker, 'add')


    def get_workers():
        w = []
        for key in r.scan_iter("worker:*"):
            w.append(r.get(key))
        return str(workers)

    cluster.register_function(get_workers, 'get')

    # Run the server's main loop
    try:
        print("Ctrl+C to exit!")
        cluster.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
