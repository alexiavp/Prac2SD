import sys
from xmlrpc.server import SimpleXMLRPCServer
# # from multiprocessing import Process
# import Worker
#
#
import redis

workers = {}
cont_workers = 0
r = redis.Redis('localhost')
for key in r.scan_iter("prefix:*"):
    r.delete(key)
with SimpleXMLRPCServer(('localhost', 9000)) as cluster:
    def close_connexion():
        sys.exit(0)


    def add_worker(url):
        global cont_workers
        UrlSet = "url"
        print(url)
        r.sadd(UrlSet, url)
        workers[cont_workers] = url
        cont_workers = cont_workers + 1
        return "Worker added successfully!"


    cluster.register_function(add_worker, 'add')


    def get_workers():
        return r.keys(pattern='*')


    cluster.register_function(get_workers, 'get')

    # Run the server's main loop
    try:
        print("Ctrl+C to exit!")
        cluster.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)

