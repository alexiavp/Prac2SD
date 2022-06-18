import sys
import threading
import time
import pandas as pd
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import redis

df = pd.DataFrame()
workers = []
r = redis.Redis('localhost', decode_responses=True)

# Init cluster or worker if master didn't exist
try:

    port_m = r.get("Master")
    master = xmlrpc.client.ServerProxy('http://localhost:' + str(port_m))
    master.ping()
    print("Getting ready the worker...")
    port = input("In which port is the worker working?\n")
    r.set("worker:" + str(port), ("http://localhost:" + str(port)))

except (Exception,):
    r.flushall()
    r.set("Master", 9000)
    port = r.get("Master")
    master = None

# Init the cluster
with SimpleXMLRPCServer(('localhost', int(port)), logRequests=True, allow_none=True) as cluster:
    ####################
    # Worker functions #
    ####################
    def load_csv(name):
        global df
        if df.empty:
            df = pd.read_csv(name)
            res = "File loaded!"
        else:
            res = "A file was already loaded!"
        return res
    cluster.register_function(load_csv, 'read')


    def minimum_function(col):
        return str(df[col].min(axis=0))
    cluster.register_function(minimum_function, 'min')


    def maximum_function(col):
        return str(df[col].max(axis=0))
    cluster.register_function(maximum_function, 'max')


    def is_in_function(label):
        # noinspection PyTypeChecker
        return str(df.isin([label]).any(axis=None))
    cluster.register_function(is_in_function, 'is_in')


    def columns_function():
        return str(df.columns.values)
    cluster.register_function(columns_function, 'col')


    def apply_function(label):
        return str(df.apply(lambda x: x[label].upper(), axis=1))
    cluster.register_function(apply_function, 'apply')


    def group_by_function(label):
        return str(df.groupby(label).sum())
    cluster.register_function(group_by_function, 'group_by')


    def items_function():
        result = ""
        for label, content in df.items():
            result = result + str(content) + " "
        return str(result)
    cluster.register_function(items_function, 'items')


    def head_function():
        return str(df.head(5))
    cluster.register_function(head_function, 'head')

    #####################
    # Cluster functions #
    #####################
    def add_worker(url):
        global workers
        print("Adding worker")
        r.publish("worker:" + str(port), ("ADD:http://localhost:" + str(port)))
        if port == port_m:
            for w in workers:
                worker = xmlrpc.client.ServerProxy(w)
                worker.add(url)
            new_worker = xmlrpc.client.ServerProxy(url)
            for w in workers:
                new_worker.add(w)
        workers.append(url)
        return "Worker added successfully!"
    cluster.register_function(add_worker, 'add')


    def ping_m():
        print("Ping!")
        return True
    cluster.register_function(ping_m, 'ping')


    def delete_worker(url):
        global workers
        workers.remove(url)
        print("Deleting worker")
        r.publish("worker:" + str(port), ("DELETE: http://localhost:" + str(port)))
        r.delete("worker:" + str(port))
        if port == port_m:
            for w in workers:
                worker = xmlrpc.client.ServerProxy(w)
                worker.delete(url)
        return "Worker deleted successfully!"
    cluster.register_function(delete_worker, 'delete')


    def get_workers():
        return str(workers)
    cluster.register_function(get_workers, 'get')


    def ping_workers():
        global master, port_m
        for url in workers:
            try:
                port_m = r.get("Master")
                if ("http://localhost:" + str(port_m)) == url:
                    master.ping()
                else:
                    print("Ping to workers!")
                    worker = xmlrpc.client.ServerProxy(url)
                    worker.ping()
            except (Exception,):
                print("Worker disconnected!")
                delete_worker(url)
    cluster.register_function(ping_workers, 'ping_w')


    def become_master():
        print("I'm the new master")
        global port, port_m
        port_m = port
        r.set("Master", port)
        delete_worker('http://localhost:' + str(port))
    cluster.register_function(become_master, 'become_master')

    #######################
    # Cluster's main loop #
    #######################
    try:
        start = threading.Thread(target=cluster.serve_forever, daemon=True)
        start.start()
        print("Ctrl+C to exit!")
        port_m = r.get("Master")
        if port != port_m:
            master.add("http://localhost:" + str(port))
            add_worker("http://localhost:" + str(port))
        while True:
            port_m = r.get("Master")
            if port_m == port:
                ping_workers()
            else:
                try:
                    master.ping()
                except (Exception,):
                    print("Master disconnected, finding new master!")
                    new_master = workers[0]
                    port_m = int(new_master.split(':')[2])
                    master = xmlrpc.client.ServerProxy('http://localhost:' + str(port_m))
                    master.become_master()
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
