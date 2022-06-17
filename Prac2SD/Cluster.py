import sys
import threading
import time
import pandas as pd
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import random
import redis

df = pd.DataFrame()
cluster = redis.Redis('localhost')
workers = []
cont_workers = 0
port = 9000

with SimpleXMLRPCServer(('localhost', port)) as cluster:
    def close_connexion():
        sys.exit(0)


    def add_worker(url):
        global cont_workers
        global workers
        print("Add worker")
        workers.append(url)
        print("Workers: " + str(workers))
        cont_workers = cont_workers + 1
        print("Contador: " + str(cont_workers))
        return "Worker added successfully!"


    cluster.register_function(add_worker, 'add')


    def delete_worker(url):
        global cont_workers
        global workers
        workers.remove(url)
        cont_workers = cont_workers - 1
        return "Worker added successfully!"


    cluster.register_function(delete_worker, 'delete')


    def get_workers():
        return str(workers)


    cluster.register_function(get_workers, 'get')


    def ping_m():
        print("Ping master")
        return True


    cluster.register_function(ping_m, 'ping')


    def ping_workers():
        for url in workers:
            try:
                print("Hago ping workers")
                print("Ping a " + str(url))
                worker = xmlrpc.client.ServerProxy(url)
                print("Workers: " + str(workers))
                worker.ping()
            except:
                try:
                    print("Borramos a " + str(url))
                    print("Borro worker")
                    delete_worker(url)
                except ConnectionRefusedError:
                    pass
                except IndexError:
                    print("No more workers to become masters")
                    exit(1)


    cluster.register_function(ping_workers, 'ping_w')


    def become_master(url):
        print("Nuevo master")
        global port
        global cluster

        print("Cerramos antiguo")
        cluster.server_close()
        print("Workers: " + str(workers))
        port = int(url.split(':')[2])
        print("Nuevo puerto:" + str(port))
        worker = xmlrpc.client.ServerProxy(url)
        worker.quit()
        delete_worker("http://localhost:" + str(port))
        print("Workers: " + str(workers))
        print("Nuevo cluster: " + str(cluster))
        cluster = SimpleXMLRPCServer(('localhost', port))
        add_worker("http://localhost:" + str(port))
        print("I'm master " + str(cluster))
        print("Workers: " + str(workers))


    cluster.register_function(become_master, 'become_master')

    # Run the server's main loop
    try:
        start = threading.Thread(target=cluster.serve_forever, daemon=True)
        start.start()
        print("Ctrl+C to exit!")
        print("Hola")
        add_worker("http://localhost:" + str(port))
        while True:
            print("Hasta aqui")
            ping_workers()
            print(cont_workers)
            time.sleep(1)

    except KeyboardInterrupt:
        try:
            print("Elimina" + str(port))
            delete_worker("http://localhost:" + str(port))
            print(workers)
            random_url = random.choice(workers)
            print(random_url)
            become_master(random_url)
        except ConnectionRefusedError:
            pass
        except IndexError:
            print("No more workers to become masters")
            exit(1)
        # sys.exit(0)


class Worker:

    print("Getting ready the worker...")
    port = input("In which port is the worker working?\n")
    cluster.set("worker:" + str(port), ("http://localhost:" + str(port)))

    # Create server
    with SimpleXMLRPCServer(('localhost', int(port)), logRequests=True) as server:

        def load_csv(self, name):
            global df
            if df.empty:
                df = pd.read_csv(name)
                res = "File loaded!"
            else:
                res = "A file was already loaded!"
            return res

        server.register_function(load_csv, 'read')

        def minimum_function(self, col):
            return str(df[col].min(axis=0))

        server.register_function(minimum_function, 'min')

        def maximum_function(self, col):
            return str(df[col].max(axis=0))

        server.register_function(maximum_function, 'max')

        def is_in_function(self, label):
            return str(df.isin([label]).any(axis=None))

        server.register_function(is_in_function, 'is_in')

        def columns_function(self):
            return str(df.columns.values)

        server.register_function(columns_function, 'col')

        def apply_function(self, label):
            return str(df.apply(lambda x: x[label].upper(), axis=1))

        server.register_function(apply_function, 'apply')

        def group_by_function(self, label):
            return str(df.groupby(label).sum())

        server.register_function(group_by_function, 'group_by')

        def items_function(self):
            result = ""
            for label, content in df.items():
                result = result + str(content) + " "
            return str(result)

        server.register_function(items_function, 'items')

        def head_function(self):
            return str(df.head(5))

        server.register_function(head_function, 'head')
