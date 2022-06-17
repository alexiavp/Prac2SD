import sys
import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import random

workers = []
cont_workers = 0
port = 9000

with SimpleXMLRPCServer(('localhost', port)) as cluster:
    def close_connexion():
        sys.exit(0)


    def add_worker(url):
        global cont_workers
        global workers
        print(12)
        workers.append(url)
        print(workers)
        cont_workers = cont_workers + 1
        print(cont_workers)
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
        print("Llegomas")
        return True


    cluster.register_function(ping_m, 'ping')


    def ping_workers():
        for url in workers:
            try:
                print("Entro")
                print(url)
                worker = xmlrpc.client.ServerProxy(url)
                print(workers)
                worker.ping()
            except:
                try:
                    print(url)
                    print("Borro")
                    delete_worker(url)
                except ConnectionRefusedError:
                    pass
                except IndexError:
                    print("No more workers to become masters")
                    exit(1)


    cluster.register_function(ping_workers, 'ping_w')


    def become_master(url):
        print("Espero que no")
        global port
        global cluster

        print("Implosiono")
        cluster.server_close()
        print(workers)
        port = int(url.split(':')[2])
        print(port)
        worker = xmlrpc.client.ServerProxy(url)
        worker.quit()
        delete_worker("http://localhost:" + str(port))
        print(workers)
        print(cluster)
        cluster = SimpleXMLRPCServer(('localhost', port))
        add_worker("http://localhost:" + str(port))
        print("I'm master " + str(cluster))
        print(workers)


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
