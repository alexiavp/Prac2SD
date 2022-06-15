import sys
import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import random
workers=[]
cont_workers = 0
is_master = False
port=9000



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



    def ping_workers():
        print("A ver")
        for url in workers:
            try:
                print("Entro")
                print(url)
                worker = xmlrpc.client.ServerProxy(url)
                print(worker)
                print(workers)
                worker.ping()
                #worker.ping()
            except:
                delete_worker(url)
    cluster.register_function(ping_workers, 'ping_w')


    def be_master():
        print("Espero que no")
        global is_master
        is_master = True
        global port
        global cluster
        cluster=SimpleXMLRPCServer(('localhost', port))
        print("I'm master")
        delete_worker("http://localhost:" + str(port))
    cluster.register_function(be_master, 'be_master')

    def ping_master():
        try:
            cluster.ping()
        except:
            new_master = False
            while not new_master:
                try:
                    random_url = random.choice(workers)
                    ##pings antes
                    master = xmlrpc.client.ServerProxy(random_url)
                    master.be_master()
                    new_master = True
                except ConnectionRefusedError:
                    pass
                except IndexError:
                    print("No more workers to become masters")
                    #exit(1)
        return 1
    cluster.register_function(ping_master, 'ping_m')
    # Run the server's main loop
    try:
        start = threading.Thread(target=cluster.serve_forever, daemon=True)
        start.start()
        print("Ctrl+C to exit!")
        if not is_master:
            print("Hola")
            #cluster.add_worker("http://localhost:" + str(port))
        while True:
            #if is_master:
            print("Hasta aqui")
            ping_workers()
            print(workers)
            print(cont_workers)
            #else:
                #ping_master()
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)

