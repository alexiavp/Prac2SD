import sys
import pandas as pd
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer

df = pd.DataFrame()
port_m = 9000

cluster = xmlrpc.client.ServerProxy('http://localhost:9000')

# Add worker to cluster.
print("Getting ready the worker...")
port = input("In which port is the worker working?\n")
print(cluster.add("http://localhost:" + str(port)))

# Create server
with SimpleXMLRPCServer(('localhost', int(port)), logRequests=True) as server:
    def ping():
        print("Llego")
        return True


    server.register_function(ping, 'ping')

    def quit_worker():
        server.server_close()
        return True


    server.register_function(quit_worker, 'quit')

    def load_csv(name):
        global df
        if df.empty:
            df = pd.read_csv(name)
            res = "File loaded!"
        else:
            res = "A file was already loaded!"
        return res


    server.register_function(load_csv, 'read')


    def minimum_function(col):
        return str(df[col].min(axis=0))


    server.register_function(minimum_function, 'min')


    def maximum_function(col):
        return str(df[col].max(axis=0))


    server.register_function(maximum_function, 'max')


    def is_in_function(list):
        return str(df.isin(list))


    server.register_function(is_in_function, 'is_in')


    def columns_function():
        return str(df.columns.values)


    server.register_function(columns_function, 'col')


    def apply_function():
        func = eval('lambda num1,num2: num1 + num2')
        return str((func(2, 3)))


    server.register_function(apply_function, 'apply')


    def group_by_function(label):
        return str(df.groupby([label]).mean())


    server.register_function(group_by_function, 'group_by')


    def items_function():
        return str(df.items)


    server.register_function(items_function, 'items')


    def head_function():
        return str(df.head(5))


    server.register_function(head_function, 'head')

    # Run the server's main loop
    try:
        print("Ctrl+C to exit!")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
