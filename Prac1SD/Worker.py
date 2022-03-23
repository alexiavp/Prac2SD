import pandas as pd
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer

df = None


cluster = xmlrpc.client.ServerProxy('http://localhost:9000')

# Add worker to cluster.
print("Getting ready the worker...")
port = input("In which port is the worker working?\n")
print(cluster.add('http://localhost'+str(port)))

# Create server
with SimpleXMLRPCServer(('localhost', int(port)), logRequests=True) as server:

    def load_csv(name):
        global df
        if df is None:
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

    def columns_function():
        return str(df.columns.values)
    server.register_function(columns_function, 'col')

    def groupby_function(label):
        return str(df.groupby([label]).mean())
    server.register_function(groupby_function, 'groupby')

    def items_function():
        return str(df.items)
    server.register_function(items_function, 'items')

    def head_function():
        return str(df.head(5))
    server.register_function(head_function, 'head')

    # Run the server's main loop
    server.serve_forever()


