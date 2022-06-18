import sys
import pandas as pd
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer

df = pd.DataFrame()
cluster = xmlrpc.client.ServerProxy('http://localhost:9000')

# Add worker to Cluster.
print("Getting ready the worker...")
port = input("In which port is the worker working?\n")
print(cluster.add("http://localhost:"+str(port)))


# Create Worker
with SimpleXMLRPCServer(('localhost', int(port)), logRequests=True) as server:
    #####################
    # Cluster functions #
    #####################
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

    def is_in_function(label):
        # noinspection PyTypeChecker
        return str(df.isin([label]).any(axis=None))
    server.register_function(is_in_function, 'is_in')

    def columns_function():
        return str(df.columns.values)
    server.register_function(columns_function, 'col')

    def apply_function(label):
        return str(df.apply(lambda x: x[label].upper(), axis=1))
    server.register_function(apply_function, 'apply')

    def group_by_function(label):
        return str(df.groupby(label).sum())
    server.register_function(group_by_function, 'group_by')

    def items_function():
        result = ""
        for label, content in df.items():
            result = result + str(content) + " "
        return str(result)
    server.register_function(items_function, 'items')

    def head_function():
        return str(df.head(5))
    server.register_function(head_function, 'head')

    ######################
    # Worker's main loop #
    ######################
    try:
        print("Ctrl+C to exit!")
        cluster.delete("http://localhost:"+str(port))
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
