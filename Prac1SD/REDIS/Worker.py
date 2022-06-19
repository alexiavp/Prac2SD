import sys
import pandas as pd
import redis
from xmlrpc.server import SimpleXMLRPCServer

df = pd.DataFrame()
cluster = redis.Redis('localhost')

# Add worker to cluster.
print("Getting ready the worker...")
port = input("In which port is the worker working?\n")
cluster.set("worker:"+str(port), ("http://localhost:"+str(port)))


# Create Worker
with SimpleXMLRPCServer(('localhost', int(port)), logRequests=True) as server:
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
        server.serve_forever()
        cluster.delete("worker:"+str(port), ("http://localhost:"+str(port)))
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        cluster.delete("worker:"+str(port))
        sys.exit(0)
