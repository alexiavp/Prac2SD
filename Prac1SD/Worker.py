import pandas as pd
# import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

df = None


# server = xmlrpc.client.ServerProxy('http://localhost:8000')

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


# Create server
with SimpleXMLRPCServer(('localhost', 8000),
                        requestHandler=RequestHandler) as server:
    server.register_introspection_functions()

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

    def groupby_function():
        return str(df.groupby)
    server.register_function(groupby_function, 'groupby')

    def items_function():
        return str(df.items)
    server.register_function(items_function, 'items')

    def head_function():
        return str(df.head(5))
    server.register_function(head_function, 'head')

    # Run the server's main loop
    server.serve_forever()


# print(df.columns.values[2])
# print(df.items)

