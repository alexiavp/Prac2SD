#!/usr/bin/env python3
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import redis
cliente:
master añadir eliminar listar
primero pregunta quien esta conectado al cluster (pregunta por los workers)
El propio worker solicita al cluster añadirse
CLuster añade y una vez todos conectados devuelve la lista
Las funciones del Dask en el worker (se pueden usar las propuas funciones del Dask)


class Master:
    __instance = None

    def __new__(cls, *args):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(this):
        this.redisCon = redis.Redis(host="localhost")
        this.workers = {}
        this.server = SimpleXMLRPCServer(("localhost", 9000), logRequests=True)
        this.server.register_function(this.addWorker)
        this.server.register_function(this.removeWorker)
        this.server.register_function(this.listWorkers)

    def listWorkers(self):
        return list(self.workers.keys())
