#!/usr/bin/env python3
import xmlrpc.client
# import redis

worker = xmlrpc.client.ServerProxy('http://localhost:8000')
print(worker.system.listMethods())
print(worker.read('cities.csv'))
print(worker.read('cities.csv'))
print(worker.min('LonD'))
print(worker.max('LonD'))
print(worker.groupby())
print(worker.col())
print(worker.items())
print(worker.head())


