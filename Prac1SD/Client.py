#!/usr/bin/env python3
import xmlrpc.client

# import redis
cluster = xmlrpc.client.ServerProxy('http://localhost:9000')
workers = cluster.get()
while workers:
    worker = xmlrpc.client.ServerProxy(workers)


def menu():
    print("Choose a function for the workers:")
    print("0. Exit!")
    print("1. Read file")
    print("2. Get the minimum")
    print("3. Get the maximum")
    print("4. Get labels")
    print("5. First five lines")
    print("6. Group by a column")
    print("7. Function items")
    print("Option chosen:\n")


ex = True
while ex:
    op = input(menu())
    if op == 0:
        ex = False
    elif op == 1:
        file = input("Enter a file's name (without extension):\n")
        print(worker.read(file + ".csv"))
        print(worker.head())
    elif op == 2:
        label = input("From which colum do you want to know the minimum:\n")
        print("The minimum of " + label + " is " + worker.min('LonD'))
    elif op == 3:
        label = input("From which colum do you want to know the maximum:\n")
        print("The minimum of " + label + " is " + worker.max('LonD'))
    elif op == 4:
        print("The columns of the dataframe are:\n" + worker.col())
    elif op == 5:
        print("The first five lines of the dataframe:\n" + worker.head())
    elif op == 6:
        label = input("With what columns do you want to group by:\n")
        print("Dataframe grouped by" + label + "\n" + worker.groupby('City'))
    elif op == 7:
        print("The function items in the dataframe:\n" + worker.items())
    else:
        print("Option not valid chose another one")
