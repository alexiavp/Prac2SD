import xmlrpc.client


class Workers:
    workers = {}
    cluster = xmlrpc.client.ServerProxy('http://localhost:9000')
    aux = cluster.get()
    w = aux.split('\'')
    i = 0
    w.pop(0)
    for x in w:
        if i % 2 == 1:
            w.pop(i)
        i = i + 1

    w.pop(i - 1)
    aux = 0
    for x in w:
        workers[aux] = xmlrpc.client.ServerProxy(x)
        aux = aux + 1

    def submit_task(funct, args):
        global workers
        for x in workers:
            results = x.funct(args)


class Client:
    Workers
    ex = True

    def menu(self):
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

    if __name__ == "__main__":

        while ex:
            menu()
            op = int(input())
            if op == 0:
               ex = False
            elif op == 1:
                file = input("Enter a file's name (without extension):\n")
                file = file + ".csv"
                print(Workers.submit_task("read", file))
            # print(worker.head())
        # elif op == 2:
        #     label = input("From which colum do you want to know the minimum:\n")
        #     # print("The minimum of " + label + " is " + worker.min('LonD'))
        # elif op == 3:
        #     label = input("From which colum do you want to know the maximum:\n")
        #     # print("The minimum of " + label + " is " + worker.max('LonD'))
        # elif op == 4:
        #     # print("The columns of the dataframe are:\n" + worker.col())
        # elif op == 5:
        #     # print("The first five lines of the dataframe:\n" + worker.head())
        # elif op == 6:
        #     label = input("With what columns do you want to group by:\n")
        #     # print("Dataframe grouped by" + label + "\n" + worker.groupby('City'))
        # elif op == 7:
        #     # print("The function items in the dataframe:\n" + worker.items())
        else:
            print("Option not valid chose another one")



