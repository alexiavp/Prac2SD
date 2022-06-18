import threading
import xmlrpc.client
import redis

r = redis.Redis(host='localhost', decode_responses=True)
workers = []


def get_workers():
    global workers, r
    workers.clear()
    master = xmlrpc.client.ServerProxy("http://localhost:"+str(r.get("Master")))
    aux = master.get()
    w = aux.split('\'')
    i = 0
    for x in w:
        if i % 2 == 1:
            workers.append(xmlrpc.client.ServerProxy(x))
        i = i + 1


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
    print("8. Function apply")
    print("9. Know if a list is in the dataframe")
    print("Option chosen:\n")



class Client:
    if __name__ == "__main__":
        global workers
        ex = False
        result = []
        get_workers()
        print(workers)
        sub = r.pubsub()
        sub.psubscribe("worker:*")
        while not ex:
            print(workers)

            menu()
            op = int(input())
            if op == 0:
                ex = True
            elif op == 1:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                for x in workers:
                    file = input("Enter a file's name (without extension) for one worker:\n")
                    file = file + ".csv"
                    print(x.read(file))

            elif op == 2:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                label = input("From which colum do you want to know the minimum:\n")
                for x in workers:
                    result.append(x.min(label))
                print("The minimum of " + label + " is " + str(min(result)))
                result.clear()

            elif op == 3:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                label = input("From which colum do you want to know the maximum:\n")
                for x in workers:
                    result.append(x.max(label))
                print("The maximum of " + label + " is " + str(max(result)))
                result.clear()

            elif op == 4:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                print("The columns of the dataframe are:\n" + workers[0].col())

            elif op == 5:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                print("The first five lines of the dataframes:\n")
                for x in workers:
                    print(x.head() + "\n")

            elif op == 6:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                label = input("With what columns do you want to group by:\n")
                print("Dataframe grouped by " + label + "\n")
                for x in workers:
                    print(x.group_by(label))

            elif op == 7:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                print("The function items in the dataframes:\n")
                for x in workers:
                    print(x.items())

            elif op == 8:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                label = input("With what columns do you want to do the function apply:\n")
                print("Dataframe with apply by" + label + "\n")
                for x in workers:
                    print(x.apply(label))

            elif op == 9:
                message = sub.get_message()
                if message is not None:
                    get_workers()
                print(workers)
                label = input("Which element do you want to know if is in the Dataframe:\n")
                for x in workers:
                    result.append(x.is_in(label))
                if 'True' in result:
                    print(label + " is in the Dataframe\n")
                else:
                    print(label + " is not in the Dataframe\n")
                result.clear()

            else:
                print("Option not valid chose another one")

        print("Bye Bye!")
