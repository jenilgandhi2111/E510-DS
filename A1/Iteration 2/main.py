# =========================================
# 	Author: Jenil J Gandhi
# 	Subject: Distributed Systems
# 	Assignment Number: 1
# 	Question Number: 1
# 	Date: Date: 27-01-2024 00:01:13
# =========================================

import threading
import os
import time
import socket
import re


class DataStore:
    def __init__(self):
        self.dataPath = "./Objects/objfile.txt"

        # Check if the file exists or not
        if not os.path.isfile(self.dataPath):
            f = open(self.dataPath, "w")
            f.close()

    def get(self, key):
        time.sleep(5)
        with open(self.dataPath, "r") as file:
            for line in file:
                line = line.split(",")
                print(line[-2], str(key), type(line[-2]), type(key))
                if str(line[-2]).strip() == str(key).strip():
                    print("kfnlksdnfksnfd")
                    return (
                        str(line[-1]).strip()
                        + " "
                        + key
                        + len(bytes(str(line[-1]).strip()), "utf-8")
                        + "\r\n"
                        + " <FOUND>\n"
                    )
        return "\n<NOT FOUND>\r\n"

    def getHelper(self, key):
        # Read the file and find out the key
        with open(self.dataPath, "r") as file:
            for line in file:
                line = line[:-1]

                ln = line.split(",")
                print(ln)
                # if ln == [""]:
                #     continue
                if ln[-2] == key:
                    return ln[-1]
        return "NOT FOUND"

    def cleanData(self, data):
        return re.sub(r"[^a-zA-Z0-9]", "", data)

    def set(self, key, value, storer="UNK"):
        print(key, value)
        # check if the key is present or not
        if self.getHelper(key) != "NOT FOUND":
            return "NOT STORED\r\n"

        with open(self.dataPath, "a") as file:
            print(value)
            file.write(
                str(time.time())
                + ","
                + storer
                + ","
                + key
                + ","
                + self.cleanData(value)
                + "\n"
            )
            time.sleep(4)
        return "STORED\r\n"


class Server:
    def __init__(self, host, port):
        self.port = port
        self.host = host
        self.dataStore = DataStore()

    def cleanData(self, data):
        return re.sub(r"[^a-zA-Z0-9]", "", data)

    # def parseCommand(self, ADDR, command: str):
    #     splitCommand = command.split(" ")
    #     print(splitCommand)
    #     if splitCommand[0].lower() == "get":
    #         return self.dataStore.get(splitCommand[-1])
    #     elif splitCommand[0].lower() == "set":
    #         return self.dataStore.set(splitCommand[-2], splitCommand[-1])
    #     return "UNK CMD"

    def parseCommand1(self, client_socket, data: str):
        data = data.split(" ")
        if data[0] == "set":
            # print("-=========================================\n", data[-1].split("\n"))
            if "noreply" in data[-1]:
                data[-1] = data[-1].split("\n")[1].split("\r")[0]
            return [client_socket, "set", data[1], self.cleanData(data[-1])]
        elif data[0] == "get":
            return [client_socket, "get", data[-1]]
        else:
            return False

    def parseCommand2(self, client_socket, data):
        print(data)
        print(len(data), data[0], data[1], data[-1])
        if data[0] == "set":
            return [client_socket, "set", data[1], data[-1]]
        elif data[0] == "get":
            return [client_socket, "get", data[-1]]

    def handle_client(self, client_socket, addr, queue):
        command = ""
        while True:
            try:
                data = client_socket.recv(1024).decode("utf-8")
                data = data.split()

                if data == []:
                    continue
                elif data[0] == "set" and "noreply" in data:
                    print("Here for memcache")
                    queue.append(self.parseCommand2(client_socket, data))
                elif data[0] == "get":
                    # Execute it and make command empty again
                    queue.append(self.parseCommand2(client_socket, data))

                    continue
                elif data[0] == "set":
                    command = data
                    continue
                elif command[0] == "set":
                    command.append(data[0])
                    queue.append(self.parseCommand2(client_socket, command))
                    command = ""
                    continue
                else:
                    client_socket.send(bytes("<UNK COMMAND>", "utf-8"))

                # print("Recieved from address", addr, " Data:", data.split())
                # if data == []:
                #     continue
                # ans = self.parseCommand1(client_socket, data)
                # if ans != False:
                #     queue.append(ans)
            except ConnectionResetError:
                print("Connection reset by client:", addr)
                break
            except Exception as e:
                print("Error:", e)
                break
            # if data[0] == "get":
            #     # queue.append([client_socket, "get", data[1]])
            #     queue.append(self.)
            # elif data[0] == "set":
            #     queue.append([client_socket, "set", data[1], data[2]])

    def getThread(self, key, socketObj):
        return socketObj.send(bytes(self.dataStore.get(key), "utf-8"))

    def setThread(self, key, value, socketObj):
        return socketObj.send(bytes(self.dataStore.set(key, value), "utf-8"))

    def consumerThread(self, queue):
        # Content of the queue should be
        # [socketObj,type,"a"]
        temp = []
        while True:
            while len(queue) > 0:
                test = []
                for th1 in temp:
                    if th1.is_alive():
                        test.append(th1)
                temp = test

                element = queue[0]
                if element[1] == "set":
                    if temp == []:
                        th = threading.Thread(
                            target=self.setThread,
                            args=(
                                element[2],
                                element[3],
                                element[0],
                            ),
                        )
                        th.start()
                        th.join()
                        queue.pop(0)
                    else:
                        break
                else:
                    th = threading.Thread(
                        target=self.getThread,
                        args=(
                            element[2],
                            element[0],
                        ),
                    )
                    th.start()
                    temp.append(th)
                    queue.pop(0)

    def startServer(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        Q = []
        th = threading.Thread(target=self.consumerThread, args=(Q,))
        th.start()
        while True:
            client_socket, addr = server_socket.accept()

            # Call client handler to handle multiple clients
            client_handler = threading.Thread(
                target=self.handle_client, args=(client_socket, addr, Q)
            )
            client_handler.start()


server = Server(host="127.0.0.1", port=9889)

server.startServer()
