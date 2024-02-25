import socket
import threading
import random
import time


class ApplicationLayer:
    def __init__(self, blockId, middlewarePort, middlewareHost, port, host):
        self.middlewarePort = middlewarePort
        self.middlewareHost = middlewareHost
        self.blockId = blockId
        self.port = port
        self.host = host
        self.processCnt = 0

        time.sleep(1)

        # Spawn a thread which listens to middleware
        threading.Thread(target=self.listenFromMiddleWare).start()
        threading.Thread(target=self.sendToMiddleware).start()

    def sendToMiddleware(self):
        capital_letters = [
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M",
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S",
            "T",
            "U",
            "V",
            "W",
            "X",
            "Y",
            "Z",
        ]

        tm = 5
        cntr = 0
        while True:
            time.sleep(tm)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.middlewareHost, self.middlewarePort))
            sock.send(bytes(capital_letters[cntr] + str(self.blockId), "utf-8"))

            sock.close()

    def processingThreadWorker(self, middleWareSocket):
        while True:
            self.processCnt += 1
            data = middleWareSocket.recv(1024).decode("utf-8")

    def listenFromMiddleWare(self):
        middleWareSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        middleWareSocket.bind((self.host, self.port))
        middleWareSocket.listen(10)

        print(f"Application Process {self.blockId} is listening on port {self.port}")

        while True:
            msocket, addr = middleWareSocket.accept()
            data = msocket.recv(1024)
            print("Data", data)
            print(
                "Application processes",
                data,
                " On block ID:",
                self.blockId,
                " procCnt:",
                self.processCnt,
            )
            self.processCnt += 1
            with open("./outputs/" + str(self.blockId) + ".txt", "a") as file:
                file.write("\n"+
                    str(self.processCnt)+".) Application processes"
                    + str(data.decode("utf-8")).split(",")[3]
                    + " On block ID:"
                    + str(self.blockId)
                )
