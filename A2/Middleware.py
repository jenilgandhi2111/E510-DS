import socket
import os
import json
from Message.Message import Message
from Message.Message import AcknowledgeMessage
from PriorityQueue.Heap import Heap
import threading
import time


class MiddleWare:

    def getBroadcastPorts(self):
        with open(
            "./Config/" + self.testcase + "/" + self.testcase + "_networkUp.json"
        ) as file:
            return json.load(file)

    def __init__(
        self,
        blockId,
        applicationUpPort,
        applicationUpHost,
        applicationDownPort,
        applicationDownHost,
        networkUpPort,
        networkUpHost,
        networkDownPort,
        networkDownHost,
        testcase,
    ):
        self.blockId = blockId
        self.applicationUpPort = applicationUpPort
        self.applicationUpHost = applicationUpHost
        self.applicationDownPort = applicationDownPort
        self.applicationDownHost = applicationDownHost
        self.networkUpPort = networkUpPort
        self.networkUpHost = networkUpHost
        self.networkDownPort = networkDownPort
        self.networkDownHost = networkDownHost
        self.time = self.blockId
        self.queue = Heap([], comparator=MiddleWare.comparator)
        self.acks = []
        self.testcase = testcase
        self.maxData = len(self.getBroadcastPorts())

        # Threads to listen for application down, network up and process queue
        threading.Thread(target=self.listenApplicationDown).start()
        threading.Thread(target=self.listenNetworkUp).start()
        threading.Thread(target=self.processQueue).start()

    def adjustTime(self, newTime):
        self.time = max(self.time, int(newTime)) + self.blockId

    def handleMessageForNetworkHelper(self, data: str):
        print("Recieved Data from Network:", data)
        if data.startswith("MSG"):
            message = Message.deserializeMessage(data, self.maxData)
            self.adjustTime(message.lamportTime)
            # add the message in the queue
            self.queue.insert(message, comparator=MiddleWare.comparator)

        elif data.startswith("ACK"):
            message = AcknowledgeMessage.deserializeMessage(data)
            self.acks.append(message)
            # compare the hashvalue in the message and the one we recieved
            # for m in self.queue.elems:
            #     for acks in self.acks:
            #         if m.hashValue == acks.hashValue:
            #             m.RemAcks -= 1
            for msg in self.queue.elems:
                for i in range(len(self.acks)):
                    ack = self.acks[i]
                    if ack.hashValue == msg.hashValue:
                        msg.RemAcks -= 1
                        self.acks.pop(i)

    def processQueue(self):
        dic = {}
        while True:
            # Check if the message at the top of the stack has recieve
            if len(self.queue.elems) != 0:
                # time.sleep(1)
                # self.queue.printHeap()
                if self.queue.elems[0].RemAcks <= 0:
                    # time.sleep(3)
                    # Process the message and send it to application layer and broadcast acks
                    self.sendMessageToApplication(
                        self.queue.delete(comparator=MiddleWare.comparator)
                    )

                else:
                    print("Remaning Acks:", self.queue.elems[0].RemAcks)
                    if self.queue.elems[0] not in dic.keys():
                        time.sleep(1)
                        self.sendMessageToNetwork(
                            AcknowledgeMessage(self.queue.elems[0].hashValue)
                        )
                        dic[self.queue.elems[0]] = True

    def listenNetworkUpWorker(self, sock):
        while True:
            data = sock.recv(1024).decode("utf-8")
            if data == "":
                continue
            self.handleMessageForNetworkHelper(data)

    def listenNetworkUp(self):
        # Recieve packets from network layer and send it to application layer
        networkSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        networkSocket.bind((self.networkUpHost, self.networkUpPort))
        networkSocket.listen(10)
        print(
            f"Process middleware {self.blockId} is listening on port {self.networkUpPort} for incoming network requests"
        )
        while True:
            sock, addr = networkSocket.accept()

            handler = threading.Thread(target=self.listenNetworkUpWorker, args=(sock,))
            handler.start()

    def sendMessageToNetwork(self, message):
        # Write the code to broadcast the message via the network down port
        # Read the file and get everyones network up ports and send messages to that port
        # Basically a broadcast
        ports = self.getBroadcastPorts()

        for port in ports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # print(port)
            sock.connect((port[0], port[1]))
            sock.sendall(
                bytes(message.serializeMessage(), "utf-8")
            )  # Sending message to the network port
            sock.close()

    def sendMessageToApplication(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.applicationUpHost, self.applicationUpPort))
        sock.sendall(
            bytes(message.serializeMessage(), "utf-8")
        )  # Sends the object from the head of the queue after it is popped
        sock.close()

    def listenApplicationDownWorker(self, sock):
        while True:
            data = sock.recv(1024).decode("utf-8")
            if data == "":
                continue
            print("Data 127", data)
            message = Message(self.blockId, self.time, data, self.maxData)

            # Send this to broadcast via the netowrkdown port
            # message = Message(self.blockId, self.time, message)
            self.time += self.blockId
            # print(type(message))
            self.sendMessageToNetwork(
                message
            )  # Making the message in appropriate format
            # time.sleep(2)

    def listenApplicationDown(self):
        applicationSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        applicationSocket.bind((self.applicationDownHost, self.applicationDownPort))
        applicationSocket.listen(10)
        print(
            f"Process middleware {self.blockId} is listening on port {self.applicationDownPort} for incoming application requests"
        )
        while True:
            sock, addr = applicationSocket.accept()
            handler = threading.Thread(
                target=self.listenApplicationDownWorker, args=(sock,)
            )  # Making this because
            handler.start()

    def comparator(elem1: Message, elem2: Message):
        if elem1.lamportTime < elem2.lamportTime:
            return -1
        elif elem1.lamportTime > elem2.lamportTime:
            return 1
        return 0
