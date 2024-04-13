# =========================================
# 	Author: Jenil J Gandhi
# 	Subject: Distributed Systems
# 	Assignment Number: 3
# 	Date: Date: 11-03-2024 22:33:24
# =========================================

import json
import sys
import os
import socket
import time
import subprocess
import threading
from Message import DoneMessage
from Message import ReducerDataMessage
from Message import PulseMessage
from Message import SendToReducerMessage
from Message import Message


class MapperWorker:
    """
    Description: Reads a config file for a testcase and get the master send port and own recv port.
    This also makes sure that only mapper can read it's own configuration.
    """

    def readConfig(self, testCase, identifier):
        fileLoc = "./Test/Test" + str(testCase) + "/conf.json"
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
            print(jsonData["mappers"])
            return jsonData["mappers"][identifier]

    """
    Description: Sends data packets to designated reducer node
    """

    def sendToReducer(self, reducerHost, reducerPort, message: Message):
        try:
            time.sleep(0.01)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((reducerHost, reducerPort))
            # message = ReducerDataMessage(self.identifier, key, value)
            sock.sendall(
                bytes(message.serializeMessage(), "utf-8")
            )  # Sends the object from the head of the queue after it is popped
            sock.close()
            return True
        except Exception as E:
            print(str(E))
            return False

    """
    Description: Read the output file and send data according to hash function to each reducers.
    """

    def sendToReducerHandler(self, sendToReducerMessage: SendToReducerMessage):
        # Loop through the output file and send message based on the hash function
        outputFile = os.path.join(
            os.getcwd(),
            "Test",
            "Test" + str(self.testcase),
            self.identifier,
            "output.txt",
        )
        with open(outputFile, "r") as file:
            data = file.read().split("\n")
            for line in data:
                if len(line.split("\t")) == 2:
                    key, value = line.split("\t")
                    # print("Key", key)
                    # value = value[:-1]  # to remove \n

                    reducerId = self.myHash(
                        str(key), len(sendToReducerMessage.reducerHosts)
                    )
                    print(
                        sendToReducerMessage.reducerHosts,
                        sendToReducerMessage.reducerPorts,
                        reducerId,
                    )
                    self.sendToReducer(
                        sendToReducerMessage.reducerHosts[reducerId],
                        sendToReducerMessage.reducerPorts[reducerId],
                        ReducerDataMessage(self.identifier, key, value),
                    )
        # Send done messages to all reducers to mark end of sending data
        for i in range(len(sendToReducerMessage.reducerHosts)):
            print(f"Mapper {self.identifier} sending done messages!")
            self.sendToReducer(
                sendToReducerMessage.reducerHosts[i],
                sendToReducerMessage.reducerPorts[i],
                DoneMessage(self.identifier),
            )

    """
    Description: this hashing function maps the input word to a number and outputs which reducer to send the data to
    """

    def myHash(self, word: str, numReducers):
        return ord(word[0]) % numReducers

    def fnv_hash(self, word: str, numReducers):
        # FNV offset basis and prime numbers
        fnv_offset_basis = 14695981039346656037
        fnv_prime = 1099511628211

        # Initialize hash to offset basis
        hash_value = fnv_offset_basis

        # Hash each character in the word
        hash_value = 0
        # print("Word", word)
        for char in word:
            # hash_value ^= ord(char)
            # hash_value *= fnv_prime
            hash_value = hash_value * 256 + ord(char)

        return hash_value % numReducers
        # return ord(word[0]) % numReducers

    """
    Description: Reads the message and designates each message to respective function
    """

    def messagehandler(self, messageStr):
        messageStr = messageStr.decode("utf-8")
        if "_SendToReducer_" in messageStr:
            print("Sending")
            self.sendToReducerHandler(
                SendToReducerMessage.deserializeMessage(messageStr)
            )

        if "_kill_" in messageStr:
            # Handle kill message here
            pass

    """
    Description: Listen for incoming data packets from the master node and distinguish it
    should listen on masterSendnode and masterSendPort
    1. Send to reducer message.
    2. Kill message <to implement fault tolerance>.
    """

    def listenFromMasterWorker(self):
        mapperSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        mapperSocket.bind((self.recvHost, self.recvPort))
        mapperSocket.listen()
        print(
            f"Mapper: {self.identifier}, is listening on port:{self.recvHost}:{self.recvPort}"
        )
        while True:
            sock, addr = mapperSocket.accept()
            data = sock.recv(1024)
            if data != "":
                # Check the recv data is not empty
                self.messagehandler(data)  # Check for different types of messages

    """
    Description: Sends pulse every one sencond to master node to mark alive mapper node
    """

    def sendPulseToMaster(self):
        time.sleep(0.1)
        print("Pulse started from mapper:", self.identifier)
        while True:
            self.sendToMaster(PulseMessage(str(self.identifier)))
            time.sleep(1)

    """
    Description: Sends messages to master
    1.  Done Message <marks end of mapping task of mapper Mid>.
    2.  Pulse message <to mark the mapper is alive>.
    """

    def sendToMaster(self, message: Message):
        time.sleep(0.01)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # print(f"Sending to master on: {self.masterSendHost}:{self.masterSendPort}")
        sock.connect((self.masterSendHost, self.masterSendPort))
        sock.sendall(
            bytes(message.serializeMessage(), "utf-8")
        )  # Sends the object from the head of the queue after it is popped
        sock.close()
        return True

    """
    Description: Executes the mapping task on the new process.
    """

    def execMapTask(self):
        print("Executing task")

        execFileLocation = os.path.join(
            os.getcwd(),
            "Test",
            "Test" + str(self.testcase),
            str(self.identifier),
            "input.py",
        )
        inputFileLocation = os.path.join(
            os.getcwd(),
            "Test",
            "Test" + str(self.testcase),
            str(self.identifier),
            "input.txt",
        )
        outputFileLocation = os.path.join(
            os.getcwd(),
            "Test",
            "Test" + str(self.testcase),
            str(self.identifier),
            "output.txt",
        )
        print(outputFileLocation, inputFileLocation, execFileLocation)
        try:
            self.jobStatus["status"] = "started"
            subprocess.run(
                [
                    "python",
                    execFileLocation,
                    "<",
                    inputFileLocation,
                    ">",
                    outputFileLocation,
                ],
                shell=True,
            )  # Run the mapping function
            self.jobStatus["status"] = "completed"
            print(f"Mapper:{self.identifier} has completed mapping task.")
            # Send a done message to the master node
            self.sendToMaster(DoneMessage(str(self.identifier)))
            return True
        except Exception as E:
            print(str(E))
            return False

    """
    Description: Init function
    """

    def __init__(
        self,
        mappingFunctionLocation,
        mappingDataLocation,
        testcase,
        identifier,
        numMappers,
    ):
        print("here in mapper 1")
        self.jobStatus = {"status": "notStarted"}
        self.mappingFunctionLocation = mappingFunctionLocation
        self.mappingDataLocation = mappingDataLocation
        self.testcase = testcase
        self.identifier = identifier
        self.configData = self.readConfig(
            testcase, identifier
        )  # Read data from the config file
        self.masterSendPort = self.configData[
            "masterPort"
        ]  # Port for sending data to master
        self.masterSendHost = self.configData[
            "masterHost"
        ]  # Host to which master is listening
        self.recvPort = self.configData["recvPort"]
        self.recvHost = self.configData["host"]
        self.numMappers = numMappers
        print("here in mapper 1")

        # Need to have a thread for sending pulse
        threading.Thread(target=self.sendPulseToMaster).start()
        # Need to have a thread for receiving messages from master
        threading.Thread(target=self.listenFromMasterWorker).start()

        # Start the mapping function
        threading.Thread(target=self.execMapTask).start()


# print("hellow world")
