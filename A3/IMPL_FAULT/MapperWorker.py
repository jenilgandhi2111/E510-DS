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
from signal import SIGINT
from colorama import Fore, Style
import random
import logging


class MapperWorker:
    """
    Description: Reads a config file for a testcase and get the master send port and own recv port.
    This also makes sure that only mapper can read it's own configuration.
    """

    def readConfig(self, testCase, identifier):
        fileLoc = "./Test/Test" + str(testCase) + "/conf.json"
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
            # print(jsonData["mappers"])
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
            self.displayMessage("Failed sending to reducer"+message.serializeMessage())
            return False


    """
    Description: Read the output file and send data according to hash function to each reducers.
    """

    def sendToReducerHandler(self, sendToReducerMessage: SendToReducerMessage):
        time.sleep(1)
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
                if not self.SENDFLAG:  # Abrupt termination from sending data
                    return -1
                if len(line.split("\t")) == 2:
                    key, value = line.split("\t")
                    reducerId = self.myHash(
                        str(key), len(sendToReducerMessage.reducerHosts)
                    )
                    self.sendToReducer(
                        sendToReducerMessage.reducerHosts[reducerId],
                        sendToReducerMessage.reducerPorts[reducerId],
                        ReducerDataMessage(self.identifier, key, value),
                    )
                self.timedKill("Sending")
        # Send done messages to all reducers to mark end of sending data

        for i in range(len(sendToReducerMessage.reducerHosts)):
            self.sendToReducer(
                sendToReducerMessage.reducerHosts[i],
                sendToReducerMessage.reducerPorts[i],
                DoneMessage(self.identifier),
            )
        self.displayMessage("Done sending <Done> messages to reducers.")
        return 0

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

    def displayMessage(self, msg,p=False):
        logger = logging.getLogger(__name__)
        logging.basicConfig(filename='mapper.log', encoding='utf-8', level=logging.DEBUG)
        logging.debug(
            Fore.CYAN
            + Style.BRIGHT
            + "> Mapper-{"
            + self.identifier
            + "}......"
            + Style.RESET_ALL
            + Fore.YELLOW
            + msg
            + Style.RESET_ALL
        )
        
        print(Fore.CYAN
            + Style.BRIGHT
            + "> Mapper-{"
            + self.identifier
            + "}......"
            + Style.RESET_ALL
            + Fore.YELLOW
            + msg
            + Style.RESET_ALL)

    """
    Description: Reads the message and designates each message to respective function
    """

    def messagehandler(self, messageStr):
        messageStr = messageStr.decode("utf-8")
        if "_SendToReducer_" in messageStr:
            self.SENDFLAG = True
            self.displayMessage("Mapper received RTS.")
            self.sendToReducerHandler(
                SendToReducerMessage.deserializeMessage(messageStr)
            )

        elif "_KillMessage_" in messageStr:
            # Handle kill message here
            self.displayMessage("Mapper Killed.")
            os.kill(os.getpid(), SIGINT)

        elif "_StopMessage_" in messageStr:
            self.displayMessage("Stop signaled by master.")
            self.SENDFLAG = False

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
        self.displayMessage(f"is listening on port:{self.recvHost}:{self.recvPort}")
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
        time.sleep(.5)
        # print(f"> Mapper{self.identifier}: Pulse started from mapper:", self.identifier)
        self.displayMessage(f"pulse started from mapper!")
        while True:
            self.sendToMaster(PulseMessage(str(self.identifier)))
            time.sleep(1)

    """
    Description: Sends messages to master
    1.  Done Message <marks end of mapping task of mapper Mid>.
    2.  Pulse message <to mark the mapper is alive>.
    """

    def sendToMaster(self, message: Message):
        try:
            time.sleep(0.01)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # print(f"Sending to master on: {self.masterSendHost}:{self.masterSendPort}")
            sock.connect((self.masterSendHost, self.masterSendPort))
            sock.sendall(
                bytes(message.serializeMessage(), "utf-8")
            )  # Sends the object from the head of the queue after it is popped
            sock.close()
            return True
        except Exception as E:
            self.displayMessage("Failed sending to master:",message.serializeMessage())
            return False
    """
    Description: Executes the mapping task on the new process.
    """

    def execMapTask(self):
        self.displayMessage("has started mapping task")

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
        # print(outputFileLocation, inputFileLocation, execFileLocation)
        try:
            self.jobStatus["status"] = "started"
            self.timedKill("mapping")
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
            self.groupBy()
            time.sleep(1)
            self.jobStatus["status"] = "completed"
            self.displayMessage("has completed mapping task")
            # Send a done message to the master node
            self.sendToMaster(DoneMessage(str(self.identifier)))
            self.displayMessage("Sent done message")
            return True
        except Exception as E:
            print(str(E))
            return False

    def timedKill(self,task):
        if self.restart: # if we restart the current node do not kill
            return
        if self.DIE and task == self.DIESTATE:
            if self.DIESTATE == "mapping":
                os.kill(os.getpid(),SIGINT)
            elif random.randint(1,1000)%self.DIEMOD:
                os.kill(os.getpid(),SIGINT)

    def clearFile(self):
        outputFileLocation = os.path.join(
            os.getcwd(),
            "Test",
            "Test" + str(self.testcase),
            str(self.identifier),
            "output.txt",
        )
        with open(outputFileLocation,'w') as file:
            pass
        return True
    def groupBy(self):
        '''
        Description: Group the keys from the output file and store it back in it
        '''
        outputFileLocation = os.path.join(
            os.getcwd(),
            "Test",
            "Test" + str(self.testcase),
            str(self.identifier),
            "output.txt",
        )
        outputDict = {}
        with open(outputFileLocation, "r") as file:
            data = file.read().split("\n")
            for line in data:
                if not self.SENDFLAG:  # Abrupt termination from sending data
                    return -1
                if len(line.split("\t")) == 2:
                    key, value = line.split("\t")
                    if key not in outputDict:
                        outputDict[key] = []
                    outputDict[key].append(value)
        
        self.clearFile()
        with open(outputFileLocation,"a") as file:
            for key in outputDict:
                file.write(key+"\t"+str(outputDict[key])+"\n")
        return True
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
        restart=False
    ):
        self.jobStatus = {"status": "notStarted"}
        self.mappingFunctionLocation = mappingFunctionLocation
        self.mappingDataLocation = mappingDataLocation
        self.testcase = testcase
        self.identifier = identifier
        self.configData = self.readConfig(testcase, identifier)  # Read data from the config file
        self.masterSendPort = self.configData["masterPort"]  # Port for sending data to master
        self.masterSendHost = self.configData["masterHost"]  # Host to which master is listening
        self.recvPort = self.configData["recvPort"]
        self.recvHost = self.configData["host"]
        self.numMappers = numMappers
        self.restart = restart
        self.SENDFLAG = True
        self.DIE = False
        self.DIEMOD = 20
        if self.configData["DEATH"] == "True":
            self.DIE = True
            self.DIESTATE = self.configData["DIESTATE"]
            self.displayMessage("DIESTATE"+self.DIESTATE)
        # print(self.configData)
        # Need to have a thread for receiving messages from master
        threading.Thread(target=self.listenFromMasterWorker).start()
        # Need to have a thread for sending pulse
        threading.Thread(target=self.sendPulseToMaster).start()
        # Start the mapping function
        threading.Thread(target=self.execMapTask).start()



# print("hellow world")
