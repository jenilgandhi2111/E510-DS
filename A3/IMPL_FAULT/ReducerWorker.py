# =========================================
# 	Author: Jenil J Gandhi
# 	Subject: Distributed systems
# 	Assignment Number: 3
# 	Date: Date: 14-03-2024 00:28:26
# =========================================

import json
import socket
import os
import subprocess
import time
import threading
from Message import DoneMessage
from Message import Message
from Message import PulseMessage
from Message import ReducerDataMessage
from signal import SIGINT
from colorama import Fore, Style


class ReducerWorker:
    def displayMessage(self,msg):
        print(
            Fore.MAGENTA
            + Style.BRIGHT
            + "> Reducer-{"
            + self.identifier
            + "}....."
            + Style.RESET_ALL
            + Fore.LIGHTGREEN_EX
            + msg
            + Style.RESET_ALL
        )
    def readConfig(self):
        fileLoc = "./Test/Test" + str(self.testcase) + "/conf.json"
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
            return jsonData["reducers"][self.identifier]

    def sendToMaster(self, message: Message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.masterSendHost, self.masterSendPort))
        sock.sendall(
            bytes(message.serializeMessage(), "utf-8")
        )  # Sends the object from the head of the queue after it is popped
        sock.close()
        return True

    def sendPulseToMaster(self):
        time.sleep(0.1)
        self.displayMessage(f"Pulse started from reducer:{self.identifier}")
        while True:
            self.sendToMaster(PulseMessage(str(self.identifier)))
            time.sleep(1)

    def startReducing(self):
        execFileLocation = os.path.join(
            os.getcwd(), "Test", self.homeDirName, self.identifier, "reducer.py"
        )
        inputFileLocation = os.path.join(
            os.getcwd(), "Test", self.homeDirName, self.identifier, "reducerInput.txt"
        )
        outputFileLocation = os.path.join(
            os.getcwd(), "Test", self.homeDirName, self.identifier, "reducerOut.txt"
        )
        command = [
            "python",
            execFileLocation,
            "<",
            inputFileLocation,
            ">",
            outputFileLocation,
        ]
        try:
            subprocess.run(command, shell=True)
            self.displayMessage(f"has completed reducing task!")
            self.sendToMaster(DoneMessage(self.identifier))
            return True
        except Exception as E:
            print(str(E))
            return False

    def stateHandler(self):
        while True:
            if self.state == 1:
                # after we get a done message from all the mappers we start the reducing process
                if self.startReducing():
                    # send done to master to indicate that reducers have completed the task
                    self.sendToMaster(DoneMessage(self.identifier))
                    self.state += 1
                else:
                    self.sendToMaster(DoneMessage(self.identifier))
            if self.state == 2:
                break

    def handleReducerDataMessage(self, message: ReducerDataMessage):
        mode = "a"
        inputPath = os.path.join(
            os.getcwd(), "Test", self.homeDirName, self.identifier, "reducerInput.txt"
        )
        if not os.path.exists(inputPath):
            mode = "w"
        with open(inputPath, mode) as file:
            file.write(message.key + "\t" + message.value + "\n")

    def handleMessage(self, messageStr: str):
        messageStr = messageStr.decode("utf-8")
        # indicated that mappers have completed their tasks increment the state variable
        if "_Done_" in messageStr:
            self.numMappers -= 1
            if self.numMappers == 0:
                self.state = 1  # Make the current state as 1 to start the reducing task

        elif "_ReducerDataMessage_" in messageStr:
            self.handleReducerDataMessage(
                ReducerDataMessage.deserializeMessage(messageStr)
            )  # Write this data to file
        elif "_KillMessage_" in messageStr:
            # Handle kill message here
            self.displayMessage(f"is killed")
            os.kill(os.getpid(), SIGINT)
        
        elif "_ClearMessage_" in messageStr:
            # Clear input buffer
            inputPath = os.path.join(
            os.getcwd(), "Test", self.homeDirName, self.identifier, "reducerInput.txt")
            f = open(inputPath,'r+')
            f.truncate(0)
            self.state = 0



    def listenRequests(self):
        reducerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reducerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        reducerSocket.bind((self.host, self.port))
        reducerSocket.listen(10)
        self.displayMessage(f"is listening on {self.host}:{self.port}")
        while True:
            sock, addr = reducerSocket.accept()
            data = sock.recv(1024)
            if data != "":
                self.handleMessage(data)


    def __init__(self, identifier, reducingFunctionLocation, testcase, numMappers,restart):
        self.state = 0
        self.identifier = identifier
        self.reducingFunctionLocation = reducingFunctionLocation
        self.testcase = testcase
        self.numMappers = numMappers
        self.configData = self.readConfig()
        self.masterSendHost = self.configData["masterHost"]
        self.masterSendPort = self.configData["masterPort"]
        self.homeDirName = "Test" + str(self.testcase)
        self.port = self.configData["recvPort"]
        self.host = self.configData["host"]
        self.restart = restart
        self.DIE = False
        if "DEATH" in self.configData:
            self.DIE = True
        threading.Thread(target=self.listenRequests).start()
        threading.Thread(target=self.sendPulseToMaster).start()
        threading.Thread(target=self.stateHandler).start()
