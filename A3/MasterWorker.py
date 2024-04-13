import os
import time
import multiprocessing
import threading
import json
import socket
from datetime import datetime
from Message import DoneMessage
from Message import PulseMessage
from Message import SendToReducerMessage
from MapperWorker import MapperWorker
from ReducerWorker import ReducerWorker


class Master:

    def readConfig(self, testCase):
        fileLoc = "./Test/Test" + str(testCase) + "/conf.json"
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
            return jsonData

    def mapperDoneHandler(self, message: DoneMessage):
        self.mapperMetadata[message.sender]["status"] = "Done"
        status = True
        for mapper in self.mapperMetadata:
            if self.mapperMetadata[mapper]["status"] != "Done":
                status = False
        if status:
            self.currentState += 1  # Transition to mapping tasks done
        return True

    def reducerDoneHandler(self, message: DoneMessage):
        self.reducerMetadata[message.sender]["status"] = "Done"
        status = True
        for reducer in self.reducerMetadata:
            print(self.reducerMetadata)
            if self.reducerMetadata[reducer]["status"] != "Done":
                status = False
        if status:
            self.currentState = 5  # Transition to reducing tasks done
        return True

    def checkPulses(self):
        time.sleep(2)
        while True:
            # print(self.mapperMetadata)
            for mapper in self.mapperMetadata:
                # print(mapper)
                if (
                    datetime.now().timestamp()
                    - self.mapperMetadata[mapper]["lastPulse"]
                    > 2
                ):
                    print("Mapper", mapper, "dead!")
                    # Go to spawnmappers state
            if self.currentState > 4:
                for reducer in self.reducerMetadata:
                    if (
                        datetime.now().timestamp()
                        - self.reducerMetadata[reducer]["lastPulse"]
                        > 2
                    ):
                        print("Reducer", reducer, "dead!")
                    # make the state to sendRts again
            time.sleep(2)

    def handleMessage(self, messageStr):
        if "_Done_" in messageStr:
            # Distinguish between mapper or reducer based on state
            if self.currentState < 2:  # On the mapping state
                # call done handler for the current mapper
                self.mapperDoneHandler(DoneMessage.deserializeMessage(messageStr))
            else:
                # call done handler for reducers
                self.reducerDoneHandler(DoneMessage.deserializeMessage(messageStr))
                print(self.currentState)

        elif "_PulseMessage_" in messageStr:
            # Distinguish between mapper or reducer based on state
            pulseMessage = PulseMessage.deserializeMessage(messageStr)
            if pulseMessage.sender[0] == "M":
                self.mapperMetadata[pulseMessage.sender][
                    "lastPulse"
                ] = datetime.now().timestamp()
            else:
                if self.currentState > 2:
                    self.reducerMetadata[pulseMessage.sender][
                        "lastPulse"
                    ] = datetime.now().timestamp()

    def listenOnRecvPort(self):
        masterSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        masterSocket.bind((self.recvHost, self.recvPort))
        masterSocket.listen(100)
        print(f"Master, is listening on port:{self.recvHost}:{self.recvPort}")
        while True:
            sock, addr = masterSocket.accept()
            data = sock.recv(1024)
            self.handleMessage(data.decode("utf-8"))

    def spawnMapperHelper(self):
        mapper = MapperWorker(
            "./input.py", "./input.txt", self.testcase, "M1", len(self.mapperMetadata)
        )

    def handleSpawnMappers(self):
        for mapper in self.mapperMetadata:
            multiprocessing.Process(
                target=MapperWorker,
                args=(
                    "./Test/Test" + str(self.testcase) + "/input.py",
                    "./Test/Test" + str(self.testcase),
                    self.testcase,
                    mapper,
                    len(self.mapperMetadata),
                ),
            ).start()

    def handleSpawnReducers(self):
        for reducer in self.reducerMetadata:
            multiprocessing.Process(
                target=ReducerWorker,
                args=(
                    reducer,
                    "./Test/Test" + str(self.testcase) + "/input.py",
                    self.testcase,
                    len(self.mapperMetadata),
                ),
            ).start()
        pass

    def sendRequestToSend(self):
        # Iterate over all mappers and send a request to send message
        reducerPortInfo = []
        print(self.reducerMetadata)
        for reducer in self.reducerMetadata:
            reducerPortInfo.append(
                {
                    "recvPort": self.reducerMetadata[reducer]["recvPort"],
                    "host": self.reducerMetadata[reducer]["host"],
                }
            )

        message = SendToReducerMessage("master", reducerPortInfo=reducerPortInfo)
        for mapper in self.mapperMetadata:  # Send everyone a RTS
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(
                (
                    self.mapperMetadata[mapper]["host"],
                    self.mapperMetadata[mapper]["recvPort"],
                )
            )
            sock.sendall(
                bytes(message.serializeMessage(), "utf-8")
            )  # Sends the object from the head of the queue after it is popped
            sock.close()

    def handleStateChange(self):
        while True:
            print(self.currentState)
            if self.currentState == 0:
                print("Spawning mappers")
                self.handleSpawnMappers()
                print("Spawning mappers done!")
                self.currentState += 1  # Enter mapping state
            elif self.currentState == 2:
                print("Mapping tasks done")
                self.currentState += 1
            elif self.currentState == 3:
                print("Spawning reducers")
                self.handleSpawnReducers()  # Spawn reducers
                self.currentState += 1
            elif self.currentState == 4:
                self.sendRequestToSend()  # Send request to send to all mappers
                print("Request to send to reducers!")
                self.currentState += 1
            elif self.currentState == 5:
                print("Reducing Tasks done!")
                exit()
                # Reducing tasks done when we get all done messages from reducers
                # Call exit here

    def __init__(self, testcase):
        # Following could be the possible states:
        # 1.) Spawn mappers
        # 2.) mapping
        # 3.) Spawn reducers
        # 4.) reducing
        # 5.) done
        self.possibleStates = [
            "spawnMappers",
            "mapping",
            "mappingDone",
            "spawnReducers",
            "sendRTS",
            "reducing",
            "reducingDone",
        ]
        self.currentState = 0
        self.testcase = testcase
        self.configData = self.readConfig(testcase)
        self.recvPort = self.configData["master"]["recvPort"]
        self.recvHost = self.configData["master"]["host"]
        self.mapperMetadata = {}
        self.reducerMetadata = {}
        self.TIMEOUT = 2

        for mapper in self.configData["mappers"]:
            self.mapperMetadata[mapper] = self.configData["mappers"][mapper]
            self.mapperMetadata[mapper]["lastPulse"] = datetime.now().timestamp()
            self.mapperMetadata[mapper]["status"] = "mapping"

        self.reducerMetadata = self.configData["reducers"]
        for reducer in self.reducerMetadata:
            self.reducerMetadata[reducer]["lastPulse"] = datetime.now().timestamp()
            self.reducerMetadata[reducer]["status"] = "reducing"

        # for reducer in self.configData["reducers"]:
        #     self.reducerMetadata[reducer] = self.configData["reducers"]
        #     self.reducerMetadata[reducer]["lastPulse"] = datetime.now().timestamp()
        #     self.reducerMetadata[reducer]["status"] = "reducing"
        threading.Thread(target=self.handleStateChange).start()
        threading.Thread(target=self.listenOnRecvPort).start()
        threading.Thread(target=self.checkPulses).start()


# if __name__ == "__main__":
#     Master(1)
