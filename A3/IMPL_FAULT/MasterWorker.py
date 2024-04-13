import os
import time
import multiprocessing
import threading
import json
import socket
from datetime import datetime
from Message import Message
from Message import DoneMessage
from Message import PulseMessage
from Message import SendToReducerMessage
from Message import KillMessage
from Message import StopMessage
from Message import ClearMessage
from MapperWorker import MapperWorker
from ReducerWorker import ReducerWorker
from signal import SIGINT
from colorama import Fore, Style
import logging



class Master:
    def displayMessage(self,msg):
        logger = logging.getLogger(__name__)
        logging.basicConfig(filename='master.log', encoding='utf-8', level=logging.DEBUG)
        logging.debug(Fore.GREEN
            + Style.BRIGHT
            + "> Master"
            + "..........."
            + Style.RESET_ALL
            + Fore.LIGHTRED_EX
            + msg
            + Style.RESET_ALL)
        print(
            Fore.GREEN
            + Style.BRIGHT
            + "> Master"
            + "..........."
            + Style.RESET_ALL
            + Fore.LIGHTRED_EX
            + msg
            + Style.RESET_ALL
        )

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
            # print(self.reducerMetadata)
            if self.reducerMetadata[reducer]["status"] != "Done":
                status = False
        if status:
            self.currentState = 6  # Transition to reducing tasks done
        return True

    def respawnMapper(self, mid):
        # Cleanup based on states
        if self.currentState >= 3:  # When reducers are spawned
            cntFailedMap = self.broadCastMappers(StopMessage("master"))  # Stopping data flow to reducers
            self.displayMessage(" Sent stop message to all mappers "+ str(cntFailedMap))
            # We send clear message to all reducers to stop the stale data
            cntfailedRed = self.broadCastReducers(ClearMessage("master"))  # To indicate clearing of input buffers for reduce task
            self.displayMessage(" Sent clear message to all reducers "+str(cntfailedRed))
            self.SPWANREDFLAG = False
        
        # If reducers are not spawned we simply just respawn the mapper and restart its execution
        self.currentState = 1 # Indicates waiting for mapping to complete
        self.mapperMetadata[mid]["status"] = "mapping" # Now again wait for the done message from this mapper
        self.handleSpawnmapperHelper(mid,True)
        self.displayMessage("Respawned mapper "+mid)
        
            

        

    def respawnReducer(self, rid):
        # Send clear buffer messages to all reducers
        countFailedMap = self.broadCastMappers(StopMessage(sender="master"))
        countFailed = self.broadCastReducers(ClearMessage(sender="master"))
        self.handleSpawnReducerHelper(rid,True)
        self.displayMessage("Respawning reducer "+rid)
        time.sleep(0.01)
        self.currentState = 4 # Resend the request to send to all mappers

    def checkPulses(self):
        time.sleep(2)
        while True:
            # print(self.mapperMetadfata)
            if self.currentState >= 6:
                break
            for mapper in self.mapperMetadata:
                # print(mapper)
                if (
                    datetime.now().timestamp()
                    - self.mapperMetadata[mapper]["lastPulse"]
                    > 2.5
                ):
                    self.displayMessage("Mapper "+mapper+" has died!")
                    # We respawn this mapper again
                    self.respawnMapper(mapper)

                    # Go to spawnmappers state
            if self.currentState >= 4:
                for reducer in self.reducerMetadata:
                    if (
                        datetime.now().timestamp()
                        - self.reducerMetadata[reducer]["lastPulse"]
                        > 2.5
                    ):
                        print(self.reducerMetadata[reducer]["lastPulse"],datetime.now().timestamp())
                        self.displayMessage("Reducer "+reducer+" has died!")
                        # respawn the reducer and make th state back to sendtoredcer
                        self.respawnReducer(reducer)
            time.sleep(4)

    def handleMessage(self, messageStr):
        if "_Done_" in messageStr:
            # Distinguish between mapper or reducer based on state
            msg = DoneMessage.deserializeMessage(messageStr)
            if str(msg.sender).startswith("M"):
                # call done handler for the current mapper    
                self.mapperDoneHandler(DoneMessage.deserializeMessage(messageStr))
            else:
                # call done handler for reducers
                self.reducerDoneHandler(DoneMessage.deserializeMessage(messageStr))
                # print(self.currentState)

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
        self.displayMessage(f"is listening on port:{self.recvHost}:{self.recvPort}")
        while True:
            sock, addr = masterSocket.accept()
            data = sock.recv(1024)
            self.handleMessage(data.decode("utf-8"))

    # def spawnMapperHelper(self,restart=False):
    #     mapper = MapperWorker(
    #         "./input.py", "./input.txt", self.testcase, "M1", len(self.mapperMetadata),restart
    #     )

    def handleSpawnmapperHelper(self, mid,restart):
        multiprocessing.Process(
            target=MapperWorker,
            args=(
                "./Test/Test" + str(self.testcase) + "/input.py",
                "./Test/Test" + str(self.testcase),
                self.testcase,
                mid,
                len(self.mapperMetadata),
                restart
            ),
        ).start()

    def handleSpawnMappers(self):
        for mapper in self.mapperMetadata:
            self.handleSpawnmapperHelper(mapper,False)

    def handleSpawnReducerHelper(self, rid,restart=False):
        multiprocessing.Process(
            target=ReducerWorker,
            args=(
                rid,
                "./Test/Test" + str(self.testcase) + "/input.py",
                self.testcase,
                len(self.mapperMetadata),
                restart
            ),
        ).start()

    def handleSpawnReducers(self):
        for reducer in self.reducerMetadata:
            self.handleSpawnReducerHelper(reducer,False)

    def send(self, port, host, message):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.sendall(bytes(message.serializeMessage(), "utf-8"))
            sock.close()
            return True
        except Exception as E:
            return False

    def sendToMapper(self, mid, message: Message):

        return self.send(
            self.mapperMetadata[mid]["recvPort"],
            self.mapperMetadata[mid]["host"],
            message,
        )

    def sendToReducer(self, rid, message: Message):
        return self.send(
            self.reducerMetadata[rid]["recvPort"],
            self.reducerMetadata[rid]["host"],
            message,
        )

    def broadCastMappers(self, message: Message):
        failed = 0
        for mapper in self.mapperMetadata:
            if not self.sendToMapper(mapper, message):
                failed += 1
        return failed

    def broadCastReducers(self, message: Message):
        failed = 0
        for reducer in self.reducerMetadata:
            if not self.sendToReducer(reducer, message):
                failed += 1
        return failed

    def sendKillMappers(self):
        self.broadCastMappers(KillMessage("master"))
        # print("> Master: killed all mappers")
        self.displayMessage("killed all mappers")

    def sendKillReducers(self):
        self.broadCastReducers(KillMessage("master"))
        self.displayMessage("killed all reducers")

    def sendKillMessages(self):
        self.sendKillMappers()
        self.sendKillReducers()
        return True

    def sendRequestToSend(self):
        # Iterate over all mappers and send a request to send message
        reducerPortInfo = []
        # print(self.reducerMetadata)
        for reducer in self.reducerMetadata:
            reducerPortInfo.append(
                {
                    "recvPort": self.reducerMetadata[reducer]["recvPort"],
                    "host": self.reducerMetadata[reducer]["host"],
                }
            )

        message = SendToReducerMessage("master", reducerPortInfo=reducerPortInfo)
        self.broadCastMappers(message)

    def handleStateChange(self):
        while True:
            if self.currentState == 0:
                self.displayMessage("Spwaning mappers.")
                self.handleSpawnMappers()
                self.displayMessage("Spwaning mappers done.")
                self.currentState += 1  # Enter mapping state
            elif self.currentState == 2:
                self.displayMessage("Mapping tasks done.")
                self.currentState += 1
            elif self.currentState == 3:
                if self.SPWANREDFLAG:
                    self.displayMessage("Spwaning reducers.")
                    self.handleSpawnReducers()  # Spawn reducers
                else:
                    self.displayMessage("Reducers already spwaned.")
                self.currentState += 1
            elif self.currentState == 4:
                self.displayMessage("sending request to send to mappers and waiting for reducers to finish.")
                self.sendRequestToSend()
                self.currentState = 5  # Waiting state
            elif self.currentState == 6:
                self.displayMessage("Sending kill messages.")
                self.sendKillMessages()
                time.sleep(1)
                self.currentState += 1
            elif self.currentState == 7:
                time.sleep(2)
                self.displayMessage("Killing master.")
                os.kill(os.getpid(), SIGINT)


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
        self.SPWANREDFLAG = True

        for mapper in self.configData["mappers"]:
            self.mapperMetadata[mapper] = self.configData["mappers"][mapper]
            self.mapperMetadata[mapper]["lastPulse"] = datetime.now().timestamp()
            self.mapperMetadata[mapper]["status"] = "mapping"

        self.reducerMetadata = self.configData["reducers"]
        for reducer in self.reducerMetadata:
            self.reducerMetadata[reducer]["lastPulse"] = datetime.now().timestamp()
            self.reducerMetadata[reducer]["status"] = "reducing"

        threading.Thread(target=self.handleStateChange).start()
        threading.Thread(target=self.listenOnRecvPort).start()
        threading.Thread(target=self.checkPulses).start()
