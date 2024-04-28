import os
import random
import json
from message import Message
import socket
import time
from message import SetMessage
from message import GetMessage
from message import GetAcknowledgementMessage
import threading
from colorama import Fore, Style
import ast

class Client():
    def __init__(self,cid,recvHost,recvPort):
        self.cid = str(cid)
        print(cid)
        self.recvHost = recvHost
        self.recvPort = recvPort
        self.replicas = self.readConfig() # Get data for replicas
        self.operations = self.getOperations() # Set all operations
        print(self.operations)
        print(self.replicas)
        threading.Thread(target=self.executeOperations).start()
        threading.Thread(target=self.listen).start()
    
    def log(self,data):
        print(
            Fore.GREEN
            + Style.BRIGHT
            + f"> Client {self.cid}"
            + ": "
            + Style.RESET_ALL
            + Fore.LIGHTRED_EX
            + data
            + Style.RESET_ALL
        )
    def logToFile(self,message):
        with open('./logs/client'+str(self.cid)+'.txt', 'a') as file:
            file.write(f"{time.time()}>{message}"+'\n')
    
    def logOperationsToFile(self,message):
        with open("./logs/clientOpr"+str(self.cid)+".txt","a") as file:
            file.write(f"{time.time()}>{message}\n")

    def executeOperations(self):

        try:
            for operation in self.operations:
                msg = None
                replicaNo = random.randint(0,len(self.replicas)-1)
                repl = self.replicas[replicaNo]
                senderHost = self.recvHost
                senderPort = self.recvPort
                toHost =  repl["host"]
                toPort =  repl["port"]
                self.log(str(replicaNo+1)+" "+str(operation))

                if operation["operation"] == "set":
                    msg = SetMessage(operation["key"],operation["value"],self.cid,senderPort,senderHost,toPort,toHost)
                    self.logOperationsToFile(f"SET {operation['key']} {operation['value']}")
                else:
                    msg = GetMessage(operation["key"],self.cid,senderPort,senderHost,toPort,toHost)
                    self.logOperationsToFile(f"GET {operation['key']}")

                
                self.log("OK Till here")
                if not self.dispatchRequest(msg):
                    raise "Failed sending message."
                time.sleep(.1)
            return True
        except Exception as E:
            print("Error in execute operations:",E)
            return False

    def readConfig(self):
        fileLoc = os.path.join(os.getcwd(),"Config","StoreConfig","data.json")
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
        data = []
        for ob in jsonData:
            data.append({"id":ob["id"],"host":ob["recvHost"],"port":ob["recvPort"]})
        return data
    
    def getOperations(self):
        self.log(self.cid)
        fileLoc = os.path.join("Config",str(self.cid),"operations.json")
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
        return jsonData
    
    def send(self,message:Message):
        host = message.toHost
        port = message.toPort
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host,port))
            sock.sendall(bytes(message.serializeMessage(), "utf-8"))
            sock.close()
            return True
        except Exception as e:
            self.log("Error in send::"+str(e))
            return False
        

    def dispatchRequest(self,message:Message):
        try:
            self.send(message) # Sends the request to random replicas
            return True
        except Exception as e:
            self.log(str(e))
            return False
        
    def handleMessages(self,message:str):
        messageDict = ast.literal_eval(message)
        print(messageDict["type"])
        if messageDict["type"] == "_GetAckMessage_":
            msg = GetAcknowledgementMessage.deserializeMessage(message)
            self.log(f"Recieved value {msg.value}")
            # self.logMessage(f"Recieved value {msg.value.decode('utf-8')}")
            self.logToFile(f"{msg.value}")
        else:
            self.log(f"SETACK")
            self.logToFile(f"SETACK")

    def listen(self):
        try:
            self.log(f"is listening on {self.recvHost}:{self.recvPort}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((self.recvHost,self.recvPort))
            sock.listen(10)
            while True:
                sck,addr = sock.accept()
                data = sck.recv(1024).decode("utf-8")
                if data != "":
                    self.handleMessages(data)

        except Exception as e:
            print("sdbfhj",e)
            return False


        
        