import os
import json
import redis
import socket
import threading
import ast
import time
import copy
from PriorityQueue import Heap
from message import Message
from message import SetMessage
from message import GetMessage
from message import AcknowledgementMessage
class Replica():
    def __init__(self,id):
        '''
        Description: Init method
        '''
        print(f"Inside Replica constructor {id}")
        self.id = int(id)
        self.host,self.port,self.redisHost,self.redisPort,self.replicas = self.readConfig()
        self.numReplicas = len(self.replicas)
        self.dataStore = redis.Redis(host=self.redisHost,port=self.redisPort)
        self.dataStore.flushall()
        self.checkRedis()
        self.acks = {}
        self.remAcks = []
        self.queue = Heap.Heap([],self.comparator)
        self.run()
    
    def log(self,message):
        print(f"> R{self.id}: ",message)
        
    def checkRedis(self):
        if self.dataStore.ping():
            self.log(f"redis working on port:{self.redisPort}")
            
    def run(self):
        self.log(f"{self.id} is listening on port {self.port}.")
        threading.Thread(target=self.listen, args=()).start()
        threading.Thread(target=self.processQueue, args=()).start()
       
    def readConfig(self):
        '''
        Description: Reads the config file.
        '''
        fileLoc = os.path.join("Config","StoreConfig","data.json")
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
        return jsonData[self.id-1]["recvHost"],jsonData[self.id-1]["recvPort"],jsonData[self.id-1]["redisHost"],jsonData[self.id-1]["redisPort"],[obj for obj in jsonData if obj["id"] != self.id]
    
    
    def send(self,host,port,message):
        message.senderHost = self.host
        message.senderPort = self.port
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.sendall(bytes(message.serializeMessage(), "utf-8"))
            # sock.sendall(bytes(message, "utf-8"))
            sock.close()
            return True
        except Exception as E:
            return False
        
    def sendMessage(self,message):
        if self.send(message.toHost,message.toPort,message):
            return True
        return False
    def broadcastMessage(self,message,sendSelf=False):
        # Would broadcast the given messages to all replicas
        failed = 0
        for replica in self.replicas:
            msg = copy.deepcopy(message)
            msg.senderPort = self.port
            msg.senderHost = self.host
            msg.toPort = replica["recvPort"]
            msg.toHost = replica["recvHost"]
            msg.timeStamp = message.timeStamp
            msg.generateMessage()
            if not self.sendMessage(msg):
                failed+=1
        if sendSelf:
            msg = copy.deepcopy(message)
            msg.toPort = msg.senderPort
            msg.toHost = msg.senderHost
            msg.generateMessage()
            if not self.sendMessage(msg):
                failed+=1
        return failed
    
    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host,self.port))
        sock.listen(10)
        self.log(f"is listening on {self.host}:{self.port}")
        while True:
            sck,addr = sock.accept()
            data = sck.recv(1024)
            self.messageHandler(data.decode("utf-8"))


    def processAck(self):
        for msg in self.queue.elems:
            for i in range(len(self.remAcks)):
                ackMessage = self.remAcks[i]
                if ackMessage.processAcknowledge(msg):
                    self.remAcks.pop(i)
                    return True
                
        return False


    def messageHandler(self,message):
        messageDict = ast.literal_eval(message)
        if messageDict["type"] == "_SetMessage_":
            self.log(" got set message")
            msg = SetMessage.deserializeMessage(message)
            msg.nacks = 3
            if msg.broadcast:
                msg.broadcast = False
                self.broadcastMessage(msg)
            self.queue.insert(msg,comparator=self.comparator)
        elif messageDict["type"] == "_GetMessage_":
            msg = GetMessage.deserializeMessage(message)
            msg.nacks = 3
            if msg.broadcast:
                msg.broadcast = False
                self.broadcastMessage(msg)
            self.queue.insert(msg,comparator=self.comparator)
        elif messageDict["type"] == "_ACK_":
            temp  = ast.literal_eval(message)
            self.log(str(self.id)+" got ack from "+str(temp["sender"]))
            self.remAcks.append(AcknowledgementMessage.deserializeMessage(message))
            self.processAck()


    def setValue(self,key,value):
        self.dataStore.set(key,value)
        return True
    def processSet(self,message:SetMessage):
        if self.setValue(message.key,message.value):
            print(message.senderPort,message.senderHost)
            self.sendSetSuccess(message)
        return False
    
    def sendSetSuccess(self,message:SetMessage):
        return self.sendMessage(message.replyMessage(self.id))



    def getValue(self,key):
        data = self.dataStore.get(key)
        if data == None:
            return "<NIL>"
        return data

    def processGet(self,message:GetMessage):
        if self.sendGetSuccess(message,self.getValue(message.key)):
            return True
        return False

    def sendGetSuccess(self,message:GetMessage,value):
        return self.sendMessage(message.replyMessage(self.id,message.key,value))

 

    def processQueue(self):
        while True:
            if len(self.queue.elems) > 0:
                self.log(self.queue.elems[0].nacks)
                if self.queue.elems[0].nacks <= 0:
                    # processs
                    if self.queue.elems[0].getMessageType() == "_GetMessage_":
                        self.log(str(self.id)+" is processing get messae")
                        self.processGet(self.queue.delete(comparator=self.comparator))
                    elif self.queue.elems[0].getMessageType() == "_SetMessage_":
                        self.log(str(self.id)+" is processing set messae")
                        self.processSet(self.queue.delete(comparator=self.comparator))
                    else:
                        self.log("Here in process queue error states")
                else:
                    # send ack if not already sent
                    self.broadcastAcks(self.queue.elems[0].getAckMessage(self.id))
  
    def broadcastAcks(self,ackMessage):
        time.sleep(1)
        if ackMessage.hash in self.acks.keys():
            return True
        if self.broadcastMessage(ackMessage,True) == 0:
            self.acks[ackMessage.hash] = True
            return True
        return False

    @staticmethod
    def comparator(elem1, elem2):
        if elem1.timeStamp < elem2.timeStamp:
            return -1
        elif elem1.timeStamp > elem2.timeStamp:
            return 1
        return 0
    
