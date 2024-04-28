import os
import json
import redis
import socket
import threading
import time
from message import Message
from message import SetAcknowledgementMessage
from message import GetAcknowledgementMessage
from message import SetMessage
from message import GetMessage
import copy
class Replica():
    def __init__(self,id):
        '''
        Description: Init method
        '''
        print(f"Inside Replica constructor {id}")
        self.id = int(id)
        self.host,self.port,self.redisHost,self.redisPort,self.replicas = self.readConfig()
        self.dataStore = redis.Redis(host=self.redisHost,port=self.redisPort)
        self.dataStore.flushall(
            
        )
        self.checkRedis()
        self.run()
        
    def log(self,message):
        print(f"> R{self.id}: ",message)
        
    def checkRedis(self):
        if self.dataStore.ping():
            self.log(f"redis working on port:{self.redisPort}")
            
    def run(self):
        self.log(f"{self.id} is listening on port {self.port}.")
        threading.Thread(target=self.listen, args=()).start()
        
    def readConfig(self):
        '''
        Description: Reads the config file.
        '''
        fileLoc = os.path.join("Config","StoreConfig","data.json")
        with open(fileLoc, "r") as file:
            jsonData = json.loads(file.read())
        return jsonData[self.id-1]["recvHost"],jsonData[self.id-1]["recvPort"],jsonData[self.id-1]["redisHost"],jsonData[self.id-1]["redisPort"],[obj for obj in jsonData if obj["id"] != self.id]
    
    def send(self,host,port,message):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, int(port)))
            sock.sendall(bytes(message.serializeMessage(), "utf-8"))
            sock.close()
            return True
        except Exception as E:
            self.log(str(E)+str(message.message)+str(host)+str(port))
            return False
    
    def sendMessage(self,message:Message):
        self.log(message.message)
        try:
            if self.send(message.toHost,message.toPort,message):
                return True
            return False
        except Exception as e:
            self.log(str(e))
            return False
        
    def broadCast(self,message:Message):
        if not message.broadcast:
            return 0 
        failed = 0
        for replica in self.replicas:
            relayMessage = message.getRelayMessage(self.id,replica["recvHost"],replica["recvPort"])
            if not self.sendMessage(relayMessage):
                self.log(f"failed sending data to [{replica['id']}] {replica['port']}")
                failed+=1
            self.log(f"Sent broadcast to replica{replica['id']}")
        return failed
    
    
    def messageDispatcher(self,message:Message):
        print(message)
        if  "_SetMessage_" in message:
            self.log(f"Got set message.")
            self.Set(SetMessage.deserializeMessage(message))
        elif "_GetMessage_" in message:
            self.Get(GetMessage.deserializeMessage(message))
    
    
    
    
        
    
    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host,self.port))
        sock.listen(10)
        self.log(f"is listening on {self.host}:{self.port}")
        while True:
            sck,addr = sock.accept()
            data = sck.recv(1024)
            self.messageDispatcher(data.decode("utf-8")) 


    ##################################################################################################
    # Set Helpers
    
    def sendSetAck(self,message:SetMessage):
        try:
            if self.sendMessage(message.getAckMessage(str(self.id),message.key)):
                return True
            return False
        except Exception as e:
            self.log(str(e))
            return False
    

    def setHelper(self,message:SetMessage):
        try:
            if self.dataStore.set(message.key,message.value):
                return True
            return False
        except Exception as e:
            self.log(str(e))
            return False
    
    def Set(self,message:SetMessage): 
        # Everntual Consitency
        try:
            time.sleep(2)
            if self.setHelper(message): # Using write before read strategy
                if self.broadCast(message) == 0:
                    return True
                return False
            return False
        except Exception as e:
            self.log(str(e))
            return False



    ##################################################################################################
    # Get Helpers

    def sendGetAck(self,message:GetMessage,value): 
        try:
            if self.sendMessage(message.getAckMessage(str(self.id),value)):
                return True
            return False
        except Exception as e:
            self.log(str(e))
            return False
        
    def getHelper(self,message:GetMessage):
        try:
            data = self.dataStore.get(message.key)
            if data!=None:
                return data
            return "<NIL>"
        except Exception as e:
            self.log(str(e))
            return False
    
    def Get(self,message:GetMessage):
        try:
            data = self.getHelper(message) # Reading local value
            self.log(data)
            if not data:
                raise "Some unknown error occured!"
            if self.sendGetAck(message,data):
                return True
            return False
        except Exception as e:
            self.log(str(e))
            return False