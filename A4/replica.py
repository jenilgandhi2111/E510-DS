import os
import json
import redis
import socket
import threading

class Replica():
    def __init__(self,id):
        '''
        Description: Init method
        '''
        print(f"Inside Replica constructor {id}")
        self.id = int(id)
        self.host,self.port,self.redisHost,self.redisPort,self.replicas = self.readConfig()
        self.dataStore = redis.Redis(host=self.redisHost,port=self.redisPort)
        self.checkRedis()
        self.run()
        
    def log(self,message):
        print(f"> {self.id}: ",message)
        
    def checkRedis(self):
        if self.dataStore.ping():
            self.log(f"redis working on port:{self.redisPort}")
            
    def run(self):
        self.log(f"{self.id} is listening on port {self.port}.")
        listenThread = threading.Thread(target=self.listen, args=()).start()
        
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
            sock.connect((host, port))
            # sock.sendall(bytes(message.serializeMessage(), "utf-8"))
            sock.sendall(bytes(message, "utf-8"))
            sock.close()
            return True
        except Exception as E:
            return False
        
    def broadCast(self):
        failed = 0
        for replica in self.replicas:
            if not self.send(replica["host"],replica["port"],"TMKC"):
                self.log(f"failed sending data to [{replica['id']}] {replica['port']}")
                failed+=1
        return failed
    
    
    def messageDispatcher(self,message):
        print(message)
    
    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host,self.port))
        sock.listen(10)
        self.log(f"is listening on {self.host}:{self.port}")
        while True:
            sck,addr = sock.accept()
            data = sck.recv(1024)
            self.messageDispatcher(data.decode("utf-8")) 