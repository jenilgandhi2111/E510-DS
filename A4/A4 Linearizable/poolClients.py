import multiprocessing
import os 
import time
from clients import Client
from colorama import Fore, Style


class PoolClients():
    
    def __init__(self,numClients):
        self.basePort = 7000
        self.commonHost = "localhost"
        self.numClients = numClients
        self.spawnClient()

    def log(self,data):
        print(
            Fore.GREEN
            + Style.BRIGHT
            + "> PoolClient"
            + ":"
            + Style.RESET_ALL
            + Fore.LIGHTRED_EX
            + data
            + Style.RESET_ALL
        )
    def spawnClientHelper(self,cid,host,port):
        try:
            multiprocessing.Process(
                target=Client,
                args=(cid,"localhost",port)
            ).start()
            return True
        except Exception as e:
            print(e)
            return False
        
    def spawnClient(self):
        try:
            for i in range(1,self.numClients+1):
                if not self.spawnClientHelper("C"+str(i),self.commonHost,self.basePort):
                    raise "Failed spawning client"
                self.log("Spawned client")
                self.basePort+=1
            return True
        except Exception as e:
            print(e)
            return False