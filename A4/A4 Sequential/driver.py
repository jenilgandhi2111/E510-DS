import multiprocessing
from replica import Replica
import time
from poolClients import PoolClients

class Driver():
    def spawnReplica(self,id):
        try:
            multiprocessing.Process(target=Replica,args=(id,)).start()
            time.sleep(.01)
            print(f"Replica {id}")
            return True
        except Exception as e:
            print(str(e))
            return False
    def spawnPoolClients(self,numClients):
        try:
            multiprocessing.Process(target=PoolClients,args=(numClients,)).start()
            print(f"Pooling clients")
            return True
        except Exception as e:
            print(str(e))
            return False

    def spawnReplicas(self):
        for i in range(self.numReplicas):
            if not self.spawnReplica(i+1):
                print("> Project break kabum!")
    

    
    def __init__(self,numReplicas,numClients):
        self.numReplicas = numReplicas
        self.numClients = numClients
        self.spawnReplicas()
        time.sleep(4)
        self.spawnPoolClients(numClients)


if __name__ == "__main__":
    Driver(3,2)
        
    