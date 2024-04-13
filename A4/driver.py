import multiprocessing
from replica import Replica
import time

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
    def __init__(self,numReplicas):
        for i in range(numReplicas):
            if not self.spawnReplica(i+1):
                print("> Project break kabum!")
                break
            

if __name__ == "__main__":
    Driver(3)
        
    