# Read the operations file for all clients
import json
import os

############################################################################
def readOperations(clientId):
    with open(os.path.join(os.getcwd(),"Config","C"+str(clientId),"operations.json")) as file:
        data = json.load(file)
    return data

def getOperations(replicaId):
    operations = readOperations(replicaId)
    data = {}
    for operation in operations:
        if operation["operation"] == "set":
            data[operation["key"] ]= operation["value"]
        else:
            operation["truth"] = data.get(operation["key"],"NIL>")
    return operations
def getReplicaOperations(numreplicas):
    replicaOperations = {}
    for i in range(numreplicas):
        replicaOperations[i+1] = getOperations(i+1)
    return replicaOperations
###########################################################################
def readClientLogs(clientid):
    with open(os.path.join(os.getcwd(),f"clientC{str(clientid)}.txt")) as file:
        data = file.read().split("\n")[:-1]
    return data

def verifyTruth(operationsDict,clientLog,clientId):
    opr = operationsDict[clientId]
    clientOpr = clientLog[clientId]
    clientOprId= 0

    for operation in opr:
        if operation["operation"] == "get":
            try:
                if operation["truth"] != "NIL>":
                    assert "NIL" not in clientOpr[clientOprId]
                else:
                    assert "NIL" in clientOpr[clientOprId]
                clientOprId+=1
            except Exception as E:
                print(E)
                return False
    return True
def getAllClientOperations(clients):
    oprDict = {}
    for i in range(clients):
        oprDict[i+1] = readClientLogs(i+1)
    return oprDict

def verifyOperations(replicaOperations,operationsDict,numReplicas):
    for i in range(numReplicas):
        if not verifyTruth(replicaOperations,operationsDict,i+1):
            # print("> Test failed!")
            return False
    return True
if __name__ == "__main__":
    replicaOperations = getReplicaOperations(2)
    operationsDict= getAllClientOperations(2)
    if verifyOperations(replicaOperations,operationsDict,2):
        print("> Test Passed!")
    else:
        print("> Test failed")