import os
import sys

def readOperations(clientid):
    with open(os.path.join(os.getcwd(),"logs",f"clientOprC{clientid}.txt")) as file:
        data = file.read().split("\n")[:-1]
        for i in range(len(data)):
            data[i] = data[i].split(">")
            opr = data[i][1].split(" ")
            dic = {}
            if opr[0] == "GET":
                dic["operation"] = "GET"
                dic["key"] = opr[1]
            else:
                dic["operation"] = "SET"
                dic["key"] = opr[1]
                dic["value"] = opr[2]
            data[i][1] = dic
    return data

def readClientOutputs(clientid):
    with open(os.path.join(os.getcwd(),"logs",f"clientC{clientid}.txt")) as file:
        data = file.read().split("\n")[:-1]
        for i in range(len(data)):
            data[i] = data[i].split(">")
    return data

def outputOperations(clients):
    data = []
    for i in range(clients):
        data+=readClientOutputs(i+1)
    data = sorted(data, key=lambda x: float(x[0]))
    print(data)
    return data

def inputOperations(numReplicas):
    inputOprs = []
    for i in range(numReplicas):
        inputOprs+=readOperations(i+1)
    inputOprs = sorted(inputOprs, key=lambda x: float(x[0]))
    
    keyDic= {}
    for [_,opr] in inputOprs:
        if opr["operation"] == "SET":
            keyDic[opr["key"]] = opr["value"]
        else:
            opr["truth"] = keyDic.get(opr["key"],"NIL")
    return inputOprs



def assertTruth(inputOperations,outputOperations):
    oo=0
    for [_,opr] in inputOperations:
        if opr["operation"] == "GET":
            print(opr,outputOperations[oo])
            try:
                assert opr["truth"] in outputOperations[oo][1]
            except Exception as E:
                print("Assertion failed " ,opr["truth"],outputOperations[oo][1])
                return False
            oo+=1
    return True
if __name__ == "__main__":
    ip = inputOperations(2)
    op = outputOperations(2)
    if assertTruth(ip,op):
        print("> test passed!")
    else:
        print("> test failed!")