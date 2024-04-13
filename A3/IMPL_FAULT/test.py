import sys
import ast

opDict = {}
for line in sys.stdin.read().split("\n"):
    if len(line.split("\t")) == 2:
        key,value = line.split("\t")
        value = ast.literal_eval(value)
        if key not in opDict:
            opDict[key] = []
        if value[0] not in opDict[key]:
            opDict[key].append(value[0])
for key in opDict:
    print(key,opDict[key])