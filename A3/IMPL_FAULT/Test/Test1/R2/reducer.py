# import sys

# opDict = {}
# for line in sys.stdin.read().split("\n"):
#     if len(line.split("\t")) == 2:
#         key, value = line.split("\t")
#         opDict[key] = opDict.get(key, 0) + int(value)

# for key in opDict:
#     print(key, opDict[key])
import sys
import ast

opDict = {}
for line in sys.stdin.read().split("\n"):
    if len(line.split("\t")) == 2:
        key,value = line.split("\t")
        if key not in opDict:
            opDict[key] = []
        opDict[key]+=ast.literal_eval(value)
for key in opDict:
    print(key,len(opDict[key]))