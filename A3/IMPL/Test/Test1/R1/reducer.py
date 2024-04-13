import sys

opDict = {}
for line in sys.stdin.read().split("\n"):
    if len(line.split("\t")) == 2:
        key, value = line.split("\t")
        opDict[key] = opDict.get(key, 0) + int(value)

for key in opDict:
    print(key, opDict[key])
