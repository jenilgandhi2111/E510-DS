# import sys

# dic = {}

# for data in sys.stdin.read().split("\n"):
#     if len(data.split("\t")) == 2:
#         [key, val] = data.split("\t")
#         if key not in dic:
#             dic[key] = []
#         dic[key].append(val)

# for key in dic.keys():
#     print(key, dic[key])
import sys
import ast
import time
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
    # time.sleep(0.04)
    print(key,opDict[key])