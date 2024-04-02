import sys

dic = {}

for data in sys.stdin.read().split("\n"):
    if len(data.split("\t")) == 2:
        [key, val] = data.split("\t")
        if key not in dic:
            dic[key] = []
        dic[key].append(val)

for key in dic.keys():
    print(key, dic[key])
