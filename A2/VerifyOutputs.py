import os
import sys

if len(sys.argv) <= 1:
    print("Usage: python VerifyOutputs.py 1")

dat = []
for f in os.listdir("./outputs/" + sys.argv[1]):
    with open("./outputs/" + sys.argv[1] + "/" + f, "r") as file:
        dat.append(file.read().split("\n")[1:])
ans = []
minLen = 100000000
for proces in dat:
    minLen = min(len(proces), minLen)
for proces in dat:
    temp = []
    for elem in proces:
        temp.append(elem.split("processes")[1][:2])
    ans.append(temp[:minLen])

for i in range(1, len(ans)):
    if ans[0] != ans[i]:
        print("Test failed")
        exit(0)
print("Test passed!")
