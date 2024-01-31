# Code for merging the outputs from both the files
# We read all the files in output and then merge it here

import os
import json
import sys


if sys.argv[1] == None:
    print("Please provide the testcase number")
    exit()
outputFilesPath = "./Outputs/" + "Test-" + sys.argv[1]

data = []
for files in os.listdir(outputFilesPath):
    with open(outputFilesPath + "/" + files, "r") as f:
        temp = f.read()
        temp = temp.split("=======================")
        dat = []
        for line in temp:
            print(line)
            t1 = []
            ans = line.split(":")
            if ans != [""]:
                t1.append(float(ans[-1][:-1]))
                t1.insert(0, files.split(".")[0])
                t1.append(line.split("::")[0].replace("\n", " "))
                dat.append(t1)
        # for line in temp:
        #     ans = line.split(":")
        #     if ans != [""]:
        #         ans[-1] = float(ans[-1][:-1])
        #         ans.insert(0, files.split(".")[0])
        #         dat.append(ans)
        data += dat
print(data)
sorted_list = sorted(data, key=lambda x: x[1])

with open("./TestOutputs/Test-" + sys.argv[1] + "/CombinedOpt.json", "w") as file:
    json.dump(sorted_list, file, indent=2)
