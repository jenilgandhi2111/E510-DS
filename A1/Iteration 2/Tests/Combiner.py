# Code for merging the outputs from both the files
# We read all the files in output and then merge it here

import os
import json

data = []
for files in os.listdir("./Outputs"):
    with open("./Outputs/" + files, "r") as f:
        temp = f.read()
        temp = temp.replace("\n", "").split("=======================")
        dat = []
        for line in temp:
            ans = line.split(":")
            if ans != [""]:
                ans[-1] = float(ans[-1][:-1])
                ans.insert(0, files.split(".")[0])
                dat.append(ans)
        data += dat
sorted_list = sorted(data, key=lambda x: x[-1])
with open("./CombinedOpt.json", "w") as file:
    json.dump(sorted_list, file, indent=2)
