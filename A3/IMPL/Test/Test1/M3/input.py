import sys
import time

data = sys.stdin.read().split(" ")
for dat in data:
    time.sleep(0.01)
    print(dat + "\t1")
