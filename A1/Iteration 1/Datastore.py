import threading
import os
import time


class DataStore:
    # If we want the data from the file that is already present we
    # can opt in for this option
    def __init__(self):
        self.dataPath = "./Objects/objfile.txt"

        # Check if the file exists or not
        if not os.path.isfile(self.dataPath):
            f = open(self.dataPath, "w")
            f.close()

    def get(self, key):
        # Read the file and find out the key
        with open(self.dataPath, "r") as file:
            for line in file:
                ln = line.split(",")
                if ln[-2] == "key":
                    return ln[-1]
        return "NOT FOUND"

    def set(self, key, value, storer="UNK"):
        print(key, value)
        # check if the key is present or not
        if self.get(key) != "NOT FOUND":
            return "NOT SET"

        with open(self.dataPath, "a") as file:
            file.write(str(time.time()) + "," + storer + "," + key + "," + value + "\n")
        return "SET"
