import json
import os
import sys
import multiprocessing
from Block import Block


class Runner:
    def spawnBlock(self, blkId, testcaseId):
        Block(blockId=blkId, testCaseId=testcaseId)
        print(f"Block spawned with blockid {blkId}")

    def __init__(self, n_proc, testcaseId):
        try:
            for i in range(n_proc):
                p = multiprocessing.Process(
                    target=self.spawnBlock,
                    args=(
                        i + 1,
                        testcaseId,
                    ),
                )
                p.start()
        except Exception as E:
            print(str(E))


def readConfig(testcaseId):
    with open("./Config/" + testcaseId + "/" + testcaseId + ".json", "r") as file:
        data = json.load(file)
        return len(data.keys())


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("Usage: python Runner.py 1 <Where 1 is the testcase number>")
        exit()
    n_proc = readConfig(
        str(sys.argv[1])
    )  # This would give the number of processes to spawn
    Runner(n_proc, testcaseId=sys.argv[1])
