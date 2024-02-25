import json
import os
import multiprocessing
from Block import Block


class Runner:
    def spawnBlock(self, blkId):
        Block(blockId=blkId)
        print(f"Block spawned with blockid {blkId}")

    def __init__(self, n_proc):
        try:
            for i in range(n_proc):
                p = multiprocessing.Process(target=self.spawnBlock, args=(i + 1,))
                p.start()
        except Exception as E:
            print(str(E))


if __name__ == "__main__":
    Runner(3)
