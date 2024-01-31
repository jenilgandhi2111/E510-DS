import os
import sys
import subprocess

if sys.argv[1] == None:
    print("Provide Number of testcases")
    exit()
N_Tests = sys.argv[1]


def spawn_test(testNo):
    try:
        proc = subprocess.run(["python", "Runclients.py", str(testNo)])
        if proc.returncode == 0:
            proc1 = subprocess.run(["python", "Combiner.py", str(testNo)])
            if proc1.returncode != 0:
                raise Exception("UNK")
    except subprocess.CalledProcessError as e:
        print(f"Error running {testNo}: {e}")


for i in range(int(N_Tests)):
    spawn_test(i + 1)
print("Spawned " + N_Tests + " Testcases")
