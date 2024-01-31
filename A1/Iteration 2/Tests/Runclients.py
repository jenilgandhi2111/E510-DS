import subprocess
import os
import sys


# Define the paths to your Python scripts
# Function to run a script in a separate process
def run_script(script_path):
    try:
        subprocess.Popen(script_path)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path}: {e}")


if __name__ == "__main__":
    if sys.argv[1] == None:
        print("Enter the testcase number")
        exit()

    scripts = []

    for fil in os.listdir("./Inputs/Test-" + str(sys.argv[1])):
        scripts.append(
            ["python", "./client1.py", "./Inputs/Test-" + sys.argv[1] + "/" + fil]
        )
        print(fil)
    for script in scripts:
        run_script(script)

    print("All scripts are running concurrently.")
