# import subprocess

# # Define the paths to your Python scripts
# # script1_path = 'script1.py'
# # script2_path = 'script2.py'
# scripts = ["client1.py", "client2.py"]


# # Function to run a script in a separate process
# def run_script(script_path):
#     try:
#         subprocess.run(["python", script_path], check=True)
#     except subprocess.CalledProcessError as e:
#         print(f"Error running {script_path}: {e}")


# if __name__ == "__main__":
#     for script in scripts:
#         process1 = subprocess.Popen(["python", script])
#         process1.wait()

#     print("Both scripts have finished executing.")

import subprocess

# Define the paths to your Python scripts
script1_path = ["python", "./client1.py", "./Inputs/client1.json"]
script2_path = ["python", "./client1.py", "./Inputs/client2.json"]
script3_path = ["python", "./client1.py", "./Inputs/client3.json"]


# Function to run a script in a separate process
def run_script(script_path):
    try:
        subprocess.Popen(script_path)
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path}: {e}")


if __name__ == "__main__":
    # Run both scripts concurrently
    print("1 Ran")
    run_script(script1_path)
    print("2 Ran")
    run_script(script2_path)
    print("3 Ran")
    run_script(script3_path)

    print("All scripts are running concurrently.")
