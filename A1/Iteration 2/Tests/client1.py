from pymemcache.client.base import Client
import time
import sys
import json

if sys.argv[1] == None:
    print("Specify file name!")
    exit()


# Create a Memcached client
client = Client(("127.0.0.1", 9889))

commands = None

with open(sys.argv[1], "r") as file:
    commands = json.load(file)

answer = []
for command in commands:
    if command["type"] == "set":
        answer.append(
            client.raw_command(
                "set " + command["key"] + "1024 noreply " + command["value"] + " \n"
            )
            + bytes("::TIME:" + str(time.time()), "utf-8")
        )

    else:
        answer.append(
            client.raw_command("get " + command["key"] + "\n")
            + bytes("::TIME:" + str(time.time()), "utf-8")
        )
    time.sleep(1)

with open(
    "./Outputs/" + sys.argv[1].split("/")[2].split(".")[0] + ".txt",
    "w",
    encoding="utf-8",
) as file:
    # Iterate over the list of bytes
    for bytes_data in answer:
        # Decode the bytes
        decoded_text = bytes_data.decode("utf-8")

        # Write the decoded string to the file
        file.write("=======================\n" + decoded_text + "\n")
