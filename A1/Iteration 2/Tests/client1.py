from pymemcache.client.base import Client
import time


# Create a Memcached client
client = Client(("127.0.0.1", 9889))

commands = [
    {
        "type": "set",
        "key": "example",
        "value": "example1",
        "expected": "NOT STORED\r\n",
    },
    {"type": "get", "key": "example", "expected": "END\r\n"},
    {"type": "get", "key": "example2", "expected": "END\r\n"},
    {
        "type": "set",
        "key": "example2",
        "value": "example2value",
        "expected": "STORED\r\n",
    },
    # {"type": "get", "key": "example2"},
    # {"type": "get", "key": "example"},
]


answer = []
for command in commands:
    if command["type"] == "set":
        answer.append(
            client.raw_command(
                "set 1024 " + command["key"] + " noreply " + command["value"] + " \n"
            )
            + bytes("::TIME:" + str(time.time()), "utf-8")
        )

    else:
        answer.append(
            client.raw_command("get " + command["key"] + "\n")
            + bytes("::TIME:" + str(time.time()), "utf-8")
        )
    time.sleep(1)

with open("./Outputs/client1.txt", "w", encoding="utf-8") as file:
    # Iterate over the list of bytes
    for bytes_data in answer:
        # Decode the bytes
        decoded_text = bytes_data.decode("utf-8")

        # Write the decoded string to the file
        file.write("=======================\n" + decoded_text + "\n")
