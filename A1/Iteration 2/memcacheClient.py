from pymemcache.client.base import Client
import time

# Create a client with logging enabled
client = Client(
    ("127.0.0.1", 9889),
    serializer=None,
    deserializer=None,
    connect_timeout=10,
    timeout=10,
)

# Perform get and set operations
# print(client.set("example_key1", "example_value"))
# time.sleep(2)
print(client.get("example_key1"))
# value = client.get("example_key")
# print(value)
