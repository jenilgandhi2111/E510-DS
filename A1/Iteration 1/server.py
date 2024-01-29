import socket
from Datastore import DataStore
import threading


class Server:
    def __init__(self, host, port):
        self.port = port
        self.host = host

        # Path to data object to be read initially while bouncing back the server
        self.dataStore = DataStore()

    def parseCommand(self, ADDR, command: str):
        splitCommand = command.split(" ")
        print(splitCommand)
        if splitCommand[0].lower() == "get":
            return self.dataStore.get(splitCommand[-1])
        elif splitCommand[0].lower() == "set":
            return self.dataStore.set(splitCommand[-2], splitCommand[-1])
        return "UNK CMD"

    def handle_client(self, client_socket, addr):
        while True:
            data = client_socket.recv(1024).decode("utf-8")
            print("Recieved from address", addr, " Data:", data)
            # Now calling the parsecommand
            client_socket.send(bytes(self.parseCommand(addr, data), "utf-8"))
        # buffer = ""
        # while True:
        #     data = client_socket.recv(1024).decode("utf-8")
        #     print("Recieved from address", addr, " Data:", data)
        #     if data == "/":
        #         break
        #     if data == ",":
        #         print(buffer)
        #         answer = self.parseCommand(addr[1], buffer)
        #         client_socket.send(bytes(answer, "utf-8"))
        #         buffer = ""

        #     else:
        #         buffer += str(data)
        # client_socket.close()

    def startServer(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()

        while True:
            client_socket, addr = server_socket.accept()

            # Call client handler to handle multiple clients
            client_handler = threading.Thread(
                target=self.handle_client, args=(client_socket, addr)
            )
            client_handler.start()


server = Server(host="127.0.0.1", port=9889)

server.startServer()
