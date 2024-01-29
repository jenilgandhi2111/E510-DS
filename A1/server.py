import socket
import threading
from Data import Data
from DataStore import DataStore
from Client import Client


class Server:
    def __init__(self, host, port):
        self.port = port
        self.host = host
        self.clients = {}
        pass

    def parseCommand(self, clientObj: Client, command: str):
        # Extracting commands
        command = command.strip().split(" ")
        match command[0].lower():
            case "get":
                data = clientObj.getData(command[-1])
                return (
                    "VALUE "
                    + command[-1]
                    + " "
                    + str(len(data))
                    + " \r\n"
                    + data
                    + "\r\nEND\r\n"
                )
            case "set":
                clientObj.addData(command[-2], command[-1])
                return "STORED\r\n"

    def handle_client(self, client_socket, addr):
        buffer = ""
        while True:
            data = client_socket.recv(1024).decode("utf-8")
            print("Recieved from address", addr, " Data:", data)
            if data == "/":
                break
            if data == ",":
                print(buffer, self.clients)
                answer = self.parseCommand(self.clients[addr[1]], buffer)
                client_socket.send(bytes(answer, "utf-8"))
                buffer = ""

            else:
                buffer += str(data)
        client_socket.close()

    # Starts a server loop and listens for
    def startserver(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        while True:
            client_socket, addr = server_socket.accept()
            if addr not in self.clients:
                self.clients[addr[1]] = Client(addr)
            client_handler = threading.Thread(
                target=self.handle_client, args=(client_socket, addr)
            )
            client_handler.start()


server = Server(host="127.0.0.1", port=9889)

server.startserver()
