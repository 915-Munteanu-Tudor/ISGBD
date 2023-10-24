import socket

from persistancy.GlobalRepository import GlobalRepository

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.usedDB = None

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(1)

        print(f"Server started on {self.host}:{self.port}")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connection from {client_address}")

            data = client_socket.recv(1024).decode('utf-8')
            response = self.execute_command(data)

            client_socket.send(response.encode('utf-8'))
            client_socket.close()

    def execute_command(self, command):
        text = command.lower().split()
        if command.startswith("use database") and len(text) == 3:
            if self.is_existent_db(text[2]):
                self.usedDB = text[2]
                return "The used database is {}.".format(self.usedDB)
            return "The database you want to use does not exist."
        if command.startswith("create database") and len(text) == 3:
            return "Database created successfully."
        if command.startswith("drop database") and len(text) == 3:
            if self.is_existent_db(text[2]):
                return "Database dropped successfully."
            return "The database you want to drop does not exist."

        return "Wrong command."

    @staticmethod
    def is_existent_db(dbName):
        return True


server = Server('localhost', 8083)
server.start()
