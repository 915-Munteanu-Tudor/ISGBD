import socket
from SqlParser import SqlParser
from persistancy.GlobalRepository import GlobalRepository


class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.parser = SqlParser()

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
        try:
            text = command
            command = self.parser.cleanup_command(command)
            if command[0] == "USE" and command[1] == "DATABASE":
                return self.parser.parse_use_database(command)
            if command[0] == "CREATE" and command[1] == "DATABASE":
                return self.parser.parse_create_database(command)
            if command[0] == "DROP" and command[1] == "DATABASE":
                return self.parser.parse_drop_database(command)
            if command[0] == "CREATE" and command[1] == "TABLE":
                return self.parser.parse_create_table(text)
            if command[0] == "DROP" and command[1] == "TABLE":
                return self.parser.parse_drop_table(command)

            return "Wrong command."
        except Exception as e:
            return e


server = Server('localhost', 8081)
server.start()
