import socket


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_command(self, command):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.host, self.port))

        client_socket.send(command.encode("utf-8"))

        response = client_socket.recv(1024).decode("utf-8")
        client_socket.close()

        return response


client = Client("localhost", 8081)
while True:
    command = input("Enter SQL command: ")
    response = client.send_command(command)
    print("Server says:")
    print(response)
