import socket
from encryption import encrypt_message

def send_via_socket(config, data_iter):
    host = config['sockets']['host']
    port = config['sockets']['port']
    for data in data_iter:
        msg = encrypt_message(config, data)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(msg)