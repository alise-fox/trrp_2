import socket
from encryption import decrypt_message
from db_postgres import insert_normalized

def receive_via_socket(config):
    host = config['sockets']['host']
    port = config['sockets']['port']
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print("Waiting for connection...")
        while True:
            conn, addr = s.accept()
            with conn:
                data = b""
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                row = decrypt_message(config, data)
                insert_normalized(config, row)