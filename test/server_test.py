import socket
import threading
import time

# Server IP and port
SERVER_IP = '127.0.0.1'
SERVER_PORT = 6379

# Number of clients to simulate
NUM_CLIENTS = 10000

# Message to send to the server
MESSAGE = b'*1\r\n$4\r\nPING\r\n'

# List to hold client sockets
clients = []

def client_thread(client_id):
    try:
        # Create a new socket for the client
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((SERVER_IP, SERVER_PORT))
        clients.append(client_sock)
        print(f"Client {client_id} connected")

        # Send a message to the server
        client_sock.sendall(MESSAGE)

        # Receive a response from the server (optional)
        response = client_sock.recv(1024)
        print(f"Client {client_id} received: {response.decode()}")

        # Keep the connection open
        time.sleep(10)  # Simulate a longer connection duration

    except Exception as e:
        print(f"Client {client_id} encountered an error: {e}")
    finally:
        client_sock.close()
        print(f"Client {client_id} disconnected")

def main():
    threads = []

    for i in range(NUM_CLIENTS):
        thread = threading.Thread(target=client_thread, args=(i,))
        thread.start()
        threads.append(thread)

        # To avoid overwhelming the server, you can add a small delay between starting clients
        time.sleep(0.001)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
