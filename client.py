import socket
import sys
import threading
import os

# client listen 
# sock_client_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# sock_client_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# sock_client_listen.bind((socket.gethostname(), process_port[sys.argv[1]]))
# sock_client_listen.listen(32)

sock_client_server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_client_server1.connect((socket.gethostname(), 5001))

sock_client_server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_client_server2.connect((socket.gethostname(), 5002))

sock_client_server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_client_server3.connect((socket.gethostname(), 5003))

sock_client_server4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_client_server4.connect((socket.gethostname(), 5004))

sock_client_server5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_client_server5.connect((socket.gethostname(), 5005))

# exit function to close all sockets
def do_exit(sock_client_server1, sock_client_server2, sock_client_server3, sock_client_server4, sock_client_server5):
    sock_client_server1.close()
    sock_client_server2.close()
    sock_client_server3.close()
    sock_client_server4.close()
    sock_client_server5.close()
    os._exit(0)

# handle inputs
def handle_inputs(): 
    while True: 
        try: 
            line = input()
            line_split = line.split(" ")
            if (line == 'write'):
                print("writing to other servers")
                sock_client_server1.sendall(b'test')
                sock_client_server2.sendall(b'test')
                sock_client_server3.sendall(b'test')
                sock_client_server4.sendall(b'test')
                sock_client_server5.sendall(b'test')
            elif (line == 'exit'):
                do_exit(sock_server_server1, sock_server_server2, sock_server_server3, sock_server_server4, sock_server_server5)
        except EOFError:
            pass

threading.Thread(target=handle_inputs, args=()).start()


while True: 
    # server listening for msgs
    try: 
        word1 = sock_client_server1.recv(1024)
        word2 = sock_client_server2.recv(1024)
        word3 = sock_client_server3.recv(1024)
        word4 = sock_client_server4.recv(1024)
        word5 = sock_client_server5.recv(1024)
        print(word1, word2, word3, word4, word5)
    except KeyboardInterrupt:
        do_exit(sock_server_server1, sock_server_server2, sock_server_server3, sock_server_server4, sock_server_server5)