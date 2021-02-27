import socket
import sys
import threading
import os

process_port = {
    "1": 5001, 
    "2": 5002,
    "3": 5003,
    "4": 5004,
    "5": 5005
}

other_clients = ["1","2","3","4","5"]
other_clients.remove(sys.argv[1])

PROCESS_ID = sys.argv[1]

SERVER_PORT = process_port[sys.argv[1]]

sock_server_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_server_listen.bind((socket.gethostname(), process_port[sys.argv[1]]))
sock_server_listen.listen(32)

sock_server_server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server_server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server_server3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server_server4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

def do_exit(sock_server_listen, sock_server_server1, sock_server_server2, sock_server_server3, sock_server_server4):
    sock_server_listen.close()
    sock_server_server1.close()
    sock_server_server2.close()
    sock_server_server3.close()
    sock_server_server4.close()
    os._exit(0)

def handle_inputs(): 
    while True: 
        try: 
            line = input()
            line_split = line.split(" ")
            if (line == 'connect'):
                print("connecting to other servers")
                sock_server_server1.connect((socket.gethostname(), process_port[other_clients[0]]))
                sock_server_server2.connect((socket.gethostname(), process_port[other_clients[1]]))
                sock_server_server3.connect((socket.gethostname(), process_port[other_clients[2]]))
                sock_server_server4.connect((socket.gethostname(), process_port[other_clients[3]]))
                print("connected to other servers")
            elif (line == 'write'):
                print("writing to other servers")
                sock_server_server1.sendall(b'test')
                sock_server_server2.sendall(b'test')
                sock_server_server3.sendall(b'test')
                sock_server_server4.sendall(b'test')
            elif (line == 'exit'):
                do_exit(sock_server_listen, sock_server_server1, sock_server_server2, sock_server_server3, sock_server_server4)
        except EOFError:
            pass

threading.Thread(target=handle_inputs, args=()).start()

# handle recvs
def handle_recvs(stream, addr):
    while True:
        try:
            word = stream.recv(1024).decode()
            print(word)
            # if a client is making a request
        except socket.error as e:
            stream.close()
            break

while True: 
    # server listening for msgs
    try: 
        stream, addr = sock_server_listen.accept()
        threading.Thread(target=handle_recvs, args=(stream, addr)).start()
    except KeyboardInterrupt:
        do_exit(sock_server_listen, sock_server_server1, sock_server_server2, sock_server_server3, sock_server_server4)