import socket
import sys
import threading
import os
from queue import Queue
from blockchain import Blockchain

global PORTS, SERVER_NUMS
global SERVER_ID, SERVER_PORT
global LISTEN_SOCK, CONNECTION_SOCKS

PORTS = {
    1: 5001, 
    2: 5002,
    3: 5003,
    4: 5004,
    5: 5005
}

SERVER_NUMS = [1,2,3,4,5]

SERVER_ID = None
SERVER_PORT = None

LISTEN_SOCK = None
CONNECTION_SOCKS = []

# Data Structures
BLOCKCHAIN = None
QUEUE = Queue()
KEY_VALUE_STORE = {}

def do_exit(LISTEN_SOCKET, sock_server_server1, sock_server_server2, sock_server_server3, sock_server_server4):
    LISTEN_SOCKET.close()
    sock_server_server1.close()
    sock_server_server2.close()
    sock_server_server3.close()
    sock_server_server4.close()
    os._exit(0)

# handle inputs
def handle_inputs(): 
    global CONNECTION_SOCKS, PORTS, SERVER_NUMS
    while True: 
        try: 
            line = input()
            line_split = line.split(" ")
            if (line == 'connect'):
                print("connecting to other servers")
                for i in range(4):
                    CONNECTION_SOCKS[i].connect((socket.gethostname(), PORTS[SERVER_NUMS[i]]))
                print("connected to other servers")
            elif (line == 'write'):
                print("writing to other servers")
                for i in range(4):
                    CONNECTION_SOCKS[i].sendall(b'test')
            elif (line_split[0] == 'blockchain'):
                op_op = line_split[1]
                op_key = line_split[2]
                op_value = line_split[3] if (len(line_split) > 3) else None
                BLOCKCHAIN.append(op_op, op_key, op_value, 'nonce_stub')
                BLOCKCHAIN.save(SERVER_ID)
                print(BLOCKCHAIN)
            elif (line_split[0] == 'load'):
                BLOCKCHAIN.load(SERVER_ID)
                print(BLOCKCHAIN)
            elif (line == 'exit'):
                do_exit(LISTEN_SOCK, CONNECTION_SOCKS[0], CONNECTION_SOCKS[1], CONNECTION_SOCKS[2], CONNECTION_SOCKS[3])
        except EOFError:
            pass

# handle recvs
def handle_recvs(stream, addr):
    while True:
        try:
            word = stream.recv(1024).decode()
            print(word)
            stream.sendall(b'received')
        except socket.error as e:
            stream.close()
            break

def listen():
    global LISTEN_SOCK
    LISTEN_SOCK.listen(32)

    while True: 
        # server listening for msgs
        try: 
            stream, addr = LISTEN_SOCK.accept()
            threading.Thread(target=handle_recvs, args=(stream, addr)).start()
        except KeyboardInterrupt:
            do_exit(LISTEN_SOCK, CONNECTION_SOCKS[0], CONNECTION_SOCKS[1], CONNECTION_SOCKS[2], CONNECTION_SOCKS[3])

if __name__ == '__main__':
    SERVER_ID = int(sys.argv[1])
    SERVER_PORT = PORTS[SERVER_ID]
    # Remove Itself From Server Array
    SERVER_NUMS.remove(SERVER_ID)

    BLOCKCHAIN = Blockchain()

    LISTEN_SOCK = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    LISTEN_SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    LISTEN_SOCK.bind((socket.gethostname(), SERVER_PORT))

    # connections to each of the other servers
    for i in range(4):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        CONNECTION_SOCKS.append(sock)

    threading.Thread(target=listen).start()

    handle_inputs()

    

