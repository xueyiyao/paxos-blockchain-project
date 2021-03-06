import socket
import sys
import threading
import os
import re
from queue import Queue
from blockchain import Blockchain

global PORTS, SERVER_NUMS
global SERVER_ID, SERVER_PORT
global LISTEN_SOCK, CONNECTION_SOCKS
global SEQ_NUM, DEPTH, LATEST_BALLOT

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
CONNECTION_SOCKS = {}

# Data Structures
BLOCKCHAIN = None
QUEUE = Queue()
KEY_VALUE_STORE = {}

# Ballot Number Tuple
SEQ_NUM = None
DEPTH = None
LATEST_BALLOT = None

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
            if (line == 'c'):
                print("connecting to other servers")
                for i in range(len(SERVER_NUMS)):
                    CONNECTION_SOCKS[SERVER_NUMS[i]].connect((socket.gethostname(), PORTS[SERVER_NUMS[i]]))
                print("connected to other servers")
            elif (line == 'w'):
                print("writing to other servers")
                for i in range(len(SERVER_NUMS)):
                    CONNECTION_SOCKS[SERVER_NUMS[i]].sendall('test'.encode())
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
            elif (line == 'e'):
                do_exit(LISTEN_SOCK, CONNECTION_SOCKS[0], CONNECTION_SOCKS[1], CONNECTION_SOCKS[2], CONNECTION_SOCKS[3])
        except EOFError:
            pass


#####PAXOS#####
global PREP_COUNT
PREP_COUNT = 0

###PHASE 1###
def prepare_helper(i):
    global PREP_COUNT
    CONNECTION_SOCKS[i].sendall("Prepare ({},{},{})".format(SEQ_NUM, SERVER_ID, DEPTH).encode())
    message = CONNECTION_SOCKS[i].recv(1024).decode()
    print(message)
    # PREP_COUNT += 1
    # if PREP_COUNT == 4:
    #     PREP_COUNT = 0

def prepare():
    global CONNECTION_SOCKS, PREP_COUNT, SEQ_NUM, SERVER_ID, DEPTH
    threads = []
    print("Preparing")
    LATEST_BALLOT = (DEPTH ,SEQ_NUM, SERVER_ID)
    for i in range(len(CONNECTION_SOCKS)):
        thread = threading.Thread(target=prepare_helper, args=[SERVER_NUMS[i]])
        threads.append(thread)
        threads[i].start()

    for thread in threads:
        thread.join()

def promise(seq_num, server_id, depth):
    global LATEST_BALLOT, CONNECTION_SOCKS, SERVER_NUMS
    print("In Promise")
    if (depth, seq_num, server_id) > LATEST_BALLOT:
        LATEST_BALLOT = (depth, seq_num, server_id)
    temp_str = "Promise ({},{},{}) {}".format(LATEST_BALLOT[0], LATEST_BALLOT[1], LATEST_BALLOT[2], "STUB")
    CONNECTION_SOCKS[SERVER_NUMS[server_id]].sendall(temp_str.encode())
        
# handle recvs
def handle_recvs(stream, addr):
    while True:
        try:
            word = stream.recv(1024).decode()
            word_split = word.split(" ")
            if word_split[0] == "Prepare":
                print(word)
                ballot = re.search("\((.*)\)", word_split[1]).group(1)
                print(ballot)
                ballot = ballot.split(',')
                promise(int(ballot[0]), int(ballot[1]), int(ballot[2]))
            elif word_split[0] == "Operation":
                stream.sendall("received in server {}".format(SERVER_ID).encode())
                prepare()
        except socket.error as e:
            stream.close()
            break

# listen
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
    for i in range(len(SERVER_NUMS)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        CONNECTION_SOCKS[SERVER_NUMS[i]] = sock

    SEQ_NUM = 0
    DEPTH = 0
    LATEST_BALLOT = (0, 0, 0)

    threading.Thread(target=listen).start()

    handle_inputs()

    

