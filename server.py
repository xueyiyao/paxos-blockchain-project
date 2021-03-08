import socket
import sys
import threading
import os
import re
import hashlib
from queue import Queue
from blockchain import Blockchain
from ast import literal_eval as make_tuple

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
                do_exit(LISTEN_SOCK, CONNECTION_SOCKS[SERVER_NUMS[0]], CONNECTION_SOCKS[SERVER_NUMS[1]],\
                    CONNECTION_SOCKS[SERVER_NUMS[2]], CONNECTION_SOCKS[SERVER_NUMS[3]])
        except EOFError:
            pass


#####PAXOS#####
global PROMISE_COUNTS, ACCEPT_NUM, ACCEPT_BLOCK, MY_VAL, HIGHEST_PROMISE_NUM
# Stores the value to send after receiving Promises, type Operation
MY_VAL = None
# Counts the number of Promises received for one ballot number
PROMISE_COUNTS = {}
# Keeps track of the highest acceptNum received from a promise
HIGHEST_PROMISE_NUM = {}
# Most recently accepted Ballot Number from the accept phase
ACCEPT_NUM = (None, None, None)
# Most recently accepted Block from the accept phase
ACCEPT_BLOCK = None
###PHASE 1###
def prepare():
    global CONNECTION_SOCKS, SEQ_NUM, SERVER_ID, DEPTH
    threads = []
    print("Preparing")
    LATEST_BALLOT = (DEPTH, SEQ_NUM, SERVER_ID)
    for i in range(len(CONNECTION_SOCKS)):
        CONNECTION_SOCKS[SERVER_NUMS[i]].sendall("Prepare ({},{},{})".format(DEPTH, SEQ_NUM, SERVER_ID).encode())

def promise(depth, seq_num, server_id):
    global LATEST_BALLOT, CONNECTION_SOCKS, SERVER_NUMS
    print("In Promise")
    if (depth, seq_num, server_id) > LATEST_BALLOT:
        LATEST_BALLOT = (depth, seq_num, server_id)
    temp_str = "Promise ({},{},{}) ({},{},{}) {}".format( \
        depth, seq_num, server_id, ACCEPT_NUM[0], ACCEPT_NUM[1], ACCEPT_NUM[2], ACCEPT_BLOCK)
    CONNECTION_SOCKS[server_id].sendall(temp_str.encode())

###PHASE 2###
def accept():
    global MY_VAL
    # Create new block with info from MY_VAL, calculate hash and nonce
    PREV_BLOCK = BLOCKCHAIN.tail
    str_to_be_hashed = str(PREV_BLOCK.operation) + str(PREV_BLOCK.nonce) + str(PREV_BLOCK.prev_hash)
    prev_hash = str(hashlib.sha256(str_to_be_hashed.encode()).hexdigest())
    # Calculate nonce
    h = ""
    nonce = 0
    while h[-1] != '0' and h[-1] != '1' and h[-1] != 2: 
        nonce_str = str(MY_VAL)
        h = str(hashlib.sha256()
    NEW_BLOCK = Block(prev_hash, op_op, op_key, op_value, nonce)

        
# handle recvs
def handle_recvs(stream, addr):
    global PROMISE_COUNTS, SERVER_ID, ACCEPT_BLOCK, ACCEPT_NUM, HIGHEST_PROMISE_NUM, MY_VAL
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
            if word_split[0] == "Promise":
                bal = word_split[1]
                print(word)
                print(word_split)
                print(bal)
                if bal not in PROMISE_COUNTS:
                    HIGHEST_PROMISE_NUM[bal] = (0,0,0)
                    if word_split[3] != 'None':
                        HIGHEST_PROMISE_NUM[bal] = make_tuple(word_split[2])
                        # TODO: need to convert word_split[3] to Block depending on how it is stored
                        MY_VAL = word_split[3] 
                    PROMISE_COUNTS[bal] = 2
                elif PROMISE_COUNTS[bal] == 2:
                    if word_split[3] != 'None' and make_tuple(word_split[2]) > HIGHEST_PROMISE_NUM[bal]:
                        HIGHEST_PROMISE_NUM[bal] = make_tuple(word_split[2])
                        # TODO: need to convert word_split[3] to Block depending on how it is stored
                        MY_VAL = word_split[3] 
                    PROMISE_COUNTS[bal] = PROMISE_COUNTS[bal] + 1
                    print("GOT MAJORITY")
                    accept()
                elif PROMISE_COUNTS[bal] == 4:
                    print("GOT ALL")
                    del PROMISE_COUNTS[bal]
                    del HIGHEST_PROMISE_NUM[bal]
                else:
                    PROMISE_COUNTS[bal] = PROMISE_COUNTS[bal] + 1
            elif word_split[0] == "Operation":
                # TODO Should store the value in MY_VAL
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
            do_exit(LISTEN_SOCK, CONNECTION_SOCKS[SERVER_NUMS[0]], CONNECTION_SOCKS[SERVER_NUMS[1]],\
                    CONNECTION_SOCKS[SERVER_NUMS[2]], CONNECTION_SOCKS[SERVER_NUMS[3]])

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

    

