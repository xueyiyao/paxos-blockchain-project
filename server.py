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
                do_exit(LISTEN_SOCK, CONNECTION_SOCKS[SERVER_NUMS[0]], CONNECTION_SOCKS[SERVER_NUMS[1]],\
                    CONNECTION_SOCKS[SERVER_NUMS[2]], CONNECTION_SOCKS[SERVER_NUMS[3]])
        except EOFError:
            pass


#####PAXOS#####
global PROMISE_COUNTS, PROMISE_SOCKETS, ACCEPT_NUM, ACCEPT_BLOCK
PROMISE_COUNTS = {}
PROMISE_SOCKETS = {}
ACCEPT_NUM = (None, None, None)
ACCEPT_BLOCK = None

global ONE_V, HIGHEST_BV
ONE_V = False
HIGHEST_BV = None


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
    temp_str = "Promise ({},{},{}) ({},{},{}) {} {}".format( \
        depth, seq_num, server_id, ACCEPT_NUM[0], ACCEPT_NUM[1], ACCEPT_NUM[2], ACCEPT_BLOCK, SERVER_ID)
    CONNECTION_SOCKS[server_id].sendall(temp_str.encode())

###PHASE 2###
def accept(bal, myVal, socks):
    print("In Accept")
    for server_id in socks:
        CONNECTION_SOCKS[server_id].sendall("Accept {} {}".format(bal, myVal).encode())
        
def str_to_tuple(s):
    arr = re.search("\((.*)\)", s).group(1)
    return eval(arr)

# handle recvs
def handle_recvs(stream, addr):
    global PROMISE_COUNTS, SERVER_ID
    global ONE_V, HIGHEST_BV
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
                print(word)
                bal = str_to_tuple(word_split[1]) if word_split[1] != "None" else None
                b = str_to_tuple(word_split[2]) if word_split[2] != "None" else None
                v = str_to_tuple(word_split[3]) if word_split[3] != "None" else None
                sid = int(word_split[4])
                if bal not in PROMISE_COUNTS:
                    PROMISE_COUNTS[bal] = 2
                    PROMISE_SOCKETS[bal] = []
                    PROMISE_SOCKETS[bal].append(sid)
                    if v is not None:
                        ONE_V = True
                        # set HIGHEST_BV
                        if HIGHEST_BV is None or HIGHEST_BV[0] < b:
                            HIGHEST_BV = (b, v)
                elif PROMISE_COUNTS[bal] == 2:
                    PROMISE_COUNTS[bal] = PROMISE_COUNTS[bal] + 1
                    PROMISE_SOCKETS[bal].append(sid)
                    print("GOT MAJORITY")
                    print(bal)
                    # check if any v is not null
                    if v is not None:
                        ONE_V = True
                        # set HIGHEST_BV
                        if HIGHEST_BV is None or HIGHEST_BV[0] < b:
                            HIGHEST_BV = (b, v)
                    
                    # Send accept message
                    if HIGHEST_BV is not None:
                        accept(bal, HIGHEST_BV[1], PROMISE_SOCKETS[bal])
                    else:
                        accept(bal, None, PROMISE_SOCKETS[bal])
                    ONE_V = False
                    HIGHEST_BV = None
                elif PROMISE_COUNTS[bal] == 4:
                    print("GOT ALL")
                    del PROMISE_COUNTS[bal]
                    del PROMISE_SOCKETS[bal]
                else:
                    PROMISE_COUNTS[bal] = PROMISE_COUNTS[bal] + 1
            elif word_split[0] == "Accept":
                print(word)
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

    

