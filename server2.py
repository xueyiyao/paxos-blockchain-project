import socket
import sys
import threading
import os
import re
import ast
import hashlib
import pickle as p
from queue import Queue
from blockchain import Blockchain, Block, Operation

global PORTS, SERVER_NUMS
global SERVER_ID, SERVER_PORT
global LISTEN_SOCK, CONNECTION_SOCKS
global SEQ_NUM, DEPTH, LATEST_BALLOT, LEADER_HINT

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
BALLOT_NUM = None
SEQ_NUM = None
DEPTH = None
LATEST_BALLOT = None

LEADER_HINT = None

CLIENT_STREAM = {}
CLIENT_SOCKETS = {}

def do_exit():
    global LISTEN_SOCK, CONNECTION_SOCKS, SERVER_NUMS
    LISTEN_SOCK.close()
    for num in SERVER_NUMS:
        CONNECTION_SOCKS[num].close()
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
            elif (line_split[0] == 'p'):
                print(BLOCKCHAIN)
                print("STORE:", KEY_VALUE_STORE)
            elif (line == 'e'):
                do_exit()
        except EOFError:
            pass


#####PAXOS#####
global BALLOT_COUNTS, ACCEPT_NUM, ACCEPT_BLOCK
# Counts the number of Promises received for one ballot number
BALLOT_COUNTS = {}
# Keep tracks of highest b and corresponding v for each bal 
# Received from promise message: ("Promise", bal, b , v)
BALLOT_BV = {}
# Most recently accepted Ballot Number from the accept phase
ACCEPT_NUM = (None, None, None)
# Most recently accepted Block from the accept phase
ACCEPT_BLOCK = None

ACCEPTED_COUNTS = {}

###PHASE 1###
def prepare():
    global CONNECTION_SOCKS, BALLOT_NUM
    print("In Prepare")
    for num in SERVER_NUMS:
        message = p.dumps(("Prepare", BALLOT_NUM))
        CONNECTION_SOCKS[num].sendall(message)

def promise(bal):
    global BALLOT_NUM, CONNECTION_SOCKS
    print("In Promise")
    if bal > BALLOT_NUM:
        BALLOT_NUM = bal
    message = p.dumps(("Promise", bal, ACCEPT_NUM, ACCEPT_BLOCK))
    server_id = bal[2]
    CONNECTION_SOCKS[server_id].sendall(message)

###PHASE 2###
def accept(bal, myVal):
    print("In Accept")

    if myVal is None:
        PREV_BLOCK = BLOCKCHAIN.tail
        prev_hash = "None"
        if PREV_BLOCK is not None:
            str_to_be_hashed = str(PREV_BLOCK.operation) + str(PREV_BLOCK.nonce) + str(PREV_BLOCK.prev_hash)
            prev_hash = str(hashlib.sha256(str_to_be_hashed.encode()).hexdigest())
        operation = QUEUE.get()
        op = operation[0]
        client = operation[1]
        CLIENT_STREAM[bal] = CLIENT_SOCKETS[client]

        # Calculate nonce
        h = "----"
        nonce = 0
        nonce_str = str(op) + str(nonce)
        h = str(hashlib.sha256(nonce_str.encode()).hexdigest())
        while h[-1] != '0' and h[-1] != '1' and h[-1] != '2': 
            nonce += 1
            nonce_str = str(op) + str(nonce)
            h = str(hashlib.sha256(nonce_str.encode()).hexdigest())
        
        myVal = Block(prev_hash=prev_hash, nonce=nonce, op=op)

    for i in range(len(CONNECTION_SOCKS)):
        message = p.dumps(("Accept", bal, myVal, client))
        CONNECTION_SOCKS[SERVER_NUMS[i]].sendall(message)

def accepted(b, v):
    global CONNECTION_SOCKS
    print("In Accepted")
    message = p.dumps(("Accepted", b, v))
    for num in SERVER_NUMS:
        CONNECTION_SOCKS[num].sendall(message)

def dict_exec(block):
    op = block.operation.op
    key = block.operation.key
    if op == "put":
        value = block.operation.value
        KEY_VALUE_STORE[key] = value
        return value
    else:
        if key in KEY_VALUE_STORE:
            return KEY_VALUE_STORE[key]
        else:
            return "DOES NOT EXIST"

        
def str_to_tuple(s):
    arr = re.search("\((.*)\)", s).group(1)
    return eval(arr)

# handle recvs
def handle_recvs(stream, addr):
    global BALLOT_COUNTS, SERVER_ID, ACCEPTED_COUNTS, LEADER_HINT, CLIENT_SOCKETS, CLIENT_STREAM
    while True:
        try:
            data = stream.recv(4096)
            data_tuple = ("", None)
            # check for empty | will EOFError if this block not present
            if data != b'':
                data_tuple = p.loads(data)
                print(data_tuple)

            if data_tuple[0] == "Prepare":
                bal = data_tuple[1]
                promise(bal)
            elif data_tuple[0] == "Promise":
                bal = data_tuple[1]
                b = data_tuple[2]
                v = data_tuple[3]
                if bal not in BALLOT_COUNTS:
                    BALLOT_COUNTS[bal] = 2
                    BALLOT_BV[bal] = ((0,0,0), None)
                    # ther is any v not null, set Ballot_BV to (b, v) with highest b
                    if v is not None:
                        BALLOT_BV[bal] = (b, v)
                elif BALLOT_COUNTS[bal] == 2:
                    BALLOT_COUNTS[bal] = BALLOT_COUNTS[bal] + 1
                    print("GOT MAJORITY")
                    # set leader to self
                    LEADER_HINT = SERVER_ID

                    # check if any v is not null and check if recevied b is higher than current b
                    if v is not None and b > BALLOT_BV[bal][0]:
                        BALLOT_BV[bal] = (b, v)

                    # Send accept message
                    accept(bal, BALLOT_BV[bal][1])
                elif BALLOT_COUNTS[bal] == 4:
                    print("GOT ALL")
                    del BALLOT_COUNTS[bal]
                    del BALLOT_BV[bal]
                else:
                    BALLOT_COUNTS[bal] = BALLOT_COUNTS[bal] + 1
            elif data_tuple[0] == "Accept":
                b = data_tuple[1]
                v = data_tuple[2]
                client = data_tuple[3]
                CLIENT_STREAM[b] = CLIENT_SOCKETS[client]
                # set to leader to proposer's id
                LEADER_HINT = b[2]
                if b > BALLOT_NUM:
                    ACCEPT_NUM = b
                    ACCEPT_BLOCK = v
                accepted(b, v)
            elif data_tuple[0] == "Accepted":
                b = data_tuple[1]
                v = data_tuple[2]
                if b not in ACCEPTED_COUNTS:
                    ACCEPTED_COUNTS[b] = 2
                elif ACCEPTED_COUNTS[b] == 2:
                    print("MAJORITY ACCEPTED")
                    ACCEPTED_COUNTS[b] = ACCEPTED_COUNTS[b] + 1
                    # append to blockchain
                    BLOCKCHAIN.append_block(v)
                    # add to dict
                    res = dict_exec(v)
                    # save to file
                    BLOCKCHAIN.save(SERVER_ID)
                    # send decision to client
                    CLIENT_STREAM[b].sendall(str(res).encode())
                    del CLIENT_STREAM[b]
                elif ACCEPTED_COUNTS[b] == 4:
                    print("ALL ACCEPTED IN PROPOSER")
                    del ACCEPTED_COUNTS[b]
                elif ACCEPTED_COUNTS[b] == 3 and LEADER_HINT != SERVER_ID:
                    print("ALL ACCEPTED IN ACCEPTOR")
                    del ACCEPTED_COUNTS[b]
                else:
                    ACCEPTED_COUNTS[b] = ACCEPTED_COUNTS[b] + 1
            elif data_tuple[0] == "Operation":
                # CLIENT_STREAM = stream
                stream.sendall("received in server {}".format(SERVER_ID).encode())
                opArr = re.search("Operation\((.*)\)", data_tuple[1]).group(1).split(',')
                op = Operation(opArr[0], opArr[1], opArr[2]) if opArr[0] == "put" else Operation(opArr[0], opArr[1])
                
                QUEUE.put((op, data_tuple[2]))
                prepare()
            elif data_tuple[0] == "client":
                CLIENT_SOCKETS[data_tuple[1]] = stream
                
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
            stream.sendall(str(SERVER_ID).encode())
            threading.Thread(target=handle_recvs, args=(stream, addr)).start()
        except KeyboardInterrupt:
            do_exit()

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

    # SEQ_NUM = 0
    # DEPTH = 0
    BALLOT_NUM = (0, 0, SERVER_ID)
    # LATEST_BALLOT = (0, 0, 0)

    LEADER_HINT = 0

    threading.Thread(target=listen).start()

    handle_inputs()

    

