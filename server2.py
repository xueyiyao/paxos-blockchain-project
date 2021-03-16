import socket
import sys
import threading
import os
import re
import ast
import hashlib
import pickle as p
import time
from queue import Queue
from blockchain import Blockchain, Block, Operation

global PORTS, SERVER_NUMS
global SERVER_ID, SERVER_PORT
global LISTEN_SOCK, CONNECTION_SOCKS
global SEQ_NUM, DEPTH, LATEST_BALLOT, LEADER_HINT
global MUTEX

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

SERVER_LINKS = {}

MUTEX = threading.Lock()
REJECT_MUTEX = threading.Lock()

def do_exit():
    global LISTEN_SOCK, CONNECTION_SOCKS, SERVER_NUMS
    LISTEN_SOCK.close()
    for num in SERVER_NUMS:
        CONNECTION_SOCKS[num].close()
    for client in CLIENT_SOCKETS:
        CLIENT_SOCKETS[client].close()
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
                    SERVER_LINKS[SERVER_NUMS[i]] = True
                print("connected to other servers")
            elif (line_split[0] == 'load'):
                BLOCKCHAIN.load(SERVER_ID)
                print(BLOCKCHAIN)
            elif (line_split[0] == 'printBlockchain'):
                print(BLOCKCHAIN)
            elif (line_split[0] == 'printKVStore'):
                print("STORE:", KEY_VALUE_STORE)
            elif (line_split[0] == 'printQueue'):
                print(list(QUEUE.queue))
            elif ('failLink' in line):
                # failLink(src,dest)
                links = re.search("failLink\((.*)\)", line).group(1).split(",")
                if int(links[0]) == SERVER_ID and int(links[1]) != SERVER_ID:
                    failLink(links[0], links[1])
                else:
                    print("link is incorrect")
            elif ('fixLink' in line): 
                links = re.search("fixLink\((.*)\)", line).group(1).split(",")
                if int(links[0]) == SERVER_ID and int(links[1]) != SERVER_ID:
                    fixLink(links[0], links[1])
                else:
                    print("link is incorrect")
            elif ('failProcess' in line):
                # notify all other servers that process is failed
                print("failing process {}".format(SERVER_ID))
                message = p.dumps(("failProcess", SERVER_ID))
                client_message = 'failProcess{}'.format(SERVER_ID)
                for num in SERVER_NUMS:
                    CONNECTION_SOCKS[num].sendall(message)
                for client in CLIENT_SOCKETS:
                    CLIENT_SOCKETS[client].sendall(client_message.encode())
                do_exit()
            elif ('reconnect' in line):
                # send message to other servers to reconnect 
                print("connecting to other servers")
                for num in SERVER_NUMS:
                    CONNECTION_SOCKS[num].connect((socket.gethostname(), PORTS[num]))
                    message = p.dumps(("reconnect", SERVER_ID))
                    CONNECTION_SOCKS[num].sendall(message)
                    SERVER_LINKS[num] = True
                print("connected to other servers")
            elif (line_split[0] == 'state'):
                print(BALLOT_BV)
                print(CLIENT_STREAM)
            elif (line == 'e'):
                do_exit()
        except EOFError:
            pass

def failLink(src, dest):
    print("in failLink")
    SERVER_LINKS[int(dest)] = False
    message = ('failLink', SERVER_ID)
    CONNECTION_SOCKS[int(dest)].sendall(p.dumps(message))

def fixLink(src, dest):
    print("in fixLink")
    SERVER_LINKS[int(dest)] = True
    message = ('fixLink', SERVER_ID)
    CONNECTION_SOCKS[int(dest)].sendall(p.dumps(message))

#####PAXOS#####
global BALLOT_COUNTS, ACCEPT_NUM, ACCEPT_BLOCK, REJECT_COUNTS, IN_PAXOS
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

REJECT_COUNTS = {}

IN_PAXOS = False

###PHASE 1###
def prepare():
    global CONNECTION_SOCKS, BALLOT_NUM
    print("In Prepare")
    BALLOT_NUM = (BALLOT_NUM[0], BALLOT_NUM[1]+1, SERVER_ID)
    message = p.dumps(("Prepare", BALLOT_NUM))
    time.sleep(3.0)
    for num in SERVER_NUMS:
        if SERVER_LINKS[num] == True:
            CONNECTION_SOCKS[num].sendall(message)

def promise(bal):
    global BALLOT_NUM, CONNECTION_SOCKS
    print("In Promise")
    # if bal[2] == SERVER_ID:
    if bal > BALLOT_NUM:
        BALLOT_NUM = bal
        message = p.dumps(("Promise", bal, ACCEPT_NUM, ACCEPT_BLOCK))
        server_id = bal[2]
        time.sleep(3.0)
        if server_id in SERVER_LINKS and SERVER_LINKS[server_id] == True:
            CONNECTION_SOCKS[server_id].sendall(message)
    elif bal < BALLOT_NUM: 
        print("Rejecting {} in Promise".format(bal))
        message = p.dumps(("Reject", bal))
        CONNECTION_SOCKS[bal[2]].sendall(message)
    else: 
        print("bal == BALLOT_NUM in promise()")

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

    message = p.dumps(("Accept", bal, myVal, client))
    time.sleep(3.0)
    for num in SERVER_NUMS:
        if SERVER_LINKS[num] == True:
            CONNECTION_SOCKS[num].sendall(message)

def accepted(b, v, client):
    global CONNECTION_SOCKS
    print("In Accepted")
    message = p.dumps(("Accepted", b, v, client, SERVER_ID))
    time.sleep(3.0)
    for num in SERVER_NUMS:
        if SERVER_LINKS[num] == True:
            print("Sending Accepted to {}".format(str(num)))
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

# handle recvs
def handle_recvs(stream, addr):
    global BALLOT_COUNTS, SERVER_ID, ACCEPTED_COUNTS, LEADER_HINT, CLIENT_SOCKETS, CLIENT_STREAM, BALLOT_NUM, MUTEX, REJECT_MUTEX, IN_PAXOS
    while True:
        try:
            data = stream.recv(4096)
            data_tuple = ("", None)
            # check for empty | will EOFError if this block not present
            if data != b'':
                data_tuple = p.loads(data)
                print("R:", data_tuple)

            if data_tuple[0] == "Prepare":
                bal = data_tuple[1]
                promise(bal)
            elif data_tuple[0] == "Promise":
                bal = data_tuple[1]
                b = data_tuple[2]
                v = data_tuple[3]
                MUTEX.acquire()
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
                    time.sleep(1)
                    del BALLOT_COUNTS[bal]
                    del BALLOT_BV[bal]
                else:
                    BALLOT_COUNTS[bal] = BALLOT_COUNTS[bal] + 1
                MUTEX.release()
            elif data_tuple[0] == "Accept":
                b = data_tuple[1]
                v = data_tuple[2]
                client = data_tuple[3]
                # CLIENT_STREAM[b] = CLIENT_SOCKETS[client]
                # set to leader to proposer's id
                if b >= BALLOT_NUM:
                    LEADER_HINT = b[2]
                    print("ACCEPT: ", b, BALLOT_NUM)
                    ACCEPT_NUM = b
                    ACCEPT_BLOCK = v
                    accepted(b, v, client)
            elif data_tuple[0] == "Accepted":
                b = data_tuple[1]
                v = data_tuple[2]
                client = data_tuple[3]
                MUTEX.acquire()
                if b not in ACCEPTED_COUNTS:
                    ACCEPTED_COUNTS[b] = 2
                    print(ACCEPTED_COUNTS[b])
                    CLIENT_STREAM[b] = CLIENT_SOCKETS[client]
                elif ACCEPTED_COUNTS[b] == 2:
                    print("MAJORITY ACCEPTED")
                    ACCEPTED_COUNTS[b] = ACCEPTED_COUNTS[b] + 1
                    print(ACCEPTED_COUNTS[b])
                    # append to blockchain
                    BLOCKCHAIN.append_block(v)
                    # increment depth
                    BALLOT_NUM = (BALLOT_NUM[0]+1, BALLOT_NUM[1], BALLOT_NUM[2])
                    # add to dict
                    res = dict_exec(v)
                    # save to file
                    BLOCKCHAIN.save(SERVER_ID)
                    # send decision to client
                    decision = "{},{}".format(res, LEADER_HINT)
                    time.sleep(3.0)
                    CLIENT_STREAM[b].sendall(decision.encode())
                    # time.sleep(1)
                    del CLIENT_STREAM[b]
                    IN_PAXOS = False
                elif ACCEPTED_COUNTS[b] == 4:
                    print("ALL ACCEPTED IN PROPOSER")
                    del ACCEPTED_COUNTS[b]
                elif ACCEPTED_COUNTS[b] == 3 and LEADER_HINT != SERVER_ID:
                    print("ALL ACCEPTED IN ACCEPTOR")
                    del ACCEPTED_COUNTS[b]
                else:
                    ACCEPTED_COUNTS[b] = ACCEPTED_COUNTS[b] + 1
                MUTEX.release()
            elif data_tuple[0] == "Operation":
                opArr = re.search("Operation\((.*)\)", data_tuple[1]).group(1).split(',')
                op = Operation(opArr[0], opArr[1], opArr[2]) if opArr[0] == "put" else Operation(opArr[0], opArr[1])
                
                if LEADER_HINT == 0:
                    print("No Leader elected, call prepare()")
                    QUEUE.put((op, data_tuple[2]))
                    prepare()
                elif LEADER_HINT == SERVER_ID:
                    print("I am the leader")
                    QUEUE.put((op, data_tuple[2]))
                    while IN_PAXOS:
                        pass
                    IN_PAXOS = True
                    accept(BALLOT_NUM, None)
                elif LEADER_HINT != SERVER_ID:
                    # forward to correct leader
                    if SERVER_LINKS[LEADER_HINT] == True:
                        print("Not the leader, sending to correct leader")
                        time.sleep(3.0)
                        CONNECTION_SOCKS[LEADER_HINT].sendall(p.dumps(data_tuple))
                    else: 
                        print("Connection with leader broken, reelecting leader")
                        QUEUE.put((op, data_tuple[2]))
                        prepare()
            elif data_tuple[0] == "Reject":
                b = data_tuple[1]
                REJECT_MUTEX.acquire()
                if b not in REJECT_COUNTS:
                    REJECT_COUNTS[b] = 1
                    print(REJECT_COUNTS[b])
                elif REJECT_COUNTS[b] == 1:
                    REJECT_COUNTS[b] = REJECT_COUNTS[b] + 1
                    print(REJECT_COUNTS[b])
                elif REJECT_COUNTS[b] == 2:
                    # third Reject received
                    print("RECEIVED MAJORITY REJECTS, POPPED OP FROM QUEUE")
                    REJECT_COUNTS[b] = REJECT_COUNTS[b] + 1
                    QUEUE.get()
                    IN_PAXOS = False
                elif REJECT_COUNTS[b] == 3 and LEADER_HINT != SERVER_ID:
                    print("RECEIVED ALL REJECTS, POPPED OP FROM QUEUE")
                    del REJECT_COUNTS[b]
                REJECT_MUTEX.release()
            elif data_tuple[0] == "client":
                CLIENT_SOCKETS[data_tuple[1]] = stream
            elif 'failLink' == data_tuple[0]:
                failed_link = data_tuple[1]
                SERVER_LINKS[failed_link] = False
                print(failed_link, "failed")
            elif 'fixLink' == data_tuple[0]:
                fixed_link = data_tuple[1]
                SERVER_LINKS[fixed_link] = True
                print(fixed_link, "fixed")
            elif 'failProcess' == data_tuple[0]:
                SERVER_LINKS[data_tuple[1]] = False
                CONNECTION_SOCKS[data_tuple[1]].close()
                del CONNECTION_SOCKS[data_tuple[1]]
                SERVER_NUMS.remove(data_tuple[1])
            elif 'reconnect' == data_tuple[0]:
                SERVER_LINKS[data_tuple[1]] = True
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                CONNECTION_SOCKS[data_tuple[1]] = sock
                CONNECTION_SOCKS[data_tuple[1]].connect((socket.gethostname(), PORTS[data_tuple[1]]))
                SERVER_NUMS.append(data_tuple[1])
 
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

    

