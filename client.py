import socket
import sys
import threading
import os
import random
import time
import pickle as p

global SERVER_PORTS, SERVER_SOCKETS, SERVER_SOCKET, LEADER_HINT, RECEIVED
SERVER_PORTS = {
    1: 5001, 
    2: 5002,
    3: 5003,
    4: 5004,
    5: 5005
}
CLIENT_PORTS = {
    1: 5006, 
    2: 5007
}
CLIENT_ID = 0
SERVER_SOCKETS = []
SERVER_SOCKET = None
LEADER_HINT = None
RECEIVED = False
FAULTY_LEADERS = []

# exit function to close all sockets
# def do_exit(sock_client_server1, sock_client_server2, sock_client_server3, sock_client_server4, sock_client_server5):
#     sock_client_server1.close()
#     sock_client_server2.close()
#     sock_client_server3.close()
#     sock_client_server4.close()
#     sock_client_server5.close()
#     os._exit(0)

def connect_server(server_id):
    global SERVER_SOCKET
    for i in range(1,6):
        SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        SERVER_SOCKET.connect((socket.gethostname(), SERVER_PORTS[i]))
        SERVER_SOCKETS.append(SERVER_SOCKET)

def close_connection():
    global SERVER_SOCKET
    SERVER_SOCKET.close()
    os._exit(0)

def time_out(duration, line): 
    global RECEIVED, LEADER_HINT, FAULTY_LEADERS
    time.sleep(duration)
    if RECEIVED == False: 
        print("not received")
        # random server that is not the failed leader (LEADER_HINT)
        print(FAULTY_LEADERS)
        FAULTY_LEADERS.append(LEADER_HINT)
        if len(FAULTY_LEADERS) >= 3:
            print("majority of leaders faulty")
        else:
            while LEADER_HINT in FAULTY_LEADERS: 
                LEADER_HINT = random.randint(0,4)
            # resend operation
            RECEIVED = False
            print("resending operation")
            SERVER_SOCKET.sendall(p.dumps(("Operation", line, CLIENT_ID)))
            timeout_thread = threading.Thread(target=time_out, args=(5.0,line))
            timeout_thread.start()
            timeout_thread.join()

# handle inputs
def handle_inputs(): 
    global SERVER_SOCKETS, LEADER_HINT, FAULTY_LEADERS
    while True: 
        try: 
            line = input()
            line_split = line.split(" ")
            if (line == 'write'):
                print("writing to other servers")
                for i in range(5):
                    SERVER_SOCKETS[i].sendall(b'test')
            elif (line == 'e'):
                # do_exit(SERVER_SOCKETS[0], SERVER_SOCKETS[1], SERVER_SOCKETS[2], SERVER_SOCKETS[3], SERVER_SOCKETS[4])
                close_connection()
            elif "Operation" in line:
                if LEADER_HINT is None:
                    temp = random.randint(0,4)
                    LEADER_HINT = temp
                    if int(CLIENT_ID) == 1:
                        temp = 0
                    else: 
                        temp = 3
                    # connect_server(temp)
                    print("sending to {}".format(temp))
                    SERVER_SOCKET = SERVER_SOCKETS[temp]
                    RECEIVED = False
                    FAULTY_LEADERS = []
                    SERVER_SOCKET.sendall(p.dumps(("Operation", line, CLIENT_ID)))
                    # thread that sleeps for 5 seconds
                    threading.Thread(target=time_out, args=(5.0,line)).start()
                    # try:
                    #     for i in range(5): 
                    #         print("set timeout for {}".format(i))
                    #         SERVER_SOCKETS[i].settimeout(5.0)
                    # except socket.timeout as error:
                    #     print(error)
                    # message_recv = SERVER_SOCKET.recv(1024).decode()
                    # if message_recv != "": 
                    #     print("message_recv: ", message_recv, flush=True)
                    # close_connection()
                else:
                    #send to server
                    print("sending to {}".format(LEADER_HINT))
                    SERVER_SOCKET = SERVER_SOCKETS[LEADER_HINT]
                    RECEIVED = False
                    FAULTY_LEADERS = []
                    SERVER_SOCKET.sendall(p.dumps(("Operation", line, CLIENT_ID)))
                    threading.Thread(target=time_out, args=(5.0,line)).start()
                    # try:
                    #     for i in range(5): 
                    #         SERVER_SOCKETS[i].settimeout(5)
                    # except socket.timeout as error:
                    #     print(error)
                    #wait for reply
        except EOFError:
            pass

def handle_recv():
    while True: 
        # server listening for msgs
        global SERVER_SOCKETS, LEADER_HINT, RECEIVED
        try: 
            word1 = SERVER_SOCKETS[0].recv(4096).decode()
            word2 = SERVER_SOCKETS[1].recv(4096).decode()
            word3 = SERVER_SOCKETS[2].recv(4096).decode()
            word4 = SERVER_SOCKETS[3].recv(4096).decode()
            word5 = SERVER_SOCKETS[4].recv(4096).decode()
            if word1 != "" or word2 != "" or word3 != "" or word4 != "" or word5 != "":
                RECEIVED = True
                print("word1: ", word1)
                print("word2: ", word2)
                print("word3: ", word3)
                print("word4: ", word4)
                print("word5: ", word5)
                if word1[2] != "" and int(word1[2]) != LEADER_HINT and int(word1[2]) != 0:
                    LEADER_HINT = int(word1[2])
                if word2[2] != "" and int(word2[2]) != LEADER_HINT and int(word2[2]) != 0:
                    LEADER_HINT = int(word2[2])
                if word3[2] != "" and int(word3[2]) != LEADER_HINT and int(word3[2]) != 0:
                    LEADER_HINT = int(word3[2])
                if word4[2] != "" and int(word4[2]) != LEADER_HINT and int(word4[2]) != 0:
                    LEADER_HINT = int(word4[2])
                if word5[2] != "" and int(word5[2]) != LEADER_HINT and int(word5[2]) != 0:
                    LEADER_HINT = int(word5[2])
        except (socket.timeout, KeyboardInterrupt) as error:
            # do_exit(SERVER_SOCKETS[0], SERVER_SOCKETS[1], SERVER_SOCKETS[2], SERVER_SOCKETS[3], SERVER_SOCKETS[4])
            print(error)
            if error != "timed out":
                close_connection()

if __name__ == '__main__':
    # for i in range(5):
    #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     SERVER_SOCKETS.append(sock)
    #     SERVER_SOCKETS[i].connect((socket.gethostname(), SERVER_PORTS[i+1]))
    CLIENT_ID = sys.argv[1]

    for i in range(1,6):
        SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        SERVER_SOCKET.connect((socket.gethostname(), SERVER_PORTS[i]))
        SERVER_SOCKET.sendall(p.dumps(("client", CLIENT_ID)))
        SERVER_SOCKETS.append(SERVER_SOCKET)

    threading.Thread(target=handle_inputs, args=()).start()
    handle_recv()