import socket
import sys
import threading
import os
import random
import pickle

global SERVER_PORTS, SERVER_SOCKETS, SERVER_SOCKET, LEADER_HINT
SERVER_PORTS = {
    1: 5001, 
    2: 5002,
    3: 5003,
    4: 5004,
    5: 5005
}
SERVER_SOCKETS = []
SERVER_SOCKET = None
LEADER_HINT = None

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
    SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    SERVER_SOCKET.connect((socket.gethostname(), SERVER_PORTS[server_id]))

def close_connection():
    global SERVER_SOCKET
    SERVER_SOCKET.close()
    os._exit(0)

# handle inputs
def handle_inputs(): 
    global SERVER_SOCKETS
    while True: 
        try: 
            line = input()
            line_split = line.split(" ")
            if (line == 'write'):
                print("writing to other servers")
                for i in range(5):
                    SERVER_SOCKETS[i].sendall(b'test')
            elif (line == 'exit'):
                # do_exit(SERVER_SOCKETS[0], SERVER_SOCKETS[1], SERVER_SOCKETS[2], SERVER_SOCKETS[3], SERVER_SOCKETS[4])
                close_connection()
            elif "Operation" in line:
                if LEADER_HINT is None:
                    temp = random.randint(1,5)
                    temp = 5
                    connect_server(temp)
                    SERVER_SOCKET.sendall(pickle.dumps(("Operation", line)))
                    message_recv = SERVER_SOCKET.recv(1024).decode()
                    print(message_recv)
                    close_connection()
                else:
                    pass
                    #send to server
                    #wait for reply
        except EOFError:
            pass

def handle_recv():
    while True: 
        # server listening for msgs
        global SERVER_SOCKETS
        try: 
            pass
            # word1 = SERVER_SOCKETS[0].recv(1024).decode()
            # word2 = SERVER_SOCKETS[1].recv(1024).decode()
            # word3 = SERVER_SOCKETS[2].recv(1024).decode()
            # word4 = SERVER_SOCKETS[3].recv(1024).decode()
            # word5 = SERVER_SOCKETS[4].recv(1024).decode()
            # print(word1, word2, word3, word4, word5)
        except KeyboardInterrupt:
            # do_exit(SERVER_SOCKETS[0], SERVER_SOCKETS[1], SERVER_SOCKETS[2], SERVER_SOCKETS[3], SERVER_SOCKETS[4])
            close_connection()

if __name__ == '__main__':
    # for i in range(5):
    #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     SERVER_SOCKETS.append(sock)
    #     SERVER_SOCKETS[i].connect((socket.gethostname(), SERVER_PORTS[i+1]))

    threading.Thread(target=handle_inputs, args=()).start()
    handle_recv()