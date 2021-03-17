# Multi-Paxos Key-Value Store 

This is the final project for the undergrad distributed systems class at UCSB. The goal is to implement a distributed key-value application on top of a simple blockchain to create a trusted but fault-tolerant system.  
By Xueyi Yao (@xueyiyao) and Justin Dong (@justindong1)

## Setup 

### **Servers:**  

1. **Starting the servers:**  
    Start up 5 terminals, one for each server.

    ```
    % python3 server.py <SERVER_ID>
    ```

    SERVER_ID should be a value from 1 to 5. Each terminal should correspond to a different SERVER_ID

2. **Connecting the servers:**  
    Simply run "c" in each of the server terminals. You should see:
    ```
    connecting to other servers
    connected to other servers
    ```

### **Clients:**

1. **Starting the clients:**
    Start up 1-3 terminals, one for each client.

    ```
    % python3 client.py <CLIENT_ID>
    ```

    CLIENT_ID should be a value from 1 to 3. Each terminal should correspond to a different CLIENT_ID. You should get a response:
    ```
    checking word for server 1
    checking word for server 2
    checking word for server 3
    checking word for server 4
    checking word for server 5
    ```

    Congrats! You have successfully setup the servers and the clients!

## User Inputs
### **Servers:** 
* printBlockchain
    * prints the blockchain for the server to terminal
* printKVStore
    * prints the key-value store for the server to terminal
* printQueue
    * prints the queue for the server to terminal
* failLink(&lt;src&gt;, &lt;dest&gt;)
    * temporarily disables the connection between src server and dest server
    * currently only works if initiated from the src server's terminal
* fixLink(&lt;src&gt;, &lt;dest&gt;)
    * re-enables the connection between src server and dest server
    * currently only works if initiated from the src server's terminal
* failProcess
    * permanently closes connections and closes the server
    * server will need to be restarted to continue working
* reconnect
    * reconnects the server to all other servers
    * **OPTIONALLY** add servers to not reconnect to
        * "reconnect 1 2" will mean to reconnect to all servers except 1 and 2
* e
    * forcibly closes all connections and exits the server

### **Clients:** 
* Operation(put, &lt;key&gt;, &lt;value&gt;)
    * add a key-value pair to the store
* Operation(get, &lt;value&gt;)
    * retrieves a value for a inputted key
* reconnect &lt;SERVER_ID&gt;
    * reconnects client to server with server_id = SERVER_ID
* e
    * forcibly closes connections and exits the client
    
    


