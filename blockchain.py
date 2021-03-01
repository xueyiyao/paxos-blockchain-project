import hashlib

class Block:
    def __init__(self, prev_hash, operation, nonce):
        self.prev_hash = prev_hash
        self.operation = operation
        self.nonce = nonce
        self.next = None
        self.prev = None

class Blockchain:
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, operation, nonce):
        if self.head is None:
            prev_hash = "None"
            NEW_BLOCK = Block(prev_hash, operation, nonce)
            self.head = NEW_BLOCK
            self.tail = NEW_BLOCK
            return
        PREV_BLOCK = self.tail
        # run hash function on PREV_BLOCK
        str_to_be_hashed = str(PREV_BLOCK.operation) + str(PREV_BLOCK.nonce) + str(PREV_BLOCK.prev_hash)
        prev_hash = str(hashlib.sha256(str_to_be_hashed.encode()).hexdigest())
        NEW_BLOCK = Block(prev_hash, operation, nonce)
        PREV_BLOCK.next = NEW_BLOCK
        NEW_BLOCK.prev = PREV_BLOCK
        self.tail = NEW_BLOCK


    def save(self, server_id):
        filename = "blockchain" + str(server_id) + ".txt"
        f = open(filename, "a")
        savestr = self.tail.prev_hash + "," + self.tail.operation + "," + self.tail.nonce + "\n"
        f.write(savestr)
        f.close()

    def load(self, server_id):
        filename = "blockchain" + str(server_id) + ".txt"
        f = open(filename, "r")
        lines = f.readlines()
        self.head = None
        self.tail = None
        for line in lines:
            line = line.rstrip("\n")
            line_split = line.split(",")
            operation = line_split[1]
            nonce = line_split[2]
            self.append(operation, nonce)

    def __str__(self):
        temp = self.head
        ans = ""
        while temp is not None:
            ans += "++++++++++++++++\n"
            ans += ("Previous Hash: " + temp.prev_hash + "\n")
            ans += ("Operation: " + temp.operation + "\n")
            ans += ("Nonce: " + temp.nonce + "\n\n")
            temp = temp.next

        return ans

