import hashlib

class Operation:
    def __init__(self, op=None, key=None, value=None):
        self._op = op
        self._key = key
        self._value = value

    def __str__(self):
        if self._value is not None:
            return "{} {} {}".format(self._op, self._key, self._value)
        else:
            return "{} {}".format(self._op, self._key)

    # op attribute
    @property
    def op(self):
        return self._op

    @op.setter
    def op(self, value):
        if value == "put" or value == "get":
            self._op = value
        else:
            print("Operation was neither put nor get.")

    @op.deleter
    def op(self):
        del self._op

    # key attribute
    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @key.deleter
    def key(self):
        del self._key

    # value attribute
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @value.deleter
    def value(self):
        del self._value

class Block:
    def __init__(self, prev_hash="None", op_op=None, op_key=None, op_value=None, nonce="nonce_stub", op=None):
        self._prev_hash = prev_hash
        if op != None:
            self._operation = op
        else:
            self._operation = Operation(op_op, op_key, op_value)
        self._nonce = None
        self.next = None
        self.prev = None

    def __str__(self):
        return "Previous Hash: {}\nOperation: {}\nNonce: {}\n".format(self._prev_hash, self._operation, self._nonce)

    # prev_hash attribute
    @property
    def prev_hash(self):
        return self._prev_hash

    @prev_hash.setter
    def prev_hash(self, value):
        self._prev_hash = value
    
    @prev_hash.deleter
    def prev_hash(self):
        del self._prev_hash

    # operation attribute
    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, value):
        self._operation = value

    @operation.deleter
    def operation(self):
        del self._operation
    
    # nonce attribute
    @property
    def nonce(self):
        return self._nonce

    @nonce.setter
    def nonce(self, value):
        self._nonce = value

    @nonce.deleter
    def nonce(self):
        del self._nonce

class Blockchain:
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, op_op, op_key, op_value, nonce):
        if self.head is None:
            prev_hash = "None"
            NEW_BLOCK = Block(prev_hash, op_op, op_key, op_value, nonce)
            self.head = NEW_BLOCK
            self.tail = NEW_BLOCK
            return
        PREV_BLOCK = self.tail
        # run hash function on PREV_BLOCK
        str_to_be_hashed = str(PREV_BLOCK.operation) + str(PREV_BLOCK.nonce) + str(PREV_BLOCK.prev_hash)
        prev_hash = str(hashlib.sha256(str_to_be_hashed.encode()).hexdigest())
        NEW_BLOCK = Block(prev_hash, op_op, op_key, op_value, nonce)
        PREV_BLOCK.next = NEW_BLOCK
        NEW_BLOCK.prev = PREV_BLOCK
        self.tail = NEW_BLOCK


    def save(self, server_id):
        filename = "blockchain{}.txt".format(str(server_id))
        f = open(filename, "a")
        b = self.tail
        savestr = "{},{},{},{},{}\n".format(b.prev_hash, b.operation.op, b.operation.key, b.operation.value, b.nonce)
        f.write(savestr)
        f.close()

    def load(self, server_id):
        filename = "blockchain{}.txt".format(str(server_id))
        f = open(filename, "r")
        lines = f.readlines()
        self.head = None
        self.tail = None
        for line in lines:
            line = line.rstrip("\n")
            line_split = line.split(",")
            op_op = line_split[1] if line_split[1] != "None" else None
            op_key = line_split[2] if line_split[2] != "None" else None
            op_value = line_split[3] if line_split[3] != "None" else None
            nonce = line_split[4] if line_split[4] != "None" else None
            self.append(op_op, op_key, op_value, nonce)

    def __str__(self):
        temp = self.head
        count = 0
        ans = ""

        while temp is not None:
            ans += "++++++++++++++++++++++++++++++++++++Block {}++++++++++++++++++++++++++++++++++++\n".format(count)
            ans += str(temp)
            ans += "\n"
            count += 1
            temp = temp.next

        return ans

