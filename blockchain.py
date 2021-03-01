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
            prev_hash = ""
            NEW_BLOCK = Block(prev_hash, operation, nonce)
            self.head = NEW_BLOCK
            self.tail = NEW_BLOCK
            return
        PREV_BLOCK = self.tail
        # run hash function on PREV_BLOCK
        prev_hash = "STUB"
        NEW_BLOCK = Block(prev_hash, operation, nonce)
        PREV_BLOCK.next = NEW_BLOCK
        NEW_BLOCK.prev = PREV_BLOCK
        self.tail = NEW_BLOCK
