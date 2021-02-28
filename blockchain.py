class Block:
    def __init__(self, prev_hash, operation, nonce):
        self.prev_hash = prev_hash
        self.operation = operation
        self.nonce = nonce
        self.prev = None

class Blockchain:
    def __init__(self):
        self.tail = None

    def append(self, operation, nonce):
        PREV_BLOCK = self.tail
        # run hash function on PREV_BLOCK
        prev_hash = "STUB"
        NEW_BLOCK = Block(prev_hash, operation, nonce)
        NEW_BLOCK.prev = PREV_BLOCK
        self.tail = NEW_BLOCK
