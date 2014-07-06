import time
import random
import os
#...
import params

'''all the data structure define used in doop'''
class Block:
    # id time_random(
    def __init__(self, size, offset):
        self.id = self._get_id()
        self.size = size
        self.offset = offset
        
    def _get_id(self):
        t = int(time.time())
        rand = random.randint(0,1000000)
        return t * 1000000 + rand
        
class DfsFile:
    def __init__(self, name, size):
        self.name = name
        self.time = time.time()
        self.size = size
        self.blocks=[]
        self._split()
     
    def _split(self):
        block_dir = params.BLOCK_DIR + self.name
        os.makedirs(block_dir)
        chunk = params.BLOCK_SIZE
        cur = 0#offset
        size = self.size  
        while size > 0:
            if size >= chunk:
                len = chunk
            else:
                len = size
            block = Block(len, cur)
            self.blocks.append(block)
            #create empty block         
            cur = cur + chunk
            size = size - len
        