import socket,struct
#...
import params

#socket receive until data len = length
class TcpHelper:
    @staticmethod
    def receive_len(sock, length):
        data = []
        left = length
        while left != 0:
            tmp = sock.recv(left)
            data.append(tmp)
            left -= len(tmp)
        return ''.join(data)
    @staticmethod
    def send(ip, port, datagram, wait_read = False, read_len=0):  
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        sock.sendall(datagram)
        if wait_read:
            data = TcpHelper.receive_len(sock, read_len)
            sock.close()
            return data
        sock.close()
        
                   
class BlockUtil:
    @staticmethod
    def read_block(block_id, start, length):
        datagram = struct.pack('!b3q', 0x01, block_id, start, length)
        ip,port = params.NAMENODE_IP, params.TCP_PORT
        data = TcpHelper.send(ip, port, datagram, True, length)
        return data
    
    @staticmethod
    def write_block(block_id, start, data):
        cmd = struct.pack('!b3q', 0x02, block_id, start, len(data))
        ip,port = params.NAMENODE_IP, params.TCP_PORT
        TcpHelper.send(ip, port, cmd + data)
    
    @staticmethod
    def write_raw_file(file_name, start, data):
        name_len = len(file_name)
        data_len = len(data)
        cmd = struct.pack('!2b2q', 0x03, name_len, start, data_len)
        ip,port = params.NAMENODE_IP, params.TCP_PORT
        TcpHelper.send(ip, port, cmd + file_name + data)

    
class DfsFileReader:    
    def __init__(self, file_obj):
        self.file = file_obj
        self.blocks = file_obj.blocks
        #for readline
        self.buffer = []
        self.buf_cur = 0
        self.file_cur = 0
        self.buf_size = params.BUFFER_SIZE
        
    def read(self, offset, length):               
        # calc start pos
        pos = offset / params.BLOCK_SIZE 
        data = []
        for i in range(pos,len(self.blocks)):
            start = 0
            if i == pos:
                start = offset - self.blocks[i].offset
            num = self.blocks[i].size - start
            count =length if num >=length else num
            new = BlockUtil.read_block(self.blocks[i].id, start, count)
            data.append(new)
            length -= len(new)
            if length == 0:
                break    
        return ''.join(data)
    
    def eof(self):
        if self.file_cur != self.file.size:
            return False
        return True
    
    def read_line(self, **kvargs):
        if kvargs.has_key('offset'):
            self.file_cur = kvargs['offset']
             
        if not self.buffer:
            self.buffer = self.read(self.file_cur, self.buf_size)
        data=[]
        while self.file_cur < self.file.size:
            if self.buffer[self.buf_cur] == '\n':
                self.buf_cur += 1
                self.file_cur += 1
                break
            else:
                data.append(self.buffer[self.buf_cur])
            self.buf_cur += 1
            self.file_cur += 1
            if self.buf_cur ==self.buf_size:
                self.buffer = self.read(self.file_cur, self.buf_size)
                self.buf_cur = 0
        
        return ''.join(data)
    
    def seek(self, offset):
        self.file_cur = offset
        

class BlockReader:
    def __init__(self, block, dfs_file):
        self.block = block
        self.reader = DfsFileReader(dfs_file)
        #for read_line, neglect first line
        if self.block.offset != 0 :
            self.reader.read_line(offset=self.block.offset)
        
    def read(self, start, length):
        return BlockUtil.read_block(self.block.id, start, length)
    
    def read_line(self):
        #read one more line
        if self.reader.file_cur >= self.block.offset + self.block.size:
            return
        return self.reader.read_line()