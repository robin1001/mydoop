import logging
import SimpleXMLRPCServer
import cPickle as pickle
import socket
import thread,threading
import struct
#...
import params, beans
from util import TcpHelper as helper

class NameNode(object):
    def __init__(self):
        self.meta = {}
        self.block_map = {}
        self.read_meta()
        self.refresh_blocks()
        self.write_block_lock = threading.RLock()
        self.write_raw_lock = threading.RLock()
    
    def ls(self):
        msg = ''
        for key in self.meta:
            msg += (key + '\n')
        return msg
    
    def open(self, file_name):
        if not self.meta.has_key(file_name):
            logging.error('not in meta file map')
            return None
        return pickle.dumps(self.meta[file_name])
    
    def start(self):
        thread.start_new_thread(self.start_rpc, ())
        self.start_tcp()
        
    def start_rpc(self):
        logging.info('start rpc server...')
        server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', params.RPC_PORT))
        server.register_instance(self)
        server.serve_forever()
        
    def start_tcp(self):
        logging.info('start tcp server...')
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('', params.TCP_PORT))
        server.listen(100)
        while True:
            sock, addr = server.accept()
            thread.start_new_thread(self.handle, (sock, addr))
            
    def handle(self, sock, addr):
        #read or write
        cmd = sock.recv(1)
        if not cmd:
            return
        #logging.info('new cmd arrived')
        
        cmd, = struct.unpack('!b', cmd)
        if cmd == 0x01:#read block
            msg = helper.receive_len(sock, 24)
            block_id, start, length = struct.unpack('!3q', msg)
            data = self.read_block(block_id, start, length)
            sock.sendall(data)
        elif cmd == 0x02:#write block
            msg = helper.receive_len(sock, 24)
            block_id, start, data_len = struct.unpack('!3q', msg)
            data = helper.receive_len(sock, data_len)
            self.write_block(block_id, start, data)
            
        elif cmd ==0x03:#write raw file
            msg = helper.receive_len(sock, 17)
            name_len, start, data_len = struct.unpack('!b2q', msg)
            file_name = sock.recv(name_len)
            data = helper.receive_len(sock, data_len)
            self.write_raw(file_name, start, data)
                
        sock.close()
        
    '''upload file directly form namenode'''
    def upload(self, file_name, size):
        if self.meta.has_key(file_name):
            return pickle.dumps(False)  
        f = beans.DfsFile(file_name, int(size))
        self.meta[f.name] = f
        self.save_meta()
        self.refresh_blocks()
        return pickle.dumps(f)
    
    def upload_raw(self, file_name):
        if self.meta.has_key(file_name):
            logging.error('already exist file')
            return pickle.dumps(False)
        self.meta[file_name] = 'raw_file'
        self.save_meta()
        return pickle.dumps(True)
    
    
    def read_meta(self):
        logging.info('read meta data')
        fid = open(params.META_FILE, 'rb')
        self.meta = pickle.load(fid)
        fid.close()
                  
    def save_meta(self):
        logging.info('save meta data')
        fid = open(params.META_FILE, 'wb')
        pickle.dump(self.meta, fid, True) 
        fid.close()  
        
    def refresh_blocks(self):
        logging.info('refresh block map')
        self.block_map ={}
        for k in self.meta:
            if self.meta[k] != 'raw_file':
                for blo in self.meta[k].blocks:
                    self.block_map[blo.id] = blo, k
        
    def read_block(self, block_id, start, length):   
        #logging.info('read block data')
        path = params.BLOCK_DIR + self.block_map[block_id][1] + '/' + str(block_id)
        fid = open(path, 'rb')
        fid.seek(start)
        data = fid.read(length)
        fid.close()
        return data
    
    def write_block(self, block_id, start, data):
        self.write_block_lock.acquire()
        path = params.BLOCK_DIR + self.block_map[block_id][1] + '/' + str(block_id)
        #lock
        fid = open(path, 'ab')
        fid.seek(start)
        fid.write(data)
        fid.close()
        self.write_block_lock.release()
    
    def write_raw(self, file_name, start, data):
        self.write_raw_lock.acquire()
        path = params.RAW_FILE_DIR + file_name
        #lock
        fid = open(path, 'ab')
        fid.seek(start)
        fid.write(data)
        fid.close()
        self.write_raw_lock.release()
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    params.init()
    NameNode().start()
