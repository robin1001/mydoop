import socket
import random
import threading
import struct
import os
import logging
import time
from multiprocessing import Process
#---
import params
from util import TcpHelper as helper

class FileServer:
    def __init__(self, base_dir):
        self._base_dir = base_dir
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        binding = False
        while not binding:
            self._port = random.randint(10000,20000)
            try:
                self.server.bind(('', self._port))
                binding = True
            except:
                continue
               
    def start(self):
        self.server.listen(100)
        while True:
            sock, addr = self.server.accept()
            t=threading.Thread(target=self.handle_request, args=(sock,))
            t.start()
            
    def handle_request(self, sock):
        name_len, = struct.unpack('!i', helper.receive_len(sock, 4))
        file_name = helper.receive_len(sock, name_len)
        #logging.info('request:'+ file_name)
        path = self._base_dir + file_name
        fid = open(path, 'rb')
        while True:
            data = fid.read(params.BUFFER_SIZE)
            if not data:
                break;
            sock.sendall(data)
        fid.close()
        sock.close()
        logging.info('response:'+ file_name)
        
    def get_port(self):
        return self._port
    
    def close(self):
        self.server.close()


class FileRequest:
    @staticmethod
    def request(addr, file_name, out_dir):
        logging.info("request:%s %s" % (addr, file_name))
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(addr);
        name_len = struct.pack('!i', len(file_name))
        client.sendall(name_len + file_name)
        
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        path = out_dir + file_name
        fid = open(path, 'wb')
        while True:
            data = client.recv(params.BUFFER_SIZE)
            if not data:
                break
            fid.write(data)        
        fid.close()
        client.close()
        logging.info("write file:"+path)
        
def rm_dir(src):
    '''delete files and folders'''
    if os.path.isfile(src):
        try:
            os.remove(src)
        except:
            pass
    elif os.path.isdir(src):
        for item in os.listdir(src):
            itemsrc=os.path.join(src,item)
            rm_dir(itemsrc) 
        try:
            os.rmdir(src)
        except:
            pass