import xmlrpclib
import cPickle as pickle
import time, os, logging
#...
import params
import util

class DfsClient:
    def __init__(self):
        self.addr = 'http://' + params.NAMENODE_IP + ':' + str(params.RPC_PORT) 
        self.server = xmlrpclib.ServerProxy(self.addr)

    def ls(self):
        print self.server.ls()
    
    def open(self, file_name):
        data = self.server.open(file_name)
        #return a DfsFile object
        return pickle.loads(data)
    
    def upload(self, local_file):
        size = os.path.getsize(local_file)
        file_name = os.path.basename(local_file)
        res = self.server.upload(file_name, str(size))
        fobj = pickle.loads(res)
        if not fobj:
            logging.error('already exist in server')
            return
        else:
            print 'uploading, wait...'
            self._write_block_file(local_file, fobj)
            print 'success upload block file'
            
    def upload_raw(self, local_file):
        file_name = os.path.basename(local_file)
        if not os.path.exists(local_file):
            logging.error('file not exist ')
        res = self.server.upload_raw(file_name)
        canUp = pickle.loads(res)
        if canUp:
            print 'uploading, wait...'
            self._write_raw(local_file)
            print 'success upload raw file...'
        else:
            logging.error('already exist in server')

    def _write_raw(self,local_name): 
        fid = open(local_name, 'rb')
        size = os.path.getsize(local_name)
        file_name = os.path.basename(local_name)
        cur = 0
        while size > 0:
            data = fid.read(params.BUFFER_SIZE)
            util.BlockUtil.write_raw_file(file_name, cur, data)
            cur += len(data)
            size -= len(data)
        fid.close()
        
    def _write_block_file(self,file_name, fobj):#fobj,DfsFile object
        fid = open(file_name, 'rb')
        buf_size = params.BUFFER_SIZE
        i,total = 1, len(fobj.blocks)
        for blo in fobj.blocks:
            cur = 0
            left = blo.size
            while left > 0:
                count = buf_size if left >=buf_size else left
                data = fid.read(count)
                util.BlockUtil.write_block(blo.id, cur, data)
                cur += len(data)
                left -= len(data)
            logging.info("%d/%d accomplish", i, total)
            i=i+1
        fid.close()