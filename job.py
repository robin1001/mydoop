import socket
import struct
import cPickle as pickle
import marshal, types
from multiprocessing import Process
import time
from threading import Thread
import logging
import heapq
import sys, os
#----
import params
from mapreduce import MapOutBuffer
from util import TcpHelper as helper
from util import BlockReader
from dfsclient import DfsClient
import file_util

class Protocal:
    def __init__(self):
        self.id = 0
        self.mapfn = None
        self.reducefn = None
        self.combinefn = None
        self.reduce_num =  0
        self.map_num = 0
        self.dfsfile = None
        self.mode = 0x02
        
    def send_command(self, command, data=' '):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((params.TASK_SERVER_IP, params.TASK_SERVER_PORT))
        msg = command + ':' + data
        msg_len = struct.pack('!i', len(msg))
        sock.sendall(msg_len + msg)
        recv_len, = struct.unpack('!i',helper.receive_len(sock, 4))
        recv_data = helper.receive_len(sock, recv_len)
        pos = recv_data.find(':')
        cmd = str(recv_data[: pos])
        msg = recv_data[pos+1:]
        sock.close()
        return cmd, msg
    
    def send_id_command(self, command, data=' '):
        idB = struct.pack('!i', self.id)
        return self.send_command(command, idB + data)
    
    def _parse_task_info(self, data):
        cur = 0
        #map_num
        self.map_num, = struct.unpack('!i', data[cur:cur+4])
        cur += 4
        #reduce num
        self.reduce_num, = struct.unpack('!i', data[cur:cur+4])
        cur += 4
        #mapfn
        map_len, = struct.unpack('!i', data[cur:cur+4])
        cur += 4
        code_string = data[cur: cur+map_len]
        self.mapfn = types.FunctionType(marshal.loads(code_string), globals(), "mapfn")
        cur += map_len
        #reducefn
        reduce_len, = struct.unpack('!i', data[cur:cur+4])
        cur += 4
        code_string = data[cur: cur+reduce_len]
        self.reducefn = types.FunctionType(marshal.loads(code_string), globals(), "reducefn")
        cur += reduce_len
        #combinefn
        isExsit,  = struct.unpack('!b', data[cur:cur+1])
        cur += 1
        if isExsit==0x01:
            combine_len, = struct.unpack('!i', data[cur:cur+4])
            cur += 4
            code_string = data[cur: cur+combine_len]
            self.combinefn = types.FunctionType(marshal.loads(code_string), globals(), "combinefn")
            cur += combine_len
        #dfs file
        self.mode,  = struct.unpack('!b', data[cur:cur+1])
        cur += 1
        if self.mode == 0x01:
            name_len, = struct.unpack('!i', data[cur:cur+4])
            cur += 4
            self.dfsfile = data[cur: cur+name_len] 


def start_map_process(mapId, task_info):
    mapper = Mapper(mapId, task_info)
    mapper.start()
    
def start_reduce_process(reduceId, task_info):
    reducer = Reducer(reduceId, task_info)
    reducer.start()

class JobClient(Protocal):
    START = 0
    INFO = 1
    HEART = 2
    def __init__(self):
        Protocal.__init__(self)
        self.id = 0
        self.task_info = None
        self.file_server = file_util.FileServer(params.MAP_TMP_DIR)
        self.file_server_port = self.file_server.get_port()
        self.map_max = params.JOB_MAP_MAX
        self.reduce_max = params.JOB_REDUCE_MAX
        
    def sayhello(self):
        info = struct.pack('!3i', self.map_max , self.reduce_max, self.file_server_port)
        cmd, msg = self.send_command('job_start', info)
        #id
        self.id, = struct.unpack('!i', msg[0:4])
        self.task_info = msg[4:]
        self._parse_task_info(msg[4:])
        
    def start(self):
        print 'job client start...'
        #say hello to task server
        self.sayhello()
        #start a file server waiting transfer map result file to reducer
        logging.info('start file server')  
        t = Thread(target=self.file_server.start)
        t.start() 
            
        #start timer to send heart beat
        logging.info('job heart beat loop')  
        while True:
            data = self.send_id_command('job_heart')
            if self._heart_beat_callback(data):
                break
            time.sleep(3)     
        logging.info('job client %d exit' % self.id)   
        logging.info('job client %d exit' % self.id) 
        os._exit(0)
        
    def delete_tmp_file(self):
        if  os.path.exists(self.tmp_dir):
            file_util.rm_dir(self.tmp_dir)
                
    #return whether to exit      
    def _heart_beat_callback(self, data):
        cmd, msg = data
        if cmd == 'job_map_assign':
            #start new process
            map_id, = struct.unpack('!i', msg)
            logging.info('start map %d' % map_id)
            #p = Thread(target=start_map_process, args=(map_id, self.task_info))
            p = Process(target=start_map_process, args=(map_id, self.task_info))
            p.start()
        elif cmd == 'job_reduce_assign':
            reduce_id, = struct.unpack('!i', msg)
            logging.info('start reduce %d' % reduce_id)
            p = Process(target=start_reduce_process, args=(reduce_id, self.task_info))
            p.start()
        elif cmd =='job_idle':
            pass
        elif cmd == 'job_exit':
            self.file_server.close()
            return True#exit
        return False

    
class Mapper(Protocal):
    def __init__(self, id, task_info):
        Protocal.__init__(self)
        self.id = id
        self._parse_task_info(task_info)
        self.outBuffer = MapOutBuffer(self.id, self.reduce_num, self.combinefn)
        
    def sayhello(self):
        cmd, res = self.send_id_command('map_start')
        self.source = pickle.loads(res) 
    
    def _mem_mode_map(self):
        res = self.mapfn(self.id, self.source)
        for kv in res:
            self.outBuffer.write(kv) 
        self.outBuffer.merge_all()
            
    def _disk_mode_map(self): 
        fobj = DfsClient().open(self.dfsfile)
        block = self.source
        reader = BlockReader(block, fobj)
        while True:
            data = reader.read_line()
            if data is None:
                break
            res = self.mapfn(self.id, data)
            for kv in res:
                self.outBuffer.write(kv)
        self.outBuffer.merge_all()
        
    def start(self):
        print 'ready, map %d starting' % self.id
        logging.info('ready, map %d starting' % self.id)
        self.sayhello()
        if self.mode == 0x01:
            self._disk_mode_map()
        else:
            self._mem_mode_map()
            
        print 'map %d work down' % self.id    
        logging.info('map %d work down' % self.id)
        cmd, msg = self.send_id_command('map_down')
        if msg =='map_exit':
            logging.info('map %d exit' % self.id )
        
class Reducer(Protocal):       
    def __init__(self, id, taskInfo):
        Protocal.__init__(self)
        self.id = id
        self._parse_task_info(taskInfo)
        self.file_servers = None
        self.dir = params.REDUCE_TMP_DIR
        self.map_files= []
        
    def sayhello(self):
        cmd, res = self.send_id_command('reduce_start')
        self.file_servers = pickle.loads(res) 
        #print self.file_servers
            
    def start(self):
        print 'ready, reduce %d starting' % self.id
        logging.info('ready, reduce %d starting' % self.id)
        self.sayhello()
        
        logging.info('reduce  %d start download map result file...' %self.id)
        self.downLodeFile()
        
        logging.info('reduce %d start merge' % self.id)
        self.merge()
        
        print 'reduce %d work down' % self.id
        logging.info('reduce %d work down' % self.id) 
        cmd, msg = self.send_id_command('reduce_down')
        if msg =='reduce_exit':
            logging.info('reduce %d exit' % self.id )
    
    def downLodeFile(self):
        for item in self.file_servers:
            ip, port, map_ids = item
            for i in map_ids:
                file_name = 'map%d_%d.seg' % (i, self.id)
                self.map_files.append(file_name)
                #print 'try to download %s' % file_name
                file_util.FileRequest.request((ip, port), file_name, self.dir) 
                #print 'download %s success' % file_name
                
    def merge(self):
        res_path = self.dir + ('reduce%d.res' % self.id)
        txt_path = self.dir + ('reduce%d.txt' % self.id)
        fidres = open(res_path, 'wb')
        fidtxt = open(txt_path, 'w')
        
        map_fids = [ (open(self.dir + i, 'rb')) for i in self.map_files ]
        heap = []
        for fid in map_fids:
            item = pickle.load(fid)
            key, values = item
            heap.append((key, values, fid))
        heapq.heapify(heap)
        
        #while--------------------------------------------#
        pre_key = None
        values = []
        while True:
            try:
                key, val, fid = heapq.heappop(heap)
                if pre_key != None and pre_key != key:
                    reduce_value = self.reducefn(pre_key, values) 
                    pickle.dump((pre_key, reduce_value), fidres, True)
                    if isinstance(reduce_value, list):
                        line = str(pre_key) + ',' +' '.join([ str(i) for i in reduce_value] ) + '\n'
                    else:
                        line = str(pre_key) + ',' +str(reduce_value) + '\n'
                    fidtxt.write(line)
                    values=[]
                try:
                    k,v = pickle.load(fid)
                    heapq.heappush(heap, (k,v,fid))
                except Exception, ex:#read down
                    #print 'in else', Exception, ex
                    fid.close()
                    continue
                pre_key = key
                if isinstance(val, list):
                    values.extend(val)
                else:
                    values.append(val) 
            except IndexError: 
                break
            except Exception, ex:
                print Exception, ex
                break
        reduce_value = self.reducefn(pre_key, values) 
        pickle.dump((pre_key, reduce_value), fidres, True)
        if isinstance(reduce_value, list):
            line = str(pre_key) + ',' +' '.join([ str(i) for i in reduce_value] ) + '\n'
        else:
            line = str(pre_key) + ',' +str(reduce_value) + '\n'
        fidtxt.write(line)
        #while--------------------------------------------#
        
        fidres.close()
        fidtxt.close()
        
        
        
if __name__ == '__main__':
    '''
    you can modify the max map and max reduce num your job
    client can deal in params.py    
    '''
    params.init()
    logging.basicConfig(level=logging.INFO)
    
    work = JobClient()
    work.start() 