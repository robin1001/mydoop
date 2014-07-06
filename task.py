import socket
import threading
import struct
import cPickle as pickle
import logging
import beans
import sys
import marshal
import time
#----
import params
from util import TcpHelper as helper
from dfsclient  import DfsClient

class JobInfo:
    def __init__(self, id, maxM, mapR, ip, port):
        self.id = id
        self.max_map = maxM
        self.max_reduce = mapR
        self.map_running=[]
        self.map_finished = []
        self.reduce_running = []
        self.reduce_finished=[]
        self.ip = ip
        self.file_server_port = port
    def setMapFinished(self, map_id):
        self.map_running.remove(map_id)
        self.map_finished.append(map_id)
    def addMapRunning(self, map_id):
        self.map_running.append(map_id)
    def setReduceFinished(self, map_id):
        self.reduce_running.remove(map_id)
        self.reduce_finished.append(map_id)
    def addReduceRunning(self, map_id):
        self.reduce_running.append(map_id)
    def canAddMap(self):
        if len(self.map_running) < self.max_map:
            return True
        return False
    def canAddReduce(self):
        if len(self.reduce_running) < self.max_reduce:
            return True
        return False
        
class Scheduler:
    def __init__(self, source):
        self.source = source
        self.length = len(source)
        self.iter = iter(source)
        self.running = {}
        self.finished = {}
        self.id_pair = {}#work(map, reduce)id, job id pair
        self.THRESHOLD = 2.0
        num = self.length % 3
        self.LEAST_FINISH = num if num>2 else self.length - 1
        
    def next_task(self):
        try:
            key = self.iter.next()
            return key
        except:
            #all were assigned, but only finish a little, wait 
            if len(self.finished) < self.LEAST_FINISH:
                return -1
            #re assign work that is too slow 
            for (k,t) in self.running.items():
                if (time.time() - t) > self.THRESHOLD * self._getAverageTime():
                    logging.info('reassign work %d' % k)
                    return k
            return -1
        
    def isDown(self):
        if len(self.finished) != self.length:
            return False
        return True
    
    def setRunning(self, id):
        self.running[id] = time.time()
    
    def setFinished(self, id):
        self.finished[id] = time.time() - self.running[id]
        del self.running[id] 
    
    def _getAverageTime(self):
        count = len(self.finished)
        sum = 0
        for k in self.finished:
            sum += self.finished[k]
        return float(sum) / count

    def addIdPair(self, kid, vid):
        self.id_pair[kid] = vid
          
       
class Task:
    START = 0x01
    MAP = 1
    REDUCE = 2
    ALLDOWN = 3
    DISK_MODE = 0x01
    MEM_MODE = 0x02
    def __init__(self):
        self.datasource = None
        #for task info
        self.reducer_num = 2
        self.mapper_num = None
        self.task_info = None
        self.mapfn = None
        self.reducefn = None
        self.combinefn = None
        self.dfsfile = None
        self.DATA_TYPE = Task.MEM_MODE
        #for scheduel
        self.jobs = {}
        self.map_scheduler = None
        self.map_result = {}
        self.job_id = 0
        self.state = Task.START
        self.reduce_scheduler = Scheduler(range(self.reducer_num))
        
        #for secure
        self.lock = threading.Lock()
        
    def server_forever(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('', params.TASK_SERVER_PORT))
        self.server.listen(100)
        while True:
            sock, addr = self.server.accept()   
            t=threading.Thread(target=self.handle, args=(sock,addr))
            t.setDaemon(True)
            t.start()
    
    def handle(self, sock, addr):
        data_len, = struct.unpack('!i', helper.receive_len(sock, 4))
        all = helper.receive_len(sock, data_len)
        pos = all.find(':')
        cmd = str(all[: pos])
        msg = all[pos+1:]
        if cmd.startswith('job'):
            self.handle_job(sock, addr, cmd, msg)
        elif cmd.startswith('map'):
            self.handle_map(sock, addr, cmd, msg)
        elif cmd.startswith('reduce'):
            self.handle_reduce(sock, addr, cmd, msg)
        sock.close()
    
        
    def handle_job(self, sock, addr, cmd, msg):
        integer, = struct.unpack('!i', msg[0:4])
        if cmd == 'job_start':
            max_map = integer
            max_reduce, port = struct.unpack('!2i', msg[4:12]) 
            job = JobInfo(self.job_id, max_map, max_reduce, addr[0], port)
            self.jobs[self.job_id] = job
            logging.info('job client %d started' % self.job_id)
            idB = struct.pack('!i', self.job_id)
            self.send_command(sock, 'job_start', idB + self.task_info)
            self.job_id += 1
            
        elif cmd == 'job_heart':
            jobId = integer
            #logging.debug('heart beat form %d' % jobId)
            #map phase
            if self.state == Task.MAP:
                if not self.map_scheduler.isDown():
                    if self.jobs[jobId].canAddMap():
                        id = self.map_scheduler.next_task()
                        if id >= 0: 
                            logging.info('assigned map work %d' % id)
                            idB = struct.pack('!i', id) 
                            with self.lock:
                                self.map_scheduler.addIdPair(id, jobId) 
                                self.jobs[jobId].addMapRunning(id)
                            self.send_command(sock, 'job_map_assign', idB)
                    self.send_command(sock, 'job_idle')
                else:
                    self.send_command(sock, 'job_idle')
                    self.state = Task.REDUCE
                    logging.info('start reducing...')
                    
            #reduce phase        
            elif self.state == Task.REDUCE:
                if not self.reduce_scheduler.isDown():
                    if  self.jobs[jobId].canAddReduce():
                        id = self.reduce_scheduler.next_task()
                        if id >= 0:
                            logging.info('assigned reduce work %d' % id)
                            idB = struct.pack('!i', id) 
                            with self.lock:
                                self.reduce_scheduler.addIdPair(id, jobId) 
                                self.jobs[jobId].addReduceRunning(id)
                            self.send_command(sock, 'job_reduce_assign', idB)
                    self.send_command(sock, 'job_idle')
                else:
                    self.send_command(sock, 'job_idle')
                    self.state = Task.ALLDOWN
                    logging.info('reducing down, task finished...')
            elif self.state == Task.ALLDOWN:
                self.send_command(sock, 'job_exit')
            
    def handle_map(self, sock, addr, cmd, msg):
        id, = struct.unpack('!i', msg[0:4])
        if cmd == 'map_start':
            logging.info('map %d start connect...' % id)
            with self.lock:
                self.map_scheduler.setRunning(id)
            source = pickle.dumps(self.map_scheduler.source[id], True)
            self.send_command(sock, 'map_start', source)
        elif cmd=='map_down':
            with self.lock:
                self.map_scheduler.setFinished(id)
                t = self.map_scheduler.finished[id]
                jobid = self.map_scheduler.id_pair[id]
                self.jobs[jobid].setMapFinished(id)
            logging.info('map %d down %f, %d/%d' % (id, t, len(self.map_scheduler.finished), self.map_scheduler.length))
            self.send_command(sock, 'map_exit')
        elif cmd =='':
            pass
            
    
    def handle_reduce(self, sock, addr, cmd, msg):
        id, = struct.unpack('!i', msg[0:4])
        if cmd == 'reduce_start':
            logging.info('reduce %d start connect...' % id)
            with self.lock:
                self.reduce_scheduler.setRunning(id)
            file_servers = []
            for (k,v) in self.jobs.items():
                file_servers.append((v.ip, v.file_server_port, v.map_finished))
            data = pickle.dumps(file_servers, True)
            self.send_command(sock, 'reduce_start', data)
        elif cmd=='reduce_down':
            with self.lock:
                self.reduce_scheduler.setFinished(id)
                t = self.reduce_scheduler.finished[id]
                jobid = self.reduce_scheduler.id_pair[id]
                self.jobs[jobid].setReduceFinished(id)
            logging.info('reduce %d down %f, %d/%d' % (id, t, len(self.reduce_scheduler.finished), self.reducer_num))
            self.send_command(sock, 'reduce_exit')
        elif cmd =='':
            pass        
                 
    '''
    task info send to jobclient, so the jobclient parse in this format
    data format:
        1.1.map_num: int 4byte
        1.2 reduce_num: int 4byte
        2.mapfn_len : int 4byte        
        3.mapfn: N byte
        4.reducefn_len : int 4byte     
        5.reducefn: N byte
        6.is combiner exists: 0x01 exist
        7.combinefn_len : int 4byte    
        8.combinefn: N byte
        9.job_mode: 1 byte, (0x01, mem)  (0x02, disk)
        10.dfsfile name len: int 4byte (mode = 0x02)
        11. dfsfile name: N byte
    '''
    def _init_task_info(self):  
        data = []
        map_reduce_num = struct.pack('!2i', self.mapper_num, self.reducer_num)
        data.append(map_reduce_num)
        #map
        mapfn = marshal.dumps(self.mapfn.func_code)
        mapfn_len = len(mapfn)
        mapfn_len_data = struct.pack('!i', mapfn_len)
        data.extend([mapfn_len_data, mapfn])
        #reduce
        reducefn = marshal.dumps(self.reducefn.func_code)
        reducefn_len = len(reducefn)
        reducefn_len_data = struct.pack('!i', reducefn_len)
        data.extend([reducefn_len_data, reducefn])
        #combine
        if self.combinefn is not None:
            isExist = struct.pack('!b', 0x01)
            combinefn = marshal.dumps(self.combinefn.func_code)
            combinefn_len = len(combinefn)
            combinefn_len_data = struct.pack('!i', combinefn_len)
            data.extend([isExist, combinefn_len_data, combinefn])
        else:
            isExist = struct.pack('!b', 0x02)
            data.append(isExist)
        #job mode, dfsfile
        if self.DATA_TYPE == Task.DISK_MODE:
            modeB = struct.pack('!b', Task.DISK_MODE)
            name_len = len(self.dfsfile)
            namelenB = struct.pack('!i', name_len)
            data.extend([modeB, namelenB, self.dfsfile])
        else:
            modeB = struct.pack('!b', Task.MEM_MODE)
            data.append(modeB)
            
        self.task_info = ''.join(data)
    
    def send_command(self, sock, cmd, msg=' '):
        data = cmd + ':' + msg
        data_len = struct.pack('!i', len(data))
        sock.sendall(data_len + data)
    
    def _process_datasource(self):
        source = {}
        if self.dfsfile is not None:
            self.DATA_TYPE = Task.DISK_MODE
            self.datasource = DfsClient().open(self.dfsfile)
            cur = 0#map id
            for x in self.datasource.blocks:
                source[cur] = x
                cur += 1
        else:
            self.DATA_TYPE = Task.MEM_MODE
            #segment the memory datasource into block,block size according to  params
            block = []
            cur = 0#map id
            start, end = 0, 0
            for x in self.datasource:
                block.append(x)
                if sys.getsizeof(block)> params.TASK_MAP_MEM_BLOCK:
                    source[cur] = block
                    block = []
                    cur += 1
            source[cur] = block
            
        self.map_scheduler = Scheduler(source)
        self.mapper_num = self.map_scheduler.length
                      
    def start(self):
        self._process_datasource()
        self._init_task_info()
        logging.info('total map count:%d, total reduce count:%d' % (self.map_scheduler.length, self.reducer_num))
        logging.info('start mapping...')
        self.state = self.MAP
        t = threading.Thread(target=self.show_info)
        t.start()
        self.server_forever()
    
    def show_info(self):
        while True:
            logging.info('running map %d, running reduce %d', \
                len(self.map_scheduler.running), len(self.reduce_scheduler.running))
            logging.info('task progress: map %d/%d, reduce %d/%d', \
                len(self.map_scheduler.finished), self.mapper_num, len(self.reduce_scheduler.finished), self.reducer_num)
            print ''
            time.sleep(30)
