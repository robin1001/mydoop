import socket
import struct
import cPickle as pickle
import params
#----
from util import TcpHelper as helper

class Protocal:
    def __init__(self):
        pass
    
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
        print cmd, msg
        sock.close()
        return cmd, msg

class JobClient(Protocal):
    START = 0
    INFO = 1
    HEART = 2
    def __init__(self):
        Protocal.__init__(self)
        self.mapfn = None
        self.reducefn = None
        self.combinefn = None
    
    def _get_task_info(self):
        map_num = struct.pack('!i', params.JOB_MAP_NUM)
        self.send_command('job_start', map_num)
        #get map,reduce,and combine function
        cmd, mapfn = self.send_command('job_mapfn')
        self.mapfn = pickle.loads(mapfn)
        print self.mapfn
        
        cmd, reducefn = self.send_command('job_reducefn')
        self.reducefn = pickle.loads(reducefn)
        print self.reducefn
        
        cmd, combinefn = self.send_command('job_combinefn')
        #self.combinefn = pickle.loads(combinefn)
        
    def start(self):
        self._get_task_info()
        #self.mapfn('yes', 'no')
        
        
if __name__ == '__main__':
    client = JobClient()
    client.start()