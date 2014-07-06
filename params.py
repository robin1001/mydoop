import os
import cPickle as pickle
'''
all the const var used in doop, and judge whether file 
or directory is exist, if not create it
'''
KB = 1024 
FS_DIR = './system/'
RPC_PORT = 8000
#read and wirte cmd tcp port
TCP_PORT = 8001
NAMENODE_IP = 'localhost'
META_FILE = FS_DIR + 'meta'
BLOCK_DIR = FS_DIR + 'blocks/' 
RAW_FILE_DIR = FS_DIR + 'raw/' 

 
MB = 1024 * KB
BLOCK_SIZE = 2 * MB
BUFFER_SIZE = 64 * KB
MEM_BUF_SIZE = 50 * MB

TASK_SERVER_PORT = 10000
TASK_SERVER_IP = 'localhost'
TASK_MAPPER_NUM = 1
TASK_MAP_MEM_BLOCK = 1 * MB

JOB_MAP_MAX = 10#one job client can start  max num maptask parallel
JOB_REDUCE_MAX = 1
MAP_LOG_FILE = FS_DIR + 'map.log'
MAP_TMP_DIR = FS_DIR + 'map_tmp/'

REDUCE_TMP_DIR = FS_DIR + 'reduce_tmp/'

def init():
    if not os.path.exists(BLOCK_DIR):
        os.makedirs(BLOCK_DIR)
    if not os.path.exists(RAW_FILE_DIR):
        os.makedirs(RAW_FILE_DIR)    
    if not os.path.exists(META_FILE):
        fid = open(META_FILE, 'wb')
        pickle.dump({}, fid, True) 
        fid.close()      