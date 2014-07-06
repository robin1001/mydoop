import os,sys
import time
import operator
import cPickle as pickle
import logging
import heapq
#---
import params
import file_util

class MapOutBuffer:
    def __init__(self, id, reduce_num, combinerfn):
        self.id = id
        self.RN = reduce_num
        #double buffer
        self.mem_buf= []
        self.record = {}
        self.combinefn=combinerfn

        for i in range(reduce_num):
            self.record[i] = []
        self.tmp_dir = params.MAP_TMP_DIR 
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
            
    def write(self, kvpair):
        if sys.getsizeof(self.mem_buf) >= params.MEM_BUF_SIZE:
            logging.debug("mem_buf is full")
            self._sort()
            self._write_disk()
               
        key,value = kvpair
        reduce_id = hash(key) % self.RN 
        item =(key, value, reduce_id)
        self.mem_buf.append(item)
      
    def _write_disk(self):   
        #write to file now
        pre_id = -1 #key, value, id
        fid = None
        start, end =0, 0
        #for--------------------------------------#
        for item in self.mem_buf:
            key, values, id = item
            if id != pre_id:
                if fid is not None:
                    end = fid.tell()
                    fid.close()
                    self.record[pre_id].append((start, end))
                
                path = self.tmp_dir + '/' + ('map%d_%d.tmp' % (self.id, id))
                fid = open(path, 'ab')
                logging.debug('write file %s' % path)
                length = len(self.record[id])
                if length > 0:
                    s, start = self.record[id][length - 1]
                    fid.seek(start)
                else:
                    start = 0
                pre_id = id             
            pickle.dump((key, values), fid, True)
        #for--------------------------------------#
        end = fid.tell()
        fid.close()
        self.record[pre_id].append((start, end))
        del self.mem_buf[:]                  
                  
    def _sort(self):
        self.mem_buf.sort(key = operator.itemgetter(2,0))
        #classify according to key
        tmpbuf=[]
        pre_key, pre_id= self.mem_buf[0][0], self.mem_buf[0][2]
        values = [] 
        for item in self.mem_buf:
            key, val, id = item
            if pre_key != key:
                it = pre_key, values, pre_id                     
                tmpbuf.append(it)
                values =[]
            pre_key, pre_id = key, id
            values.append(val)
            
        it = pre_key, values, pre_id                      
        tmpbuf.append(it)
        
        del self.mem_buf[:]
        self.mem_buf = tmpbuf   

    def merge_all(self): 
        #write left in mem buffer to file
        self._sort()
        self._write_disk()
        for k in self.record:
            self.merge_one(k)
        logging.debug('merge all down')
    
    def merge_one(self, id):
        path = self.tmp_dir + '/' + ('map%d_%d.tmp' % (self.id, id))
        fid = open(path, 'rb')
        seg_name = self.tmp_dir + '/' + ('map%d_%d.seg' % (self.id, id))
        outfile = open(seg_name, 'wb')
        
        txt_name = self.tmp_dir + '/' + ('map%d_%d.txt' % (self.id, id))
        txtfile = open(txt_name, 'w')
        
        total = self.record[id]
        heap = []
        for start_end in total:
            start, end = start_end
            fid.seek(start)
            item = pickle.load(fid)
            start = fid.tell()
            ele = item, (start, end)
            heap.append(ele)
            
        heapq.heapify(heap)
        #while--------------------------------------------#
        pre_key = None
        values = []
        while True:
            try:
                (key, val),(start,end) = heapq.heappop(heap)
                if pre_key !=None and pre_key != key:
                    if self.combinefn is not None:
                        combine_value = self.combinefn(pre_key, values)
                        pickle.dump((pre_key, combine_value), outfile, True)
                        line = str(pre_key) + ','  +str(combine_value) + '\n'
                        txtfile.write(line)
                    else:
                        print pre_key, values
                        line = str(pre_key) + ','  +' '.join([str(i) for i in values]) + '\n'
                        txtfile.write(line)
                        pickle.dump((pre_key, values), outfile, True)
                    values=[]
                #add new to heap
                if start != end:
                    fid.seek(start)
                    item_read = pickle.load(fid)
                    start = fid.tell()
                    ele = item_read, (start, end)
                    heapq.heappush(heap, ele)
                
                pre_key = key
                values.extend(val)
            except IndexError: 
                break
            except Exception, ex:
                logging.error(ex)
                break
            
        values.extend(val)
        if self.combinefn is not None:
            combine_value = self.combinefn(pre_key, values)
            pickle.dump((pre_key, combine_value), outfile, True)
            line = str(pre_key) + ','  +str(combine_value) + '\n'
            txtfile.write(line)
        else:
            pickle.dump((pre_key, values), outfile, True)
            line = str(pre_key) + ','  +' '.join([str(i) for i in values]) + '\n'
            txtfile.write(line)
        #while--------------------------------------------#   
        fid.close()
        outfile.close()
        txtfile.close()
        logging.debug('merge %d down' % id)
        
    def delete_tmp_file(self):
        if  os.path.exists(self.tmp_dir):
            file_util.rm_dir(self.tmp_dir)        
