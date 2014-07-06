'''
robin1001 2013-8-13
1.if you want modify the defalut params, go to params.py
2.two server:namenode and task server, in practice, namenode 
  is persistent progress,and task server is for dispatch a task
3.if you want to deal with a rather big file in this framework,
    you should do like this, refer to upload.py
      client = dfsclient.DfsClient()
      client.upload('local file')
      after successful upload you can use it in next as s.dfsfile='your file'
4.you can modify the reducer like(task.reducer_num = n), default 1
5.yet you can deal memory data like: s.datasource = your datasource
6.have fun with it
'''
import logging
#---
import task
import params
import dfsclient


def mapfn(k,v):
    for x in v.split():
        yield  x, 1

def reducefn(k,v):
    return sum(v)

if __name__ == '__main__':
    params.init()
    logging.basicConfig(level=logging.INFO)
    s = task.Task()
    s.dfsfile ='robin.txt' #a dfsfile
    s.mapfn = mapfn
    s.reducefn = reducefn
    s.combinefn = reducefn
    s.start()
