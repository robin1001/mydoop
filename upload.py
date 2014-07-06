import time
import logging
#----
import dfsclient
import params


if __name__ == '__main__':
    params.init()
    logging.basicConfig(level=logging.INFO)
    s = time.time()
    client = dfsclient.DfsClient()
    client.upload('robin.txt')
    e = time.time()
    print 'upload down, time %f', e-s
    pass