#coding: utf8

import itertools
from socket import * 
from time import ctime
import time

class LineFile(object):
    def __init__(self, fn):
        self.f = open(fn, "r")

    def __del__(self):
        self.f.close()

    def __iter__(self):
        return self

    def next(self):
        while True:
            l = self.f.readline()
            if not l:
                raise StopIteration()
            else:
                l = l.strip(" ").strip("\n")
                if l == "":
                    continue
                else:
                    return l

def push_filestream(filename):
    return LineFile(filename)

def stream_server(source, total, BATCH_SIZE = 2, HOST = '127.0.0.1', PORT = 9999):
    ADDR = (HOST, PORT)
    BUFSIZE = 1024
    tcpSvrSock = socket(AF_INET, SOCK_STREAM)
    tcpSvrSock.bind(ADDR)
    tcpSvrSock.listen(5) # backlog
    while True:
        print 'wating for connection...'
        tcpCliSock, addr = tcpSvrSock.accept()
        print 'connection accepted, start data streaming...'

        percentile, bi = total / 100, 0

        for (i, item) in itertools.izip(itertools.count(0), source):
            bi += 1
            if i % percentile == 0:
                print "completing percent %d " % int(i / percentile)
            item = item.strip("\n").strip(" ").split(",")
            data = "{},{},{},{}".format(i, item[0], item[1], item[2])
            tcpCliSock.send(data.decode('utf8') + "\n")

            if bi >= BATCH_SIZE:
                time.sleep(1)

        print "data transmition succeed, now wait peer disconnecting"
        data = tcpCliSock.recv(BUFSIZE)  
        if not data:
            print "connection closed..."
            break  
    # nerver do active close
    # tcpSvrSock.close()  

if __name__ == '__main__':
    ITEMC = 100
    print "querying the top %d items from file" % ITEMC
    source = push_filestream("part.txt")
    
    stream_server(source, ITEMC)
