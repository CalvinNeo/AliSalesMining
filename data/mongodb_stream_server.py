#coding: utf8

from pymongo import *
import itertools
from socket import * 
from time import ctime
import time

def push_dbstream(connection):
    alidb = connection.alidb
    pay_table = alidb.pay
    res = pay_table.find().sort("ts")
    return res

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

            data = "{},{},{},{}".format(i, item['uid'], item['sid'], item['ts'])
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
    connection = MongoClient("mongodb://localhost:27017/")
    # clean_db(connection)
    ITEMC = 9999
    print "querying the top %d items from db" % ITEMC
    source = push_dbstream(connection)

    # f = open("datatttt.txt", "w")
    # for item in source:
    #     f.write("{} {} {}\n".format(item['uid'], item['sid'], item['ts']))
    # f.close()
    
    stream_server(source, ITEMC)
