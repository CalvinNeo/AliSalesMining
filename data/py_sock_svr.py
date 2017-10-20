# coding: utf8

from socket import *  
from time import ctime  
  
HOST = '127.0.0.1'  
PORT = 21567  
BUFSIZE = 1024  
ADDR = (HOST,PORT)  
  
tcpSvrSock = socket(AF_INET, SOCK_STREAM)  
tcpSvrSock.bind(ADDR)  
tcpSvrSock.listen(5)  
  
while True:  
    print 'wating for connection...'  
    tcpCliSock,addr = tcpSvrSock.accept()  
    while True:  
        data = tcpCliSock.recv(BUFSIZE)  
        print ">", data
        if not data:  
            break  
        tcpCliSock.send('[%s] %s' % (ctime(),data))  
    tcpCliSock.close() 
  
          
tcpSvrSock.close()  