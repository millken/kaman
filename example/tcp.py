#!/usr/bin/python
 
import socket
import time
 
HOST = '127.0.0.1'    
PORT = 9999  
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
i=1
while i <= 10000000:
	#s.send(time.ctime() + "\n")
	s.send( str(i) + "\n")
	i += 1
	time.sleep(0.1)
	
#s.close()
 
