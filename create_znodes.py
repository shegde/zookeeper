"""
# Shailesh Hegde

Script to create znodes
Run from ZK node or on the same network for speed
Install kazoo using pip before running 

Usage:
python create_nodes_uuid.py <ZK node IP> <# of znodes> <Path for znodes> <size of node in bytes> <seed id>

For example:
python create_nodes_uuid.py 10.5.107.37 100 /path/hello1 10 123456789

This will connect to  node 10.5.107.37, and create 100 znodes under /path/hello1, each of size 10 bytes and value
starting with seed:uuid incremented seed for each node.
"""
from kazoo.client import KazooClient
from kazoo import exceptions as k_exceptions
from random    import SystemRandom
from string    import ascii_uppercase, digits
import logging
import time
import os
import sys
from sys import argv
import uuid

zkLeader = argv[1]
zkPort = '2181'
connString = zkLeader + ':' + zkPort
connections = []
paths = []
numNodes = int(argv[2])
path = argv[3]
a = argv[4]
z = ''.join(SystemRandom().choice(ascii_uppercase + digits) for _ in range(int(a)))
seed = argv[5]
uid = str(uuid.uuid4())

def connect():
    zk = KazooClient(hosts=connString, logger=logging)
    connections.append(zk)
    zk.start()
    print "Connected"
    return zk

def createNodes(zk,path,r):
    zk.create(path+"/"+str(r), z, makepath=True)
    paths.append(path+"/"+str(r))

# not used
def printNodes(zk,path):
    if zk.exists(path):
        data, stat = zk.get(path)
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
        children = zk.get_children(path)
        print("There are %s children with names %s" % (len(children), children))

# not used.  easy delete is to rmr the said path via zkCli.sh
def deleteNodes(zk,path):
    zk.delete(path, recursive=False)
    print "Deleted node: %s" % path

if __name__ == '__main__':
    zk = connect()
    for i in range(int(seed), numNodes + int(seed) + 1):
        createNodes(zk,path,str(i) + ':' + uid)
    print "Done"
