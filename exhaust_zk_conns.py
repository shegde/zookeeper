"""
Script to create 1000s of connections to Zookeeper node
Uses multiple processes to create these connections in parallel

maxClientCnxns - Limits the number of concurrent connections (at the socket level) 
that a single client, identified by IP address, 
may make to a single member of the ZooKeeper ensemble. Set to 2000 on RES cluster

Use 'pip install kazoo' for client library used by this script

Set numConnections and numProcs accordingly as their product is the total number of
connections attempted to ZK node

Modify ZK IP/port as needed below.

Useful links:
http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html
https://kazoo.readthedocs.org/en/latest/basic_usage.html#connection-handling

"""

from kazoo.client import KazooClient
from multiprocessing import Process
import logging
import time
import os

#logging.basicConfig(level=logging.DEBUG)

zkLeader = '10.5.107.38'
zkPort = '2181'
connString = zkLeader + ':' + zkPort
connections = []
numConnections = 192
numProcs = 10
sleepTime = 300

def create():
    pid = os.getpid()
    print '%s CONNECTING TO: %s, STARTING %s' % (pid, connString, numConnections)
    for n in range(1, numConnections+1):
        zk = KazooClient(hosts=connString, logger=logging, timeout=sleepTime)
        connections.append(zk)
        zk.start()
        if n != 0 and n % 200 == 0:
            print 'CONNECTED %s' % str(n)
    
    wait()

def wait():
    print 'WAITING ...'
    time.sleep(sleepTime)
    
def close():
    print 'STOPPING'
    for conn in connections:
        conn.stop()
        conn.close()
    print 'STOPPED'

if __name__ == '__main__':
    jobs = []
    for i in range(0, numProcs):
        p = Process(target=create)
        jobs.append(p)
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()
    
