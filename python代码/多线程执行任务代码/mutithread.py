#! /usr/bin/env python
# -*- coding: UTF-8 -*-

import time
import threading
import Queue
import sys
import signal
import random

q = Queue.Queue()

def quit(signum, frame):
    print 'You choose to stop me'
    sys.exit()

##向队列里面增加要做的任务
for i in range(100):
    q.put(i)

##线程内开始做事情
def get_object():
    print q.empty()
    while not q.empty():
         time.sleep(1)
         if not q.empty():
           print q.get()
#####
#添加要做的事情在这里
####
           q.task_done()
##开启线程
try:
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)

    for a in range(5):
        t=threading.Thread(target=get_object,args=())
        t.setDaemon(True)
        t.start()
except Exception, exc:
        print exc
##事情做完了
while not q.empty():
    time.sleep(5)
    pass

q.join()
print "结束"