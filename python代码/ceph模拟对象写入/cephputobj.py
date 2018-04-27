#! /usr/bin/env python
# -*- coding: UTF-8 -*-

import time
import threading
import Queue
import sys
import signal
import random
import commands
import os
import sys
import json
from rados import Rados
from rados import Error as RadosError


##
threadnum = 100
##


objectname_base="benchmark_data_node191_3235_object"
q = Queue.Queue()

def main():
    poolname =sys.argv[1]
    objects=int(sys.argv[2])
    list_all_nodes_osd_usage(poolname,objects)

def quit(signum, frame):
    print 'You choose to stop me'
    sys.exit()


class CephClusterCommand(dict):
    def __init__(self, cluster, **kwargs):
        dict.__init__(self)
        ret, buf, err = cluster.mon_command(json.dumps(kwargs), '', timeout=5)
        if ret != 0:
            self['err'] = err
        else:
            self.update(json.loads(buf))


def list_all_nodes_osd_usage(poolname,objects):
    config={'conffile': '/etc/ceph/ceph.conf', 'conf': {}}
    pglist = {}
    try:
        pg_info_get  = commands.getoutput('ceph osd pool get %s pg_num -f json 2>/dev/null' %poolname)
        pool_info=json.loads(pg_info_get)
    except:
        print "无法获取存储池信息，检查存储池是否存在"
        sys.exit()
    pg_num = int(pool_info['pg_num'])
    pool_id=pool_info['pool_id']
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print "开始写PG空列表"
    for pgname in range(pg_num):
        newpg_name=str(pool_id)+"."+str(hex(pgname)[2:])
        pglist[newpg_name]= int(0)
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print "结束写空列表"
    print "开始进行对象映射查询"

    for i in range(objects):
        q.put(i)
	##线程内开始做事情
    def add_to_list():
#        print q.empty()
        while not q.empty():
            if not q.empty():
                object=q.get()
                with Rados(**config) as cluster:
                    object=str(objectname_base)+str(object)
#                    print object
                    cluster_status = CephClusterCommand(cluster, prefix='osd map', pool=poolname,object=object,format='json')
                    object_in_pg=cluster_status['pgid']
                    pglist[object_in_pg] = pglist[object_in_pg] + 1
                q.task_done()
	##开启线程
    try:
		signal.signal(signal.SIGINT, quit)
		signal.signal(signal.SIGTERM, quit)

		for a in range(threadnum):
			t=threading.Thread(target=add_to_list,args=())
			t.setDaemon(True)
			t.start()
    except Exception, exc:
			print exc


	##事情做完了
    while not q.empty():
		time.sleep(5)
		pass

    q.join()
    print "pg_stat objects"
    for key in sorted(pglist.keys()):
        print key," ",pglist[key]

    print "结束"

if __name__ == '__main__':
    main()
