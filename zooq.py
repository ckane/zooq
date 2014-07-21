#!/usr/bin/python
#

#
# Schema for pending queue objects:
# {'task_obj': <obj id>, 'task_name': 'task name', 'pid': 999, 'priority': 'low'}
#

from time import sleep
from os import waitpid, WNOHANG, fork, pipe, fdopen, close, O_NONBLOCK
from select import select
from fcntl import fcntl, F_SETFL
import json
from ztasks import *
import ztasks.ztask_base
import sys

class ZooQ(object):
    def __init__(self, max_procs=8, heartbeat=10):
        self.__max_procs = max_procs
        self.__pending_queue = []
        self.__active_queue = []
        self.__shutdown = False
        self.__heartbeat = heartbeat
        self.__runq_pid = -1
        self.__pwrite = False
        self.__qread = False

    def qsize(self):
        return len(self.__pending_queue) + len(self.__active_queue)

    def waitsize(self):
        return len(self.__pending_queue)

    def sendtask(self, task_name, objid, priority='low'):
        self.__pwrite.write(json.dumps({'task_name': task_name, 'task_obj': objid, 'pid': -1, 'priority': priority}) + '\n')
        self.__pwrite.flush()

    def sendshutdown(self):
        self.__pwrite.write('shutdown\n')
        self.__pwrite.flush()

    def Run(self):
        self.__qread, self.__pwrite = pipe()
        self.__runq_pid = fork()

        if self.__runq_pid != 0:
            close(self.__qread)
            self.__qread = False
            self.__pwrite = fdopen(self.__pwrite, 'w')
            return

        sys.stdin.close()
        close(self.__pwrite)
        self.__pwrite = False
        self.__qread = fdopen(self.__qread)
        fcntl(self.__qread, F_SETFL, O_NONBLOCK)
        while not self.__shutdown:
            # Clean up any exited children
            try:
                p_id, r_status = waitpid(-1, WNOHANG)
            except OSError as e:
                if e.errno == 10:
                    p_id = 0

            while p_id != 0:
                for x in xrange(len(self.__active_queue)):
                    if self.__active_queue[x]['pid'] == p_id:
                        self.__active_queue.pop(x)
                        break

            # If workers are full, or no pending work to do, then just sleep
            if len(self.__active_queue) >= self.__max_procs or len(self.__pending_queue) == 0:
                rs, ws, xs = select([self.__qread], [], [], self.__heartbeat)
                if len(rs) > 0:
                    job_request = self.__qread.readline()
                    print("Request received: {0}".format(job_request.strip()))
                    if(len(job_request) > 0):
                        if job_request.strip().lower() == 'shutdown':
                            self.__shutdown = True
                        else:
                            newtask = json.loads(job_request.strip())
                            newtask['pid'] = -1
                            if newtask['priority'] == 'high':
                                self.__pending_queue.append(newtask)
                            else:
                                self.__pending_queue.insert(0, newtask)
            else:
                while len(self.__active_queue) < self.__max_procs and len(self.__pending_queue) > 0:
                    # Attempt to migrate more tasks from the pending queue while there are pending tasks,
                    # and as long as there are available worker slots
                    nextjob = self.__pending_queue.pop()
                    print('Available: {0}'.format(json.dumps(dir(ztasks))))
                    if nextjob['task_name'] in dir(ztasks):
                        task_instance = eval('ztasks.{0}.{0}'.format(nextjob['task_name']))(nextjob['task_obj'])
                        nextjob['pid'] = fork()

                        if nextjob['pid'] == 0:
                            # We are executing as the child
                            task_instance.dowork()
                            sys.exit(0)
                        else:
                            # We are executing as the parent
                            self.__active_queue.append(nextjob)
                    else:
                        # In the event that the task spec referenced a non-existent task_name, display a
                        # friendly error, and discard it
                        print("Task {0} is not defined, discarding".format(nextjob['task_name']))

