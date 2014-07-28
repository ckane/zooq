#!/usr/bin/python
#

#
# Schema for pending queue objects:
# {'task_obj': <obj id>, 'task_name': 'task name', 'pid': 999, 'priority': 'low', 'dependson': []}
#

from time import sleep
from os import waitpid, WNOHANG, fork, pipe, fdopen, close, O_NONBLOCK, remove, access
import os.path
from socket import socket, AF_UNIX, SOCK_STREAM
from select import select
from fcntl import fcntl, F_SETFL, F_SETFD, FD_CLOEXEC
import json
from ztasks import *
import ztasks.ztask_base
import sys

class ZooQ(object):
    def __init__(self, max_procs=8, heartbeat=10, socket_name='/tmp/zooq.sock'):
        self.__max_procs = max_procs
        self.__pending_queue = []
        self.__active_queue = []
        self.__shutdown = False
        self.__heartbeat = heartbeat
        self.__runq_pid = -1
        self.__pwrite = False
        self.__qread = False
        self.__listener = None
        self.__connected = []
        self.__socket_name = socket_name

    def qsize(self):
        return len(self.__pending_queue) + len(self.__active_queue)

    def waitsize(self):
        return len(self.__pending_queue)

    def sendtask(self, task_name, objid, dependson=[], priority='low'):
        self.__pwrite.write(json.dumps({'task_name': task_name, 'task_obj': objid, 'pid': -1, 'priority': priority, 'dependson': dependson}) + '\n')
        self.__pwrite.flush()

    def sendshutdown(self):
        self.__pwrite.write('shutdown\n')
        self.__pwrite.flush()

    def in_queue(self, task_name, task_obj):
        return filter(lambda a_task: a_task['task_name'] == task_name and a_task['task_obj'] == task_obj,
                      self.__pending_queue + self.__active_queue)

    def getwork(self, heartbeat=0):
        rdrs = [self.__qread] + self.__connected

        if self.__listener:
            rdrs.append(self.__listener)

        rs, ws, xs = select(rdrs, [], rdrs, heartbeat)
        for r in rs:
            if self.__listener is r:
                conn_sock, conn_addr = self.__listener.accept()
                conn_sock.setblocking(0)
                fcntl(conn_sock, F_SETFD, FD_CLOEXEC)
                self.__connected.append(conn_sock.makefile())
            else:
                job_request = r.readline()
                print("Request received: {0}".format(job_request.strip()))
                if(len(job_request) > 0):
                    if job_request.strip().lower() == 'shutdown':
                        self.__shutdown = True
                    else:
                        newtask = json.loads(job_request.strip())
                        newtask['pid'] = -1
                        if not self.in_queue(newtask['task_name'], newtask['task_obj']):
                            if newtask['priority'] == 'high':
                                self.__pending_queue.append(newtask)
                            else:
                                self.__pending_queue.insert(0, newtask)

        for x in xs:
            if x is self.__listener:
                self.__listener.close()
                self.__listener = None
            else:
                for i in xrange(0, len(self.__connected)):
                    if x is self.__connected[i]:
                        self.__connected[i].close()
                        self.__connected.pop(i)
                        break

    def cleanchildren(self):
        try:
            p_id, r_status = waitpid(-1, WNOHANG)
        except OSError as e:
            if e.errno == 10:
                p_id = 0

        while p_id != 0:
            print("Reclaimed {0}".format(p_id))
            for x in xrange(len(self.__active_queue)):
                if self.__active_queue[x]['pid'] == p_id:
                    self.__active_queue.pop(x)
                    print('Active: {0}'.format(json.dumps(self.__active_queue)))
                    break
            try:
                p_id, r_status = waitpid(-1, WNOHANG)
            except OSError as e:
                if e.errno == 10:
                    p_id = 0

    def Run(self):
        # Create a pipe between the calling process and the run-queue
        self.__qread, self.__pwrite = pipe()

        # Create a UNIX socket listener as well
        self.__runq_pid = fork()

        if self.__runq_pid != 0:
            close(self.__qread)
            self.__qread = False
            self.__pwrite = fdopen(self.__pwrite, 'w')
            return

        # Create a UNIX socket, in the run-queue process only
        self.__listener = socket(AF_UNIX, SOCK_STREAM)
        self.__listener.setblocking(0)

        try:
            remove(self.__socket_name)
        except OSError:
            if os.path.exists(self.__socket_name):
                raise

        self.__listener.bind(self.__socket_name)
        self.__listener.listen(100)

        sys.stdin.close()
        close(self.__pwrite)
        self.__pwrite = False
        self.__qread = fdopen(self.__qread)
        fcntl(self.__qread, F_SETFL, O_NONBLOCK)
        fcntl(self.__qread, F_SETFD, FD_CLOEXEC)
        fcntl(self.__listener, F_SETFD, FD_CLOEXEC)
        while not self.__shutdown:
            # Clean up any exited children
            self.cleanchildren()

            # If workers are full, or no pending work to do, then just sleep
            if len(self.__active_queue) >= self.__max_procs or len(self.__pending_queue) == 0:
                self.getwork(heartbeat=self.__heartbeat)
            else:
                while len(self.__active_queue) < self.__max_procs and len(self.__pending_queue) > 0:
                    # Attempt to migrate more tasks from the pending queue while there are pending tasks,
                    # and as long as there are available worker slots
                    nextjob = None
                    active_sigs = set(['{0}-{1}'.format(x['task_name'], x['task_obj']) for x in self.__active_queue])
                    for i in xrange(len(self.__pending_queue) - 1, -1, -1):
                        #print(self.__pending_queue[i])
                        pending_sigs = set(self.__pending_queue[i]['dependson'])
                        if len(active_sigs & pending_sigs) == 0:
                            nextjob = self.__pending_queue.pop(i)
                            break

                    if nextjob:
                        print('Submitting: {0}'.format(json.dumps(nextjob)))
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
                                print('Active: {0}'.format(json.dumps(self.__active_queue)))
                        else:
                            # In the event that the task spec referenced a non-existent task_name, display a
                            # friendly error, and discard it
                            print("Task {0} is not defined, discarding".format(nextjob['task_name']))
                    else:
                        self.cleanchildren()
                        self.getwork(0.25) # Also check for new work here, to prevent deadlocks, but check faster

        # Exit the Run-Queue
        sys.exit(0)
