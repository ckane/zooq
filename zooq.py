#!/usr/bin/python
#

#
# Schema for pending queue objects:
# {'task_obj': <obj id>, 'task_name': 'task name', 'pid': 999}
#

from time import sleep
from os import waitpid, WNOHANG, fork
from ztasks import ztask_base
from ztasks import *

class ZooQ(object):
    def __init__(self, max_procs=8, heartbeat=10):
        self.__max_procs = max_procs
        self.__pending_queue = []
        self.__active_queue = []
        self.__shutdown = False
        self.__heartbeat = heartbeat

    def qsize(self):
        return len(self.__pending_queue) + len(self.__active_queue)

    def waitsize(self):
        return len(self.__pending_queue)

    def Run(self):
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
                sleep(self.__heartbeat)
            else:
                while len(self.__active_queue) < self.__max_procs and len(self.__pending_queue) > 0:
                    # Attempt to migrate more tasks from the pending queue while there are pending tasks,
                    # and as long as there are available worker slots
                    nextjob = self.__pending_queue.pop()
                    if nextjob['task_name'] in dir():
                        task_instance = eval('{0}'.format(nextjob['task_name']))(nextjob['task_obj'])
                        nextjob['pid'] = os.fork()

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
