#!/usr/bin/env python3
#

# Basically a base-class for implementing
# queueing for the ZooQ.
#
# This default implementation is a memory-backed volatile
# queue, relying upon Python arrays
#
class ZooQDB(object):
    def __init__(self):
        self.__pending_queue = []
        self.__active_queue = []
        pass

    def qsize(self):
        return len(self.__pending_queue) + len(self.__active_queue)

    def waitsize(self):
        return len(self.__pending_queue)

    def in_queue(self, task_name, task_obj):
        return filter(lambda a_task: a_task['task_name'] == task_name and a_task['task_obj'] == task_obj,
                      self.__pending_queue + self.__active_queue)

    def enqueue(self, task):
        if task['priority'] == 'high':
            self.__pending_queue.append(task)
        else:
            self.__pending_queue.insert(0, task)

    def reclaim(self, p_id):
        for x in range(len(self.__active_queue)):
            if self.__active_queue[x]['pid'] == p_id:
                self.__active_queue.pop(x)
                return True
        return False

    def get_active(self):
        return self.__active_queue.copy()

    def get_pending(self):
        return self.__pending_queue.copy()

    def get_all(self):
        return self.__active_queue + self.__pending_queue

    def get_alen(self):
        return len(self.__active_queue)

    def get_plen(self):
        return len(self.__pending_queue)

    def pop_next(self):
        active_sigs = set(['{0}-{1}'.format(x['task_name'],
                          x['task_obj']) for x in self.get_all()])
        nextjob = None
        for i in range(self.get_plen() - 1, -1, -1):
            pending_sigs = set(self.__pending_queue[i]['dependson'])
            if len(active_sigs & pending_sigs) == 0:
                nextjob = self.__pending_queue.pop(i)
                return nextjob

        return None

    def active_next(self, job):
        self.__active_queue.append(job)
