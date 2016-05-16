#!/usr/bin/python
#
# SQLite3 implementation of ZooQ
#
# This provides a persistent queue, that can be used to facilitate
# restartable ZooQ implementations
#
from zooqdb import ZooQDB
import sqlite3

# TODO - Queries below have duplicated rows for when multiple depends_on exist.
#        Need to de-dupe in queries
class ZooQDB_SQLite(ZooQDB):
    def __init__(self, dbname):
        self.__dbconn = sqlite3.connect(dbname)
        self.__dbconn.execute("""CREATE TABLE IF NOT EXISTS `zooq` (`task_name` TEXT NOT NULL,
                                                                    `priority` INTEGER NOT NULL,
                                                                    `depends_on` TEXT,
                                                                    `pid` INTEGER,
                                                                    `task_obj` TEXT,
                                                                    PRIMARY KEY(`task_name`,`depends_on`,`task_obj`))""")
        curs = self.__dbconn.cursor()
        curs.execute('UPDATE `zooq` SET `priority`=0,`pid`=NULL WHERE `pid` IS NOT NULL')
        self.__dbconn.commit()
        curs.close()

    def qsize(self):
        curs = self.__dbconn.cursor()
        curs.execute('SELECT DISTINCT `task_name`,`task_obj` FROM `zooq`')
        res = curs.fetchall()
        curs.close()
        return len(res)

    def waitsize(self):
        curs = self.__dbconn.cursor()
        # When PID is NULL, the task is in pending queue
        curs.execute('SELECT DISTINCT `task_name`,`task_obj` FROM `zooq` WHERE `pid` IS NULL')
        res = curs.fetchall()
        curs.close()
        return len(res)

    def in_queue(self, task_name, task_obj):
        curs = self.__dbconn.cursor()
        curs.execute('''SELECT DISTINCT `task_name`,`task_obj`
                        FROM `zooq` WHERE `task_name` = ? AND `task_obj` = ?''', (task_name, task_obj))
        res = curs.fetchall()
        curs.close()
        return len(res) > 0

    def enqueue(self, task):
        curs = self.__dbconn.cursor()
        pri = 1
        if task['priority'] == 'high':
            pri = 0
        if len(task['dependson']) == 0:
            curs.execute('INSERT INTO `zooq` (`task_name`,`priority`,`task_obj`) VALUES (?,?,?)',
                         (task['task_name'], pri, task['task_obj']))
        for dep in task['dependson']:
            curs.execute('INSERT INTO `zooq` (`task_name`,`priority`,`depends_on`,`task_obj`) VALUES (?,?,?,?)',
                         (task['task_name'], pri, dep, task['task_obj']))
        self.__dbconn.commit()
        curs.close()

    def reclaim(self, p_id):
        curs = self.__dbconn.cursor()
        curs.execute('DELETE FROM `zooq` WHERE `pid`=?', (p_id,))
        rc = curs.rowcount
        self.__dbconn.commit()
        curs.close()
        return rc > 0

    def get_tasks(self, pending=False):
        query = 'SELECT * FROM `zooq` WHERE `pid` IS NOT NULL GROUP BY `task_name`,`task_obj`'
        if pending:
            query = 'SELECT * FROM `zooq` WHERE `pid` IS NULL GROUP BY `task_name`,`task_obj`'
        activeq = []
        active_item = {}
        curs = self.__dbconn.cursor()
        cur_task_name = ''
        cur_task_obj = ''
        for row in curs.execute(query):
            pri = 'low'
            if row[1] == 0:
                pri = 'high'
            if row[0] != cur_task_name or row[4] != cur_task_obj:
                if len(cur_task_name) > 0 and len(cur_task_obj) > 0:
                    if active_item:
                        active_item['dependson'] = filter(lambda x: x != None, active_item['dependson'])
                    activeq.append(active_item)

                active_item = {'task_name': row[0], 'priority': pri,
                               'dependson': [row[2]],
                               'pid': -1 if pending else row[3],
                               'task_obj': row[4]}
                cur_task_name = active_item['task_name']
                cur_task_obj = active_item['task_obj']
            else:
                active_item['dependson'].append(row[2])

        if active_item:
            active_item['dependson'] = filter(lambda x: x != None, active_item['dependson'])
            activeq.append(active_item)
        curs.close()
        return activeq

    def get_active(self):
        return self.get_tasks(pending=False)

    def get_pending(self):
        return self.get_tasks(pending=True)

    def get_all(self):
        return self.get_active() + self.get_pending()

    def get_alen(self):
        curs = self.__dbconn.cursor()
        curs.execute('SELECT DISTINCT `task_name`,`task_obj` FROM `zooq` WHERE `pid` IS NOT NULL')
        res = curs.fetchall()
        curs.close()
        return len(res)

    def get_plen(self):
        curs = self.__dbconn.cursor()
        curs.execute('SELECT DISTINCT `task_name`,`task_obj` FROM `zooq` WHERE `pid` IS NULL')
        res = curs.fetchall()
        curs.close()
        return len(res)

    def pop_next(self):
        active_sigs = set(['{0}-{1}'.format(x['task_name'],
                          x['task_obj']) for x in self.get_all()])
        nextjob = None
        pqueue = self.get_pending()
        for i in xrange(len(pqueue) - 1, -1, -1):
            pending_sigs = set(pqueue[i]['dependson'])
            if len(active_sigs & pending_sigs) == 0:
                nextjob = pqueue[i]
                curs = self.__dbconn.cursor()
                curs.execute('DELETE FROM `zooq` WHERE `task_name`=? AND `task_obj`=?',
                            (nextjob['task_name'], nextjob['task_obj']))
                self.__dbconn.commit()
                curs.close()
                return nextjob

        return None

    def active_next(self, task):
        pri = 1
        if task['priority'] == 'high':
            pri = 0
        if not task['dependson']:
            self.__dbconn.execute('INSERT INTO `zooq` (`task_name`,`priority`,`pid`,`task_obj`) VALUES (?,?,?,?)',
                                  (task['task_name'], pri, task['pid'], task['task_obj']))
        else:
            for d in task['dependson']:
                self.__dbconn.execute('INSERT INTO `zooq` (`task_name`,`priority`,`pid`,`depends_on`,`task_obj`) VALUES (?,?,?,?,?)',
                                      (task['task_name'], pri, task['pid'], d, task['task_obj']))
        self.__dbconn.commit()
