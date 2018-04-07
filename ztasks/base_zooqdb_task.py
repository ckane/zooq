#!/usr/bin/env python3
#
# Author: Coleman Kane <ckane@colemankane.org>
# Base class for any modules utilizing the 'samples.sqlite' DB
#
import sqlite3
import os
from subprocess import Popen, DEVNULL, PIPE

from ztasks.ztask_base import ztask_base

class base_zooqdb_task(ztask_base):
    def __init__(self, objid, dir):
        super(base_zooqdb_task, self).__init__(objid, dir)
        self.__sqlite3 = '{dir}/samples.sqlite'.format(dir=self.dirname())

    def get_mw_path(self):
        dbconn = sqlite3.connect(self.__sqlite3)
        cur = dbconn.cursor()
        cur.execute('SELECT `mwpath` from `samples` WHERE `mwid`=?', (self.objid(),))
        res = cur.fetchall()
        dbconn.close()
        if res:
            return res[0][0]

        # On error, return None
        return None

    def dowork(self):
        pass
