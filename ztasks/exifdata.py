#!/usr/bin/env python3
#
# Author: Coleman Kane <ckane@colemankane.org>
# This module will run "exiftool" against an artifact and will
# write results into a file named exiftool_output.csv
#
import sqlite3
import os
from subprocess import Popen, DEVNULL, PIPE

from ztasks.ztask_base import ztask_base

class exifdata(ztask_base):
    def __init__(self, objid, dir):
        super(exifdata, self).__init__(objid, dir)

    def dowork(self):
        dbconn = sqlite3.connect('{dir}/samples.sqlite'.format(dir=self.dirname()))
        cur = dbconn.cursor()
        cur.execute('SELECT `mwpath` from `samples` WHERE `mwid`=?', (self.objid(),))
        res = cur.fetchall()
        dbconn.close()
        if res:
            mwpath = res[0][0]
            mwdir = os.path.dirname(mwpath)
            outfile = open(mwdir+'/exiftool.json', 'wb')
            proc = Popen(['exiftool', '-j', mwpath],
                         stderr=DEVNULL, stdout=outfile, stdin=DEVNULL)
            proc.wait()
