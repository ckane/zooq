#!/usr/bin/env python3
from os import getpid
from time import sleep

class ztask_base(object):
    def objid(self):
        return self.__objid

    def dirname(self):
        return self.__dir

    def __init__(self, objid, dir):
        self.__objid = objid
        self.__dir = dir

    def dowork(self):
        print("Doing work, waiting {0} seconds...".format(self.__objid))
        sleep(float(self.__objid))
        print("Done waiting {0} seconds...".format(self.__objid))
