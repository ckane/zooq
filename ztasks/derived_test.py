#!/usr/bin/env python3
#
# This is an example task which will be automatically discovered
# by the name of 'derived_test' by ZooQ, and will be plugged in
# as a supported 'task_name'.
#
# Simply demonstrates inheritance as well as how the framework
# and discovery works for new task definitions. In this case, it
# is a modification which executes each task twice.
#
from ztasks.ztask_base import ztask_base

class derived_test(ztask_base):
    def __init__(self, objid):
        super(derived_test, self).__init__(objid)

    def dowork(self):
        # Execute underlying work action twice
        super(derived_test, self).dowork()
        super(derived_test, self).dowork()
