zooq
====

Queue for MWZoo (and maybe other things too)

- Tasks discovered in ztasks/ subfolder
- Implement "zooq" class implementation, to be imported into other projects and extended
- Track tasks by creating unique id's of "taskname-objid" to track "task ids", utilize this to identify and de-dupe task submissions
- Support queue with low-pri FIFO insertion, and high-pri LIFO insertion characteristics
- Child tasks inherit insertion characteristics of parent task
- Don't support preemption
- Utilize fork-run-exit model (for now)
