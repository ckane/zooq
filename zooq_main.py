#!/usr/bin/env python3
#
#
from zooq import ZooQ
from zooqdb_sqlite import ZooQDB_SQLite
import time
from argparse import ArgumentParser

ap = ArgumentParser(description="Example main service for a ZooQ")
ap.add_argument('-d', '--dir', required=False, default='.', action='store',
                help='Root folder for ZooQ')
ap.add_argument('-n', '--numprocs', required=False, default=4, action='store',
                type=int, help='Number of parallel processes')
ap.add_argument('-f', '--foreground', required=False, default=False, action='store_true',
                type=bool, help='Remain in foreground')
args = ap.parse_args()

db = ZooQDB_SQLite('{dir}/zooqdb.sqlite'.format(dir=args.dir))

z = ZooQ(max_procs=args.numprocs, db=db, socket_name='{dir}/zooq.sock'.format(dir=args.dir),
         dirname=args.dir)
z.Run()

while args.foreground and True:
    time.sleep(1)
