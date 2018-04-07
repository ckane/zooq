#!/usr/bin/env python3
#
from socket import socket, AF_UNIX, SOCK_STREAM
import json
import sys
import shutil
import sqlite3
import os
from hashlib import sha256
from argparse import ArgumentParser

ap = ArgumentParser(description="ZooQ Submission Script")
ap.add_argument('-f', '--filename', required=True, type=str, action='store', help='File name to submit')
ap.add_argument('-d', '--dirname', required=True, type=str, action='store', help='Folder to organize it within')
ap.add_argument('-r', '--root', required=True, type=str, action='store', help='ZooQ root folder')
args = ap.parse_args()

# Compute MD5
hasher = sha256()
hasher.update(open(args.filename, 'rb').read())
malsha256 = hasher.hexdigest()

# Base file name
basefile = os.path.basename(args.filename)

# Connect to DB
dbconn = sqlite3.connect('{dir}/samples.sqlite'.format(dir=args.root))
cur = dbconn.cursor()
cur.execute('SELECT `mwpath` from `samples` WHERE `mwid`=?', (malsha256,))
res = cur.fetchall()
if res:
    dbconn.close()
    print("Malware {sha256} already in Zoo".format(sha256=malsha256))
    sys.exit(0)

cur.close()

# First, need to create new folder for the sample, and store
mod_analysis_folder = basefile.replace('.', '_')
if not os.access('{root}/{dir}'.format(root=args.root, dir=args.dirname), os.X_OK):
    os.mkdir('{root}/{dir}'.format(root=args.root, dir=args.dirname), mode=0o775)

mod_analysis_folder_base = mod_analysis_folder
iters = 0
while os.access('{root}/{dir}/{f}'.format(root=args.root, dir=args.dirname, f=mod_analysis_folder), os.F_OK):
    iters = iters + 1
    mod_analysis_folder = mod_analysis_folder_base + '_{c}'.format(c=iters)

if not os.access('{root}/{dir}/{f}'.format(root=args.root, dir=args.dirname, f=mod_analysis_folder), os.F_OK):
    os.mkdir('{root}/{dir}/{f}'.format(root=args.root, dir=args.dirname, f=mod_analysis_folder), mode=0o775)

shutil.copy(args.filename, '{root}/{dir}/{f}/{fn}'.format(root=args.root, dir=args.dirname,
                                                          f=mod_analysis_folder, fn=basefile))

cur = dbconn.cursor()
cur.execute('INSERT INTO `samples` (`mwpath`,`mwid`) VALUES (?,?)', ('{root}/{dir}/{f}/{fn}'.format(root=args.root,
                                                                                             dir=args.dirname,
                                                                                             f=mod_analysis_folder,
                                                                                             fn=basefile),malsha256,))
dbconn.commit()
cur.close()

# Signature describing what actions (modules) to execute
data = {'task_obj': malsha256, 'task_name': 'exifdata', 'pid': -1,
         'priority': 'low', 'dependson': []}

s = socket(AF_UNIX, SOCK_STREAM)
s.connect('{dir}/zooq.sock'.format(dir=args.root))
s.send(bytes(json.dumps(data) + '\n', 'utf-8'))
