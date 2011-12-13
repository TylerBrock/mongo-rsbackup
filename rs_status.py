#!/usr/bin/env python

import pymongo
import argparse

host = 'localhost'
port = 27017

parser = argparse.ArgumentParser(description='Backup a Replica Set')
parser.add_argument('--host', help='host to backup')
args = parser.parse_args()

if args.host:
    host_args = args.host.split(':')
    host = host_args[0]
    if len(host_args) > 1:
        port = host_args[1]

connection = pymongo.Connection(':'.join([host,str(port)]))

repl_status = connection.admin.command({ "replSetGetStatus" : 1 })

for member in repl_status["members"]:
    print member["name"], member["state"]

# Ask member to step down: {replSetStepDown : 1}

#dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else None
#json.dumps(datetime.datetime.now(), default=dthandler)