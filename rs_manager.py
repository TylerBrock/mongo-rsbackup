#!/usr/bin/env python

"""
Manage a MongoDB replica set. rs_manager.py -h for usage help.
"""

import argparse
import json
import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time

import pymongo

home = os.environ.get('HOME')
default_dbpath = os.path.join(home, 'data', 'pymongo_replica_set')
dbpath = os.environ.get('DBPATH', default_dbpath)

default_logpath = os.path.join(home, 'log', 'pymongo_replica_set')
logpath = os.environ.get('LOGPATH', default_logpath)

hostname = socket.gethostname()
port = int(os.environ.get('DBPORT', 27017))
mongod = os.environ.get('MONGOD', 'mongod')
set_name = os.environ.get('SETNAME', 'repl0')

nodes = {}

def kill_members(members, sig=2):
    for member in members:
        try:
            pid = nodes[member]['pid']
            logging.info('Killing pid %s' % pid)
            # Not sure if cygwin makes sense here...
            if sys.platform in ('win32', 'cygwin'):
                os.kill(pid, signal.CTRL_C_EVENT)
            else:
                os.kill(pid, sig)

            # Make sure it's dead
            os.waitpid(pid, 0)
            logging.info('Killed.')
        except OSError:
            pass # already dead

        del nodes[member]

def kill_all_members():
    kill_members(nodes.keys())

def wait_for(proc, port):
    trys = 0
    while proc.poll() is None and trys < 40: # ~10 seconds
        trys += 1
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect((hostname, port))
                return True
            except (IOError, socket.error):
                time.sleep(0.25)
        finally:
            s.close()

    kill_all_members()
    return False

def start_replica_set(num_members=3, with_arbiter=True, fresh=False):
    if fresh:
        try:
            shutil.rmtree(dbpath)
            shutil.rmtree(logpath)
        except OSError:
            # dbpath doesn't exist yet
            pass

    start_time = time.time()
    members = []
    for i in xrange(num_members):
        cur_port = port + i
        host = '%s:%d' % (hostname, cur_port)
        members.append({'_id': i, 'host': host})
        path = os.path.join(dbpath, 'db' + str(i))
        if not os.path.exists(path):
            os.makedirs(path)
        member_logpath = os.path.join(logpath, 'db' + str(i) + '.log')
        if not os.path.exists(os.path.dirname(member_logpath)):
            os.makedirs(os.path.dirname(member_logpath))
        cmd = [mongod,
               '--dbpath', path,
               '--port', str(cur_port),
               '--replSet', set_name,
               '--logpath', member_logpath,
               '--journal',
               # Various attempts to make startup faster on Mac by limiting
               # the size of files created at startup
               '--oplogSize', '5', # 5MB oplog, not 1G, to speed startup
               '--nohttpinterface',
               '--noprealloc',
               '--smallfiles',
               '--nssize', '1',
               '--fastsync',
        ]
        logging.info('Starting %s' % ' '.join(cmd))
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        nodes[host] = {
            'pid': proc.pid,
            'cmd': cmd,
            'logpath': member_logpath,
            'host': hostname,
            'port': cur_port,
        }
        res = wait_for(proc, cur_port)
        if not res:
            return None
    if with_arbiter:
        members[-1]['arbiterOnly'] = True
    config = {'_id': set_name, 'members': members}
    primary = members[0]['host']
    c = pymongo.Connection(primary)
    logging.info('Initiating replica set....')
    c.admin.command('replSetInitiate', config)

    # Wait for all members to come online
    expected_secondaries = num_members - 1
    if with_arbiter: expected_secondaries -= 1
    expected_arbiters = 1 if with_arbiter else 0
    while True:
        time.sleep(2)

        try:
            if (
                len(get_primary()) == 1 and
                len(get_secondaries()) == expected_secondaries and
                len(get_arbiters()) == expected_arbiters
            ):
                break
        except pymongo.errors.AutoReconnect:
            # Keep waiting
            pass
    logging.debug('Started %s members in %s seconds' % (
        num_members, int(time.time() - start_time)
    ))

    return primary, set_name

def get_members_in_state(state):
    c = pymongo.Connection(nodes.keys())
    try:
        status = c.admin.command('replSetGetStatus')
        members = status['members']
        return [k['name'] for k in members if k['state'] == state]
    except pymongo.errors.OperationFailure, e:
        logging.warning(e)
        return []

def get_primary():
    return get_members_in_state(1)

def get_random_secondary():
    secondaries = get_members_in_state(2)
    if len(secondaries):
        return [secondaries[random.randrange(0, len(secondaries))]]
    return secondaries

def get_secondaries():
    return get_members_in_state(2)

def get_arbiters():
    return get_members_in_state(7)

def kill_primary():
    primary = get_primary()
    kill_members(primary)
    return primary

def kill_secondary():
    secondary = get_random_secondary()
    kill_members(secondary)
    return secondary

def kill_all_secondaries():
    secondaries = get_all_secondaries()
    kill_members(secondaries)
    return secondaries

def stepdown_primary():
    primary = get_primary()
    if primary:
        c = pymongo.Connection(primary)
        c.admin.command('replSetStepDown')

def restart_members(members):
    restarted = []
    for member in members:
        cmd = nodes[member]['cmd']
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        nodes[member]['pid'] = proc.pid
        res = wait_for(proc, int(member.split(':')[1]))
        if res:
            restarted.append(member)
    return restarted

def connect(node):
    """
    Connect directly to a member of the replica set
    @param node:  An entry in the global nodes dictionary
    @return:      A pymongo.Connection
    """



def setup_logging(level=logging.DEBUG):
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.info('Logger up.')

def load_nodes():
    global nodes
    logging.info('Loading node info from file....')
    try:
        nodes = json.load(open('nodes.json'))
        logging.info('Loaded.')
    except IOError:
        logging.info('No prior node info')
    except ValueError as e:
        logging.exception('Loading node info')
        logging.info('Continuing....')

def save_nodes():
    logging.info('Saving node info to file....')
    with open('nodes.json', 'w') as f:
        json.dump(nodes, fp=f, indent=4)
        f.write('\n')
    logging.info('Saved.')

def parse_args():
    parser = argparse.ArgumentParser(description='Manage a MongoDB replica set')
    parser.add_argument('command', choices=['start', 'stop'])
    parser.add_argument('-n', dest='n', type=int, help='Number of nodes')
    parser.add_argument('--arbiter', action='store_true', default=False,
        help='Whether to make one of the secondaries an arbiter',
    )
    parser.add_argument('--verbose', action='store_true', default=False,
        help="Debug-level logging",
    )
    return parser.parse_args()

def main(args):
    setup_logging(logging.DEBUG if args.verbose else logging.INFO)
    load_nodes()
    if args.command == 'start':
        if not args.n:
            raise ValueError("Must specify -n with command 'start'")

        start_replica_set(
            num_members=args.n,
            with_arbiter=args.arbiter,
            fresh=True,
        )
    elif args.command == 'stop':
        kill_all_members()
    else:
        raise ValueError("Unrecognized command '%s'" % args.command)

    save_nodes()

if __name__ == '__main__':
    main(parse_args())
