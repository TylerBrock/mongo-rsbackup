#!/usr/bin/env python

"""
Backup a member of a replica set
"""

import argparse
import os
import sys
import subprocess
import logging
import tarfile
import copy

import pymongo

# Local defaults
cwd = os.getcwd()
backup_dir = os.path.join(cwd, 'backups')

# Remote defaults
source_dir = os.path.join('data','db')

# Global defaults
compression_level = 1

class ReplSet:
    def __init__(self):
        

class Host:
    def __init__(self, hostname='localhost', port=27017, user=None, 
        password=None, sshuser=None, sshpass=None, sshport=22):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.sshpass = sshpass
        self.sshport = sshport

        # must connect as root user for file permissions
        self.sshuser = 'root'

    def __str__(self):
        return self.hostname + ":" + str(self.port)

    def connect(self):
        self.connection = pymongo.Connection(self.mongo_uri())
        self.primary = self.getPrimary()
        self.pri_con = pymongo.Connection(self.primary)
    
    def adminCommand(self, command, value=1, primary=False):
        if primary:
            return self.pri_con.admin.command(command, value)
        else:
            return self.connection.admin.command(command, value)

    def replStatus(self):
        return self.adminCommand("replSetGetStatus")

    def replConfig(self):
        return self.connection.local.system.replset.find_one()

    def isSecondary(self):
        return self.adminCommand('isMaster')['secondary']

    def getPrimary(self):
        return self.adminCommand('isMaster')['primary']

    def getMe(self):
        return self.adminCommand('isMaster')['me']

    def replRemove(self):
        logging.debug('Removing {host} from replica set'.format(host=self))
        self.old_config = self.replConfig()
        old_members = self.old_config['members']
        me = self.getMe()
        new_members = [member for member in old_members if member['host'] != me]
        new_config = copy.copy(self.old_config)
        new_config['members'] = new_members
        new_config['version'] += 1
        self.adminCommand('replSetReconfig', new_config, primary=True)

    def replRestore(self):
        logging.debug('Adding {host} back to replica set'.format(host=self))
        self.old_config['version'] += 2
        self.adminCommand('replSetReconfig', self.old_config, primary=True)

    def mongo_uri(self):
        #mongodb://[username:password@]host1[:port1][/[database]
        if self.user:
            return "mongodb://{user}:{password}@{host}:{port}".format(
                user=self.user,
                password=self.password,
                host=self.hostname,
                port=self.port
            )
        else:
            return "mongodb://{host}:{port}".format(
                host=self.hostname,
                port=self.port
            )
        
class Backup:
    def __init__(self, host, kind='mongodump', offline=False):
        logging.debug('Initializing {kind} backup object, offline: {offline}'.format(
            kind=kind,
            offline=offline
        ))
        self.host = host
        self.kind = kind
        self.offline = offline
        
        if self.offline:
            pass
            #self.connection = pymongo.Connection(self.host.hostname, self.host.port)
            #if self.host.user and self.host.password:
            #    self.connection.admin.authenticate(self.host.user, self.host.password)
        if self.kind == 'raw':
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
    def prepare(self):
        logging.info('Preparing to backup {host}'.format( host=self.host ))
        # Shutdown the database
        self.host.connect()
        if self.offline:
            if self.host.isSecondary():
                self.host.replRemove()
            else:
                logging.error("Host is not secondary")
                sys.exit()
        
    def backup(self):
        logging.info('Backing up {host}'.format( host=self.host ))
        if self.kind == 'mongodump':
            logging.info('Performing mongodump...')
            cmd = ["mongodump",
                "-o", backup_dir,
                "-h", str(self.host.hostname),
                "--port", str(self.host.port),
            ]
            subprocess.Popen(cmd,
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
        elif self.kind == 'raw': # and check for offline status
            logging.info('Performing raw backup...')
            self.ssh_connect()
            # ssh -n remotehost "tar jcvf - SOURCEDIR" > DESTFILE.tar.gz
            #cmd = ["tar", "zcf", "-", source_dir]
            #stdin, stdout, sterr = self.ssh.exec_command(" ".join(cmd))
            #print stdout.read()
            if not os.path.exists(backup_dir):
                os.makdirs(backup_dir)
            tar_cmd = '"tar jcf - {source_dir}"'.format(source_dir=source_dir)
            cmd = ["ssh", "-n", self.host.hostname, "-p5555", tar_cmd, ">", "backup.tar.gz"]
            subprocess.call(cmd)
            # Make a tar archive from ssh pipe
            #tar = tarfile.open(backup_dir + 'backup.tar.gz', 'w:gz')
            #tar.add(stdout)
            #tar.close()
            
            # tar zcvf - /wwwdata | ssh root@dumpserver.nixcraft.in "cat > /backup/wwwdata.tar.gz"
        else:
            # DB was not locked
            logging.info('Database needs to be locked for raw backup')
    
    def ssh_connect(self):
        logging.info("Connecting via ssh")
        self.ssh.connect(self.host.hostname,
            username=self.host.sshuser,
            password=self.host.sshpass,
            port=self.host.sshport
        )
    
    def restore(self):
        logging.info('Restoring {host}'.format(host=self.host))
        self.host.replRestore()
        
    def run(self):
        self.prepare()
        try: self.backup()
        except Exception as e: logging.error('Error backing up (%s) -- aborting', e)
        self.restore()
    
def setup_logging(level=logging.DEBUG):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=level
    )
    logging.info('Logger initialized')

def parse_args():
    parser = argparse.ArgumentParser(description='Backup a MongoDB host')
    parser.add_argument('--host', help='hostname to backup')
    parser.add_argument('--user', help='MongoDB username (mongodump backup)')
    parser.add_argument('--password', help='MongoDB password (mongodump backup)')
    parser.add_argument('--port', help='port of mongod to backup', type=int)
    parser.add_argument('--sshpass', help='ssh password (raw backup)')
    parser.add_argument('--sshport', help='port of ssh host to backup', type=int)
    parser.add_argument('--srcdir', help='remote database data directory')
    parser.add_argument('--dir', help='directory to create backups')
    parser.add_argument('--kind', choices=['mongodump', 'raw'],
        default='mongodump',
        help='mongodump (default) or raw (copy dbfiles) requires lock to avoid corruption'
    )
    parser.add_argument('--offline', action='store_true', default=False,
        help='take database offline during backup',
    )
    parser.add_argument('--verbose', action='store_true', default=False,
        help='show debuging information'
    )
    return parser.parse_args()

def main(args):
    setup_logging(logging.DEBUG if args.verbose else logging.INFO)
    host = Host()
    if args.host: host.hostname = args.host
    if args.port: host.port = args.port
    if args.user: host.user = args.user
    if args.password: host.password = args.password
    if args.sshpass: host.sshpassword = args.sshpass
    if args.sshport: host.sshport = args.sshport
    backup_args = {}
    if args.kind == 'raw': backup_args['kind'] = 'raw' 
    if args.offline: backup_args['offline'] = True
    backup = Backup(host, **backup_args)
    if args.dir: backup_dir = args.dir
    if args.srcdir: source_dir = args.srcdir
    backup.run()

if __name__ == '__main__':
    main(parse_args())