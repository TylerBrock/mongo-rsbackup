#!/usr/bin/env python

"""
Backup a member of a replica set
"""

import argparse
import os
import subprocess
import paramiko
import logging
import tarfile

import pymongo

# Local defaults
cwd = os.getcwd()
backup_dir = os.path.join(cwd, 'backups')

# Remote defaults
source_dir = os.path.join('data','db')

# Global defaults
compression_level = 1

class Host:
    def __init__(self, hostname='localhost', port=27017, user=None, 
        password=None, sshuser=None, sshpass=None, sshport=22):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.sshuser = sshuser
        self.sshpass = sshpass
        self.sshport = sshport
    
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
    
    def __str__(self):
        return self.hostname
        
class Backup:
    def __init__(self, host, kind='mongodump', lock=False):
        logging.debug('Initializing %s backup object with lock: %s',
            kind, lock
        )
        self.host = host
        self.kind = kind
        self.lock = lock
        
        if self.lock:
            self.connection = pymongo.Connection(self.host.hostname, self.host.port)
            if self.host.user and self.host.password:
                self.connection.admin.authenticate(self.host.user, self.host.password)
        if self.kind == 'raw':
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
    def prepare(self):
        logging.info('Preparing to backup %s', self.host)
        if self.lock: self.connection.admin.command("fsync", lock=True)
        
    def backup(self):
        logging.info('Backing up %s', self.host)
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
        elif self.kind == 'raw' and self.connection.is_locked:
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
        logging.info('Restoring %s', self.host)
        if self.lock: self.connection.unlock()
        
    def run(self):
        self.prepare()
        try: self.backup()
        except Exception as e: logging.error('Error backing up (%s) -- aborting', e)
        self.restore()
    
def setup_logging(level=logging.DEBUG):
    logger = logging.basicConfig(
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
    parser.add_argument('--sshuser', help='ssh username (raw backup)')
    parser.add_argument('--sshpass', help='ssh password (raw backup)')
    parser.add_argument('--sshport', help='port of ssh host to backup', type=int)
    parser.add_argument('--srcdir', help='remote database data directory')
    parser.add_argument('--dir', help='directory to create backups')
    parser.add_argument('--kind', choices=['mongodump', 'raw'],
        default='mongodump',
        help='mongodump (default) or raw (copy dbfiles) requires lock to avoid corruption'
    )
    parser.add_argument('--lock', action='store_true', default=False,
        help='fsync and lock database during backup',
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
    if args.sshuser: host.sshuser = args.sshuser
    if args.sshpass: host.sshpassword = args.sshpass
    if args.sshport: host.sshport = args.sshport
    backup_args = {}
    if args.kind == 'raw': backup_args['kind'] = 'raw' 
    if args.lock: backup_args['lock'] = True
    backup = Backup(host, **backup_args)
    if args.dir: backup_dir = args.dir
    if args.srcdir: source_dir = args.srcdir
    backup.run()

if __name__ == '__main__':
    main(parse_args())