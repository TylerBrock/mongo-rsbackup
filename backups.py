import subprocess
import pymongo
import paramiko

class Backup(object):
    
    def __init__(self, host="localhost", port=27017, lock=False):
        self.host = host
        self.port = port
    
    def __str__(self):
        return "%s: %s:%d" % ("Backup", self.host, self.port)
        
    def run(self):
        self.prepare()
        self.backup()
        self.restore()
        
    def prepare(self):
        self.connection.admin.command("fsync", lock=True)
        
    def backup(self):
        subprocess.call(["mongodump", "-h", str(self.host), "--port", str(self.port)])
        
    def restore(self):
        self.connection.unlock()