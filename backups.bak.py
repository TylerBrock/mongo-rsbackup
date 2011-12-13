import subprocess
import pymongo
import paramiko

class Backup(object):
    
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
    
    def __str__(self):
        return "%s: %s:%d" % ("Backup", self.host, self.port)
        
    def run(self):
        self.prepare()
        self.backup()
        self.restore()
        
    def prepare(self):
        pass
        
    def backup(self):
        print "Backup Function not defined for this class"
        
    def restore(self):
        pass
        
class MDBackup(Backup):
    
    def backup(self):
        subprocess.call(["mongodump", "-h", str(self.host), "--port", str(self.port)])

class LockedBackup(Backup):
    
    def __init__(self, **kwargs):
        super(LockedBackup, self).__init__(**kwargs)
        self.connection = pymongo.Connection(self.host, self.port)
    
    def prepare(self):
        self.connection.admin.command("fsync", lock=True)
        
    def backup(self):
        if self.connection.is_locked:
            super(LockedBackup, self).backup()
        
    def restore(self):
        self.connection.unlock()

class LockedMDBackup(MDBackup, LockedBackup):
    pass
    
class FileBackup(LockedBackup):
        
    def backup(self):
        pass