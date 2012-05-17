import subprocess
from itertools import izip
import hashlib
import os
import re

def qstat(userid):
    if(userid is not None):
        x = subprocess.Popen('qstat -u %s' % (userid),shell=True,stdout=subprocess.PIPE)
        lines = x.stdout
        lines.next()
        lines.next()
        lines.next()
        colnames = lines.next().strip().split()
        lines.next()
        for line in x.stdout:
            print dict(izip(colnames,line.strip().split()))

def hashString(s):
    s = re.sub(r'\s','',s)
    return hashlib.sha512(s).hexdigest()

def checkCommand(command,jobdb='~/.jobdb'):
    cmds = {}
    with open(os.path.expanduser(jobdb),'r') as f:
        for line in f:
            line.strip().split('\t')
            cmds[line[2]]=line
    hash = hashlib.sha512(command).hexdigest()
    if(hash in cmds):
        return cmds[hash]
    else:
        return None
        
            
if __name__ == '__main__':
    #qstat('sedavis')
    cmd = """
        #!/bin/bash
        /usr/local/bin/bwa aln -t 24 /data/CCRBioinfo/public/bwa/GRCh37 %s > %s
        touch %s
        """ % ('/data/sedavis/sequencing/fastq/1_1_70CTCAAXX.278_BUSTARD-2011-09-10.fq.gz','/data/CCRBioinfo/projects/TargetOsteosarcoma/exomes/bam/1_1_70CTCAAXX.278_BUSTARD-2011-09-10.sai','/data/CCRBioinfo/projects/TargetOsteosarcoma/exomes/bam/1_1_70CTCAAXX.278_BUSTARD-2011-09-10.sai.finished')
    print checkCommand(cmd)