import uuid
import os
import utils
import time

UPTODATE=-2


def inputsNewer(job):
    """Checks if any inputs are newer than any outputs

    inputs: list of file names
    outputs: list of file names

    Returns: True if the job is up-to-date

    If there are no outputs, the default behavior is to rerun the job.
    If there are no inputs, the job is rerun if the outputs do not
    all exist.  If there are both inputs and outputs, then dates are
    checked.  If that fails for any reason, the job is rerun.  
    """
    if(len(job.outputs)==0):
        return False
    if(len(job.inputs)==0):
        for f in job.outputs:
            if(not os.path.exists(f)):
                return False
        return True            
    try:
        try:
            newestInput = max(os.stat(x).st_mtime for x in job.inputs)
        except OSError:
            print "Looks like an input was not satisfied for this job"
            raise
        try:
            oldestOutput = min(os.stat(x).st_mtime for x in job.outputs)
            return (newestInput<oldestOutput)
        except OSError:
            return False
        return False
    except ValueError:
        return False
    print "should not be here"
    raise Exception
        
# Note: Can probably do the entire workflow thing using just Job class variables?

class Job(object):
    """Encapsulates a Job."""
    def __init__(self,command,nodes='1',params='',name=None,
                 inputs=[],outputs=[],uptodate=inputsNewer):
        self.inputs=inputs
        self.outputs=outputs
        self.nodes=nodes
        self.params=params
        self.command=command
        self.name=name
        self._uptodateFcn = uptodate
        self.dependencies=set()
        self.dependsOnMe=set()

    def addDependency(self,dependency):
        """Add dependencies to the job"""
        if(not isinstance(dependency,Job)):
            raise Exception("addDependency takes as input a single Job (or subclass)")
        self.dependencies.add(dependency)
        dependency.dependsOnMe.add(self)

    def __repr__(self):
        return "<Job name=%s>" % (self.name)


    def hashString(self):
        """
        Return a sha512 hexdigest of the command string after all whitespace is stripped

        This is meant to be a unique ID for the command line to be submitted.
        """
        return utils.hashString(self.command)
    
    def submit(self):
        """
        Submit the job to the queue using helix QSub
        """
        name=self.hashString()[0:10]
        if(self.name is not None):
            name=self.name
        if(self._uptodateFcn(self)):
            return UPTODATE
        import helix
        j = helix.QSub(command=self.command)
        (so,se) = j.submit(jobname=name,nodes=self.nodes,params=self.params)
        return so.strip(),se
        

class Workflow(object):
    """Holds a bunch of jobs and deals with submitting with dependencies

    Use like:

    a = Job(command='hostname',name='a',nodes='1:c2',params='')
    b = Job(command='hostname',name='b',nodes='1:c2',params='')
    c = Job(command='hostname',name='c',nodes='1:c2',params='')
    d = Job(command='hostname',name='d',nodes='1:c2',params='')
    wf = Workflow()
    wf.addJobs([a,b,c,d])
    c.addDependencies([a,b])
    d.addDependencies([c])
    print wf.getAllDependencies(d)
    wf.submit()

"""
    
    submitLog = {}
    
    def __init__(self,jobs=[],jobdb='~/.jobdb'):
        self.jobs=set()
        for job in jobs:
            self.jobs.append(job)
        self._subJobs = {}
        self._jobdb=os.path.expanduser(jobdb)

    def addJob(self,job):
        """Add more jobs to the workflow"""
        if(not isinstance(job,Job)):
            raise Exception("Argument to addJob must be a Job (or subclass), got %s" % str(job))
        self.jobs.add(job)

    def getJobsWithNoDependencies(self):
        return set([job for job in self.jobs if len(job.dependencies)==0])

    def getJobsWithNoDependsOnMe(self):
        return set([job for job in self.jobs if len(job.dependsOnMe)==0])
        
    def _getAllDependencies(self,job,deps):
        if(len(job.dependencies)==0):
            return(deps)
        else:
            for i in job.dependencies:
                deps.update(self._getAllDependencies(i,deps))
            for i in job.dependencies:
                deps.add(i)
            return deps
            
    def getAllDependencies(self,job):
        """Simply return all the dependencies (recursively) of a given job"""
        # must set the initial deps to the empty set, then do recursion
        return(self._getAllDependencies(job,deps=set()))


    def _submit(self,job,deps=set()):
        """Submit the given job

        Returns the biowulf job id of the submitted job"""
        if(len(deps)>0):
            job.params='-W depend=afterany:%s' % (":".join(deps))
        if(job in self.submitLog):
            return [self.submitLog[job]]
        else:
            self.submitLog[job]=job.submit()
            with open(self._jobdb,'a') as f:
                f.write("%s\t%s\t%s\n" %(str(self.submitLog[job]),str(job.name),
                                         utils.hashString(job.command)))
            return [self.submitLog[job]]
    
    def _recursiveSubmit(self,job,deps=set()):
        """Recursively submit a job and all its dependencies"""
        if(len(job.dependencies)==0):
            res = self._submit(job)
            if(len(res)==1 and res==UPTODATE):
                return(set())
            return(res)
        else:
            mydeps = set()
            for i in job.dependencies:
                mydeps.update(self._recursiveSubmit(i,deps))
            return(self._submit(job,mydeps))

    def submit(self,sleep=2):
        """Submit all the jobs in the workflow to Biowulf"""
        for job in self.jobs:
            time.sleep(sleep)
            print "submitting job %s" % str(job)
            self._recursiveSubmit(job)
        
        print self.submitLog
            
if __name__=="__main__":
    a = Job(command='hostname',name='a')
    b = Job(command='hostname',name='b')
    c = Job(command='hostname',name='c')
    d = Job(command='hostname',name='d')
    wf = Workflow()
    for job in [a,b,c,d]:
        wf.addJob(job)
    c.addDependency(a)
    c.addDependency(b)
    d.addDependency(c)
    print "a",wf.getAllDependencies(a)
    print "d",wf.getAllDependencies(d)
    print "a",wf.getAllDependencies(a)
    print "c",wf.getAllDependencies(c)
    print wf.getJobsWithNoDependencies()
    print wf.getJobsWithNoDependsOnMe()
    #wf.submit()
