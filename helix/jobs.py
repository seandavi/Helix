import uuid
import os

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
        newestInput = max(os.stat(x).st_mtime for x in job.inputs)
        try:
            oldestOutput = min(os.stat(x).st_mtime for x in job.outputs)
            return (newestInput<oldestOutput)
        except OSError:
            return False
        return False
    except ValueError:
        return False
    finally:
        print "Looks like an input was not satisfied for this job"
        raise
        
# Note: Can probably do the entire workflow thing using just Job class variables?

class Job(object):
    """Encapsulates a Job."""
    def __init__(self,command,nodes,params,name=None,dependencies=[],
                 inputs=[],outputs=[],uptodate=inputsNewer):
        self.inputs=inputs
        self.outputs=outputs
        self.nodes=nodes
        self.params=params
        self.command=command
        self._uuid = uuid.uuid4()
        self.name=name
        self._uptodateFcn = uptodate
        self.dependencies=dependencies

    def addDependencies(self,dependencies):
        """Add dependencies to the job"""
        self.dependencies=self.dependencies+dependencies

    def __repr__(self):
        return "<Job name=%s id=%s command=%s>" % (self.name,str(self._uuid),self.command)

    def submit(self):
        """Submit the job to the queue using helix QSub"""
        name="J"+str(self._uuid)[:10]
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
    
    def __init__(self,jobs=[]):
        self.jobs=set()
        for job in jobs:
            self.jobs.append(job)
        self._subJobs = {}
            
    def addJobs(self,jobs):
        """Add more jobs to the workflow"""
        for job in jobs:
            self.jobs.add(job)
        
    def getAllDependencies(self,job,deps=[]):
        """Simply return all the dependencies (recursively) of a given job"""
        if(len(job.dependencies)==0):
            return(deps)
        else:
            for i in job.dependencies:
                deps = [i] + self.getAllDependencies(i,deps)
            return deps

    def _submit(self,job,deps=set()):
        """Submit the given job

        Returns the biowulf job id of the submitted job"""
        if(len(deps)>0):
            job.params='-W depend=afterany:%s' % (":".join(deps))
        if(job in self.submitLog):
            return [self.submitLog[job]]
        else:
            self.submitLog[job]=job.submit()[0]
            return [self.submitLog[job]]
    
    def _recursiveSubmit(self,job,deps=set()):
        """Recursively submit a job and all its dependencies"""
        if(len(job.dependencies)==0):
            return(self._submit(job))
        else:
            mydeps = set()
            for i in job.dependencies:
                mydeps.update(self._recursiveSubmit(i,deps))
            return(self._submit(job,mydeps))

    def submit(self):
        """Submit all the jobs in the workflow to Biowulf"""
        for job in self.jobs:
            print "submitting job %s" % str(job)
            self._recursiveSubmit(job)
        
        print self.submitLog
            
if __name__=="__main__":
    a = Job(command='hostname',name='a',nodes='1:c2',params='')
    b = Job(command='hostname',name='b',nodes='1:c2',params='')
    c = Job(command='hostname',name='c',nodes='1:c2',params='')
    d = Job(command='hostname',name='d',nodes='1:c2',params='')
    wf = Workflow()
    wf.addJobs([a,b,c,d])
    c.addDependencies([a,b])
    d.addDependencies([c])
    #print wf.getAllDependencies(d)
    wf.submit()
