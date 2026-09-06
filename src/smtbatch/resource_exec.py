"""Apply an inherited address-space limit before exec, without threaded preexec_fn."""
import os
import resource
import sys

if __name__=='__main__':
    memory=int(sys.argv[1])*1024*1024
    resource.setrlimit(resource.RLIMIT_AS,(memory,memory))
    os.execvp(sys.argv[2],sys.argv[2:])
