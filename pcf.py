import logging
from subprocess import check_output, CalledProcessError
import re
from time import sleep
import sys

class ClusterJob(object):
    
    def __init__(self, **kwargs):
        
        bridge_headers = {
            'project': '#MSUB -A',
            'ncores': '#MSUB -c',
            'error_file': '#MSUB -e',
            'ntasks': '#MSUB -n',
            'nnodes': '#MSUB -N',
            'output_file': '#MSUB -o',
            'queue': '#MSUB -q',
            'time': '#MSUB -T'
        }

        self.shell_header = '#!/bin/bash'
        self.job_name = 'cmd' 
        self.job_id = '0'
        self.shell_cmd = []
        self.job_headers = []

        # set job name (=shell script name)
        try:
            if kwargs['job_name']:
                self.job_name = kwargs['job_name'] 
        except:
            pass
 

        # set multiple commands with the ccc_mprun prefix + wait final
        try:
            if kwargs['msub']:
                for cmd in kwargs['msub']:
                    cmd_line = 'ccc_mprun -n1 -E' + "'--exclusive' " + 'bash -c "' + cmd + '" &' 
                    self.shell_cmd.append(cmd_line)
                self.shell_cmd.append('wait')
        except:
            pass
                # set shell commands for the job
        try:
            if kwargs['cmd']:
                for line in kwargs['cmd']:
                    self.shell_cmd.append(line)
        except:
            pass

        headers_dict = bridge_headers 
        # set job options passed in arguments
        for k,v in kwargs.iteritems():
            try:
                self.job_headers.append(headers_dict[k] + ' ' + v) 
            except:
                pass
        
    def submit(self):

        fh = open(self.job_name, 'w')
        fh.write(self.shell_header + '\n')
        for line in self.job_headers:
            fh.write(line + '\n')
        for line in self.shell_cmd:
            fh.write(line + '\n')
        fh.close()

        # submit job and get job number
        #out = check_output('bash %s' % self.job_name, shell = True)
        out = None
        try:
            out = check_output('ccc_msub %s' % self.job_name, shell = True)
        except CalledProcessError as e:
            logging.error("An error occured while trying to submit the job to the cluster")
            logging.error(e.output)
            sys.exit(1)

        match = re.search('(\d+)', out)
        if match:
            self.job_id = match.group(1)

    # monitor job in the cluster queue. If the job is not in the queue, it is assumed to be finished.
    def monitor(self):
        while( self.__check_cluster_queue() == True):
            sleep(10)

    def __check_cluster_queue(self):
        ret = False 
        out = check_output('squeue', shell = True)
        for line in out.split('\n'):
            if len(line) > 0 and self.job_id == line.strip().split()[0]:
                logging.info("job " + self.job_id + " status " + line.strip().split()[4])
                ret = True
                break
        return ret

    def print_cmd(self):
        """just print the job shell command to the screen."""
        print self.job_name
        print self.shell_header
        for line in self.job_headers:
            print line
        for line in self.shell_cmd:
            print line

    def check_job(self):
        """ check the status of the job after the run, and if anything strange happened, just quit. """
        out = check_output('sacct --jobs ' + self.job_id, shell = True)
        failed = False
        for w in ['CANCELLED', 'FAILED', 'NODE_FAIL', 'TIMEOUT']:
            m = re.search(w, out)
            if m:
                failed = True
        for line in out.split('\n'):
            logging.info("sacct output: " + line)

        if failed == True:
            logging.error("job " + self.job_id + " failed")
            sys.exit(1)

 

class Utils(object):

    def list_files(self):
        pass

