import os
import subprocess

from workers import cmd

from .base import ExecutorBase


class LocalExecutor(ExecutorBase):
    """Runs jobs locally via subprocess (current implementation)"""
    
    def submit_job(self, command):
        # Current cmd.execute() logic
        process = subprocess.Popen(command)
        return str(process.pid)
    
    def get_job_status(self, job_id: str):
        # Check if PID is running
        try:
            os.kill(int(job_id), 0)
            return {"state": "RUNNING"}
        except OSError:
            return {"state": "COMPLETED"}
