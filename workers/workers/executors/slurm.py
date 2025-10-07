import time
from io import StringIO
from pathlib import Path

from fabric import Connection

from .base import ExecutorBase

# todo - make SSH connection optional

class SlurmExecutor(ExecutorBase):
    """Executor for SLURM-based HPC clusters"""

    def validate_config(self):
        """Validate required SLURM configuration"""
        required_keys = ['host', 'ssh_user', 'remote_work_dir']
        missing = [k for k in required_keys if k not in self.config]
        if missing:
            raise ValueError(f"Missing required SLURM config keys: {missing}")

    def __init__(self,
                config: dict,
                process_request_id: int):
        """
        Initialize SLURM executor

        Args:
            config: Dict with keys:
                - host: SLURM head node hostname
                - ssh_user: SSH username
                - remote_work_dir: Directory on SLURM host for job scripts
                - ssh_key_path: Optional path to SSH private key
            process_request_id: ID to fetch artifacts from database
        """
        super().__init__(config, process_request_id)

        self.remote_work_dir = config['remote_work_dir']
        
        connection_config = config['connection']
        self.host = connection_config['host']
        self.ssh_user = connection_config['user']
        self.ssh_key_path = connection_config.get('ssh_key_path')

        # Initialize Fabric connection
        connect_kwargs = {}
        if self.ssh_key_path:
            connect_kwargs['key_filename'] = self.ssh_key_path

        self.conn = Connection(
            host=self.host,
            user=self.ssh_user,
            connect_kwargs=connect_kwargs
        )

    def submit_job(self, **kwargs) -> str:
        """
        Submit a job to SLURM using artifacts from database

        Args:
            **kwargs: Additional parameters (not used currently)

        Returns:
            SLURM job ID as string
        """
        # Fetch artifacts for this process request
        artifacts = self.fetch_artifacts()

        if 'JOB_SCRIPT' not in artifacts:
            raise ValueError(f"No JOB_SCRIPT artifact found for process_request_id: {self.process_request_id}")

        job_script_content = artifacts['JOB_SCRIPT']['content_inline']

        # Generate unique script name
        timestamp = int(time.time())
        script_name = f"job_{self.process_request_id}_{timestamp}.sh"
        remote_path = f"{self.remote_work_dir}/{script_name}"

        # Write script to remote host via SSH
        self.conn.put(StringIO(job_script_content), remote_path)
        self.conn.run(f"chmod +x {remote_path}", hide=True)

        # Submit to SLURM via sbatch
        result = self.conn.run(f"sbatch {remote_path}", hide=True)

        if result.failed:
            raise Exception(f"sbatch failed: {result.stderr}")

        # Parse job ID from "Submitted batch job 12345"
        slurm_job_id = result.stdout.strip().split()[-1]
        return slurm_job_id

    def get_job_status(self, job_id: str) -> dict:
        """
        Get current status of a SLURM job

        Args:
            job_id: SLURM job ID

        Returns:
            Dict with: {
                'state': str,       # PENDING, RUNNING, COMPLETED, FAILED, etc.
                'exit_code': str,   # Exit code
                'elapsed': str,     # Elapsed time
                'max_memory': str   # Max memory used
            }
        """
        # First try squeue (for active jobs)
        result = self.conn.run(
            f"squeue -j {job_id} --format='%T' --noheader",
            warn=True,
            hide=True
        )

        # If job is in queue, return state
        if result.ok and result.stdout.strip():
            return {
                'state': result.stdout.strip(),
                'exit_code': None,
                'elapsed': None,
                'max_memory': None
            }

        # Job not in queue, check sacct (for completed/failed jobs)
        result = self.conn.run(
            f"sacct -j {job_id} --format=State,ExitCode,Elapsed,MaxRSS --parsable2 --noheader",
            warn=True,
            hide=True
        )

        if not result.ok or not result.stdout.strip():
            return {
                'state': 'UNKNOWN',
                'exit_code': None,
                'elapsed': None,
                'max_memory': None
            }

        # Parse sacct output
        fields = result.stdout.strip().split('|')

        return {
            'state': fields[0] if len(fields) > 0 else 'UNKNOWN',
            'exit_code': fields[1] if len(fields) > 1 else None,
            'elapsed': fields[2] if len(fields) > 2 else None,
            'max_memory': fields[3] if len(fields) > 3 else None
        }

    def cancel_job(self, job_id: str) -> None:
        """
        Cancel a SLURM job

        Args:
            job_id: SLURM job ID to cancel
        """
        result = self.conn.run(f"scancel {job_id}", warn=True, hide=True)

        if result.failed:
            raise Exception(f"scancel failed for job {job_id}: {result.stderr}")

    def is_job_complete(self, job_id: str) -> bool:
        """
        Check if a SLURM job has finished

        Args:
            job_id: SLURM job ID

        Returns:
            True if job is in terminal state (COMPLETED, FAILED, CANCELLED, TIMEOUT)
        """
        status = self.get_job_status(job_id)
        terminal_states = ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL']
        return status['state'] in terminal_states
