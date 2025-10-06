from pathlib import Path

from fabric import Connection

from workers.config import config

from .base import ExecutorBase


class SlurmExecutor(ExecutorBase):
    """Executor for SLURM-based HPC clusters"""

    def __init__(self):
        execution_config = config['execution_config']['SLURM']

        self.host = execution_config['connection']['host']
        self.ssh_user = execution_config['connection']['user']
        self.ssh_key_path = execution_config['connection'].get('private_key')
        # self.defaults = execution_config.get('defaults', {})

        # Container directory for SLURM submission scripts
        self.slurm_script_dir = Path(execution_config['slurm_script_dir'])
        self.slurm_script_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Fabric connection
        connect_kwargs = {}
        if self.ssh_key_path:
            connect_kwargs['key_filename'] = self.ssh_key_path

        self.conn = Connection(
            host=self.host,
            user=self.ssh_user,
            connect_kwargs=connect_kwargs
        )

    def submit_job(self,
                   command,
                   working_dir,
                   worker_process_id,
                  #  resources,
                  #  environment=None
                   ) -> str:
        """
        Submit a job to SLURM

        Args:
            command: Command to execute (list or string)
            working_dir: Working directory for the job
            worker_process_id: ID for tracking the worker process
            resources: Dict of SLURM resources (gpus, mem, time_limit, partition, etc.)
            environment: Optional dict of environment variables

        Returns:
            SLURM job ID as string
        """
        # Merge defaults with job-specific resources
        # job_resources = {**self.defaults, **resources}

        # Prepare command
        if isinstance(command, list):
            command_str = ' '.join(str(c) for c in command)
        else:
            command_str = str(command)

        # Generate SLURM script
        script_path = self.generate_slurm_script(
            name='submission_script',
            command=command_str,
            working_dir=working_dir,
            # resources=job_resources,
            # environment=environment
        )

        # Submit to SLURM via SSH
        result = self.conn.run(f"sbatch {script_path}", hide=True)

        if result.failed:
            raise Exception(f"sbatch failed: {result.stderr}")

        # Parse job ID from "Submitted batch job 12345"
        job_id = result.stdout.strip().split()[-1]
        return job_id

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

    def generate_slurm_script(self,
                               name: str,
                               directives: str,
                               command: str,
                               working_dir: str) -> str:
        # script_path.chmod(0o755)

        pass


    def close(self):
        """Close the SSH connection"""
        if self.conn:
            self.conn.close()
