"""
Example usage of the executor system

This shows how to use the SlurmExecutor to submit jobs using artifacts
stored in the database.

NOTE: Since process_request represents ONE platform job (1:1 relationship),
      each process_request has its own executor instance.
      For multi-step workflows, create multiple process_requests.
"""

from workers.executors.slurm import SlurmExecutor


def submit_slurm_job(process_request_id: int):
    """
    Submit a SLURM job for a given process request

    Args:
        process_request_id: ID of the process_request record (one process_request = one SLURM job)

    Returns:
        SLURM job ID
    """
    # Configuration for SLURM connection
    config = {
        'host': 'slurm.example.com',
        'ssh_user': 'bioloop',
        'remote_work_dir': '/slate-scratch/jobs',
        'ssh_key_path': '/path/to/ssh/key'  # Optional
    }

    # Create executor for this specific process request
    executor = SlurmExecutor(config=config, process_request_id=process_request_id)

    # Submit job - executor will:
    # 1. Fetch JOB_SCRIPT artifact from database for this process_request
    # 2. Write script to remote SLURM host
    # 3. Submit via sbatch
    # 4. Return SLURM job ID
    slurm_job_id = executor.submit_job()

    print(f"Submitted SLURM job: {slurm_job_id}")

    return slurm_job_id


def check_job_status(process_request_id: int, slurm_job_id: str):
    """
    Check status of a SLURM job

    Args:
        process_request_id: ID of the process_request record
        slurm_job_id: SLURM job ID

    Returns:
        Job status dict
    """
    config = {
        'host': 'slurm.example.com',
        'ssh_user': 'bioloop',
        'remote_work_dir': '/slate-scratch/jobs',
    }

    executor = SlurmExecutor(config=config, process_request_id=process_request_id)

    status = executor.get_job_status(slurm_job_id)
    print(f"Job {slurm_job_id} status: {status['state']}")

    return status


def submit_multi_step_workflow(conversion_id: int):
    """
    Example: Submit a multi-step workflow
    (e.g., conversion → fastqc → multiqc)

    Each step has its own process_request record with job_step label.
    You would create 3 process_requests in the database:
      - process_request 1: {conversion_id: X, job_step: 'convert', artifacts: [convert script]}
      - process_request 2: {conversion_id: X, job_step: 'fastqc', artifacts: [fastqc script]}
      - process_request 3: {conversion_id: X, job_step: 'multiqc', artifacts: [multiqc script]}
    """
    # Assume these process_request IDs were created when the conversion was initiated
    # (typically done in the API when user submits the conversion)
    convert_pr_id = 123  # process_request with job_step='convert'
    fastqc_pr_id = 124   # process_request with job_step='fastqc'
    multiqc_pr_id = 125  # process_request with job_step='multiqc'

    config = {
        'host': 'slurm.example.com',
        'ssh_user': 'bioloop',
        'remote_work_dir': '/slate-scratch/jobs',
    }

    # Step 1: Run conversion
    convert_executor = SlurmExecutor(config=config, process_request_id=convert_pr_id)
    convert_job_id = convert_executor.submit_job()
    print(f"Submitted convert job: {convert_job_id}")

    # Wait for conversion to complete (in real code, use polling)
    while not convert_executor.is_job_complete(convert_job_id):
        import time
        time.sleep(30)

    # Step 2: Run FastQC
    fastqc_executor = SlurmExecutor(config=config, process_request_id=fastqc_pr_id)
    fastqc_job_id = fastqc_executor.submit_job()
    print(f"Submitted fastqc job: {fastqc_job_id}")

    # Step 3: Run MultiQC (could submit in parallel or wait for fastqc)
    multiqc_executor = SlurmExecutor(config=config, process_request_id=multiqc_pr_id)
    multiqc_job_id = multiqc_executor.submit_job()
    print(f"Submitted multiqc job: {multiqc_job_id}")

    return {
        'convert': convert_job_id,
        'fastqc': fastqc_job_id,
        'multiqc': multiqc_job_id
    }


if __name__ == '__main__':
    # Example: Submit a single job
    process_request_id = 123  # From database
    job_id = submit_slurm_job(process_request_id)

    # Check status
    status = check_job_status(process_request_id, job_id)
