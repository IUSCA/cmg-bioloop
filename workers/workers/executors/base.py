from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from workers import api


class ExecutorBase(ABC):
    """Base class for all execution platforms"""

    def __init__(self, config: dict, process_request_id: int):
        """
        Args:
            config: Platform-specific configuration (host, credentials, paths, etc.)
            process_request_id: ID to fetch artifacts from database via API
        """
        self.config = config
        self.process_request_id = process_request_id
        self.validate_config()

    def validate_config(self):
        """Override in subclasses to validate platform-specific config requirements"""
        pass

    def fetch_artifacts(self, artifact_type: Optional[str] = None) -> Dict[str, dict]:
        """
        Fetch artifacts from database via API, optionally filtered by artifact_type.

        Args:
            artifact_type: Optional filter for artifact type (e.g., 'JOB_SCRIPT')

        Returns:
            Dict mapping artifact_type to artifact dict
        """
        artifacts = api.get_process_artifacts(
            process_request_id=self.process_request_id,
            artifact_type=artifact_type
        )

        # Convert list to dict keyed by artifact_type for easier lookup
        return {a['artifact_type']: a for a in artifacts}

    @abstractmethod
    def submit_job(self, **kwargs) -> str:
        """
        Submit job to execution platform.

        Args:
            **kwargs: Additional platform-specific parameters

        Returns:
            External job ID (e.g., SLURM job ID, K8s job name, etc.)
        """
        pass

    @abstractmethod
    def get_job_status(self, job_id: str) -> dict:
        """
        Get current status of a job.

        Args:
            job_id: External job ID

        Returns:
            Dict with job status information (state, metrics, etc.)
        """
        pass

    @abstractmethod
    def cancel_job(self, job_id: str) -> None:
        """
        Cancel a running job.

        Args:
            job_id: External job ID
        """
        pass

    @abstractmethod
    def is_job_complete(self, job_id: str) -> bool:
        """
        Check if job has finished.

        Args:
            job_id: External job ID

        Returns:
            True if job is in terminal state
        """
        pass
