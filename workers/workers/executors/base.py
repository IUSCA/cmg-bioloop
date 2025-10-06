from abc import ABC, abstractmethod


class ExecutorBase(ABC):
    """Base class for all execution platforms"""
    
    @abstractmethod
    def submit_job(self, command: list[str]) -> str:
        """Submit job and return job ID"""
        pass
    
    @abstractmethod
    def get_job_status(self, job_id: str) -> dict:
        """Returns: {state: str, metrics: dict, logs: str}"""
        pass
    
    @abstractmethod
    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job"""
        pass
    
    @abstractmethod
    def is_job_complete(self, job_id: str) -> bool:
        """Check if job has finished"""
        pass
