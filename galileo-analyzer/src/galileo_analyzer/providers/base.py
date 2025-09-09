from abc import ABC, abstractmethod
from typing import List, Dict, Any
from ..core.config import CloudCredentials


class BaseCloudProvider(ABC):
    """Base interface for all cloud providers"""

    def __init__(self, credentials: CloudCredentials):
        self.credentials = credentials
        self._client = None

    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the cloud provider"""
        pass

    @abstractmethod
    def get_all_jobs(self) -> List[str]:
        """List all data jobs"""
        pass

    @abstractmethod
    def get_job_details(self, job_name: str) -> Dict[str, Any]:
        """Get detailed job information"""
        pass

    @abstractmethod
    def get_recent_runs(
        self, job_name: str, max_results: int = 5
    ) -> List[Dict[str, Any]]:
        """Get recent job executions"""
        pass

    def is_authenticated(self) -> bool:
        """Check if provider is properly authenticated"""
        return self._client is not None
