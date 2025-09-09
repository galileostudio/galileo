from typing import Dict, List, Any
from ..base import BaseCloudProvider
from ...core.config import DatabricksCredentials
from ...core.exceptions import ProviderError


class DatabricksProvider(BaseCloudProvider):
    """Placeholder Databricks provider - Not yet implemented"""

    def __init__(self, credentials: DatabricksCredentials):
        super().__init__(credentials)
        self._client = None

    def authenticate(self) -> bool:
        """Authenticate with Databricks"""
        raise NotImplementedError(
            "Databricks provider is not yet implemented. "
            "Coming in future version. Use --provider aws for now."
        )

    def get_all_jobs(self) -> List[str]:
        """List all Databricks jobs"""
        raise NotImplementedError("Databricks provider not yet implemented")

    def get_job_details(self, job_name: str) -> Dict[str, Any]:
        """Get Databricks job details"""
        raise NotImplementedError("Databricks provider not yet implemented")

    def get_recent_runs(
        self, job_name: str, max_results: int = 5
    ) -> List[Dict[str, Any]]:
        """Get Databricks job runs"""
        raise NotImplementedError("Databricks provider not yet implemented")
