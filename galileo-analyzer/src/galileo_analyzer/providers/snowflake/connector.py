from typing import Dict, List, Any
from ..base import BaseCloudProvider
from ...core.config import SnowflakeCredentials
from ...core.exceptions import ProviderError


class SnowflakeProvider(BaseCloudProvider):
    """Placeholder Snowflake provider - Not yet implemented"""

    def __init__(self, credentials: SnowflakeCredentials):
        super().__init__(credentials)
        self._client = None

    def authenticate(self) -> bool:
        """Authenticate with Snowflake"""
        raise NotImplementedError(
            "Snowflake provider is not yet implemented. "
            "Coming in future version. Use --provider aws for now."
        )

    def get_all_jobs(self) -> List[str]:
        """List all Snowflake tasks/procedures"""
        raise NotImplementedError("Snowflake provider not yet implemented")

    def get_job_details(self, job_name: str) -> Dict[str, Any]:
        """Get Snowflake job details"""
        raise NotImplementedError("Snowflake provider not yet implemented")

    def get_recent_runs(
        self, job_name: str, max_results: int = 5
    ) -> List[Dict[str, Any]]:
        """Get Snowflake job runs"""
        raise NotImplementedError("Snowflake provider not yet implemented")
