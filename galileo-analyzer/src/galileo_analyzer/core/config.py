from dataclasses import dataclass
from typing import Optional, Dict, Any
import os
from pathlib import Path


@dataclass
class CloudCredentials:
    """Base class for cloud credentials"""

    pass


@dataclass
class AWSCredentials(CloudCredentials):
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    region: Optional[str] = None
    profile: Optional[str] = None

    @classmethod
    def from_env(cls) -> "AWSCredentials":
        """Load credentials from environment variables"""
        return cls(
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            session_token=os.getenv("AWS_SESSION_TOKEN"),
            region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )

    @classmethod
    def from_params(
        cls,
        access_key_id: str,
        secret_access_key: str,
        session_token: Optional[str] = None,
        region: str = "us-east-1",
    ) -> "AWSCredentials":
        """Create credentials from explicit parameters"""
        return cls(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region=region,
        )

    @classmethod
    def from_profile(cls, profile: str, region: str = "us-east-1") -> "AWSCredentials":
        """Use AWS CLI profile"""
        return cls(profile=profile, region=region)


@dataclass
class DatabricksCredentials(CloudCredentials):
    """Placeholder for Databricks credentials"""

    workspace_url: str
    access_token: str


@dataclass
class SnowflakeCredentials(CloudCredentials):
    """Placeholder for Snowflake credentials"""

    account: str
    username: str
    password: str
    warehouse: Optional[str] = None
    database: Optional[str] = None


class GalileoConfig:
    """Main configuration class"""

    def __init__(self, config_file: Optional[Path] = None):
        self.config_file = config_file or Path.home() / ".galileo" / "config.yaml"
        self._load_config()

    def _load_config(self):
        """Load configuration from file if exists"""
        # TODO: Implement YAML config loading
        pass

    def get_aws_credentials(self) -> AWSCredentials:
        """Get AWS credentials with fallback priority"""
        # 1. Try environment variables first
        env_creds = AWSCredentials.from_env()
        if env_creds.access_key_id and env_creds.secret_access_key:
            return env_creds

        # 2. Try default profile
        return AWSCredentials.from_profile("default")
